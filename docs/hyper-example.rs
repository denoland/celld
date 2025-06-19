#![deny(warnings)]

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::Empty;
use hyper::header::{CONNECTION, SEC_WEBSOCKET_KEY, UPGRADE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::fs;
use std::net::SocketAddr;
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio_tungstenite::tungstenite::protocol::Message;

// Custom Error and Result types
type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

/// WebSocket Echo Server using HTTP upgrade on a Unix domain socket
async fn run_uds_echo_server() -> Result<()> {
    // Clean up existing socket file if it exists
    let _ = fs::remove_file("/tmp/foo.sock");

    // Bind to the Unix domain socket
    let listener = UnixListener::bind("/tmp/foo.sock")?;
    println!("WebSocket UDS server listening on /tmp/foo.sock");

    // Handle incoming connections
    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        // Process connections in a separate task
        tokio::spawn(async move {
            // Define a service that processes WebSocket requests
            async fn handle_connection(
                req: Request<hyper::body::Incoming>,
            ) -> Result<Response<Empty<Bytes>>> {
                // Check if it's a WebSocket upgrade request
                if !req.headers().contains_key(SEC_WEBSOCKET_KEY) {
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Empty::new())
                        .unwrap());
                }

                // Create a WebSocket upgrade response
                let mut res = Response::builder()
                    .status(StatusCode::SWITCHING_PROTOCOLS)
                    .header(UPGRADE, "websocket")
                    .header(CONNECTION, "upgrade");

                // Add the accept key
                if let Some(key) = req.headers().get(SEC_WEBSOCKET_KEY) {
                    let accept_key = tokio_tungstenite::tungstenite::handshake::derive_accept_key(
                        key.as_bytes(),
                    );
                    res = res.header("Sec-WebSocket-Accept", accept_key);
                }

                let res = res.body(Empty::new()).unwrap();

                // Handle the upgraded connection
                let req = req;
                tokio::spawn(async move {
                    match hyper::upgrade::on(req).await {
                        Ok(upgraded) => {
                            let ws_stream = tokio_tungstenite::WebSocketStream::from_raw_socket(
                                TokioIo::new(upgraded),
                                tokio_tungstenite::tungstenite::protocol::Role::Server,
                                None,
                            )
                            .await;

                            let (mut write, mut read) = ws_stream.split();

                            // Echo each message back to the client
                            while let Some(msg) = read.next().await {
                                match msg {
                                    Ok(msg) => {
                                        if msg.is_close() {
                                            break;
                                        }

                                        if let Err(e) = write.send(msg).await {
                                            eprintln!("Error sending WebSocket message: {}", e);
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("Error receiving WebSocket message: {}", e);
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => eprintln!("Error during WebSocket upgrade: {}", e),
                    }
                });

                Ok(res)
            }

            // Create a Hyper service from our handler function
            let service = service_fn(handle_connection);

            // Serve the HTTP connection with upgrade support
            let conn = http1::Builder::new()
                .serve_connection(io, service)
                .with_upgrades();

            if let Err(e) = conn.await {
                eprintln!("UDS server connection error: {}", e);
            }
        });
    }
}

/// HTTP handler for proxy WebSocket connections to the UDS server
async fn handle_http_upgrade(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<Empty<Bytes>>> {
    // Check if it's a WebSocket upgrade request
    if !req.headers().contains_key(SEC_WEBSOCKET_KEY) {
        return Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Empty::new())
            .unwrap());
    }

    // Connect to the Unix domain socket
    let uds_stream = match UnixStream::connect("/tmp/foo.sock").await {
        Ok(stream) => stream,
        Err(e) => {
            eprintln!("Failed to connect to UDS: {}", e);
            return Ok(Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .body(Empty::new())
                .unwrap());
        }
    };

    // Forward the original WebSocket upgrade request to the UDS socket
    let mut req_builder = Request::builder()
        .method("GET")
        .uri("/")
        .version(hyper::Version::HTTP_11);

    // Copy over headers required for the WebSocket upgrade
    for (name, value) in req.headers() {
        if name == UPGRADE
            || name == CONNECTION
            || name == SEC_WEBSOCKET_KEY
            || name.as_str().starts_with("sec-websocket-")
        {
            req_builder = req_builder.header(name, value);
        }
    }

    // Build the upgrade request for the UDS socket
    let uds_req = req_builder.body(Empty::<Bytes>::new()).unwrap();

    // Create a client for the UDS socket
    let io = TokioIo::new(uds_stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;

    // Start a task to handle the HTTP connection
    tokio::spawn(async move {
        if let Err(e) = conn.with_upgrades().await {
            eprintln!("UDS client connection error: {}", e);
        }
    });

    // Send the WebSocket upgrade request to the UDS server
    let uds_res = sender.send_request(uds_req).await?;

    // Check if the upgrade was successful
    if uds_res.status() != StatusCode::SWITCHING_PROTOCOLS {
        return Ok(Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .body(Empty::new())
            .unwrap());
    }

    // Build the upgrade response for the client
    let mut res_builder = Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "upgrade");

    // Copy relevant headers from the UDS server response
    for (name, value) in uds_res.headers() {
        if name.as_str().starts_with("sec-websocket-") {
            res_builder = res_builder.header(name, value);
        }
    }

    // Create the final response
    let res = res_builder.body(Empty::new()).unwrap();

    // Handle the upgraded connection
    let req = req;
    tokio::spawn(async move {
        match hyper::upgrade::on(req).await {
            Ok(upgraded) => {
                // Forward data between client and UDS
                match hyper::upgrade::on(uds_res).await {
                    Ok(uds_upgraded) => {
                        let mut client_io = TokioIo::new(upgraded);
                        let mut uds_io = TokioIo::new(uds_upgraded);

                        // Copy data between the two connections
                        match tokio::io::copy_bidirectional(&mut client_io, &mut uds_io).await {
                            Ok((from_client, from_uds)) => {
                                println!(
                                    "Proxy complete: {} bytes from client, {} bytes from UDS",
                                    from_client, from_uds
                                );
                            }
                            Err(e) => eprintln!("Error in WebSocket proxy: {}", e),
                        }
                    }
                    Err(e) => eprintln!("Error upgrading UDS connection: {}", e),
                }
            }
            Err(e) => eprintln!("Error upgrading client connection: {}", e),
        }
    });

    Ok(res)
}

/// TCP server that proxies WebSocket connections to the UDS server
async fn run_tcp_proxy() -> Result<()> {
    // Create TCP listener
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = TcpListener::bind(addr).await?;
    println!("WebSocket proxy server listening on {}", addr);

    // Handle incoming connections
    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        // Process each connection in a separate task
        tokio::spawn(async move {
            let service = service_fn(handle_http_upgrade);
            let conn = http1::Builder::new()
                .serve_connection(io, service)
                .with_upgrades();

            if let Err(e) = conn.await {
                eprintln!("HTTP connection error: {}", e);
            }
        });
    }
}

/// Test client for the WebSocket echo server
async fn run_test_client() -> Result<()> {
    // Allow servers to start
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Connect to the WebSocket server via the proxy
    let (ws_stream, _) = tokio_tungstenite::connect_async("ws://localhost:3000").await?;
    println!("WebSocket connection established");

    let (mut write, mut read) = ws_stream.split();

    // Test messages
    let test_messages = [
        Message::Text("Hello from WebSocket!".to_string()),
        Message::Binary(vec![1, 2, 3, 4, 5]),
        Message::Text("Another test message".to_string()),
    ];

    // Send test messages and verify responses
    for msg in &test_messages {
        write.send(msg.clone()).await?;
        println!("Sent: {:?}", msg);

        if let Some(Ok(response)) = read.next().await {
            println!("Received: {:?}", response);
            assert_eq!(&response, msg, "Received message differs from sent message");
        } else {
            return Err("Connection closed or error occurred".into());
        }
    }

    // Close connection gracefully
    write.send(Message::Close(None)).await?;
    println!("Test completed successfully");

    Ok(())
}

/// Main function
#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting WebSocket UDS and proxy servers...");

    // Start the WebSocket server on the Unix domain socket
    let uds_handle = tokio::spawn(run_uds_echo_server());

    // Start the HTTP proxy server
    let proxy_handle = tokio::spawn(run_tcp_proxy());

    // Give servers time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Run the test client
    run_test_client().await?;

    // Wait for the servers (they run indefinitely)
    let _ = tokio::join!(uds_handle, proxy_handle);

    Ok(())
}

/// Self-contained WebSocket echo server for testing
#[cfg(test)]
mod test_server {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    // Simple WebSocket echo server for testing
    pub async fn run_echo_server() -> Result<SocketAddr> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        println!("Test WebSocket server listening on {}", addr);

        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    if let Ok(ws_stream) = accept_async(stream).await {
                        let (mut write, mut read) = ws_stream.split();

                        // Echo back each message
                        while let Some(Ok(msg)) = read.next().await {
                            if msg.is_close() {
                                break;
                            }

                            let _ = write.send(msg).await;
                        }
                    }
                });
            }
        });

        Ok(addr)
    }
}

/// Integration tests
#[cfg(test)]
mod tests {
    use super::test_server;
    use super::*;

    #[tokio::test]
    async fn test_direct_websocket() -> Result<()> {
        // Start a standalone WebSocket echo server
        let addr = test_server::run_echo_server().await?;

        // Connect directly to test the WebSocket echo functionality
        let url = format!("ws://{}", addr);
        let (ws_stream, _) = tokio_tungstenite::connect_async(&url).await?;
        let (mut write, mut read) = ws_stream.split();

        // Test a simple message exchange
        let test_msg = Message::Text("Test message".to_string());
        write.send(test_msg.clone()).await?;

        if let Some(Ok(msg)) = read.next().await {
            assert_eq!(msg, test_msg);
        } else {
            return Err("Did not receive echo response".into());
        }

        write.send(Message::Close(None)).await?;
        Ok(())
    }
}
