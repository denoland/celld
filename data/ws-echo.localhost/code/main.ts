// Create a WebSocket server and handle HTTP requests
Deno.serve((request) => {
  const url = new URL(request.url);
  console.log(`Request for path: ${url.pathname}`);
  
  // Check if it's a WebSocket upgrade request
  if (request.headers.get("upgrade") === "websocket") {
    console.log("WebSocket upgrade request detected");
    const { socket, response } = Deno.upgradeWebSocket(request);
    
    // Setup WebSocket event handlers
    socket.onopen = () => {
      console.log("WebSocket opened");
      // Send a welcome message 
      socket.send(JSON.stringify({ type: "welcome", message: "Welcome to ws-echo.local!" }));
    };
    
    socket.onmessage = (event) => {
      console.log("Received message:", event.data);
      // Echo the message back with a timestamp
      const timestamp = new Date().toISOString();
      socket.send(JSON.stringify({ 
        type: "echo", 
        originalMessage: event.data,
        timestamp
      }));
    };
    
    socket.onclose = () => console.log("WebSocket closed");
    socket.onerror = (e) => console.error("WebSocket error:", e);
    
    return response;
  }
  
  // Regular HTTP request
  if (url.pathname === "/ping") {
    return new Response("pong");
  }
  
  return new Response("hello from ws-echo.local\n");
});