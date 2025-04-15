// Create a WebSocket server and handle HTTP requests
Deno.serve((request) => {
  // Check if it's a WebSocket upgrade request
  if (request.headers.get("upgrade") === "websocket") {
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
  return new Response("hello from ws-echo.local\n");
});