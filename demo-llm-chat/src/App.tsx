import { useState, useEffect, useRef } from 'react'
import './App.css'

interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: string;
}

function App() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [isConnected, setIsConnected] = useState(false);
  const [isWaiting, setIsWaiting] = useState(false);
  const ws = useRef<WebSocket | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    // Calculate WebSocket URL based on current location
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}`;
    
    // Create WebSocket connection
    ws.current = new WebSocket(wsUrl);
    
    ws.current.onopen = () => {
      console.log('Connected to chat server');
      setIsConnected(true);
    };
    
    ws.current.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        
        if (data.type === 'message') {
          setMessages(prev => [...prev, {
            id: crypto.randomUUID(),
            role: data.role,
            content: data.content,
            timestamp: data.timestamp
          }]);
          setIsWaiting(false);
        } else if (data.type === 'history') {
          setMessages(data.messages.map((msg: any) => ({
            ...msg,
            id: crypto.randomUUID()
          })));
        }
      } catch (error) {
        console.error('Error parsing message:', error);
      }
    };
    
    ws.current.onclose = () => {
      console.log('Disconnected from chat server');
      setIsConnected(false);
    };
    
    ws.current.onerror = (error) => {
      console.error('WebSocket error:', error);
    };
    
    return () => {
      if (ws.current) {
        ws.current.close();
      }
    };
  }, []);

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  const handleSendMessage = (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!input.trim() || !ws.current || ws.current.readyState !== WebSocket.OPEN) {
      return;
    }
    
    const message = {
      type: 'message',
      role: 'user',
      content: input.trim(),
      timestamp: new Date().toISOString()
    };
    
    ws.current.send(JSON.stringify(message));
    
    // Add user message to the UI immediately
    setMessages(prev => [...prev, {
      id: crypto.randomUUID(),
      role: 'user',
      content: input.trim(),
      timestamp: new Date().toISOString()
    }]);
    
    setInput('');
    setIsWaiting(true);
  };

  const handleClearHistory = () => {
    if (ws.current && ws.current.readyState === WebSocket.OPEN) {
      ws.current.send(JSON.stringify({ type: 'clear' }));
      setMessages([]);
    }
  };

  return (
    <div className="chat-container">
      <header className="chat-header">
        <h1>AI Chat</h1>
        <button 
          onClick={handleClearHistory} 
          className="clear-button"
          disabled={!isConnected}
        >
          Clear History
        </button>
      </header>
      
      <div className="messages-container">
        {messages.length === 0 ? (
          <div className="welcome-message">
            <h2>Welcome to AI Chat</h2>
            <p>Start a conversation with the AI assistant.</p>
          </div>
        ) : (
          messages.map((message) => (
            <div 
              key={message.id} 
              className={`message ${message.role === 'user' ? 'user-message' : 'assistant-message'}`}
            >
              <div className="message-content">{message.content}</div>
              <div className="message-timestamp">
                {new Date(message.timestamp).toLocaleTimeString()}
              </div>
            </div>
          ))
        )}
        {isWaiting && (
          <div className="message assistant-message">
            <div className="message-content">Thinking...</div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>
      
      <form className="input-form" onSubmit={handleSendMessage}>
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="Type your message..."
          disabled={!isConnected || isWaiting}
        />
        <button 
          type="submit" 
          disabled={!isConnected || !input.trim() || isWaiting}
        >
          Send
        </button>
      </form>
    </div>
  )
}

export default App