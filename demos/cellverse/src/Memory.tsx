import { useEffect, useState } from "preact/hooks";

interface Memory {
  id: number;
  key: string;
  value: string;
  created_at: string;
  updated_at: string;
}

interface MemoryProps {
  channelId: string;
  channelName: string;
  onClose: () => void;
}

export function Memory({ channelId, channelName, onClose }: MemoryProps) {
  const [memories, setMemories] = useState<Memory[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isOwner, setIsOwner] = useState(false);
  const [deleting, setDeleting] = useState<number | null>(null);

  useEffect(() => {
    loadMemories();
  }, [channelId]);

  const loadMemories = async () => {
    try {
      setLoading(true);
      const token = localStorage.getItem("auth_token");
      if (!token) {
        setError("Not authenticated");
        return;
      }

      const response = await fetch(
        `http://localhost:8000/cell/${channelId}/memories`,
        {
          headers: {
            "Authorization": `Bearer ${token}`,
          },
        },
      );

      if (!response.ok) {
        if (response.status === 403) {
          setError("Only channel owners can view memories");
          setIsOwner(false);
        } else {
          setError("Failed to load memories");
        }
        return;
      }

      const data = await response.json();
      setMemories(data.memories);
      setIsOwner(true);
      setError(null);
    } catch (err) {
      setError("Failed to load memories");
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteMemory = async (memoryId: number) => {
    if (!confirm("Are you sure you want to delete this memory?")) {
      return;
    }

    try {
      setDeleting(memoryId);
      const token = localStorage.getItem("auth_token");
      const response = await fetch(
        `http://localhost:8000/cell/${channelId}/memories/${memoryId}`,
        {
          method: "DELETE",
          headers: {
            "Authorization": `Bearer ${token}`,
          },
        },
      );

      if (!response.ok) {
        throw new Error("Failed to delete memory");
      }

      await loadMemories();
    } catch (err) {
      setError("Failed to delete memory");
      console.error(err);
    } finally {
      setDeleting(null);
    }
  };

  const formatDate = (dateStr: string) => {
    return new Date(dateStr + "Z").toLocaleString();
  };

  return (
    <div className="memory-container">
      <div className="memory-header">
        <div className="memory-header-info">
          <h2>🧠 Bot Memory - #{channelName}</h2>
          <span className="memory-count">
            {memories.length} {memories.length === 1 ? "memory" : "memories"}
          </span>
        </div>
        <button className="close-memory-btn" onClick={onClose}>
          ✕
        </button>
      </div>

      {loading
        ? (
          <div className="memory-loading">
            <div className="loading-spinner"></div>
            <p>Loading memories...</p>
          </div>
        )
        : error
        ? (
          <div className="memory-error">
            <div className="error-message">{error}</div>
          </div>
        )
        : !isOwner
        ? (
          <div className="memory-error">
            <div className="error-message">
              Only channel owners can access bot memories
            </div>
          </div>
        )
        : (
          <div className="memories-list">
            {memories.length === 0
              ? (
                <div className="no-memories">
                  <div className="no-memories-icon">🤖</div>
                  <h3>No memories yet</h3>
                  <p>The bot hasn't stored any memories in this channel.</p>
                </div>
              )
              : (
                memories.map((memory) => (
                  <div key={memory.id} className="memory-item">
                    <div className="memory-content">
                      <div className="memory-key">{memory.key}</div>
                      <div className="memory-value">{memory.value}</div>
                      <div className="memory-meta">
                        <span>Created: {formatDate(memory.created_at)}</span>
                        {memory.created_at !== memory.updated_at && (
                          <span>
                            • Updated: {formatDate(memory.updated_at)}
                          </span>
                        )}
                      </div>
                    </div>
                    <button
                      className="delete-memory-btn"
                      onClick={() =>
                        handleDeleteMemory(memory.id)}
                      disabled={deleting === memory.id}
                      title="Delete memory"
                    >
                      {deleting === memory.id ? "..." : "🗑️"}
                    </button>
                  </div>
                ))
              )}
          </div>
        )}
    </div>
  );
}
