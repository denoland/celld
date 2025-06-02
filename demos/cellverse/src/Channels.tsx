import { useEffect, useState } from "preact/hooks";
import { type Channel, channelService } from "./channelService";

interface ChannelsProps {
  selectedChannel: { id: string; name: string } | null;
  onSelectChannel: (channel: { id: string; name: string } | null) => void;
}

export function Channels({ selectedChannel, onSelectChannel }: ChannelsProps) {
  const [channels, setChannels] = useState<Channel[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [newChannelName, setNewChannelName] = useState("");
  const [creating, setCreating] = useState(false);

  useEffect(() => {
    loadChannels();
  }, []);

  const loadChannels = async () => {
    try {
      setLoading(true);
      const channelList = await channelService.listChannels();
      setChannels(channelList);
      setError(null);
    } catch (err) {
      setError("Failed to load channels");
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const handleCreateChannel = async (e: Event) => {
    e.preventDefault();
    if (!newChannelName.trim()) return;

    try {
      setCreating(true);
      await channelService.createChannel(newChannelName.trim());
      setNewChannelName("");
      setShowCreateForm(false);
      await loadChannels();
    } catch (err) {
      setError("Failed to create channel");
      console.error(err);
    } finally {
      setCreating(false);
    }
  };

  if (loading) {
    return (
      <div className="channels-loading">
        <div className="loading-spinner"></div>
        <p>Loading channels...</p>
      </div>
    );
  }

  return (
    <div className="channels-container">
      <div className="channels-header">
        <h2>Channels</h2>
        <button
          className="create-channel-btn"
          onClick={() => setShowCreateForm(!showCreateForm)}
        >
          {showCreateForm ? "Cancel" : "+ New Channel"}
        </button>
      </div>

      {error && <div className="error-message">{error}</div>}

      {showCreateForm && (
        <form className="create-channel-form" onSubmit={handleCreateChannel}>
          <input
            type="text"
            placeholder="Channel name"
            value={newChannelName}
            onInput={(e) =>
              setNewChannelName((e.target as HTMLInputElement).value)}
            disabled={creating}
            autoFocus
          />
          <button type="submit" disabled={creating || !newChannelName.trim()}>
            {creating ? "Creating..." : "Create"}
          </button>
        </form>
      )}

      <div className="channels-list">
        {channels.length === 0
          ? (
            <div className="no-channels">
              <p>No channels yet. Create the first one!</p>
            </div>
          )
          : (
            channels.map((channel) => (
              <div key={channel.id} className="channel-item">
                <div className="channel-icon">#</div>
                <div className="channel-info">
                  <h3>{channel.name}</h3>
                  <p>
                    Created {new Date(channel.created_at).toLocaleDateString()}
                  </p>
                </div>
                <button
                  className="join-channel-btn"
                  onClick={() =>
                    onSelectChannel({ id: channel.id, name: channel.name })}
                >
                  {selectedChannel?.id === channel.id ? "Leave" : "Join"}
                </button>
              </div>
            ))
          )}
      </div>
    </div>
  );
}
