import { useEffect, useState } from "preact/hooks";
import { type Channel, channelService } from "./channelService";
import { formatDate } from "./utils";
import { authService } from "./auth";

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
  const [deleting, setDeleting] = useState<string | null>(null);
  
  const currentUser = authService.getUser();

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

  const handleDeleteChannel = async (channelId: string) => {
    if (!confirm("Are you sure you want to delete this channel?")) {
      return;
    }

    try {
      setDeleting(channelId);
      await channelService.deleteChannel(channelId);
      await loadChannels();
      // If the deleted channel was selected, deselect it
      if (selectedChannel?.id === channelId) {
        onSelectChannel(null);
      }
    } catch (err: any) {
      setError(err.message || "Failed to delete channel");
      console.error(err);
    } finally {
      setDeleting(null);
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
                    Created {formatDate(channel.created_at)}
                  </p>
                </div>
                <div className="channel-actions">
                  <button
                    className="join-channel-btn"
                    onClick={() =>
                      onSelectChannel({ id: channel.id, name: channel.name })}
                  >
                    {selectedChannel?.id === channel.id ? "Leave" : "Join"}
                  </button>
                  {currentUser?.github_id === channel.creator_github_id && (
                    <button
                      className="delete-channel-btn"
                      onClick={() => handleDeleteChannel(channel.id)}
                      disabled={deleting === channel.id}
                      title="Delete channel"
                    >
                      {deleting === channel.id ? "..." : "🗑️"}
                    </button>
                  )}
                </div>
              </div>
            ))
          )}
      </div>
    </div>
  );
}
