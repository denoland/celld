import { useEffect, useState } from "preact/hooks";
import { type Channel, channelService } from "./channelService";
import { formatDate } from "./utils";
import { authService } from "./auth";

interface ChannelsProps {
  selectedChannel: { id: string; name: string } | null;
  onSelectChannel: (
    channel: { id: string; name: string } | null,
    mode?: "chat" | "memory",
  ) => void;
}

export function Channels({ selectedChannel, onSelectChannel }: ChannelsProps) {
  const [channels, setChannels] = useState<Channel[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [newChannelName, setNewChannelName] = useState("");
  const [creating, setCreating] = useState(false);
  const [deleting, setDeleting] = useState<string | null>(null);
  const [selectedChannelInfo, setSelectedChannelInfo] = useState<
    Channel | null
  >(null);

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
                  <p className="channel-date">
                    Created {formatDate(channel.created_at)}
                  </p>
                  {channel.personality && (
                    <p
                      className="channel-personality"
                      title={channel.personality}
                    >
                      {channel.personality.substring(0, 100)}...
                    </p>
                  )}
                </div>
                <div className="channel-actions">
                  {channel.personality && (
                    <button
                      className="info-channel-btn"
                      onClick={() => setSelectedChannelInfo(channel)}
                      title="View channel info"
                    >
                      ℹ️
                    </button>
                  )}
                  <button
                    className="join-channel-btn"
                    onClick={() =>
                      onSelectChannel({ id: channel.id, name: channel.name })}
                  >
                    {selectedChannel?.id === channel.id ? "Leave" : "Join"}
                  </button>
                  {currentUser?.github_id === channel.creator_github_id && (
                    <>
                      <button
                        className="memory-channel-btn"
                        onClick={() =>
                          onSelectChannel(
                            { id: channel.id, name: channel.name },
                            "memory",
                          )}
                        title="Manage bot memories"
                      >
                        🧠
                      </button>
                      <button
                        className="delete-channel-btn"
                        onClick={() => handleDeleteChannel(channel.id)}
                        disabled={deleting === channel.id}
                        title="Delete channel"
                      >
                        {deleting === channel.id ? "..." : "🗑️"}
                      </button>
                    </>
                  )}
                </div>
              </div>
            ))
          )}
      </div>

      {/* Channel Info Modal */}
      {selectedChannelInfo && (
        <div
          className="modal-overlay"
          onClick={() => setSelectedChannelInfo(null)}
        >
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2>#{selectedChannelInfo.name}</h2>
              <button
                className="modal-close-btn"
                onClick={() => setSelectedChannelInfo(null)}
              >
                ✕
              </button>
            </div>
            <div className="modal-body">
              <h3>AI Personality</h3>
              <p className="personality-full">
                {selectedChannelInfo.personality}
              </p>
              <div className="modal-meta">
                <p>Created: {formatDate(selectedChannelInfo.created_at)}</p>
                <p>ID: {selectedChannelInfo.id}</p>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
