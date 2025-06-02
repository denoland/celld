export interface Channel {
  id: string;
  name: string;
  creator_github_id: string;
  created_at: string;
}

class ChannelService {
  private baseUrl = "/cell/channel-registry";

  async createChannel(name: string): Promise<Channel> {
    const token = localStorage.getItem("auth_token");
    if (!token) throw new Error("No auth token");

    const response = await fetch(`${this.baseUrl}/create`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
      },
      body: JSON.stringify({ name }),
    });

    if (!response.ok) {
      throw new Error(`Failed to create channel: ${response.statusText}`);
    }

    return response.json();
  }

  async listChannels(): Promise<Channel[]> {
    const token = localStorage.getItem("auth_token");
    if (!token) throw new Error("No auth token");

    const response = await fetch(`${this.baseUrl}/list`, {
      headers: {
        "Authorization": `Bearer ${token}`,
      },
    });

    if (!response.ok) {
      throw new Error(`Failed to list channels: ${response.statusText}`);
    }

    return response.json();
  }

  async deleteChannel(channelId: string): Promise<void> {
    const token = localStorage.getItem("auth_token");
    if (!token) throw new Error("No auth token");

    const response = await fetch(`${this.baseUrl}/delete`, {
      method: "DELETE",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
      },
      body: JSON.stringify({ channelId }),
    });

    if (!response.ok) {
      const error = await response.text();
      throw new Error(error || `Failed to delete channel: ${response.statusText}`);
    }
  }
}

export const channelService = new ChannelService();
