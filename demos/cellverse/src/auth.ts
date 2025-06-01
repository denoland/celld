export interface User {
  github_id: string;
  username: string;
  email?: string;
  avatar_url?: string;
}

export class AuthService {
  private token: string | null = null;
  private user: User | null = null;

  constructor() {
    // Load token from localStorage on init
    const storedToken = localStorage.getItem("auth_token");
    if (storedToken) {
      this.token = storedToken;
      // Verify token and load user info
      this.loadUser();
    }
  }

  async loadUser(): Promise<void> {
    if (!this.token) return;

    try {
      const response = await fetch("/cell/auth/me", {
        headers: {
          Authorization: `Bearer ${this.token}`,
        },
      });

      if (response.ok) {
        this.user = await response.json();
      } else {
        // Token is invalid, clear it
        this.logout();
      }
    } catch (error) {
      console.error("Failed to load user:", error);
      this.logout();
    }
  }

  login(): void {
    window.location.href = "/cell/auth/github/login";
  }

  logout(): void {
    this.token = null;
    this.user = null;
    localStorage.removeItem("auth_token");
  }

  handleAuthCallback(token: string): void {
    this.token = token;
    localStorage.setItem("auth_token", token);
    this.loadUser();
  }

  getToken(): string | null {
    return this.token;
  }

  getUser(): User | null {
    return this.user;
  }

  isAuthenticated(): boolean {
    return !!this.token && !!this.user;
  }
}

export const authService = new AuthService();
