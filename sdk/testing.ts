import { DatabaseSync } from "node:sqlite";

/**
 * Create an in-memory SQLite database for testing
 */
export function createTestDatabase(): DatabaseSync {
  return new DatabaseSync(":memory:");
}

/**
 * Wait for a condition to be true, with timeout
 */
export async function waitFor(
  condition: () => boolean | Promise<boolean>,
  options?: {
    timeoutMs?: number;
    intervalMs?: number;
    message?: string;
  },
): Promise<void> {
  const timeoutMs = options?.timeoutMs ?? 5000;
  const intervalMs = options?.intervalMs ?? 100;
  const message = options?.message ?? "Condition not met";

  const startTime = Date.now();

  while (true) {
    const result = await condition();
    if (result) {
      return;
    }

    if (Date.now() - startTime > timeoutMs) {
      throw new Error(`Timeout waiting for condition: ${message}`);
    }

    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}

/**
 * Helper to create test data in database
 */
export function setupTestData(
  db: DatabaseSync,
  setup: (db: DatabaseSync) => void,
): void {
  setup(db);
}

/**
 * Time control utilities for testing
 */
export class TimeController {
  #currentTime: number;
  #originalDateNow: typeof Date.now;
  #originalSetTimeout: typeof setTimeout;
  #timers: Map<number, { callback: () => void; triggerTime: number }>;
  #nextTimerId: number;

  constructor(initialTime?: number) {
    this.#currentTime = initialTime ?? Date.now();
    this.#originalDateNow = Date.now;
    this.#originalSetTimeout = globalThis.setTimeout;
    this.#timers = new Map();
    this.#nextTimerId = 1;
  }

  /**
   * Install time mocks
   */
  install(): void {
    // Mock Date.now
    Date.now = () => this.#currentTime;

    // Mock setTimeout
    (globalThis as any).setTimeout = (callback: () => void, delay: number) => {
      const timerId = this.#nextTimerId++;
      this.#timers.set(timerId, {
        callback,
        triggerTime: this.#currentTime + delay,
      });
      return timerId;
    };
  }

  /**
   * Restore original time functions
   */
  uninstall(): void {
    Date.now = this.#originalDateNow;
    globalThis.setTimeout = this.#originalSetTimeout;
  }

  /**
   * Advance time by specified milliseconds
   */
  advance(ms: number): void {
    this.#currentTime += ms;

    // Trigger any timers that should fire
    for (const [timerId, timer] of this.#timers.entries()) {
      if (timer.triggerTime <= this.#currentTime) {
        timer.callback();
        this.#timers.delete(timerId);
      }
    }
  }

  /**
   * Set current time to specific value
   */
  setTime(time: number | Date): void {
    this.#currentTime = typeof time === "number" ? time : time.getTime();
  }

  /**
   * Get current mocked time
   */
  get currentTime(): number {
    return this.#currentTime;
  }
}
