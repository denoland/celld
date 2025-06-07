import type { Serializable } from "./types.ts";

/**
 * Safe JSON serialization that handles undefined values
 */
export function toJson(v: Serializable | undefined): string {
  return v === undefined ? "null" : JSON.stringify(v);
}

/**
 * Safe JSON deserialization that converts null back to undefined
 */
export function fromJson(json: string | null): Serializable {
  const parsed = JSON.parse(json ?? "null");
  return parsed === null ? undefined : parsed;
}
