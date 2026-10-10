export type StopOutcome = "STOP_REQUESTED" | "STOP_UNCONFIRMED" | "CONFIRMED_STOPPED" | null;

export function tradingState(enabled: boolean, outcome: StopOutcome, stale = false): string {
  if (stale) return "Unknown";
  if (outcome === "STOP_REQUESTED" || outcome === "STOP_UNCONFIRMED") return "Stop unconfirmed";
  return enabled ? "Enabled" : "Disabled";
}
