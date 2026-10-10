export function ObserverState({ state, stale = false }: { state: string; stale?: boolean }) {
  const known = ["offline", "connecting", "synchronizing", "scanning", "degraded", "reconnecting", "stale", "failed", "unknown"];
  const display = stale ? "stale" : known.includes(state) ? state : "unknown";
  return <span role="status">{display.replaceAll("_", " ")}</span>;
}
