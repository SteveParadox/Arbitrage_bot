import { tradingState, type StopOutcome } from "./tradingState";

export function TradingState({ enabled, outcome, stale = false }: { enabled: boolean; outcome: StopOutcome; stale?: boolean }) {
  return <span role="status">{tradingState(enabled, outcome, stale)}</span>;
}
