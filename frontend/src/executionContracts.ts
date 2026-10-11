export type ExecutionStatus = {
  account_id: string; mode: "testnet" | "live"; environment: "testnet" | "live" | "offline_mock";
  state: string; authorized: boolean; blocked: boolean; recovering: boolean; emergency_stop: boolean;
  runtime_online: boolean; can_control: boolean; mock_evidence: boolean; exchange_confirmed: boolean;
  active_cycles: number; snapshot_ms: number | null; committed_usdt: string;
  market_ready: boolean;
  balances: {wallet?: Record<string, string>; available?: Record<string, string>; locked?: Record<string, string>};
};
const decimal = /^-?\d+(\.\d+)?$/;
export function validateExecutionStatus(value: unknown): ExecutionStatus {
  if (!value || typeof value !== "object") throw new Error("Invalid execution status");
  const v = value as ExecutionStatus;
  if (!["testnet", "live"].includes(v.mode) || !["testnet", "live", "offline_mock"].includes(v.environment) ||
      v.environment !== (v.mock_evidence ? "offline_mock" : v.mode) || v.mock_evidence && v.mode !== "testnet" ||
      v.exchange_confirmed && v.mock_evidence || !["STOPPED", "STARTING", "SYNCHRONIZING", "RECOVERING", "READY", "RUNNING", "DEGRADED", "STOPPING", "BLOCKED", "FAILED"].includes(v.state)) {
    throw new Error("Execution mode or lifecycle mismatch");
  }
  for (const field of ["authorized", "blocked", "recovering", "emergency_stop", "runtime_online", "can_control", "mock_evidence", "exchange_confirmed", "market_ready"] as const) {
    if (typeof v[field] !== "boolean") throw new Error("Missing execution permission or health");
  }
  if (!Number.isSafeInteger(v.active_cycles) || v.active_cycles < 0 || typeof v.committed_usdt !== "string" || !decimal.test(v.committed_usdt) || v.committed_usdt.startsWith("-")) throw new Error("Invalid capital commitment");
  if (!v.balances || typeof v.balances !== "object") throw new Error("Missing balance evidence");
  for (const amounts of [v.balances.wallet, v.balances.available, v.balances.locked]) {
    if (amounts && Object.values(amounts).some(v => typeof v !== "string" || !decimal.test(v) || v.startsWith("-"))) throw new Error("Invalid decimal balance");
  }
  return v;
}

export type ExecutionCycle = {id: string; state: string; starting: string; realized_pnl: string | null; holdings: Record<string,string>};
export type ExecutionOrder = {link_id: string; state: string; intent: {symbol: string; side: string}; evidence: null | {filled_quantity?: string; fees?: Record<string,string>}};
function amounts(value: unknown): boolean {
  return !!value && typeof value === "object" && !Array.isArray(value) && Object.values(value).every(v => typeof v === "string" && decimal.test(v));
}
export function validateExecutionCycles(value: unknown): ExecutionCycle[] {
  if (!Array.isArray(value) || value.some(v => !v || typeof v.id !== "string" || typeof v.state !== "string" || typeof v.starting !== "string" || !decimal.test(v.starting) || v.realized_pnl !== null && (typeof v.realized_pnl !== "string" || !decimal.test(v.realized_pnl)) || !amounts(v.holdings))) throw new Error("Invalid execution cycles");
  if (new Set(value.map(v => v.id)).size !== value.length) throw new Error("Duplicate execution cycle");
  return value;
}
export function validateExecutionOrders(value: unknown): ExecutionOrder[] {
  if (!Array.isArray(value) || value.some(v => !v || typeof v.link_id !== "string" || typeof v.state !== "string" || typeof v.intent?.symbol !== "string" || !["Buy", "Sell"].includes(v.intent?.side) || v.evidence !== null && (!v.evidence || typeof v.evidence !== "object" || v.evidence.filled_quantity !== undefined && (typeof v.evidence.filled_quantity !== "string" || !decimal.test(v.evidence.filled_quantity)) || v.evidence.fees !== undefined && !amounts(v.evidence.fees)))) throw new Error("Invalid execution orders");
  if (new Set(value.map(v => v.link_id)).size !== value.length) throw new Error("Duplicate execution order");
  return value;
}
