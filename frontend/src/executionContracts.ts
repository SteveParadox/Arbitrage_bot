export type ExecutionStatus = {
  account_id: string; mode: "testnet" | "live"; environment: "testnet" | "live" | "offline_mock";
  state: string; authorized: boolean; blocked: boolean; recovering: boolean; emergency_stop: boolean;
  runtime_online: boolean; can_control: boolean; mock_evidence: boolean; exchange_confirmed: boolean;
  active_cycles: number; snapshot_ms: number | null; committed_usdt: string;
  balances: {wallet?: Record<string, string>; available?: Record<string, string>; locked?: Record<string, string>};
};
const decimal = /^-?\d+(\.\d+)?$/;
export function validateExecutionStatus(value: unknown): ExecutionStatus {
  if (!value || typeof value !== "object") throw new Error("Invalid execution status");
  const v = value as ExecutionStatus;
  if (!["testnet", "live"].includes(v.mode) || !["testnet", "live", "offline_mock"].includes(v.environment) ||
      v.environment !== (v.mock_evidence ? "offline_mock" : v.mode) || v.mock_evidence && v.mode !== "testnet" ||
      v.exchange_confirmed === v.mock_evidence || !["STOPPED", "STARTING", "SYNCHRONIZING", "RECOVERING", "READY", "RUNNING", "DEGRADED", "STOPPING", "BLOCKED", "FAILED"].includes(v.state)) {
    throw new Error("Execution mode or lifecycle mismatch");
  }
  for (const field of ["authorized", "blocked", "recovering", "emergency_stop", "runtime_online", "can_control", "mock_evidence", "exchange_confirmed"] as const) {
    if (typeof v[field] !== "boolean") throw new Error("Missing execution permission or health");
  }
  if (!Number.isSafeInteger(v.active_cycles) || v.active_cycles < 0 || typeof v.committed_usdt !== "string" || !decimal.test(v.committed_usdt) || v.committed_usdt.startsWith("-")) throw new Error("Invalid capital commitment");
  if (!v.balances || typeof v.balances !== "object") throw new Error("Missing balance evidence");
  for (const amounts of [v.balances.wallet, v.balances.available, v.balances.locked]) {
    if (amounts && Object.values(amounts).some(v => typeof v !== "string" || !decimal.test(v) || v.startsWith("-"))) throw new Error("Invalid decimal balance");
  }
  return v;
}
