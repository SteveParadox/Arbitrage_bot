import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { validatePaperStatus } from "../src/paperContracts";
const status = { account_id: "demo", mode: "paper", simulated: true, state: "executing",
  authorized: true, blocked: false, runtime_online: true, active_cycles: 1, pending_orders: 1,
  balances: [{ asset: "USDT", available: "400", reserved: "100" }] };
describe("paper dashboard contract", () => {
  it("shows authoritative lifecycle and decimal balances", () => {
    expect(validatePaperStatus(status).state).toBe("executing");
    expect(validatePaperStatus(status).balances[0].reserved).toBe("100");
  });
  it("rejects live or exchange-confirmed records and invalid financial data", () => {
    for (const changed of [{ mode: "live" }, { simulated: false }, { active_cycles: -1 },
      { balances: [{ asset: "USDT", available: -100, reserved: "0" }] }, { state: "PROFITABLE" }]) {
      expect(() => validatePaperStatus({ ...status, ...changed })).toThrow();
    }
  });
});

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { PaperPanel } from "../src/PaperPanel";
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });
it("renders simulated lifecycle, fees and quarantined residual inventory", async () => {
  vi.stubGlobal("fetch", vi.fn(async (url: string) => {
    const value = url.endsWith("status") ? { ...status, state: "blocked", blocked: true } :
      url.endsWith("performance") ? { mode: "paper", simulated: true, closed_cycles: 0, net_realized_pnl: "0", win_rate_pct: null, average_win: null, average_loss: null, profit_factor: null, max_drawdown_start_asset: "0", average_duration_ms: null } :
      url.endsWith("valuation") ? {mode: "paper", simulated: true, estimated_equity: null, estimated_unrealized_pnl: null, start_asset: "USDT", valued_at_ms: 1} :
      url.endsWith("cycles") ? [{ id: "paper-cycle", state: "RECOVERY_REQUIRED", starting: "100", expected_profit: "0.4", realized_pnl: null, cash_delta: "-100", holdings: { BTC: "0.999" } }] :
      url.endsWith("orders") ? [{ id: "paper-leg1", cycle_id: "paper-cycle", state: "TERMINAL", intent: { symbol: "BTCUSDT", side: "Buy" }, result: { quantity: "1", quote_value: "100", price: "100", fee: "0.001", fee_currency: "BTC", remaining: "0", status: "Filled" } }] : [];
    return { ok: true, json: async () => value };
  }));
  render(<PaperPanel />);
  fireEvent.change(screen.getByLabelText("Paper control token"), { target: { value: "test-token" } });
  fireEvent.click(screen.getByText("Connect paper account"));
  await waitFor(() => expect(screen.getByText("RECOVERY_REQUIRED")).toBeTruthy());
  expect(screen.getByText("Unclosed inventory")).toBeTruthy();
  expect(screen.getByText("0.001 BTC")).toBeTruthy();
  expect(screen.getByText("0.999 BTC")).toBeTruthy();
  expect((screen.getByText("Start paper trading") as HTMLButtonElement).disabled).toBe(true);
});

it("validates the status emitted by the real-service autonomous replay", () => {
  const evidence = JSON.parse(readFileSync(resolve("../docs/evidence/paper-soak.json"), "utf8"));
  const emitted = evidence.runs[0].after_graceful_shutdown.status;
  expect(validatePaperStatus(emitted).state).toBe("stopped");
  expect(emitted.active_cycles).toBe(0);
  expect(emitted.pending_orders).toBe(0);
});

it("does not restore an account after disconnecting with a refresh in flight", async () => {
  let resolveStatus: (value: unknown) => void = () => {};
  const pending = new Promise(resolve => { resolveStatus = resolve; });
  vi.stubGlobal("fetch", vi.fn(async (url: string) => ({ ok: true, json: async () =>
    url.endsWith("status") ? pending : url.endsWith("performance") ?
      { mode: "paper", simulated: true, net_realized_pnl: "0" } : url.endsWith("valuation") ?
      { mode: "paper", simulated: true, estimated_equity: "500" } : [] })));
  render(<PaperPanel />);
  fireEvent.click(screen.getByText("Connect paper account"));
  await waitFor(() => expect(vi.mocked(fetch).mock.calls.length).toBe(6));
  fireEvent.click(screen.getByText("Disconnect"));
  resolveStatus({ ...status, authorized: false });
  await waitFor(() => expect(screen.queryByText("Start paper trading")).toBeNull());
  // Let every completion handler run before checking for stale controls.
  await new Promise(resolve => setTimeout(resolve, 20));
  expect(screen.queryByText("Start paper trading")).toBeNull();
});
