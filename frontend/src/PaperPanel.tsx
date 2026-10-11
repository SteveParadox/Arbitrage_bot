import { useCallback, useEffect, useRef, useState } from "react";
import { API_URL } from "./api";
import { validatePaperStatus, type PaperStatus } from "./paperContracts";

type Cycle = { id: string; state: string; starting: string; expected_profit: string;
  realized_pnl: string | null; cash_delta: string | null; holdings: Record<string, string> };
type Order = { id: string; cycle_id: string; state: string; intent: { symbol: string; side: string };
  result: null | { quantity: string; quote_value: string; price: string | null;
    fee: string; fee_currency: string; remaining: string; status: string } };
type Performance = { mode: "paper"; simulated: true; closed_cycles: number; net_realized_pnl: string;
  win_rate_pct: string | null; average_win: string | null; average_loss: string | null;
  profit_factor: string | null; max_drawdown_start_asset: string; average_duration_ms: number | null };
type Valuation = {mode: "paper"; simulated: true; estimated_equity: string | null; estimated_unrealized_pnl: string | null; start_asset: string; valued_at_ms: number | null};
type Event = { id: number; kind: string; payload: unknown };
export function PaperPanel() {
  const [account, setAccount] = useState("demo");
  const [token, setToken] = useState("");
  const [connected, setConnected] = useState(false);
  const [status, setStatus] = useState<PaperStatus | null>(null);
  const [cycles, setCycles] = useState<Cycle[]>([]);
  const [orders, setOrders] = useState<Order[]>([]);
  const [events, setEvents] = useState<Event[]>([]);
  const [performance, setPerformance] = useState<Performance | null>(null);
  const [valuation, setValuation] = useState<Valuation | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const refreshGeneration = useRef(0);
  const refresh = useCallback(async () => {
    if (!connected) return;
    const generation = ++refreshGeneration.current;
    try {
      const get = async (path: string) => {
        const response = await fetch(`${API_URL}/paper/${encodeURIComponent(account)}/${path}`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        if (!response.ok) throw new Error(`Paper API ${response.status}`);
        return response.json();
      };
      const [s, c, o, e, p, v] = await Promise.all([get("status"), get("cycles"), get("orders"), get("events"), get("performance"), get("valuation")]);
      if (generation !== refreshGeneration.current) return;
      const validatedStatus = validatePaperStatus(s);
      if (!Array.isArray(c) || !Array.isArray(o) || !Array.isArray(e)) throw new Error("Invalid paper activity response");
      if (p.mode !== "paper" || p.simulated !== true || typeof p.net_realized_pnl !== "string") throw new Error("Invalid paper performance response");
      if (v.mode !== "paper" || v.simulated !== true || (typeof v.estimated_equity !== "string" && v.estimated_equity !== null)) throw new Error("Invalid paper valuation response");
      setStatus(validatedStatus);
      setValuation(v);
      setPerformance(p); setCycles(c); setOrders(o); setEvents(e); setError("");
    } catch (e) { if (generation === refreshGeneration.current) { setError(e instanceof Error ? e.message : "Paper API unavailable"); setStatus(null); } }
  }, [account, token, connected]);
  useEffect(() => {
    const generation = refreshGeneration;
    void refresh(); const interval = window.setInterval(() => void refresh(), 2000);
    return () => { window.clearInterval(interval); ++generation.current; };
  }, [refresh]);
  async function command(action: "start" | "stop") {
    setBusy(true);
    try {
      const response = await fetch(`${API_URL}/paper/${encodeURIComponent(account)}/control/${action}`, {
        method: "POST", headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
        body: JSON.stringify({ request_id: crypto.randomUUID(), reason: `Dashboard paper ${action}` }),
      });
      if (!response.ok) { const body = await response.json(); throw new Error(String(body.detail)); }
      await refresh();
    } catch (e) { setError(e instanceof Error ? e.message : "Paper command failed"); }
    finally { setBusy(false); }
  }

  return <section className="analytics-section paper-panel" aria-label="Autonomous paper trading">
    <div className="section-heading"><div><span className="eyebrow">SIMULATED EXECUTION</span>
      <h2>Paper trading</h2><p>Virtual balances and simulated fills. Residual inventory is excluded from realized P&amp;L.</p></div>
      {status && <strong className={status.blocked ? "negative" : "positive"}>{status.state.toUpperCase()}</strong>}
    </div>
    <div className="paper-controls">
      <label>Account <input aria-label="Paper account" value={account} disabled={connected} onChange={e => setAccount(e.target.value)} /></label>
      <label>Control token <input aria-label="Paper control token" type="password" value={token} disabled={connected} onChange={e => setToken(e.target.value)} autoComplete="off" /></label>
      <button onClick={() => { setConnected(!connected); setStatus(null); }}>{connected ? "Disconnect" : "Connect paper account"}</button>
      {status && <><button disabled={busy || status.blocked || !status.runtime_online || status.authorized} onClick={() => void command("start")}>Start paper trading</button>
        <button disabled={busy || !status.authorized} onClick={() => void command("stop")}>Stop paper trading</button></>}
    </div>
    {error && <p role="alert">{error}</p>}
    {status && <>
      <div className="secondary-kpi-grid">
        <article className="kpi-card"><div className="kpi-card__label">Paper realized P&amp;L · {performance?.closed_cycles ?? 0} closed cycles</div><div className="kpi-card__value">{performance?.net_realized_pnl ?? "—"}</div></article>
        <article className="kpi-card"><div className="kpi-card__label">Active cycles / pending simulated orders</div><div className="kpi-card__value">{status.active_cycles} / {status.pending_orders}</div></article>
        <article className="kpi-card"><div className="kpi-card__label">Authorization / runtime</div><div className="kpi-card__value">{status.authorized ? "Enabled" : "Disabled"} · {status.runtime_online ? "Online" : "Offline"}</div></article>
      </div>
      {performance && <p>Win rate {performance.win_rate_pct ?? "—"}% · Average win {performance.average_win ?? "—"} · Average loss {performance.average_loss ?? "—"} · Profit factor {performance.profit_factor ?? "—"} · Maximum drawdown {performance.max_drawdown_start_asset} · Average duration {performance.average_duration_ms?.toFixed(0) ?? "—"} ms</p>}
      <h3>Virtual portfolio</h3>
      {valuation && <p>Estimated paper equity {valuation.estimated_equity ?? "Unavailable: stale or missing executable evidence"} {valuation.start_asset} · Estimated unrealized P&amp;L {valuation.estimated_unrealized_pnl ?? "Unavailable"}. Valuation assumes modeled liquidation fees; it is separate from realized P&amp;L.</p>}<div className="table-scroll"><table><thead><tr><th>Asset</th><th>Available</th><th>Committed reservation</th></tr></thead><tbody>
        {status.balances.map(b => <tr key={b.asset}><td>{b.asset}</td><td>{b.available}</td><td>{b.reserved}</td></tr>)}
      </tbody></table></div>
      {status?.clock === "virtual" && <p>Virtual-clock replay: financial timestamps follow the fixture clock.</p>}
      <h3>Paper cycles</h3><div className="table-scroll"><table><thead><tr><th>Cycle</th><th>State</th><th>Starting capital</th><th>Expected profit</th><th>Realized paper P&amp;L</th><th>Actual holdings</th></tr></thead><tbody>
        {cycles.map(c => <tr key={c.id}><td title={c.id}>{c.id.slice(0, 12)}</td><td>{c.state}</td><td>{c.starting}</td><td>{c.expected_profit}</td><td>{c.realized_pnl ?? "Unclosed inventory"}</td><td>{Object.entries(c.holdings).map(([a,v]) => `${v} ${a}`).join(" · ")}</td></tr>)}
      </tbody></table></div>
      <h3>Simulated orders and fills</h3><div className="table-scroll"><table><thead><tr><th>Order / leg</th><th>Symbol</th><th>Side</th><th>State</th><th>Executed quantity</th><th>Price</th><th>Fee</th><th>Remaining</th></tr></thead><tbody>
        {orders.map(o => <tr key={o.id}><td>{o.id}</td><td>{o.intent.symbol}</td><td>{o.intent.side}</td><td>{o.result?.status ?? o.state}</td><td>{o.result?.quantity ?? "—"}</td><td>{o.result?.price ?? "—"}</td><td>{o.result ? `${o.result.fee} ${o.result.fee_currency}` : "—"}</td><td>{o.result?.remaining ?? "—"}</td></tr>)}
      </tbody></table></div>
      <details><summary>Risk, reconciliation and recovery journal</summary>{events.map(e => <p key={e.id}><strong>{e.kind}</strong> <code>{JSON.stringify(e.payload)}</code></p>)}</details>
    </>}
  </section>;
}
