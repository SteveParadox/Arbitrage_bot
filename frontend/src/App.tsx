import { useCallback, useEffect, useMemo, useState } from "react";
import {
  API_URL,
  type DashboardSummary,
  type Execution,
  type Opportunity,
  fetchDashboard,
  fetchExecutions,
  fetchOpportunities,
} from "./api";

type Tone = "positive" | "negative" | "neutral" | "warning";

function money(value: number | null, digits = 2): string {
  if (value === null || Number.isNaN(value)) return "—";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  }).format(value);
}

function percent(value: number | null, digits = 2): string {
  if (value === null || Number.isNaN(value)) return "—";
  return `${value >= 0 ? "+" : ""}${value.toFixed(digits)}%`;
}

function bps(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "—";
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)} bps`;
}

function latency(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "—";
  return `${value.toFixed(0)} ms`;
}

function formatTime(value: string | null): string {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatDateTime(value: string | null): string {
  if (!value) return "No data yet";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "No data yet";
  return date.toLocaleString();
}

function toneForNumber(value: number): Tone {
  if (value > 0) return "positive";
  if (value < 0) return "negative";
  return "neutral";
}

function KpiCard({
  label,
  value,
  detail,
  tone = "neutral",
}: {
  label: string;
  value: string;
  detail?: string;
  tone?: Tone;
}) {
  return (
    <article className={`kpi-card kpi-card--${tone}`}>
      <div className="kpi-card__label">{label}</div>
      <div className="kpi-card__value">{value}</div>
      {detail ? <div className="kpi-card__detail">{detail}</div> : null}
    </article>
  );
}

function StatusPill({
  label,
  value,
  state,
  detail,
}: {
  label: string;
  value: string;
  state: "ok" | "warn" | "bad" | "muted";
  detail?: string;
}) {
  return (
    <div className="status-item">
      <div className="status-item__heading">
        <span className={`status-dot status-dot--${state}`} />
        <span>{label}</span>
      </div>
      <strong>{value}</strong>
      {detail ? <small>{detail}</small> : null}
    </div>
  );
}

function RouteFlow({ execution }: { execution: Execution }) {
  const assets = execution.route_id.split(">").filter(Boolean);
  const prices = execution.detection_leg_prices;
  return (
    <div className="route-flow">
      {assets.map((asset, index) => (
        <div className="route-flow__segment" key={`${asset}-${index}`}>
          <div className="route-flow__asset">{asset}</div>
          {index < assets.length - 1 ? (
            <div className="route-flow__connector">
              <span>↓</span>
              <small>
                Leg {index + 1}
                {prices[index] != null
                  ? ` · @ ${Number(prices[index]).toPrecision(7)}`
                  : ""}
              </small>
            </div>
          ) : null}
        </div>
      ))}
    </div>
  );
}

function statusClass(status: string): string {
  if (status === "accepted" || status === "completed") return "status-positive";
  if (status === "rejected" || status === "failed") return "status-negative";
  return "status-warning";
}

export function App() {
  const [summary, setSummary] = useState<DashboardSummary | null>(null);
  const [opportunities, setOpportunities] = useState<Opportunity[]>([]);
  const [executions, setExecutions] = useState<Execution[]>([]);
  const [selectedTrade, setSelectedTrade] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async (silent = false) => {
    if (silent) setRefreshing(true);
    else setLoading(true);
    try {
      const [dashboard, recentOpportunities, recentExecutions] =
        await Promise.all([
          fetchDashboard(),
          fetchOpportunities(),
          fetchExecutions(),
        ]);
      setSummary(dashboard);
      setOpportunities(recentOpportunities);
      setExecutions(recentExecutions);
      setSelectedTrade((current) => {
        if (
          current &&
          recentExecutions.some((item) => item.trade_id === current)
        ) {
          return current;
        }
        return recentExecutions[0]?.trade_id ?? null;
      });
      setError(null);
    } catch (reason) {
      setError(
        reason instanceof Error
          ? reason.message
          : "Unable to load operations data",
      );
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    void load();
    const id = window.setInterval(() => {
      void load(true);
    }, 5000);
    return () => window.clearInterval(id);
  }, [load]);

  const selectedExecution = useMemo(
    () =>
      executions.find((item) => item.trade_id === selectedTrade) ??
      executions[0] ??
      null,
    [executions, selectedTrade],
  );

  if (loading && !summary) {
    return (
      <main className="shell shell--centered">
        <div className="loader" />
        <p>Loading operations telemetry…</p>
      </main>
    );
  }

  const performance = summary?.performance;
  const risk = summary?.system.risk;
  const apiOnline = !error && summary?.system.api_status === "online";
  const wsState =
    summary?.system.websocket_status === "connected"
      ? "ok"
      : summary?.system.websocket_status === "stale"
        ? "warn"
        : "bad";
  const riskState =
    risk?.state === "ready" ? "ok" : risk?.state ? "bad" : "muted";

  return (
    <main className="shell">
      <header className="topbar">
        <div>
          <div className="eyebrow">TRIANGULAR ARBITRAGE · OPERATIONS</div>
          <h1>Execution Control Center</h1>
          <p>
            Live research, canary calibration, and risk telemetry in one place.
          </p>
        </div>
        <div className="topbar__actions">
          <div className="updated">
            <span>Last refresh</span>
            <strong>
              {summary ? formatTime(summary.generated_at) : "Unavailable"}
            </strong>
          </div>
          <button
            className="refresh-button"
            disabled={refreshing}
            onClick={() => void load(true)}
            type="button"
          >
            {refreshing ? "Refreshing…" : "Refresh"}
          </button>
        </div>
      </header>

      {error ? (
        <div className="alert alert--error">
          <strong>Dashboard API unavailable.</strong>
          <span>{error}</span>
          <code>{API_URL}</code>
        </div>
      ) : null}

      <section className="status-strip" aria-label="System status">
        <StatusPill
          label="API"
          value={apiOnline ? "Online" : "Offline"}
          state={apiOnline ? "ok" : "bad"}
          detail={API_URL}
        />
        <StatusPill
          label="Market stream"
          value={summary?.system.websocket_status ?? "Unknown"}
          state={wsState}
          detail={
            summary?.system.last_market_event
              ? `Last event ${formatTime(summary.system.last_market_event)}`
              : "No recent opportunity activity"
          }
        />
        <StatusPill
          label="Trading"
          value={
            summary?.system.trading_enabled ? "Enabled" : "Disabled"
          }
          state={summary?.system.trading_enabled ? "warn" : "ok"}
          detail="Global live-trading flag"
        />
        <StatusPill
          label="Risk state"
          value={risk?.state?.replaceAll("_", " ") ?? "Unknown"}
          state={riskState}
          detail={
            risk?.circuit_breaker?.kind
              ? `Breaker: ${risk.circuit_breaker.kind}`
              : "No active circuit breaker"
          }
        />
        <StatusPill
          label="Kill switch"
          value={risk?.kill_switch_active ? "ENGAGED" : "Clear"}
          state={risk?.kill_switch_active ? "bad" : "ok"}
          detail={risk?.kill_switch_detail ?? "Operator stop is clear"}
        />
      </section>

      <section className="kpi-grid" aria-label="Key metrics">
        <KpiCard
          label="Account balance"
          value={money(summary?.account.balance ?? null)}
          detail={
            summary?.account.snapshot_at
              ? `Snapshot ${formatTime(summary.account.snapshot_at)}`
              : "Awaiting canary account snapshot"
          }
        />
        <KpiCard
          label="Today's P&L"
          value={money(performance?.today_pnl ?? 0)}
          tone={toneForNumber(performance?.today_pnl ?? 0)}
          detail="Reconciled micro-live cycles"
        />
        <KpiCard
          label="Weekly P&L"
          value={money(performance?.weekly_pnl ?? 0)}
          tone={toneForNumber(performance?.weekly_pnl ?? 0)}
          detail="Last 7 days"
        />
        <KpiCard
          label="Net return"
          value={percent(performance?.net_return_pct ?? 0)}
          tone={toneForNumber(performance?.net_return_pct ?? 0)}
          detail="P&L ÷ reconciled capital"
        />
        <KpiCard
          label="Detected opportunities"
          value={(performance?.detected_opportunities ?? 0).toLocaleString()}
          detail="Last 24 hours"
        />
        <KpiCard
          label="Executed trades"
          value={(performance?.executed_trades ?? 0).toLocaleString()}
          detail="Reconciled today"
        />
        <KpiCard
          label="Rejected opportunities"
          value={(performance?.rejected_opportunities ?? 0).toLocaleString()}
          detail="Last 24 hours"
          tone="warning"
        />
        <KpiCard
          label="Success rate"
          value={percent(performance?.success_rate_pct ?? 0)}
          detail="Profitable reconciled trades"
          tone={
            (performance?.success_rate_pct ?? 0) >= 50
              ? "positive"
              : "neutral"
          }
        />
        <KpiCard
          label="Average net edge"
          value={bps(performance?.average_net_edge_bps ?? null)}
          detail="Accepted opportunities"
        />
        <KpiCard
          label="Average latency"
          value={latency(performance?.average_latency_ms ?? null)}
          detail="Actual execution time today"
        />
      </section>

      <section className="content-grid">
        <article className="panel panel--opportunities">
          <div className="panel__header">
            <div>
              <div className="eyebrow">MARKET SCANNER</div>
              <h2>Recent opportunities</h2>
            </div>
            <span className="panel__count">{opportunities.length} rows</span>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Triangle</th>
                  <th>Gross edge</th>
                  <th>Net edge</th>
                  <th>Capital</th>
                  <th>Status</th>
                  <th>Reason rejected</th>
                </tr>
              </thead>
              <tbody>
                {opportunities.length ? (
                  opportunities.map((item) => (
                    <tr key={item.id}>
                      <td className="mono">{formatTime(item.time)}</td>
                      <td>
                        <strong>{item.triangle}</strong>
                        <small>{item.route_id}</small>
                      </td>
                      <td>{percent(item.gross_edge_pct, 3)}</td>
                      <td>{percent(item.net_edge_pct, 3)}</td>
                      <td>{money(item.capital, 2)}</td>
                      <td>
                        <span
                          className={`table-status ${statusClass(item.status)}`}
                        >
                          {item.status}
                        </span>
                      </td>
                      <td className="reason">
                        {item.reason_rejected?.replaceAll("_", " ") ?? "—"}
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td className="empty-cell" colSpan={7}>
                      No opportunity observations yet.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </article>

        <aside className="panel execution-panel">
          <div className="panel__header">
            <div>
              <div className="eyebrow">CANARY EXECUTION</div>
              <h2>Execution view</h2>
            </div>
            {selectedExecution ? (
              <span
                className={`table-status ${statusClass(
                  selectedExecution.execution_status,
                )}`}
              >
                {selectedExecution.execution_status}
              </span>
            ) : null}
          </div>

          {selectedExecution ? (
            <>
              <label className="execution-select">
                <span>Trade</span>
                <select
                  value={selectedExecution.trade_id}
                  onChange={(event) => setSelectedTrade(event.target.value)}
                >
                  {executions.map((item) => (
                    <option value={item.trade_id} key={item.trade_id}>
                      {formatTime(item.time)} · {item.triangle}
                    </option>
                  ))}
                </select>
              </label>

              <RouteFlow execution={selectedExecution} />

              <div className="execution-edge">
                <span>Expected net edge</span>
                <strong>{bps(selectedExecution.expected_net_edge_bps)}</strong>
              </div>

              <div className="pnl-comparison">
                <div>
                  <span>Expected</span>
                  <strong>{money(selectedExecution.expected_pnl, 4)}</strong>
                </div>
                <div>
                  <span>Actual</span>
                  <strong
                    className={
                      (selectedExecution.realized_pnl ?? 0) >= 0
                        ? "text-positive"
                        : "text-negative"
                    }
                  >
                    {money(selectedExecution.realized_pnl, 4)}
                  </strong>
                </div>
              </div>

              <div className="execution-facts">
                <div>
                  <span>Prediction error</span>
                  <strong>{money(selectedExecution.prediction_error, 4)}</strong>
                </div>
                <div>
                  <span>Execution time</span>
                  <strong>{latency(selectedExecution.execution_time_ms)}</strong>
                </div>
                <div>
                  <span>Actual slippage</span>
                  <strong>{bps(selectedExecution.actual_slippage_bps)}</strong>
                </div>
                <div>
                  <span>Capital</span>
                  <strong>{money(selectedExecution.starting_capital)}</strong>
                </div>
              </div>
            </>
          ) : (
            <div className="empty-state">
              <strong>No canary execution data</strong>
              <p>
                Phase 13 candidates and reconciled results will appear here.
              </p>
            </div>
          )}
        </aside>
      </section>

      <footer>
        <span>
          Market-stream status is inferred from recent opportunity activity.
        </span>
        <span>
          Risk and kill-switch state are read from the persisted risk files.
        </span>
      </footer>
    </main>
  );
}
