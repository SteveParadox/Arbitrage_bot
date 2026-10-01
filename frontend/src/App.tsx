import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  API_URL,
  type DashboardSummary,
  type DistributionSummary,
  type Execution,
  type Opportunity,
  type PerformanceAnalytics,
  fetchDashboard,
  fetchExecutions,
  fetchOpportunities,
  fetchPerformanceAnalytics,
} from "./api";

type Tone = "positive" | "negative" | "neutral" | "warning";

function amount(
  value: number | null,
  asset: string | null = "USDT",
  digits = 2,
): string {
  if (value === null || Number.isNaN(value)) return "—";
  const formatted = new Intl.NumberFormat("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  }).format(value);
  return asset ? `${formatted} ${asset}` : formatted;
}

function percent(value: number | null, digits = 2): string {
  if (value === null || Number.isNaN(value)) return "—";
  return `${value >= 0 ? "+" : ""}${value.toFixed(digits)}%`;
}

function rate(value: number | null, digits = 2): string {
  if (value === null || Number.isNaN(value)) return "—";
  return `${value.toFixed(digits)}%`;
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


function distributionValue(
  value: number | null,
  unit: string,
): string {
  if (value === null || Number.isNaN(value)) return "—";
  if (unit === "ms") return latency(value);
  if (unit === "bps") return bps(value);
  return value.toFixed(2);
}

function DistributionCard({
  title,
  distribution,
}: {
  title: string;
  distribution: DistributionSummary;
}) {
  const maxCount = Math.max(
    1,
    ...distribution.bins.map((item) => item.count),
  );

  return (
    <article className="analytics-card">
      <div className="analytics-card__header">
        <div>
          <div className="eyebrow">DISTRIBUTION</div>
          <h3>{title}</h3>
        </div>
        <span>{distribution.count.toLocaleString()} samples</span>
      </div>
      <div className="distribution-stats">
        <div>
          <span>Median</span>
          <strong>
            {distributionValue(
              distribution.median,
              distribution.unit,
            )}
          </strong>
        </div>
        <div>
          <span>P95</span>
          <strong>
            {distributionValue(distribution.p95, distribution.unit)}
          </strong>
        </div>
        <div>
          <span>Mean</span>
          <strong>
            {distributionValue(distribution.mean, distribution.unit)}
          </strong>
        </div>
      </div>
      <div className="histogram" aria-label={`${title} histogram`}>
        {distribution.bins.length ? (
          distribution.bins.map((item, index) => (
            <div className="histogram__row" key={index}>
              <span>
                {distributionValue(item.lower, distribution.unit)}
              </span>
              <div className="histogram__track">
                <div
                  className="histogram__bar"
                  style={{
                    width: `${(item.count / maxCount) * 100}%`,
                  }}
                />
              </div>
              <strong>{item.count}</strong>
            </div>
          ))
        ) : (
          <div className="analytics-empty">No samples yet</div>
        )}
      </div>
    </article>
  );
}

function FunnelStage({
  label,
  count,
  value,
  detail,
}: {
  label: string;
  count: number;
  value?: string;
  detail?: string;
}) {
  return (
    <div className="funnel-stage">
      <span>{label}</span>
      <strong>{count.toLocaleString()}</strong>
      {value ? <b>{value}</b> : null}
      {detail ? <small>{detail}</small> : null}
    </div>
  );
}

export function App() {
  const [summary, setSummary] = useState<DashboardSummary | null>(null);
  const [opportunities, setOpportunities] = useState<Opportunity[]>([]);
  const [executions, setExecutions] = useState<Execution[]>([]);
  const [selectedTrade, setSelectedTrade] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [analytics, setAnalytics] =
    useState<PerformanceAnalytics | null>(null);
  const [analyticsError, setAnalyticsError] =
    useState<string | null>(null);
  const requestInFlight = useRef(false);
  const analyticsInFlight = useRef(false);

  const loadAnalytics = useCallback(async () => {
    if (analyticsInFlight.current) return;
    analyticsInFlight.current = true;
    try {
      const value = await fetchPerformanceAnalytics(7, "USDT");
      setAnalytics(value);
      setAnalyticsError(null);
    } catch (reason) {
      setAnalyticsError(
        reason instanceof Error
          ? reason.message
          : "Unable to load performance analytics",
      );
    } finally {
      analyticsInFlight.current = false;
    }
  }, []);

  const load = useCallback(async (silent = false) => {
    if (requestInFlight.current) return;
    requestInFlight.current = true;
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
      requestInFlight.current = false;
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

  useEffect(() => {
    void loadAnalytics();
    const id = window.setInterval(() => {
      void loadAnalytics();
    }, 30000);
    return () => window.clearInterval(id);
  }, [loadAnalytics]);

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

  if (!summary) {
    return (
      <main className="shell shell--centered">
        <div className="fatal-state">
          <div className="eyebrow">OPERATIONS API UNAVAILABLE</div>
          <h1>No telemetry loaded</h1>
          <p>
            {error ?? "The dashboard could not load operational data."}
          </p>
          <code>{API_URL}</code>
          <button
            className="refresh-button"
            onClick={() => void load()}
            type="button"
          >
            Retry
          </button>
        </div>
      </main>
    );
  }

  const performance = summary.performance;
  const risk = summary.system.risk;
  const apiState =
    error
      ? "bad"
      : summary.system.api_status === "ok"
        ? "ok"
        : summary.system.api_status === "degraded"
          ? "warn"
          : "bad";
  const wsState =
    summary.system.websocket_status === "connected"
      ? "ok"
      : summary.system.websocket_status === "stale"
        ? "warn"
        : summary.system.websocket_status === "unknown"
          ? "muted"
          : "bad";
  const riskState =
    risk?.state === "ready"
      ? "ok"
      : risk?.state === "unavailable" ||
          risk?.state === "no_persisted_state"
        ? "muted"
        : risk?.state
          ? "bad"
          : "muted";

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
          value={error ? "offline" : summary.system.api_status}
          state={apiState}
          detail={`${API_URL} · DB ${summary.system.database_status}`}
        />
        <StatusPill
          label="Market stream"
          value={summary.system.websocket_status ?? "Unknown"}
          state={wsState}
          detail={
            summary.system.last_market_event
              ? `Last event ${formatTime(summary.system.last_market_event)}`
              : "No recent opportunity activity"
          }
        />
        <StatusPill
          label="Trading"
          value={summary.system.trading_enabled ? "Enabled" : "Stopped"}
          state={summary.system.trading_enabled ? "warn" : "ok"}
          detail={
            !summary.system.trading_deployment_enabled
              ? "Deployment master gate is disabled"
              : !summary.system.trading_runtime_enabled
                ? "Runtime control gate is stopped"
                : !summary.system.trading_risk_allows_new_orders
                  ? "Risk gate is blocking new orders"
                  : "Deployment, runtime, and risk gates are open"
          }
        />
        <StatusPill
          label="Risk state"
          value={risk?.state?.replaceAll("_", " ") ?? "Unknown"}
          state={riskState}
          detail={
            risk?.state === "unavailable"
              ? "Risk runtime is not visible to the API"
              : risk?.state === "no_persisted_state"
                ? "No persisted breaker state exists yet"
                : risk?.circuit_breaker?.kind
                  ? `Breaker: ${risk.circuit_breaker.kind}`
                  : "No active circuit breaker"
          }
        />
        <StatusPill
          label="Kill switch"
          value={
            risk?.available === false
              ? "Unavailable"
              : risk?.kill_switch_active
                ? "ENGAGED"
                : "Clear"
          }
          state={
            risk?.available === false
              ? "muted"
              : risk?.kill_switch_active
                ? "bad"
                : "ok"
          }
          detail={
            risk?.available === false
              ? "API cannot see the Rust risk runtime directory"
              : risk?.kill_switch_detail ?? "Operator stop is clear"
          }
        />
      </section>

      <section className="kpi-grid" aria-label="Key metrics">
        <KpiCard
          label="Account balance"
          value={amount(
            summary.account.balance ?? null,
            summary.account.base_asset ?? "USDT",
          )}
          detail={
            summary.account.snapshot_at
              ? `Snapshot ${formatTime(summary.account.snapshot_at)}`
              : "Awaiting canary account snapshot"
          }
        />
        <KpiCard
          label="Today's P&L"
          value={amount(
            performance.today_pnl ?? 0,
            summary.account.base_asset ?? "USDT",
          )}
          tone={toneForNumber(performance.today_pnl ?? 0)}
          detail="Reconciled micro-live cycles · UTC day"
        />
        <KpiCard
          label="Weekly P&L"
          value={amount(
            performance.weekly_pnl ?? 0,
            summary.account.base_asset ?? "USDT",
          )}
          tone={toneForNumber(performance.weekly_pnl ?? 0)}
          detail="Last 7 days"
        />
        <KpiCard
          label="Net return"
          value={percent(performance.net_return_pct ?? null)}
          tone={
            performance.net_return_pct == null
              ? "neutral"
              : toneForNumber(performance.net_return_pct)
          }
          detail="P&L ÷ reconciled capital"
        />
        <KpiCard
          label="Detected opportunities"
          value={(performance.detected_opportunities ?? 0).toLocaleString()}
          detail="Last 24 hours"
        />
        <KpiCard
          label="Executed trades"
          value={(performance.executed_trades ?? 0).toLocaleString()}
          detail="Reconciled · UTC day"
        />
        <KpiCard
          label="Rejected opportunities"
          value={(performance.rejected_opportunities ?? 0).toLocaleString()}
          detail="Last 24 hours"
          tone="warning"
        />
        <KpiCard
          label="Success rate"
          value={rate(performance.success_rate_pct)}
          detail="Profitable reconciled trades"
          tone={
            performance.success_rate_pct != null &&
            performance.success_rate_pct >= 50
              ? "positive"
              : "neutral"
          }
        />
        <KpiCard
          label="Average net edge"
          value={bps(performance.average_net_edge_bps ?? null)}
          detail="Accepted opportunities"
        />
        <KpiCard
          label="Average latency"
          value={latency(performance.average_latency_ms ?? null)}
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
                      <td>{amount(item.capital, item.capital_asset, 2)}</td>
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
                  <strong>
                    {amount(
                      selectedExecution.expected_pnl,
                      selectedExecution.base_asset,
                      4,
                    )}
                  </strong>
                </div>
                <div>
                  <span>Actual</span>
                  <strong
                    className={
                      selectedExecution.realized_pnl == null
                        ? undefined
                        : selectedExecution.realized_pnl >= 0
                          ? "text-positive"
                          : "text-negative"
                    }
                  >
                    {amount(
                      selectedExecution.realized_pnl,
                      selectedExecution.base_asset,
                      4,
                    )}
                  </strong>
                </div>
              </div>

              <div className="execution-facts">
                <div>
                  <span>Prediction error</span>
                  <strong>
                    {amount(
                      selectedExecution.prediction_error,
                      selectedExecution.base_asset,
                      4,
                    )}
                  </strong>
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
                  <strong>
                    {amount(
                      selectedExecution.starting_capital,
                      selectedExecution.base_asset,
                    )}
                  </strong>
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


      <section className="analytics-section">
        <div className="section-heading">
          <div>
            <div className="eyebrow">PHASE 17 · PERFORMANCE ANALYTICS</div>
            <h2>Where the edge becomes money</h2>
            <p>
              Seven-day analytics for observed opportunities, execution,
              and realized outcomes.
            </p>
          </div>
          {analytics ? (
            <span className="panel__count">
              {analytics.window.base_asset} · {analytics.window.days} days
            </span>
          ) : null}
        </div>

        {analyticsError ? (
          <div className="alert alert--error">
            <strong>Performance analytics unavailable.</strong>
            <span>{analyticsError}</span>
          </div>
        ) : null}

        {analytics ? (
          <>
            <div className="analytics-kpis">
              <KpiCard
                label="Profit per cycle"
                value={amount(
                  analytics.profit.profit_per_cycle,
                  analytics.window.base_asset,
                  4,
                )}
                detail={`${analytics.profit.cycles_with_known_pnl} cycles with known P&L`}
                tone={toneForNumber(
                  analytics.profit.profit_per_cycle ?? 0,
                )}
              />
              <KpiCard
                label="Average profit per day"
                value={amount(
                  analytics.profit.average_profit_per_day,
                  analytics.window.base_asset,
                  4,
                )}
                detail="Across the selected 7-day window"
                tone={toneForNumber(
                  analytics.profit.average_profit_per_day,
                )}
              />
              <KpiCard
                label="Profit per 1,000 turnover"
                value={amount(
                  analytics.profit.profit_per_1000_turnover,
                  analytics.window.base_asset,
                  4,
                )}
                detail="Estimated three-leg turnover basis"
                tone={toneForNumber(
                  analytics.profit.profit_per_1000_turnover ?? 0,
                )}
              />
              <KpiCard
                label="Total realized P&L"
                value={amount(
                  analytics.profit.total_profit,
                  analytics.window.base_asset,
                  4,
                )}
                detail={
                  `Engine ${analytics.profit.by_source.engine.cycles} · ` +
                  `Canary ${analytics.profit.by_source.micro_canary.cycles}`
                }
                tone={toneForNumber(analytics.profit.total_profit)}
              />
            </div>

            <article className="panel funnel-panel">
              <div className="panel__header">
                <div>
                  <div className="eyebrow">MONEY FUNNEL</div>
                  <h2>Observed → expected → attempted → actual</h2>
                </div>
              </div>
              <div className="funnel-grid">
                <FunnelStage
                  label="Observed opportunity"
                  count={analytics.funnel.observed_opportunities}
                  detail="Distinct Phase 7 windows"
                />
                <FunnelStage
                  label="Expected profit"
                  count={
                    analytics.funnel.expected_profitable_opportunities
                  }
                  value={amount(
                    analytics.funnel.expected_profit_total,
                    analytics.window.base_asset,
                    4,
                  )}
                  detail="Sum of max expected profit per window"
                />
                <FunnelStage
                  label="Trade attempted"
                  count={analytics.funnel.trade_attempted}
                  detail="Distinct terminal/canary trade ids"
                />
                <FunnelStage
                  label="Actual profit"
                  count={analytics.funnel.actual_profit_known}
                  value={amount(
                    analytics.funnel.actual_profit_total,
                    analytics.window.base_asset,
                    4,
                  )}
                  detail="Cycles with known realized P&L"
                />
              </div>
              <div className="funnel-diagnostics">
                <span>
                  Attempt → known outcome{" "}
                  <strong>
                    {rate(analytics.funnel.attempt_to_actual_known_pct)}
                  </strong>
                </span>
                <span>
                  Aggregate expected → actual profit{" "}
                  <strong>
                    {rate(analytics.funnel.aggregate_profit_capture_pct)}
                  </strong>
                </span>
              </div>
            </article>

            <div className="analytics-grid">
              <DistributionCard
                title="Net edge"
                distribution={analytics.distributions.net_edge_bps}
              />
              <DistributionCard
                title="Opportunity survival"
                distribution={
                  analytics.distributions.opportunity_survival_ms
                }
              />
              <DistributionCard
                title="Execution latency"
                distribution={analytics.distributions.latency_ms}
              />
              <DistributionCard
                title="Actual slippage"
                distribution={analytics.distributions.slippage_bps}
              />
            </div>

            <div className="analytics-bottom-grid">
              <article className="analytics-card">
                <div className="analytics-card__header">
                  <div>
                    <div className="eyebrow">OUTCOME DISTRIBUTION</div>
                    <h3>Wins and losses</h3>
                  </div>
                </div>
                <div className="outcome-grid">
                  <div className="outcome outcome--win">
                    <span>Wins</span>
                    <strong>
                      {analytics.distributions.win_loss.wins}
                    </strong>
                  </div>
                  <div className="outcome outcome--loss">
                    <span>Losses</span>
                    <strong>
                      {analytics.distributions.win_loss.losses}
                    </strong>
                  </div>
                  <div className="outcome">
                    <span>Breakeven</span>
                    <strong>
                      {analytics.distributions.win_loss.breakeven}
                    </strong>
                  </div>
                  <div className="outcome">
                    <span>Win rate</span>
                    <strong>
                      {rate(
                        analytics.distributions.win_loss.win_rate_pct,
                      )}
                    </strong>
                  </div>
                </div>
              </article>

              <article className="analytics-card">
                <div className="analytics-card__header">
                  <div>
                    <div className="eyebrow">PROFIT PER DAY</div>
                    <h3>Daily realized P&L</h3>
                  </div>
                </div>
                <div className="daily-profit">
                  {analytics.profit.daily.map((item) => {
                    const maxAbsolute = Math.max(
                      1e-9,
                      ...analytics.profit.daily.map((day) =>
                        Math.abs(day.profit),
                      ),
                    );
                    return (
                      <div className="daily-profit__row" key={item.date}>
                        <span>{item.date.slice(5)}</span>
                        <div className="daily-profit__track">
                          <div
                            className={
                              item.profit >= 0
                                ? "daily-profit__bar daily-profit__bar--positive"
                                : "daily-profit__bar daily-profit__bar--negative"
                            }
                            style={{
                              width: `${(Math.abs(item.profit) / maxAbsolute) * 100}%`,
                            }}
                          />
                        </div>
                        <strong>
                          {amount(
                            item.profit,
                            analytics.window.base_asset,
                            3,
                          )}
                        </strong>
                      </div>
                    );
                  })}
                </div>
              </article>
            </div>

            {analytics.data_quality.profit_metrics_sampled ? (
              <div className="analytics-warning">
                Profit and execution metrics are sampled at the configured row
                limit. Engine events loaded{" "}
                {analytics.data_quality.engine_events_loaded.toLocaleString()} of{" "}
                {analytics.data_quality.engine_events_total.toLocaleString()};
                canary cycles loaded{" "}
                {analytics.data_quality.micro_canary_cycles.toLocaleString()} of{" "}
                {analytics.data_quality.micro_canary_cycles_total.toLocaleString()}.
              </div>
            ) : null}

            {analytics.data_quality.orphan_order_attempts_without_terminal_trade >
            0 ? (
              <div className="analytics-warning">
                {analytics.data_quality
                  .orphan_order_attempts_without_terminal_trade
                  .toLocaleString()}{" "}
                order-level trade ids have neither an explicit route-attempt
                event nor a terminal route event in the selected event sample.
                They remain a data-quality warning rather than being assigned
                to the {analytics.window.base_asset} funnel.
              </div>
            ) : null}
          </>
        ) : analyticsError ? null : (
          <div className="analytics-loading">
            Loading performance distributions…
          </div>
        )}
      </section>

      <footer>
        <span>
          Market-stream status is inferred from recent opportunity activity.
        </span>
        <span>
          Trading controls terminate at FastAPI; Rust reads the shared runtime gate.
        </span>
      </footer>
    </main>
  );
}
