export type RiskStatus = {
  available: boolean;
  state:
    | "ready"
    | "halted"
    | "circuit_breaker"
    | "unavailable"
    | "no_persisted_state"
    | string;
  kill_switch_active: boolean;
  kill_switch_detail: string | null;
  circuit_breaker: {
    kind?: string;
    detail?: string;
    tripped_at_ms?: number;
  } | null;
  execution_failures_recorded: number;
};

export type Performance = {
  generated_at: string;
  today_pnl: number;
  weekly_pnl: number;
  net_return_pct: number | null;
  detected_opportunities: number;
  executed_trades: number;
  rejected_opportunities: number;
  success_rate_pct: number | null;
  average_net_edge_bps: number | null;
  average_latency_ms: number | null;
};

export type BalanceSnapshot = {
  generated_at: string;
  base_asset: string | null;
  balance: number | null;
  equity_usd: number | null;
  exposure_usd: number | null;
  snapshot_at: string | null;
};

export type HealthResponse = {
  status: "ok" | "degraded" | string;
  generated_at: string;
  environment: string;
  database_status: "online" | "offline" | string;
  market_stream_status: string;
  last_market_event: string | null;
  risk: RiskStatus;
  trading: {
    deployment_enabled: boolean;
    runtime_enabled: boolean;
    risk_allows_new_orders: boolean;
    effective_enabled: boolean;
    updated_at: string | null;
    reason: string | null;
    source: string;
  };
  control_auth_configured: boolean;
};

export type DashboardSummary = {
  generated_at: string;
  account: Omit<BalanceSnapshot, "generated_at">;
  performance: Omit<Performance, "generated_at">;
  system: {
    api_status: string;
    database_status: string;
    websocket_status: string;
    websocket_status_source: string;
    last_market_event: string | null;
    trading_enabled: boolean;
    trading_deployment_enabled: boolean;
    trading_runtime_enabled: boolean;
    trading_risk_allows_new_orders: boolean;
    trading_control_reason: string | null;
    control_auth_configured: boolean;
    risk: RiskStatus;
  };
};

export type Opportunity = {
  id: number;
  time: string;
  triangle: string;
  route_id: string;
  gross_edge_pct: number | null;
  net_edge_pct: number | null;
  net_edge_bps: number | null;
  capital: number | null;
  capital_asset: string;
  status: "accepted" | "rejected" | string;
  reason_rejected: string | null;
};

export type Execution = {
  trade_id: string;
  time: string;
  triangle: string;
  route_id: string;
  starting_capital: number | null;
  base_asset: string;
  expected_pnl: number | null;
  realized_pnl: number | null;
  prediction_error: number | null;
  expected_net_edge_bps: number | null;
  actual_slippage_bps: number | null;
  execution_time_ms: number | null;
  execution_status: string;
  reconciled: boolean;
  detection_leg_prices: Array<number | null>;
};


export type DistributionBin = {
  lower: number;
  upper: number;
  count: number;
};

export type DistributionSummary = {
  count: number;
  sample_count: number;
  unit: string;
  min: number | null;
  p25: number | null;
  median: number | null;
  p75: number | null;
  p95: number | null;
  max: number | null;
  mean: number | null;
  bins: DistributionBin[];
};

export type PerformanceAnalytics = {
  generated_at: string;
  window: {
    days: number;
    start: string;
    end: string;
    base_asset: string;
  };
  profit: {
    total_profit: number;
    cycles_with_known_pnl: number;
    profit_per_cycle: number | null;
    average_profit_per_day: number;
    estimated_turnover: number;
    profit_per_1000_turnover: number | null;
    turnover_basis: string;
    daily: Array<{
      date: string;
      profit: number;
      cycles: number;
    }>;
    by_source: {
      engine: { cycles: number; profit: number };
      micro_canary: { cycles: number; profit: number };
    };
  };
  funnel: {
    observed_opportunities: number;
    expected_profitable_opportunities: number;
    expected_profit_total: number;
    expected_profit_basis: string;
    trade_attempted: number;
    trade_attempted_engine: number;
    trade_attempted_micro_canary: number;
    actual_profit_known: number;
    actual_profit_total: number;
    attempt_to_actual_known_pct: number | null;
    aggregate_profit_capture_pct: number | null;
    population_note: string;
  };
  distributions: {
    net_edge_bps: DistributionSummary & { basis: string };
    win_loss: {
      wins: number;
      losses: number;
      breakeven: number;
      unknown_pnl: number;
      win_rate_pct: number | null;
    };
    opportunity_survival_ms: DistributionSummary & {
      open_windows: number;
      basis: string;
    };
    latency_ms: DistributionSummary & { basis: string };
    slippage_bps: DistributionSummary & { basis: string };
  };
  data_quality: {
    sample_limit: number;
    opportunity_windows_sampled: boolean;
    net_edge_sampled: boolean;
    slippage_sampled: boolean;
    engine_terminal_events: number;
    engine_trade_attempt_events: number;
    engine_events_loaded: number;
    engine_events_total: number;
    engine_events_sampled: boolean;
    micro_canary_cycles: number;
    micro_canary_cycles_total: number;
    micro_canary_cycles_sampled: boolean;
    profit_metrics_sampled: boolean;
    deduplicated_canary_trade_ids: number;
    orphan_order_attempts_without_terminal_trade: number;
    note: string;
  };
};

export type TradingControlResponse = {
  status: "started" | "stopped";
  effective_enabled: boolean;
  control: {
    version: number;
    enabled: boolean;
    updated_at: string;
    reason: string;
    source: string;
  };
};

const configuredApiUrl = import.meta.env.VITE_API_URL?.trim();
const defaultApiUrl = import.meta.env.DEV
  ? "http://localhost:8000"
  : "/api";
const API_URL = (configuredApiUrl || defaultApiUrl).replace(/\/$/, "");

async function request<T>(
  path: string,
  init: RequestInit = {},
): Promise<T> {
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), 8000);
  const headers = new Headers(init.headers);
  headers.set("Accept", "application/json");

  try {
    const response = await fetch(`${API_URL}${path}`, {
      ...init,
      headers,
      signal: controller.signal,
    });
    if (!response.ok) {
      const detail = await response
        .json()
        .then((body: { detail?: string }) => body.detail)
        .catch(() => undefined);
      throw new Error(
        detail
          ? `API ${response.status}: ${detail}`
          : `API ${response.status}: ${response.statusText}`,
      );
    }
    return (await response.json()) as T;
  } catch (reason) {
    if (reason instanceof DOMException && reason.name === "AbortError") {
      throw new Error("Dashboard API request timed out");
    }
    throw reason;
  } finally {
    window.clearTimeout(timeoutId);
  }
}

export async function fetchDashboard(): Promise<DashboardSummary> {
  const [performance, balances, health] = await Promise.all([
    request<Performance>("/performance"),
    request<BalanceSnapshot>("/balances"),
    request<HealthResponse>("/health"),
  ]);

  return {
    generated_at: health.generated_at,
    account: {
      base_asset: balances.base_asset,
      balance: balances.balance,
      equity_usd: balances.equity_usd,
      exposure_usd: balances.exposure_usd,
      snapshot_at: balances.snapshot_at,
    },
    performance: {
      today_pnl: performance.today_pnl,
      weekly_pnl: performance.weekly_pnl,
      net_return_pct: performance.net_return_pct,
      detected_opportunities: performance.detected_opportunities,
      executed_trades: performance.executed_trades,
      rejected_opportunities: performance.rejected_opportunities,
      success_rate_pct: performance.success_rate_pct,
      average_net_edge_bps: performance.average_net_edge_bps,
      average_latency_ms: performance.average_latency_ms,
    },
    system: {
      api_status: health.status,
      database_status: health.database_status,
      websocket_status: health.market_stream_status,
      websocket_status_source: "Phase 15 FastAPI health",
      last_market_event: health.last_market_event,
      trading_enabled: health.trading.effective_enabled,
      trading_deployment_enabled: health.trading.deployment_enabled,
      trading_runtime_enabled: health.trading.runtime_enabled,
      trading_risk_allows_new_orders:
        health.trading.risk_allows_new_orders,
      trading_control_reason: health.trading.reason,
      control_auth_configured: health.control_auth_configured,
      risk: health.risk,
    },
  };
}

export async function fetchOpportunities(): Promise<Opportunity[]> {
  return request<Opportunity[]>("/opportunities?limit=50");
}

export async function fetchExecutions(): Promise<Execution[]> {
  return request<Execution[]>("/trades?limit=20");
}

export async function fetchPerformanceAnalytics(
  days = 7,
  baseAsset = "USDT",
): Promise<PerformanceAnalytics> {
  const params = new URLSearchParams({
    days: String(days),
    base_asset: baseAsset,
    bins: "10",
  });
  return request<PerformanceAnalytics>(
    `/analytics/performance?${params.toString()}`,
  );
}

async function tradingCommand(
  action: "start" | "stop",
  token: string,
  reason: string,
): Promise<TradingControlResponse> {
  const headers = new Headers({
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  });
  return request<TradingControlResponse>(`/trading/${action}`, {
    method: "POST",
    headers,
    body: JSON.stringify({ reason }),
  });
}

export function startTrading(
  token: string,
  reason = "operator started trading",
): Promise<TradingControlResponse> {
  return tradingCommand("start", token, reason);
}

export function stopTrading(
  token: string,
  reason = "operator stopped trading",
): Promise<TradingControlResponse> {
  return tradingCommand("stop", token, reason);
}

export { API_URL };
