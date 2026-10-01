export type RiskStatus = {
  state: "ready" | "halted" | "circuit_breaker" | string;
  kill_switch_active: boolean;
  kill_switch_detail: string | null;
  circuit_breaker: {
    kind?: string;
    detail?: string;
    tripped_at_ms?: number;
  } | null;
  execution_failures_recorded: number;
};

export type DashboardSummary = {
  generated_at: string;
  account: {
    balance: number | null;
    equity_usd: number | null;
    exposure_usd: number | null;
    snapshot_at: string | null;
  };
  performance: {
    today_pnl: number;
    weekly_pnl: number;
    net_return_pct: number;
    detected_opportunities: number;
    executed_trades: number;
    rejected_opportunities: number;
    success_rate_pct: number;
    average_net_edge_bps: number | null;
    average_latency_ms: number | null;
  };
  system: {
    api_status: string;
    websocket_status: string;
    websocket_status_source: string;
    last_market_event: string | null;
    trading_enabled: boolean;
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
  status: "accepted" | "executable" | "rejected" | string;
  reason_rejected: string | null;
};

export type Execution = {
  trade_id: string;
  time: string;
  triangle: string;
  route_id: string;
  starting_capital: number | null;
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

const API_URL = (
  import.meta.env.VITE_API_URL ?? "http://localhost:8000"
).replace(/\/$/, "");

async function request<T>(path: string): Promise<T> {
  const response = await fetch(`${API_URL}${path}`, {
    headers: { Accept: "application/json" },
  });
  if (!response.ok) {
    throw new Error(`API ${response.status}: ${response.statusText}`);
  }
  return response.json() as Promise<T>;
}

export async function fetchDashboard(): Promise<DashboardSummary> {
  return request<DashboardSummary>("/operations/dashboard");
}

export async function fetchOpportunities(): Promise<Opportunity[]> {
  return request<Opportunity[]>("/operations/opportunities?limit=50");
}

export async function fetchExecutions(): Promise<Execution[]> {
  return request<Execution[]>("/operations/executions?limit=20");
}

export { API_URL };
