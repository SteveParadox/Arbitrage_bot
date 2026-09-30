use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq)]
pub struct SymbolRules {
    pub qty_step: Decimal,
    pub min_order_qty: Decimal,
    pub tick_size: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProposedOrderLeg {
    pub symbol: String,
    pub quantity: Decimal,
    pub limit_price: Option<Decimal>,
    pub rules: SymbolRules,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TradeIntent {
    pub trade_id: String,
    pub route_id: String,
    pub starting_asset: String,
    pub starting_notional: Decimal,
    pub projected_peak_exposure: Decimal,
    pub expected_net_edge_bps: Decimal,
    pub estimated_slippage_bps: Decimal,
    pub available_liquidity: Decimal,
    pub available_liquidity_ratio: Decimal,
    pub market_data_timestamp_ms: u64,
    pub legs: Vec<ProposedOrderLeg>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServiceHealth {
    pub healthy: bool,
    pub last_ok_ms: u64,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RiskContext {
    pub account_balance: Decimal,
    pub current_exposure: Decimal,
    pub daily_realized_pnl: Decimal,
    pub api_health: ServiceHealth,
    pub exchange_health: ServiceHealth,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RiskCheck {
    ManualKillSwitch,
    CircuitBreaker,
    MarketDataFreshness,
    MinimumNetEdge,
    MaximumSlippage,
    AvailableLiquidity,
    AccountBalance,
    MaximumTradeSize,
    SymbolPrecision,
    MaximumExposure,
    MaximumDailyLoss,
    ApiHealth,
    ExchangeHealth,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RiskCheckResult {
    pub check: RiskCheck,
    pub passed: bool,
    pub detail: String,
}

#[derive(Debug, Serialize)]
pub struct RiskDecision {
    pub trade_id: String,
    pub approved: bool,
    pub checks: Vec<RiskCheckResult>,
    #[serde(skip_serializing)]
    approval: Option<RiskApproval>,
}

impl RiskDecision {
    pub fn into_approval(self) -> Option<RiskApproval> {
        self.approval
    }
}

#[derive(Debug)]
pub struct RiskApproval {
    trade_id: String,
    approved_at_ms: u64,
    expires_at_ms: u64,
}

impl RiskApproval {
    pub fn trade_id(&self) -> &str {
        &self.trade_id
    }

    pub fn approved_at_ms(&self) -> u64 {
        self.approved_at_ms
    }

    pub fn expires_at_ms(&self) -> u64 {
        self.expires_at_ms
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BreakerKind {
    ExecutionFailures,
    StaleMarketData,
    DailyLossLimit,
    ApiHealth,
    ExchangeHealth,
    ClockSkew,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CircuitBreakerState {
    pub kind: BreakerKind,
    pub tripped_at_ms: u64,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RiskStatus {
    pub manual_kill_switch_active: bool,
    pub manual_kill_switch_detail: Option<String>,
    pub circuit_breaker: Option<CircuitBreakerState>,
    pub recent_execution_failures: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct PersistentRiskState {
    pub circuit_breaker: Option<CircuitBreakerState>,
    pub execution_failures_ms: Vec<u64>,
}
