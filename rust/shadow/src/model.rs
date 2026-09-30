use risk::RiskCheckResult;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ShadowOpportunity {
    pub run_id: String,
    pub observation_id: String,
    pub detected_at_ms: u64,
    pub route_id: String,
    pub triangle_id: String,
    pub start_asset: String,
    pub starting_capital: String,
    pub detection_final_amount: String,
    pub detection_gross_profit: String,
    pub expected_profit: String,
    pub latency_neutral_detection_profit: String,
    pub expected_net_edge_bps: String,
    pub detected: bool,
    pub approved: bool,
    pub would_execute: bool,
    pub approval_error: Option<String>,
    pub risk_checks: Vec<RiskCheckResult>,
    pub account_balance: Option<String>,
    pub account_equity_usd: Option<String>,
    pub account_exposure_usd: Option<String>,
    pub session_pnl_proxy_usd: Option<String>,
    pub detection_leg_prices: Vec<Option<f64>>,
    pub oldest_book_timestamp_ms: Option<u64>,
    pub newest_book_timestamp_ms: Option<u64>,
    pub book_timestamp_skew_ms: Option<u64>,
    pub latency_tracking: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShadowLatencySample {
    pub run_id: String,
    pub observation_id: String,
    pub route_id: String,
    pub latency_ms: u64,
    pub target_at_ms: u64,
    pub sampled_at_ms: u64,
    pub scheduler_lag_ms: u64,
    pub sample_valid: bool,
    pub failure_reason: Option<String>,
    pub final_amount: Option<String>,
    pub net_profit: Option<String>,
    pub net_edge_bps: Option<String>,
    pub profit_drift_from_detection: Option<String>,
    pub route_final_drift_bps: Option<String>,
    pub leg_price_drift_bps: Vec<Option<f64>>,
    pub profitable_after_latency: bool,
    pub still_meets_min_edge: bool,
    pub leg_average_prices: Vec<Option<f64>>,
    pub oldest_book_timestamp_ms: Option<u64>,
    pub newest_book_timestamp_ms: Option<u64>,
    pub book_timestamp_skew_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ShadowEvent {
    RunStarted {
        run_id: String,
        started_at_ms: u64,
        base_asset: String,
        latency_ms: Vec<u64>,
        minimum_observations: usize,
        no_order_endpoints: bool,
        mainnet_market_data: bool,
        mainnet_read_only_account: bool,
    },
    AccountSnapshot {
        run_id: String,
        synchronized_at_ms: u64,
        healthy: bool,
        detail: String,
        base_available: Option<String>,
        total_equity_usd: Option<String>,
        non_base_exposure_usd: Option<String>,
        session_pnl_proxy_usd: Option<String>,
    },
    Opportunity {
        #[serde(flatten)]
        observation: ShadowOpportunity,
    },
    LatencySample {
        #[serde(flatten)]
        sample: ShadowLatencySample,
    },
    Readiness {
        run_id: String,
        observed_count: usize,
        sampled_observation_count: usize,
        approved_count: usize,
        would_execute_count: usize,
        minimum_observations: usize,
        ready_for_analysis: bool,
    },
}
