use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct MicroCanaryRun {
    #[serde(rename = "type")]
    pub event_type: &'static str,
    pub session_id: String,
    pub started_at_ms: u64,
    pub base_asset: String,
    pub cycle_notional: String,
    pub hard_cycle_cap: String,
    pub manual_execution_required: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct MicroCanaryCandidate {
    #[serde(rename = "type")]
    pub event_type: &'static str,
    pub session_id: String,
    pub trade_id: String,
    pub detected_at_ms: u64,
    pub route_id: String,
    pub triangle_id: String,
    pub base_asset: String,
    pub starting_capital: String,
    pub expected_pnl: String,
    pub expected_fees: String,
    pub expected_slippage: String,
    pub expected_slippage_bps: String,
    pub expected_net_edge_bps: String,
    pub fee_bps_per_leg: Vec<String>,
    pub detection_leg_prices: Vec<Option<f64>>,
    pub account_balance: String,
    pub account_equity_usd: String,
    pub account_exposure_usd: String,
    pub manual_execution_required: bool,
}
