mod config;
pub mod control;
mod engine;
mod model;

pub use config::{load_risk_config, RiskConfig};
pub use engine::{current_time_ms, RiskEngine};
pub use model::{
    BreakerKind, CircuitBreakerState, EmergencyUnwindIntent, ProposedOrderLeg, RiskApproval,
    RiskApprovalKind, RiskCheck, RiskCheckResult, RiskContext, RiskDecision, RiskStatus,
    ServiceHealth, SymbolRules, TradeIntent,
};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum RiskError {
    #[error("risk I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("risk JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid decimal for {field}: {value}")]
    InvalidDecimal { field: String, value: String },
    #[error("invalid risk configuration: {0}")]
    InvalidConfig(String),
    #[error("invalid operator action: {0}")]
    InvalidOperatorAction(String),
    #[error("risk gate closed: {0}")]
    GateClosed(String),
}

pub fn within_notional_limit(order_notional: f64, max_notional: f64) -> bool {
    order_notional >= 0.0 && order_notional <= max_notional
}
