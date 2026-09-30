use risk::{RiskApproval, RiskEngine, RiskError};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    Paper,
    Live,
}

#[derive(Debug, PartialEq, Eq)]
pub struct PreparedExecution {
    trade_id: String,
    mode: ExecutionMode,
    risk_approved_at_ms: u64,
}

impl PreparedExecution {
    pub fn trade_id(&self) -> &str {
        &self.trade_id
    }

    pub fn mode(&self) -> ExecutionMode {
        self.mode
    }

    pub fn risk_approved_at_ms(&self) -> u64 {
        self.risk_approved_at_ms
    }
}

#[derive(Debug, Error)]
pub enum ExecutionPreparationError {
    #[error(transparent)]
    Risk(#[from] RiskError),
    #[error("live execution is disabled")]
    LiveExecutionDisabled,
}

pub fn prepare_execution(
    risk_engine: &mut RiskEngine,
    trade_id: &str,
    approval: RiskApproval,
    now_ms: u64,
    mode: ExecutionMode,
    live_enabled: bool,
) -> Result<PreparedExecution, ExecutionPreparationError> {
    risk_engine.validate_approval(&approval, trade_id, now_ms)?;

    if mode == ExecutionMode::Live && !live_enabled {
        return Err(ExecutionPreparationError::LiveExecutionDisabled);
    }

    Ok(PreparedExecution {
        trade_id: trade_id.to_string(),
        mode,
        risk_approved_at_ms: approval.approved_at_ms(),
    })
}

pub fn live_execution_allowed(enabled: bool) -> bool {
    enabled
}
