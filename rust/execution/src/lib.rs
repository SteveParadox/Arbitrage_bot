mod auth;
mod client;
mod config;
mod model;

pub use client::BybitExecutionClient;
pub use config::ExecutionConfig;
pub use model::{
    BalanceEntry, BalanceSnapshot, CancelAck, ExecutionFill, ExecutionOrderRequest,
    ExecutionResult, MarketUnit, MonitorResult, OrderExecutionState, OrderSide, OrderType,
    PlaceOrderAck, TimeInForce,
};

use risk::{RiskApproval, RiskEngine, RiskError};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    Paper,
    Testnet,
    Live,
}

#[derive(Debug)]
pub struct PreparedExecution {
    trade_id: String,
    mode: ExecutionMode,
    risk_approved_at_ms: u64,
    approval: RiskApproval,
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

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("invalid execution configuration: {0}")]
    InvalidConfig(String),
    #[error("invalid order request: {0}")]
    InvalidOrder(String),
    #[error("execution environment mismatch: {0}")]
    EnvironmentMismatch(String),
    #[error("authentication signing failed: {0}")]
    Authentication(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("HTTP status {status}: {body}")]
    HttpStatus { status: u16, body: String },
    #[error("Bybit error {code}: {message}")]
    Bybit { code: i64, message: String },
    #[error("failed to decode Bybit response: {0}")]
    Decode(String),
    #[error("missing expected Bybit response data: {0}")]
    MissingData(String),
    #[error("numeric field {field} is invalid: {value}")]
    InvalidNumber { field: String, value: String },
    #[error("order {0} was not found")]
    OrderNotFound(String),
    #[error("risk state update failed after execution: {0}")]
    RiskState(String),
}

impl ExecutionError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::HttpStatus { status, .. } => *status == 429 || *status >= 500,
            Self::Bybit { code, .. } => matches!(
                *code,
                429 | 10000 | 10006 | 10016 | 170001 | 170005 | 170007 | 170032 | 500000
            ),
            _ => false,
        }
    }

    pub fn is_duplicate_request(&self) -> bool {
        matches!(self, Self::Bybit { code: 10014, .. })
    }

    pub fn counts_as_execution_failure(&self) -> bool {
        matches!(
            self,
            Self::Transport(_)
                | Self::HttpStatus { .. }
                | Self::Bybit { .. }
                | Self::Decode(_)
                | Self::MissingData(_)
                | Self::OrderNotFound(_)
        )
    }
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
        approval,
    })
}

pub fn live_execution_allowed(enabled: bool) -> bool {
    enabled
}
