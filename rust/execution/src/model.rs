use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::ExecutionError;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum OrderType {
    Market,
    Limit,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TimeInForce {
    #[serde(rename = "GTC")]
    Gtc,
    #[serde(rename = "IOC")]
    Ioc,
    #[serde(rename = "FOK")]
    Fok,
    #[serde(rename = "PostOnly")]
    PostOnly,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MarketUnit {
    #[serde(rename = "baseCoin")]
    BaseCoin,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionOrderRequest {
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub requested_quantity: Decimal,
    pub estimated_notional: Decimal,
    pub price: Option<Decimal>,
    pub time_in_force: TimeInForce,
    pub order_link_id: String,
    pub slippage_tolerance_percent: Option<Decimal>,
}

impl ExecutionOrderRequest {
    pub fn validate(&self, max_order_notional: Decimal) -> Result<(), ExecutionError> {
        if self.symbol.trim().is_empty() || self.symbol != self.symbol.to_uppercase() {
            return Err(ExecutionError::InvalidOrder(
                "symbol must be non-empty uppercase text".to_string(),
            ));
        }
        if self.requested_quantity <= Decimal::ZERO {
            return Err(ExecutionError::InvalidOrder(
                "requested quantity must be positive".to_string(),
            ));
        }
        if self.estimated_notional <= Decimal::ZERO || self.estimated_notional > max_order_notional
        {
            return Err(ExecutionError::InvalidOrder(format!(
                "estimated notional {} exceeds execution cap {} or is non-positive",
                self.estimated_notional, max_order_notional
            )));
        }
        if self.order_link_id.is_empty()
            || self.order_link_id.len() > 36
            || !self.order_link_id.is_ascii()
        {
            return Err(ExecutionError::InvalidOrder(
                "order_link_id must be unique ASCII text between 1 and 36 characters".to_string(),
            ));
        }

        match self.order_type {
            OrderType::Limit => {
                let price = self.price.ok_or_else(|| {
                    ExecutionError::InvalidOrder(
                        "limit orders require a positive price".to_string(),
                    )
                })?;
                if price <= Decimal::ZERO {
                    return Err(ExecutionError::InvalidOrder(
                        "limit order price must be positive".to_string(),
                    ));
                }
                if self.slippage_tolerance_percent.is_some() {
                    return Err(ExecutionError::InvalidOrder(
                        "slippage tolerance is only supported for market orders".to_string(),
                    ));
                }
            }
            OrderType::Market => {
                if self.price.is_some() {
                    return Err(ExecutionError::InvalidOrder(
                        "market orders must not specify a limit price".to_string(),
                    ));
                }
                if self.time_in_force != TimeInForce::Ioc {
                    return Err(ExecutionError::InvalidOrder(
                        "market orders must use IOC".to_string(),
                    ));
                }
                if let Some(tolerance) = self.slippage_tolerance_percent {
                    if tolerance < Decimal::new(1, 2)
                        || tolerance > Decimal::new(10, 0)
                        || tolerance.scale() > 2
                    {
                        return Err(ExecutionError::InvalidOrder(
                            "market slippage tolerance must be 0.01%-10% with at most 2 decimals"
                                .to_string(),
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaceOrderAck {
    pub order_id: String,
    pub order_link_id: String,
    pub accepted_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelAck {
    pub order_id: String,
    pub order_link_id: String,
    pub accepted_at_ms: u64,
    pub already_terminal: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionFill {
    pub execution_id: String,
    pub order_id: String,
    pub quantity: Decimal,
    pub price: Decimal,
    pub value: Decimal,
    pub fee: Decimal,
    pub fee_currency: String,
    pub is_maker: bool,
    pub executed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderExecutionState {
    pub order_id: String,
    pub order_link_id: String,
    pub symbol: String,
    pub status: String,
    pub requested_quantity: Decimal,
    pub filled_quantity: Decimal,
    pub remaining_quantity: Decimal,
    pub average_fill_price: Option<Decimal>,
    pub fees: BTreeMap<String, Decimal>,
    pub fills: Vec<ExecutionFill>,
    pub terminal: bool,
    pub fully_filled: bool,
    pub fills_confirmed: bool,
    pub reject_reason: Option<String>,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MonitorResult {
    pub state: OrderExecutionState,
    pub timed_out: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionStage {
    Submission,
    Monitoring,
    Cancellation,
    CancelConfirmation,
}

#[derive(Debug)]
pub struct ExecutionAttemptError {
    pub stage: ExecutionStage,
    pub place_ack: Option<PlaceOrderAck>,
    pub source: ExecutionError,
}

impl ExecutionAttemptError {
    pub fn safe_to_unwind_prior_exposure(&self) -> bool {
        if self.stage != ExecutionStage::Submission || self.place_ack.is_some() {
            return false;
        }

        match &self.source {
            ExecutionError::InvalidConfig(_)
            | ExecutionError::InvalidOrder(_)
            | ExecutionError::EnvironmentMismatch(_)
            | ExecutionError::Authentication(_)
            | ExecutionError::RiskState(_)
            | ExecutionError::EventPipeline(_) => true,
            ExecutionError::HttpStatus { status, .. } => *status < 500 && *status != 429,
            ExecutionError::Bybit { .. } => {
                !self.source.is_retryable() && !self.source.is_duplicate_request()
            }
            ExecutionError::Transport(_)
            | ExecutionError::Decode(_)
            | ExecutionError::MissingData(_)
            | ExecutionError::InvalidNumber { .. }
            | ExecutionError::OrderNotFound(_) => false,
        }
    }

    pub fn order_state_unknown(&self) -> bool {
        self.place_ack.is_some() || !self.safe_to_unwind_prior_exposure()
    }
}

impl std::fmt::Display for ExecutionAttemptError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "execution failed during {:?}: {}",
            self.stage, self.source
        )
    }
}

impl std::error::Error for ExecutionAttemptError {}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionResult {
    pub place_ack: PlaceOrderAck,
    pub monitor: MonitorResult,
    pub cancellation: Option<CancelAck>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BalanceEntry {
    pub coin: String,
    pub wallet_balance: Decimal,
    pub locked: Decimal,
    pub spot_borrow: Decimal,
    pub equity: Decimal,
    pub usd_value: Decimal,
    pub estimated_spot_available: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BalanceSnapshot {
    pub account_type: String,
    pub total_equity_usd: Decimal,
    pub total_wallet_balance_usd: Decimal,
    pub total_available_balance_usd: Decimal,
    pub coins: Vec<BalanceEntry>,
    pub synchronized_at_ms: u64,
}

pub(crate) fn parse_decimal(field: &str, value: &str) -> Result<Decimal, ExecutionError> {
    if value.is_empty() {
        return Ok(Decimal::ZERO);
    }
    Decimal::from_str_exact(value).map_err(|_| ExecutionError::InvalidNumber {
        field: field.to_string(),
        value: value.to_string(),
    })
}

pub(crate) fn terminal_status(status: &str) -> bool {
    matches!(
        status,
        "Rejected" | "PartiallyFilledCanceled" | "Filled" | "Cancelled" | "Deactivated"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(value: &str) -> Decimal {
        Decimal::from_str_exact(value).unwrap()
    }

    #[test]
    fn validates_small_market_order_with_base_quantity() {
        let request = ExecutionOrderRequest {
            symbol: "BTCUSDT".to_string(),
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            requested_quantity: d("0.0001"),
            estimated_notional: d("4.50"),
            price: None,
            time_in_force: TimeInForce::Ioc,
            order_link_id: "arb-test-1".to_string(),
            slippage_tolerance_percent: Some(d("0.10")),
        };

        request.validate(d("5")).unwrap();
    }

    #[test]
    fn rejects_order_above_execution_cap() {
        let request = ExecutionOrderRequest {
            symbol: "BTCUSDT".to_string(),
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            requested_quantity: d("0.01"),
            estimated_notional: d("500"),
            price: None,
            time_in_force: TimeInForce::Ioc,
            order_link_id: "arb-too-large".to_string(),
            slippage_tolerance_percent: None,
        };

        assert!(request.validate(d("5")).is_err());
    }

    #[test]
    fn ambiguous_submission_decode_error_is_not_safe_to_unwind() {
        let error = ExecutionAttemptError {
            stage: ExecutionStage::Submission,
            place_ack: None,
            source: ExecutionError::Decode("truncated response".to_string()),
        };
        assert!(!error.safe_to_unwind_prior_exposure());

        let rejected = ExecutionAttemptError {
            stage: ExecutionStage::Submission,
            place_ack: None,
            source: ExecutionError::Bybit {
                code: 10001,
                message: "bad request".to_string(),
            },
        };
        assert!(rejected.safe_to_unwind_prior_exposure());
    }

    #[test]
    fn recognizes_terminal_bybit_order_states() {
        assert!(terminal_status("Filled"));
        assert!(terminal_status("PartiallyFilledCanceled"));
        assert!(terminal_status("Rejected"));
        assert!(!terminal_status("PartiallyFilled"));
        assert!(!terminal_status("New"));
    }
}
