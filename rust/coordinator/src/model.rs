use std::collections::BTreeMap;

use execution::{OrderSide, OrderType, TimeInForce};
use risk::SymbolRules;
use rust_decimal::Decimal;
use scanner::{TradeSide, TriangleLeg};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversionLeg {
    pub symbol: String,
    pub from_asset: String,
    pub to_asset: String,
    pub side: TradeSide,
    pub base_asset: String,
    pub quote_asset: String,
}

impl From<&TriangleLeg> for ConversionLeg {
    fn from(leg: &TriangleLeg) -> Self {
        Self {
            symbol: leg.symbol.clone(),
            from_asset: leg.from_asset.clone(),
            to_asset: leg.to_asset.clone(),
            side: leg.side,
            base_asset: leg.base_asset.clone(),
            quote_asset: leg.quote_asset.clone(),
        }
    }
}

impl ConversionLeg {
    pub fn reversed(&self) -> Self {
        Self {
            symbol: self.symbol.clone(),
            from_asset: self.to_asset.clone(),
            to_asset: self.from_asset.clone(),
            side: match self.side {
                TradeSide::Buy => TradeSide::Sell,
                TradeSide::Sell => TradeSide::Buy,
            },
            base_asset: self.base_asset.clone(),
            quote_asset: self.quote_asset.clone(),
        }
    }

    pub fn order_side(&self) -> OrderSide {
        match self.side {
            TradeSide::Buy => OrderSide::Buy,
            TradeSide::Sell => OrderSide::Sell,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlannedOrder {
    pub conversion: ConversionLeg,
    pub input_amount: Decimal,
    pub estimated_output: Decimal,
    pub estimated_notional_base: Decimal,
    pub estimated_slippage_bps: Decimal,
    pub liquidity_ratio: Decimal,
    pub market_timestamp_ms: u64,
    pub rules: SymbolRules,
    pub request: execution::ExecutionOrderRequest,
}

impl PlannedOrder {
    pub fn proposed_risk_leg(&self) -> risk::ProposedOrderLeg {
        risk::ProposedOrderLeg {
            symbol: self.request.symbol.clone(),
            quantity: self.request.requested_quantity,
            limit_price: self.request.price,
            rules: self.rules.clone(),
        }
    }

    pub fn is_market_ioc(&self) -> bool {
        self.request.order_type == OrderType::Market
            && self.request.time_in_force == TimeInForce::Ioc
    }
}

pub type Holdings = BTreeMap<String, Decimal>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorStatus {
    Completed,
    CompletedWithResidualCleanup,
    RecoveredByUnwind,
    NotStarted,
    HaltedUnresolved,
    UnwindFailed,
}

#[derive(Debug, Clone)]
pub struct LegExecutionReport {
    pub leg_index: usize,
    pub symbol: String,
    pub from_asset: String,
    pub to_asset: String,
    pub requested_quantity: Decimal,
    pub filled_quantity: Decimal,
    pub remaining_quantity: Decimal,
    pub actual_input_spent: Decimal,
    pub actual_output_received: Decimal,
    pub average_fill_price: Option<Decimal>,
    pub fees: BTreeMap<String, Decimal>,
    pub status: String,
    pub fully_filled: bool,
    pub fills_confirmed: bool,
    pub order_id: String,
}

#[derive(Debug, Clone)]
pub struct UnwindExecutionReport {
    pub asset: String,
    pub attempt: usize,
    pub symbol: String,
    pub requested_quantity: Decimal,
    pub filled_quantity: Decimal,
    pub output_base_received: Decimal,
    pub fees: BTreeMap<String, Decimal>,
    pub order_id: String,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct RouteExecutionReport {
    pub trade_id: String,
    pub route_id: String,
    pub base_asset: String,
    pub starting_amount: Decimal,
    pub final_base_amount: Decimal,
    pub realized_base_pnl: Decimal,
    pub residual_value_base: Option<Decimal>,
    pub economic_pnl: Option<Decimal>,
    pub status: CoordinatorStatus,
    pub holdings: Holdings,
    pub legs: Vec<LegExecutionReport>,
    pub unwind_orders: Vec<UnwindExecutionReport>,
    pub failure_reason: Option<String>,
}
