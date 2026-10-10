use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use orderbook::{BookUpdate, ExecutionEstimate, OrderBookEngine};
use risk::SymbolRules;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use scanner::{TradeSide, TriangleRoute};
use tokio::sync::RwLock;

use crate::{ConversionLeg, CoordinatorConfig, CoordinatorError, PlannedOrder};

#[async_trait]
pub trait RoutePlanner: Send + Sync {
    async fn plan_conversion(
        &self,
        route: &TriangleRoute,
        conversion: &ConversionLeg,
        input_amount: Decimal,
        order_link_id: String,
        emergency: bool,
    ) -> Result<PlannedOrder, CoordinatorError>;

    async fn value_in_base(
        &self,
        route: &TriangleRoute,
        asset: &str,
        amount: Decimal,
    ) -> Result<Decimal, CoordinatorError>;
}

#[derive(Clone)]
pub struct LiveBookPlanner {
    books: Arc<RwLock<OrderBookEngine>>,
    rules: Arc<HashMap<String, SymbolRules>>,
    config: CoordinatorConfig,
}

impl LiveBookPlanner {
    pub fn new(
        rules: HashMap<String, SymbolRules>,
        config: CoordinatorConfig,
    ) -> Result<Self, CoordinatorError> {
        config.validate()?;
        Ok(Self {
            books: Arc::new(RwLock::new(OrderBookEngine::default())),
            rules: Arc::new(rules),
            config,
        })
    }

    pub fn books(&self) -> Arc<RwLock<OrderBookEngine>> {
        Arc::clone(&self.books)
    }

    pub async fn apply_book_update(&self, update: BookUpdate) -> Result<(), CoordinatorError> {
        self.books
            .write()
            .await
            .apply(update)
            .map_err(|error| CoordinatorError::Planning(error.to_string()))
    }

    fn rules_for(&self, symbol: &str) -> Result<SymbolRules, CoordinatorError> {
        self.rules.get(symbol).cloned().ok_or_else(|| {
            CoordinatorError::Planning(format!("missing execution rules for {symbol}"))
        })
    }
}

#[async_trait]
impl RoutePlanner for LiveBookPlanner {
    async fn plan_conversion(
        &self,
        route: &TriangleRoute,
        conversion: &ConversionLeg,
        input_amount: Decimal,
        order_link_id: String,
        emergency: bool,
    ) -> Result<PlannedOrder, CoordinatorError> {
        if input_amount <= Decimal::ZERO {
            return Err(CoordinatorError::Planning(
                "conversion input must be positive".to_string(),
            ));
        }

        let rules = self.rules_for(&conversion.symbol)?;
        let books = self.books.read().await;
        let input_f64 = to_f64("input_amount", input_amount)?;
        let rough = estimate_input_conversion(&books, conversion, input_f64)?;
        if !rough.complete && !emergency {
            return Err(CoordinatorError::Planning(format!(
                "insufficient visible liquidity on {}",
                conversion.symbol
            )));
        }

        let requested_base = match conversion.side {
            TradeSide::Buy => floor_to_step(
                decimal_from_f64("estimated_base", rough.filled_base_quantity)?,
                rules.qty_step,
            ),
            TradeSide::Sell if emergency && !rough.complete => floor_to_step(
                decimal_from_f64("visible_sell_base", rough.filled_base_quantity)?,
                rules.qty_step,
            ),
            TradeSide::Sell => floor_to_step(input_amount, rules.qty_step),
        };
        if requested_base < rules.min_order_qty {
            return Err(CoordinatorError::Planning(format!(
                "{} rounded quantity {} is below minimum {}",
                conversion.symbol, requested_base, rules.min_order_qty
            )));
        }

        let exact = match conversion.side {
            TradeSide::Buy => books.buy_base(
                &conversion.symbol,
                to_f64("requested_base", requested_base)?,
            ),
            TradeSide::Sell => books.sell_base(
                &conversion.symbol,
                to_f64("requested_base", requested_base)?,
            ),
        }
        .map_err(|error| CoordinatorError::Planning(error.to_string()))?;

        if !exact.complete {
            return Err(CoordinatorError::Planning(format!(
                "rounded order quantity is not fillable on {}",
                conversion.symbol
            )));
        }

        let quote_spend = decimal_from_f64("estimated_quote", exact.filled_quote_quantity)?;
        if conversion.side == TradeSide::Buy && quote_spend > input_amount {
            return Err(CoordinatorError::Planning(format!(
                "rounded BUY would spend {} {}, more than available {}",
                quote_spend, conversion.from_asset, input_amount
            )));
        }

        let estimated_output = match conversion.side {
            TradeSide::Buy => decimal_from_f64("estimated_output", exact.filled_base_quantity)?,
            TradeSide::Sell => quote_spend,
        };
        let planned_input_amount = match conversion.side {
            TradeSide::Buy => quote_spend,
            TradeSide::Sell => requested_base,
        };
        let estimated_notional_base =
            value_in_base_with_books(&books, route, &conversion.from_asset, planned_input_amount)?;
        let liquidity_ratio = if emergency && !rough.complete {
            (planned_input_amount / input_amount).min(Decimal::ONE)
        } else {
            Decimal::ONE
        };

        let slippage =
            decimal_from_f64("slippage_bps", exact.slippage_bps.unwrap_or(0.0).max(0.0))?;
        let tolerance = if emergency {
            self.config.emergency_slippage_tolerance_percent
        } else {
            self.config.normal_slippage_tolerance_percent
        };

        Ok(PlannedOrder {
            conversion: conversion.clone(),
            input_amount,
            estimated_output,
            estimated_notional_base,
            estimated_slippage_bps: slippage,
            liquidity_ratio,
            market_timestamp_ms: exact.timestamp,
            rules,
            request: execution::ExecutionOrderRequest {
                symbol: conversion.symbol.clone(),
                side: conversion.order_side(),
                order_type: execution::OrderType::Market,
                requested_quantity: requested_base,
                estimated_notional: estimated_notional_base,
                price: None,
                time_in_force: execution::TimeInForce::Ioc,
                order_link_id,
                slippage_tolerance_percent: Some(tolerance),
            },
        })
    }

    async fn value_in_base(
        &self,
        route: &TriangleRoute,
        asset: &str,
        amount: Decimal,
    ) -> Result<Decimal, CoordinatorError> {
        let books = self.books.read().await;
        value_in_base_with_books(&books, route, asset, amount)
    }
}

fn value_in_base_with_books(
    books: &OrderBookEngine,
    route: &TriangleRoute,
    asset: &str,
    amount: Decimal,
) -> Result<Decimal, CoordinatorError> {
    if amount == Decimal::ZERO {
        return Ok(Decimal::ZERO);
    }
    if amount < Decimal::ZERO {
        return Err(CoordinatorError::Planning(format!(
            "cannot mark negative holding {} {} without an external valuation",
            amount, asset
        )));
    }
    if asset == route.start_asset {
        return Ok(amount);
    }

    let conversion = unwind_conversion(route, asset)?;
    let estimate = estimate_input_conversion(books, &conversion, to_f64("mark_amount", amount)?)?;
    if !estimate.complete {
        return Err(CoordinatorError::Planning(format!(
            "cannot fully value {} {} back to {}",
            amount, asset, route.start_asset
        )));
    }
    let output = match conversion.side {
        TradeSide::Buy => estimate.filled_base_quantity,
        TradeSide::Sell => estimate.filled_quote_quantity,
    };
    decimal_from_f64("marked_base_value", output)
}

fn unwind_conversion(
    route: &TriangleRoute,
    asset: &str,
) -> Result<ConversionLeg, CoordinatorError> {
    if route.legs.len() != 3 {
        return Err(CoordinatorError::InvalidRoute(
            "route must contain three legs".to_string(),
        ));
    }
    let first = ConversionLeg::from(&route.legs[0]);
    let third = ConversionLeg::from(&route.legs[2]);

    if asset == first.to_asset {
        return Ok(first.reversed());
    }
    if asset == third.from_asset && third.to_asset == route.start_asset {
        return Ok(third);
    }
    Err(CoordinatorError::Planning(format!(
        "no direct emergency unwind path from {asset} to {} in route {}",
        route.start_asset, route.id
    )))
}

fn estimate_input_conversion(
    books: &OrderBookEngine,
    conversion: &ConversionLeg,
    input_f64: f64,
) -> Result<ExecutionEstimate, CoordinatorError> {
    let result = match conversion.side {
        TradeSide::Buy => books.buy_with_quote(&conversion.symbol, input_f64),
        TradeSide::Sell => books.sell_base(&conversion.symbol, input_f64),
    };
    result.map_err(|error| CoordinatorError::Planning(error.to_string()))
}

fn floor_to_step(value: Decimal, step: Decimal) -> Decimal {
    if value <= Decimal::ZERO || step <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    value - (value % step)
}

fn to_f64(field: &str, value: Decimal) -> Result<f64, CoordinatorError> {
    value
        .to_f64()
        .filter(|number| number.is_finite() && *number > 0.0)
        .ok_or_else(|| {
            CoordinatorError::Numeric(format!(
                "{field} cannot be represented as positive finite f64"
            ))
        })
}

fn decimal_from_f64(field: &str, value: f64) -> Result<Decimal, CoordinatorError> {
    if !value.is_finite() || value < 0.0 {
        return Err(CoordinatorError::Numeric(format!(
            "{field} is not finite/non-negative"
        )));
    }
    let text = format!("{value:.18}");
    Decimal::from_str_exact(&text)
        .map_err(|_| CoordinatorError::Numeric(format!("{field} could not convert from {value}")))
}

pub(crate) fn emergency_conversion_for_asset(
    route: &TriangleRoute,
    asset: &str,
) -> Result<ConversionLeg, CoordinatorError> {
    unwind_conversion(route, asset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use orderbook::PriceLevel;

    fn d(value: &str) -> Decimal {
        Decimal::from_str_exact(value).unwrap()
    }

    fn config() -> CoordinatorConfig {
        CoordinatorConfig {
            version: 1,
            normal_slippage_tolerance_percent: d("0.10"),
            emergency_slippage_tolerance_percent: d("0.50"),
            max_unwind_attempts_per_asset: 3,
            max_dust_notional_base: d("0.01"),
        }
    }

    #[tokio::test]
    async fn buy_plan_uses_actual_quote_budget_and_rounds_base_quantity_down() {
        let mut rules = HashMap::new();
        rules.insert(
            "BTCUSDT".to_string(),
            SymbolRules {
                qty_step: d("0.001"),
                min_order_qty: d("0.001"),
                tick_size: d("0.01"),
            },
        );
        let planner = LiveBookPlanner::new(rules, config()).unwrap();
        planner
            .apply_book_update(BookUpdate {
                symbol: "BTCUSDT".to_string(),
                bids: vec![PriceLevel {
                    price: 9.9,
                    quantity: 100.0,
                }],
                asks: vec![PriceLevel {
                    price: 10.0,
                    quantity: 100.0,
                }],
                timestamp: 1_000,
                update_id: 1,
                sequence: 1,
                is_snapshot: true,
            })
            .await
            .unwrap();

        let route = TriangleRoute {
            id: "USDT>BTC>ETH>USDT".to_string(),
            triangle_id: "BTC-ETH-USDT".to_string(),
            start_asset: "USDT".to_string(),
            assets: vec![
                "USDT".to_string(),
                "BTC".to_string(),
                "ETH".to_string(),
                "USDT".to_string(),
            ],
            pair1: "BTCUSDT".to_string(),
            pair2: "ETHBTC".to_string(),
            pair3: "ETHUSDT".to_string(),
            legs: vec![
                scanner::TriangleLeg {
                    symbol: "BTCUSDT".to_string(),
                    from_asset: "USDT".to_string(),
                    to_asset: "BTC".to_string(),
                    side: TradeSide::Buy,
                    base_asset: "BTC".to_string(),
                    quote_asset: "USDT".to_string(),
                },
                scanner::TriangleLeg {
                    symbol: "ETHBTC".to_string(),
                    from_asset: "BTC".to_string(),
                    to_asset: "ETH".to_string(),
                    side: TradeSide::Buy,
                    base_asset: "ETH".to_string(),
                    quote_asset: "BTC".to_string(),
                },
                scanner::TriangleLeg {
                    symbol: "ETHUSDT".to_string(),
                    from_asset: "ETH".to_string(),
                    to_asset: "USDT".to_string(),
                    side: TradeSide::Sell,
                    base_asset: "ETH".to_string(),
                    quote_asset: "USDT".to_string(),
                },
            ],
        };

        let plan = planner
            .plan_conversion(
                &route,
                &ConversionLeg::from(&route.legs[0]),
                d("4.567"),
                "plan-test".to_string(),
                false,
            )
            .await
            .unwrap();

        assert_eq!(plan.request.requested_quantity, d("0.456"));
        assert!(plan.estimated_output <= d("0.456000000000000100"));
        assert!(plan.request.estimated_notional <= d("4.567"));
    }
}
