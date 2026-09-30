use std::{
    collections::{BTreeMap, VecDeque},
    sync::Mutex,
};

use async_trait::async_trait;
use coordinator::{
    CoordinatorConfig, CoordinatorError, CoordinatorStatus, CoordinatorVenue,
    LegAuthorizationContext, PlannedOrder, RiskEngineAuthorizer, RiskIntentProvider,
    RoutePlanner, ThreeLegCoordinator,
};
use execution::{
    ExecutionAttemptError, ExecutionError, ExecutionFill, ExecutionMode,
    ExecutionOrderRequest, ExecutionResult, ExecutionStage, MonitorResult,
    OrderExecutionState, OrderSide, OrderType, PlaceOrderAck, PreparedExecution,
    TimeInForce,
};
use risk::{
    current_time_ms, RiskConfig, RiskContext, RiskEngine, ServiceHealth,
    SymbolRules, TradeIntent,
};
use rust_decimal::Decimal;
use scanner::{TradeSide, TriangleLeg, TriangleRoute};

fn d(value: &str) -> Decimal {
    Decimal::from_str_exact(value).unwrap()
}

fn route() -> TriangleRoute {
    TriangleRoute {
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
            TriangleLeg {
                symbol: "BTCUSDT".to_string(),
                from_asset: "USDT".to_string(),
                to_asset: "BTC".to_string(),
                side: TradeSide::Buy,
                base_asset: "BTC".to_string(),
                quote_asset: "USDT".to_string(),
            },
            TriangleLeg {
                symbol: "ETHBTC".to_string(),
                from_asset: "BTC".to_string(),
                to_asset: "ETH".to_string(),
                side: TradeSide::Buy,
                base_asset: "ETH".to_string(),
                quote_asset: "BTC".to_string(),
            },
            TriangleLeg {
                symbol: "ETHUSDT".to_string(),
                from_asset: "ETH".to_string(),
                to_asset: "USDT".to_string(),
                side: TradeSide::Sell,
                base_asset: "ETH".to_string(),
                quote_asset: "USDT".to_string(),
            },
        ],
    }
}

fn coordinator_config() -> CoordinatorConfig {
    CoordinatorConfig {
        version: 1,
        normal_slippage_tolerance_percent: d("0.10"),
        emergency_slippage_tolerance_percent: d("0.50"),
        max_unwind_attempts_per_asset: 3,
        max_dust_notional_base: d("0.001"),
    }
}

fn risk_config(name: &str) -> RiskConfig {
    let base = std::env::temp_dir().join(format!(
        "arb-coordinator-test-{}-{}",
        name,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    RiskConfig {
        version: 1,
        max_market_data_age_ms: 500,
        min_net_edge_bps: d("5"),
        max_slippage_bps: d("10"),
        min_liquidity_ratio: d("1"),
        max_trade_size: d("500"),
        max_total_exposure: d("1000"),
        max_daily_loss: d("50"),
        execution_failure_limit: 3,
        execution_failure_window_ms: 300_000,
        api_health_max_age_ms: 5_000,
        exchange_health_max_age_ms: 5_000,
        approval_ttl_ms: 100,
        emergency_max_market_data_age_ms: 2_000,
        kill_switch_file: base.join("KILL_SWITCH"),
        state_file: base.join("risk_state.json"),
    }
}

#[derive(Clone)]
struct MockPlanner;

#[async_trait]
impl RoutePlanner for MockPlanner {
    async fn plan_conversion(
        &self,
        route: &TriangleRoute,
        conversion: &coordinator::ConversionLeg,
        input_amount: Decimal,
        order_link_id: String,
        emergency: bool,
    ) -> Result<PlannedOrder, CoordinatorError> {
        let requested_quantity = match (
            conversion.symbol.as_str(),
            conversion.side,
        ) {
            ("BTCUSDT", TradeSide::Buy) => input_amount / d("10"),
            ("BTCUSDT", TradeSide::Sell) => input_amount,
            ("ETHBTC", TradeSide::Buy) => input_amount * d("2"),
            ("ETHBTC", TradeSide::Sell) => input_amount,
            ("ETHUSDT", TradeSide::Sell) => input_amount,
            ("ETHUSDT", TradeSide::Buy) => input_amount / d("6"),
            _ => {
                return Err(CoordinatorError::Planning(
                    "unsupported mock conversion".to_string(),
                ))
            }
        };

        let estimated_output = match (
            conversion.symbol.as_str(),
            conversion.side,
        ) {
            ("BTCUSDT", TradeSide::Buy) => requested_quantity,
            ("BTCUSDT", TradeSide::Sell) => requested_quantity * d("9"),
            ("ETHBTC", TradeSide::Buy) => requested_quantity,
            ("ETHBTC", TradeSide::Sell) => requested_quantity * d("0.49"),
            ("ETHUSDT", TradeSide::Sell) => requested_quantity * d("6"),
            ("ETHUSDT", TradeSide::Buy) => requested_quantity,
            _ => Decimal::ZERO,
        };

        let base_value = self
            .value_in_base(route, &conversion.from_asset, input_amount)
            .await?;

        Ok(PlannedOrder {
            conversion: conversion.clone(),
            input_amount,
            estimated_output,
            estimated_notional_base: base_value,
            estimated_slippage_bps: d("1"),
            liquidity_ratio: Decimal::ONE,
            market_timestamp_ms: current_time_ms(),
            rules: SymbolRules {
                qty_step: d("0.0001"),
                min_order_qty: d("0.0001"),
                tick_size: d("0.01"),
            },
            request: ExecutionOrderRequest {
                symbol: conversion.symbol.clone(),
                side: conversion.order_side(),
                order_type: OrderType::Market,
                requested_quantity,
                estimated_notional: base_value,
                price: None,
                time_in_force: TimeInForce::Ioc,
                order_link_id,
                slippage_tolerance_percent: Some(if emergency {
                    d("0.50")
                } else {
                    d("0.10")
                }),
            },
        })
    }

    async fn value_in_base(
        &self,
        route: &TriangleRoute,
        asset: &str,
        amount: Decimal,
    ) -> Result<Decimal, CoordinatorError> {
        if asset == route.start_asset {
            return Ok(amount);
        }
        match asset {
            "BTC" => Ok(amount * d("9")),
            "ETH" => Ok(amount * d("6")),
            other => Err(CoordinatorError::Planning(format!(
                "cannot value mock asset {other}"
            ))),
        }
    }
}

struct MockProvider;

#[async_trait]
impl RiskIntentProvider for MockProvider {
    async fn risk_inputs(
        &mut self,
        context: &LegAuthorizationContext<'_>,
    ) -> Result<(TradeIntent, RiskContext), CoordinatorError> {
        let now = context.now_ms;
        let notional = context.planned.estimated_notional_base;
        Ok((
            TradeIntent {
                trade_id: context.trade_id.to_string(),
                route_id: context.route.id.clone(),
                starting_asset: context.planned.conversion.from_asset.clone(),
                starting_notional: notional,
                projected_peak_exposure: notional,
                expected_net_edge_bps: d("20"),
                estimated_slippage_bps: context.planned.estimated_slippage_bps,
                available_liquidity: notional,
                available_liquidity_ratio: Decimal::ONE,
                market_data_timestamp_ms: context.planned.market_timestamp_ms,
                legs: vec![context.planned.proposed_risk_leg()],
            },
            RiskContext {
                account_balance: d("1000"),
                current_exposure: Decimal::ZERO,
                daily_realized_pnl: Decimal::ZERO,
                api_health: ServiceHealth {
                    healthy: true,
                    last_ok_ms: now,
                    detail: "ok".to_string(),
                },
                exchange_health: ServiceHealth {
                    healthy: true,
                    last_ok_ms: now,
                    detail: "ok".to_string(),
                },
            },
        ))
    }

    async fn emergency_health(
        &mut self,
    ) -> Result<(ServiceHealth, ServiceHealth), CoordinatorError> {
        let now = current_time_ms();
        let health = ServiceHealth {
            healthy: true,
            last_ok_ms: now,
            detail: "ok".to_string(),
        };
        Ok((health.clone(), health))
    }
}

#[derive(Clone)]
enum Script {
    Fill {
        fraction: Decimal,
        fee_currency: Option<String>,
        fee: Decimal,
    },
    Unresolved,
}

struct MockVenue {
    scripts: Mutex<VecDeque<Script>>,
    requests: Mutex<Vec<ExecutionOrderRequest>>,
}

impl MockVenue {
    fn new(scripts: Vec<Script>) -> Self {
        Self {
            scripts: Mutex::new(scripts.into()),
            requests: Mutex::new(Vec::new()),
        }
    }

}

#[async_trait]
impl CoordinatorVenue for MockVenue {
    async fn execute(
        &self,
        _risk_engine: &mut RiskEngine,
        _prepared: &PreparedExecution,
        request: &ExecutionOrderRequest,
    ) -> Result<ExecutionResult, ExecutionAttemptError> {
        self.requests.lock().unwrap().push(request.clone());
        let script = self
            .scripts
            .lock()
            .unwrap()
            .pop_front()
            .expect("missing mock execution script");

        let (fraction, fee_currency, fee) = match script {
            Script::Unresolved => {
                return Err(ExecutionAttemptError {
                    stage: ExecutionStage::Monitoring,
                    place_ack: Some(PlaceOrderAck {
                        order_id: "unknown-order".to_string(),
                        order_link_id: request.order_link_id.clone(),
                        accepted_at_ms: current_time_ms(),
                    }),
                    source: ExecutionError::OrderNotFound(
                        "unknown-order".to_string(),
                    ),
                });
            }
            Script::Fill {
                fraction,
                fee_currency,
                fee,
            } => (fraction, fee_currency, fee),
        };

        let requested = request.requested_quantity;
        let filled = requested * fraction;
        let remaining = requested - filled;
        let price = match (request.symbol.as_str(), request.side) {
            ("BTCUSDT", OrderSide::Buy) => d("10"),
            ("BTCUSDT", OrderSide::Sell) => d("9"),
            ("ETHBTC", OrderSide::Buy) => d("0.5"),
            ("ETHBTC", OrderSide::Sell) => d("0.49"),
            ("ETHUSDT", OrderSide::Sell) => d("6"),
            ("ETHUSDT", OrderSide::Buy) => d("6.1"),
            _ => panic!("unsupported mock symbol/side"),
        };
        let value = filled * price;
        let order_id = format!("order-{}", self.requests.lock().unwrap().len());
        let mut fees = BTreeMap::new();
        if let Some(currency) = fee_currency.as_ref() {
            fees.insert(currency.clone(), fee);
        }

        Ok(ExecutionResult {
            place_ack: PlaceOrderAck {
                order_id: order_id.clone(),
                order_link_id: request.order_link_id.clone(),
                accepted_at_ms: current_time_ms(),
            },
            monitor: MonitorResult {
                state: OrderExecutionState {
                    order_id: order_id.clone(),
                    order_link_id: request.order_link_id.clone(),
                    symbol: request.symbol.clone(),
                    status: if fraction == Decimal::ONE {
                        "Filled".to_string()
                    } else {
                        "PartiallyFilledCanceled".to_string()
                    },
                    requested_quantity: requested,
                    filled_quantity: filled,
                    remaining_quantity: remaining,
                    average_fill_price: Some(price),
                    fees,
                    fills: if filled > Decimal::ZERO {
                        vec![ExecutionFill {
                            execution_id: format!("exec-{order_id}"),
                            order_id: order_id.clone(),
                            quantity: filled,
                            price,
                            value,
                            fee,
                            fee_currency: fee_currency.unwrap_or_default(),
                            is_maker: false,
                            executed_at_ms: current_time_ms(),
                        }]
                    } else {
                        Vec::new()
                    },
                    terminal: true,
                    fully_filled: fraction == Decimal::ONE,
                    fills_confirmed: true,
                    reject_reason: None,
                    updated_at_ms: current_time_ms(),
                },
                timed_out: false,
            },
            cancellation: None,
        })
    }
}

#[tokio::test]
async fn propagates_actual_post_fee_quantity_through_all_three_legs() {
    let venue = MockVenue::new(vec![
        Script::Fill {
            fraction: Decimal::ONE,
            fee_currency: Some("BTC".to_string()),
            fee: d("0.01"),
        },
        Script::Fill {
            fraction: Decimal::ONE,
            fee_currency: None,
            fee: Decimal::ZERO,
        },
        Script::Fill {
            fraction: Decimal::ONE,
            fee_currency: None,
            fee: Decimal::ZERO,
        },
    ]);
    let planner = MockPlanner;
    let authorizer =
        RiskEngineAuthorizer::new(MockProvider, ExecutionMode::Testnet, false);
    let mut coordinator =
        ThreeLegCoordinator::new(coordinator_config(), venue, planner, authorizer)
            .unwrap();
    let mut risk = RiskEngine::new(risk_config("actual-quantity")).unwrap();

    let report = coordinator
        .execute_route(&mut risk, "route-success", &route(), d("4"))
        .await
        .unwrap();

    assert_eq!(report.status, CoordinatorStatus::Completed);
    assert_eq!(report.legs.len(), 3);
    assert_eq!(report.legs[0].actual_output_received, d("0.39"));
    assert_eq!(report.legs[1].requested_quantity, d("0.78"));
    assert_eq!(report.legs[2].requested_quantity, d("0.78"));
    assert_eq!(report.final_base_amount, d("4.68"));
    assert_eq!(report.realized_base_pnl, d("0.68"));
}

#[tokio::test]
async fn partial_leg_two_unwinds_both_remaining_intermediate_assets() {
    let venue = MockVenue::new(vec![
        Script::Fill {
            fraction: Decimal::ONE,
            fee_currency: None,
            fee: Decimal::ZERO,
        },
        Script::Fill {
            fraction: d("0.5"),
            fee_currency: None,
            fee: Decimal::ZERO,
        },
        Script::Fill {
            fraction: Decimal::ONE,
            fee_currency: None,
            fee: Decimal::ZERO,
        },
        Script::Fill {
            fraction: Decimal::ONE,
            fee_currency: None,
            fee: Decimal::ZERO,
        },
    ]);
    let planner = MockPlanner;
    let authorizer =
        RiskEngineAuthorizer::new(MockProvider, ExecutionMode::Testnet, false);
    let mut coordinator =
        ThreeLegCoordinator::new(coordinator_config(), venue, planner, authorizer)
            .unwrap();
    let mut risk = RiskEngine::new(risk_config("partial-leg-two")).unwrap();

    let report = coordinator
        .execute_route(&mut risk, "route-partial", &route(), d("4"))
        .await
        .unwrap();

    assert_eq!(report.status, CoordinatorStatus::RecoveredByUnwind);
    assert_eq!(report.legs.len(), 2);
    assert_eq!(report.unwind_orders.len(), 2);
    assert_eq!(
        report.holdings.get("BTC").copied().unwrap_or_default(),
        Decimal::ZERO
    );
    assert_eq!(
        report.holdings.get("ETH").copied().unwrap_or_default(),
        Decimal::ZERO
    );
    assert_eq!(report.final_base_amount, d("4.2"));
}

#[tokio::test]
async fn unresolved_leg_two_halts_without_blind_opposite_order() {
    let venue = MockVenue::new(vec![
        Script::Fill {
            fraction: Decimal::ONE,
            fee_currency: None,
            fee: Decimal::ZERO,
        },
        Script::Unresolved,
    ]);
    let planner = MockPlanner;
    let authorizer =
        RiskEngineAuthorizer::new(MockProvider, ExecutionMode::Testnet, false);
    let mut coordinator =
        ThreeLegCoordinator::new(coordinator_config(), venue, planner, authorizer)
            .unwrap();
    let mut risk = RiskEngine::new(risk_config("unresolved-leg-two")).unwrap();

    let report = coordinator
        .execute_route(&mut risk, "route-unresolved", &route(), d("4"))
        .await
        .unwrap();

    assert_eq!(report.status, CoordinatorStatus::HaltedUnresolved);
    assert!(report.unwind_orders.is_empty());
    assert_eq!(report.legs.len(), 1);

    let status = risk.status(current_time_ms()).unwrap();
    assert!(status.manual_kill_switch_active);
}
