use std::{collections::BTreeMap, time::Instant};

use event_bus::EventPublisher;
use execution::{prepare_execution, ExecutionResult};
use risk::{current_time_ms, EmergencyUnwindIntent, RiskEngine};
use rust_decimal::Decimal;
use scanner::{TradeSide, TriangleRoute};
use serde_json::json;
use sha2::{Digest, Sha256};
use tracing::error;

use crate::{
    authorizer::{LegAuthorizationContext, RouteRiskAuthorizer},
    planner::emergency_conversion_for_asset,
    ConversionLeg, CoordinatorConfig, CoordinatorError, CoordinatorStatus, CoordinatorVenue,
    Holdings, LegExecutionReport, PlannedOrder, RouteExecutionReport, RoutePlanner,
    UnwindExecutionReport,
};

pub struct ThreeLegCoordinator<V, P, A> {
    config: CoordinatorConfig,
    venue: V,
    planner: P,
    authorizer: A,
    events: EventPublisher,
}

impl<V, P, A> ThreeLegCoordinator<V, P, A>
where
    V: CoordinatorVenue,
    P: RoutePlanner,
    A: RouteRiskAuthorizer,
{
    pub fn new(
        config: CoordinatorConfig,
        venue: V,
        planner: P,
        authorizer: A,
    ) -> Result<Self, CoordinatorError> {
        config.validate()?;
        let events = EventPublisher::try_from_env("coordinator")
            .map_err(|error| CoordinatorError::InvalidConfig(error.to_string()))?;
        Ok(Self {
            config,
            venue,
            planner,
            authorizer,
            events,
        })
    }

    pub async fn execute_route(
        &mut self,
        risk_engine: &mut RiskEngine,
        trade_id: &str,
        route: &TriangleRoute,
        starting_amount: Decimal,
    ) -> Result<RouteExecutionReport, CoordinatorError> {
        self.execute_route_with_opportunity(risk_engine, trade_id, route, starting_amount, None)
            .await
    }

    pub async fn execute_route_for_opportunity(
        &mut self,
        risk_engine: &mut RiskEngine,
        trade_id: &str,
        route: &TriangleRoute,
        starting_amount: Decimal,
        opportunity_window_id: &str,
    ) -> Result<RouteExecutionReport, CoordinatorError> {
        let opportunity_window_id = opportunity_window_id.trim();
        if opportunity_window_id.is_empty() {
            return Err(CoordinatorError::InvalidRoute(
                "opportunity_window_id must not be empty".to_string(),
            ));
        }
        self.execute_route_with_opportunity(
            risk_engine,
            trade_id,
            route,
            starting_amount,
            Some(opportunity_window_id),
        )
        .await
    }

    async fn execute_route_with_opportunity(
        &mut self,
        risk_engine: &mut RiskEngine,
        trade_id: &str,
        route: &TriangleRoute,
        starting_amount: Decimal,
        opportunity_window_id: Option<&str>,
    ) -> Result<RouteExecutionReport, CoordinatorError> {
        self.events
            .ensure_critical_ready()
            .map_err(|error| CoordinatorError::Execution(error.to_string()))?;
        let started = Instant::now();
        let result = self
            .execute_route_inner(
                risk_engine,
                trade_id,
                route,
                starting_amount,
                opportunity_window_id,
            )
            .await;
        let execution_time_ms = started.elapsed().as_millis() as u64;
        let event_result = self.publish_trade_event(
            trade_id,
            route,
            starting_amount,
            execution_time_ms,
            opportunity_window_id,
            &result,
        );
        match event_result {
            Ok(()) => result,
            Err(error) => Err(error),
        }
    }

    async fn execute_route_inner(
        &mut self,
        risk_engine: &mut RiskEngine,
        trade_id: &str,
        route: &TriangleRoute,
        starting_amount: Decimal,
        opportunity_window_id: Option<&str>,
    ) -> Result<RouteExecutionReport, CoordinatorError> {
        validate_route(route, trade_id, starting_amount)?;
        self.events
            .publish_critical(
                "trade.attempted",
                json!({
                    "trade_id": trade_id,
                    "route_id": route.id.clone(),
                    "triangle_id": route.triangle_id.clone(),
                    "base_asset": route.start_asset.clone(),
                    "starting_amount": starting_amount.to_string(),
                    "asset_path": route.assets.clone(),
                    "opportunity_window_id": opportunity_window_id,
                }),
            )
            .map_err(|error| CoordinatorError::Execution(error.to_string()))?;

        let mut holdings = BTreeMap::new();
        holdings.insert(route.start_asset.clone(), starting_amount);
        let mut leg_reports = Vec::new();
        let mut unwind_reports = Vec::new();

        for leg_index in 0..3 {
            let conversion = ConversionLeg::from(&route.legs[leg_index]);
            let input_amount = positive_holding(&holdings, &conversion.from_asset);
            if input_amount <= Decimal::ZERO {
                return self
                    .recover_or_finish(
                        risk_engine,
                        trade_id,
                        route,
                        starting_amount,
                        holdings,
                        leg_reports,
                        unwind_reports,
                        CoordinatorStatus::RecoveredByUnwind,
                        Some(format!(
                            "leg {} has no usable {} input",
                            leg_index + 1,
                            conversion.from_asset
                        )),
                    )
                    .await;
            }

            let link_id = order_link_id(trade_id, "leg", leg_index + 1, 0);
            let plan = match self
                .planner
                .plan_conversion(route, &conversion, input_amount, link_id, false)
                .await
            {
                Ok(value) => value,
                Err(error) if leg_index == 0 => {
                    return Ok(report(
                        trade_id,
                        route,
                        starting_amount,
                        holdings,
                        leg_reports,
                        unwind_reports,
                        CoordinatorStatus::NotStarted,
                        Some(error.to_string()),
                        Some(Decimal::ZERO),
                    ));
                }
                Err(error) => {
                    return self
                        .recover_or_finish(
                            risk_engine,
                            trade_id,
                            route,
                            starting_amount,
                            holdings,
                            leg_reports,
                            unwind_reports,
                            CoordinatorStatus::RecoveredByUnwind,
                            Some(format!("leg planning failed: {error}")),
                        )
                        .await;
                }
            };

            let prepared = if leg_index == 2 {
                match self
                    .prepare_risk_reducing_order(risk_engine, trade_id, route, &plan, input_amount)
                    .await
                {
                    Ok(value) => value,
                    Err(error) => {
                        return self
                            .recover_or_finish(
                                risk_engine,
                                trade_id,
                                route,
                                starting_amount,
                                holdings,
                                leg_reports,
                                unwind_reports,
                                CoordinatorStatus::RecoveredByUnwind,
                                Some(format!("final-leg close authorization failed: {error}")),
                            )
                            .await;
                    }
                }
            } else {
                let context = LegAuthorizationContext {
                    trade_id,
                    route,
                    leg_index,
                    planned: &plan,
                    holdings: &holdings,
                    now_ms: current_time_ms(),
                };
                match self.authorizer.authorize_leg(risk_engine, &context).await {
                    Ok(value) => value,
                    Err(error) if leg_index == 0 => {
                        return Ok(report(
                            trade_id,
                            route,
                            starting_amount,
                            holdings,
                            leg_reports,
                            unwind_reports,
                            CoordinatorStatus::NotStarted,
                            Some(error.to_string()),
                            Some(Decimal::ZERO),
                        ));
                    }
                    Err(error) => {
                        return self
                            .recover_or_finish(
                                risk_engine,
                                trade_id,
                                route,
                                starting_amount,
                                holdings,
                                leg_reports,
                                unwind_reports,
                                CoordinatorStatus::RecoveredByUnwind,
                                Some(format!("risk reauthorization failed: {error}")),
                            )
                            .await;
                    }
                }
            };

            let result = match self
                .venue
                .execute(risk_engine, &prepared, &plan.request)
                .await
            {
                Ok(value) => value,
                Err(error) if error.safe_to_unwind_prior_exposure() && leg_index == 0 => {
                    return Ok(report(
                        trade_id,
                        route,
                        starting_amount,
                        holdings,
                        leg_reports,
                        unwind_reports,
                        CoordinatorStatus::NotStarted,
                        Some(error.to_string()),
                        Some(Decimal::ZERO),
                    ));
                }
                Err(error) if error.safe_to_unwind_prior_exposure() => {
                    return self
                        .recover_or_finish(
                            risk_engine,
                            trade_id,
                            route,
                            starting_amount,
                            holdings,
                            leg_reports,
                            unwind_reports,
                            CoordinatorStatus::RecoveredByUnwind,
                            Some(error.to_string()),
                        )
                        .await;
                }
                Err(error) => {
                    let reason = format!(
                        "unresolved exchange state after leg {}: {}",
                        leg_index + 1,
                        error
                    );
                    engage_unresolved_kill_switch(risk_engine, &reason)?;
                    return Ok(report(
                        trade_id,
                        route,
                        starting_amount,
                        holdings,
                        leg_reports,
                        unwind_reports,
                        CoordinatorStatus::HaltedUnresolved,
                        Some(reason),
                        None,
                    ));
                }
            };

            if !result.monitor.state.terminal || !result.monitor.state.fills_confirmed {
                let reason = format!(
                    concat!(
                        "leg {} order {} is not terminal/confirmed: ",
                        "status={} terminal={} fills_confirmed={}"
                    ),
                    leg_index + 1,
                    result.monitor.state.order_id,
                    result.monitor.state.status,
                    result.monitor.state.terminal,
                    result.monitor.state.fills_confirmed
                );
                engage_unresolved_kill_switch(risk_engine, &reason)?;
                return Ok(report(
                    trade_id,
                    route,
                    starting_amount,
                    holdings,
                    leg_reports,
                    unwind_reports,
                    CoordinatorStatus::HaltedUnresolved,
                    Some(reason),
                    None,
                ));
            }

            let applied = apply_execution(&mut holdings, &conversion, &result)?;
            leg_reports.push(LegExecutionReport {
                leg_index,
                symbol: conversion.symbol.clone(),
                from_asset: conversion.from_asset.clone(),
                to_asset: conversion.to_asset.clone(),
                requested_quantity: result.monitor.state.requested_quantity,
                filled_quantity: result.monitor.state.filled_quantity,
                remaining_quantity: result.monitor.state.remaining_quantity,
                actual_input_spent: applied.input_spent,
                actual_output_received: applied.net_output,
                average_fill_price: result.monitor.state.average_fill_price,
                fees: result.monitor.state.fees.clone(),
                status: result.monitor.state.status.clone(),
                fully_filled: result.monitor.state.fully_filled,
                fills_confirmed: result.monitor.state.fills_confirmed,
                order_id: result.monitor.state.order_id.clone(),
            });

            if !result.monitor.state.fully_filled {
                return self
                    .recover_or_finish(
                        risk_engine,
                        trade_id,
                        route,
                        starting_amount,
                        holdings,
                        leg_reports,
                        unwind_reports,
                        CoordinatorStatus::RecoveredByUnwind,
                        Some(format!(
                            "leg {} partially filled or ended without full fill",
                            leg_index + 1
                        )),
                    )
                    .await;
            }
        }

        let positive_residual_before = self.positive_residual_value_base(route, &holdings).await?;
        if positive_residual_before > self.config.max_dust_notional_base {
            let unwind_result = self
                .unwind_all(
                    risk_engine,
                    trade_id,
                    route,
                    &mut holdings,
                    &mut unwind_reports,
                )
                .await;
            if let Err(error) = unwind_result {
                let reason = format!("post-route residual cleanup failed: {error}");
                engage_unresolved_kill_switch(risk_engine, &reason)?;
                return Ok(report(
                    trade_id,
                    route,
                    starting_amount,
                    holdings,
                    leg_reports,
                    unwind_reports,
                    CoordinatorStatus::UnwindFailed,
                    Some(reason),
                    None,
                ));
            }

            let signed_residual = self.signed_residual_value_base(route, &holdings).await?;
            return Ok(report(
                trade_id,
                route,
                starting_amount,
                holdings,
                leg_reports,
                unwind_reports,
                CoordinatorStatus::CompletedWithResidualCleanup,
                None,
                signed_residual,
            ));
        }

        let signed_residual = self.signed_residual_value_base(route, &holdings).await?;
        Ok(report(
            trade_id,
            route,
            starting_amount,
            holdings,
            leg_reports,
            unwind_reports,
            CoordinatorStatus::Completed,
            None,
            signed_residual,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    async fn recover_or_finish(
        &mut self,
        risk_engine: &mut RiskEngine,
        trade_id: &str,
        route: &TriangleRoute,
        starting_amount: Decimal,
        mut holdings: Holdings,
        leg_reports: Vec<LegExecutionReport>,
        mut unwind_reports: Vec<UnwindExecutionReport>,
        success_status: CoordinatorStatus,
        reason: Option<String>,
    ) -> Result<RouteExecutionReport, CoordinatorError> {
        let positive_residual = self.positive_residual_value_base(route, &holdings).await?;
        if positive_residual <= self.config.max_dust_notional_base {
            let signed_residual = self.signed_residual_value_base(route, &holdings).await?;
            return Ok(report(
                trade_id,
                route,
                starting_amount,
                holdings,
                leg_reports,
                unwind_reports,
                success_status,
                reason,
                signed_residual,
            ));
        }

        if let Err(error) = self
            .unwind_all(
                risk_engine,
                trade_id,
                route,
                &mut holdings,
                &mut unwind_reports,
            )
            .await
        {
            let failure = format!(
                "{}; emergency unwind failed: {}",
                reason.unwrap_or_else(|| "route recovery requested".to_string()),
                error
            );
            engage_unresolved_kill_switch(risk_engine, &failure)?;
            return Ok(report(
                trade_id,
                route,
                starting_amount,
                holdings,
                leg_reports,
                unwind_reports,
                CoordinatorStatus::UnwindFailed,
                Some(failure),
                None,
            ));
        }

        let signed_residual_after = self.signed_residual_value_base(route, &holdings).await?;
        Ok(report(
            trade_id,
            route,
            starting_amount,
            holdings,
            leg_reports,
            unwind_reports,
            success_status,
            reason,
            signed_residual_after,
        ))
    }

    async fn unwind_all(
        &mut self,
        risk_engine: &mut RiskEngine,
        trade_id: &str,
        route: &TriangleRoute,
        holdings: &mut Holdings,
        reports: &mut Vec<UnwindExecutionReport>,
    ) -> Result<(), CoordinatorError> {
        let mut assets = vec![route.assets[2].clone(), route.assets[1].clone()];
        assets.dedup();

        for asset in assets {
            for attempt in 1..=self.config.max_unwind_attempts_per_asset {
                let amount = positive_holding(holdings, &asset);
                if amount <= Decimal::ZERO {
                    break;
                }
                let base_value = self.planner.value_in_base(route, &asset, amount).await?;
                if base_value <= self.config.max_dust_notional_base {
                    break;
                }

                let conversion = emergency_conversion_for_asset(route, &asset)?;
                let unwind_trade_id = format!("{}:unwind:{}:{}", trade_id, asset, attempt);
                let plan = self
                    .planner
                    .plan_conversion(
                        route,
                        &conversion,
                        amount,
                        order_link_id(trade_id, "unwind", attempt, reports.len() + 1),
                        true,
                    )
                    .await?;
                let prepared = self
                    .prepare_risk_reducing_order(
                        risk_engine,
                        &unwind_trade_id,
                        route,
                        &plan,
                        amount,
                    )
                    .await?;

                let result = match self
                    .venue
                    .execute(risk_engine, &prepared, &plan.request)
                    .await
                {
                    Ok(value) => value,
                    Err(error) => {
                        return Err(CoordinatorError::Unwind(format!(
                            "{} unwind execution failed: {}",
                            asset, error
                        )));
                    }
                };
                if !result.monitor.state.terminal || !result.monitor.state.fills_confirmed {
                    return Err(CoordinatorError::Unwind(format!(
                        "{} unwind order {} unresolved",
                        asset, result.monitor.state.order_id
                    )));
                }

                let applied = apply_execution(holdings, &conversion, &result)?;
                reports.push(UnwindExecutionReport {
                    asset: asset.clone(),
                    attempt,
                    symbol: conversion.symbol.clone(),
                    requested_quantity: result.monitor.state.requested_quantity,
                    filled_quantity: result.monitor.state.filled_quantity,
                    output_base_received: applied.net_output,
                    fees: result.monitor.state.fees.clone(),
                    order_id: result.monitor.state.order_id.clone(),
                    status: result.monitor.state.status.clone(),
                });

                if result.monitor.state.fully_filled {
                    let remaining = self
                        .planner
                        .value_in_base(route, &asset, positive_holding(holdings, &asset))
                        .await
                        .unwrap_or(Decimal::ZERO);
                    if remaining <= self.config.max_dust_notional_base {
                        break;
                    }
                }
            }

            let remaining = positive_holding(holdings, &asset);
            if remaining > Decimal::ZERO {
                let base_value = self.planner.value_in_base(route, &asset, remaining).await?;
                if base_value > self.config.max_dust_notional_base {
                    return Err(CoordinatorError::Unwind(format!(
                        "{} remains as {} {} (~{} {}) after maximum unwind attempts",
                        asset, remaining, asset, base_value, route.start_asset
                    )));
                }
            }
        }
        Ok(())
    }

    async fn prepare_risk_reducing_order(
        &mut self,
        risk_engine: &mut RiskEngine,
        trade_id: &str,
        route: &TriangleRoute,
        plan: &PlannedOrder,
        exposure_amount: Decimal,
    ) -> Result<execution::PreparedExecution, CoordinatorError> {
        let (api_health, exchange_health) = self.authorizer.emergency_health().await?;
        let exposure_notional = self
            .planner
            .value_in_base(route, &plan.conversion.from_asset, exposure_amount)
            .await?;
        let intent = EmergencyUnwindIntent {
            trade_id: trade_id.to_string(),
            exposure_asset: plan.conversion.from_asset.clone(),
            base_asset: route.start_asset.clone(),
            exposure_notional,
            unwind_notional: plan.estimated_notional_base.min(exposure_notional),
            market_data_timestamp_ms: plan.market_timestamp_ms,
            api_health,
            exchange_health,
        };
        let now = current_time_ms();
        let approval = risk_engine
            .approve_emergency_unwind(&intent, now)
            .map_err(|error| CoordinatorError::Risk(error.to_string()))?;
        prepare_execution(
            risk_engine,
            trade_id,
            approval,
            now,
            self.authorizer.mode(),
            self.authorizer.live_enabled(),
        )
        .map_err(|error| CoordinatorError::Risk(error.to_string()))
    }

    fn publish_trade_event(
        &self,
        trade_id: &str,
        route: &TriangleRoute,
        starting_amount: Decimal,
        execution_time_ms: u64,
        opportunity_window_id: Option<&str>,
        result: &Result<RouteExecutionReport, CoordinatorError>,
    ) -> Result<(), CoordinatorError> {
        let persisted = match result {
            Ok(report)
                if matches!(
                    report.status,
                    CoordinatorStatus::Completed | CoordinatorStatus::CompletedWithResidualCleanup
                ) =>
            {
                self.events.publish_critical(
                    "trade.executed",
                    json!({
                        "trade_id": report.trade_id.clone(),
                        "route_id": report.route_id.clone(),
                        "base_asset": report.base_asset.clone(),
                        "starting_amount": report.starting_amount.to_string(),
                        "final_base_amount": report.final_base_amount.to_string(),
                        "realized_base_pnl": report.realized_base_pnl.to_string(),
                        "economic_pnl": report.economic_pnl.map(|value| value.to_string()),
                        "status": format!("{:?}", report.status),
                        "leg_count": report.legs.len(),
                        "unwind_count": report.unwind_orders.len(),
                        "estimated_turnover_base": estimated_report_turnover_base(report)
                            .to_string(),
                        "turnover_basis": "base-flow estimate with cross-leg fill-ratio proxy",
                        "opportunity_window_id": opportunity_window_id,
                        "execution_time_ms": execution_time_ms,
                    }),
                )
            }
            Ok(report) => self.events.publish_critical(
                "trade.failed",
                json!({
                    "trade_id": report.trade_id.clone(),
                    "route_id": report.route_id.clone(),
                    "base_asset": report.base_asset.clone(),
                    "starting_amount": report.starting_amount.to_string(),
                    "final_base_amount": report.final_base_amount.to_string(),
                    "realized_base_pnl": report.realized_base_pnl.to_string(),
                    "economic_pnl": report.economic_pnl.map(|value| value.to_string()),
                    "status": format!("{:?}", report.status),
                    "failure_reason": report.failure_reason.clone(),
                    "leg_count": report.legs.len(),
                    "unwind_count": report.unwind_orders.len(),
                    "estimated_turnover_base": estimated_report_turnover_base(report)
                        .to_string(),
                    "turnover_basis": "base-flow estimate with cross-leg fill-ratio proxy",
                    "opportunity_window_id": opportunity_window_id,
                    "execution_time_ms": execution_time_ms,
                }),
            ),
            Err(error) => self.events.publish_critical(
                "trade.failed",
                json!({
                    "trade_id": trade_id,
                    "route_id": route.id.clone(),
                    "base_asset": route.start_asset.clone(),
                    "starting_amount": starting_amount.to_string(),
                    "status": "coordinator_error",
                    "failure_reason": error.to_string(),
                    "opportunity_window_id": opportunity_window_id,
                    "execution_time_ms": execution_time_ms,
                }),
            ),
        };

        persisted
            .map(|_| ())
            .map_err(|error| CoordinatorError::Execution(error.to_string()))
    }

    async fn positive_residual_value_base(
        &self,
        route: &TriangleRoute,
        holdings: &Holdings,
    ) -> Result<Decimal, CoordinatorError> {
        let mut total = Decimal::ZERO;
        for asset in route.assets.iter().take(3) {
            if asset == &route.start_asset {
                continue;
            }
            let amount = positive_holding(holdings, asset);
            if amount > Decimal::ZERO {
                total += self.planner.value_in_base(route, asset, amount).await?;
            }
        }
        Ok(total)
    }

    async fn signed_residual_value_base(
        &self,
        route: &TriangleRoute,
        holdings: &Holdings,
    ) -> Result<Option<Decimal>, CoordinatorError> {
        let mut total = Decimal::ZERO;
        for (asset, amount) in holdings {
            if asset == &route.start_asset || *amount == Decimal::ZERO {
                continue;
            }
            if !route
                .assets
                .iter()
                .take(3)
                .any(|route_asset| route_asset == asset)
            {
                return Ok(None);
            }
            let magnitude = if *amount < Decimal::ZERO {
                -*amount
            } else {
                *amount
            };
            let value = self.planner.value_in_base(route, asset, magnitude).await?;
            total += if *amount < Decimal::ZERO {
                -value
            } else {
                value
            };
        }
        Ok(Some(total))
    }
}

struct AppliedExecution {
    input_spent: Decimal,
    net_output: Decimal,
}

fn apply_execution(
    holdings: &mut Holdings,
    conversion: &ConversionLeg,
    result: &ExecutionResult,
) -> Result<AppliedExecution, CoordinatorError> {
    let state = &result.monitor.state;
    if !state.fills_confirmed {
        return Err(CoordinatorError::Unresolved(format!(
            "cannot apply unconfirmed fills for order {}",
            state.order_id
        )));
    }

    let execution_value = state
        .fills
        .iter()
        .fold(Decimal::ZERO, |total, fill| total + fill.value);
    let (input_spent, gross_output) = match conversion.side {
        TradeSide::Buy => (execution_value, state.filled_quantity),
        TradeSide::Sell => (state.filled_quantity, execution_value),
    };

    adjust_holding(holdings, &conversion.from_asset, -input_spent);
    adjust_holding(holdings, &conversion.to_asset, gross_output);

    let mut output_fee = Decimal::ZERO;
    for (currency, fee) in &state.fees {
        adjust_holding(holdings, currency, -*fee);
        if currency == &conversion.to_asset {
            output_fee += *fee;
        }
    }

    Ok(AppliedExecution {
        input_spent,
        net_output: (gross_output - output_fee).max(Decimal::ZERO),
    })
}

fn adjust_holding(holdings: &mut Holdings, asset: &str, delta: Decimal) {
    *holdings.entry(asset.to_string()).or_insert(Decimal::ZERO) += delta;
}

fn positive_holding(holdings: &Holdings, asset: &str) -> Decimal {
    holdings
        .get(asset)
        .copied()
        .unwrap_or(Decimal::ZERO)
        .max(Decimal::ZERO)
}

fn validate_route(
    route: &TriangleRoute,
    trade_id: &str,
    starting_amount: Decimal,
) -> Result<(), CoordinatorError> {
    if trade_id.trim().is_empty() {
        return Err(CoordinatorError::InvalidRoute(
            "trade_id must not be empty".to_string(),
        ));
    }
    if route.legs.len() != 3 || route.assets.len() != 4 {
        return Err(CoordinatorError::InvalidRoute(
            "coordinator requires an exact three-leg triangle".to_string(),
        ));
    }
    if route.assets[0] != route.assets[3] || route.start_asset != route.assets[0] {
        return Err(CoordinatorError::InvalidRoute(
            "route must return to its start asset".to_string(),
        ));
    }
    if starting_amount <= Decimal::ZERO {
        return Err(CoordinatorError::InvalidRoute(
            "starting amount must be positive".to_string(),
        ));
    }
    Ok(())
}

fn order_link_id(trade_id: &str, kind: &str, primary: usize, secondary: usize) -> String {
    let mut hasher = Sha256::new();
    hasher.update(trade_id.as_bytes());
    let digest = hex::encode(hasher.finalize());
    format!(
        "arb-{}-{}{}{}",
        &digest[..20],
        if kind == "unwind" { "u" } else { "l" },
        primary,
        secondary
    )
}

fn engage_unresolved_kill_switch(
    risk_engine: &RiskEngine,
    reason: &str,
) -> Result<(), CoordinatorError> {
    error!(
        reason,
        "engaging manual kill switch for unresolved execution state"
    );
    risk_engine
        .engage_manual_kill_switch(reason, current_time_ms())
        .map_err(|error| CoordinatorError::Risk(error.to_string()))
}

fn estimated_report_turnover_base(report: &RouteExecutionReport) -> Decimal {
    let starting_amount = nonnegative_decimal(report.starting_amount);
    let mut turnover = Decimal::ZERO;

    for leg in &report.legs {
        if leg.filled_quantity <= Decimal::ZERO {
            continue;
        }

        let base_equivalent = if leg.from_asset == report.base_asset {
            nonnegative_decimal(leg.actual_input_spent)
        } else if leg.to_asset == report.base_asset {
            nonnegative_decimal(leg.actual_output_received)
        } else if leg.requested_quantity > Decimal::ZERO && starting_amount > Decimal::ZERO {
            let ratio = leg.filled_quantity / leg.requested_quantity;
            let capped_ratio = if ratio > Decimal::ONE {
                Decimal::ONE
            } else {
                ratio
            };
            starting_amount * capped_ratio
        } else {
            Decimal::ZERO
        };

        turnover += base_equivalent;
    }

    for unwind in &report.unwind_orders {
        if unwind.filled_quantity > Decimal::ZERO {
            turnover += nonnegative_decimal(unwind.output_base_received);
        }
    }

    turnover
}

fn nonnegative_decimal(value: Decimal) -> Decimal {
    if value < Decimal::ZERO {
        -value
    } else {
        value
    }
}

#[allow(clippy::too_many_arguments)]
fn report(
    trade_id: &str,
    route: &TriangleRoute,
    starting_amount: Decimal,
    holdings: Holdings,
    leg_reports: Vec<LegExecutionReport>,
    unwind_orders: Vec<UnwindExecutionReport>,
    status: CoordinatorStatus,
    failure_reason: Option<String>,
    signed_residual_value_base: Option<Decimal>,
) -> RouteExecutionReport {
    let final_base_amount = holdings
        .get(&route.start_asset)
        .copied()
        .unwrap_or(Decimal::ZERO);
    let realized_base_pnl = final_base_amount - starting_amount;
    let economic_pnl = signed_residual_value_base.map(|residual| realized_base_pnl + residual);

    RouteExecutionReport {
        trade_id: trade_id.to_string(),
        route_id: route.id.clone(),
        base_asset: route.start_asset.clone(),
        starting_amount,
        final_base_amount,
        realized_base_pnl,
        residual_value_base: signed_residual_value_base,
        economic_pnl,
        status,
        holdings,
        legs: leg_reports,
        unwind_orders,
        failure_reason,
    }
}
