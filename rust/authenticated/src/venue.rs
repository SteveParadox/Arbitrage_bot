use crate::{DeploymentPermit, Ledger};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use coordinator::{
    ConversionLeg, CoordinatorError, CoordinatorVenue, LegAuthorizationContext, PlannedOrder,
    RiskIntentProvider, RoutePlanner,
};
use execution::{
    BybitExecutionClient, ExecutionAttemptError, ExecutionError, ExecutionMode,
    ExecutionOrderRequest, ExecutionResult, ExecutionStage, PreparedExecution,
};
use paper::{PaperPlanner, StopGuard};
use risk::{
    current_time_ms, ProposedOrderLeg, RiskContext, RiskEngine, ServiceHealth, TradeIntent,
};
use rust_decimal::Decimal;
use scanner::TriangleRoute;
use serde_json::json;
use std::path::PathBuf;

/// Reuse the existing decimal depth/filter planner; this adapter does no
/// simulated matching or paper financial mutations.
#[derive(Clone)]
pub struct ExchangePlanner(pub PaperPlanner);
#[async_trait]
impl RoutePlanner for ExchangePlanner {
    async fn plan_conversion(
        &self,
        route: &TriangleRoute,
        conversion: &ConversionLeg,
        input: Decimal,
        link: String,
        emergency: bool,
    ) -> Result<PlannedOrder, CoordinatorError> {
        let planned: Result<PlannedOrder> = async {
            let side = conversion.order_side();
            // The exchange anchors market slippage to its own arrival-time book.
            // An explicit IOC limit preserves the locally authorized price bound.
            let tolerance = if emergency {
                Decimal::new(50, 2)
            } else {
                self.0.settings.slippage_percent
            };
            let budget = if side == execution::OrderSide::Buy {
                input / (Decimal::ONE + tolerance / Decimal::from(100) + self.0.settings.fee_rate)
            } else {
                input
            };
            let mut plan = self
                .0
                .plan_conversion(route, conversion, budget, link, emergency)
                .await?;
            let (book, instrument) = self.0.books.snapshot(&conversion.symbol, side).await?;
            if instrument.base != conversion.base_asset
                || instrument.quote != conversion.quote_asset
            {
                bail!("limit-order metadata changed route identity");
            }
            if current_time_ms()
                .checked_sub(book.timestamp)
                .is_none_or(|age| age > self.0.settings.max_age_ms)
                || current_time_ms()
                    .checked_sub(instrument.timestamp)
                    .is_none_or(|age| age > 300_000)
            {
                bail!("stale limit-order evidence");
            }
            let best = if side == execution::OrderSide::Buy {
                book.asks.first()
            } else {
                book.bids.first()
            }
            .context("missing limit-order price evidence")?
            .0;
            let (quantity, price) = limit_bounds(
                side,
                input,
                plan.request.requested_quantity,
                best,
                &instrument,
                self.0.settings.fee_rate,
                tolerance,
            )?;
            plan.input_amount = input;
            plan.request.requested_quantity = quantity;
            plan.request.order_type = execution::OrderType::Limit;
            plan.request.price = Some(price);
            plan.request.slippage_tolerance_percent = None;
            plan.rules = risk::SymbolRules {
                qty_step: instrument.step,
                min_order_qty: instrument.step,
                tick_size: instrument.tick,
            };
            plan.market_timestamp_ms = book.timestamp.min(plan.market_timestamp_ms);
            plan.estimated_output = if side == execution::OrderSide::Buy {
                quantity * (Decimal::ONE - self.0.settings.fee_rate)
            } else {
                quantity * price * (Decimal::ONE - self.0.settings.fee_rate)
            };
            plan.estimated_notional_base = self
                .0
                .value_in_base(
                    route,
                    &conversion.from_asset,
                    if side == execution::OrderSide::Buy {
                        quantity * price
                    } else {
                        quantity
                    },
                )
                .await?;
            plan.request.estimated_notional = plan.estimated_notional_base;
            Ok(plan)
        }
        .await;
        planned.map_err(|e| CoordinatorError::Planning(e.to_string()))
    }

    async fn value_in_base(
        &self,
        route: &TriangleRoute,
        asset: &str,
        amount: Decimal,
    ) -> Result<Decimal, CoordinatorError> {
        self.0.value_in_base(route, asset, amount).await
    }
}

fn limit_bounds(
    side: execution::OrderSide,
    input: Decimal,
    planned_quantity: Decimal,
    best: Decimal,
    instrument: &paper::Instrument,
    fee: Decimal,
    tolerance_percent: Decimal,
) -> Result<(Decimal, Decimal)> {
    let maximum = instrument
        .max_limit_quantity
        .context("maxLimitOrderQty missing")?;
    if input <= Decimal::ZERO
        || planned_quantity <= Decimal::ZERO
        || best <= Decimal::ZERO
        || maximum <= Decimal::ZERO
        || instrument.step <= Decimal::ZERO
        || instrument.tick <= Decimal::ZERO
        || fee < Decimal::ZERO
        || fee >= Decimal::ONE
        || tolerance_percent < Decimal::ZERO
        || tolerance_percent >= Decimal::from(100)
    {
        bail!("invalid bounded IOC inputs");
    }
    let buy = side == execution::OrderSide::Buy;
    let multiplier = if buy {
        Decimal::ONE + tolerance_percent / Decimal::from(100)
    } else {
        Decimal::ONE - tolerance_percent / Decimal::from(100)
    };
    let raw = best
        .checked_mul(multiplier)
        .context("limit price overflow")?;
    // Buy caps round down; sell floors round up. Neither can loosen the bound.
    let mut price = raw - raw % instrument.tick;
    if !buy && price != raw {
        price = price
            .checked_add(instrument.tick)
            .context("limit price overflow")?;
    }
    if price <= Decimal::ZERO {
        bail!("price below tick size");
    }
    let affordable = if buy {
        input
            .checked_div(
                price
                    .checked_mul(Decimal::ONE + fee)
                    .context("fee budget overflow")?,
            )
            .context("quantity overflow")?
    } else {
        input
    };
    let raw_quantity = planned_quantity.min(maximum).min(affordable);
    let quantity = raw_quantity - raw_quantity % instrument.step;
    if quantity <= Decimal::ZERO
        || quantity.checked_mul(price).context("notional overflow")? < instrument.min_amount
    {
        bail!("bounded IOC below minimum order amount");
    }
    Ok((quantity, price))
}

#[derive(Clone)]
pub struct Gate {
    pub account: String,
    pub mode: ExecutionMode,
    pub mock: bool,
    pub permit_path: PathBuf,
    pub stop: StopGuard,
}
impl Gate {
    pub fn check(&self, reducing: bool) -> Result<()> {
        let permit = DeploymentPermit::load(&self.permit_path)?;
        permit.verify(&self.account, self.mode, current_time_ms(), self.mock)?;
        if runtime_clock::Clock::current().is_virtual() {
            bail!("private venue refuses virtual clock");
        }
        if std::env::var("ARB_TRADING_MODE").as_deref()
            != Ok(match self.mode {
                ExecutionMode::Testnet => "testnet",
                ExecutionMode::Live => "live",
                _ => bail!("invalid authenticated mode"),
            })
        {
            bail!("runtime execution mode changed");
        }
        if self.mode == ExecutionMode::Testnet {
            if std::env::var("ARB_TESTNET_TRADING_ENABLED").as_deref() != Ok("true")
                || std::env::var("ARB_LIVE_TRADING_ENABLED").as_deref() != Ok("false")
            {
                bail!("testnet deployment permission invalid");
            }
        } else if std::env::var("ARB_LIVE_TRADING_ENABLED").as_deref() != Ok("true")
            || std::env::var("ARB_TESTNET_TRADING_ENABLED").as_deref() != Ok("false")
        {
            bail!("live deployment disabled");
        }
        if !reducing {
            risk::control::read_control(&self.stop.control)?
                .context("global control state missing; execution permission unknown")?;
            self.stop.check()?;
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct Venue {
    pub client: BybitExecutionClient,
    pub ledger: Ledger,
    pub cycle: String,
    pub route: TriangleRoute,
    pub gate: Gate,
    pub balance_age_ms: u64,
    pub fee_ceiling: Decimal,
}
fn journal_error(e: impl std::fmt::Display) -> ExecutionAttemptError {
    // Database ambiguity is never evidence of a definitive exchange rejection.
    ExecutionAttemptError {
        stage: ExecutionStage::Monitoring,
        place_ack: None,
        source: ExecutionError::MissingData(e.to_string()),
    }
}
#[async_trait]
impl CoordinatorVenue for Venue {
    async fn execute(
        &self,
        risk: &mut RiskEngine,
        prepared: &PreparedExecution,
        request: &ExecutionOrderRequest,
    ) -> Result<ExecutionResult, ExecutionAttemptError> {
        let preflight:Result<()> = async {
            if prepared.mode()!=self.gate.mode || !(prepared.trade_id()==self.cycle || prepared.is_risk_reducing() && prepared.trade_id().starts_with(&format!("{}:unwind:",self.cycle))) { bail!("venue/risk/account identity mismatch"); }
            prepared.revalidate(risk,current_time_ms())?;
            self.gate.check(prepared.is_risk_reducing())?;
            paper::redis_ready().await?;
            self.client.verify_no_open_orders().await?;
            let permit=DeploymentPermit::load(&self.gate.permit_path)?;
            self.client.verify_account(&permit.exchange_uid).await?;
            let balance=self.client.sync_balances(&[]).await?;
            if !self.ledger.reconcile(&balance,current_time_ms(),self.balance_age_ms).await? { bail!("exchange balance discrepancy"); }
            let portfolio=self.ledger.portfolio(current_time_ms(),self.balance_age_ms).await?;
            if !prepared.is_risk_reducing() && !portfolio.enabled { bail!("account execution disabled"); }
            let leg=self.route.legs.iter().find(|l|l.symbol==request.symbol).context("order is not in reserved route")?;
            let (from,to)=if request.side==execution::OrderSide::Buy {(&leg.quote_asset,&leg.base_asset)} else {(&leg.base_asset,&leg.quote_asset)};
            let holdings=self.ledger.holdings(&self.cycle).await?;
            if holdings.values().any(|v|*v<Decimal::ZERO) || holdings.get(from).copied().unwrap_or_default()<=Decimal::ZERO { bail!("unknown fees or unavailable cycle holding"); }
            if request.order_type != execution::OrderType::Limit || request.time_in_force != execution::TimeInForce::Ioc || request.slippage_tolerance_percent.is_some() { bail!("authenticated runner requires a price-bounded IOC"); }
            let price=request.price.context("explicit IOC price required")?;
            let maximum_spend=if request.side==execution::OrderSide::Buy {request.requested_quantity.checked_mul(price).and_then(|v|v.checked_mul(Decimal::ONE+self.fee_ceiling)).context("order spend overflow")?} else {request.requested_quantity};
            let input_budget=holdings.get(from).copied().unwrap_or_default();
            if maximum_spend>input_budget {bail!("bounded order exceeds cycle-owned capital");}
            let fee_rate=self.client.verify_spot_fee_rate(&request.symbol,self.fee_ceiling).await?;
            // Private request cannot begin before the immutable intent commits.
            self.ledger.intent(&self.cycle,&request.order_link_id,&json!({"symbol":request.symbol,"side":request.side,"from":from,"to":to,"quantity":request.requested_quantity.to_string(),"order_type":request.order_type,"limit_price":price.to_string(),"input_budget":input_budget.to_string(),"maximum_spend":maximum_spend.to_string(),"fee_ceiling":self.fee_ceiling.to_string(),"verified_taker_fee_rate":fee_rate.to_string(),"risk_approved_at_ms":prepared.risk_approved_at_ms(),"risk_reducing":prepared.is_risk_reducing(),"mode":format!("{:?}",prepared.mode())})).await?;
            crate::checkpoint(&self.client,"after_intent");
            self.ledger.order_state(&request.order_link_id,"SUBMITTING").await?;
            Ok(())
        }.await;
        if let Err(e) = preflight {
            return Err(journal_error(e));
        }
        let outcome = self
            .client
            .execute_with_journal(risk, prepared, request, Some(self))
            .await;
        crate::checkpoint(&self.client, "after_exchange_before_journal");
        match outcome {
            Ok(result) => {
                self.ledger
                    .apply(&request.order_link_id, &result.monitor.state)
                    .await
                    .map_err(journal_error)?;
                crate::checkpoint(
                    &self.client,
                    &format!(
                        "after_{}",
                        request.order_link_id.rsplit('-').next().unwrap_or("fill")
                    ),
                );
                Ok(result)
            }
            Err(error) => {
                if error.order_state_unknown() {
                    self.ledger
                        .order_state(&request.order_link_id, "SUBMISSION_UNKNOWN")
                        .await
                        .map_err(journal_error)?;
                    self.ledger
                        .state("BLOCKED", true, false)
                        .await
                        .map_err(journal_error)?;
                } else {
                    self.ledger
                        .rejected(&request.order_link_id, &error.to_string())
                        .await
                        .map_err(journal_error)?;
                }
                Err(error)
            }
        }
    }
}

#[async_trait]
impl execution::ExecutionJournal for Venue {
    async fn before_submission(
        &self,
        prepared: &PreparedExecution,
        request: &ExecutionOrderRequest,
    ) -> Result<(), ExecutionError> {
        self.gate
            .check(prepared.is_risk_reducing())
            .map_err(|e| ExecutionError::RiskState(e.to_string()))?;
        self.ledger
            .allow_submission(&request.order_link_id, prepared.is_risk_reducing())
            .await
            .map_err(|e| ExecutionError::MissingData(e.to_string()))?;
        crate::checkpoint(&self.client, "before_submission");
        Ok(())
    }
    async fn acknowledged(&self, ack: &execution::PlaceOrderAck) -> Result<(), ExecutionError> {
        self.ledger
            .acknowledged(ack)
            .await
            .map_err(|e| ExecutionError::MissingData(e.to_string()))?;
        crate::checkpoint(&self.client, "after_acknowledgement");
        Ok(())
    }
    async fn cancel_pending(&self, ack: &execution::PlaceOrderAck) -> Result<(), ExecutionError> {
        self.ledger
            .order_state(&ack.order_link_id, "CANCEL_PENDING")
            .await
            .map_err(|e| ExecutionError::MissingData(e.to_string()))?;
        crate::checkpoint(&self.client, "during_cancellation");
        Ok(())
    }
}

pub struct Provider {
    pub ledger: Ledger,
    pub planner: ExchangePlanner,
    pub gate: Gate,
    pub starting: Decimal,
    pub edge: Decimal,
    pub balance_age_ms: u64,
}
#[async_trait]
impl RiskIntentProvider for Provider {
    async fn risk_inputs(
        &mut self,
        c: &LegAuthorizationContext<'_>,
    ) -> Result<(TradeIntent, RiskContext), CoordinatorError> {
        let input: Result<_> = async {
            self.gate.check(false)?;
            paper::redis_ready().await?;
            let p = self.ledger.portfolio(c.now_ms, self.balance_age_ms).await?;
            if !p.enabled {
                bail!("account not authorized");
            }
            let limit = |name: &str| -> Result<Decimal> {
                Ok(Decimal::from_str_exact(
                    p.limits[name].as_str().context("account limit")?,
                )?)
            };
            if p.drawdown >= limit("max_drawdown")?
                || p.losses
                    >= p.limits["max_consecutive_losses"]
                        .as_u64()
                        .context("consecutive loss limit")? as usize
            {
                bail!("account drawdown or loss limit");
            }
            let mut output = c
                .holdings
                .get(&c.planned.conversion.from_asset)
                .copied()
                .context("missing actual holding")?;
            for i in c.leg_index..3 {
                output = self
                    .planner
                    .plan_conversion(
                        c.route,
                        &ConversionLeg::from(&c.route.legs[i]),
                        output,
                        format!("revalidate-{i}"),
                        false,
                    )
                    .await?
                    .estimated_output;
            }
            let cash = if c.leg_index == 0 {
                Decimal::ZERO
            } else {
                c.holdings.get("USDT").copied().unwrap_or_default()
            };
            let edge = ((output + cash - self.starting) / self.starting * Decimal::from(10000))
                .min(self.edge);
            let health = ServiceHealth {
                healthy: true,
                last_ok_ms: c.now_ms,
                detail: "fenced exchange journal, recent reconciled wallet and synchronized books"
                    .into(),
            };
            Ok((
                TradeIntent {
                    trade_id: c.trade_id.into(),
                    route_id: c.route.id.clone(),
                    starting_asset: "USDT".into(),
                    starting_notional: c.planned.estimated_notional_base,
                    projected_peak_exposure: Decimal::ZERO,
                    expected_net_edge_bps: edge,
                    estimated_slippage_bps: c.planned.estimated_slippage_bps,
                    available_liquidity: c.planned.estimated_notional_base,
                    available_liquidity_ratio: c.planned.liquidity_ratio,
                    market_data_timestamp_ms: c.planned.market_timestamp_ms,
                    legs: vec![ProposedOrderLeg {
                        symbol: c.planned.request.symbol.clone(),
                        quantity: c.planned.request.requested_quantity,
                        limit_price: c.planned.request.price,
                        rules: c.planned.rules.clone(),
                    }],
                },
                RiskContext {
                    account_balance: p.available + p.exposure,
                    current_exposure: p.exposure,
                    daily_realized_pnl: p.daily_pnl,
                    api_health: health.clone(),
                    exchange_health: health,
                },
            ))
        }
        .await;
        input.map_err(|e| CoordinatorError::Risk(e.to_string()))
    }
    async fn emergency_health(
        &mut self,
    ) -> Result<(ServiceHealth, ServiceHealth), CoordinatorError> {
        self.gate
            .check(true)
            .map_err(|e| CoordinatorError::Risk(e.to_string()))?;
        self.ledger
            .heartbeat()
            .await
            .map_err(|e| CoordinatorError::Risk(e.to_string()))?;
        let health = ServiceHealth {
            healthy: true,
            last_ok_ms: current_time_ms(),
            detail:
                "fenced recovery, venue independently rechecks exchange evidence before submission"
                    .into(),
        };
        Ok((health.clone(), health))
    }
}

#[cfg(test)]
mod limit_tests {
    use super::*;
    fn d(s: &str) -> Decimal {
        Decimal::from_str_exact(s).unwrap()
    }
    fn instrument() -> paper::Instrument {
        paper::Instrument {
            base: "BTC".into(),
            quote: "USDT".into(),
            step: d("0.00000001"),
            quote_step: d("0.00000001"),
            tick: d("0.01"),
            min_amount: d("0.01"),
            max_quantity: d("1000"),
            max_limit_quantity: Some(d("1000")),
            timestamp: 0,
        }
    }
    #[test]
    fn worst_case_buy_and_input_fee_fit_exact_reserved_capital() {
        let (q, p) = limit_bounds(
            execution::OrderSide::Buy,
            d("100"),
            d("1"),
            d("100"),
            &instrument(),
            d("0.001"),
            d("0.1"),
        )
        .unwrap();
        assert_eq!(p, d("100.10"));
        assert_eq!(q, d("0.99800299"));
        assert!(q * p * d("1.001") <= d("100"));
        assert!((q + d("0.00000001")) * p * d("1.001") > d("100"));
    }
    #[test]
    fn rounding_never_loosens_buy_cap_or_sell_floor() {
        let i = instrument();
        assert_eq!(
            limit_bounds(
                execution::OrderSide::Buy,
                d("100"),
                d("1"),
                d("99.999"),
                &i,
                d("0.001"),
                d("0.1")
            )
            .unwrap()
            .1,
            d("100.09")
        );
        assert_eq!(
            limit_bounds(
                execution::OrderSide::Sell,
                d("1"),
                d("1"),
                d("99.999"),
                &i,
                d("0.001"),
                d("0.1")
            )
            .unwrap()
            .1,
            d("99.90")
        );
    }
    #[test]
    fn limit_quantity_cap_is_distinct_from_market_cap() {
        let mut i = instrument();
        i.max_limit_quantity = Some(d("0.25"));
        assert_eq!(
            limit_bounds(
                execution::OrderSide::Buy,
                d("100"),
                d("1"),
                d("100"),
                &i,
                d("0.001"),
                d("0.1")
            )
            .unwrap()
            .0,
            d("0.25")
        );
        i.max_limit_quantity = None;
        assert!(limit_bounds(
            execution::OrderSide::Buy,
            d("100"),
            d("1"),
            d("100"),
            &i,
            d("0.001"),
            d("0.1")
        )
        .is_err());
    }
    #[test]
    fn invalid_or_below_minimum_limit_fails_closed() {
        for input in [Decimal::ZERO, d("-1"), d("0.00001")] {
            assert!(limit_bounds(
                execution::OrderSide::Buy,
                input,
                d("1"),
                d("100"),
                &instrument(),
                d("0.001"),
                d("0.1")
            )
            .is_err());
        }
        assert!(limit_bounds(
            execution::OrderSide::Buy,
            d("100"),
            d("1"),
            Decimal::MAX,
            &instrument(),
            d("0.001"),
            d("0.1")
        )
        .is_err());
    }
}
