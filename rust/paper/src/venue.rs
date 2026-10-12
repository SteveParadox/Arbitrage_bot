use crate::matching::floor;
use crate::{decimal, match_order, Book, FillResult, Instrument, Ledger, MatchSettings};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use coordinator::{
    ConversionLeg, CoordinatorError, CoordinatorVenue, LegAuthorizationContext, PlannedOrder,
    RiskIntentProvider, RoutePlanner,
};
use execution::{
    ExecutionAttemptError, ExecutionError, ExecutionFill, ExecutionMode, ExecutionOrderRequest,
    ExecutionResult, ExecutionStage, MonitorResult, OrderExecutionState, OrderSide, PlaceOrderAck,
    PreparedExecution,
};
use market_data::model::InstrumentMetadata;
use orderbook::{BookUpdate, OrderBookEngine};
use risk::{
    current_time_ms, ProposedOrderLeg, RiskContext, RiskEngine, ServiceHealth, SymbolRules,
    TradeIntent,
};
use rust_decimal::Decimal;
use scanner::TriangleRoute;
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;

#[derive(Default)]
struct BooksInner {
    engine: OrderBookEngine,
    instruments: HashMap<String, Instrument>,
    consumed: HashMap<String, BTreeMap<String, Decimal>>,
    ready: bool,
    max_skew_ms: Option<u64>,
}
#[derive(Clone, Default)]
pub struct PaperBooks(Arc<Mutex<BooksInner>>);
impl PaperBooks {
    pub async fn reset(&self) {
        let mut b = self.0.lock().await;
        b.engine = OrderBookEngine::default();
        b.instruments.clear();
        b.consumed.clear();
        b.ready = false;
    }
    pub async fn set_skew_limit(&self, limit: u64) -> Result<()> {
        if limit == 0 {
            bail!("invalid market-data skew limit");
        }
        self.0.lock().await.max_skew_ms = Some(limit);
        Ok(())
    }
    pub async fn route_consistent(&self, route: &TriangleRoute) -> Result<()> {
        let b = self.0.lock().await;
        let mut oldest = u64::MAX;
        let mut newest = 0;
        for leg in &route.legs {
            let book = b.engine.top_n(&leg.symbol, 1)?;
            oldest = oldest.min(book.timestamp);
            newest = newest.max(book.timestamp);
        }
        if newest - oldest > b.max_skew_ms.unwrap_or(100) {
            bail!("ROUTE_BOOK_SKEW");
        }
        Ok(())
    }
    pub async fn ready(&self, ready: bool) {
        self.0.lock().await.ready = ready;
    }
    pub async fn instrument(&self, m: &InstrumentMetadata) -> Result<()> {
        if m.status != "Trading" {
            bail!("inactive instrument");
        }
        let mut i = Instrument {
            base: m.base_coin.clone(),
            quote: m.quote_coin.clone(),
            step: decimal(m.qty_step.context("basePrecision missing")?)?,
            quote_step: decimal(m.quote_precision.context("quotePrecision missing")?)?,
            tick: decimal(m.tick_size.context("tickSize missing")?)?,
            min_amount: decimal(m.min_order_amt.context("minOrderAmt missing")?)?,
            max_quantity: decimal(
                m.max_market_order_qty
                    .context("maxMarketOrderQty missing")?,
            )?,
            max_limit_quantity: None,
            timestamp: m.timestamp,
        };
        if let Some(exact) = &m.execution_filters {
            i.step = Decimal::from_str_exact(&exact.base_precision)?;
            i.quote_step = Decimal::from_str_exact(&exact.quote_precision)?;
            i.tick = Decimal::from_str_exact(&exact.tick_size)?;
            i.min_amount = Decimal::from_str_exact(&exact.min_order_amount)?;
            i.max_quantity = Decimal::from_str_exact(&exact.max_market_quantity)?;
            i.max_limit_quantity = exact
                .max_limit_quantity
                .as_deref()
                .map(Decimal::from_str_exact)
                .transpose()?;
        }
        if i.step <= Decimal::ZERO
            || i.quote_step <= Decimal::ZERO
            || i.max_quantity <= Decimal::ZERO
        {
            bail!("invalid filters");
        }
        self.0.lock().await.instruments.insert(m.symbol.clone(), i);
        Ok(())
    }
    pub async fn update(&self, u: BookUpdate) -> Result<()> {
        let symbol = u.symbol.clone();
        let mut b = self.0.lock().await;
        // A delta supplies new evidence only for the price levels it contains.
        // An update to bids must not restore previously consumed asks (or vice versa).
        let refreshed = u
            .bids
            .iter()
            .map(|l| (OrderSide::Sell, l.price))
            .chain(u.asks.iter().map(|l| (OrderSide::Buy, l.price)))
            .map(|(side, price)| Ok(format!("{side:?}:{}", decimal(price)?)))
            .collect::<Result<Vec<_>>>()?;
        let snapshot = u.is_snapshot || u.update_id == 1;
        b.engine.apply(u)?;
        if snapshot {
            b.consumed.remove(&symbol);
        } else if let Some(consumed) = b.consumed.get_mut(&symbol) {
            for key in refreshed {
                consumed.remove(&key);
            }
        }
        Ok(())
    }
    /// One synchronized, read-only book and metadata snapshot for venue planning.
    pub async fn snapshot(&self, symbol: &str, side: OrderSide) -> Result<(Book, Instrument)> {
        let b = self.0.lock().await;
        if !b.ready {
            bail!("scanner/subscription readiness unavailable");
        }
        let v = b.engine.top_n(symbol, usize::MAX)?;
        let version = format!("{symbol}:{}:{}:{}", v.timestamp, v.update_id, v.sequence);
        let consumed = b.consumed.get(symbol);
        let levels = |levels: Vec<orderbook::PriceLevel>| -> Result<Vec<(Decimal, Decimal)>> {
            levels
                .into_iter()
                .map(|l| {
                    let p = decimal(l.price)?;
                    let q = decimal(l.quantity)?;
                    let key = format!("{side:?}:{p}");
                    Ok((
                        p,
                        (q - consumed
                            .and_then(|c| c.get(&key))
                            .copied()
                            .unwrap_or_default())
                        .max(Decimal::ZERO),
                    ))
                })
                .collect()
        };
        Ok((
            Book {
                bids: levels(v.bids)?,
                asks: levels(v.asks)?,
                timestamp: v.timestamp,
                version,
            },
            b.instruments
                .get(symbol)
                .context("metadata absent")?
                .clone(),
        ))
    }
    async fn consume(&self, book: &Book, side: OrderSide, result: &FillResult) {
        let mut b = self.0.lock().await;
        let symbol = book.version.split(':').next().unwrap_or_default();
        let Some(instrument) = b.instruments.get(symbol) else {
            return;
        };
        let step = instrument.step;
        let c = b.consumed.entry(symbol.into()).or_default();
        let mut left = result.quantity;
        let levels = if side == OrderSide::Buy {
            &book.asks
        } else {
            &book.bids
        };
        for (p, q) in levels {
            // Matching executes complete lots; substep liquidity was never filled.
            let take = floor(left.min(*q), step);
            *c.entry(format!("{side:?}:{p}")).or_default() += take;
            left -= take;
            if left == Decimal::ZERO {
                break;
            }
        }
    }
}

#[derive(Clone)]
pub struct PaperPlanner {
    pub books: PaperBooks,
    pub settings: MatchSettings,
}
#[async_trait]
impl RoutePlanner for PaperPlanner {
    async fn plan_conversion(
        &self,
        route: &TriangleRoute,
        conversion: &ConversionLeg,
        input: Decimal,
        order_link_id: String,
        emergency: bool,
    ) -> Result<PlannedOrder, CoordinatorError> {
        self.plan(route, conversion, input, order_link_id, emergency)
            .await
            .map_err(|e| CoordinatorError::Planning(e.to_string()))
    }
    async fn value_in_base(
        &self,
        route: &TriangleRoute,
        asset: &str,
        amount: Decimal,
    ) -> Result<Decimal, CoordinatorError> {
        self.value(route, asset, amount)
            .await
            .map_err(|e| CoordinatorError::Planning(e.to_string()))
    }
}
impl PaperPlanner {
    async fn value(&self, route: &TriangleRoute, asset: &str, amount: Decimal) -> Result<Decimal> {
        if asset == route.start_asset || amount == Decimal::ZERO {
            return Ok(amount);
        }
        let first = ConversionLeg::from(&route.legs[0]);
        let last = ConversionLeg::from(&route.legs[2]);
        let conversion = if asset == first.to_asset {
            first.reversed()
        } else if asset == last.from_asset {
            last
        } else {
            bail!("no residual valuation path");
        };
        let (b, i) = self
            .books
            .snapshot(&conversion.symbol, conversion.order_side())
            .await?;
        let quantity = quantity_for(&b, &i, conversion.order_side(), amount)?;
        if quantity == Decimal::ZERO {
            return Ok(Decimal::ZERO);
        }
        let r = match_order(
            &b,
            &i,
            conversion.order_side(),
            quantity,
            amount,
            &self.settings,
            current_time_ms(),
        )?;
        if r.remaining > Decimal::ZERO || r.quantity == Decimal::ZERO {
            bail!("residual cannot be valued at executable depth");
        }
        Ok(if conversion.order_side() == OrderSide::Buy {
            r.quantity - r.fee
        } else {
            r.quote_value - r.fee
        })
    }
    pub async fn valuation(
        &self,
        route: &TriangleRoute,
        asset: &str,
        amount: Decimal,
    ) -> Result<(Decimal, serde_json::Value)> {
        let first = ConversionLeg::from(&route.legs[0]);
        let last = ConversionLeg::from(&route.legs[2]);
        let conversion = if asset == first.to_asset {
            first.reversed()
        } else if asset == last.from_asset {
            last
        } else {
            bail!("missing supported valuation market");
        };
        let (book, instrument) = self
            .books
            .snapshot(&conversion.symbol, conversion.order_side())
            .await?;
        let quantity = quantity_for(&book, &instrument, conversion.order_side(), amount)?;
        if quantity == Decimal::ZERO {
            bail!("residual is below executable quantity precision");
        }
        let result = match_order(
            &book,
            &instrument,
            conversion.order_side(),
            quantity,
            amount,
            &self.settings,
            current_time_ms(),
        )?;
        if result.remaining > Decimal::ZERO || result.quantity == Decimal::ZERO {
            bail!("insufficient executable valuation depth");
        }
        let value = if conversion.order_side() == OrderSide::Buy {
            result.quantity - result.fee
        } else {
            result.quote_value - result.fee
        };
        Ok((
            value,
            json!({"symbol":conversion.symbol,"market_timestamp_ms":book.timestamp,"book_version":book.version,"valuation_source":if std::env::var("ARB_OBSERVER_REPLAY_FILE").is_ok(){"synthetic_replay_executable_depth"}else{"bybit_public_executable_depth"},"modeled_liquidation_fees_included":true}),
        ))
    }
    async fn plan(
        &self,
        route: &TriangleRoute,
        conversion: &ConversionLeg,
        input: Decimal,
        id: String,
        emergency: bool,
    ) -> Result<PlannedOrder> {
        if !emergency {
            self.books.route_consistent(route).await?;
        }
        let side = conversion.order_side();
        let (b, i) = self.books.snapshot(&conversion.symbol, side).await?;
        if i.base != conversion.base_asset || i.quote != conversion.quote_asset {
            bail!("catalog metadata mismatch");
        }
        let q = quantity_for(&b, &i, side, input)?.min(i.max_quantity);
        let r = match_order(&b, &i, side, q, input, &self.settings, current_time_ms())?;
        if r.quantity == Decimal::ZERO || (!emergency && r.remaining > Decimal::ZERO) {
            bail!("insufficient executable depth or minimum order amount");
        }
        let input_spent = if side == OrderSide::Buy {
            r.quote_value
        } else {
            r.quantity
        };
        let notional = self
            .value(route, &conversion.from_asset, input_spent)
            .await?;
        let best = if side == OrderSide::Buy {
            b.asks.first()
        } else {
            b.bids.first()
        }
        .context("empty book")?
        .0;
        let avg = r.price.context("no fill price")?;
        let slip = (avg - best).abs() / best * Decimal::from(10000);
        Ok(PlannedOrder {
            conversion: conversion.clone(),
            input_amount: input,
            estimated_output: if side == OrderSide::Buy {
                r.quantity - r.fee
            } else {
                r.quote_value - r.fee
            },
            estimated_notional_base: notional,
            estimated_slippage_bps: slip,
            liquidity_ratio: Decimal::ONE,
            market_timestamp_ms: b.timestamp,
            rules: SymbolRules {
                qty_step: i.step,
                min_order_qty: i.step,
                tick_size: i.tick,
            },
            request: ExecutionOrderRequest {
                symbol: conversion.symbol.clone(),
                side,
                order_type: execution::OrderType::Market,
                requested_quantity: r.quantity,
                estimated_notional: notional,
                price: None,
                time_in_force: execution::TimeInForce::Ioc,
                order_link_id: id,
                slippage_tolerance_percent: Some(self.settings.slippage_percent),
            },
        })
    }
}
fn quantity_for(
    book: &Book,
    instrument: &Instrument,
    side: OrderSide,
    input: Decimal,
) -> Result<Decimal> {
    if input <= Decimal::ZERO {
        bail!("no input holding");
    }
    if side == OrderSide::Sell {
        return Ok(floor(input, instrument.step));
    }
    let mut left = input;
    let mut q = Decimal::ZERO;
    for (p, available) in &book.asks {
        let take = floor((left / *p).min(*available), instrument.step);
        q += take;
        left -= take * *p;
    }
    Ok(q)
}

/// The existing global emergency stop also applies between paper legs.
#[derive(Clone)]
pub struct StopGuard {
    pub control: PathBuf,
    pub kill: PathBuf,
}
impl StopGuard {
    pub fn check(&self) -> Result<()> {
        let control = risk::control::read_control(&self.control)?;
        let stop = risk::control::read_stop_intent(&self.control)?;
        if risk::control::stop_pending(&self.control)
            || self.kill.exists()
            || control
                .as_ref()
                .is_some_and(|c| !c.enabled && c.request_id.is_some())
            || (stop.is_some() && !control.as_ref().is_some_and(|c| c.enabled))
        {
            bail!("GLOBAL_STOP_OR_KILL_SWITCH");
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct PaperVenue {
    pub cycle_id: String,
    pub route: TriangleRoute,
    pub stop_guard: StopGuard,
    pub books: PaperBooks,
    pub ledger: Ledger,
    pub settings: MatchSettings,
}
#[async_trait]
impl CoordinatorVenue for PaperVenue {
    fn is_paper(&self) -> bool {
        true
    }
    async fn execute(
        &self,
        risk: &mut RiskEngine,
        prepared: &PreparedExecution,
        request: &ExecutionOrderRequest,
    ) -> Result<ExecutionResult, ExecutionAttemptError> {
        prepared
            .revalidate(risk, current_time_ms())
            .map_err(|e| ExecutionAttemptError {
                stage: ExecutionStage::Submission,
                place_ack: None,
                source: ExecutionError::RiskState(e.to_string()),
            })?;
        if !prepared.is_risk_reducing() {
            self.books
                .route_consistent(&self.route)
                .await
                .map_err(|e| ExecutionAttemptError {
                    stage: ExecutionStage::Submission,
                    place_ack: None,
                    source: ExecutionError::RiskState(e.to_string()),
                })?;
            self.stop_guard.check().map_err(|e| ExecutionAttemptError {
                stage: ExecutionStage::Submission,
                place_ack: None,
                source: ExecutionError::RiskState(e.to_string()),
            })?;
            crate::redis_ready()
                .await
                .map_err(|e| ExecutionAttemptError {
                    stage: ExecutionStage::Submission,
                    place_ack: None,
                    source: ExecutionError::EventPipeline(e.to_string()),
                })?;
        }
        self.submit(risk, prepared, request)
            .await
            .map_err(|e| ExecutionAttemptError {
                stage: ExecutionStage::Monitoring,
                place_ack: None,
                source: ExecutionError::RiskState(e.to_string()),
            })
    }
}
impl PaperVenue {
    async fn submit(
        &self,
        risk: &mut RiskEngine,
        p: &PreparedExecution,
        r: &ExecutionOrderRequest,
    ) -> Result<ExecutionResult> {
        p.revalidate(risk, current_time_ms())?;
        if p.trade_id() != self.cycle_id
            && !(p.is_risk_reducing()
                && p.trade_id()
                    .starts_with(&format!("{}:unwind:", self.cycle_id)))
        {
            bail!("risk identity does not belong to bound paper cycle");
        }
        if p.mode() != ExecutionMode::Paper {
            bail!("paper venue refuses nonpaper token");
        }
        if r.order_type != execution::OrderType::Market
            || r.time_in_force != execution::TimeInForce::Ioc
        {
            bail!("paper venue supports market IOC only");
        }
        let (submission_book, instrument) = self.books.snapshot(&r.symbol, r.side).await?;
        let (from, to) = if r.side == OrderSide::Buy {
            (&instrument.quote, &instrument.base)
        } else {
            (&instrument.base, &instrument.quote)
        };
        let risk_reducing = p.is_risk_reducing();
        let leg_number = r
            .order_link_id
            .rsplit_once("-l")
            .map(|(_, suffix)| suffix)
            .and_then(|suffix| suffix.chars().next())
            .and_then(|digit| digit.to_digit(10));
        let intent = json!({"cycle_id":self.cycle_id,"leg_number":leg_number,"order_role":if risk_reducing {"recovery_or_close"} else {"normal"},"symbol":r.symbol,"side":r.side,"quantity":r.requested_quantity.to_string(),"from":from,"to":to,"risk_reducing":risk_reducing,"simulated":true,"submission_book":{"timestamp":submission_book.timestamp,"version":submission_book.version,"bids":submission_book.bids,"asks":submission_book.asks}});
        if let Some(leg) = leg_number {
            crate::crash_checkpoint(&format!("before_leg_{leg}_intent"));
        }
        let old = self
            .ledger
            .order_intent(&self.cycle_id, &r.order_link_id, &intent)
            .await?;
        crate::crash_checkpoint("after_intent");
        if let Some(leg) = leg_number {
            crate::crash_checkpoint(&format!("after_leg_{leg}_intent"));
        }
        let result = if let Some(old) = old {
            old
        } else {
            runtime_clock::sleep(Duration::from_millis(
                self.settings.latency_ms.min(self.settings.timeout_ms),
            ))
            .await;
            if let Some(leg) = leg_number {
                crate::crash_checkpoint(&format!("during_leg_{leg}_matching"));
            }
            // Use a fresh synchronized snapshot at fill time; receiver remains running during latency.
            let (mut book, instrument) = self.books.snapshot(&r.symbol, r.side).await?;
            let mut matching_settings = self.settings.clone();
            matching_settings.slippage_percent = r
                .slippage_tolerance_percent
                .unwrap_or(self.settings.slippage_percent);
            let original_best = if r.side == OrderSide::Buy {
                submission_book.asks.first()
            } else {
                submission_book.bids.first()
            }
            .context("missing submission reference")?
            .0;
            let tolerance = matching_settings.slippage_percent / Decimal::from(100);
            // The IOC price cap stays tied to submission evidence, including movement during latency.
            if r.side == OrderSide::Buy {
                book.asks
                    .retain(|(price, _)| *price <= original_best * (Decimal::ONE + tolerance));
            } else {
                book.bids
                    .retain(|(price, _)| *price >= original_best * (Decimal::ONE - tolerance));
            }
            let budget = self.ledger.budget(&self.cycle_id, from).await?;
            let mut result = match_order(
                &book,
                &instrument,
                r.side,
                r.requested_quantity,
                budget,
                &matching_settings,
                current_time_ms(),
            )?;
            if result.quantity > Decimal::ZERO {
                let reference = match_order(
                    &submission_book,
                    &instrument,
                    r.side,
                    result.quantity,
                    budget,
                    &matching_settings,
                    submission_book.timestamp,
                )
                .ok()
                .and_then(|f| f.price);
                result.submission_reference_price = reference;
                result.execution_slippage_bps =
                    reference.zip(result.price).map(|(reference, filled)| {
                        if r.side == OrderSide::Buy {
                            (filled - reference) / reference * Decimal::from(10000)
                        } else {
                            (reference - filled) / reference * Decimal::from(10000)
                        }
                    });
            } else if (r.side == OrderSide::Buy && book.asks.is_empty())
                || (r.side == OrderSide::Sell && book.bids.is_empty())
            {
                result.reason = Some("SUBMISSION_SLIPPAGE_CAP_OR_NO_LIQUIDITY".into());
            }
            if !risk_reducing && self.books.route_consistent(&self.route).await.is_err() {
                result.quantity = Decimal::ZERO;
                result.quote_rounding_dust = Decimal::ZERO;
                result.quote_value = Decimal::ZERO;
                result.fee = Decimal::ZERO;
                result.remaining = result.requested;
                result.price = None;
                result.submission_reference_price = None;
                result.execution_slippage_bps = None;
                result.status = "Rejected".into();
                result.reason = Some("ROUTE_BOOK_SKEW".into());
            }
            if self.settings.latency_ms >= self.settings.timeout_ms {
                result.quantity = Decimal::ZERO;
                result.quote_rounding_dust = Decimal::ZERO;
                result.quote_value = Decimal::ZERO;
                result.fee = Decimal::ZERO;
                result.remaining = result.requested;
                result.price = None;
                result.status = "Cancelled".into();
                result.reason = Some("ORDER_TIMEOUT_NO_FILL".into());
            }
            let (spent, gross) = if r.side == OrderSide::Buy {
                (result.quote_value, result.quantity)
            } else {
                (result.quantity, result.quote_value)
            };
            self.ledger
                .fill(
                    &self.cycle_id,
                    &r.order_link_id,
                    from,
                    to,
                    spent,
                    gross,
                    &result,
                )
                .await?;
            self.books.consume(&book, r.side, &result).await;
            if r.order_link_id.contains("-u") {
                crate::crash_checkpoint("after_unwind_fill");
            }
            if std::env::var("ARB_ENV").as_deref() == Ok("development")
                && std::env::var("ARB_PAPER_CRASH_AFTER_LEG")
                    .ok()
                    .is_some_and(|leg| r.order_link_id.ends_with(&format!("-l{leg}0")))
            {
                std::process::exit(88);
            }
            result
        };
        let fills = if result.quantity > Decimal::ZERO {
            vec![ExecutionFill {
                execution_id: format!("paper:{}:fill", r.order_link_id),
                order_id: r.order_link_id.clone(),
                quantity: result.quantity,
                price: result.price.context("missing execution price")?,
                value: result.quote_value,
                fee: result.fee,
                fee_currency: result.fee_currency.clone(),
                is_maker: false,
                executed_at_ms: result.executed_at_ms,
            }]
        } else {
            vec![]
        };
        let mut fees = BTreeMap::new();
        if result.fee > Decimal::ZERO {
            fees.insert(result.fee_currency.clone(), result.fee);
        }
        Ok(ExecutionResult {
            place_ack: PlaceOrderAck {
                order_id: r.order_link_id.clone(),
                order_link_id: r.order_link_id.clone(),
                accepted_at_ms: result.executed_at_ms,
            },
            monitor: MonitorResult {
                timed_out: result.reason.as_deref() == Some("ORDER_TIMEOUT_NO_FILL"),
                state: OrderExecutionState {
                    order_id: r.order_link_id.clone(),
                    order_link_id: r.order_link_id.clone(),
                    symbol: r.symbol.clone(),
                    side: Some(r.side),
                    status: result.status,
                    requested_quantity: result.requested,
                    filled_quantity: result.quantity,
                    remaining_quantity: result.remaining,
                    average_fill_price: result.price,
                    fees,
                    fills,
                    terminal: true,
                    fully_filled: result.remaining == Decimal::ZERO,
                    fills_confirmed: true,
                    reject_reason: result.reason,
                    updated_at_ms: result.executed_at_ms,
                },
            },
            cancellation: None,
        })
    }
}

pub struct Provider {
    pub ledger: Ledger,
    pub edge: Decimal,
    pub starting: Decimal,
    pub planner: PaperPlanner,
    pub stop_guard: StopGuard,
    pub portfolio_limits: risk::PortfolioLimits,
}
#[async_trait]
impl RiskIntentProvider for Provider {
    async fn risk_inputs(
        &mut self,
        c: &LegAuthorizationContext<'_>,
    ) -> Result<(TradeIntent, RiskContext), CoordinatorError> {
        if let Err(e) = self.stop_guard.check() {
            self.ledger
                .disable()
                .await
                .map_err(|e| CoordinatorError::Risk(e.to_string()))?;
            return Err(CoordinatorError::Risk(e.to_string()));
        }
        crate::redis_ready()
            .await
            .map_err(|e| CoordinatorError::Risk(e.to_string()))?;
        let portfolio = self
            .ledger
            .portfolio(&c.route.start_asset)
            .await
            .map_err(|e| CoordinatorError::Risk(e.to_string()))?;
        if !portfolio.enabled || portfolio.blocked {
            return Err(CoordinatorError::Risk(
                "paper authorization disabled".into(),
            ));
        }
        // Reprice remaining legs using actual holdings; residual dust is valued at zero here.
        // This conservative gate can reject a deteriorated edge before adding new exposure.
        let mut remaining = *c
            .holdings
            .get(&c.planned.conversion.from_asset)
            .unwrap_or(&Decimal::ZERO);
        let mut peak_exposure = c.planned.estimated_notional_base;
        for index in c.leg_index..3 {
            let plan = self
                .planner
                .plan_conversion(
                    c.route,
                    &ConversionLeg::from(&c.route.legs[index]),
                    remaining,
                    format!("revalidate-{index}"),
                    false,
                )
                .await?;
            peak_exposure = peak_exposure.max(plan.estimated_notional_base);
            remaining = plan.estimated_output;
        }
        let stats = self
            .ledger
            .portfolio_stats(peak_exposure)
            .await
            .map_err(|e| CoordinatorError::Risk(e.to_string()))?;
        let checks = risk::portfolio_checks(&self.portfolio_limits, &stats);
        if checks.iter().any(|check| !check.passed) {
            return Err(CoordinatorError::Risk(json!(checks).to_string()));
        }
        let cash = if c.leg_index == 0 {
            Decimal::ZERO
        } else {
            *c.holdings
                .get(&c.route.start_asset)
                .unwrap_or(&Decimal::ZERO)
        };
        let remaining_edge =
            (remaining + cash - self.starting) / self.starting * Decimal::from(10000);
        let health = ServiceHealth {
            healthy: true,
            last_ok_ms: c.now_ms,
            detail: "PostgreSQL paper ledger and synchronized public books".into(),
        };
        Ok((
            TradeIntent {
                trade_id: c.trade_id.into(),
                route_id: c.route.id.clone(),
                starting_asset: c.route.start_asset.clone(),
                starting_notional: c.planned.estimated_notional_base,
                projected_peak_exposure: (peak_exposure - portfolio.exposure).max(Decimal::ZERO),
                expected_net_edge_bps: self.edge.min(remaining_edge),
                estimated_slippage_bps: c.planned.estimated_slippage_bps,
                available_liquidity: c.planned.estimated_notional_base,
                available_liquidity_ratio: c.planned.liquidity_ratio,
                market_data_timestamp_ms: c.planned.market_timestamp_ms,
                legs: vec![ProposedOrderLeg {
                    symbol: c.planned.request.symbol.clone(),
                    quantity: c.planned.request.requested_quantity,
                    limit_price: None,
                    rules: c.planned.rules.clone(),
                }],
            },
            RiskContext {
                account_balance: portfolio.available + portfolio.exposure,
                current_exposure: portfolio.exposure,
                daily_realized_pnl: portfolio.daily_pnl,
                api_health: health.clone(),
                exchange_health: health,
            },
        ))
    }
    async fn emergency_health(
        &mut self,
    ) -> Result<(ServiceHealth, ServiceHealth), CoordinatorError> {
        self.ledger
            .heartbeat()
            .await
            .map_err(|e| CoordinatorError::Risk(e.to_string()))?;
        let h = ServiceHealth {
            healthy: true,
            last_ok_ms: current_time_ms(),
            detail: "paper ledger connected".into(),
        };
        Ok((h.clone(), h))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn settings() -> MatchSettings {
        MatchSettings {
            fee_rate: Decimal::ZERO,
            latency_ms: 0,
            max_age_ms: 1000,
            timeout_ms: 1000,
            slippage_percent: Decimal::from(20),
        }
    }
    async fn books(asks: Vec<(f64, f64)>) -> PaperBooks {
        let books = PaperBooks::default();
        books.0.lock().await.instruments.insert(
            "TEST".into(),
            Instrument {
                base: "B".into(),
                quote: "A".into(),
                step: Decimal::new(1, 3),
                quote_step: Decimal::new(1, 5),
                tick: Decimal::ONE,
                min_amount: Decimal::ZERO,
                max_quantity: Decimal::from(100),
                max_limit_quantity: None,
                timestamp: 1000,
            },
        );
        books
            .update(BookUpdate {
                symbol: "TEST".into(),
                bids: vec![orderbook::PriceLevel {
                    price: 9.0,
                    quantity: 10.0,
                }],
                asks: asks
                    .into_iter()
                    .map(|(price, quantity)| orderbook::PriceLevel { price, quantity })
                    .collect(),
                timestamp: 1000,
                update_id: 1,
                sequence: 1,
                is_snapshot: true,
            })
            .await
            .unwrap();
        books.ready(true).await;
        books
    }
    #[tokio::test]
    async fn unrelated_delta_does_not_replenish_consumed_liquidity() {
        let books = books(vec![(10.0, 2.0), (11.0, 4.0)]).await;
        let (book, instrument) = books.snapshot("TEST", OrderSide::Buy).await.unwrap();
        let fill = match_order(
            &book,
            &instrument,
            OrderSide::Buy,
            Decimal::ONE,
            Decimal::from(100),
            &settings(),
            1000,
        )
        .unwrap();
        books.consume(&book, OrderSide::Buy, &fill).await;
        books
            .update(BookUpdate {
                symbol: "TEST".into(),
                bids: vec![orderbook::PriceLevel {
                    price: 9.0,
                    quantity: 3.0,
                }],
                asks: vec![],
                timestamp: 1001,
                update_id: 2,
                sequence: 2,
                is_snapshot: false,
            })
            .await
            .unwrap();
        let (book, _) = books.snapshot("TEST", OrderSide::Buy).await.unwrap();
        assert_eq!(book.asks[0].1, Decimal::ONE);
    }
    #[tokio::test]
    async fn consumption_uses_executed_lots_not_substep_dust() {
        let books = books(vec![(10.0, 0.0005), (11.0, 2.0)]).await;
        let (book, instrument) = books.snapshot("TEST", OrderSide::Buy).await.unwrap();
        let fill = match_order(
            &book,
            &instrument,
            OrderSide::Buy,
            Decimal::new(1, 3),
            Decimal::from(100),
            &settings(),
            1000,
        )
        .unwrap();
        assert_eq!(fill.price, Some(Decimal::from(11)));
        books.consume(&book, OrderSide::Buy, &fill).await;
        let (book, _) = books.snapshot("TEST", OrderSide::Buy).await.unwrap();
        assert_eq!(book.asks[0].1, Decimal::new(5, 4));
        assert_eq!(book.asks[1].1, Decimal::new(1999, 3));
    }
    #[test]
    fn confirmed_global_stop_remains_a_paper_exposure_gate() {
        let control =
            std::env::temp_dir().join(format!("paper-stop-test-{}.json", std::process::id()));
        let stop = control.with_extension("stop.json");
        std::fs::write(&stop,json!({"version":1,"request_id":"confirmed-stop-test","reason":"test confirmed stop","status":"CONFIRMED_STOPPED","updated_at":"2000-01-01T00:00:00Z"}).to_string()).unwrap();
        let guard = StopGuard {
            control: control.clone(),
            kill: control.with_extension("kill"),
        };
        assert!(!risk::control::stop_pending(&control));
        assert!(guard.check().is_err());
        std::fs::remove_file(stop).unwrap();
    }
}
