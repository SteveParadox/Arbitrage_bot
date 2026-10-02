use std::collections::{BTreeSet, HashMap, VecDeque};

use market_data::model::InstrumentMetadata;
use orderbook::{BookUpdate, LocalOrderBook, OrderBookEngine};
use risk::{
    ProposedOrderLeg, RiskContext, RiskEngine, RiskCheckResult, ServiceHealth,
    SymbolRules, TradeIntent,
};
use rust_decimal::Decimal;
use scanner::{
    ArbitrageScanRecord, ArbitrageScanner, ProfitabilityConfig, ScanStatus,
    ScannerSettings, TradeSide, TriangleConfig, TriangleRoute,
};
use sha2::{Digest, Sha256};

use crate::{
    ReadOnlyAccountSnapshot, ShadowConfig, ShadowError, ShadowEvent,
    ShadowLatencySample, ShadowOpportunity,
};

struct AccountState {
    snapshot: ReadOnlyAccountSnapshot,
    baseline_equity_usd: Decimal,
    last_ok_ms: u64,
    healthy: bool,
    detail: String,
}

struct ExchangeState {
    last_ok_ms: u64,
    healthy: bool,
    detail: String,
}

#[derive(Clone)]
struct HistoricalBook {
    received_at_ms: u64,
    book: LocalOrderBook,
}

struct PendingOpportunity {
    observation_id: String,
    route_id: String,
    start_amount: Decimal,
    detected_at_ms: u64,
    latency_neutral_detection_profit: Decimal,
    detection_final_amount: Decimal,
    detection_leg_prices: Vec<Option<f64>>,
    remaining_latencies: BTreeSet<u64>,
}

pub struct ShadowEngine {
    config: ShadowConfig,
    run_id: String,
    scanner: ArbitrageScanner,
    routes: HashMap<String, TriangleRoute>,
    profitability_latency_neutral: ProfitabilityConfig,
    risk_engine: RiskEngine,
    books: OrderBookEngine,
    history: HashMap<String, VecDeque<HistoricalBook>>,
    symbol_rules: HashMap<String, SymbolRules>,
    account: Option<AccountState>,
    exchange: ExchangeState,
    pending: VecDeque<PendingOpportunity>,
    observed_count: usize,
    sampled_observation_count: usize,
    approved_count: usize,
    would_execute_count: usize,
    readiness_emitted: bool,
}

impl ShadowEngine {
    pub fn new(
        config: ShadowConfig,
        run_id: String,
        triangle_config: TriangleConfig,
        scanner_settings: ScannerSettings,
        profitability: ProfitabilityConfig,
        risk_engine: RiskEngine,
    ) -> Result<Self, ShadowError> {
        config.validate()?;
        triangle_config
            .validate()
            .map_err(|error| ShadowError::Engine(error.to_string()))?;
        if triangle_config.routes.is_empty() {
            return Err(ShadowError::Engine(
                concat!(
                    "triangle configuration is empty; generate current ",
                    "mainnet spot routes before shadow mode"
                )
                .to_string(),
            ));
        }
        if triangle_config.source.testnet {
            return Err(ShadowError::Engine(
                "shadow mode requires a mainnet triangle configuration".to_string(),
            ));
        }

        let routes = triangle_config
            .routes
            .iter()
            .cloned()
            .map(|route| (route.id.clone(), route))
            .collect::<HashMap<_, _>>();
        let scanner = ArbitrageScanner::new(
            triangle_config,
            scanner_settings,
            profitability.clone(),
        )
        .map_err(ShadowError::Engine)?;

        let mut profitability_latency_neutral = profitability;
        profitability_latency_neutral.latency_buffer_bps = Decimal::ZERO;

        Ok(Self {
            config,
            run_id,
            scanner,
            routes,
            profitability_latency_neutral,
            risk_engine,
            books: OrderBookEngine::default(),
            history: HashMap::new(),
            symbol_rules: HashMap::new(),
            account: None,
            exchange: ExchangeState {
                last_ok_ms: 0,
                healthy: false,
                detail: "waiting for mainnet market data".to_string(),
            },
            pending: VecDeque::new(),
            observed_count: 0,
            sampled_observation_count: 0,
            approved_count: 0,
            would_execute_count: 0,
            readiness_emitted: false,
        })
    }

    pub fn run_started_event(&self, started_at_ms: u64) -> ShadowEvent {
        ShadowEvent::RunStarted {
            run_id: self.run_id.clone(),
            started_at_ms,
            base_asset: self.config.base_asset.clone(),
            latency_ms: self.config.latency_ms.clone(),
            minimum_observations: self.config.minimum_observations,
            no_order_endpoints: true,
            mainnet_market_data: true,
            mainnet_read_only_account: true,
        }
    }

    pub fn update_account(
        &mut self,
        snapshot: ReadOnlyAccountSnapshot,
        received_at_ms: u64,
    ) -> ShadowEvent {
        let baseline = self
            .account
            .as_ref()
            .map(|state| state.baseline_equity_usd)
            .unwrap_or(snapshot.total_equity_usd);
        let pnl_proxy = snapshot.total_equity_usd - baseline;
        self.account = Some(AccountState {
            snapshot: snapshot.clone(),
            baseline_equity_usd: baseline,
            last_ok_ms: received_at_ms,
            healthy: true,
            detail: "mainnet wallet balance synchronized".to_string(),
        });

        ShadowEvent::AccountSnapshot {
            run_id: self.run_id.clone(),
            synchronized_at_ms: received_at_ms,
            healthy: true,
            detail: "mainnet wallet balance synchronized".to_string(),
            base_available: Some(decimal_text(snapshot.base_available)),
            total_equity_usd: Some(decimal_text(snapshot.total_equity_usd)),
            non_base_exposure_usd: Some(decimal_text(
                snapshot.non_base_exposure_usd,
            )),
            session_pnl_proxy_usd: Some(decimal_text(pnl_proxy)),
        }
    }

    pub fn mark_account_error(
        &mut self,
        now_ms: u64,
        detail: String,
    ) -> ShadowEvent {
        if let Some(state) = self.account.as_mut() {
            state.healthy = false;
            state.detail = detail.clone();
        }
        ShadowEvent::AccountSnapshot {
            run_id: self.run_id.clone(),
            synchronized_at_ms: now_ms,
            healthy: false,
            detail,
            base_available: self
                .account
                .as_ref()
                .map(|state| decimal_text(state.snapshot.base_available)),
            total_equity_usd: self
                .account
                .as_ref()
                .map(|state| decimal_text(state.snapshot.total_equity_usd)),
            non_base_exposure_usd: self
                .account
                .as_ref()
                .map(|state| decimal_text(state.snapshot.non_base_exposure_usd)),
            session_pnl_proxy_usd: self.account.as_ref().map(|state| {
                decimal_text(
                    state.snapshot.total_equity_usd - state.baseline_equity_usd,
                )
            }),
        }
    }

    pub fn mark_exchange_event(&mut self, received_at_ms: u64) {
        self.exchange.last_ok_ms = received_at_ms;
        self.exchange.healthy = true;
        self.exchange.detail = "mainnet public market stream active".to_string();
    }

    pub fn mark_exchange_status(
        &mut self,
        healthy: bool,
        received_at_ms: u64,
        detail: String,
    ) {
        self.exchange.healthy = healthy;
        self.exchange.detail = detail;
        if healthy {
            self.exchange.last_ok_ms = received_at_ms;
        }
    }

    pub fn update_instrument(
        &mut self,
        instrument: &InstrumentMetadata,
    ) -> Result<(), ShadowError> {
        let (Some(tick), Some(step), Some(min_qty)) = (
            instrument.tick_size,
            instrument.qty_step,
            instrument.min_order_qty,
        ) else {
            return Ok(());
        };

        let tick_size = decimal_from_f64("tick_size", tick)?;
        let qty_step = decimal_from_f64("qty_step", step)?;
        let min_order_qty = decimal_from_f64("min_order_qty", min_qty)?;
        if tick_size <= Decimal::ZERO
            || qty_step <= Decimal::ZERO
            || min_order_qty <= Decimal::ZERO
        {
            return Ok(());
        }

        self.symbol_rules.insert(
            instrument.symbol.clone(),
            SymbolRules {
                qty_step,
                min_order_qty,
                tick_size,
            },
        );
        Ok(())
    }

    pub fn on_book_update(
        &mut self,
        update: BookUpdate,
        received_at_ms: u64,
    ) -> Result<Vec<ShadowEvent>, ShadowError> {
        self.mark_exchange_event(received_at_ms);
        self.apply_history(update.clone(), received_at_ms)?;

        let records = self
            .scanner
            .on_book_update(update)
            .map_err(|error| ShadowError::Engine(error.to_string()))?;

        let mut events = Vec::new();
        for record in records {
            if record.status != ScanStatus::Complete
                || record.gross_profitable != Some(true)
            {
                continue;
            }
            if record.start_asset != self.config.base_asset {
                continue;
            }

            let route = match self.routes.get(&record.route_id) {
                Some(route) => route.clone(),
                None => continue,
            };
            let observation = self.build_observation(&route, &record)?;
            if observation.approved {
                self.approved_count += 1;
            }
            if observation.would_execute {
                self.would_execute_count += 1;
            }
            self.observed_count += 1;

            let tracked = observation.latency_tracking;
            let observation_id = observation.observation_id.clone();
            let start_amount = decimal_from_option_f64(
                "start_amount",
                record.start_amount,
            )?;
            let neutral_profit = self.latency_neutral_profit(&record)?;

            events.push(ShadowEvent::Opportunity {
                observation,
            });

            if tracked {
                self.pending.push_back(PendingOpportunity {
                    observation_id,
                    route_id: route.id.clone(),
                    start_amount,
                    detected_at_ms: record.scan_timestamp,
                    latency_neutral_detection_profit: neutral_profit,
                    detection_final_amount: decimal_from_option_f64(
                        "final_amount",
                        record.final_amount,
                    )?,
                    detection_leg_prices: record
                        .legs
                        .iter()
                        .map(|leg| leg.execution.average_execution_price)
                        .collect(),
                    remaining_latencies: self
                        .config
                        .latency_ms
                        .iter()
                        .copied()
                        .collect(),
                });
            }
        }

        Ok(events)
    }

    pub fn sample_due(
        &mut self,
        now_ms: u64,
    ) -> Result<Vec<ShadowEvent>, ShadowError> {
        let mut events = Vec::new();
        let mut keep = VecDeque::new();

        while let Some(mut pending) = self.pending.pop_front() {
            let due = pending
                .remaining_latencies
                .iter()
                .copied()
                .filter(|latency| {
                    pending.detected_at_ms.saturating_add(*latency) <= now_ms
                })
                .collect::<Vec<_>>();

            for latency_ms in due {
                let target_at_ms =
                    pending.detected_at_ms.saturating_add(latency_ms);
                let sample = self.build_latency_sample(
                    &pending,
                    latency_ms,
                    target_at_ms,
                    now_ms,
                );
                pending.remaining_latencies.remove(&latency_ms);
                events.push(ShadowEvent::LatencySample { sample });
            }

            if pending.remaining_latencies.is_empty() {
                self.sampled_observation_count += 1;
            } else {
                keep.push_back(pending);
            }
        }

        self.pending = keep;

        if !self.readiness_emitted
            && self.sampled_observation_count >= self.config.minimum_observations
        {
            self.readiness_emitted = true;
            events.push(self.readiness_event(true));
        }

        Ok(events)
    }

    pub fn progress_event(&self) -> ShadowEvent {
        self.readiness_event(
            self.sampled_observation_count >= self.config.minimum_observations,
        )
    }

    fn readiness_event(&self, ready: bool) -> ShadowEvent {
        ShadowEvent::Readiness {
            run_id: self.run_id.clone(),
            observed_count: self.observed_count,
            sampled_observation_count: self.sampled_observation_count,
            approved_count: self.approved_count,
            would_execute_count: self.would_execute_count,
            minimum_observations: self.config.minimum_observations,
            ready_for_analysis: ready,
        }
    }

    fn build_observation(
        &mut self,
        route: &TriangleRoute,
        record: &ArbitrageScanRecord,
    ) -> Result<ShadowOpportunity, ShadowError> {
        let observation_id = observation_id(
            &self.run_id,
            &record.route_id,
            record.scan_timestamp,
            record.trigger_sequence,
        );
        let start_amount =
            decimal_from_option_f64("start_amount", record.start_amount)?;
        let detection_final = decimal_from_option_f64(
            "final_amount",
            record.final_amount,
        )?;
        let gross_profit =
            decimal_from_option_f64("gross_profit", record.gross_profit)?;
        let expected_profit = decimal_from_option_f64(
            "expected_net_profit",
            record.expected_net_profit,
        )?;
        let expected_edge = decimal_from_option_f64(
            "expected_net_return_bps",
            record.expected_net_return_bps,
        )?;

        let latency_neutral = self
            .profitability_latency_neutral
            .evaluate(start_amount, detection_final)
            .map_err(|error| ShadowError::Engine(error.to_string()))?;

        let mut risk_checks: Vec<RiskCheckResult> = Vec::new();
        let mut approval_error = None;
        let mut approved = false;

        match self.build_risk_inputs(
            &observation_id,
            route,
            record,
            start_amount,
            expected_edge,
        ) {
            Ok((intent, context)) => {
                match self
                    .risk_engine
                    .preview(&intent, &context, record.scan_timestamp)
                {
                    Ok(decision) => {
                        approved = decision.approved;
                        risk_checks = decision.checks;
                    }
                    Err(error) => {
                        approval_error = Some(error.to_string());
                    }
                }
            }
            Err(error) => {
                approval_error = Some(error);
            }
        }

        let latency_tracking =
            self.pending.len() < self.config.max_pending_observations;
        if !latency_tracking && approval_error.is_none() {
            approval_error = Some(
                "latency tracking queue is full; opportunity was not scheduled for delayed samples"
                    .to_string(),
            );
        }

        let account = self.account.as_ref();
        let session_pnl = account.map(|state| {
            state.snapshot.total_equity_usd - state.baseline_equity_usd
        });

        Ok(ShadowOpportunity {
            run_id: self.run_id.clone(),
            observation_id,
            detected_at_ms: record.scan_timestamp,
            route_id: record.route_id.clone(),
            triangle_id: record.triangle_id.clone(),
            start_asset: record.start_asset.clone(),
            starting_capital: decimal_text(start_amount),
            detection_final_amount: decimal_text(detection_final),
            detection_gross_profit: decimal_text(gross_profit),
            expected_profit: decimal_text(expected_profit),
            latency_neutral_detection_profit: decimal_text(
                latency_neutral.expected_net_profit,
            ),
            expected_net_edge_bps: decimal_text(expected_edge),
            detected: true,
            approved,
            would_execute: approved,
            approval_error,
            risk_checks,
            account_balance: account.map(|state| {
                decimal_text(state.snapshot.base_available)
            }),
            account_equity_usd: account.map(|state| {
                decimal_text(state.snapshot.total_equity_usd)
            }),
            account_exposure_usd: account.map(|state| {
                decimal_text(state.snapshot.non_base_exposure_usd)
            }),
            session_pnl_proxy_usd: session_pnl.map(decimal_text),
            detection_leg_prices: record
                .legs
                .iter()
                .map(|leg| leg.execution.average_execution_price)
                .collect(),
            oldest_book_timestamp_ms: record.oldest_book_timestamp,
            newest_book_timestamp_ms: record.newest_book_timestamp,
            book_timestamp_skew_ms: record.book_timestamp_skew_ms,
            latency_tracking,
        })
    }

    fn build_risk_inputs(
        &self,
        observation_id: &str,
        route: &TriangleRoute,
        record: &ArbitrageScanRecord,
        start_amount: Decimal,
        expected_edge: Decimal,
    ) -> Result<(TradeIntent, RiskContext), String> {
        let account = self.account.as_ref().ok_or_else(|| {
            "real account has not synchronized yet".to_string()
        })?;

        let mut proposed_legs = Vec::with_capacity(record.legs.len());
        let mut estimated_slippage_bps = Decimal::ZERO;

        for leg in &record.legs {
            let rules = self.symbol_rules.get(&leg.symbol).ok_or_else(|| {
                format!("missing live instrument precision for {}", leg.symbol)
            })?;
            let raw_quantity = decimal_from_f64(
                "filled_base_quantity",
                leg.execution.filled_base_quantity,
            )
            .map_err(|error| error.to_string())?;
            let quantity = floor_to_step(raw_quantity, rules.qty_step);
            proposed_legs.push(ProposedOrderLeg {
                symbol: leg.symbol.clone(),
                quantity,
                limit_price: None,
                rules: rules.clone(),
            });

            if let Some(value) = leg.execution.slippage_bps {
                if value > 0.0 {
                    estimated_slippage_bps += decimal_from_f64(
                        "slippage_bps",
                        value,
                    )
                    .map_err(|error| error.to_string())?;
                }
            }
        }

        if proposed_legs.len() != 3 {
            return Err("shadow opportunity does not contain three legs".to_string());
        }

        let expected_final = decimal_from_option_f64(
            "expected_final_amount",
            record.expected_final_amount,
        )
        .map_err(|error| error.to_string())?;
        let projected_peak_exposure = if expected_final > start_amount {
            expected_final
        } else {
            start_amount
        };

        let oldest_timestamp = record
            .oldest_book_timestamp
            .ok_or_else(|| "missing oldest book timestamp".to_string())?;

        let pnl_proxy =
            account.snapshot.total_equity_usd - account.baseline_equity_usd;
        let context = RiskContext {
            account_balance: account.snapshot.base_available,
            current_exposure: account.snapshot.non_base_exposure_usd,
            daily_realized_pnl: pnl_proxy,
            api_health: ServiceHealth {
                healthy: account.healthy,
                last_ok_ms: account.last_ok_ms,
                detail: format!(
                    "shadow session proxy; {}",
                    account.detail
                ),
            },
            exchange_health: ServiceHealth {
                healthy: self.exchange.healthy,
                last_ok_ms: self.exchange.last_ok_ms,
                detail: self.exchange.detail.clone(),
            },
        };

        let intent = TradeIntent {
            trade_id: observation_id.to_string(),
            route_id: route.id.clone(),
            starting_asset: route.start_asset.clone(),
            starting_notional: start_amount,
            projected_peak_exposure,
            expected_net_edge_bps: expected_edge,
            estimated_slippage_bps,
            available_liquidity: start_amount,
            available_liquidity_ratio: Decimal::ONE,
            market_data_timestamp_ms: oldest_timestamp,
            legs: proposed_legs,
        };

        Ok((intent, context))
    }

    fn latency_neutral_profit(
        &self,
        record: &ArbitrageScanRecord,
    ) -> Result<Decimal, ShadowError> {
        let start =
            decimal_from_option_f64("start_amount", record.start_amount)?;
        let final_amount =
            decimal_from_option_f64("final_amount", record.final_amount)?;
        self.profitability_latency_neutral
            .evaluate(start, final_amount)
            .map(|value| value.expected_net_profit)
            .map_err(|error| ShadowError::Engine(error.to_string()))
    }

    fn build_latency_sample(
        &self,
        pending: &PendingOpportunity,
        latency_ms: u64,
        target_at_ms: u64,
        sampled_at_ms: u64,
    ) -> ShadowLatencySample {
        let Some(route) = self.routes.get(&pending.route_id) else {
            return invalid_sample(
                &self.run_id,
                pending,
                latency_ms,
                target_at_ms,
                sampled_at_ms,
                "route disappeared from shadow configuration".to_string(),
            );
        };

        let evaluation = self.evaluate_route_at(
            route,
            pending.start_amount,
            target_at_ms,
        );

        match evaluation {
            Ok(evaluation) => {
                let profitability = match self
                    .profitability_latency_neutral
                    .evaluate(pending.start_amount, evaluation.final_amount)
                {
                    Ok(value) => value,
                    Err(error) => {
                        return invalid_sample(
                            &self.run_id,
                            pending,
                            latency_ms,
                            target_at_ms,
                            sampled_at_ms,
                            error.to_string(),
                        );
                    }
                };
                let drift = profitability.expected_net_profit
                    - pending.latency_neutral_detection_profit;
                let route_final_drift_bps =
                    if pending.detection_final_amount > Decimal::ZERO {
                        Some(
                            ((evaluation.final_amount
                                / pending.detection_final_amount)
                                - Decimal::ONE)
                                * Decimal::new(10_000, 0),
                        )
                    } else {
                        None
                    };
                let leg_price_drift_bps = route
                    .legs
                    .iter()
                    .zip(pending.detection_leg_prices.iter())
                    .zip(evaluation.leg_average_prices.iter())
                    .map(|((leg, detected), sampled)| {
                        adverse_price_drift_bps(
                            leg.side,
                            *detected,
                            *sampled,
                        )
                    })
                    .collect();

                ShadowLatencySample {
                    run_id: self.run_id.clone(),
                    observation_id: pending.observation_id.clone(),
                    route_id: pending.route_id.clone(),
                    latency_ms,
                    target_at_ms,
                    sampled_at_ms,
                    scheduler_lag_ms: sampled_at_ms
                        .saturating_sub(target_at_ms),
                    sample_valid: true,
                    failure_reason: None,
                    final_amount: Some(decimal_text(evaluation.final_amount)),
                    net_profit: Some(decimal_text(
                        profitability.expected_net_profit,
                    )),
                    net_edge_bps: Some(decimal_text(
                        profitability.expected_net_return_bps,
                    )),
                    profit_drift_from_detection: Some(decimal_text(drift)),
                    route_final_drift_bps: route_final_drift_bps
                        .map(decimal_text),
                    leg_price_drift_bps,
                    profitable_after_latency: profitability.net_profitable,
                    still_meets_min_edge: profitability.expected_net_return_bps
                        >= self.risk_engine.config().min_net_edge_bps,
                    leg_average_prices: evaluation.leg_average_prices,
                    oldest_book_timestamp_ms: evaluation.oldest_timestamp,
                    newest_book_timestamp_ms: evaluation.newest_timestamp,
                    book_timestamp_skew_ms: evaluation
                        .newest_timestamp
                        .zip(evaluation.oldest_timestamp)
                        .map(|(newest, oldest)| newest.saturating_sub(oldest)),
                }
            }
            Err(error) => invalid_sample(
                &self.run_id,
                pending,
                latency_ms,
                target_at_ms,
                sampled_at_ms,
                error,
            ),
        }
    }

    fn evaluate_route_at(
        &self,
        route: &TriangleRoute,
        start_amount: Decimal,
        target_at_ms: u64,
    ) -> Result<RouteEvaluation, String> {
        let mut amount = start_amount
            .to_string()
            .parse::<f64>()
            .map_err(|_| "start amount does not fit f64".to_string())?;
        let mut prices = Vec::with_capacity(3);
        let mut timestamps = Vec::with_capacity(3);

        for leg in &route.legs {
            let history = self.history.get(&leg.symbol).ok_or_else(|| {
                format!("no book history for {}", leg.symbol)
            })?;
            let snapshot = history
                .iter()
                .rev()
                .find(|entry| entry.received_at_ms <= target_at_ms)
                .ok_or_else(|| {
                    format!(
                        "no {} book received at or before latency target",
                        leg.symbol
                    )
                })?;
            let book_age_ms = target_at_ms
                .saturating_sub(snapshot.received_at_ms);
            if book_age_ms > self.risk_engine.config().max_market_data_age_ms {
                return Err(format!(
                    "{} book is {}ms old at latency target; maximum is {}ms",
                    leg.symbol,
                    book_age_ms,
                    self.risk_engine.config().max_market_data_age_ms
                ));
            }

            let estimate = match leg.side {
                TradeSide::Buy => snapshot.book.buy_with_quote(amount),
                TradeSide::Sell => snapshot.book.sell_base(amount),
            }
            .map_err(|error| error.to_string())?;

            if !estimate.complete {
                return Err(format!(
                    "insufficient depth on {} at latency target",
                    leg.symbol
                ));
            }

            amount = match leg.side {
                TradeSide::Buy => estimate.filled_base_quantity,
                TradeSide::Sell => estimate.filled_quote_quantity,
            };
            prices.push(estimate.average_execution_price);
            timestamps.push(estimate.timestamp);
        }

        let final_amount = decimal_from_f64("latency_final_amount", amount)
            .map_err(|error| error.to_string())?;
        Ok(RouteEvaluation {
            final_amount,
            leg_average_prices: prices,
            oldest_timestamp: timestamps.iter().min().copied(),
            newest_timestamp: timestamps.iter().max().copied(),
        })
    }

    fn apply_history(
        &mut self,
        update: BookUpdate,
        received_at_ms: u64,
    ) -> Result<(), ShadowError> {
        let symbol = update.symbol.clone();
        self.books
            .apply(update)
            .map_err(|error| ShadowError::Engine(error.to_string()))?;
        let book = self
            .books
            .get(&symbol)
            .cloned()
            .ok_or_else(|| {
                ShadowError::Engine(format!(
                    "book vanished after applying update for {symbol}"
                ))
            })?;

        let entries = self.history.entry(symbol).or_default();
        entries.push_back(HistoricalBook {
            received_at_ms,
            book,
        });

        let cutoff =
            received_at_ms.saturating_sub(self.config.history_retention_ms);
        while entries
            .front()
            .is_some_and(|entry| entry.received_at_ms < cutoff)
        {
            entries.pop_front();
        }
        Ok(())
    }
}

struct RouteEvaluation {
    final_amount: Decimal,
    leg_average_prices: Vec<Option<f64>>,
    oldest_timestamp: Option<u64>,
    newest_timestamp: Option<u64>,
}

fn invalid_sample(
    run_id: &str,
    pending: &PendingOpportunity,
    latency_ms: u64,
    target_at_ms: u64,
    sampled_at_ms: u64,
    reason: String,
) -> ShadowLatencySample {
    ShadowLatencySample {
        run_id: run_id.to_string(),
        observation_id: pending.observation_id.clone(),
        route_id: pending.route_id.clone(),
        latency_ms,
        target_at_ms,
        sampled_at_ms,
        scheduler_lag_ms: sampled_at_ms.saturating_sub(target_at_ms),
        sample_valid: false,
        failure_reason: Some(reason),
        final_amount: None,
        net_profit: None,
        net_edge_bps: None,
        profit_drift_from_detection: None,
        route_final_drift_bps: None,
        leg_price_drift_bps: Vec::new(),
        profitable_after_latency: false,
        still_meets_min_edge: false,
        leg_average_prices: Vec::new(),
        oldest_book_timestamp_ms: None,
        newest_book_timestamp_ms: None,
        book_timestamp_skew_ms: None,
    }
}

fn adverse_price_drift_bps(
    side: TradeSide,
    detected: Option<f64>,
    sampled: Option<f64>,
) -> Option<f64> {
    let (Some(detected), Some(sampled)) = (detected, sampled) else {
        return None;
    };
    if !detected.is_finite()
        || !sampled.is_finite()
        || detected <= 0.0
    {
        return None;
    }
    Some(match side {
        TradeSide::Buy => ((sampled / detected) - 1.0) * 10_000.0,
        TradeSide::Sell => (1.0 - (sampled / detected)) * 10_000.0,
    })
}

fn observation_id(
    run_id: &str,
    route_id: &str,
    scan_timestamp: u64,
    trigger_sequence: u64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(run_id.as_bytes());
    hasher.update(b"|");
    hasher.update(route_id.as_bytes());
    hasher.update(b"|");
    hasher.update(scan_timestamp.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(trigger_sequence.to_string().as_bytes());
    hex::encode(hasher.finalize())
}

fn floor_to_step(value: Decimal, step: Decimal) -> Decimal {
    if value <= Decimal::ZERO || step <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    value - (value % step)
}

fn decimal_from_option_f64(
    field: &str,
    value: Option<f64>,
) -> Result<Decimal, ShadowError> {
    decimal_from_f64(
        field,
        value.ok_or_else(|| {
            ShadowError::Engine(format!("missing {field}"))
        })?,
    )
}

fn decimal_from_f64(
    field: &str,
    value: f64,
) -> Result<Decimal, ShadowError> {
    if !value.is_finite() {
        return Err(ShadowError::Engine(format!(
            "{field} is not finite"
        )));
    }
    Decimal::from_str_exact(&value.to_string()).map_err(|_| {
        ShadowError::Engine(format!(
            "{field} could not convert from {value}"
        ))
    })
}

fn decimal_text(value: Decimal) -> String {
    value.normalize().to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use orderbook::PriceLevel;
    use risk::RiskConfig;
    use scanner::{ProfitabilityConfigFile, TriangleLeg, TriangleSource};

    use super::*;

    fn d(value: &str) -> Decimal {
        Decimal::from_str_exact(value).unwrap()
    }

    fn shadow_config() -> ShadowConfig {
        ShadowConfig {
            version: 1,
            base_asset: "USDT".to_string(),
            latency_ms: vec![50, 100, 250],
            minimum_observations: 3_000,
            max_pending_observations: 5_000,
            account_refresh_ms: 1_000,
            sample_tick_ms: 5,
            history_retention_ms: 1_000,
        }
    }

    fn route_config() -> TriangleConfig {
        TriangleConfig {
            version: 1,
            exchange: "bybit".to_string(),
            market: "spot".to_string(),
            generated_at: None,
            source: TriangleSource {
                endpoint: "mainnet".to_string(),
                category: "spot".to_string(),
                status: "Trading".to_string(),
                testnet: false,
            },
            start_assets: vec!["USDT".to_string()],
            instrument_count: 3,
            triangle_count: 1,
            route_count: 1,
            routes: vec![TriangleRoute {
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
            }],
        }
    }

    fn profitability() -> ProfitabilityConfig {
        ProfitabilityConfigFile {
            version: 1,
            fee_profile: "test".to_string(),
            fee_bps_per_leg: vec!["0".into(), "0".into(), "0".into()],
            expected_slippage_bps: "0".into(),
            rounding_loss_bps: "0".into(),
            latency_buffer_bps: "0".into(),
            safety_margin_bps: "0".into(),
        }
        .try_into()
        .unwrap()
    }

    fn risk_engine() -> RiskEngine {
        let base = std::env::temp_dir().join(format!(
            "shadow-risk-{}",
            std::process::id()
        ));
        let config = RiskConfig {
            version: 1,
            max_market_data_age_ms: 500,
            min_net_edge_bps: d("0"),
            max_slippage_bps: d("100"),
            min_liquidity_ratio: d("1"),
            max_trade_size: d("1000"),
            max_total_exposure: d("10000"),
            max_daily_loss: d("1000"),
            execution_failure_limit: 3,
            execution_failure_window_ms: 300_000,
            api_health_max_age_ms: 5_000,
            exchange_health_max_age_ms: 5_000,
            approval_ttl_ms: 100,
            emergency_max_market_data_age_ms: 2_000,
            kill_switch_file: base.join("KILL"),
            state_file: base.join("STATE"),
            trading_control_file: base.join("TRADING_CONTROL"),
            runtime_limits_file: base.join("RISK_LIMITS"),
        };
        let _ = std::fs::remove_dir_all(&base);
        RiskEngine::new(config).unwrap()
    }

    fn scanner_settings() -> ScannerSettings {
        ScannerSettings {
            version: 1,
            start_amounts: HashMap::from([("USDT".to_string(), 100.0)]),
            record_path: "unused".to_string(),
            profitability_config_path: "unused".to_string(),
            max_book_age_ms: 1_000,
            max_book_skew_ms: 100,
        }
    }

    fn snapshot(
        symbol: &str,
        bid: f64,
        ask: f64,
        exchange_ts: u64,
        seq: u64,
    ) -> BookUpdate {
        BookUpdate {
            symbol: symbol.to_string(),
            bids: vec![PriceLevel {
                price: bid,
                quantity: 1_000.0,
            }],
            asks: vec![PriceLevel {
                price: ask,
                quantity: 1_000.0,
            }],
            timestamp: exchange_ts,
            update_id: seq,
            sequence: seq,
            is_snapshot: true,
        }
    }

    #[test]
    fn delayed_sample_never_uses_book_received_after_target() {
        let triangle = route_config();
        let route = triangle.routes[0].clone();
        let mut engine = ShadowEngine::new(
            shadow_config(),
            "test-run".to_string(),
            triangle,
            scanner_settings(),
            profitability(),
            risk_engine(),
        )
        .unwrap();

        engine.apply_history(
            snapshot("BTCUSDT", 9.9, 10.0, 900, 1),
            1_000,
        )
        .unwrap();
        engine.apply_history(
            snapshot("ETHBTC", 0.49, 0.5, 900, 1),
            1_000,
        )
        .unwrap();
        engine.apply_history(
            snapshot("ETHUSDT", 6.0, 6.1, 900, 1),
            1_000,
        )
        .unwrap();

        engine.apply_history(
            snapshot("ETHUSDT", 4.0, 4.1, 960, 2),
            1_060,
        )
        .unwrap();

        let evaluation = engine
            .evaluate_route_at(&route, d("100"), 1_050)
            .unwrap();

        assert_eq!(evaluation.leg_average_prices[2], Some(6.0));
    }
}
