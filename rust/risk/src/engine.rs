use std::{
    fs,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use rust_decimal::Decimal;
use serde_json::json;

use crate::{
    BreakerKind, CircuitBreakerState, EmergencyUnwindIntent, PersistentRiskState,
    ProposedOrderLeg, RiskApproval, RiskApprovalKind, RiskCheck, RiskCheckResult, RiskConfig,
    RiskContext, RiskDecision, RiskError, RiskStatus, ServiceHealth, TradeIntent,
};

pub struct RiskEngine {
    config: RiskConfig,
    state: PersistentRiskState,
}

impl RiskEngine {
    pub fn new(config: RiskConfig) -> Result<Self, RiskError> {
        config.validate()?;
        let state = load_state(&config.state_file)?;
        Ok(Self { config, state })
    }

    pub fn config(&self) -> &RiskConfig {
        &self.config
    }

    pub fn evaluate(
        &mut self,
        intent: &TradeIntent,
        context: &RiskContext,
        now_ms: u64,
    ) -> Result<RiskDecision, RiskError> {
        self.evaluate_internal(intent, context, now_ms, true)
    }

    pub fn preview(
        &mut self,
        intent: &TradeIntent,
        context: &RiskContext,
        now_ms: u64,
    ) -> Result<RiskDecision, RiskError> {
        self.evaluate_internal(intent, context, now_ms, false)
    }

    fn evaluate_internal(
        &mut self,
        intent: &TradeIntent,
        context: &RiskContext,
        now_ms: u64,
        latch_breakers: bool,
    ) -> Result<RiskDecision, RiskError> {
        self.refresh_state()?;
        let mut checks = Vec::with_capacity(13);

        let kill_detail = self.manual_kill_switch_detail()?;
        let kill_active = kill_detail.is_some();
        checks.push(check(
            RiskCheck::ManualKillSwitch,
            !kill_active,
            kill_detail
                .clone()
                .unwrap_or_else(|| "manual kill switch is clear".to_string()),
        ));
        if kill_active {
            return Ok(rejected(intent, checks));
        }

        if let Some(breaker) = &self.state.circuit_breaker {
            checks.push(check(
                RiskCheck::CircuitBreaker,
                false,
                format!(
                    "circuit breaker {:?} active since {}: {}",
                    breaker.kind, breaker.tripped_at_ms, breaker.detail
                ),
            ));
            return Ok(rejected(intent, checks));
        }
        checks.push(check(
            RiskCheck::CircuitBreaker,
            true,
            "no circuit breaker is active".to_string(),
        ));

        let mut trip: Option<(BreakerKind, String)> = None;

        let market_fresh = if intent.market_data_timestamp_ms > now_ms {
            let detail = format!(
                "market timestamp {} is ahead of local time {}",
                intent.market_data_timestamp_ms, now_ms
            );
            trip = Some((BreakerKind::ClockSkew, detail.clone()));
            checks.push(check(RiskCheck::MarketDataFreshness, false, detail));
            false
        } else {
            let age = now_ms - intent.market_data_timestamp_ms;
            let passed = age <= self.config.max_market_data_age_ms;
            let detail = format!(
                "market data age={}ms, limit={}ms",
                age, self.config.max_market_data_age_ms
            );
            if !passed {
                trip = Some((BreakerKind::StaleMarketData, detail.clone()));
            }
            checks.push(check(RiskCheck::MarketDataFreshness, passed, detail));
            passed
        };

        let edge_ok = intent.expected_net_edge_bps >= self.config.min_net_edge_bps;
        checks.push(check(
            RiskCheck::MinimumNetEdge,
            edge_ok,
            format!(
                "net edge={} bps, minimum={} bps",
                intent.expected_net_edge_bps, self.config.min_net_edge_bps
            ),
        ));

        let slippage_ok = intent.estimated_slippage_bps >= Decimal::ZERO
            && intent.estimated_slippage_bps <= self.config.max_slippage_bps;
        checks.push(check(
            RiskCheck::MaximumSlippage,
            slippage_ok,
            format!(
                "estimated slippage={} bps, maximum={} bps",
                intent.estimated_slippage_bps, self.config.max_slippage_bps
            ),
        ));

        let liquidity_ok = intent.available_liquidity >= intent.starting_notional
            && intent.available_liquidity >= Decimal::ZERO
            && intent.available_liquidity_ratio >= self.config.min_liquidity_ratio
            && intent.available_liquidity_ratio <= Decimal::ONE;
        checks.push(check(
            RiskCheck::AvailableLiquidity,
            liquidity_ok,
            format!(
                "available={}, required={}, ratio={}, minimum_ratio={}",
                intent.available_liquidity,
                intent.starting_notional,
                intent.available_liquidity_ratio,
                self.config.min_liquidity_ratio
            ),
        ));

        let balance_ok = context.account_balance >= intent.starting_notional
            && context.account_balance >= Decimal::ZERO;
        checks.push(check(
            RiskCheck::AccountBalance,
            balance_ok,
            format!(
                "balance={}, required={}",
                context.account_balance, intent.starting_notional
            ),
        ));

        let trade_size_ok =
            intent.starting_notional > Decimal::ZERO
                && intent.starting_notional <= self.config.max_trade_size;
        checks.push(check(
            RiskCheck::MaximumTradeSize,
            trade_size_ok,
            format!(
                "trade_size={}, maximum={}",
                intent.starting_notional, self.config.max_trade_size
            ),
        ));

        let precision = validate_precision(&intent.legs);
        checks.push(check(
            RiskCheck::SymbolPrecision,
            precision.is_none(),
            precision.unwrap_or_else(|| "all leg quantities/prices match symbol rules".to_string()),
        ));

        let exposure_after = context.current_exposure + intent.projected_peak_exposure;
        let exposure_ok =
            context.current_exposure >= Decimal::ZERO
                && intent.projected_peak_exposure >= Decimal::ZERO
                && exposure_after <= self.config.max_total_exposure;
        checks.push(check(
            RiskCheck::MaximumExposure,
            exposure_ok,
            format!(
                "current={}, projected_peak={}, combined_peak={}, maximum={}",
                context.current_exposure,
                intent.projected_peak_exposure,
                exposure_after,
                self.config.max_total_exposure
            ),
        ));

        let daily_loss_ok = context.daily_realized_pnl > -self.config.max_daily_loss;
        let daily_detail = format!(
            "daily_realized_pnl={}, loss_limit=-{}",
            context.daily_realized_pnl, self.config.max_daily_loss
        );
        if !daily_loss_ok && trip.is_none() {
            trip = Some((BreakerKind::DailyLossLimit, daily_detail.clone()));
        }
        checks.push(check(
            RiskCheck::MaximumDailyLoss,
            daily_loss_ok,
            daily_detail,
        ));

        let (api_ok, api_detail) = service_health(
            "api",
            &context.api_health,
            now_ms,
            self.config.api_health_max_age_ms,
        );
        if !api_ok && trip.is_none() {
            trip = Some((BreakerKind::ApiHealth, api_detail.clone()));
        }
        checks.push(check(RiskCheck::ApiHealth, api_ok, api_detail));

        let (exchange_ok, exchange_detail) = service_health(
            "exchange",
            &context.exchange_health,
            now_ms,
            self.config.exchange_health_max_age_ms,
        );
        if !exchange_ok && trip.is_none() {
            trip = Some((BreakerKind::ExchangeHealth, exchange_detail.clone()));
        }
        checks.push(check(
            RiskCheck::ExchangeHealth,
            exchange_ok,
            exchange_detail,
        ));

        if latch_breakers {
            if let Some((kind, detail)) = trip {
                self.trip_breaker(kind, detail, now_ms)?;
            }
        }

        let approved = market_fresh && checks.iter().all(|item| item.passed);
        let approval = approved.then(|| RiskApproval {
            trade_id: intent.trade_id.clone(),
            approved_at_ms: now_ms,
            expires_at_ms: now_ms.saturating_add(self.config.approval_ttl_ms),
            kind: RiskApprovalKind::Normal,
        });

        Ok(RiskDecision {
            trade_id: intent.trade_id.clone(),
            approved,
            checks,
            approval,
        })
    }

    pub fn validate_approval(
        &mut self,
        approval: &RiskApproval,
        trade_id: &str,
        now_ms: u64,
    ) -> Result<(), RiskError> {
        self.refresh_state()?;
        if approval.kind() == RiskApprovalKind::Normal {
            if self.manual_kill_switch_detail()?.is_some() {
                return Err(RiskError::GateClosed(
                    "manual kill switch is active".to_string(),
                ));
            }
            if let Some(breaker) = &self.state.circuit_breaker {
                return Err(RiskError::GateClosed(format!(
                    "circuit breaker {:?} is active: {}",
                    breaker.kind, breaker.detail
                )));
            }
        }
        if approval.trade_id() != trade_id {
            return Err(RiskError::GateClosed(
                "risk approval does not match trade id".to_string(),
            ));
        }
        if now_ms < approval.approved_at_ms() {
            return Err(RiskError::GateClosed(
                "local clock is behind approval timestamp".to_string(),
            ));
        }
        if now_ms > approval.expires_at_ms() {
            return Err(RiskError::GateClosed(format!(
                "risk approval expired at {}",
                approval.expires_at_ms()
            )));
        }
        Ok(())
    }

    pub fn approve_emergency_unwind(
        &mut self,
        intent: &EmergencyUnwindIntent,
        now_ms: u64,
    ) -> Result<RiskApproval, RiskError> {
        if intent.trade_id.trim().is_empty()
            || intent.exposure_asset.trim().is_empty()
            || intent.base_asset.trim().is_empty()
        {
            return Err(RiskError::GateClosed(
                "emergency unwind identifiers must not be empty".to_string(),
            ));
        }
        if intent.exposure_asset == intent.base_asset {
            return Err(RiskError::GateClosed(
                "emergency unwind exposure asset must differ from base asset".to_string(),
            ));
        }
        if intent.exposure_notional <= Decimal::ZERO
            || intent.unwind_notional <= Decimal::ZERO
            || intent.unwind_notional > intent.exposure_notional
        {
            return Err(RiskError::GateClosed(
                "emergency unwind notional must be positive and no larger than known exposure"
                    .to_string(),
            ));
        }
        if intent.market_data_timestamp_ms > now_ms {
            return Err(RiskError::GateClosed(
                "emergency unwind market timestamp is ahead of local time".to_string(),
            ));
        }
        let age = now_ms - intent.market_data_timestamp_ms;
        if age > self.config.emergency_max_market_data_age_ms {
            return Err(RiskError::GateClosed(format!(
                "emergency unwind market data age={}ms exceeds {}ms",
                age, self.config.emergency_max_market_data_age_ms
            )));
        }

        let (api_ok, api_detail) = service_health(
            "api",
            &intent.api_health,
            now_ms,
            self.config.api_health_max_age_ms,
        );
        if !api_ok {
            return Err(RiskError::GateClosed(api_detail));
        }
        let (exchange_ok, exchange_detail) = service_health(
            "exchange",
            &intent.exchange_health,
            now_ms,
            self.config.exchange_health_max_age_ms,
        );
        if !exchange_ok {
            return Err(RiskError::GateClosed(exchange_detail));
        }

        Ok(RiskApproval {
            trade_id: intent.trade_id.clone(),
            approved_at_ms: now_ms,
            expires_at_ms: now_ms.saturating_add(self.config.approval_ttl_ms),
            kind: RiskApprovalKind::EmergencyUnwind,
        })
    }

    pub fn record_execution_failure(
        &mut self,
        now_ms: u64,
        detail: impl Into<String>,
    ) -> Result<bool, RiskError> {
        self.refresh_state()?;
        let start = now_ms.saturating_sub(self.config.execution_failure_window_ms);
        self.state
            .execution_failures_ms
            .retain(|timestamp| *timestamp >= start && *timestamp <= now_ms);
        self.state.execution_failures_ms.push(now_ms);

        let should_trip =
            self.state.execution_failures_ms.len() >= self.config.execution_failure_limit;
        if should_trip && self.state.circuit_breaker.is_none() {
            self.state.circuit_breaker = Some(CircuitBreakerState {
                kind: BreakerKind::ExecutionFailures,
                tripped_at_ms: now_ms,
                detail: format!(
                    "{} execution failures within {}ms; latest: {}",
                    self.state.execution_failures_ms.len(),
                    self.config.execution_failure_window_ms,
                    detail.into()
                ),
            });
        }
        self.persist_state()?;
        Ok(should_trip)
    }

    pub fn record_execution_success(&mut self, now_ms: u64) -> Result<(), RiskError> {
        self.refresh_state()?;
        let start = now_ms.saturating_sub(self.config.execution_failure_window_ms);
        self.state
            .execution_failures_ms
            .retain(|timestamp| *timestamp >= start && *timestamp <= now_ms);
        self.persist_state()
    }

    pub fn engage_manual_kill_switch(
        &self,
        reason: &str,
        now_ms: u64,
    ) -> Result<(), RiskError> {
        if reason.trim().is_empty() {
            return Err(RiskError::InvalidOperatorAction(
                "kill-switch reason must not be empty".to_string(),
            ));
        }
        ensure_parent(&self.config.kill_switch_file)?;
        let payload = json!({
            "engaged_at_ms": now_ms,
            "reason": reason,
        });
        fs::write(
            &self.config.kill_switch_file,
            serde_json::to_vec_pretty(&payload)?,
        )?;
        Ok(())
    }

    pub fn clear_manual_kill_switch(&self, operator_note: &str) -> Result<(), RiskError> {
        if operator_note.trim().is_empty() {
            return Err(RiskError::InvalidOperatorAction(
                "operator note is required to clear kill switch".to_string(),
            ));
        }
        match fs::remove_file(&self.config.kill_switch_file) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub fn reset_circuit_breaker(&mut self, operator_note: &str) -> Result<(), RiskError> {
        if operator_note.trim().is_empty() {
            return Err(RiskError::InvalidOperatorAction(
                "operator note is required to reset circuit breaker".to_string(),
            ));
        }
        self.refresh_state()?;
        self.state.circuit_breaker = None;
        self.state.execution_failures_ms.clear();
        self.persist_state()
    }

    pub fn status(&mut self, now_ms: u64) -> Result<RiskStatus, RiskError> {
        self.refresh_state()?;
        let detail = self.manual_kill_switch_detail()?;
        let start = now_ms.saturating_sub(self.config.execution_failure_window_ms);
        let recent = self
            .state
            .execution_failures_ms
            .iter()
            .filter(|timestamp| **timestamp >= start && **timestamp <= now_ms)
            .count();

        Ok(RiskStatus {
            manual_kill_switch_active: detail.is_some(),
            manual_kill_switch_detail: detail,
            circuit_breaker: self.state.circuit_breaker.clone(),
            recent_execution_failures: recent,
        })
    }

    fn trip_breaker(
        &mut self,
        kind: BreakerKind,
        detail: String,
        now_ms: u64,
    ) -> Result<(), RiskError> {
        if self.state.circuit_breaker.is_none() {
            self.state.circuit_breaker = Some(CircuitBreakerState {
                kind,
                tripped_at_ms: now_ms,
                detail,
            });
            self.persist_state()?;
        }
        Ok(())
    }

    fn manual_kill_switch_detail(&self) -> Result<Option<String>, RiskError> {
        match fs::read_to_string(&self.config.kill_switch_file) {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn refresh_state(&mut self) -> Result<(), RiskError> {
        self.state = load_state(&self.config.state_file)?;
        Ok(())
    }

    fn persist_state(&self) -> Result<(), RiskError> {
        ensure_parent(&self.config.state_file)?;
        fs::write(
            &self.config.state_file,
            serde_json::to_vec_pretty(&self.state)?,
        )?;
        Ok(())
    }
}

pub fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn load_state(path: &Path) -> Result<PersistentRiskState, RiskError> {
    match fs::read(path) {
        Ok(raw) => Ok(serde_json::from_slice(&raw)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(PersistentRiskState::default())
        }
        Err(error) => Err(error.into()),
    }
}

fn ensure_parent(path: &Path) -> Result<(), RiskError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn rejected(intent: &TradeIntent, checks: Vec<RiskCheckResult>) -> RiskDecision {
    RiskDecision {
        trade_id: intent.trade_id.clone(),
        approved: false,
        checks,
        approval: None,
    }
}

fn check(check: RiskCheck, passed: bool, detail: String) -> RiskCheckResult {
    RiskCheckResult {
        check,
        passed,
        detail,
    }
}

fn service_health(
    label: &str,
    health: &ServiceHealth,
    now_ms: u64,
    max_age_ms: u64,
) -> (bool, String) {
    if !health.healthy {
        return (
            false,
            format!("{label} reports unhealthy: {}", health.detail),
        );
    }
    if health.last_ok_ms > now_ms {
        return (
            false,
            format!(
                "{label} health timestamp {} is ahead of local time {}",
                health.last_ok_ms, now_ms
            ),
        );
    }
    let age = now_ms - health.last_ok_ms;
    (
        age <= max_age_ms,
        format!("{label} health age={age}ms, limit={max_age_ms}ms"),
    )
}

fn validate_precision(legs: &[ProposedOrderLeg]) -> Option<String> {
    if legs.is_empty() || legs.len() > 3 {
        return Some(format!(
            "risk evaluation requires between 1 and 3 proposed legs, received {}",
            legs.len()
        ));
    }

    for leg in legs {
        if leg.symbol.trim().is_empty() {
            return Some("leg symbol must not be empty".to_string());
        }
        if leg.quantity <= Decimal::ZERO {
            return Some(format!("{} quantity must be positive", leg.symbol));
        }
        if leg.rules.qty_step <= Decimal::ZERO
            || leg.rules.min_order_qty <= Decimal::ZERO
            || leg.rules.tick_size <= Decimal::ZERO
        {
            return Some(format!("{} has invalid symbol precision rules", leg.symbol));
        }
        if leg.quantity < leg.rules.min_order_qty {
            return Some(format!(
                "{} quantity {} is below minimum {}",
                leg.symbol, leg.quantity, leg.rules.min_order_qty
            ));
        }
        if leg.quantity % leg.rules.qty_step != Decimal::ZERO {
            return Some(format!(
                "{} quantity {} is not aligned to qty_step {}",
                leg.symbol, leg.quantity, leg.rules.qty_step
            ));
        }
        if let Some(price) = leg.limit_price {
            if price <= Decimal::ZERO {
                return Some(format!("{} limit price must be positive", leg.symbol));
            }
            if price % leg.rules.tick_size != Decimal::ZERO {
                return Some(format!(
                    "{} price {} is not aligned to tick_size {}",
                    leg.symbol, price, leg.rules.tick_size
                ));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rust_decimal::Decimal;

    use super::*;
    use crate::{ProposedOrderLeg, SymbolRules, TradeIntent};

    fn d(value: &str) -> Decimal {
        Decimal::from_str_exact(value).unwrap()
    }

    fn paths(name: &str) -> (PathBuf, PathBuf) {
        let base = std::env::temp_dir().join(format!(
            "arbitrage-risk-{}-{}",
            name,
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&base);
        (
            base.join("KILL_SWITCH"),
            base.join("risk_state.json"),
        )
    }

    fn config(name: &str) -> RiskConfig {
        let (kill_switch_file, state_file) = paths(name);
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
            kill_switch_file,
            state_file,
        }
    }

    fn leg(symbol: &str) -> ProposedOrderLeg {
        ProposedOrderLeg {
            symbol: symbol.to_string(),
            quantity: d("1.000"),
            limit_price: Some(d("100.00")),
            rules: SymbolRules {
                qty_step: d("0.001"),
                min_order_qty: d("0.001"),
                tick_size: d("0.01"),
            },
        }
    }

    fn intent(now: u64) -> TradeIntent {
        TradeIntent {
            trade_id: "trade-1".to_string(),
            route_id: "USDT>BTC>ETH>USDT".to_string(),
            starting_asset: "USDT".to_string(),
            starting_notional: d("450"),
            projected_peak_exposure: d("450"),
            expected_net_edge_bps: d("18"),
            estimated_slippage_bps: d("5"),
            available_liquidity: d("450"),
            available_liquidity_ratio: d("1"),
            market_data_timestamp_ms: now - 100,
            legs: vec![leg("BTCUSDT"), leg("ETHBTC"), leg("ETHUSDT")],
        }
    }

    fn context(now: u64) -> RiskContext {
        RiskContext {
            account_balance: d("1000"),
            current_exposure: d("100"),
            daily_realized_pnl: d("-5"),
            api_health: ServiceHealth {
                healthy: true,
                last_ok_ms: now - 50,
                detail: "ok".to_string(),
            },
            exchange_health: ServiceHealth {
                healthy: true,
                last_ok_ms: now - 50,
                detail: "ok".to_string(),
            },
        }
    }

    #[test]
    fn approves_trade_when_every_gate_passes() {
        let now = 1_000_000;
        let mut engine = RiskEngine::new(config("approve")).unwrap();
        let decision = engine.evaluate(&intent(now), &context(now), now).unwrap();

        assert!(decision.approved);
        let approval = decision.into_approval().unwrap();
        assert_eq!(approval.trade_id(), "trade-1");
        engine
            .validate_approval(&approval, "trade-1", now + 50)
            .unwrap();
    }

    #[test]
    fn preview_rejects_stale_market_without_latching_breaker() {
        let now = 1_500_000;
        let mut engine = RiskEngine::new(config("preview-stale")).unwrap();
        let mut stale = intent(now);
        stale.market_data_timestamp_ms = now - 501;

        let decision = engine.preview(&stale, &context(now), now).unwrap();
        assert!(!decision.approved);

        let fresh = intent(now + 10);
        let recovered = engine
            .preview(&fresh, &context(now + 10), now + 10)
            .unwrap();
        assert!(recovered.approved);
        assert!(engine
            .status(now + 10)
            .unwrap()
            .circuit_breaker
            .is_none());
    }

    #[test]
    fn stale_market_data_trips_latched_breaker() {
        let now = 2_000_000;
        let mut engine = RiskEngine::new(config("stale")).unwrap();
        let mut stale = intent(now);
        stale.market_data_timestamp_ms = now - 501;

        let decision = engine.evaluate(&stale, &context(now), now).unwrap();
        assert!(!decision.approved);

        let fresh = intent(now + 10);
        let blocked = engine
            .evaluate(&fresh, &context(now + 10), now + 10)
            .unwrap();
        assert!(!blocked.approved);

        engine.reset_circuit_breaker("feed investigated").unwrap();
        let recovered = engine
            .evaluate(&fresh, &context(now + 10), now + 10)
            .unwrap();
        assert!(recovered.approved);
    }

    #[test]
    fn three_execution_failures_within_five_minutes_trip_breaker() {
        let now = 3_000_000;
        let mut engine = RiskEngine::new(config("failures")).unwrap();

        assert!(!engine.record_execution_failure(now, "leg1").unwrap());
        assert!(!engine
            .record_execution_failure(now + 1_000, "leg2")
            .unwrap());
        assert!(engine
            .record_execution_failure(now + 2_000, "leg3")
            .unwrap());

        let decision = engine
            .evaluate(&intent(now + 3_000), &context(now + 3_000), now + 3_000)
            .unwrap();
        assert!(!decision.approved);
    }

    #[test]
    fn old_execution_failure_does_not_count_toward_window() {
        let now = 4_000_000;
        let mut engine = RiskEngine::new(config("failure-window")).unwrap();

        engine
            .record_execution_failure(now - 400_000, "old")
            .unwrap();
        assert!(!engine.record_execution_failure(now, "first").unwrap());
        assert!(!engine
            .record_execution_failure(now + 1_000, "second")
            .unwrap());
    }

    #[test]
    fn manual_kill_switch_blocks_and_survives_new_engine_instance() {
        let now = 5_000_000;
        let cfg = config("kill");
        let engine = RiskEngine::new(cfg.clone()).unwrap();
        engine
            .engage_manual_kill_switch("operator emergency stop", now)
            .unwrap();

        let mut restarted = RiskEngine::new(cfg).unwrap();
        let decision = restarted.evaluate(&intent(now), &context(now), now).unwrap();
        assert!(!decision.approved);

        restarted
            .clear_manual_kill_switch("incident resolved")
            .unwrap();
        let decision = restarted.evaluate(&intent(now), &context(now), now).unwrap();
        assert!(decision.approved);
    }

    #[test]
    fn rejects_low_edge_slippage_liquidity_balance_size_and_exposure() {
        let now = 6_000_000;
        let mut engine = RiskEngine::new(config("candidate-rejects")).unwrap();
        let mut trade = intent(now);
        trade.expected_net_edge_bps = d("4");
        trade.estimated_slippage_bps = d("11");
        trade.available_liquidity = d("200");
        trade.available_liquidity_ratio = d("0.5");
        trade.starting_notional = d("501");
        trade.projected_peak_exposure = d("600");

        let mut ctx = context(now);
        ctx.account_balance = d("400");
        ctx.current_exposure = d("900");

        let decision = engine.evaluate(&trade, &ctx, now).unwrap();
        assert!(!decision.approved);
        assert!(decision
            .checks
            .iter()
            .any(|item| item.check == RiskCheck::MinimumNetEdge && !item.passed));
        assert!(decision
            .checks
            .iter()
            .any(|item| item.check == RiskCheck::MaximumExposure && !item.passed));
    }

    #[test]
    fn precision_mismatch_rejects_candidate() {
        let now = 7_000_000;
        let mut engine = RiskEngine::new(config("precision")).unwrap();
        let mut trade = intent(now);
        trade.legs[0].quantity = d("1.0005");

        let decision = engine.evaluate(&trade, &context(now), now).unwrap();
        assert!(!decision.approved);
        assert!(decision
            .checks
            .iter()
            .any(|item| item.check == RiskCheck::SymbolPrecision && !item.passed));
    }

    #[test]
    fn daily_loss_and_unhealthy_services_trip_breakers() {
        let now = 8_000_000;
        let mut daily = RiskEngine::new(config("daily-loss")).unwrap();
        let mut ctx = context(now);
        ctx.daily_realized_pnl = d("-50");
        assert!(!daily.evaluate(&intent(now), &ctx, now).unwrap().approved);

        let mut api = RiskEngine::new(config("api-health")).unwrap();
        let mut ctx = context(now);
        ctx.api_health.healthy = false;
        assert!(!api.evaluate(&intent(now), &ctx, now).unwrap().approved);

        let mut exchange = RiskEngine::new(config("exchange-health")).unwrap();
        let mut ctx = context(now);
        ctx.exchange_health.last_ok_ms = now - 5_001;
        assert!(!exchange.evaluate(&intent(now), &ctx, now).unwrap().approved);
    }

    #[test]
    fn newly_engaged_kill_switch_invalidates_existing_approval() {
        let now = 8_500_000;
        let mut engine = RiskEngine::new(config("post-approval-kill")).unwrap();
        let approval = engine
            .evaluate(&intent(now), &context(now), now)
            .unwrap()
            .into_approval()
            .unwrap();

        engine
            .engage_manual_kill_switch("emergency after approval", now + 10)
            .unwrap();

        let error = engine
            .validate_approval(&approval, "trade-1", now + 20)
            .unwrap_err();
        assert!(matches!(error, RiskError::GateClosed(_)));
    }

    #[test]
    fn negative_slippage_and_invalid_liquidity_ratio_are_rejected() {
        let now = 8_750_000;
        let mut engine = RiskEngine::new(config("invalid-metrics")).unwrap();
        let mut trade = intent(now);
        trade.estimated_slippage_bps = d("-1");
        trade.available_liquidity_ratio = d("1.1");

        let decision = engine.evaluate(&trade, &context(now), now).unwrap();
        assert!(!decision.approved);
        assert!(decision
            .checks
            .iter()
            .any(|item| item.check == RiskCheck::MaximumSlippage && !item.passed));
        assert!(decision
            .checks
            .iter()
            .any(|item| item.check == RiskCheck::AvailableLiquidity && !item.passed));
    }

    #[test]
    fn emergency_unwind_can_be_approved_while_kill_switch_is_active() {
        let now = 8_900_000;
        let cfg = config("emergency-unwind");
        let mut engine = RiskEngine::new(cfg).unwrap();
        engine
            .engage_manual_kill_switch("stop new risk", now)
            .unwrap();

        let approval = engine
            .approve_emergency_unwind(
                &EmergencyUnwindIntent {
                    trade_id: "unwind-1".to_string(),
                    exposure_asset: "BTC".to_string(),
                    base_asset: "USDT".to_string(),
                    exposure_notional: d("4.5"),
                    unwind_notional: d("4.5"),
                    market_data_timestamp_ms: now - 100,
                    api_health: context(now).api_health,
                    exchange_health: context(now).exchange_health,
                },
                now,
            )
            .unwrap();

        assert_eq!(approval.kind(), RiskApprovalKind::EmergencyUnwind);
        engine
            .validate_approval(&approval, "unwind-1", now + 20)
            .unwrap();
    }

    #[test]
    fn emergency_unwind_rejects_unknown_or_stale_exposure() {
        let now = 8_950_000;
        let mut engine = RiskEngine::new(config("emergency-invalid")).unwrap();
        let mut intent = EmergencyUnwindIntent {
            trade_id: "unwind-2".to_string(),
            exposure_asset: "ETH".to_string(),
            base_asset: "USDT".to_string(),
            exposure_notional: d("4"),
            unwind_notional: d("5"),
            market_data_timestamp_ms: now - 100,
            api_health: context(now).api_health,
            exchange_health: context(now).exchange_health,
        };
        assert!(engine.approve_emergency_unwind(&intent, now).is_err());

        intent.unwind_notional = d("4");
        intent.market_data_timestamp_ms = now - 2_001;
        assert!(engine.approve_emergency_unwind(&intent, now).is_err());
    }

    #[test]
    fn expired_approval_cannot_cross_execution_gate() {
        let now = 9_000_000;
        let mut engine = RiskEngine::new(config("approval-expiry")).unwrap();
        let approval = engine
            .evaluate(&intent(now), &context(now), now)
            .unwrap()
            .into_approval()
            .unwrap();

        let error = engine
            .validate_approval(&approval, "trade-1", now + 101)
            .unwrap_err();
        assert!(matches!(error, RiskError::GateClosed(_)));
    }
}
