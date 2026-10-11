//! Bounded candidate dispatcher hosted by the same process as public observation.
use anyhow::{bail, Context, Result};
use coordinator::{
    ConversionLeg, CoordinatorConfig, RiskEngineAuthorizer, RoutePlanner, ThreeLegCoordinator,
};
use event_bus::EventPublisher;
use execution::ExecutionMode;
use paper::{
    decimal, Ledger, MatchSettings, PaperBooks, PaperPlanner, PaperVenue, Provider, StopGuard,
};
use risk::{
    current_time_ms, load_risk_config, ProposedOrderLeg, RiskContext, RiskEngine, ServiceHealth,
    TradeIntent,
};
use rust_decimal::Decimal;
use scanner::{ArbitrageScanRecord, ScanStatus, TriangleRoute};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::HashSet, path::Path};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

pub struct Runtime {
    pub books: PaperBooks,
    pub ledger: Ledger,
    pub planner: PaperPlanner,
    clock: runtime_clock::Clock,
    sender: mpsc::Sender<(String, ArbitrageScanRecord, TriangleRoute)>,
    stop: watch::Sender<bool>,
    worker: JoinHandle<Result<()>>,
}
impl Runtime {
    pub async fn start(root: &Path, events: EventPublisher) -> Result<Self> {
        paper::validate_mode(
            &std::env::var("ARB_TRADING_MODE").unwrap_or_default(),
            std::env::var("ARB_PAPER_TRADING_ENABLED").as_deref() == Ok("true"),
            std::env::var("ARB_LIVE_TRADING_ENABLED").as_deref() != Ok("false"),
        )?;
        let account = std::env::var("ARB_PAPER_ACCOUNT").unwrap_or_else(|_| "demo".into());
        if account.is_empty()
            || !account
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            bail!("invalid paper account identity");
        }
        let mut ledger = Ledger::connect(
            &std::env::var("ARB_DATABASE_URL").context("paper requires PostgreSQL URL")?,
            account.clone(),
        )
        .await?;
        while !ledger.account_exists().await? {
            events.publish(
                "paper.runtime_waiting_account",
                json!({"account":account,"execution_enabled":false}),
            );
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {},
                _ = tokio::signal::ctrl_c() => bail!("shutdown while waiting for virtual account"),
            }
        }
        ledger.claim_and_recover().await?;
        let books = PaperBooks::default();
        let shared_books = books.clone();
        let (sender, mut receiver) =
            mpsc::channel::<(String, ArbitrageScanRecord, TriangleRoute)>(32);
        let (stop, mut stopped) = watch::channel(false);
        let risk_path =
            super::observer::configured_path(root, "ARB_RISK_CONFIG", "shared/config/risk.json");
        let mut config = load_risk_config(risk_path)?;
        // Reuse all deterministic risk checks; only paper authorization/persistent breaker state is isolated.
        let state_dir = super::observer::configured_path(root, "ARB_PAPER_STATE_DIR", "data/paper")
            .join(&account);
        std::fs::create_dir_all(&state_dir)?;
        let global_control = config.trading_control_file.clone();
        let global_kill = config.kill_switch_file.clone();
        config.kill_switch_file = state_dir.join("KILL_SWITCH");
        config.trading_control_file = state_dir.join("control.json");
        config.state_file = state_dir.join("risk.json");
        let settings = MatchSettings {
            fee_rate: Decimal::from_str_exact(
                &std::env::var("ARB_PAPER_FEE_RATE").unwrap_or_else(|_| "0.001".into()),
            )?,
            latency_ms: std::env::var("ARB_PAPER_LATENCY_MS")
                .unwrap_or_else(|_| "50".into())
                .parse()?,
            timeout_ms: std::env::var("ARB_PAPER_ORDER_TIMEOUT_MS")
                .unwrap_or_else(|_| "1000".into())
                .parse()?,
            max_age_ms: config.max_market_data_age_ms,
            slippage_percent: Decimal::new(10, 2),
        };
        settings.validate()?;
        let cooldown: i64 = std::env::var("ARB_PAPER_ROUTE_COOLDOWN_MS")
            .unwrap_or_else(|_| "1000".into())
            .parse()?;
        if cooldown < 0 {
            bail!("negative cooldown");
        }
        let reload_path = super::observer::configured_path(
            root,
            "ARB_STRATEGY_RELOAD_FILE",
            "data/control/strategy_reload.json",
        );
        let initial_reload = std::fs::read_to_string(&reload_path).ok();
        let clock = runtime_clock::Clock::current();
        let work_clock = clock.clone();
        let published_ledger = ledger.clone();
        let published_planner = PaperPlanner {
            books: shared_books.clone(),
            settings: settings.clone(),
        };
        let worker = tokio::spawn(async move {
            work_clock.scope(async {
            let mut risk = RiskEngine::new(config.clone())?;
            let planner = PaperPlanner {
                books: shared_books.clone(),
                settings: settings.clone(),
            };
            let mut fingerprints = HashSet::new();
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                let work = tokio::select! {
                    _=stopped.changed()=>{ledger.disable().await?;return Ok(());},
                    _=tick.tick()=>{
                        if std::fs::read_to_string(&reload_path).ok()!=initial_reload {
                            ledger.disable().await?;
                            bail!("controlled strategy reload requires recovery-first managed restart");
                        }
                        if (StopGuard{control:global_control.clone(),kill:global_kill.clone()}).check().is_err() {ledger.disable().await?;}
                        ledger.heartbeat().await?;
                        if !work_clock.is_virtual() { refresh_valuations(&ledger, &planner).await?; }
                        continue;
                    },
                    work=receiver.recv()=>match work {Some(w)=>w,None=>return Ok(())},
                };
                let _clock_work = work_clock.admit_work().await?;
                let (candidate_id, record, route) = work;
                let id = format!("{}:{}", ledger.account, candidate_id);
                let outcome:Result<()> = async {
                    events.ensure_critical_ready()?;
                    paper::redis_ready().await?;
                    if let Err(e)= (StopGuard{control:global_control.clone(),kill:global_kill.clone()}).check() {ledger.disable().await?;return Err(e);}
                    ledger.heartbeat().await?;
                    ledger.reconcile().await?;
                    let portfolio=ledger.portfolio(&route.start_asset).await?;
                    if !portfolio.enabled||portfolio.blocked {bail!("PAPER_NOT_AUTHORIZED");}
                    if record.status!=ScanStatus::Complete||record.net_profitable!=Some(true) {bail!("NET_PROFITABILITY_OR_DATA_REJECTED");}
                    if current_time_ms().checked_sub(record.trigger_timestamp).is_none_or(|age|age>config.max_market_data_age_ms) {bail!("CANDIDATE_EXPIRED");}
                    if fingerprints.contains(&record.market_fingerprint) {bail!("UNCHANGED_MARKET_EVIDENCE");}
                    let start=decimal(record.start_amount.context("missing starting capital")?)?;
                    let edge=decimal(record.expected_net_return_bps.context("missing net edge")?)?;
                    let expected=decimal(record.expected_net_profit.context("missing net profit")?)?;
                    let mut amount=start;let mut planned=vec![];
                    for (i,leg) in route.legs.iter().enumerate() {
                        let plan=planner.plan_conversion(&route,&ConversionLeg::from(leg),amount,format!("validate-{i}"),false).await?;
                        amount=plan.estimated_output;planned.push(plan);
                    }
                    if amount<=start||(amount-start)/start*Decimal::from(10000)<config.min_net_edge_bps {bail!("DECIMAL_NET_EDGE_REJECTED");}
                    if risk::control::stop_pending(&config.trading_control_file) {bail!("STOP_PENDING");}
                    super::write_control(&config.trading_control_file,true,"paper database authorization".into(),None)?;
                    let now=current_time_ms();let health=ServiceHealth{healthy:true,last_ok_ms:now,detail:"PostgreSQL and synchronized public books".into()};
                    let projected_asset_exposure=planned.iter().map(|p|p.estimated_notional_base).max().unwrap_or(start).max(start);
                    let intent=TradeIntent{trade_id:id.clone(),route_id:route.id.clone(),starting_asset:route.start_asset.clone(),starting_notional:start,projected_peak_exposure:projected_asset_exposure,
                        expected_net_edge_bps:edge.min((amount-start)/start*Decimal::from(10000)),estimated_slippage_bps:planned.iter().map(|p|p.estimated_slippage_bps).sum(),available_liquidity:start,available_liquidity_ratio:Decimal::ONE,
                        market_data_timestamp_ms:planned.iter().map(|p|p.market_timestamp_ms).min().context("no plans")?,legs:planned.iter().map(|p|ProposedOrderLeg{symbol:p.request.symbol.clone(),quantity:p.request.requested_quantity,limit_price:None,rules:p.rules.clone()}).collect()};
                    let context=RiskContext{account_balance:portfolio.available,current_exposure:portfolio.exposure,daily_realized_pnl:portfolio.daily_pnl,api_health:health.clone(),exchange_health:health};
                    let stats=ledger.portfolio_stats(projected_asset_exposure).await?;
                    let decision=risk.evaluate_with_portfolio(&intent,&context,&stats,now)?;
                    let evidence_key=format!("{:x}", Sha256::digest(json!({"route":route.id,"config":record.configuration_hash,"amount":start.to_string(),"fees":settings.fee_rate.to_string(),"depth":record.legs.iter().map(|l|json!({"symbol":l.symbol,"quantity":l.execution.filled_base_quantity,"quote":l.execution.filled_quote_quantity,"best":l.execution.best_price,"worst":l.execution.worst_price})).collect::<Vec<_>>()}).to_string().as_bytes()));
                    let audit=json!({"evidence_key":evidence_key,"projected_asset_exposure":projected_asset_exposure.to_string(),"version":config.version,"candidate_id":candidate_id,"decision":decision,"candidate":record,"decimal_expected_profit":(amount-start).to_string()});
                    if !decision.approved {
                        ledger.reject(&candidate_id,"RUST_RISK_DENIED",audit.clone()).await?;
                        return Ok(());
                    }
                    paper::crash_checkpoint("before_reservation");
                    if !ledger.reserve(&id,&route,start,expected,audit,config.max_total_exposure,config.max_daily_loss,&config.paper_portfolio_limits,cooldown).await? {return Ok(());}
                    paper::crash_checkpoint("after_reservation");
                    fingerprints.insert(record.market_fingerprint.clone());
                    // Persisted uniqueness remains authoritative even if the bounded in-memory optimization resets.
                    if fingerprints.len()>4096 {fingerprints.clear();}
                    let stop_guard=StopGuard{control:global_control.clone(),kill:global_kill.clone()};
                    let venue=PaperVenue{cycle_id:id.clone(),route:route.clone(),stop_guard:stop_guard.clone(),books:shared_books.clone(),ledger:ledger.clone(),settings:settings.clone()};
                    let authorizer=RiskEngineAuthorizer::new(Provider{ledger:ledger.clone(),edge,starting:start,planner:planner.clone(),stop_guard,portfolio_limits:config.paper_portfolio_limits.clone()},ExecutionMode::Paper,false);
                    let mut coordinator=ThreeLegCoordinator::with_events(CoordinatorConfig{version:1,normal_slippage_tolerance_percent:settings.slippage_percent,emergency_slippage_tolerance_percent:Decimal::new(50,2),max_unwind_attempts_per_asset:3,max_dust_notional_base:Decimal::ZERO},venue,planner.clone(),authorizer,events.clone())?;
                    let report=coordinator.execute_route(&mut risk,&id,&route,start).await;
                    paper::crash_checkpoint("before_settlement");
                    match report {
                        Ok(r)=>{
                            let durable=ledger.cycle_holdings(&id).await?;
                            if durable!=r.holdings {ledger.disable().await?;bail!("COORDINATOR_LEDGER_MISMATCH");}
                            ledger.settle(&id,match r.status {coordinator::CoordinatorStatus::Completed|coordinator::CoordinatorStatus::CompletedWithResidualCleanup=>"COMPLETED",coordinator::CoordinatorStatus::RecoveredByUnwind=>"RECOVERED",_=>"FAILED"},r.failure_reason.as_deref()).await?;
                        },
                        Err(e)=>{ledger.settle(&id,"FAILED",Some(&e.to_string())).await?;},
                    }
                    paper::crash_checkpoint("after_settlement");
                    Ok(())
                }.await;
                if let Err(e) = outcome {
                    ledger
                        .reject(&candidate_id, rejection_code(&e.to_string()), json!({"reason":format!("{e:#}"),"risk_configuration_version":config.version}))
                        .await?;
                }
            }
        }).await
        });
        Ok(Self {
            books,
            ledger: published_ledger,
            planner: published_planner,
            clock,
            sender,
            stop,
            worker,
        })
    }
    pub fn is_finished(&self) -> bool {
        self.worker.is_finished()
    }
    pub fn dispatch(
        &self,
        id: String,
        record: ArbitrageScanRecord,
        route: TriangleRoute,
    ) -> Result<()> {
        if self.worker.is_finished() {
            bail!("paper dispatcher is offline");
        }
        self.clock.queue();
        if let Err(error) = self.sender.try_send((id, record, route)) {
            self.clock.unqueue();
            return Err(error).context("paper execution queue full or closed");
        }
        Ok(())
    }
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.stop.send(true);
        self.worker.await??;
        Ok(())
    }
}

fn rejection_code(reason: &str) -> &'static str {
    for code in [
        "GLOBAL_STOP_OR_KILL_SWITCH",
        "PAPER_NOT_AUTHORIZED",
        "NET_PROFITABILITY_OR_DATA_REJECTED",
        "CANDIDATE_EXPIRED",
        "UNCHANGED_MARKET_EVIDENCE",
        "DECIMAL_NET_EDGE_REJECTED",
        "STOP_PENDING",
        "CONCURRENT_EXPOSURE_LIMIT",
        "DAILY_LOSS_LIMIT",
        "ROUTE_COOLDOWN",
        "INSUFFICIENT_CAPITAL",
        "ACCOUNT_START_ASSET_MISMATCH",
        "COORDINATOR_LEDGER_MISMATCH",
    ] {
        if reason.contains(code) {
            return code;
        }
    }
    "DEPENDENCY_OR_VALIDATION_REJECTED"
}

pub async fn refresh_valuations(ledger: &Ledger, planner: &PaperPlanner) -> Result<()> {
    let (start, holdings, route) = ledger.valuation_inputs().await?;
    for (asset, amount) in holdings {
        let now = current_time_ms();
        let (value, mut details) = if asset == start || amount == Decimal::ZERO {
            (
                Some(amount),
                json!({"valuation_source":"paper_ledger_cash","market_timestamp_ms":now}),
            )
        } else {
            match &route {
                Some(route) => match planner.valuation(route, &asset, amount).await {
                    Ok((value, details)) => (Some(value), details),
                    Err(error) => (None, json!({"missing_evidence":error.to_string()})),
                },
                None => (None, json!({"missing_evidence":"no durable route"})),
            }
        };
        details["start_asset"] = json!(start);
        details["max_age_ms"] = json!(planner.settings.max_age_ms);
        ledger
            .record_valuation(&asset, amount, value, details)
            .await?;
    }
    Ok(())
}
