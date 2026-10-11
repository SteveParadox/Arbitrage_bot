//! Authenticated worker under the existing observer supervisor and candidate journal.
use anyhow::{bail, Context, Result};
use authenticated::{DeploymentPermit, ExchangePlanner, Gate, Ledger, Provider, Venue};
use coordinator::{
    ConversionLeg, CoordinatorConfig, RiskEngineAuthorizer, RoutePlanner, ThreeLegCoordinator,
};
use event_bus::EventPublisher;
use execution::{BybitExecutionClient, ExecutionConfig, ExecutionMode};
use paper::{MatchSettings, PaperBooks, PaperPlanner, StopGuard};
use risk::{
    current_time_ms, load_risk_config, RiskContext, RiskEngine, ServiceHealth, TradeIntent,
};
use rust_decimal::Decimal;
use scanner::{ArbitrageScanRecord, ScanStatus, TriangleRoute};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{path::Path, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

pub struct Runtime {
    pub books: PaperBooks,
    pub ledger: Ledger,
    sender: mpsc::Sender<(String, ArbitrageScanRecord, TriangleRoute)>,
    stop: watch::Sender<bool>,
    worker: JoinHandle<Result<()>>,
}
impl Runtime {
    pub async fn start(root: &Path, events: EventPublisher, mode: ExecutionMode) -> Result<Self> {
        let account = std::env::var("ARB_EXECUTION_ACCOUNT")
            .context("explicit execution account required")?;
        let mock = std::env::var("ARB_EXECUTION_OFFLINE_MOCK").as_deref() == Ok("true");
        if runtime_clock::Clock::current().is_virtual()
            || std::env::var("ARB_PAPER_TRADING_ENABLED").as_deref() == Ok("true")
        {
            bail!("authenticated runtime cannot use paper authorization or virtual replay");
        }
        if std::env::var("ARB_OBSERVER_REPLAY_FILE").is_ok() && !mock {
            bail!("replay cannot access real private execution");
        }
        if mock
            && (std::env::var("ARB_ENV").as_deref() != Ok("development")
                || mode != ExecutionMode::Testnet)
        {
            bail!("offline mock requires development/testnet mode");
        }
        for name in ["BYBIT_API_KEY", "BYBIT_API_SECRET"] {
            if std::env::var(name).is_ok_and(|s| !s.is_empty()) {
                bail!("generic exchange credentials forbidden in managed execution");
            }
        }
        let permit_path = super::observer::configured_path(
            root,
            "ARB_EXECUTION_DEPLOYMENT_PERMIT",
            "data/execution/deployment-permit.json",
        );
        let permit = DeploymentPermit::load(&permit_path)?;
        permit.verify(&account, mode, current_time_ms(), mock)?;
        let mut risk_config = load_risk_config(super::observer::configured_path(
            root,
            "ARB_RISK_CONFIG",
            "shared/config/risk.json",
        ))?;
        let global_stop = StopGuard {
            control: risk_config.trading_control_file.clone(),
            kill: risk_config.kill_switch_file.clone(),
        };
        let gate = Gate {
            account: account.clone(),
            mode,
            mock,
            permit_path,
            stop: global_stop,
        };
        gate.check(true)?;
        let prefix = if mode == ExecutionMode::Testnet {
            "BYBIT_TESTNET"
        } else {
            "BYBIT_LIVE"
        };
        let (key, secret) = if mock {
            for p in ["BYBIT_TESTNET", "BYBIT_LIVE"] {
                for suffix in ["API_KEY", "API_SECRET"] {
                    if std::env::var(format!("{p}_{suffix}")).is_ok_and(|s| !s.is_empty()) {
                        bail!("offline mock refuses private credentials");
                    }
                }
            }
            ("mock-key".into(), "mock-secret".into())
        } else {
            if std::env::var(format!(
                "{}_API_KEY",
                if prefix == "BYBIT_TESTNET" {
                    "BYBIT_LIVE"
                } else {
                    "BYBIT_TESTNET"
                }
            ))
            .is_ok_and(|s| !s.is_empty())
            {
                bail!("conflicting exchange credential environment");
            }
            (
                std::env::var(format!("{prefix}_API_KEY"))?,
                std::env::var(format!("{prefix}_API_SECRET"))?,
            )
        };
        let mut cfg = ExecutionConfig::from_credentials(
            mode == ExecutionMode::Testnet,
            mode == ExecutionMode::Live,
            key,
            secret,
        )?;
        if mock {
            cfg = cfg.with_offline_mock(
                &std::env::var("ARB_EXECUTION_MOCK_URL").context("mock endpoint required")?,
            )?;
        }
        let client = BybitExecutionClient::new(cfg)?;
        let mut ledger =
            Ledger::connect(&std::env::var("ARB_DATABASE_URL")?, account.clone()).await?;
        ledger
            .claim(
                if mode == ExecutionMode::Testnet {
                    "testnet"
                } else {
                    "live"
                },
                &permit.exchange_uid,
                mock,
            )
            .await?;
        // Recovery only queries old IDs. It never replays submissions or enables exposure.
        client.verify_account(&permit.exchange_uid).await?;
        for (link, symbol) in ledger.pending_orders().await? {
            if let Some(state) = client.reconcile_order(&symbol, &link).await? {
                ledger.apply(&link, &state).await?;
            }
        }
        let wallet = client.sync_balances(&[]).await?;
        let healthy = ledger.reconcile(&wallet, current_time_ms(), 1000).await?;
        for cycle in ledger.active_cycles().await? {
            ledger.settle(&cycle,"RECOVERED",Some("startup confirms evidence; unresolved exposure quarantined without automatic new orders")).await?;
        }
        let unresolved = !ledger.active_cycles().await?.is_empty();
        let open = client.verify_no_open_orders().await.is_err();
        ledger
            .state(
                if healthy && !unresolved && !open {
                    "READY"
                } else {
                    "BLOCKED"
                },
                !healthy || unresolved || open,
                false,
            )
            .await?;
        let state_dir =
            super::observer::configured_path(root, "ARB_EXECUTION_STATE_DIR", "data/execution")
                .join(&account);
        std::fs::create_dir_all(&state_dir)?;
        risk_config.state_file = state_dir.join("risk.json");
        risk_config.trading_control_file = state_dir.join("control.json");
        // Global stop/kill still checked by Gate; local risk authorization is backed by account commands.
        super::write_control(
            &risk_config.trading_control_file,
            false,
            "restart requires explicit account start".into(),
            None,
        )?;
        let settings = MatchSettings {
            fee_rate: Decimal::from_str_exact(
                &std::env::var("ARB_EXECUTION_FEE_RATE").unwrap_or_else(|_| "0.001".into()),
            )?,
            latency_ms: 0,
            timeout_ms: 1000,
            max_age_ms: risk_config.max_market_data_age_ms,
            slippage_percent: Decimal::new(10, 2),
        };
        settings.validate()?;
        let books = PaperBooks::default();
        let planner = ExchangePlanner(PaperPlanner {
            books: books.clone(),
            settings,
        });
        let (sender, mut receiver) =
            mpsc::channel::<(String, ArbitrageScanRecord, TriangleRoute)>(32);
        let (stop, mut stopped) = watch::channel(false);
        let work_ledger = ledger.clone();
        let reload_path = super::observer::configured_path(
            root,
            "ARB_STRATEGY_RELOAD_FILE",
            "data/control/strategy_reload.json",
        );
        let initial_reload = std::fs::read_to_string(&reload_path).ok();
        let worker = tokio::spawn(async move {
            let ledger = work_ledger;
            let result:Result<()> = async {
                let mut risk=RiskEngine::new(risk_config.clone())?;
                let mut tick=tokio::time::interval(Duration::from_secs(1));
                let mut last_cycle=0;
                loop {
                    let (candidate,record,route)=tokio::select!{
                        _=stopped.changed()=>{ledger.state("STOPPING",true,false).await?;ledger.flush_events(&events).await?;return Ok(());},
                        _=tick.tick()=>{
                            ledger.heartbeat().await?;
                            ledger.flush_events(&events).await?;
                            if std::fs::read_to_string(&reload_path).ok()!=initial_reload {bail!("strategy reload requires controlled reconciliation-first restart");}
                            if gate.check(false).is_err() {ledger.state("BLOCKED",true,false).await?;}
                            if ledger.active_cycles().await?.is_empty() {
                                client.verify_no_open_orders().await?;
                                client.verify_account(&permit.exchange_uid).await?;
                                let wallet=client.sync_balances(&[]).await?;
                                if !ledger.reconcile(&wallet,current_time_ms(),1000).await? {ledger.state("BLOCKED",true,false).await?;}
                            }
                            continue;
                        },
                        item=receiver.recv()=>item.context("candidate queue closed")?,
                    };
                    let outcome:Result<()> = async {
                        events.ensure_critical_ready()?;paper::redis_ready().await?;gate.check(false)?;
                        if route.start_asset!="USDT"||record.route_id!=route.id||record.status!=ScanStatus::Complete||record.net_profitable!=Some(true) {bail!("candidate identity, denomination or profitability rejected");}
                        if current_time_ms().checked_sub(record.trigger_timestamp).is_none_or(|a|a>risk_config.max_market_data_age_ms) {bail!("candidate expired");}
                        if current_time_ms().saturating_sub(last_cycle)<1000 {bail!("account cooldown");}
                        client.verify_no_open_orders().await?;
                        let wallet=client.sync_balances(&[]).await?;
                        if !ledger.reconcile(&wallet,current_time_ms(),1000).await? {bail!("balance reconciliation discrepancy");}
                        let p=ledger.portfolio(current_time_ms(),1000).await?;
                        if !p.enabled {bail!("explicit account start required");}
                        let start=paper::decimal(record.start_amount.context("candidate start amount")?)?;
                        let mut output=start;let mut plans=Vec::new();
                        for leg in &route.legs {let plan=planner.plan_conversion(&route,&ConversionLeg::from(leg),output,"prevalidate".into(),false).await?;output=plan.estimated_output;plans.push(plan);}
                        let edge=(output-start)/start*Decimal::from(10000);
                        let id=format!("{:x}",Sha256::digest(format!("{}:{candidate}",ledger.account).as_bytes()));
                        super::write_control(&risk_config.trading_control_file,true,"authenticated database account start".into(),Some("account-start".into()))?;
                        let now=current_time_ms();let health=ServiceHealth{healthy:true,last_ok_ms:now,detail:"verified unleveraged exchange wallet and fenced journal".into()};
                        let intent=TradeIntent{trade_id:id.clone(),route_id:route.id.clone(),starting_asset:"USDT".into(),starting_notional:start,projected_peak_exposure:plans.iter().map(|p|p.estimated_notional_base).max().unwrap_or(start).max(start),expected_net_edge_bps:edge,estimated_slippage_bps:plans.iter().map(|p|p.estimated_slippage_bps).sum(),available_liquidity:start,available_liquidity_ratio:Decimal::ONE,market_data_timestamp_ms:plans.iter().map(|p|p.market_timestamp_ms).min().context("no executable plans")?,legs:plans.iter().map(|p|p.proposed_risk_leg()).collect()};
                        let context=RiskContext{account_balance:p.available,current_exposure:p.exposure,daily_realized_pnl:p.daily_pnl,api_health:health.clone(),exchange_health:health};
                        let decision=risk.evaluate(&intent,&context,now)?;
                        if !decision.approved {bail!("risk denied: {}",json!(decision));}
                        if !ledger.reserve(&id,&candidate,&route,start,json!({"decision":decision,"configuration_hash":record.configuration_hash,"market_fingerprint":record.market_fingerprint,"candidate":record,"risk_version":risk_config.version}),now,1000).await? {return Ok(());}
                        authenticated::checkpoint(&client,"after_reservation");
                        last_cycle=now;ledger.state("RUNNING",false,false).await?;
                        let venue=Venue{client:client.clone(),ledger:ledger.clone(),cycle:id.clone(),route:route.clone(),gate:gate.clone(),balance_age_ms:1000};
                        let provider=Provider{ledger:ledger.clone(),planner:planner.clone(),gate:gate.clone(),starting:start,edge,balance_age_ms:1000};
                        let authorizer=RiskEngineAuthorizer::new(provider,mode,mode==ExecutionMode::Live);
                        let mut coordinator=ThreeLegCoordinator::with_events(CoordinatorConfig{version:1,normal_slippage_tolerance_percent:Decimal::new(10,2),emergency_slippage_tolerance_percent:Decimal::new(50,2),max_unwind_attempts_per_asset:3,max_dust_notional_base:Decimal::ZERO},venue,planner.clone(),authorizer,events.clone())?;
                        let report=coordinator.execute_route_for_opportunity(&mut risk,&id,&route,start,&candidate).await;
                        // Journal is authoritative even if a coordinator event could not publish.
                        let snapshot=client.sync_balances(&[]).await?;
                        if !ledger.reconcile(&snapshot,current_time_ms(),1000).await? {bail!("post-execution wallet discrepancy");}
                        let (state,reason)=match report {Ok(r)=>{
                            if ledger.holdings(&id).await?!=r.holdings {bail!("coordinator/journal holdings disagree");}
                            (match r.status {coordinator::CoordinatorStatus::Completed|coordinator::CoordinatorStatus::CompletedWithResidualCleanup=>"COMPLETED",coordinator::CoordinatorStatus::NotStarted=>"NOT_STARTED",_=>"RECOVERED"},r.failure_reason)
                        },Err(e)=>("RECOVERED",Some(e.to_string()))};
                        authenticated::checkpoint(&client,"before_settlement");
                        if ledger.settle(&id,state,reason.as_deref()).await? {ledger.state("READY",false,false).await?;}
                        authenticated::checkpoint(&client,"after_settlement");
                        Ok(())
                    }.await;
                    if let Err(error)=outcome {
                        ledger.audit("execution.candidate_rejected",json!({"candidate_id":candidate,"reason":format!("{error:#}")})).await?;
                        if !ledger.active_cycles().await?.is_empty(){ledger.quarantine(&format!("{error:#}")).await?;}
                    }
                    ledger.flush_events(&events).await?;
                }
            }.await;
            if result.is_err() {
                let _ = ledger
                    .quarantine("worker failed; restart reconciliation required")
                    .await;
                let _ = ledger.state("FAILED", true, false).await;
            }
            result
        });
        Ok(Self {
            books,
            ledger,
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
            bail!("authenticated dispatcher offline");
        }
        self.sender
            .try_send((id, record, route))
            .map_err(|e| anyhow::anyhow!("bounded execution queue: {e}"))
    }
    pub async fn shutdown(self) -> Result<()> {
        self.stop.send(true)?;
        self.worker.await??;
        self.ledger.state("STOPPED", true, false).await
    }
}
