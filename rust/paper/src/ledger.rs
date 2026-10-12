use crate::FillResult;
use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;
use tokio_postgres::{Client, Transaction};

#[derive(Clone)]
pub struct Ledger {
    client: Arc<Mutex<Client>>,
    pub account: String,
    pub epoch: i64,
}
#[derive(Clone, Debug)]
pub struct Portfolio {
    pub enabled: bool,
    pub blocked: bool,
    pub available: Decimal,
    pub exposure: Decimal,
    pub daily_pnl: Decimal,
    pub active: i64,
}
fn d(text: &str) -> Result<Decimal> {
    Ok(Decimal::from_str_exact(text)?)
}
fn holdings(v: &Value) -> Result<BTreeMap<String, Decimal>> {
    v.as_object()
        .context("invalid durable holdings")?
        .iter()
        .map(|(k, v)| {
            Ok((
                k.clone(),
                d(v.as_str().context("holdings must be decimal strings")?)?,
            ))
        })
        .collect()
}
fn values(h: &BTreeMap<String, Decimal>) -> Value {
    json!(h
        .iter()
        .map(|(k, v)| (k.clone(), v.to_string()))
        .collect::<BTreeMap<_, _>>())
}
fn financial_ms() -> Option<i64> {
    runtime_clock::Clock::current()
        .is_virtual()
        .then(|| runtime_clock::now_ms() as i64)
}
async fn event(
    tx: &Transaction<'_>,
    account: &str,
    cycle: Option<&str>,
    kind: &str,
    payload: Value,
) -> Result<()> {
    let mut payload = payload;
    infrastructure::redact_value(&mut payload);
    tx.execute(
        "INSERT INTO paper_events(account_id,cycle_id,kind,payload,created_at) VALUES($1,$2,$3,$4,COALESCE(to_timestamp($5::bigint/1000.0),now()))",
        &[&account, &cycle, &kind, &payload, &financial_ms()],
    )
    .await?;
    Ok(())
}
#[allow(clippy::too_many_arguments)]
async fn transfer(
    tx: &Transaction<'_>,
    account: &str,
    cycle: Option<&str>,
    operation: &str,
    asset: &str,
    from: &str,
    to: &str,
    amount: Decimal,
) -> Result<()> {
    if amount == Decimal::ZERO {
        return Ok(());
    }
    if amount < Decimal::ZERO {
        bail!("negative ledger transfer");
    }
    tx.execute("INSERT INTO paper_ledger(account_id,cycle_id,operation_id,asset,debit_scope,credit_scope,amount,created_at) VALUES($1,$2,$3,$4,$5,$6,$7::text::numeric,COALESCE(to_timestamp($8::bigint/1000.0),now()))",
        &[&account,&cycle,&operation,&asset,&from,&to,&amount.to_string(),&financial_ms()]).await?;
    Ok(())
}
impl Ledger {
    pub async fn connect(url: &str, account: String) -> Result<Self> {
        let mut config: tokio_postgres::Config = infrastructure::database_config(url)?;
        config.application_name(format!("paper:{account}"));
        config.connect_timeout(std::time::Duration::from_secs(3));
        // Database contention must fail closed instead of hanging the dispatcher
        // and graceful shutdown indefinitely. Server-side limits also roll back writes.
        let options = format!("{} -c lock_timeout=2000 -c statement_timeout=5000 -c idle_in_transaction_session_timeout=10000",
                              config.get_options().unwrap_or_default());
        config.options(options);
        let client = infrastructure::connect_database(config).await?;
        if std::env::var("ARB_ENV").as_deref() == Ok("production") {
            let unsafe_role: bool = client.query_one("SELECT rolsuper OR rolcreatedb OR rolcreaterole FROM pg_roles WHERE rolname=current_user", &[]).await?.get(0);
            if unsafe_role {
                bail!("production paper runtime requires a restricted PostgreSQL role");
            }
        }
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            account,
            epoch: 0,
        })
    }
    pub async fn audit(&self, kind: &str, payload: Value) -> Result<()> {
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        let epoch: i64 = tx
            .query_one(
                "SELECT epoch FROM paper_accounts WHERE id=$1 FOR UPDATE",
                &[&self.account],
            )
            .await?
            .get(0);
        if epoch != self.epoch {
            bail!("runtime fenced");
        }
        event(&tx, &self.account, None, kind, payload).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn has_financial_history(&self) -> Result<bool> {
        Ok(self
            .client
            .lock()
            .await
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM paper_cycles WHERE account_id=$1)",
                &[&self.account],
            )
            .await?
            .get(0))
    }
    pub async fn verify_replay_finished(&self) -> Result<()> {
        self.disable().await?;
        self.reconcile().await?;
        let pending: bool = self.client.lock().await.query_one("SELECT EXISTS(SELECT 1 FROM paper_cycles WHERE account_id=$1 AND NOT settled) OR EXISTS(SELECT 1 FROM paper_orders o JOIN paper_cycles c ON c.id=o.cycle_id WHERE c.account_id=$1 AND o.state='SUBMITTING')", &[&self.account]).await?.get(0);
        if pending {
            bail!("replay ended with unresolved financial work");
        }
        Ok(())
    }
    pub async fn account_exists(&self) -> Result<bool> {
        Ok(self
            .client
            .lock()
            .await
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM paper_accounts WHERE id=$1)",
                &[&self.account],
            )
            .await?
            .get(0))
    }
    /// Read-only synchronization for development replay fixtures. Financial
    /// authority remains with the runtime owner and its fenced transactions.
    pub async fn pending_leg_exists(&self, leg: u32) -> Result<bool> {
        let row = self.client.lock().await.query_one(
            "SELECT EXISTS(SELECT 1 FROM paper_orders o JOIN paper_cycles c ON c.id=o.cycle_id WHERE c.account_id=$1 AND o.intent->>'leg_number'=$2 AND o.result IS NULL)",
            &[&self.account, &leg.to_string()],
        ).await?;
        Ok(row.get(0))
    }
    /// Session-scoped advisory ownership plus epoch fencing; connection loss loses both permission and lock.
    pub async fn claim_and_recover(&mut self) -> Result<()> {
        let mut c = self.client.lock().await;
        let locked: bool = c
            .query_one(
                "SELECT pg_try_advisory_lock(hashtextextended($1,8149))",
                &[&self.account],
            )
            .await?
            .get(0);
        if !locked {
            bail!("paper account already has an authoritative runtime");
        }
        let tx = c.transaction().await?;
        let configuration: Value = tx
            .query_one(
                "SELECT config FROM paper_accounts WHERE id=$1 FOR UPDATE",
                &[&self.account],
            )
            .await?
            .get(0);
        let clock = runtime_clock::Clock::current();
        let mode = if clock.is_virtual() {
            "virtual"
        } else {
            "wall"
        };
        if configuration["runtime_clock"]
            .as_str()
            .is_some_and(|old| old != mode)
        {
            bail!("account financial clock mode is immutable");
        }
        let history: bool = tx
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM paper_cycles WHERE account_id=$1)",
                &[&self.account],
            )
            .await?
            .get(0);
        if clock.is_virtual() && history && configuration["runtime_clock"] != "virtual" {
            bail!("cannot replay an account with untagged financial history");
        }
        if clock.is_virtual() && history {
            let restored: Option<i64> = tx.query_one("SELECT max(t) FROM (SELECT (extract(epoch FROM greatest(created_at,completed_at))*1000)::bigint t FROM paper_cycles WHERE account_id=$1 UNION ALL SELECT (o.result->>'executed_at_ms')::bigint FROM paper_orders o JOIN paper_cycles c ON c.id=o.cycle_id WHERE c.account_id=$1 AND o.result IS NOT NULL) clocks", &[&self.account]).await?.get(0);
            if let Some(restored) = restored {
                clock.advance(clock.now_ms().max(restored.try_into()?), true)?;
            }
        }
        tx.execute(
            "UPDATE paper_accounts SET config=jsonb_set(config,'{runtime_clock}',$2) WHERE id=$1",
            &[&self.account, &json!(mode)],
        )
        .await?;
        self.epoch=tx.query_one("UPDATE paper_accounts SET enabled=false,recovering=true,epoch=epoch+1,heartbeat=NULL WHERE id=$1 RETURNING epoch",&[&self.account]).await?.get(0);
        event(
            &tx,
            &self.account,
            None,
            "paper.runtime_recovery_started",
            json!({"epoch":self.epoch}),
        )
        .await?;
        tx.commit().await?;
        drop(c);
        let unfinished=self.client.lock().await.query("SELECT id FROM paper_cycles WHERE account_id=$1 AND NOT settled ORDER BY created_at",&[&self.account]).await?;
        for row in unfinished {
            let id: String = row.get(0);
            self.settle(
                &id,
                "RECOVERED",
                Some("restart: unresolved inventory preserved; operator review required"),
            )
            .await?;
        }
        self.reconcile().await?;
        self.client.lock().await.execute("UPDATE paper_accounts SET recovering=false,heartbeat=now() WHERE id=$1 AND epoch=$2", &[&self.account,&self.epoch]).await?;
        Ok(())
    }
    pub async fn portfolio(&self, asset: &str) -> Result<Portfolio> {
        let c = self.client.lock().await;
        let r=c.query_one("SELECT (a.enabled AND NOT a.recovering),a.blocked,COALESCE(b.available,0)::text,(SELECT COALESCE(sum(starting),0)::text FROM paper_cycles WHERE account_id=$1 AND NOT settled),(SELECT COALESCE(sum(realized_pnl),0)::text FROM paper_cycles WHERE account_id=$1 AND completed_at>=(date_trunc('day',COALESCE(to_timestamp($3::bigint/1000.0),now()) AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')),(SELECT count(*) FROM paper_cycles WHERE account_id=$1 AND NOT settled) FROM paper_accounts a LEFT JOIN paper_balances b ON b.account_id=a.id AND b.asset=$2 WHERE a.id=$1",&[&self.account,&asset,&financial_ms()]).await?;
        Ok(Portfolio {
            enabled: r.get(0),
            blocked: r.get(1),
            available: d(r.get(2))?,
            exposure: d(r.get(3))?,
            daily_pnl: d(r.get(4))?,
            active: r.get(5),
        })
    }
    pub async fn valuation_inputs(
        &self,
    ) -> Result<(
        String,
        BTreeMap<String, Decimal>,
        Option<scanner::TriangleRoute>,
    )> {
        let mut c = self.client.lock().await;
        let tx = c
            .build_transaction()
            .read_only(true)
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .start()
            .await?;
        let config: Value = tx
            .query_one(
                "SELECT config FROM paper_accounts WHERE id=$1",
                &[&self.account],
            )
            .await?
            .get(0);
        let start = config["start_asset"]
            .as_str()
            .context("account starting asset missing")?
            .to_string();
        let mut h = BTreeMap::new();
        for row in tx
            .query(
                "SELECT asset,available::text FROM paper_balances WHERE account_id=$1",
                &[&self.account],
            )
            .await?
        {
            h.insert(row.get::<_, String>(0), d(row.get(1))?);
        }
        for row in tx
            .query(
                "SELECT holdings FROM paper_cycles WHERE account_id=$1 AND NOT settled",
                &[&self.account],
            )
            .await?
        {
            for (asset, amount) in holdings(&row.get::<_, Value>(0))? {
                *h.entry(asset).or_default() += amount;
            }
        }
        let route=tx.query_opt("SELECT route FROM paper_cycles WHERE account_id=$1 ORDER BY created_at DESC LIMIT 1", &[&self.account]).await?
            .map(|row|serde_json::from_value(row.get::<_,Value>(0))).transpose()?;
        tx.commit().await?;
        Ok((start, h, route))
    }
    pub async fn record_valuation(
        &self,
        asset: &str,
        amount: Decimal,
        value: Option<Decimal>,
        details: Value,
    ) -> Result<()> {
        self.client.lock().await.execute("INSERT INTO paper_valuations(account_id,asset,amount,estimated_value,details,updated_at) SELECT $1,$3,$4::text::numeric,$5::text::numeric,$6,COALESCE(to_timestamp($7::bigint/1000.0),now()) FROM paper_accounts WHERE id=$1 AND epoch=$2 ON CONFLICT(account_id,asset) DO UPDATE SET amount=EXCLUDED.amount,estimated_value=EXCLUDED.estimated_value,details=EXCLUDED.details,updated_at=EXCLUDED.updated_at", &[&self.account,&self.epoch,&asset,&amount.to_string(),&value.map(|v|v.to_string()),&details,&financial_ms()]).await?;
        Ok(())
    }
    pub async fn portfolio_stats(&self, projected: Decimal) -> Result<risk::PortfolioStats> {
        let rows=self.client.lock().await.query("SELECT realized_pnl::text FROM paper_cycles WHERE account_id=$1 AND realized_pnl IS NOT NULL ORDER BY completed_at,id", &[&self.account]).await?;
        stats(&rows, projected)
    }
    pub async fn heartbeat(&self) -> Result<()> {
        let n = self
            .client
            .lock()
            .await
            .execute(
                "UPDATE paper_accounts SET heartbeat=now() WHERE id=$1 AND epoch=$2",
                &[&self.account, &self.epoch],
            )
            .await?;
        if n != 1 {
            bail!("runtime fenced");
        }
        Ok(())
    }
    pub async fn disable(&self) -> Result<()> {
        self.client
            .lock()
            .await
            .execute(
                "UPDATE paper_accounts SET enabled=false WHERE id=$1 AND epoch=$2",
                &[&self.account, &self.epoch],
            )
            .await?;
        Ok(())
    }
    /// An exited worker must not retain a fresh eligibility heartbeat.
    pub async fn offline(&self) -> Result<()> {
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        if tx
            .execute(
                "UPDATE paper_accounts SET enabled=false,heartbeat=NULL WHERE id=$1 AND epoch=$2",
                &[&self.account, &self.epoch],
            )
            .await?
            != 1
        {
            bail!("offline worker fenced");
        }
        event(
            &tx,
            &self.account,
            None,
            "paper.runtime_offline",
            json!({"epoch":self.epoch}),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn cycle_holdings(&self, id: &str) -> Result<BTreeMap<String, Decimal>> {
        let r = self
            .client
            .lock()
            .await
            .query_one(
                "SELECT holdings FROM paper_cycles WHERE id=$1 AND account_id=$2",
                &[&id, &self.account],
            )
            .await?;
        holdings(&r.get::<_, Value>(0))
    }
    pub async fn reject(&self, candidate: &str, reason: &str, detail: Value) -> Result<()> {
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        event(
            &tx,
            &self.account,
            None,
            "candidate.rejected",
            json!({"candidate_id":candidate,"reason_code":reason,"detail":detail}),
        )
        .await?;
        if matches!(
            reason,
            "INSUFFICIENT_CAPITAL" | "CONCURRENT_EXPOSURE_LIMIT" | "CAPITAL_RESERVATION_FAILED"
        ) {
            event(
                &tx,
                &self.account,
                None,
                "capital.reservation_rejected",
                json!({"candidate_id":candidate,"reason_code":reason}),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn reserve(
        &self,
        id: &str,
        route: &scanner::TriangleRoute,
        starting: Decimal,
        expected: Decimal,
        risk: Value,
        max_exposure: Decimal,
        max_loss: Decimal,
        portfolio_limits: &risk::PortfolioLimits,
        cooldown_ms: i64,
    ) -> Result<bool> {
        infrastructure::ensure_new_exposure_allowed()?;
        if starting <= Decimal::ZERO {
            bail!("invalid starting capital");
        }
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        let row = tx
            .query_one(
                "SELECT (enabled AND NOT recovering),blocked,epoch,config FROM paper_accounts WHERE id=$1 FOR UPDATE",
                &[&self.account],
            )
            .await?;
        let account_config: Value = row.get(3);
        if account_config["start_asset"].as_str() != Some(&route.start_asset) {
            bail!("ACCOUNT_START_ASSET_MISMATCH");
        }
        if !row.get::<_, bool>(0) || row.get::<_, bool>(1) || row.get::<_, i64>(2) != self.epoch {
            bail!("paper authorization or ownership denied");
        }
        let evidence = risk["evidence_key"].as_str().unwrap_or(id).to_string();
        let candidate = risk["candidate_id"].as_str().unwrap_or(id).to_string();
        if tx
            .query_opt(
                "SELECT id FROM paper_cycles WHERE account_id=$1 AND evidence_key=$2",
                &[&self.account, &evidence],
            )
            .await?
            .is_some()
        {
            event(
                &tx,
                &self.account,
                None,
                "candidate.rejected",
                json!({"candidate_id":candidate,"reason_code":"UNCHANGED_ECONOMIC_EVIDENCE"}),
            )
            .await?;
            tx.commit().await?;
            return Ok(false);
        }
        if tx
            .query_opt(
                "SELECT id FROM paper_cycles WHERE account_id=$1 AND candidate_id=$2",
                &[&self.account, &candidate],
            )
            .await?
            .is_some()
        {
            event(
                &tx,
                &self.account,
                None,
                "candidate.rejected",
                json!({"candidate_id":candidate,"reason_code":"DUPLICATE_CANDIDATE"}),
            )
            .await?;
            tx.commit().await?;
            return Ok(false);
        }
        let active=tx.query_one("SELECT count(*),COALESCE(sum(starting),0)::text FROM paper_cycles WHERE account_id=$1 AND NOT settled",&[&self.account]).await?;
        let projected = risk["projected_asset_exposure"]
            .as_str()
            .map(d)
            .transpose()?
            .unwrap_or(starting);
        // One authoritative sequential coordinator per account. Multiple accounts can run concurrently.
        if active.get::<_, i64>(0) > 0 || d(active.get(1))? + projected > max_exposure {
            bail!("CONCURRENT_EXPOSURE_LIMIT");
        }
        let history=tx.query("SELECT realized_pnl::text FROM paper_cycles WHERE account_id=$1 AND realized_pnl IS NOT NULL ORDER BY completed_at,id", &[&self.account]).await?;
        let checks = risk::portfolio_checks(portfolio_limits, &stats(&history, projected)?);
        if checks.iter().any(|c| !c.passed) {
            bail!("PORTFOLIO_LIMIT: {}", json!(checks));
        }
        let pnl=tx.query_one("SELECT COALESCE(sum(realized_pnl),0)::text FROM paper_cycles WHERE account_id=$1 AND completed_at>=(date_trunc('day',COALESCE(to_timestamp($2::bigint/1000.0),now()) AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')",&[&self.account,&financial_ms()]).await?;
        if d(pnl.get(0))? <= -max_loss {
            bail!("DAILY_LOSS_LIMIT");
        }
        if tx.query_one("SELECT EXISTS(SELECT 1 FROM paper_cycles WHERE account_id=$1 AND route_id=$2 AND created_at > COALESCE(to_timestamp($4::bigint/1000.0),now())-($3::bigint*interval '1 millisecond'))",&[&self.account,&route.id,&cooldown_ms,&financial_ms()]).await?.get::<_,bool>(0) {bail!("ROUTE_COOLDOWN");}
        let n=tx.execute("UPDATE paper_balances SET available=available-$3::text::numeric,reserved=reserved+$3::text::numeric,version=version+1 WHERE account_id=$1 AND asset=$2 AND available >= $3::text::numeric",&[&self.account,&route.start_asset,&starting.to_string()]).await?;
        if n != 1 {
            bail!("INSUFFICIENT_CAPITAL");
        }
        let h = json!({route.start_asset.clone():starting.to_string()});
        tx.execute("INSERT INTO paper_cycles(id,account_id,candidate_id,evidence_key,route_id,start_asset,starting,expected_profit,epoch,state,holdings,route,risk,created_at) VALUES($1,$2,$11,$12,$3,$4,$5::text::numeric,$6::text::numeric,$7,'RESERVED',$8,$9,$10,COALESCE(to_timestamp($13::bigint/1000.0),now()))",&[&id,&self.account,&route.id,&route.start_asset,&starting.to_string(),&expected.to_string(),&self.epoch,&h,&json!(route),&risk,&candidate,&evidence,&financial_ms()]).await?;
        transfer(
            &tx,
            &self.account,
            Some(id),
            &format!("{id}:reservation"),
            &route.start_asset,
            "available",
            "cycle",
            starting,
        )
        .await?;
        event(
            &tx,
            &self.account,
            Some(id),
            "capital.reserved",
            json!({"amount":starting.to_string(),"risk":risk}),
        )
        .await?;
        event(
            &tx,
            &self.account,
            Some(id),
            "candidate.approved",
            json!({"candidate_id":candidate,"risk":risk}),
        )
        .await?;
        event(
            &tx,
            &self.account,
            Some(id),
            "paper.cycle_started",
            json!({"simulated":true,"reservation_id":id}),
        )
        .await?;
        tx.commit().await?;
        Ok(true)
    }
    pub async fn order_intent(
        &self,
        cycle: &str,
        id: &str,
        intent: &Value,
    ) -> Result<Option<FillResult>> {
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        let a = tx
            .query_one(
                "SELECT epoch,enabled,blocked FROM paper_accounts WHERE id=$1 FOR UPDATE",
                &[&self.account],
            )
            .await?;
        if a.get::<_, i64>(0) != self.epoch {
            bail!("runtime fenced");
        }
        if let Some(r) = tx
            .query_opt(
                "SELECT intent,result FROM paper_orders WHERE id=$1 AND cycle_id=$2",
                &[&id, &cycle],
            )
            .await?
        {
            let mut old: Value = r.get(0);
            let mut comparable = intent.clone();
            if let Some(object) = old.as_object_mut() {
                object.remove("submission_book");
            }
            if let Some(object) = comparable.as_object_mut() {
                object.remove("submission_book");
            }
            if old != comparable {
                bail!("conflicting order identity");
            }
            let result: Option<Value> = r.get(1);
            if let Some(result) = result {
                return Ok(Some(serde_json::from_value(result)?));
            }
            bail!("outstanding intent requires recovery; never resubmit");
        }
        let r=tx.query_one("SELECT state,settled,epoch FROM paper_cycles WHERE id=$1 AND account_id=$2 FOR UPDATE",&[&cycle,&self.account]).await?;
        if r.get::<_, bool>(1) || r.get::<_, i64>(2) != self.epoch {
            bail!("cycle settled or fenced");
        }
        if !matches!(r.get::<_, String>(0).as_str(), "RESERVED" | "EXECUTING") {
            bail!("invalid cycle transition to execution");
        }
        let risk_reducing = intent["risk_reducing"].as_bool().unwrap_or(false);
        if (!a.get::<_, bool>(1) || a.get::<_, bool>(2)) && !risk_reducing {
            bail!("new exposure disabled");
        }
        tx.execute(
            "INSERT INTO paper_orders(id,cycle_id,intent,state,created_at) VALUES($1,$2,$3,'SUBMITTING',COALESCE(to_timestamp($4::bigint/1000.0),now()))",
            &[&id, &cycle, &intent, &financial_ms()],
        )
        .await?;
        tx.execute("UPDATE paper_cycles SET state='EXECUTING' WHERE id=$1 AND state IN ('RESERVED','EXECUTING')",&[&cycle]).await?;
        if risk_reducing {
            event(
                &tx,
                &self.account,
                Some(cycle),
                "paper.cycle_unwinding",
                json!({"order_id":id,"intent":intent}),
            )
            .await?;
        }
        event(
            &tx,
            &self.account,
            Some(cycle),
            "paper.leg_submitted",
            json!({"order_id":id,"intent":intent}),
        )
        .await?;
        tx.commit().await?;
        Ok(None)
    }
    pub async fn budget(&self, cycle: &str, asset: &str) -> Result<Decimal> {
        let r = self
            .client
            .lock()
            .await
            .query_one(
                "SELECT holdings FROM paper_cycles WHERE id=$1 AND account_id=$2 AND NOT settled",
                &[&cycle, &self.account],
            )
            .await?;
        Ok(*holdings(&r.get::<_, Value>(0))?
            .get(asset)
            .unwrap_or(&Decimal::ZERO))
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn fill(
        &self,
        cycle: &str,
        id: &str,
        from: &str,
        to: &str,
        spent: Decimal,
        gross: Decimal,
        result: &FillResult,
    ) -> Result<()> {
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        let a = tx
            .query_one(
                "SELECT epoch FROM paper_accounts WHERE id=$1 FOR UPDATE",
                &[&self.account],
            )
            .await?;
        if a.get::<_, i64>(0) != self.epoch {
            bail!("runtime fenced");
        }
        let row=tx.query_one("SELECT holdings,settled FROM paper_cycles WHERE id=$1 AND account_id=$2 FOR UPDATE",&[&cycle,&self.account]).await?;
        if row.get::<_, bool>(1) {
            bail!("settled cycle cannot fill");
        }
        let order = tx
            .query_one(
                "SELECT result,intent FROM paper_orders WHERE id=$1 AND cycle_id=$2 FOR UPDATE",
                &[&id, &cycle],
            )
            .await?;
        if let Some(old) = order.get::<_, Option<Value>>(0) {
            if old != json!(result) {
                bail!("fill conflict");
            }
            return Ok(());
        }
        let intent: Value = order.get(1);
        let buy = intent["side"].as_str() == Some("Buy");
        let expected_spent = if buy {
            result.quote_value
        } else {
            result.quantity
        };
        let expected_gross = if buy {
            result.quantity
        } else {
            result.quote_value
        };
        if from == to
            || intent["from"].as_str() != Some(from)
            || intent["to"].as_str() != Some(to)
            || spent != expected_spent
            || gross != expected_gross
            || result.fee_currency != to
            || result.quantity < Decimal::ZERO
            || result.remaining < Decimal::ZERO
            || result.requested
                != d(intent["quantity"]
                    .as_str()
                    .context("missing intent quantity")?)?
            || result.quantity + result.remaining != result.requested
        {
            bail!("fill receipt does not reconcile with durable order intent");
        }
        let mut h = holdings(&row.get::<_, Value>(0))?;
        let balance = h.entry(from.into()).or_default();
        if spent < Decimal::ZERO || *balance < spent {
            bail!("fill overspends cycle holding");
        }
        *balance -= spent;
        *h.entry(to.into()).or_default() += gross;
        let fee_balance = h.entry(result.fee_currency.clone()).or_default();
        if result.fee < Decimal::ZERO || *fee_balance < result.fee {
            bail!("fee exceeds holdings");
        }
        *fee_balance -= result.fee;
        transfer(
            &tx,
            &self.account,
            Some(cycle),
            id,
            from,
            "cycle",
            "simulated_venue",
            spent,
        )
        .await?;
        transfer(
            &tx,
            &self.account,
            Some(cycle),
            id,
            to,
            "simulated_venue",
            "cycle",
            gross,
        )
        .await?;
        transfer(
            &tx,
            &self.account,
            Some(cycle),
            id,
            &result.fee_currency,
            "cycle",
            "fees",
            result.fee,
        )
        .await?;
        let quote_asset = if intent["side"].as_str() == Some("Buy") {
            from
        } else {
            to
        };
        transfer(
            &tx,
            &self.account,
            Some(cycle),
            id,
            quote_asset,
            "simulated_venue",
            "rounding_dust",
            result.quote_rounding_dust,
        )
        .await?;
        tx.execute(
            "UPDATE paper_cycles SET holdings=$2 WHERE id=$1",
            &[&cycle, &values(&h)],
        )
        .await?;
        tx.execute(
            "UPDATE paper_orders SET result=$2,state='TERMINAL',completed_at=COALESCE(to_timestamp($3::bigint/1000.0),now()) WHERE id=$1",
            &[&id, &json!(result), &financial_ms()],
        )
        .await?;
        event(
            &tx,
            &self.account,
            Some(cycle),
            if result.quantity == Decimal::ZERO {
                "paper.leg_failed"
            } else if result.remaining == Decimal::ZERO {
                "paper.leg_filled"
            } else {
                "paper.leg_partially_filled"
            },
            json!({"order_id":id,"simulated":true,"result":result,"holdings":values(&h)}),
        )
        .await?;
        let leg = intent["leg_number"].as_u64();
        if let Some(leg) = leg {
            crate::crash_checkpoint(&format!("before_leg_{leg}_fill_commit"));
        }
        tx.commit().await?;
        if let Some(leg) = leg {
            crate::crash_checkpoint(&format!("after_leg_{leg}_fill_commit"));
        }
        Ok(())
    }
    pub async fn settle(&self, id: &str, status: &str, reason: Option<&str>) -> Result<()> {
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        let a = tx
            .query_one(
                "SELECT epoch FROM paper_accounts WHERE id=$1 FOR UPDATE",
                &[&self.account],
            )
            .await?;
        if a.get::<_, i64>(0) != self.epoch {
            bail!("runtime fenced");
        }
        let row=tx.query_one("SELECT start_asset,starting::text,holdings,settled FROM paper_cycles WHERE id=$1 AND account_id=$2 FOR UPDATE",&[&id,&self.account]).await?;
        if row.get::<_, bool>(3) {
            return Ok(());
        }
        // Pending paper intents are guaranteed to have no external effect: fills and ledger are one transaction.
        tx.execute("UPDATE paper_orders SET state='EXPIRED',completed_at=COALESCE(to_timestamp($2::bigint/1000.0),now()) WHERE cycle_id=$1 AND result IS NULL",&[&id,&financial_ms()]).await?;
        let asset: String = row.get(0);
        let starting = d(row.get(1))?;
        let h = holdings(&row.get::<_, Value>(2))?;
        let residual = h.iter().any(|(k, v)| k != &asset && *v > Decimal::ZERO);
        let cash = *h.get(&asset).unwrap_or(&Decimal::ZERO) - starting;
        let pnl = if residual {
            None
        } else {
            Some(cash.to_string())
        };
        let state = if residual {
            "RECOVERY_REQUIRED"
        } else {
            status
        };
        tx.execute("UPDATE paper_balances SET reserved=reserved-$3::text::numeric,version=version+1 WHERE account_id=$1 AND asset=$2",&[&self.account,&asset,&starting.to_string()]).await?;
        for (asset, amount) in &h {
            if *amount < Decimal::ZERO {
                bail!("negative holding at settlement");
            }
            tx.execute("INSERT INTO paper_balances(account_id,asset,available) VALUES($1,$2,$3::text::numeric) ON CONFLICT(account_id,asset) DO UPDATE SET available=paper_balances.available+EXCLUDED.available,version=paper_balances.version+1,reconciled_at=now()",&[&self.account,&asset,&amount.to_string()]).await?;
            transfer(
                &tx,
                &self.account,
                Some(id),
                &format!("{id}:settlement"),
                asset,
                "cycle",
                "available",
                *amount,
            )
            .await?;
        }
        tx.execute("UPDATE paper_cycles SET state=$2,settled=true,realized_pnl=$3::text::numeric,cash_delta=$4::text::numeric,failure_reason=$5,completed_at=COALESCE(to_timestamp($6::bigint/1000.0),now()) WHERE id=$1",&[&id,&state,&pnl,&cash.to_string(),&reason,&financial_ms()]).await?;
        if residual {
            tx.execute(
                "UPDATE paper_accounts SET blocked=true,enabled=false WHERE id=$1",
                &[&self.account],
            )
            .await?;
        }
        event(&tx,&self.account,Some(id),"paper.balance_settled",json!({"state":state,"holdings":values(&h),"realized_pnl":pnl,"cash_delta":cash.to_string(),"residual_inventory":residual,"reason":reason})).await?;
        event(&tx,&self.account,Some(id),"paper.pnl_finalized",json!({"simulated":true,"realized_pnl":pnl,"cash_delta":cash.to_string(),"residual_inventory":residual})).await?;
        event(
            &tx,
            &self.account,
            Some(id),
            if residual {
                "paper.cycle_recovery_required"
            } else if state == "FAILED" {
                "paper.cycle_failed"
            } else {
                "paper.cycle_completed"
            },
            json!({"state":state,"reason":reason}),
        )
        .await?;
        crate::crash_checkpoint("before_settlement_commit");
        tx.commit().await?;
        crate::crash_checkpoint("after_pnl_finalized");
        drop(c);
        self.reconcile().await?;
        Ok(())
    }
    pub async fn reconcile(&self) -> Result<()> {
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        tx.query_one(
            "SELECT id FROM paper_accounts WHERE id=$1 FOR UPDATE",
            &[&self.account],
        )
        .await?;
        let rows=tx.query("WITH assets AS (SELECT asset FROM paper_balances WHERE account_id=$1 UNION SELECT asset FROM paper_ledger WHERE account_id=$1 UNION SELECT start_asset FROM paper_cycles WHERE account_id=$1 AND NOT settled) SELECT s.asset,COALESCE(b.available,0)::text,COALESCE((SELECT sum(CASE WHEN credit_scope='available' THEN amount ELSE -amount END) FROM paper_ledger l WHERE l.account_id=$1 AND l.asset=s.asset AND (debit_scope='available' OR credit_scope='available')),0)::text,COALESCE(b.reserved,0)::text,COALESCE((SELECT sum(starting) FROM paper_cycles c WHERE c.account_id=$1 AND c.start_asset=s.asset AND NOT settled),0)::text FROM assets s LEFT JOIN paper_balances b ON b.account_id=$1 AND b.asset=s.asset ORDER BY s.asset",&[&self.account]).await?;
        let mut discrepancies = vec![];
        for r in rows {
            if d(r.get(1))? != d(r.get(2))? || d(r.get(3))? != d(r.get(4))? {
                discrepancies.push(r.get::<_, String>(0));
            }
        }
        let cycles = tx
            .query(
                "SELECT id,holdings,settled FROM paper_cycles WHERE account_id=$1",
                &[&self.account],
            )
            .await?;
        for cycle in cycles {
            let id: String = cycle.get(0);
            let h = holdings(&cycle.get::<_, Value>(1))?;
            let settled: bool = cycle.get(2);
            let entries=tx.query("SELECT asset,sum(CASE WHEN credit_scope='cycle' THEN amount ELSE -amount END)::text FROM paper_ledger WHERE cycle_id=$1 AND (credit_scope='cycle' OR debit_scope='cycle') GROUP BY asset", &[&id]).await?;
            let mut seen = std::collections::HashSet::new();
            for entry in entries {
                let asset: String = entry.get(0);
                seen.insert(asset.clone());
                let expected = if settled {
                    Decimal::ZERO
                } else {
                    *h.get(&asset).unwrap_or(&Decimal::ZERO)
                };
                if d(entry.get(1))? != expected {
                    discrepancies.push(format!("cycle:{id}:{asset}"));
                }
            }
            if !settled {
                for (asset, amount) in &h {
                    if *amount != Decimal::ZERO && !seen.contains(asset) {
                        discrepancies.push(format!("cycle:{id}:{asset}:missing-ledger"));
                    }
                }
            }
        }
        if !discrepancies.is_empty() {
            tx.execute(
                "UPDATE paper_accounts SET blocked=true,enabled=false WHERE id=$1",
                &[&self.account],
            )
            .await?;
            event(
                &tx,
                &self.account,
                None,
                "paper.reconciliation_failed",
                json!({"assets":discrepancies}),
            )
            .await?;
            tx.commit().await?;
            bail!("paper ledger does not reconcile");
        }
        event(
            &tx,
            &self.account,
            None,
            "paper.reconciled",
            json!({"balanced":true}),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

fn stats(rows: &[tokio_postgres::Row], projected: Decimal) -> Result<risk::PortfolioStats> {
    let mut cumulative = Decimal::ZERO;
    let mut peak = Decimal::ZERO;
    let mut losses = 0;
    for row in rows {
        let pnl = d(row.get(0))?;
        cumulative += pnl;
        peak = peak.max(cumulative);
        losses = if pnl < Decimal::ZERO { losses + 1 } else { 0 };
    }
    Ok(risk::PortfolioStats {
        projected_asset_exposure: projected,
        drawdown: peak - cumulative,
        consecutive_losses: losses,
    })
}
