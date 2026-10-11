use anyhow::{bail, Context, Result};
use execution::{BalanceSnapshot, OrderExecutionState};
use rust_decimal::Decimal;
use scanner::TriangleRoute;
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls, Transaction};

#[derive(Clone)]
pub struct Ledger {
    client: Arc<Mutex<Client>>,
    pub account: String,
    pub epoch: i64,
}
pub struct Portfolio {
    pub enabled: bool,
    pub available: Decimal,
    pub exposure: Decimal,
    pub daily_pnl: Decimal,
    pub drawdown: Decimal,
    pub losses: usize,
    pub limits: Value,
}
pub fn amounts(v: &Value) -> Result<BTreeMap<String, Decimal>> {
    v.as_object()
        .context("invalid decimal holdings")?
        .iter()
        .map(|(asset, value)| {
            Ok((
                asset.clone(),
                Decimal::from_str_exact(value.as_str().context("holdings must be strings")?)?,
            ))
        })
        .collect()
}
fn values(v: &BTreeMap<String, Decimal>) -> Value {
    json!(v
        .iter()
        .map(|(k, v)| (k, v.to_string()))
        .collect::<BTreeMap<_, _>>())
}
async fn event(tx: &Transaction<'_>, account: &str, kind: &str, data: Value) -> Result<()> {
    tx.execute(
        "INSERT INTO execution_events(account_id,kind,payload) VALUES($1,$2,$3)",
        &[&account, &kind, &data],
    )
    .await?;
    Ok(())
}
impl Ledger {
    pub async fn connect(url: &str, account: String) -> Result<Self> {
        let mut config: tokio_postgres::Config =
            url.replace("postgresql+psycopg:", "postgresql:").parse()?;
        config.connect_timeout(std::time::Duration::from_secs(3));
        config.application_name(format!("exchange:{account}"));
        config.options("-c lock_timeout=2000 -c statement_timeout=5000 -c idle_in_transaction_session_timeout=10000");
        let (client, connection) = config.connect(NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("exchange journal disconnected: {e}");
            }
        });
        if std::env::var("ARB_ENV").as_deref() == Ok("production") {
            let unsafe_role: bool = client.query_one("SELECT rolsuper OR rolcreatedb OR rolcreaterole FROM pg_roles WHERE rolname=current_user", &[]).await?.get(0);
            if unsafe_role {
                bail!("exchange runtime requires restricted PostgreSQL role");
            }
        }
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            account,
            epoch: 0,
        })
    }
    pub async fn claim(&mut self, mode: &str, uid: &str, mock: bool) -> Result<()> {
        let mut c = self.client.lock().await;
        let locked: bool = c
            .query_one(
                "SELECT pg_try_advisory_lock(hashtextextended($1,9150))",
                &[&self.account],
            )
            .await?
            .get(0);
        if !locked {
            bail!("exchange account already owned");
        }
        let tx = c.transaction().await?;
        let row = tx
            .query_one(
                "SELECT mode,exchange_uid,mock FROM execution_accounts WHERE id=$1 FOR UPDATE",
                &[&self.account],
            )
            .await?;
        if row.get::<_, String>(0) != mode
            || row.get::<_, String>(1) != uid
            || row.get::<_, bool>(2) != mock
        {
            bail!("exchange account environment mismatch");
        }
        self.epoch = tx.query_one("UPDATE execution_accounts SET epoch=epoch+1,enabled=false,blocked=true,recovering=true,state='RECOVERING',heartbeat=now() WHERE id=$1 RETURNING epoch", &[&self.account]).await?.get(0);
        event(
            &tx,
            &self.account,
            "execution.owner",
            json!({"epoch":self.epoch,"new_exposure":false}),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
    async fn fence(&self, tx: &Transaction<'_>) -> Result<Value> {
        let row = tx
            .query_one(
                "SELECT epoch,limits FROM execution_accounts WHERE id=$1 FOR UPDATE",
                &[&self.account],
            )
            .await?;
        if row.get::<_, i64>(0) != self.epoch {
            bail!("stale execution ownership epoch");
        }
        Ok(row.get(1))
    }
    pub async fn state(&self, state: &str, blocked: bool, recovering: bool) -> Result<()> {
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        self.fence(&tx).await?;
        tx.execute("UPDATE execution_accounts SET state=$2,blocked=$3,recovering=$4,enabled=CASE WHEN $3 OR $4 OR $2 IN ('STOPPED','FAILED') THEN false ELSE enabled END,heartbeat=CASE WHEN $2 IN ('STOPPED','FAILED') THEN NULL ELSE now() END WHERE id=$1", &[&self.account,&state,&blocked,&recovering]).await?;
        event(
            &tx,
            &self.account,
            "execution.state",
            json!({"state":state,"epoch":self.epoch}),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn heartbeat(&self) -> Result<()> {
        let c = self.client.lock().await;
        if c.execute(
            "UPDATE execution_accounts SET heartbeat=now() WHERE id=$1 AND epoch=$2",
            &[&self.account, &self.epoch],
        )
        .await?
            != 1
        {
            bail!("runtime fenced");
        }
        Ok(())
    }
    pub async fn market_health(&self, ready: bool, now: u64) -> Result<()> {
        let c = self.client.lock().await;
        if c.execute(
            "UPDATE execution_accounts SET market_ready=$3,market_ms=$4 WHERE id=$1 AND epoch=$2",
            &[&self.account, &self.epoch, &ready, &(now as i64)],
        )
        .await?
            != 1
        {
            bail!("market health owner fenced");
        }
        Ok(())
    }
    pub async fn audit(&self, kind: &str, payload: Value) -> Result<()> {
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        self.fence(&tx).await?;
        event(&tx, &self.account, kind, payload).await?;
        tx.commit().await?;
        Ok(())
    }
    /// Fresh wallet evidence is compared, never used to erase unexplained deltas.
    pub async fn reconcile(
        &self,
        snapshot: &BalanceSnapshot,
        now: u64,
        max_age: u64,
    ) -> Result<bool> {
        if now.abs_diff(snapshot.synchronized_at_ms) > max_age {
            bail!("stale exchange balance evidence");
        }
        let mut wallet = BTreeMap::new();
        let mut available = BTreeMap::new();
        let mut locked = BTreeMap::new();
        for coin in &snapshot.coins {
            if wallet
                .insert(coin.coin.clone(), coin.wallet_balance)
                .is_some()
            {
                bail!("duplicate wallet asset");
            }
            available.insert(coin.coin.clone(), coin.estimated_spot_available);
            locked.insert(coin.coin.clone(), coin.locked);
        }
        if wallet.is_empty() {
            bail!("empty wallet evidence");
        }
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        self.fence(&tx).await?;
        let expected: Option<Value> = tx
            .query_one(
                "SELECT expected_wallet FROM execution_accounts WHERE id=$1",
                &[&self.account],
            )
            .await?
            .get(0);
        let expected = expected.as_ref().map(amounts).transpose()?;
        let healthy = expected.as_ref().is_none_or(|e| {
            e.iter().chain(wallet.iter()).all(|(a, _)| {
                e.get(a).copied().unwrap_or_default() == wallet.get(a).copied().unwrap_or_default()
            })
        });
        let evidence = json!({"wallet":values(&wallet),"available":values(&available),"locked":values(&locked),"synchronized_at_ms":snapshot.synchronized_at_ms,"expected":expected.as_ref().map(values)});
        tx.execute(
            "INSERT INTO execution_reconciliation(account_id,healthy,evidence) VALUES($1,$2,$3)",
            &[&self.account, &healthy, &evidence],
        )
        .await?;
        if healthy {
            tx.execute("UPDATE execution_accounts SET expected_wallet=COALESCE(expected_wallet,$2),balance_evidence=$3,snapshot_ms=$4 WHERE id=$1", &[&self.account,&values(&wallet),&evidence,&(snapshot.synchronized_at_ms as i64)]).await?;
        } else {
            tx.execute("UPDATE execution_accounts SET blocked=true,enabled=false,state='BLOCKED' WHERE id=$1", &[&self.account]).await?;
        }
        event(
            &tx,
            &self.account,
            "execution.reconciliation",
            json!({"healthy":healthy,"evidence":evidence}),
        )
        .await?;
        tx.commit().await?;
        Ok(healthy)
    }
    pub async fn portfolio(&self, now: u64, max_age: u64) -> Result<Portfolio> {
        let c = self.client.lock().await;
        let row=c.query_one("SELECT enabled AND NOT blocked AND NOT recovering AND NOT emergency_stop, balance_evidence, snapshot_ms,limits,epoch FROM execution_accounts WHERE id=$1", &[&self.account]).await?;
        if row.get::<_, i64>(4) != self.epoch {
            bail!("runtime fenced");
        }
        let time: Option<i64> = row.get(2);
        if time.is_none_or(|t| now.checked_sub(t as u64).is_none_or(|a| a > max_age)) {
            bail!("stale balance snapshot");
        }
        let evidence: Value = row.get(1);
        let available = amounts(&evidence["available"])?;
        let exposure: String=c.query_one("SELECT COALESCE(sum(starting),0)::text FROM execution_cycles WHERE account_id=$1 AND committed", &[&self.account]).await?.get(0);
        let daily: String=c.query_one("SELECT COALESCE(sum(realized_pnl),0)::text FROM execution_cycles WHERE account_id=$1 AND completed_at>=date_trunc('day',now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'", &[&self.account]).await?.get(0);
        let (drawdown, losses) = performance(&*c, &self.account).await?;
        let exposure = Decimal::from_str_exact(&exposure)?;
        Ok(Portfolio {
            enabled: row.get(0),
            available: (available.get("USDT").copied().unwrap_or_default() - exposure)
                .max(Decimal::ZERO),
            exposure,
            daily_pnl: Decimal::from_str_exact(&daily)?,
            drawdown,
            losses,
            limits: row.get(3),
        })
    }
    /// Account row is the database serialization boundary, not this object's mutex.
    #[allow(clippy::too_many_arguments)]
    pub async fn reserve(
        &self,
        id: &str,
        candidate: &str,
        route: &TriangleRoute,
        amount: Decimal,
        risk: Value,
        now: u64,
        max_age: u64,
    ) -> Result<bool> {
        if amount <= Decimal::ZERO || route.start_asset != "USDT" || route.legs.len() != 3 {
            bail!("invalid exchange reservation");
        }
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        let limits = self.fence(&tx).await?;
        if tx
            .query_opt(
                "SELECT id FROM execution_cycles WHERE account_id=$1 AND candidate_id=$2",
                &[&self.account, &candidate],
            )
            .await?
            .is_some()
        {
            return Ok(false);
        }
        let row=tx.query_one("SELECT enabled AND NOT blocked AND NOT recovering AND NOT emergency_stop,balance_evidence,snapshot_ms FROM execution_accounts WHERE id=$1", &[&self.account]).await?;
        let time: Option<i64> = row.get(2);
        if !row.get::<_, bool>(0)
            || time.is_none_or(|t| now.checked_sub(t as u64).is_none_or(|age| age > max_age))
        {
            bail!("exchange account not authorized or fresh");
        }
        if tx
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM execution_cycles WHERE account_id=$1 AND committed)",
                &[&self.account],
            )
            .await?
            .get::<_, bool>(0)
        {
            bail!("account has an active capital commitment");
        }
        let evidence: Value = row.get(1);
        let balances = amounts(&evidence["available"])?;
        let max = |name: &str| -> Result<Decimal> {
            Ok(Decimal::from_str_exact(
                limits[name]
                    .as_str()
                    .context("missing account risk limit")?,
            )?)
        };
        let pnl:String=tx.query_one("SELECT COALESCE(sum(realized_pnl),0)::text FROM execution_cycles WHERE account_id=$1 AND completed_at>=date_trunc('day',now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'", &[&self.account]).await?.get(0);
        let (drawdown, losses) = performance(&tx, &self.account).await?;
        if amount > balances.get("USDT").copied().unwrap_or_default()
            || amount > max("max_trade")?
            || amount > max("max_exposure")?
            || amount > max("max_asset_exposure")?
            || Decimal::from_str_exact(&pnl)? <= -max("max_daily_loss")?
            || drawdown >= max("max_drawdown")?
            || losses
                >= limits["max_consecutive_losses"]
                    .as_u64()
                    .context("missing loss limit")? as usize
        {
            bail!("authoritative reservation capital or loss limit rejected");
        }
        tx.execute("INSERT INTO execution_cycles(id,account_id,candidate_id,route,risk,start_asset,starting,holdings,state,epoch) VALUES($1,$2,$3,$4,$5,'USDT',$6::text::numeric,$7,'RESERVED',$8)", &[&id,&self.account,&candidate,&json!(route),&risk,&amount.to_string(),&json!({"USDT":amount.to_string()}),&self.epoch]).await?;
        event(&tx,&self.account,"execution.reserved",json!({"cycle_id":id,"candidate_id":candidate,"amount":amount.to_string(),"epoch":self.epoch})).await?;
        tx.commit().await?;
        Ok(true)
    }
    pub async fn intent(&self, cycle: &str, link: &str, intent: &Value) -> Result<()> {
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        self.fence(&tx).await?;
        if intent["risk_reducing"]!=true && !tx.query_one("SELECT enabled AND NOT blocked AND NOT recovering AND NOT emergency_stop FROM execution_accounts WHERE id=$1", &[&self.account]).await?.get::<_,bool>(0) {bail!("new exposure stopped before intent commit");}
        let row=tx.query_one("SELECT committed,settled,holdings FROM execution_cycles WHERE id=$1 AND account_id=$2 FOR UPDATE", &[&cycle,&self.account]).await?;
        if !row.get::<_, bool>(0) || row.get::<_, bool>(1) {
            bail!("closed reservation cannot create an order");
        }
        let holdings = amounts(&row.get::<_, Value>(2))?;
        let input = intent["from"].as_str().context("order input asset")?;
        let capital = holdings.get(input).copied().unwrap_or_default();
        let quantity =
            Decimal::from_str_exact(intent["quantity"].as_str().context("order quantity")?)?;
        let price = Decimal::from_str_exact(
            intent["limit_price"]
                .as_str()
                .context("order price bound")?,
        )?;
        let fee = Decimal::from_str_exact(
            intent["fee_ceiling"]
                .as_str()
                .context("order fee ceiling")?,
        )?;
        let maximum = if intent["side"] == "Buy" {
            quantity
                .checked_mul(price)
                .and_then(|v| v.checked_mul(Decimal::ONE + fee))
                .context("order spend overflow")?
        } else {
            quantity
        };
        if intent["order_type"] != "Limit"
            || quantity <= Decimal::ZERO
            || price <= Decimal::ZERO
            || fee < Decimal::ZERO
            || fee >= Decimal::ONE
            || maximum > capital
            || holdings.values().any(|v| *v < Decimal::ZERO)
        {
            bail!("authoritative bounded order exceeds cycle capital");
        }
        // A persisted order of any state cannot be submitted again. Recovery only queries it.
        if tx
            .query_opt(
                "SELECT link_id FROM execution_orders WHERE account_id=$1 AND link_id=$2",
                &[&self.account, &link],
            )
            .await?
            .is_some()
        {
            bail!("order intent already exists; reconcile without resubmission");
        }
        tx.execute("INSERT INTO execution_orders(account_id,link_id,cycle_id,intent,state) VALUES($1,$2,$3,$4,'INTENT_RECORDED')", &[&self.account,&link,&cycle,&intent]).await?;
        tx.execute(
            "UPDATE execution_cycles SET state='EXECUTING' WHERE id=$1",
            &[&cycle],
        )
        .await?;
        event(
            &tx,
            &self.account,
            "execution.intent",
            json!({"cycle_id":cycle,"link_id":link,"intent":intent}),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn allow_submission(&self, link: &str, reducing: bool) -> Result<()> {
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        self.fence(&tx).await?;
        let row=tx.query_one("SELECT enabled AND NOT blocked AND NOT recovering AND NOT emergency_stop,snapshot_ms FROM execution_accounts WHERE id=$1", &[&self.account]).await?;
        if !reducing && !row.get::<_, bool>(0) {
            bail!("account authorization revoked before submission");
        }
        let snapshot: Option<i64> = row.get(1);
        if snapshot.is_none_or(|t| {
            risk::current_time_ms()
                .checked_sub(t as u64)
                .is_none_or(|age| age > 1000)
        }) {
            bail!("balance evidence expired before submission");
        }
        if tx.query_one("SELECT EXISTS(SELECT 1 FROM execution_orders WHERE account_id=$1 AND link_id<>$2 AND (NOT terminal OR NOT confirmed))", &[&self.account,&link]).await?.get::<_,bool>(0) {bail!("conflicting unresolved order blocks submission");}
        let valid:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM execution_orders o JOIN execution_cycles c ON c.id=o.cycle_id WHERE o.account_id=$1 AND o.link_id=$2 AND o.state='SUBMITTING' AND c.committed AND NOT c.settled)", &[&self.account,&link]).await?.get(0);
        if !valid {
            bail!("submission lacks its durable capital-owned intent");
        }
        tx.commit().await?;
        Ok(())
    }
    pub async fn acknowledged(&self, ack: &execution::PlaceOrderAck) -> Result<()> {
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        self.fence(&tx).await?;
        if tx.execute("UPDATE execution_orders SET exchange_order_id=$3,state='ACKNOWLEDGED',updated_at=now() WHERE account_id=$1 AND link_id=$2 AND state='SUBMITTING' AND exchange_order_id IS NULL", &[&self.account,&ack.order_link_id,&ack.order_id]).await?!=1 {bail!("acknowledgement conflicts with order intent");}
        event(&tx,&self.account,"execution.acknowledged",json!({"order_link_id":ack.order_link_id,"exchange_order_id":ack.order_id,"accepted_at_ms":ack.accepted_at_ms,"filled":false})).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn quarantine(&self, reason: &str) -> Result<()> {
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        self.fence(&tx).await?;
        tx.execute("UPDATE execution_cycles SET state='RECOVERY_REQUIRED',reason=$2 WHERE account_id=$1 AND committed", &[&self.account,&reason]).await?;
        tx.execute(
            "UPDATE execution_accounts SET enabled=false,blocked=true,state='BLOCKED' WHERE id=$1",
            &[&self.account],
        )
        .await?;
        event(
            &tx,
            &self.account,
            "execution.quarantined",
            json!({"reason":reason}),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn order_state(&self, link: &str, state: &str) -> Result<()> {
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        self.fence(&tx).await?;
        if tx.execute("UPDATE execution_orders SET state=$3,updated_at=now() WHERE account_id=$1 AND link_id=$2 AND NOT terminal", &[&self.account,&link,&state]).await?!=1 { bail!("invalid durable order transition"); }
        event(
            &tx,
            &self.account,
            "execution.order_state",
            json!({"link_id":link,"state":state}),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn rejected(&self, link: &str, reason: &str) -> Result<()> {
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        self.fence(&tx).await?;
        if tx.execute("UPDATE execution_orders SET state='REJECTED',terminal=true,confirmed=true,evidence=$3,updated_at=now() WHERE account_id=$1 AND link_id=$2 AND exchange_order_id IS NULL AND NOT terminal", &[&self.account,&link,&json!({"definitive_submission_rejection":reason})]).await?!=1 {bail!("rejection conflicts with exchange evidence");}
        event(
            &tx,
            &self.account,
            "execution.rejected",
            json!({"link_id":link,"reason":reason}),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn apply(&self, link: &str, state: &OrderExecutionState) -> Result<()> {
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        self.fence(&tx).await?;
        let row=tx.query_one("SELECT cycle_id,intent,exchange_order_id,evidence,terminal,confirmed FROM execution_orders WHERE account_id=$1 AND link_id=$2 FOR UPDATE", &[&self.account,&link]).await?;
        let cycle: String = row.get(0);
        let intent: Value = row.get(1);
        let previous: Option<String> = row.get(2);
        if state.order_link_id != link
            || state.symbol != intent["symbol"].as_str().context("symbol")?
            || previous.as_ref().is_some_and(|id| id != &state.order_id)
            || state.side != Some(serde_json::from_value(intent["side"].clone())?)
            || state.requested_quantity
                != Decimal::from_str_exact(intent["quantity"].as_str().context("intent quantity")?)?
            || state.filled_quantity < Decimal::ZERO
            || state.filled_quantity > state.requested_quantity
            || state.remaining_quantity < Decimal::ZERO
            || state.filled_quantity.checked_add(state.remaining_quantity)
                != Some(state.requested_quantity)
        {
            bail!("order confirmation identity mismatch");
        }
        if let Some(old) = row.get::<_, Option<Value>>(3) {
            let old: OrderExecutionState = serde_json::from_value(old)
                .context("exchange evidence contradicts a definitive rejection")?;
            if state.filled_quantity < old.filled_quantity
                || row.get::<_, bool>(4) && !state.terminal
                || row.get::<_, bool>(5) && !state.fills_confirmed
                || old.fills.iter().any(|fill| !state.fills.contains(fill))
            {
                bail!("confirmed exchange evidence regressed");
            }
        }
        let from = intent["from"].as_str().context("from")?;
        let to = intent["to"].as_str().context("to")?;
        let buy = intent["side"] == "Buy";
        let limit = intent["limit_price"]
            .as_str()
            .map(Decimal::from_str_exact)
            .transpose()?;
        let row = tx
            .query_one(
                "SELECT holdings,settled FROM execution_cycles WHERE id=$1 FOR UPDATE",
                &[&cycle],
            )
            .await?;
        let mut holdings = amounts(&row.get::<_, Value>(0))?;
        let settled: bool = row.get(1);
        let mut wallet = amounts(
            &tx.query_one(
                "SELECT expected_wallet FROM execution_accounts WHERE id=$1",
                &[&self.account],
            )
            .await?
            .get::<_, Value>(0),
        )?;
        for fill in &state.fills {
            if fill.order_id != state.order_id
                || fill.execution_id.is_empty()
                || fill.quantity <= Decimal::ZERO
                || fill.value <= Decimal::ZERO
                || fill.price <= Decimal::ZERO
                || limit.is_some_and(|price| {
                    if buy {
                        fill.price > price
                    } else {
                        fill.price < price
                    }
                })
                || (fill.fee != Decimal::ZERO && fill.fee_currency.is_empty())
            {
                bail!("invalid or unknown economic evidence");
            }
            let payload = json!(fill);
            if let Some(old)=tx.query_opt("SELECT evidence,link_id FROM execution_fills WHERE account_id=$1 AND execution_id=$2", &[&self.account,&fill.execution_id]).await? {
                if old.get::<_,Value>(0)!=payload || old.get::<_,String>(1)!=link { bail!("conflicting authoritative execution identity"); }
                continue;
            }
            if settled {
                bail!("new fill after settled cycle requires reconciliation");
            }
            tx.execute("INSERT INTO execution_fills(account_id,execution_id,link_id,cycle_id,evidence) VALUES($1,$2,$3,$4,$5)", &[&self.account,&fill.execution_id,&link,&cycle,&payload]).await?;
            let (spent, gross) = if buy {
                (fill.value, fill.quantity)
            } else {
                (fill.quantity, fill.value)
            };
            for (asset, role, delta) in [
                (from, "input", -spent),
                (to, "output", gross),
                (fill.fee_currency.as_str(), "fee", -fill.fee),
            ] {
                if delta == Decimal::ZERO {
                    continue;
                }
                let h = holdings.entry(asset.into()).or_default();
                *h = h.checked_add(delta).context("holdings overflow")?;
                let w = wallet.entry(asset.into()).or_default();
                *w = w.checked_add(delta).context("wallet overflow")?;
                if *w < Decimal::ZERO {
                    bail!("exchange economic evidence exceeds known wallet");
                }
                tx.execute("INSERT INTO execution_movements(account_id,execution_id,asset,role,amount) VALUES($1,$2,$3,$4,$5::text::numeric)", &[&self.account,&fill.execution_id,&asset,&role,&delta.to_string()]).await?;
            }
        }
        let status = if state.terminal && state.fills_confirmed {
            match state.status.as_str() {
                "Filled" => "FILLED",
                "Rejected" => "REJECTED",
                "Expired" => "EXPIRED",
                _ => "CANCELLED",
            }
        } else if state.filled_quantity > Decimal::ZERO {
            "PARTIALLY_FILLED"
        } else {
            "ACKNOWLEDGED"
        };
        tx.execute("UPDATE execution_orders SET exchange_order_id=$3,state=$4,evidence=$5,terminal=$6,confirmed=$7,updated_at=now() WHERE account_id=$1 AND link_id=$2", &[&self.account,&link,&state.order_id,&status,&json!(state),&state.terminal,&state.fills_confirmed]).await?;
        tx.execute(
            "UPDATE execution_cycles SET holdings=$2 WHERE id=$1",
            &[&cycle, &values(&holdings)],
        )
        .await?;
        tx.execute(
            "UPDATE execution_accounts SET expected_wallet=$2 WHERE id=$1",
            &[&self.account, &values(&wallet)],
        )
        .await?;
        event(
            &tx,
            &self.account,
            "execution.confirmed_evidence",
            json!({"cycle_id":cycle,"link_id":link,"state":state}),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn holdings(&self, cycle: &str) -> Result<BTreeMap<String, Decimal>> {
        amounts(
            &self
                .client
                .lock()
                .await
                .query_one(
                    "SELECT holdings FROM execution_cycles WHERE id=$1 AND account_id=$2",
                    &[&cycle, &self.account],
                )
                .await?
                .get::<_, Value>(0),
        )
    }
    pub async fn settle(
        &self,
        cycle: &str,
        requested_state: &str,
        reason: Option<&str>,
    ) -> Result<bool> {
        let mut c = self.client.lock().await;
        let tx = c.transaction().await?;
        self.fence(&tx).await?;
        let row=tx.query_one("SELECT holdings,starting::text,settled FROM execution_cycles WHERE id=$1 AND account_id=$2 FOR UPDATE", &[&cycle,&self.account]).await?;
        if row.get::<_, bool>(2) {
            return Ok(true);
        }
        let holdings = amounts(&row.get::<_, Value>(0))?;
        let unresolved:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM execution_orders WHERE cycle_id=$1 AND (NOT terminal OR NOT confirmed))", &[&cycle]).await?.get(0);
        let reconciled:bool=tx.query_one("SELECT COALESCE((SELECT r.healthy AND r.created_at>=COALESCE((SELECT max(updated_at) FROM execution_orders WHERE cycle_id=$2),'epoch'::timestamptz) AND r.created_at>now()-interval '2 seconds' FROM execution_reconciliation r WHERE r.account_id=$1 ORDER BY r.id DESC LIMIT 1),false)", &[&self.account,&cycle]).await?.get(0);
        let closed = reconciled
            && !unresolved
            && holdings
                .iter()
                .all(|(a, v)| a == "USDT" || *v == Decimal::ZERO)
            && holdings.values().all(|v| *v >= Decimal::ZERO);
        let pnl = holdings.get("USDT").copied().unwrap_or_default()
            - Decimal::from_str_exact(&row.get::<_, String>(1))?;
        if closed {
            tx.execute("UPDATE execution_cycles SET state=$2,committed=false,settled=true,realized_pnl=$3::text::numeric,completed_at=now(),reason=$4 WHERE id=$1", &[&cycle,&requested_state,&pnl.to_string(),&reason]).await?;
        } else {
            tx.execute(
                "UPDATE execution_cycles SET state='RECOVERY_REQUIRED',reason=$2 WHERE id=$1",
                &[&cycle, &reason],
            )
            .await?;
            tx.execute("UPDATE execution_accounts SET blocked=true,enabled=false,state='BLOCKED' WHERE id=$1", &[&self.account]).await?;
        }
        event(&tx,&self.account,"execution.settlement",json!({"cycle_id":cycle,"closed":closed,"holdings":values(&holdings),"realized_pnl":closed.then(||pnl.to_string()),"reason":reason})).await?;
        tx.commit().await?;
        Ok(closed)
    }
    pub async fn pending_orders(&self) -> Result<Vec<(String, String)>> {
        Ok(self.client.lock().await.query("SELECT o.link_id,o.intent->>'symbol' FROM execution_orders o JOIN execution_cycles c ON c.id=o.cycle_id WHERE o.account_id=$1 AND c.committed ORDER BY o.created_at,o.link_id", &[&self.account]).await?.iter().map(|r|(r.get(0),r.get(1))).collect())
    }
    pub async fn active_cycles(&self) -> Result<Vec<String>> {
        Ok(self
            .client
            .lock()
            .await
            .query(
                "SELECT id FROM execution_cycles WHERE account_id=$1 AND committed ORDER BY id",
                &[&self.account],
            )
            .await?
            .iter()
            .map(|r| r.get(0))
            .collect())
    }
    pub async fn flush_events(&self, events: &event_bus::EventPublisher) -> Result<()> {
        let c = self.client.lock().await;
        for row in c.query("SELECT id,kind,payload,(extract(epoch FROM created_at)*1000)::bigint FROM execution_events WHERE account_id=$1 AND NOT published ORDER BY id LIMIT 100", &[&self.account]).await? {
            let id:i64=row.get(0); let kind:String=row.get(1); let payload:Value=row.get(2);
            events.publish_critical_at(format!("exchange:{}:{id}",self.account),&kind,row.get::<_,i64>(3) as u64,payload)?;
            if kind == "execution.settlement" { crate::checkpoint_journal("after_outbox_fsync"); }
            if c.execute("UPDATE execution_events SET published=true WHERE id=$1 AND EXISTS(SELECT 1 FROM execution_accounts WHERE id=$2 AND epoch=$3)", &[&id,&self.account,&self.epoch]).await?!=1 { bail!("outbox owner fenced"); }
        }
        Ok(())
    }
}
async fn performance<C: tokio_postgres::GenericClient>(
    c: &C,
    account: &str,
) -> Result<(Decimal, usize)> {
    let mut equity = Decimal::ZERO;
    let mut peak = Decimal::ZERO;
    let mut drawdown = Decimal::ZERO;
    let mut losses = 0;
    for row in c.query("SELECT realized_pnl::text FROM execution_cycles WHERE account_id=$1 AND settled ORDER BY completed_at,id", &[&account]).await? {
        let pnl=Decimal::from_str_exact(&row.get::<_,String>(0))?;
        equity+=pnl; peak=peak.max(equity); drawdown=drawdown.max(peak-equity); losses=if pnl<Decimal::ZERO {losses+1} else {0};
    }
    Ok((drawdown, losses))
}
