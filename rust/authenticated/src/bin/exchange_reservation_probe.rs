//! Explicit development-only database contention probe; never constructs an exchange client.
use anyhow::{bail, Result};
use authenticated::Ledger;
use rust_decimal::Decimal;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var("ARB_ENV").as_deref() != Ok("development")
        || std::env::var("ARB_LIVE_TRADING_ENABLED").as_deref() != Ok("false")
    {
        bail!("offline database probe only");
    }
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        bail!("account uid routes concurrency required");
    }
    let count: usize = args[4].parse()?;
    if ![2, 10, 50, 100].contains(&count) {
        bail!("unsupported contention case");
    }
    let url = std::env::var("ARB_DATABASE_URL")?;
    let route = scanner::load_triangle_config(&args[3])?.routes.remove(0);
    let mut owner = Ledger::connect(&url, args[1].clone()).await?;
    owner.claim("testnet", &args[2], true).await?;
    let client = infrastructure::connect_database(infrastructure::database_config(&url)?).await?;
    let now = risk::current_time_ms();
    client.execute("UPDATE execution_accounts SET enabled=true,blocked=false,recovering=false,snapshot_ms=$2,balance_evidence=$3,expected_wallet=$4 WHERE id=$1",&[&args[1],&(now as i64),&json!({"available":{"USDT":"100"},"wallet":{"USDT":"100"}}),&json!({"USDT":"100"})]).await?;
    // Workers use separate PostgreSQL sessions under one authoritative owner.
    let mut workers = Vec::new();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(count));
    for i in 0..count {
        let mut worker = Ledger::connect(&url, args[1].clone()).await?;
        worker.epoch = owner.epoch;
        let route = route.clone();
        let barrier = barrier.clone();
        workers.push(tokio::spawn(async move {
            barrier.wait().await;
            worker
                .reserve(
                    &format!("{}:probe:{i}", worker.account),
                    &format!("probe:{i}"),
                    &route,
                    Decimal::from(100),
                    json!({"test_only":true}),
                    risk::current_time_ms(),
                    10000,
                )
                .await
        }));
    }
    let mut accepted = 0;
    let mut rejected = 0;
    for work in workers {
        match work.await? {
            Ok(true) => accepted += 1,
            Ok(false) | Err(_) => rejected += 1,
        }
    }
    if accepted != 1 || rejected != count - 1 {
        bail!("capital invariant failed");
    }
    // A second process/session cannot acquire ownership while this owner lives.
    let mut other = Ledger::connect(&url, args[1].clone()).await?;
    let competing_owner_denied = other.claim("testnet", &args[2], true).await.is_err();
    if !competing_owner_denied {
        bail!("ownership invariant failed");
    }
    client
        .execute(
            "UPDATE execution_accounts SET epoch=epoch+1 WHERE id=$1",
            &[&args[1]],
        )
        .await?;
    let stale_owner_denied = owner.heartbeat().await.is_err();
    if !stale_owner_denied {
        bail!("fencing invariant failed");
    }
    println!(
        "{}",
        json!({"attempts":count,"accepted":accepted,"rejected":rejected,"separate_database_sessions":true,"competing_owner_denied":competing_owner_denied,"stale_owner_denied":stale_owner_denied})
    );
    Ok(())
}
