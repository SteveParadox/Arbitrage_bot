//! Development-only probe for independently competing PostgreSQL clients.
use anyhow::{bail, Result};
use paper::Ledger;
use rust_decimal::Decimal;
use scanner::load_triangle_config;
use serde_json::json;
#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var("ARB_ENV").as_deref() != Ok("development") {
        bail!("development probe only");
    }
    paper::validate_mode(
        &std::env::var("ARB_TRADING_MODE").unwrap_or_default(),
        std::env::var("ARB_PAPER_TRADING_ENABLED").as_deref() == Ok("true"),
        std::env::var("ARB_LIVE_TRADING_ENABLED").as_deref() != Ok("false"),
    )?;
    let args: Vec<_> = std::env::args().collect();
    if args.len() < 6 {
        bail!("expected account epoch catalog reserve|settle cycle [route-index]");
    }
    let mut ledger = Ledger::connect(&std::env::var("ARB_DATABASE_URL")?, args[1].clone()).await?;
    ledger.epoch = args[2].parse()?;
    let config = load_triangle_config(&args[3])?;
    if args[4] == "settle" {
        ledger
            .settle(&args[5], "FAILED", Some("probe release before fill"))
            .await?;
    } else {
        let reserved = ledger
            .reserve(
                &args[5],
                &config.routes[args
                    .get(6)
                    .map(|index| index.parse::<usize>())
                    .transpose()?
                    .unwrap_or_default()
                    % config.routes.len()],
                Decimal::from(100),
                Decimal::ONE,
                json!({"test":true}),
                Decimal::from(500),
                Decimal::from(50),
                &risk::PortfolioLimits::default(),
                0,
            )
            .await?;
        println!("{reserved}");
    }
    Ok(())
}
