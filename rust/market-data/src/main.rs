use std::env;

use anyhow::Result;
use market_data::{config::Config, connector, model::MarketDataEvent};
use tokio::sync::mpsc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let default_level = env::var("ARB_LOG_LEVEL")
        .unwrap_or_else(|_| "INFO".to_string())
        .to_lowercase();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new(format!("market_data={default_level},{default_level}"))
        }))
        .json()
        .init();

    let config = Config::from_env()?;
    info!(
        testnet = config.testnet,
        category = config.category.as_str(),
        symbols = ?config.symbols,
        depth = config.orderbook_depth,
        "starting Bybit market-data connector"
    );

    let (sender, mut receiver) = mpsc::channel::<MarketDataEvent>(4096);
    let connector_config = config.clone();
    let connector_task =
        tokio::spawn(async move { connector::run(connector_config, sender).await });

    while let Some(event) = receiver.recv().await {
        match serde_json::to_string(&event) {
            Ok(line) => println!("{line}"),
            Err(error) => error!(%error, "failed to serialize market-data event"),
        }
    }

    connector_task.await??;
    Ok(())
}
