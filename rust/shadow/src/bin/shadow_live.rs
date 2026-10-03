use std::{
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use market_data::{
    config::{Category, Config as MarketConfig},
    connector,
    model::MarketDataEvent,
};
use orderbook::BookUpdate;
use risk::{load_risk_config, RiskEngine};
use scanner::{load_profitability_config, load_triangle_config, ScannerSettings};
use shadow::{
    load_shadow_config, ReadOnlyAccountClient, ReadOnlyAccountSnapshot, ShadowEngine, ShadowEvent,
};
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc,
    time,
};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

enum AccountUpdate {
    Snapshot(ReadOnlyAccountSnapshot),
    Error(String),
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    if env::var("ARB_LIVE_TRADING_ENABLED")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()
        .unwrap_or(false)
    {
        bail!("shadow-live refuses to start while ARB_LIVE_TRADING_ENABLED=true");
    }

    init_logging();

    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let shadow_path = config_path("ARB_SHADOW_CONFIG", &repo_root, "shared/config/shadow.json");
    let triangle_path = config_path(
        "ARB_TRIANGLE_CONFIG",
        &repo_root,
        "shared/config/triangles.json",
    );
    let scanner_path = config_path(
        "ARB_SCANNER_CONFIG",
        &repo_root,
        "shared/config/scanner.json",
    );
    let risk_path = config_path("ARB_RISK_CONFIG", &repo_root, "shared/config/risk.json");

    let shadow_config = load_shadow_config(&shadow_path)?;
    let triangle_config = load_triangle_config(&triangle_path)?;
    if triangle_config.routes.is_empty() {
        bail!(concat!(
            "shared/config/triangles.json contains no routes; ",+            "generate current mainnet spot triangles first"
        ));
    }
    if triangle_config.source.testnet {
        bail!("shadow-live requires mainnet triangle metadata");
    }

    let scanner_settings = load_scanner_settings(&scanner_path)?;
    let profitability_path = env::var("ARB_PROFITABILITY_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| resolve_path(&repo_root, &scanner_settings.profitability_config_path));
    let profitability = load_profitability_config(&profitability_path)?;
    let risk_config = load_risk_config(&risk_path)?;
    let risk_engine = RiskEngine::new(risk_config)?;

    let run_id = format!("shadow-{}-{}", now_ms(), std::process::id());
    let account_client = ReadOnlyAccountClient::from_env(&shadow_config.base_asset)?;
    let initial_account = account_client
        .sync()
        .await
        .context("initial mainnet account synchronization failed")?;

    let required_symbols = triangle_config
        .required_symbols()
        .into_iter()
        .collect::<Vec<_>>();
    let mut market_config = MarketConfig::from_env()?;
    market_config.testnet = false;
    market_config.category = Category::Spot;
    market_config.symbols = required_symbols;
    market_config.subscribe_trades = false;
    market_config.subscribe_tickers = false;
    if market_config.orderbook_depth == 1 {
        bail!(concat!(
            "shadow mode requires BYBIT_ORDERBOOK_DEPTH of at least 50 ",
            "for depth-aware latency measurement"
        ));
    }

    let mut engine = ShadowEngine::new(
        shadow_config.clone(),
        run_id.clone(),
        triangle_config,
        scanner_settings,
        profitability,
        risk_engine,
    )?;

    let (output_tx, output_rx) = mpsc::channel::<String>(65_536);
    let output_task = tokio::spawn(output_writer(output_rx));
    emit(&output_tx, &engine.run_started_event(now_ms())).await?;
    emit(
        &output_tx,
        &engine.update_account(initial_account, now_ms()),
    )
    .await?;

    let (market_tx, mut market_rx) = mpsc::channel::<MarketDataEvent>(16_384);
    let connector_config = market_config.clone();
    let connector_task =
        tokio::spawn(async move { connector::run(connector_config, market_tx).await });

    let (account_tx, mut account_rx) = mpsc::channel::<AccountUpdate>(16);
    let account_refresh_ms = shadow_config.account_refresh_ms;
    let account_task = tokio::spawn(async move {
        account_refresh_loop(account_client, account_tx, account_refresh_ms).await
    });

    let mut sample_tick = time::interval(std::time::Duration::from_millis(
        shadow_config.sample_tick_ms,
    ));
    sample_tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    let mut progress_tick = time::interval(std::time::Duration::from_secs(30));
    progress_tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    info!(
        run_id,
        symbols = market_config.symbols.len(),
        depth = market_config.orderbook_depth,
        "live shadow mode started with mainnet market data and GET-only account access"
    );

    loop {
        tokio::select! {
            maybe_event = market_rx.recv() => {
                let Some(event) = maybe_event else {
                    break;
                };
                let received_at_ms = now_ms();
                match event {
                    MarketDataEvent::OrderBook {
                        symbol,
                        bids,
                        asks,
                        timestamp,
                        update_id,
                        sequence,
                        is_snapshot,
                    } => {
                        let events = engine.on_book_update(
                            BookUpdate {
                                symbol,
                                bids,
                                asks,
                                timestamp,
                                update_id,
                                sequence,
                                is_snapshot,
                            },
                            received_at_ms,
                        )?;
                        emit_all(&output_tx, events).await?;
                    }
                    MarketDataEvent::Instrument(instrument) => {
                        engine.mark_exchange_event(received_at_ms);
                        engine.update_instrument(&instrument)?;
                    }
                    MarketDataEvent::Status(status) => {
                        let state = status.state.to_lowercase();
                        let unhealthy = state.contains("stale")
                            || state.contains("disconnect")
                            || state.contains("error")
                            || state.contains("failed");
                        engine.mark_exchange_status(
                            !unhealthy,
                            received_at_ms,
                            format!("{}: {}", status.state, status.detail),
                        );
                    }
                    _ => {
                        engine.mark_exchange_event(received_at_ms);
                    }
                }
            }
            maybe_account = account_rx.recv() => {
                let Some(update) = maybe_account else {
                    warn!("read-only account refresh task ended");
                    continue;
                };
                let event = match update {
                    AccountUpdate::Snapshot(snapshot) => {
                        engine.update_account(snapshot, now_ms())
                    }
                    AccountUpdate::Error(detail) => {
                        warn!(detail, "read-only account refresh failed");
                        engine.mark_account_error(now_ms(), detail)
                    }
                };
                emit(&output_tx, &event).await?;
            }
            _ = sample_tick.tick() => {
                let events = engine.sample_due(now_ms())?;
                emit_all(&output_tx, events).await?;
            }
            _ = progress_tick.tick() => {
                emit(&output_tx, &engine.progress_event()).await?;
            }
        }
    }

    drop(output_tx);
    account_task.abort();

    connector_task.await??;
    output_task.await??;
    error!("mainnet market-data connector ended");
    Ok(())
}

async fn account_refresh_loop(
    client: ReadOnlyAccountClient,
    sender: mpsc::Sender<AccountUpdate>,
    refresh_ms: u64,
) -> Result<()> {
    let mut interval = time::interval(std::time::Duration::from_millis(refresh_ms));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    interval.tick().await;

    loop {
        interval.tick().await;
        let update = match client.sync().await {
            Ok(snapshot) => AccountUpdate::Snapshot(snapshot),
            Err(error) => AccountUpdate::Error(error.to_string()),
        };
        if sender.send(update).await.is_err() {
            return Ok(());
        }
    }
}

async fn output_writer(mut receiver: mpsc::Receiver<String>) -> Result<()> {
    let stdout = tokio::io::stdout();
    let mut writer = BufWriter::new(stdout);
    while let Some(line) = receiver.recv().await {
        writer.write_all(line.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }
    writer.flush().await?;
    Ok(())
}

async fn emit(sender: &mpsc::Sender<String>, event: &ShadowEvent) -> Result<()> {
    sender
        .send(serde_json::to_string(event)?)
        .await
        .map_err(|_| anyhow::anyhow!("shadow output writer stopped"))
}

async fn emit_all(sender: &mpsc::Sender<String>, events: Vec<ShadowEvent>) -> Result<()> {
    for event in events {
        emit(sender, &event).await?;
    }
    Ok(())
}

fn load_scanner_settings(path: &Path) -> Result<ScannerSettings> {
    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let settings: ScannerSettings = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    settings.validate().map_err(anyhow::Error::msg)?;
    Ok(settings)
}

fn config_path(env_name: &str, repo_root: &Path, default: &str) -> PathBuf {
    env::var(env_name)
        .map(PathBuf::from)
        .unwrap_or_else(|_| repo_root.join(default))
}

fn resolve_path(repo_root: &Path, configured: &str) -> PathBuf {
    let path = PathBuf::from(configured);
    if path.is_absolute() {
        path
    } else {
        repo_root.join(path)
    }
}

fn init_logging() {
    let level = env::var("ARB_LOG_LEVEL")
        .unwrap_or_else(|_| "INFO".to_string())
        .to_lowercase();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(format!("shadow={level},{level}"))),
        )
        .json()
        .init();
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
