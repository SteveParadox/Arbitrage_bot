use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use market_data::{
    config::{Category, Config as MarketConfig},
    connector,
    model::MarketDataEvent,
};
use micro_canary::{
    load_micro_canary_config, MicroCanaryCandidate, MicroCanaryRun, ABSOLUTE_MAX_CYCLE_NOTIONAL,
};
use orderbook::BookUpdate;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use scanner::{
    load_profitability_config, load_triangle_config, ArbitrageScanRecord, ArbitrageScanner,
    ProfitabilityConfig, ScanStatus, ScannerSettings, TriangleRoute,
};
use shadow::ReadOnlyAccountClient;
use tokio::sync::mpsc;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    init_logging();

    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let canary_path = config_path(
        "ARB_MICRO_CANARY_CONFIG",
        &repo_root,
        "shared/config/micro_canary.json",
    );
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

    let canary = load_micro_canary_config(&canary_path)?;
    canary.validate_environment()?;

    let triangle_config = load_triangle_config(&triangle_path)?;
    if triangle_config.routes.is_empty() {
        bail!("triangle configuration is empty");
    }
    if triangle_config.source.testnet {
        bail!("micro-canary requires mainnet triangle metadata");
    }

    let mut scanner_settings = load_scanner_settings(&scanner_path)?;
    scanner_settings.start_amounts.clear();
    scanner_settings.start_amounts.insert(
        canary.base_asset.clone(),
        canary
            .cycle_notional
            .to_f64()
            .context("cycle_notional cannot fit scanner input")?,
    );

    let profitability_path = env::var("ARB_PROFITABILITY_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| resolve_path(&repo_root, &scanner_settings.profitability_config_path));
    let profitability = load_profitability_config(&profitability_path)?;
    let prediction_template = profitability.clone();
    let mut scanner =
        ArbitrageScanner::new(triangle_config.clone(), scanner_settings, profitability)
            .map_err(anyhow::Error::msg)?;

    let routes = triangle_config
        .routes
        .iter()
        .cloned()
        .map(|route| (route.id.clone(), route))
        .collect::<HashMap<_, _>>();

    let account = ReadOnlyAccountClient::from_env(&canary.base_asset)?;
    let mut fee_bps_by_symbol = HashMap::new();
    for symbol in triangle_config.required_symbols() {
        let rate = account
            .get_spot_fee_rate(&symbol)
            .await
            .with_context(|| format!("failed to load read-only account fee rate for {symbol}"))?;
        if rate.taker_fee_rate < Decimal::ZERO || rate.taker_fee_rate >= Decimal::ONE {
            bail!("invalid taker fee rate for {symbol}");
        }
        fee_bps_by_symbol.insert(symbol, rate.taker_fee_rate * Decimal::from(10_000));
    }

    let required_symbols = triangle_config
        .required_symbols()
        .into_iter()
        .collect::<Vec<_>>();
    let mut market = MarketConfig::from_env()?;
    market.testnet = false;
    market.category = Category::Spot;
    market.symbols = required_symbols;
    market.subscribe_trades = false;
    market.subscribe_tickers = false;
    if market.orderbook_depth == 1 {
        bail!("micro-canary requires depth-aware books, not depth=1");
    }

    let session_id = format!("micro-canary-{}", now_ms());
    println!(
        "{}",
        serde_json::to_string(&MicroCanaryRun {
            event_type: "micro_live_run",
            session_id: session_id.clone(),
            started_at_ms: now_ms(),
            base_asset: canary.base_asset.clone(),
            cycle_notional: text(canary.cycle_notional),
            hard_cycle_cap: text(ABSOLUTE_MAX_CYCLE_NOTIONAL),
            manual_execution_required: true,
        })?
    );

    let (sender, mut receiver) = mpsc::channel::<MarketDataEvent>(16_384);
    let connector = tokio::spawn(async move { connector::run(market, sender).await });

    let mut emitted = 0usize;
    let mut last_candidate_ms = 0u64;

    while let Some(event) = receiver.recv().await {
        if emitted >= canary.max_candidates_per_session {
            break;
        }

        match event {
            MarketDataEvent::Status(status) => {
                if matches!(
                    status.state.as_str(),
                    "connected" | "reconnecting" | "metadata_retry"
                ) {
                    scanner.reset_books();
                }
            }
            MarketDataEvent::OrderBook {
                symbol,
                bids,
                asks,
                timestamp,
                update_id,
                sequence,
                is_snapshot,
            } => {
                let update = BookUpdate {
                    symbol,
                    bids,
                    asks,
                    timestamp,
                    update_id,
                    sequence,
                    is_snapshot,
                };
                let records = match scanner.on_book_update(update) {
                    Ok(records) => records,
                    Err(error) => {
                        warn!(%error, "book invalidated; waiting for fresh snapshots");
                        scanner.reset_books();
                        continue;
                    }
                };
                let now = now_ms();
                if last_candidate_ms != 0
                    && now.saturating_sub(last_candidate_ms) < canary.cooldown_ms
                {
                    continue;
                }

                let Some((record, route, prediction)) = select_candidate(
                    &records,
                    &routes,
                    &canary.base_asset,
                    &prediction_template,
                    &fee_bps_by_symbol,
                ) else {
                    continue;
                };

                let account_snapshot = match account.sync().await {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        warn!(%error, "read-only account refresh failed");
                        continue;
                    }
                };
                if account_snapshot.base_available < canary.cycle_notional {
                    warn!(
                        available = %account_snapshot.base_available,
                        required = %canary.cycle_notional,
                        "candidate suppressed because available base balance is too low"
                    );
                    continue;
                }

                emitted += 1;
                last_candidate_ms = now;
                let trade_id = format!("micro-manual-{}-{}", now, emitted);
                let candidate = MicroCanaryCandidate {
                    event_type: "micro_live_candidate",
                    session_id: session_id.clone(),
                    trade_id,
                    detected_at_ms: record.scan_timestamp,
                    route_id: route.id.clone(),
                    triangle_id: route.triangle_id.clone(),
                    base_asset: route.start_asset.clone(),
                    starting_capital: text(canary.cycle_notional),
                    expected_pnl: text(prediction.expected_net_profit),
                    expected_fees: text(prediction.fee_amount),
                    expected_slippage: text(prediction.expected_slippage_amount),
                    expected_slippage_bps: text(prediction.expected_slippage_bps),
                    expected_net_edge_bps: text(prediction.expected_net_return_bps),
                    fee_bps_per_leg: prediction
                        .fee_bps_per_leg
                        .iter()
                        .copied()
                        .map(text)
                        .collect(),
                    detection_leg_prices: record
                        .legs
                        .iter()
                        .map(|leg| leg.execution.average_execution_price)
                        .collect(),
                    account_balance: text(account_snapshot.base_available),
                    account_equity_usd: text(account_snapshot.total_equity_usd),
                    account_exposure_usd: text(account_snapshot.non_base_exposure_usd),
                    manual_execution_required: true,
                };
                println!("{}", serde_json::to_string(&candidate)?);
            }
            _ => {}
        }
    }

    connector.abort();
    info!(emitted, "micro-canary session finished");
    Ok(())
}

fn select_candidate(
    records: &[ArbitrageScanRecord],
    routes: &HashMap<String, TriangleRoute>,
    base_asset: &str,
    template: &ProfitabilityConfig,
    fees: &HashMap<String, Decimal>,
) -> Option<(
    ArbitrageScanRecord,
    TriangleRoute,
    scanner::ProfitabilityResult,
)> {
    let mut best = None;

    for record in records {
        if record.status != ScanStatus::Complete
            || record.start_asset != base_asset
            || record.gross_profitable != Some(true)
        {
            continue;
        }
        let route = routes.get(&record.route_id)?.clone();
        let mut config = template.clone();
        let mut route_fees = Vec::with_capacity(3);
        for leg in &route.legs {
            route_fees.push(*fees.get(&leg.symbol)?);
        }
        config.fee_bps_per_leg = route_fees;

        let (Some(start), Some(final_amount)) = (record.start_amount, record.final_amount) else {
            continue;
        };
        let Ok(prediction) = config.evaluate_f64(start, final_amount) else {
            continue;
        };
        if !prediction.net_profitable {
            continue;
        }

        let replace = best.as_ref().is_none_or(
            |(_, _, current): &(
                ArbitrageScanRecord,
                TriangleRoute,
                scanner::ProfitabilityResult,
            )| { prediction.expected_net_return_bps > current.expected_net_return_bps },
        );
        if replace {
            best = Some((record.clone(), route, prediction));
        }
    }

    best
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

fn text(value: Decimal) -> String {
    value.normalize().to_string()
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn init_logging() {
    let level = env::var("ARB_LOG_LEVEL")
        .unwrap_or_else(|_| "INFO".to_string())
        .to_lowercase();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(format!("micro_canary={level},{level}"))),
        )
        .json()
        .init();
}
