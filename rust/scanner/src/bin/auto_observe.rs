//! Read-only supervised connection: Bybit market data -> existing depth-aware scanner.
//! No dependency on the live order execution client. This binary NEVER places orders.
use std::{collections::HashSet, env, fs, fs::OpenOptions, path::{Path, PathBuf},
          time::{SystemTime, UNIX_EPOCH}};
use anyhow::{bail, Context, Result};
use event_bus::EventPublisher;
use fs2::FileExt;
use market_data::{config::{Category, Config as MarketConfig}, connector, model::MarketDataEvent};
use orderbook::BookUpdate;
use scanner::{load_profitability_config, load_triangle_config, ArbitrageScanner,
              NdjsonRecorder, ScanStatus, ScannerSettings};
use serde_json::{json, Value};
use tokio::sync::mpsc;

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

fn resolve(root: &Path, value: &str) -> PathBuf {
    let candidate = PathBuf::from(value);
    if candidate.is_absolute() { candidate } else { root.join(candidate) }
}

fn config_path(root: &Path, key: &str, default: &str) -> PathBuf {
    env::var(key).map(|v| resolve(root, &v)).unwrap_or_else(|_| root.join(default))
}

fn validate_read_only_mode(mode: &str, live_permission: &str) -> Result<()> {
    if mode != "observe" {
        bail!("auto-observe only supports ARB_TRADING_MODE=observe; execution is not wired");
    }
    if live_permission != "false" {
        bail!("auto-observe requires ARB_LIVE_TRADING_ENABLED=false");
    }
    Ok(())
}

fn health_is_usable(health: &Value, required: &HashSet<String>, now: u64, max_age: u64) -> bool {
    let Some(stamp) = health.get("timestamp").and_then(Value::as_u64) else {
        return false;
    };
    if health["state"] != "connected_and_fresh" || health["subscriptions_confirmed"] != true
        || stamp > now || now - stamp > max_age {
        return false;
    }
    let Some(symbols) = health.get("symbols").and_then(Value::as_object) else {
        return false;
    };
    !required.is_empty() && required.iter().all(|name| {
        symbols.get(name).is_some_and(|s| s["initialized"] == true && s["synchronized"] == true)
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let mode = env::var("ARB_TRADING_MODE").unwrap_or_else(|_| "observe".into()).to_lowercase();
    let live_permission = env::var("ARB_LIVE_TRADING_ENABLED").unwrap_or_else(|_| "false".into());
    validate_read_only_mode(&mode, &live_permission)?;

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let lock_path = config_path(&root, "ARB_AUTONOMOUS_OBSERVE_LOCK", "data/control/auto-observe.lock");
    fs::create_dir_all(lock_path.parent().context("lock path has no parent")?)?;
    let lock = OpenOptions::new().read(true).write(true).create(true).open(&lock_path)?;
    lock.try_lock_exclusive().context("another auto-observe supervisor is running")?;

    let routes = load_triangle_config(config_path(
        &root, "ARB_TRIANGLE_CONFIG", "shared/config/triangles.json"))?;
    if routes.routes.is_empty() {
        bail!("triangle catalog is empty; generate spot routes before starting");
    }
    let settings_path = config_path(&root, "ARB_SCANNER_CONFIG", "shared/config/scanner.json");
    let settings: ScannerSettings = serde_json::from_str(&fs::read_to_string(&settings_path)?)?;
    settings.validate().map_err(anyhow::Error::msg)?;
    let profitability = load_profitability_config(config_path(
        &root, "ARB_PROFITABILITY_CONFIG", &settings.profitability_config_path))?;
    let required = routes.required_symbols().into_iter().collect::<HashSet<_>>();
    let mut market = MarketConfig::from_env()?;
    if market.category != Category::Spot || market.testnet != routes.source.testnet {
        bail!("Bybit spot environment and triangle catalog must match BYBIT_TESTNET");
    }
    if market.orderbook_depth < 50 {
        bail!("BYBIT_ORDERBOOK_DEPTH must be at least 50 for depth-aware monitoring");
    }
    market.symbols = required.iter().cloned().collect();
    market.symbols.sort();
    market.subscribe_trades = false;
    market.subscribe_tickers = false;

    let mut scanner = ArbitrageScanner::new(routes, settings.clone(), profitability)
        .map_err(anyhow::Error::msg)?;
    let mut journal = NdjsonRecorder::open(resolve(&root, &settings.record_path))?;
    let events = EventPublisher::try_from_env("auto-observe")
        .map_err(|err| anyhow::anyhow!("event publisher failed to initialize: {err}"))?;
    events.publish("engine.observe_started", json!({
        "mode":"observe", "execution_enabled":false, "symbols":market.symbols
    }));

    let (tx, mut rx) = mpsc::channel::<MarketDataEvent>(4096);
    let mut task = tokio::spawn(async move { connector::run(market, tx).await });
    let mut active = HashSet::<String>::new();
    let mut ready = false;
    let mut health_at = 0_u64;
    loop {
        tokio::select! {
            result = &mut task => {
                bail!("market data connector unexpectedly terminated: {result:?}");
            }
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to handle shutdown")?;
                break;
            }
            event = rx.recv() => {
                let Some(event) = event else { bail!("market data channel terminated") };
                match event {
                    MarketDataEvent::Instrument(m) if required.contains(&m.symbol) => {
                        if m.status != "Trading" {
                            bail!("required instrument {} is not Trading", m.symbol);
                        }
                        active.insert(m.symbol);
                    }
                    MarketDataEvent::Status(s) => {
                        if matches!(s.state.as_str(), "connected" | "reconnecting" | "metadata_retry") {
                            scanner.reset_books();
                            ready = false;
                            health_at = 0;
                        }
                        events.publish("market.health", json!({
                            "state":s.state, "detail":s.detail, "execution_enabled":false
                        }));
                    }
                    MarketDataEvent::Health(h) => {
                        let now = now_ms();
                        let is_ready = active.len() == required.len()
                            && health_is_usable(&h, &required, now, settings.max_book_age_ms);
                        if ready && !is_ready { scanner.reset_books(); }
                        ready = is_ready;
                        health_at = if is_ready { now } else { 0 };
                        events.publish("market.health", h);
                    }
                    MarketDataEvent::OrderBook {
                        symbol, bids, asks, timestamp, update_id, sequence, is_snapshot
                    } => {
                        let update = BookUpdate {
                            symbol, bids, asks, timestamp, update_id, sequence, is_snapshot
                        };
                        let records = match scanner.on_book_update(update) {
                            Ok(records) => records,
                            Err(err) => {
                                scanner.reset_books();
                                ready = false;
                                health_at = 0;
                                events.publish("market.health", json!({
                                    "state":"resynchronizing", "reason":err.to_string()
                                }));
                                continue;
                            }
                        };
                        let now = now_ms();
                        if !ready || health_at > now || now - health_at > settings.max_book_age_ms {
                            continue;
                        }
                        // These are estimates, NEVER exchange-confirmed fills or realized P&L.
                        journal.record_batch(&records)?;
                        for record in records {
                            let label = if record.status == ScanStatus::Complete
                                && record.net_profitable == Some(true) {
                                "opportunity.detected"
                            } else {
                                "opportunity.rejected"
                            };
                            events.publish(label, serde_json::to_value(&record)?);
                            println!("{}", serde_json::to_string(&record)?);
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    task.abort();
    let _ = task.await;
    events.publish("engine.observe_stopped", json!({"execution_enabled":false}));
    drop(lock);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn production_orders_cannot_be_enabled() {
        assert!(validate_read_only_mode("observe", "false").is_ok());
        for mode in ["paper", "testnet", "micro_live", "live"] {
            assert!(validate_read_only_mode(mode, "false").is_err());
        }
        assert!(validate_read_only_mode("observe", "true").is_err());
        assert!(validate_read_only_mode("observe", "yes").is_err());
    }

    #[test]
    fn unusable_market_health_blocks_scanning() {
        let required = HashSet::from(["BTCUSDT".to_string(), "ETHBTC".to_string()]);
        let valid = json!({
            "state":"connected_and_fresh", "timestamp":1000,
            "subscriptions_confirmed":true,
            "symbols":{
                "BTCUSDT":{"initialized":true,"synchronized":true},
                "ETHBTC":{"initialized":true,"synchronized":true}
            }
        });
        assert!(health_is_usable(&valid, &required, 1200, 500));
        assert!(!health_is_usable(&valid, &required, 1600, 500));
        assert!(!health_is_usable(&valid, &required, 900, 500));
        let mut broken = valid.clone();
        broken["symbols"]["ETHBTC"]["initialized"] = json!(false);
        assert!(!health_is_usable(&broken, &required, 1200, 500));
        let mut unconfirmed = valid.clone();
        unconfirmed["subscriptions_confirmed"] = json!(false);
        assert!(!health_is_usable(&unconfirmed, &required, 1200, 500));
    }
}
