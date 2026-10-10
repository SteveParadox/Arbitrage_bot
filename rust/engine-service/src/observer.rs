//! Managed public-data observation. No exchange order can be submitted here.
use std::{
    collections::{BTreeSet, HashMap},
    fs,
    io::{BufRead, BufReader},
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use event_bus::EventPublisher;
use market_data::{
    config::{Category, Config},
    connector,
    model::{InstrumentMetadata, MarketDataEvent},
};
use orderbook::BookUpdate;
use scanner::{
    load_profitability_config, load_triangle_config, ArbitrageScanner, ScanStatus, ScannerSettings,
    TriangleConfig,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::warn;

pub async fn run(triangle_path: PathBuf, repo_root: PathBuf, events: EventPublisher) -> Result<()> {
    let routes = load_triangle_config(&triangle_path)?;
    let required = routes.required_symbols();
    if required.is_empty() {
        bail!("observation autostart requires at least one configured triangle");
    }
    let scanner_path = configured_path(
        &repo_root,
        "ARB_SCANNER_CONFIG",
        "shared/config/scanner.json",
    );
    let settings: ScannerSettings = serde_json::from_str(
        &fs::read_to_string(&scanner_path)
            .with_context(|| format!("reading {}", scanner_path.display()))?,
    )?;
    settings.validate().map_err(|error| anyhow!(error))?;
    let profitability_path = configured_path(
        &repo_root,
        "ARB_PROFITABILITY_CONFIG",
        &settings.profitability_config_path,
    );
    let profitability = load_profitability_config(profitability_path)?;
    let mut scanner = ArbitrageScanner::new(routes.clone(), settings.clone(), profitability)
        .map_err(|error| anyhow!(error))?;

    let (sender, mut receiver) = mpsc::channel(1024);
    let connector_task: JoinHandle<Result<()>> =
        if let Ok(replay) = std::env::var("ARB_OBSERVER_REPLAY_FILE") {
            if !replay_allowed(
                std::env::var("ARB_ENV").ok().as_deref(),
                std::env::var("ARB_LIVE_TRADING_ENABLED").ok().as_deref(),
            ) {
                bail!(
                "observer replay requires ARB_ENV=development and ARB_LIVE_TRADING_ENABLED=false"
            );
            }
            tokio::spawn(replay_events(PathBuf::from(replay), sender))
        } else {
            let mut feed = Config::from_env()?;
            if feed.category != Category::Spot || feed.testnet != routes.source.testnet {
                bail!("market feed must be spot and match the triangle configuration environment");
            }
            feed.symbols = required.iter().cloned().collect();
            // The scanner's time bound governs the connection heartbeat as well.
            feed.stale_after = feed
                .stale_after
                .min(std::time::Duration::from_millis(settings.max_book_age_ms));
            tokio::spawn(connector::run(feed, sender))
        };
    let _guard = ConnectorGuard(Some(connector_task));
    let mut metadata = HashMap::new();
    let mut ready_at = None;
    events.publish(
        "engine.observer",
        json!({"state": "connecting", "required_symbols": required, "execution_enabled": false}),
    );
    loop {
        let event = receiver
            .recv()
            .await
            .ok_or_else(|| anyhow!("market-data connector ended"))?;
        match event {
            MarketDataEvent::Instrument(instrument) => {
                validate_instrument(&routes, &instrument)?;
                metadata.insert(instrument.symbol.clone(), instrument);
            }
            MarketDataEvent::Status(status)
                if matches!(
                    status.state.as_str(),
                    "connected" | "reconnecting" | "metadata_retry"
                ) =>
            {
                scanner.reset_books();
                ready_at = None;
                events.publish("engine.observer", json!({"state": "synchronizing", "reason": status.state, "execution_enabled": false}));
            }
            MarketDataEvent::Health(value) => {
                let ready = feed_ready(&value, &required, now_ms(), settings.max_book_age_ms)
                    && required.iter().all(|symbol| metadata.contains_key(symbol));
                ready_at = ready.then_some(std::time::Instant::now());
                events.publish("market.health", value);
                events.publish("engine.observer", json!({"state": if ready {"scanning"} else {"synchronizing"}, "execution_enabled": false}));
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
                if !metadata.contains_key(&symbol) {
                    continue;
                }
                let records = match scanner.on_book_update(BookUpdate {
                    symbol,
                    bids,
                    asks,
                    timestamp,
                    update_id,
                    sequence,
                    is_snapshot,
                }) {
                    Ok(records) => records,
                    Err(error) => {
                        ready_at = None;
                        warn!(%error, "book rejected; waiting for new snapshots");
                        events.publish("engine.observer", json!({"state":"synchronizing", "reason":error.to_string(), "execution_enabled":false}));
                        continue;
                    }
                };
                if ready_at.is_none_or(|at: std::time::Instant| {
                    at.elapsed().as_millis() > settings.max_book_age_ms as u128
                }) {
                    continue;
                }
                for record in records {
                    events
                        .ensure_critical_ready()
                        .map_err(|error| anyhow!(error.to_string()))?;
                    let candidate_id = candidate_id(&record);
                    let accepted = record.status == ScanStatus::Complete
                        && record.net_profitable == Some(true)
                        && record
                            .expected_net_profit
                            .is_some_and(|profit| profit > 0.0);
                    let kind = if accepted {
                        "opportunity.detected"
                    } else {
                        "opportunity.rejected"
                    };
                    let mut payload = serde_json::to_value(&record)?;
                    payload["candidate_id"] = Value::String(candidate_id.clone());
                    payload["evaluation_status"] =
                        Value::String(if accepted { "observed" } else { "rejected" }.into());
                    payload["execution_enabled"] = Value::Bool(false);
                    events
                        .publish_critical_with_id(candidate_id, kind, payload)
                        .map_err(|error| anyhow!("candidate journal unavailable: {error}"))?;
                }
            }
            _ => {}
        }
    }
}

fn replay_allowed(environment: Option<&str>, live_gate: Option<&str>) -> bool {
    environment == Some("development") && live_gate == Some("false")
}

async fn replay_events(path: PathBuf, sender: mpsc::Sender<MarketDataEvent>) -> Result<()> {
    let file =
        fs::File::open(&path).with_context(|| format!("opening replay {}", path.display()))?;
    for (index, line) in BufReader::new(file).lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let event: MarketDataEvent = serde_json::from_str(&line)
            .with_context(|| format!("invalid replay event at line {}", index + 1))?;
        sender
            .send(event)
            .await
            .context("observer stopped during replay")?;
    }
    // Preserve the running control service and allow a health query after replay.
    tokio::signal::ctrl_c().await?;
    Ok(())
}

struct ConnectorGuard(Option<JoinHandle<Result<()>>>);
impl Drop for ConnectorGuard {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

fn configured_path(root: &std::path::Path, name: &str, default: &str) -> PathBuf {
    let path = PathBuf::from(std::env::var(name).unwrap_or_else(|_| default.into()));
    if path.is_absolute() {
        path
    } else {
        root.join(path)
    }
}

fn feed_ready(
    value: &Value,
    required: &BTreeSet<String>,
    now: u64,
    max_age_ms: u64,
) -> bool {
    // Do not trust a top-level healthy flag alone. Require individual book
    // synchronization and both exchange/receive freshness clocks.
    let Some(health_at) = value["timestamp"].as_u64() else {
        return false;
    };
    let Some(telemetry_age) = now.checked_sub(health_at) else {
        return false;
    };
    if required.is_empty()
        || value["state"] != "connected_and_fresh"
        || value["subscriptions_confirmed"] != true
        || telemetry_age > max_age_ms
    {
        return false;
    }
    let Some(symbols) = value["symbols"].as_object() else {
        return false;
    };
    required.iter().all(|symbol| {
        let Some(book) = symbols.get(symbol) else {
            return false;
        };
        let Some(exchange_age) = book["exchange_timestamp_ms"]
            .as_u64()
            .and_then(|timestamp| now.checked_sub(timestamp))
        else {
            return false;
        };
        let Some(receive_age) = book["receive_age_ms"]
            .as_u64()
            .and_then(|age| age.checked_add(telemetry_age))
        else {
            return false;
        };
        book["initialized"] == true
            && book["synchronized"] == true
            && exchange_age <= max_age_ms
            && receive_age <= max_age_ms
    })
}

fn validate_instrument(routes: &TriangleConfig, instrument: &InstrumentMetadata) -> Result<()> {
    if instrument.status != "Trading" {
        bail!("{} is not Trading", instrument.symbol);
    }
    let valid = routes
        .routes
        .iter()
        .flat_map(|route| route.legs.iter())
        .filter(|leg| leg.symbol == instrument.symbol)
        .all(|leg| {
            leg.base_asset == instrument.base_coin && leg.quote_asset == instrument.quote_coin
        });
    if !valid {
        bail!(
            "{} metadata disagrees with configured conversion route",
            instrument.symbol
        );
    }
    Ok(())
}

fn candidate_id(record: &scanner::ArbitrageScanRecord) -> String {
    let key = format!(
        "{}:{}:{}:{}:{}",
        record.route_id,
        record.trigger_symbol,
        record.trigger_timestamp,
        record.trigger_sequence,
        record.trigger_update_id
    );
    format!("{:x}", Sha256::digest(key.as_bytes()))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn health_requires_confirmed_current_books() {
        let required = BTreeSet::from(["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
        let fresh = json!({
            "state":"connected_and_fresh",
            "subscriptions_confirmed":true,
            "timestamp":1000,
            "symbols": {
                "BTCUSDT": {"initialized":true, "synchronized":true,
                    "exchange_timestamp_ms":1000, "receive_age_ms":0},
                "ETHUSDT": {"initialized":true, "synchronized":true,
                    "exchange_timestamp_ms":1000, "receive_age_ms":0}
            }
        });
        assert!(feed_ready(&fresh, &required, 1000, 100));
        assert!(!feed_ready(&fresh, &required, 1101, 100));
        assert!(!feed_ready(&fresh, &required, 999, 100));
        let mut incomplete = fresh.clone();
        incomplete["symbols"]["ETHUSDT"]["synchronized"] = Value::Bool(false);
        assert!(!feed_ready(&incomplete, &required, 1000, 100));
        let mut stale_exchange = fresh.clone();
        stale_exchange["symbols"]["BTCUSDT"]["exchange_timestamp_ms"] = json!(800);
        assert!(!feed_ready(&stale_exchange, &required, 1000, 100));
        let mut stale_receipt = fresh.clone();
        stale_receipt["symbols"]["ETHUSDT"]["receive_age_ms"] = json!(95);
        assert!(!feed_ready(&stale_receipt, &required, 1010, 100));
        let mut unconfirmed = fresh.clone();
        unconfirmed["subscriptions_confirmed"] = Value::Bool(false);
        assert!(!feed_ready(&unconfirmed, &required, 1000, 100));
        let mut missing = fresh;
        missing["symbols"].as_object_mut().unwrap().remove("ETHUSDT");
        assert!(!feed_ready(&missing, &required, 1000, 100));
    }

    #[test]
    fn replay_rejects_missing_or_enabled_live_gate() {
        assert!(replay_allowed(Some("development"), Some("false")));
        assert!(!replay_allowed(Some("development"), None));
        assert!(!replay_allowed(Some("development"), Some("true")));
        assert!(!replay_allowed(Some("production"), Some("false")));
    }
}
