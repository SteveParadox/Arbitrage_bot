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
use fs2::FileExt;
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
    validate_read_only_mode(
        std::env::var("ARB_TRADING_MODE").ok().as_deref(),
        std::env::var("ARB_LIVE_TRADING_ENABLED").ok().as_deref(),
    )?;
    let lock_path = configured_path(
        &repo_root,
        "ARB_OBSERVER_LOCK",
        "data/control/observer.lock",
    );
    fs::create_dir_all(lock_path.parent().context("observer lock has no parent")?)?;
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)?;
    lock.try_lock_exclusive()
        .context("another managed observer is running")?;
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
    let (recovery_sender, recovery) = mpsc::channel(1);
    let replay_mode = std::env::var("ARB_OBSERVER_REPLAY_FILE").is_ok();
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
            if feed.orderbook_depth < 50 {
                bail!("observer requires book depth >= 50");
            }
            feed.symbols = required.iter().cloned().collect();
            feed.subscribe_trades = false;
            feed.subscribe_tickers = false;
            // The scanner's time bound governs the connection heartbeat as well.
            feed.stale_after = feed
                .stale_after
                .min(std::time::Duration::from_millis(settings.max_book_age_ms));
            tokio::spawn(connector::run_with_recovery(feed, sender, recovery))
        };
    let _guard = ConnectorGuard(Some(connector_task));
    let mut lifecycle = Lifecycle::default();
    let mut receipts: HashMap<String, std::time::Instant> = HashMap::new();
    let mut feed_health = Value::Null;
    let mut state = "connecting";
    let mut reason = "starting public observation".to_string();
    let mut health_tick = tokio::time::interval(std::time::Duration::from_millis(250));
    loop {
        let event = tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal?;
                publish_health(&events, "offline", "observer shutdown", lifecycle.generation);
                return Ok(());
            }
            _ = health_tick.tick() => {
                let ready = observer_ready(&lifecycle, &routes, &required, &scanner,
                    &receipts, &feed_health, now_ms(), settings.max_book_age_ms);
                if state == "scanning" && !ready { state = "stale"; reason = "readiness expired".into(); }
                publish_health(&events, state, &reason, lifecycle.generation);
                continue;
            }
            event = receiver.recv() => match event {
                Some(event) => event,
                None => {
                    publish_health(&events, "failed", "market connector terminated", lifecycle.generation);
                    bail!("market-data connector ended");
                }
            }
        };
        let MarketDataEvent::Session { generation, event } = event else {
            // Ungenerated events cannot reopen a managed observation gate.
            continue;
        };
        if let MarketDataEvent::Status(status) = event.as_ref() {
            if status.state == "connecting" && generation > lifecycle.generation {
                lifecycle.begin(generation);
                scanner.reset_books();
                receipts.clear();
                feed_health = Value::Null;
                state = "connecting";
                reason = status.detail.clone();
            }
        }
        if generation != lifecycle.generation || generation == 0 {
            continue;
        }
        match *event {
            MarketDataEvent::Instrument(instrument) => {
                if lifecycle.recovering {
                    continue;
                }
                if let Err(error) = validate_instrument(&routes, &instrument) {
                    lifecycle.metadata.remove(&instrument.symbol);
                    lifecycle.recovering = true;
                    state = "degraded";
                    reason = error.to_string();
                    if !replay_mode {
                        let _ = recovery_sender.try_send(());
                    }
                } else {
                    lifecycle
                        .metadata
                        .insert(instrument.symbol.clone(), instrument);
                }
            }
            MarketDataEvent::Status(status) => {
                match status.state.as_str() {
                    "connected" if !lifecycle.recovering => {
                        lifecycle.connected = true;
                        state = "synchronizing";
                    }
                    "reconnecting" | "metadata_retry" | "disconnected" => {
                        lifecycle.invalidate();
                        scanner.reset_books();
                        receipts.clear();
                        feed_health = Value::Null;
                        state = "reconnecting";
                    }
                    _ => {}
                }
                reason = status.detail;
                events.publish(
                    "market.health",
                    json!({"state":status.state,
                    "timestamp":now_ms(), "generation":generation, "detail":reason}),
                );
            }
            MarketDataEvent::Health(value) => {
                if lifecycle.recovering {
                    continue;
                }
                feed_health = value;
                let ready = observer_ready(
                    &lifecycle,
                    &routes,
                    &required,
                    &scanner,
                    &receipts,
                    &feed_health,
                    now_ms(),
                    settings.max_book_age_ms,
                );
                state = if ready {
                    "scanning"
                } else if feed_health["state"] == "connected_but_stale" {
                    "stale"
                } else {
                    "synchronizing"
                };
                reason = if ready {
                    "all metadata, acknowledgements and books ready"
                } else {
                    "required observation dependencies incomplete or stale"
                }
                .into();
                events.publish("market.health", feed_health.clone());
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
                if lifecycle.recovering
                    || !lifecycle.connected
                    || !lifecycle.metadata.contains_key(&symbol)
                {
                    continue;
                }
                let receipt_symbol = symbol.clone();
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
                        lifecycle.invalidate();
                        scanner.reset_books();
                        receipts.clear();
                        feed_health = Value::Null;
                        state = "reconnecting";
                        reason = format!("scanner desynchronized: {error}");
                        warn!(%error, "scanner requested coordinated snapshot recovery");
                        if !replay_mode {
                            let _ = recovery_sender.try_send(());
                        }
                        publish_health(&events, state, &reason, generation);
                        continue;
                    }
                };
                receipts.insert(receipt_symbol, std::time::Instant::now());
                if !observer_ready(
                    &lifecycle,
                    &routes,
                    &required,
                    &scanner,
                    &receipts,
                    &feed_health,
                    now_ms(),
                    settings.max_book_age_ms,
                ) {
                    continue;
                }
                state = "scanning";
                for record in records {
                    events
                        .ensure_critical_ready()
                        .map_err(|error| anyhow!(error.to_string()))?;
                    let (candidate_id, kind, payload) =
                        candidate_event(&record, &lifecycle.metadata)?;
                    events
                        .publish_critical_at(candidate_id, kind, record.trigger_timestamp, payload)
                        .map_err(|error| anyhow!("candidate journal unavailable: {error}"))?;
                }
            }
            _ => {}
        }
        publish_health(&events, state, &reason, generation);
    }
}

fn validate_read_only_mode(mode: Option<&str>, live: Option<&str>) -> Result<()> {
    if mode.unwrap_or("observe") != "observe" || live != Some("false") {
        bail!("managed observation requires ARB_TRADING_MODE=observe and ARB_LIVE_TRADING_ENABLED=false");
    }
    Ok(())
}

#[derive(Default)]
struct Lifecycle {
    generation: u64,
    metadata: HashMap<String, InstrumentMetadata>,
    connected: bool,
    recovering: bool,
}
impl Lifecycle {
    fn begin(&mut self, generation: u64) {
        self.generation = generation;
        self.metadata.clear();
        self.connected = false;
        self.recovering = false;
    }
    fn invalidate(&mut self) {
        self.metadata.clear();
        self.connected = false;
        self.recovering = true;
    }
}

#[allow(clippy::too_many_arguments)]
fn observer_ready(
    lifecycle: &Lifecycle,
    routes: &TriangleConfig,
    required: &BTreeSet<String>,
    scanner: &ArbitrageScanner,
    receipts: &HashMap<String, std::time::Instant>,
    feed: &Value,
    now: u64,
    max_age: u64,
) -> bool {
    lifecycle.connected
        && !lifecycle.recovering
        && feed_ready(feed, required, now, max_age)
        && scanner.books_ready(required, now, max_age)
        && required.iter().all(|symbol| {
            receipts
                .get(symbol)
                .is_some_and(|at| at.elapsed().as_millis() <= max_age as u128)
                && lifecycle.metadata.get(symbol).is_some_and(|m| {
                    validate_instrument(routes, m).is_ok()
                        && now
                            .checked_sub(m.timestamp)
                            .is_some_and(|age| age <= 300_000)
                })
        })
}

fn publish_health(events: &EventPublisher, state: &str, reason: &str, generation: u64) {
    events.publish(
        "engine.observer",
        json!({"state":state, "reason":reason,
        "generation":generation, "scanner_ready":state == "scanning", "execution_enabled":false,
        "persistence_state":"local_outbox_accepted_is_not_database_confirmation"}),
    );
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

fn feed_ready(value: &Value, required: &BTreeSet<String>, now: u64, max_age_ms: u64) -> bool {
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
    if !routes.required_symbols().contains(&instrument.symbol) {
        bail!("unexpected instrument {}", instrument.symbol);
    }
    for (name, filter) in [
        ("tickSize", instrument.tick_size),
        ("basePrecision", instrument.qty_step),
        ("minOrderAmt", instrument.min_order_amt),
        ("maxMarketOrderQty", instrument.max_market_order_qty),
    ] {
        if !filter.is_some_and(|v| v.is_finite() && v > 0.0) {
            bail!("{} missing/invalid {name}", instrument.symbol);
        }
    }
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

fn candidate_event(
    record: &scanner::ArbitrageScanRecord,
    metadata: &HashMap<String, InstrumentMetadata>,
) -> Result<(String, &'static str, Value)> {
    let instruments: serde_json::Map<String, Value> = record
        .market_versions
        .iter()
        .filter_map(|version| {
            let symbol = version["symbol"].as_str()?;
            let m = metadata.get(symbol)?;
            Some((
                symbol.to_string(),
                json!({"status":m.status,"base_coin":m.base_coin,"quote_coin":m.quote_coin,
            "tick_size":m.tick_size,"qty_step":m.qty_step,"min_order_amt":m.min_order_amt,
            "max_market_order_qty":m.max_market_order_qty}),
            ))
        })
        .collect();
    let instrument_hash = format!(
        "{:x}",
        Sha256::digest(Value::Object(instruments.clone()).to_string().as_bytes())
    );
    let identity = json!({"identity_version":2, "route_id":record.route_id,
        "configuration_hash":record.configuration_hash, "strategy_version":record.strategy_version,
        "trigger_symbol":record.trigger_symbol, "trigger_timestamp":record.trigger_timestamp,
        "trigger_sequence":record.trigger_sequence, "trigger_update_id":record.trigger_update_id,
        "market_versions":record.market_versions,"instrument_config_hash":instrument_hash});
    let candidate_id = format!("{:x}", Sha256::digest(identity.to_string().as_bytes()));
    let accepted = record.status == ScanStatus::Complete
        && record.net_profitable == Some(true)
        && record
            .expected_net_profit
            .is_some_and(|profit| profit > 0.0);
    let mut payload = serde_json::to_value(record)?;
    // Canonical market time drives the journal; processing time is separate audit metadata.
    payload["scan_timestamp"] = json!(record.trigger_timestamp);
    payload["processing"] = json!({"processed_at_ms":record.scan_timestamp});
    payload["candidate_id"] = json!(candidate_id);
    payload["instrument_config_hash"] = json!(instrument_hash);
    payload["instrument_filters"] = Value::Object(instruments);
    payload["identity_version"] = json!(2);
    payload["evaluation_status"] = json!(if accepted { "observed" } else { "rejected" });
    payload["execution_enabled"] = json!(false);
    Ok((
        candidate_id,
        if accepted {
            "opportunity.detected"
        } else {
            "opportunity.rejected"
        },
        payload,
    ))
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
        missing["symbols"]
            .as_object_mut()
            .unwrap()
            .remove("ETHUSDT");
        assert!(!feed_ready(&missing, &required, 1000, 100));
    }

    #[test]
    fn replay_rejects_missing_or_enabled_live_gate() {
        assert!(replay_allowed(Some("development"), Some("false")));
        assert!(!replay_allowed(Some("development"), None));
        assert!(!replay_allowed(Some("development"), Some("true")));
        assert!(!replay_allowed(Some("production"), Some("false")));
    }
    #[test]
    fn managed_mode_rejects_execution_or_implicit_live_permission() {
        assert!(validate_read_only_mode(None, Some("false")).is_ok());
        for mode in ["live", "micro_live", "paper", "testnet"] {
            assert!(validate_read_only_mode(Some(mode), Some("false")).is_err());
        }
        assert!(validate_read_only_mode(Some("observe"), None).is_err());
        assert!(validate_read_only_mode(Some("observe"), Some("true")).is_err());
    }

    #[test]
    fn reconnect_invalidates_metadata_before_connected_event() {
        let mut lifecycle = Lifecycle::default();
        lifecycle.begin(1);
        let instrument = InstrumentMetadata {
            symbol: "BTCUSDT".into(),
            status: "Trading".into(),
            base_coin: "BTC".into(),
            quote_coin: "USDT".into(),
            settle_coin: None,
            tick_size: Some(0.01),
            qty_step: Some(0.001),
            min_order_qty: None,
            min_order_amt: Some(5.),
            max_market_order_qty: Some(100.),
            timestamp: 1000,
        };
        lifecycle
            .metadata
            .insert(instrument.symbol.clone(), instrument.clone());
        lifecycle.connected = true;
        lifecycle.invalidate();
        assert!(lifecycle.metadata.is_empty());
        assert!(!lifecycle.connected);
        lifecycle.begin(2);
        lifecycle
            .metadata
            .insert(instrument.symbol.clone(), instrument);
        // Connected messages only acknowledge this generation, never erase its metadata.
        lifecycle.connected = true;
        assert_eq!(lifecycle.metadata.len(), 1);
        lifecycle.invalidate();
        assert!(lifecycle.recovering);
        assert!(lifecycle.metadata.is_empty());
    }
}
