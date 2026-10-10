use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use orderbook::{BookUpdate, OrderBookEngine, PriceLevel};
use serde_json::json;
use tokio::{
    sync::mpsc,
    time::{self, Instant},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::{
    config::Config,
    model::{
        InstrumentMetadata, InstrumentsResponse, MarketDataEvent, NormalizedQuote, NormalizedTrade,
        OrderbookData, StatusEvent, TickerData, TickerUpdate, TradeData, WsEnvelope,
    },
};

type TickerCache = HashMap<String, (Option<f64>, Option<f64>, Option<f64>)>;

/// Backward-compatible public feed; managed observers may request resynchronization.
pub async fn run(config: Config, sender: mpsc::Sender<MarketDataEvent>) -> Result<()> {
    let (_recovery_sender, recovery) = mpsc::channel(1);
    let (scoped, mut receiver) = mpsc::channel(1024);
    let forwarding = tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            if let MarketDataEvent::Session { event, .. } = event {
                if sender.send(*event).await.is_err() {
                    break;
                }
            }
        }
    });
    let result = run_with_recovery(config, scoped, recovery).await;
    forwarding.abort();
    result
}

struct EventSender {
    sender: mpsc::Sender<MarketDataEvent>,
    generation: u64,
}
impl EventSender {
    async fn send(&self, event: MarketDataEvent) -> Result<()> {
        self.sender
            .send(MarketDataEvent::Session {
                generation: self.generation,
                event: Box::new(event),
            })
            .await
            .map_err(|_| anyhow!("market-data receiver closed"))
    }
}

pub async fn run_with_recovery(
    config: Config,
    sender: mpsc::Sender<MarketDataEvent>,
    recovery: mpsc::Receiver<()>,
) -> Result<()> {
    let rest_url = config.rest_base_url().to_string();
    let websocket_url = config.websocket_url();
    run_supervised(config, sender, recovery, rest_url, websocket_url).await
}

async fn run_supervised(
    config: Config,
    sender: mpsc::Sender<MarketDataEvent>,
    mut recovery: mpsc::Receiver<()>,
    rest_url: String,
    websocket_url: String,
) -> Result<()> {
    let mut delay = config.reconnect_min;
    let mut generation = 0_u64;
    loop {
        generation = generation
            .checked_add(1)
            .context("connection generation exhausted")?;
        let scoped = EventSender {
            sender: sender.clone(),
            generation,
        };
        emit_status(
            &scoped,
            "connecting",
            "loading complete instrument snapshot".into(),
        )
        .await;
        let started = Instant::now();
        let attempt = async {
            fetch_and_emit_instruments(&config, &scoped, &rest_url).await?;
            run_connection(&config, &scoped, &websocket_url).await
        };
        let error = tokio::select! {
            result = attempt => result.err().unwrap_or_else(|| anyhow!("connector ended")),
            Some(()) = recovery.recv() => anyhow!("scanner requested fresh snapshots"),
        };
        if sender.is_closed() {
            return Err(error);
        }
        // Only a sustained session resets backoff; rapid disconnects remain bounded.
        if started.elapsed() >= config.reconnect_max {
            delay = config.reconnect_min;
        }
        warn!(%error, reconnect_ms = delay.as_millis(), generation, "market data recovery");
        emit_status(
            &scoped,
            "reconnecting",
            format!("{error}; retry in {} ms", delay.as_millis()),
        )
        .await;
        time::sleep(delay).await;
        while recovery.try_recv().is_ok() {}
        delay = (delay * 2).min(config.reconnect_max);
    }
}

async fn run_connection(config: &Config, sender: &EventSender, url: &str) -> Result<()> {
    info!(%url, "connecting to Bybit public websocket");
    let (socket, _) = connect_async(url)
        .await
        .with_context(|| format!("failed to connect to {url}"))?;
    let (mut write, mut read) = socket.split();

    let topics = subscription_topics(config);
    let mut pending_subscriptions: HashSet<String> = subscription_requests(&topics)
        .iter()
        .filter_map(|v| v["req_id"].as_str().map(str::to_owned))
        .collect();
    validate_subscription_topics(&topics)?;
    for request in subscription_requests(&topics) {
        write
            .send(Message::Text(request.to_string()))
            .await
            .context("failed to send subscription request")?;
    }

    emit_status(
        sender,
        "connected",
        format!("requested {} topics; awaiting snapshots", topics.len()),
    )
    .await;

    let mut heartbeat = time::interval(config.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    let health_interval_ms = std::env::var("BYBIT_HEALTH_INTERVAL_MS")
        .unwrap_or_else(|_| "250".into())
        .parse::<u64>()
        .context("invalid BYBIT_HEALTH_INTERVAL_MS")?;
    if !(50..=5000).contains(&health_interval_ms) {
        return Err(anyhow!(
            "BYBIT_HEALTH_INTERVAL_MS must be between 50 and 5000"
        ));
    }
    let stale_check_every = Duration::from_millis(health_interval_ms);
    let mut stale_check = time::interval(stale_check_every);
    stale_check.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    let mut book_receipts: HashMap<String, Instant> = config
        .symbols
        .iter()
        .map(|symbol| (symbol.clone(), Instant::now()))
        .collect();
    let mut books = OrderBookEngine::default();
    let mut ticker_cache = TickerCache::new();

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                write.send(Message::Text(
                    json!({"op": "ping", "req_id": "market-data-heartbeat"}).to_string()
                )).await.context("failed to send heartbeat")?;
            }
            _ = stale_check.tick() => {
                let health = crate::health::snapshot(&config.symbols, &pending_subscriptions,
                    &books, &book_receipts, config.stale_after, now_ms());
                sender.send(MarketDataEvent::Health(health)).await?;
                if let Some((symbol, received)) = book_receipts.iter()
                    .find(|(_, received)| received.elapsed() > config.stale_after) {
                    return Err(anyhow!(
                        "stale order book {symbol}: no book event for {} ms",
                        received.elapsed().as_millis()
                    ));
                }
            }
            message = read.next() => {
                let message = message.ok_or_else(|| anyhow!("websocket stream closed"))??;
                match message {
                    Message::Text(text) => {
                        let envelope: WsEnvelope = serde_json::from_str(text.as_ref())?;
                        if envelope.op.as_deref() == Some("subscribe") {
                            let request_id = envelope.req_id.as_deref().ok_or_else(|| anyhow!("subscription acknowledgement missing req_id"))?;
                            if envelope.success != Some(true) || !pending_subscriptions.remove(request_id) {
                                return Err(anyhow!("subscription acknowledgement rejected or unexpected"));
                            }
                        }
                        handle_text(
                            text.as_ref(),
                            &mut books,
                            &mut ticker_cache,
                            &mut book_receipts,
                            sender,
                        ).await?;
                    }
                    Message::Ping(payload) => {
                        write.send(Message::Pong(payload)).await?;
                    }
                    Message::Pong(_) => {}
                    Message::Close(frame) => {
                        return Err(anyhow!("websocket closed: {frame:?}"));
                    }
                    _ => {}
                }
            }
        }
    }
}

fn subscription_topics(config: &Config) -> Vec<String> {
    let mut topics = Vec::new();
    for symbol in &config.symbols {
        topics.push(format!("orderbook.{}.{}", config.orderbook_depth, symbol));
        if config.subscribe_trades {
            topics.push(format!("publicTrade.{symbol}"));
        }
        if config.subscribe_tickers {
            topics.push(format!("tickers.{symbol}"));
        }
    }
    topics
}

fn validate_subscription_topics(topics: &[String]) -> Result<()> {
    let total_chars = topics.iter().map(String::len).sum::<usize>();
    if total_chars > 21_000 {
        return Err(anyhow!(
            "public websocket topic args use {total_chars} characters; Bybit limit is 21000 per connection"
        ));
    }
    Ok(())
}

fn subscription_requests(topics: &[String]) -> Vec<serde_json::Value> {
    topics
        .chunks(10)
        .enumerate()
        .map(|(index, chunk)| {
            json!({
                "op": "subscribe", "args": chunk, "req_id": format!("market-data-{index}")
            })
        })
        .collect()
}

async fn handle_text(
    text: &str,
    books: &mut OrderBookEngine,
    ticker_cache: &mut TickerCache,
    book_receipts: &mut HashMap<String, Instant>,
    sender: &EventSender,
) -> Result<bool> {
    let envelope: WsEnvelope =
        serde_json::from_str(text).context("failed to decode Bybit websocket message")?;

    if envelope.op.as_deref() == Some("ping") || envelope.ret_msg.as_deref() == Some("pong") {
        return Ok(false);
    }
    if envelope.op.as_deref() == Some("subscribe") {
        if envelope.success == Some(false) {
            return Err(anyhow!("Bybit rejected subscription: {text}"));
        }
        return Ok(false);
    }

    let Some(topic) = envelope.topic.as_deref() else {
        return Ok(false);
    };
    let timestamp = envelope
        .ts
        .ok_or_else(|| anyhow!("topic message missing timestamp"))?;
    let data = envelope
        .data
        .ok_or_else(|| anyhow!("topic message missing data: {topic}"))?;

    if topic.starts_with("orderbook.") {
        let data: OrderbookData = serde_json::from_value(data)?;
        let symbol = data.symbol.clone();
        if !topic.ends_with(&format!(".{symbol}")) || !book_receipts.contains_key(&symbol) {
            return Err(anyhow!("unexpected order book symbol/topic"));
        }
        let is_snapshot =
            envelope.message_type.as_deref() == Some("snapshot") || data.update_id == 1;

        let bids = parse_levels(&symbol, data.bids)?;
        let asks = parse_levels(&symbol, data.asks)?;

        let update = BookUpdate {
            symbol: symbol.clone(),
            bids,
            asks,
            timestamp,
            update_id: data.update_id,
            sequence: data.seq,
            is_snapshot,
        };
        books.apply(update.clone())?;
        book_receipts.insert(symbol.clone(), Instant::now());
        sender
            .send(MarketDataEvent::OrderBook {
                symbol: update.symbol,
                bids: update.bids,
                asks: update.asks,
                timestamp: update.timestamp,
                update_id: update.update_id,
                sequence: update.sequence,
                is_snapshot: update.is_snapshot,
            })
            .await?;

        if let Some(book) = books.get(&symbol) {
            if let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) {
                sender
                    .send(MarketDataEvent::Quote(NormalizedQuote {
                        symbol,
                        bid: bid.price,
                        ask: ask.price,
                        timestamp: book.timestamp(),
                    }))
                    .await?;
            }
        }
        return Ok(true);
    }

    if topic.starts_with("publicTrade.") {
        let trades: Vec<TradeData> = serde_json::from_value(data)?;
        for trade in trades {
            sender
                .send(MarketDataEvent::Trade(NormalizedTrade {
                    symbol: trade.symbol,
                    side: trade.side,
                    price: trade.price.parse().context("invalid trade price")?,
                    size: trade.size.parse().context("invalid trade size")?,
                    trade_id: trade.trade_id,
                    timestamp: trade.timestamp,
                }))
                .await?;
        }
        return Ok(true);
    }

    if topic.starts_with("tickers.") {
        let ticker: TickerData = serde_json::from_value(data)?;
        let entry = ticker_cache
            .entry(ticker.symbol.clone())
            .or_insert((None, None, None));
        merge_number(&mut entry.0, ticker.last_price.as_deref())?;
        merge_number(&mut entry.1, ticker.bid.as_deref())?;
        merge_number(&mut entry.2, ticker.ask.as_deref())?;

        sender
            .send(MarketDataEvent::Ticker(TickerUpdate {
                symbol: ticker.symbol,
                last_price: entry.0,
                bid: entry.1,
                ask: entry.2,
                timestamp,
            }))
            .await?;
        return Ok(true);
    }

    Ok(false)
}

fn parse_levels(symbol: &str, levels: Vec<[String; 2]>) -> Result<Vec<PriceLevel>> {
    levels
        .into_iter()
        .map(|[price, quantity]| {
            Ok(PriceLevel {
                price: price
                    .parse::<f64>()
                    .with_context(|| format!("invalid order-book price for {symbol}"))?,
                quantity: quantity
                    .parse::<f64>()
                    .with_context(|| format!("invalid order-book quantity for {symbol}"))?,
            })
        })
        .collect()
}

fn merge_number(slot: &mut Option<f64>, value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        *slot = Some(
            value
                .parse::<f64>()
                .context("invalid ticker numeric field")?,
        );
    }
    Ok(())
}

async fn fetch_and_emit_instruments(
    config: &Config,
    sender: &EventSender,
    rest_url: &str,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let mut snapshot = Vec::with_capacity(config.symbols.len());
    for symbol in &config.symbols {
        let response = client
            .get(format!("{}/v5/market/instruments-info", rest_url))
            .query(&[
                ("category", config.category.as_str()),
                ("symbol", symbol.as_str()),
            ])
            .send()
            .await
            .with_context(|| format!("failed to load metadata for {symbol}"))?
            .error_for_status()?
            .json::<InstrumentsResponse>()
            .await?;

        if response.ret_code != 0 {
            return Err(anyhow!(
                "Bybit instrument metadata error {}: {}",
                response.ret_code,
                response.ret_msg
            ));
        }

        let instrument = response
            .result
            .list
            .into_iter()
            .find(|item| item.symbol == *symbol)
            .ok_or_else(|| anyhow!("instrument metadata not found for {symbol}"))?;

        snapshot.push(normalize_instrument(instrument, response.time)?);
    }

    // No partially refreshed snapshot is emitted if any required fetch fails.
    for instrument in snapshot {
        sender.send(MarketDataEvent::Instrument(instrument)).await?;
    }
    Ok(())
}

fn normalize_instrument(
    instrument: crate::model::RawInstrument,
    timestamp: u64,
) -> Result<InstrumentMetadata> {
    let lot = instrument.lot_size_filter.as_ref();
    Ok(InstrumentMetadata {
        symbol: instrument.symbol,
        status: instrument.status,
        base_coin: instrument.base_coin,
        quote_coin: instrument.quote_coin,
        settle_coin: instrument.settle_coin,
        tick_size: parse_optional(
            instrument
                .price_filter
                .as_ref()
                .and_then(|filter| filter.tick_size.as_deref()),
        )?,
        qty_step: parse_optional(lot.and_then(|filter| {
            filter
                .base_precision
                .as_deref()
                .or(filter.qty_step.as_deref())
        }))?,
        min_order_qty: parse_optional(lot.and_then(|filter| filter.min_order_qty.as_deref()))?,
        min_order_amt: parse_optional(lot.and_then(|filter| filter.min_order_amt.as_deref()))?,
        max_market_order_qty: parse_optional(
            lot.and_then(|filter| filter.max_market_order_qty.as_deref()),
        )?,
        timestamp,
    })
}

fn parse_optional(value: Option<&str>) -> Result<Option<f64>> {
    value
        .map(|value| {
            let number = value
                .parse::<f64>()
                .context("invalid instrument numeric field")?;
            if !number.is_finite() || number <= 0.0 {
                return Err(anyhow!("instrument filter must be finite and positive"));
            }
            Ok(number)
        })
        .transpose()
}

async fn emit_status(sender: &EventSender, state: &str, detail: String) {
    let _ = sender
        .send(MarketDataEvent::Status(StatusEvent {
            state: state.to_string(),
            detail,
            timestamp: now_ms(),
        }))
        .await;
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spot_filters_use_base_precision_and_quote_minimum() {
        let raw = serde_json::from_value(json!({
            "symbol":"BTCUSDT", "status":"Trading", "baseCoin":"BTC",
            "quoteCoin":"USDT", "priceFilter":{"tickSize":"0.01"},
            "lotSizeFilter":{"basePrecision":"0.000001", "quotePrecision":"0.00000001",
                "minOrderQty":"0.000001", "minOrderAmt":"5",
                "maxMarketOrderQty":"2"}
        }))
        .unwrap();
        let metadata = normalize_instrument(raw, 123).unwrap();
        assert_eq!(metadata.qty_step, Some(0.000001));
        assert_eq!(metadata.min_order_amt, Some(5.0));
        assert_eq!(metadata.max_market_order_qty, Some(2.0));
        assert_eq!(metadata.tick_size, Some(0.01));
    }

    #[test]
    fn spot_subscriptions_are_batched_at_ten_arguments() {
        let topics: Vec<String> = (0..31).map(|i| format!("tickers.SYM{i}")).collect();
        let requests = subscription_requests(&topics);
        assert_eq!(requests.len(), 4);
        assert_eq!(requests[0]["args"].as_array().unwrap().len(), 10);
        assert_eq!(requests[3]["args"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn ticker_activity_cannot_refresh_orderbook_freshness() {
        let (sender, _receiver) = mpsc::channel(10);
        let sender = EventSender {
            sender,
            generation: 1,
        };
        let mut books = OrderBookEngine::default();
        let mut cache = HashMap::new();
        let old = Instant::now() - Duration::from_secs(20);
        let mut receipts = HashMap::from([("BTCUSDT".to_string(), old)]);
        handle_text(r#"{"topic":"tickers.BTCUSDT","ts":1000,"data":{"symbol":"BTCUSDT","lastPrice":"100"}}"#,
            &mut books, &mut cache, &mut receipts, &sender).await.unwrap();
        assert_eq!(receipts["BTCUSDT"], old);
        assert!(handle_text(
            r#"{"topic":"orderbook.50.BTCUSDT","data":{}}"#,
            &mut books,
            &mut cache,
            &mut receipts,
            &sender
        )
        .await
        .is_err());
    }

    #[test]
    fn builds_all_topics_per_symbol() {
        let config = Config {
            testnet: true,
            category: crate::config::Category::Linear,
            symbols: vec!["BTCUSDT".into(), "ETHUSDT".into()],
            orderbook_depth: 50,
            subscribe_trades: true,
            subscribe_tickers: true,
            heartbeat_interval: Duration::from_secs(20),
            stale_after: Duration::from_secs(10),
            reconnect_min: Duration::from_millis(500),
            reconnect_max: Duration::from_secs(30),
        };
        let topics = subscription_topics(&config);
        assert_eq!(topics.len(), 6);
        assert!(topics.contains(&"orderbook.50.BTCUSDT".to_string()));
        assert!(topics.contains(&"publicTrade.ETHUSDT".to_string()));
        assert!(topics.contains(&"tickers.BTCUSDT".to_string()));
    }

    #[test]
    fn can_subscribe_to_orderbooks_only() {
        let config = Config {
            testnet: false,
            category: crate::config::Category::Spot,
            symbols: vec!["BTCUSDT".into(), "ETHUSDT".into()],
            orderbook_depth: 50,
            subscribe_trades: false,
            subscribe_tickers: false,
            heartbeat_interval: Duration::from_secs(20),
            stale_after: Duration::from_secs(10),
            reconnect_min: Duration::from_millis(500),
            reconnect_max: Duration::from_secs(30),
        };
        assert_eq!(
            subscription_topics(&config),
            vec![
                "orderbook.50.BTCUSDT".to_string(),
                "orderbook.50.ETHUSDT".to_string(),
            ]
        );
    }

    #[test]
    fn rejects_topic_sets_above_connection_character_limit() {
        let topics = vec!["x".repeat(10_501), "y".repeat(10_500)];
        assert!(validate_subscription_topics(&topics).is_err());
    }

    #[test]
    fn parses_bybit_levels() {
        let levels =
            parse_levels("BTCUSDT", vec![["68250.1".to_string(), "0.25".to_string()]]).unwrap();

        assert_eq!(levels[0].price, 68250.1);
        assert_eq!(levels[0].quantity, 0.25);
    }
    async fn mock_metadata(missing: bool) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buffer = vec![0_u8; 4096];
                let count = socket.read(&mut buffer).await.unwrap();
                let request = String::from_utf8_lossy(&buffer[..count]);
                let symbol = if request.contains("symbol=ETHUSDT") {
                    "ETHUSDT"
                } else {
                    "BTCUSDT"
                };
                let list = if missing && symbol == "ETHUSDT" {
                    json!([])
                } else {
                    json!([{
                        "symbol":symbol,"status":"Trading","baseCoin":if symbol == "BTCUSDT" {"BTC"} else {"ETH"},
                        "quoteCoin":"USDT","priceFilter":{"tickSize":"0.01"},
                        "lotSizeFilter":{"basePrecision":"0.001","minOrderAmt":"5","maxMarketOrderQty":"100"}
                    }])
                };
                let body =
                    json!({"retCode":0,"retMsg":"OK","result":{"list":list},"time":now_ms()})
                        .to_string();
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body);
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (format!("http://{address}"), task)
    }

    fn mock_config() -> Config {
        Config {
            testnet: true,
            category: crate::config::Category::Spot,
            symbols: vec!["BTCUSDT".into(), "ETHUSDT".into()],
            orderbook_depth: 50,
            subscribe_trades: false,
            subscribe_tickers: false,
            heartbeat_interval: Duration::from_secs(1),
            stale_after: Duration::from_secs(2),
            reconnect_min: Duration::from_millis(10),
            reconnect_max: Duration::from_millis(100),
        }
    }

    #[tokio::test]
    async fn metadata_snapshot_is_atomic_on_missing_instrument() {
        let (url, server) = mock_metadata(true).await;
        let (sender, mut receiver) = mpsc::channel(10);
        let scoped = EventSender {
            sender,
            generation: 1,
        };
        assert!(fetch_and_emit_instruments(&mock_config(), &scoped, &url)
            .await
            .is_err());
        assert!(receiver.try_recv().is_err());
        server.abort();
    }

    #[tokio::test]
    async fn scanner_recovery_reconnects_and_refreshes_metadata_generation() {
        let (url, http) = mock_metadata(false).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_url = format!("ws://{}", listener.local_addr().unwrap());
        let websocket = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
                // Keep transport alive while the supervised scanner requests resubscription.
                while let Some(Ok(message)) = ws.next().await {
                    if let Message::Text(text) = message {
                        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                        if value["op"] == "subscribe"
                            && ws
                                .send(Message::Text(
                                    json!({"op":"subscribe","success":true,
                                "req_id":value["req_id"]})
                                    .to_string(),
                                ))
                                .await
                                .is_err()
                        {
                            break;
                        }
                    }
                }
            }
        });
        let (sender, mut receiver) = mpsc::channel(100);
        let (request, recovery) = mpsc::channel(1);
        let task = tokio::spawn(run_supervised(mock_config(), sender, recovery, url, ws_url));
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            let mut metadata_generations = std::collections::BTreeSet::new();
            while let Some(MarketDataEvent::Session { generation, event }) = receiver.recv().await {
                match *event {
                    MarketDataEvent::Instrument(_) => {
                        metadata_generations.insert(generation);
                    }
                    MarketDataEvent::Status(status)
                        if status.state == "connected" && generation == 1 =>
                    {
                        request.send(()).await.unwrap();
                    }
                    MarketDataEvent::Status(status)
                        if status.state == "connected" && generation == 2 =>
                    {
                        assert_eq!(
                            metadata_generations,
                            std::collections::BTreeSet::from([1, 2])
                        );
                        break;
                    }
                    _ => {}
                }
            }
        })
        .await;
        task.abort();
        http.abort();
        websocket.abort();
        result.unwrap();
    }
}
