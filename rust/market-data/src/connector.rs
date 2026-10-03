use std::{collections::HashMap, time::Duration};

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

pub async fn run(config: Config, sender: mpsc::Sender<MarketDataEvent>) -> Result<()> {
    let mut metadata_delay = config.reconnect_min;
    loop {
        match fetch_and_emit_instruments(&config, &sender).await {
            Ok(()) => break,
            Err(error) => {
                warn!(
                    error = %error,
                    retry_ms = metadata_delay.as_millis(),
                    "instrument metadata load failed"
                );
                emit_status(
                    &sender,
                    "metadata_retry",
                    format!(
                        "instrument metadata unavailable: {error}; retrying in {} ms",
                        metadata_delay.as_millis()
                    ),
                )
                .await;
                time::sleep(metadata_delay).await;
                metadata_delay = (metadata_delay * 2).min(config.reconnect_max);
            }
        }
    }

    let mut delay = config.reconnect_min;
    loop {
        match run_connection(&config, &sender).await {
            Ok(()) => delay = config.reconnect_min,
            Err(error) => {
                warn!(error = %error, reconnect_ms = delay.as_millis(), "market data connection ended");
                emit_status(
                    &sender,
                    "reconnecting",
                    format!("{error}; retrying in {} ms", delay.as_millis()),
                )
                .await;
                time::sleep(delay).await;
                delay = (delay * 2).min(config.reconnect_max);
            }
        }
    }
}

async fn run_connection(config: &Config, sender: &mpsc::Sender<MarketDataEvent>) -> Result<()> {
    let url = config.websocket_url();
    info!(%url, "connecting to Bybit public websocket");
    let (socket, _) = connect_async(&url)
        .await
        .with_context(|| format!("failed to connect to {url}"))?;
    let (mut write, mut read) = socket.split();

    let topics = subscription_topics(config);
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

    let stale_check_every = (config.stale_after / 2).max(Duration::from_millis(500));
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
    sender: &mpsc::Sender<MarketDataEvent>,
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
    sender: &mpsc::Sender<MarketDataEvent>,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    for symbol in &config.symbols {
        let response = client
            .get(format!(
                "{}/v5/market/instruments-info",
                config.rest_base_url()
            ))
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

        sender
            .send(MarketDataEvent::Instrument(InstrumentMetadata {
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
                qty_step: parse_optional(
                    instrument
                        .lot_size_filter
                        .as_ref()
                        .and_then(|filter| filter.qty_step.as_deref()),
                )?,
                min_order_qty: parse_optional(
                    instrument
                        .lot_size_filter
                        .as_ref()
                        .and_then(|filter| filter.min_order_qty.as_deref()),
                )?,
                timestamp: response.time,
            }))
            .await?;
    }

    Ok(())
}

fn parse_optional(value: Option<&str>) -> Result<Option<f64>> {
    value
        .map(|value| {
            value
                .parse::<f64>()
                .context("invalid instrument numeric field")
        })
        .transpose()
}

async fn emit_status(sender: &mpsc::Sender<MarketDataEvent>, state: &str, detail: String) {
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
}
