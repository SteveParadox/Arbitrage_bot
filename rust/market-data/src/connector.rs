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
    write
        .send(Message::Text(
            json!({
                "op": "subscribe",
                "args": topics,
                "req_id": "market-data-subscribe"
            })
            .to_string()
            .into(),
        ))
        .await
        .context("failed to send subscription request")?;

    emit_status(
        sender,
        "connected",
        format!("subscribed to {} topics", topics.len()),
    )
    .await;

    let mut heartbeat = time::interval(config.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    let stale_check_every = (config.stale_after / 2).max(Duration::from_millis(500));
    let mut stale_check = time::interval(stale_check_every);
    stale_check.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    let mut last_market_data = Instant::now();
    let mut books = OrderBookEngine::default();
    let mut ticker_cache: HashMap<String, (Option<f64>, Option<f64>, Option<f64>)> = HashMap::new();

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                write.send(Message::Text(
                    json!({"op": "ping", "req_id": "market-data-heartbeat"}).to_string().into()
                )).await.context("failed to send heartbeat")?;
            }
            _ = stale_check.tick() => {
                if last_market_data.elapsed() > config.stale_after {
                    return Err(anyhow!(
                        "stale market data: no market event for {} ms",
                        last_market_data.elapsed().as_millis()
                    ));
                }
            }
            message = read.next() => {
                let message = message.ok_or_else(|| anyhow!("websocket stream closed"))??;
                match message {
                    Message::Text(text) => {
                        let observed_market_data = handle_text(
                            text.as_ref(),
                            &mut books,
                            &mut ticker_cache,
                            sender,
                        ).await?;
                        if observed_market_data {
                            last_market_data = Instant::now();
                        }
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
    config
        .symbols
        .iter()
        .flat_map(|symbol| {
            [
                format!("orderbook.{}.{}", config.orderbook_depth, symbol),
                format!("publicTrade.{symbol}"),
                format!("tickers.{symbol}"),
            ]
        })
        .collect()
}

async fn handle_text(
    text: &str,
    books: &mut OrderBookEngine,
    ticker_cache: &mut HashMap<String, (Option<f64>, Option<f64>, Option<f64>)>,
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
    let timestamp = envelope.ts.unwrap_or_else(now_ms);
    let data = envelope
        .data
        .ok_or_else(|| anyhow!("topic message missing data: {topic}"))?;

    if topic.starts_with("orderbook.") {
        let data: OrderbookData = serde_json::from_value(data)?;
        let symbol = data.symbol.clone();
        let is_snapshot =
            envelope.message_type.as_deref() == Some("snapshot") || data.update_id == 1;

        let bids = parse_levels(&symbol, data.bids)?;
        let asks = parse_levels(&symbol, data.asks)?;

        books.apply(BookUpdate {
            symbol: symbol.clone(),
            bids,
            asks,
            timestamp,
            update_id: data.update_id,
            sequence: data.seq,
            is_snapshot,
        })?;

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
        *slot = Some(value.parse::<f64>().context("invalid ticker numeric field")?);
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
    fn builds_all_topics_per_symbol() {
        let config = Config {
            testnet: true,
            category: crate::config::Category::Linear,
            symbols: vec!["BTCUSDT".into(), "ETHUSDT".into()],
            orderbook_depth: 50,
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
    fn parses_bybit_levels() {
        let levels = parse_levels(
            "BTCUSDT",
            vec![["68250.1".to_string(), "0.25".to_string()]],
        )
        .unwrap();

        assert_eq!(levels[0].price, 68250.1);
        assert_eq!(levels[0].quantity, 0.25);
    }
}
