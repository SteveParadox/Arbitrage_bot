use orderbook::BookUpdate;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MarketDataEvent {
    OrderBook(BookUpdate),
    Quote(NormalizedQuote),
    Trade(NormalizedTrade),
    Instrument(InstrumentMetadata),
    Ticker(TickerUpdate),
    Status(StatusEvent),
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct NormalizedQuote {
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct NormalizedTrade {
    pub symbol: String,
    pub side: String,
    pub price: f64,
    pub size: f64,
    pub trade_id: Option<String>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TickerUpdate {
    pub symbol: String,
    pub last_price: Option<f64>,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct InstrumentMetadata {
    pub symbol: String,
    pub status: String,
    pub base_coin: String,
    pub quote_coin: String,
    pub settle_coin: Option<String>,
    pub tick_size: Option<f64>,
    pub qty_step: Option<f64>,
    pub min_order_qty: Option<f64>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct StatusEvent {
    pub state: String,
    pub detail: String,
    pub timestamp: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct WsEnvelope {
    pub topic: Option<String>,
    #[serde(rename = "type")]
    pub message_type: Option<String>,
    pub ts: Option<u64>,
    pub data: Option<serde_json::Value>,
    pub success: Option<bool>,
    pub op: Option<String>,
    pub ret_msg: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct OrderbookData {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b", default)]
    pub bids: Vec<[String; 2]>,
    #[serde(rename = "a", default)]
    pub asks: Vec<[String; 2]>,
    #[serde(rename = "u")]
    pub update_id: u64,
    pub seq: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TradeData {
    #[serde(rename = "T")]
    pub timestamp: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "S")]
    pub side: String,
    #[serde(rename = "v")]
    pub size: String,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "i")]
    pub trade_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TickerData {
    pub symbol: String,
    #[serde(rename = "lastPrice")]
    pub last_price: Option<String>,
    #[serde(rename = "bid1Price")]
    pub bid: Option<String>,
    #[serde(rename = "ask1Price")]
    pub ask: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct InstrumentsResponse {
    #[serde(rename = "retCode")]
    pub ret_code: i64,
    #[serde(rename = "retMsg")]
    pub ret_msg: String,
    pub result: InstrumentsResult,
    pub time: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct InstrumentsResult {
    pub list: Vec<RawInstrument>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RawInstrument {
    pub symbol: String,
    pub status: String,
    pub base_coin: String,
    pub quote_coin: String,
    pub settle_coin: Option<String>,
    pub price_filter: Option<PriceFilter>,
    pub lot_size_filter: Option<LotSizeFilter>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PriceFilter {
    pub tick_size: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LotSizeFilter {
    pub qty_step: Option<String>,
    pub min_order_qty: Option<String>,
}


#[cfg(test)]
mod tests {
    use orderbook::{BookUpdate, PriceLevel};

    use super::*;

    #[test]
    fn serializes_order_book_event_for_scanner_pipe() {
        let event = MarketDataEvent::OrderBook(BookUpdate {
            symbol: "BTCUSDT".to_string(),
            bids: vec![PriceLevel {
                price: 68_250.1,
                quantity: 0.5,
            }],
            asks: vec![PriceLevel {
                price: 68_250.2,
                quantity: 0.4,
            }],
            timestamp: 1_790_549_000_000,
            update_id: 10,
            sequence: 20,
            is_snapshot: true,
        });

        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["type"], "order_book");
        assert_eq!(value["symbol"], "BTCUSDT");
        assert_eq!(value["sequence"], 20);
    }
}
