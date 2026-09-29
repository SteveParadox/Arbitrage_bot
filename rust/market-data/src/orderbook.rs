use std::collections::{BTreeMap, HashMap};

use ordered_float::OrderedFloat;
use thiserror::Error;

use crate::model::{NormalizedQuote, OrderbookData};

#[derive(Debug, Error, PartialEq)]
pub enum OrderbookError {
    #[error("sequence regressed for {symbol}: last={last}, next={next}")]
    SequenceRegression {
        symbol: String,
        last: u64,
        next: u64,
    },
    #[error("update id regressed for {symbol}: last={last}, next={next}")]
    UpdateRegression {
        symbol: String,
        last: u64,
        next: u64,
    },
    #[error("invalid numeric level for {symbol}")]
    InvalidLevel { symbol: String },
}

#[derive(Debug, Default)]
struct Book {
    bids: BTreeMap<OrderedFloat<f64>, f64>,
    asks: BTreeMap<OrderedFloat<f64>, f64>,
    last_seq: Option<u64>,
    last_update_id: Option<u64>,
}

#[derive(Debug, Default)]
pub struct OrderbookStore {
    books: HashMap<String, Book>,
}

impl OrderbookStore {
    pub(crate) fn apply(
        &mut self,
        kind: &str,
        timestamp: u64,
        data: OrderbookData,
    ) -> Result<Option<NormalizedQuote>, OrderbookError> {
        let book = self.books.entry(data.symbol.clone()).or_default();
        let is_snapshot = kind == "snapshot" || data.update_id == 1;

        if !is_snapshot {
            if let Some(last) = book.last_seq {
                if data.seq <= last {
                    return Err(OrderbookError::SequenceRegression {
                        symbol: data.symbol,
                        last,
                        next: data.seq,
                    });
                }
            }
            if let Some(last) = book.last_update_id {
                if data.update_id <= last {
                    return Err(OrderbookError::UpdateRegression {
                        symbol: data.symbol,
                        last,
                        next: data.update_id,
                    });
                }
            }
        }

        if is_snapshot {
            book.bids.clear();
            book.asks.clear();
        }

        apply_levels(&data.symbol, &mut book.bids, data.bids)?;
        apply_levels(&data.symbol, &mut book.asks, data.asks)?;
        book.last_seq = Some(data.seq);
        book.last_update_id = Some(data.update_id);

        let bid = book.bids.iter().next_back().map(|(price, _)| price.0);
        let ask = book.asks.iter().next().map(|(price, _)| price.0);

        Ok(match (bid, ask) {
            (Some(bid), Some(ask)) => Some(NormalizedQuote {
                symbol: data.symbol,
                bid,
                ask,
                timestamp,
            }),
            _ => None,
        })
    }
}

fn apply_levels(
    symbol: &str,
    side: &mut BTreeMap<OrderedFloat<f64>, f64>,
    levels: Vec<[String; 2]>,
) -> Result<(), OrderbookError> {
    for [price, size] in levels {
        let price = price
            .parse::<f64>()
            .map_err(|_| OrderbookError::InvalidLevel {
                symbol: symbol.to_string(),
            })?;
        let size = size
            .parse::<f64>()
            .map_err(|_| OrderbookError::InvalidLevel {
                symbol: symbol.to_string(),
            })?;
        let key = OrderedFloat(price);
        if size == 0.0 {
            side.remove(&key);
        } else {
            side.insert(key, size);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> OrderbookData {
        OrderbookData {
            symbol: "BTCUSDT".into(),
            bids: vec![["100".into(), "2".into()], ["99".into(), "1".into()]],
            asks: vec![["101".into(), "3".into()], ["102".into(), "1".into()]],
            update_id: 10,
            seq: 100,
        }
    }

    #[test]
    fn derives_best_bid_and_ask() {
        let mut store = OrderbookStore::default();
        let quote = store.apply("snapshot", 123, snapshot()).unwrap().unwrap();
        assert_eq!(quote.bid, 100.0);
        assert_eq!(quote.ask, 101.0);
        assert_eq!(quote.timestamp, 123);
    }

    #[test]
    fn applies_delta_and_deletes_zero_size() {
        let mut store = OrderbookStore::default();
        store.apply("snapshot", 1, snapshot()).unwrap();
        let delta = OrderbookData {
            symbol: "BTCUSDT".into(),
            bids: vec![["100".into(), "0".into()], ["100.5".into(), "1".into()]],
            asks: vec![],
            update_id: 11,
            seq: 101,
        };
        let quote = store.apply("delta", 2, delta).unwrap().unwrap();
        assert_eq!(quote.bid, 100.5);
    }

    #[test]
    fn rejects_regressing_sequence() {
        let mut store = OrderbookStore::default();
        store.apply("snapshot", 1, snapshot()).unwrap();
        let delta = OrderbookData {
            symbol: "BTCUSDT".into(),
            bids: vec![],
            asks: vec![],
            update_id: 11,
            seq: 99,
        };
        assert!(matches!(
            store.apply("delta", 2, delta),
            Err(OrderbookError::SequenceRegression { .. })
        ));
    }
}
