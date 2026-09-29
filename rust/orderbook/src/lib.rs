use std::collections::{BTreeMap, HashMap};

use ordered_float::OrderedFloat;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub struct PriceLevel {
    pub price: f64,
    pub quantity: f64,
}

#[derive(Debug, Clone)]
pub struct BookUpdate {
    pub symbol: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub timestamp: u64,
    pub update_id: u64,
    pub sequence: u64,
    pub is_snapshot: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ExecutionEstimate {
    pub symbol: String,
    pub side: ExecutionSide,
    pub requested_base_quantity: Option<f64>,
    pub requested_quote_quantity: Option<f64>,
    pub filled_base_quantity: f64,
    pub filled_quote_quantity: f64,
    pub average_execution_price: Option<f64>,
    pub best_price: Option<f64>,
    pub worst_price: Option<f64>,
    pub slippage_bps: Option<f64>,
    pub levels_consumed: usize,
    pub complete: bool,
    pub timestamp: u64,
    pub update_id: u64,
    pub sequence: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct BookView {
    pub symbol: String,
    pub best_bid: Option<PriceLevel>,
    pub best_ask: Option<PriceLevel>,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub total_bid_base_quantity: f64,
    pub total_ask_base_quantity: f64,
    pub total_bid_quote_quantity: f64,
    pub total_ask_quote_quantity: f64,
    pub timestamp: u64,
    pub update_id: u64,
    pub sequence: u64,
}

#[derive(Debug, Error, PartialEq)]
pub enum OrderBookError {
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
    #[error("invalid level for {symbol}: price={price}, quantity={quantity}")]
    InvalidLevel {
        symbol: String,
        price: f64,
        quantity: f64,
    },
    #[error("amount must be finite and greater than zero")]
    InvalidAmount,
    #[error("order book not found for {0}")]
    BookNotFound(String),
}

#[derive(Debug, Default, Clone)]
pub struct LocalOrderBook {
    symbol: String,
    bids: BTreeMap<OrderedFloat<f64>, f64>,
    asks: BTreeMap<OrderedFloat<f64>, f64>,
    timestamp: u64,
    update_id: u64,
    sequence: u64,
    initialized: bool,
}

impl LocalOrderBook {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            ..Self::default()
        }
    }

    pub fn apply(&mut self, update: BookUpdate) -> Result<(), OrderBookError> {
        if self.symbol.is_empty() {
            self.symbol = update.symbol.clone();
        }

        if update.symbol != self.symbol {
            return Err(OrderBookError::BookNotFound(update.symbol));
        }

        validate_levels(&update.symbol, &update.bids)?;
        validate_levels(&update.symbol, &update.asks)?;

        if self.initialized && !update.is_snapshot {
            if update.sequence <= self.sequence {
                return Err(OrderBookError::SequenceRegression {
                    symbol: update.symbol,
                    last: self.sequence,
                    next: update.sequence,
                });
            }
            if update.update_id <= self.update_id {
                return Err(OrderBookError::UpdateRegression {
                    symbol: update.symbol,
                    last: self.update_id,
                    next: update.update_id,
                });
            }
        }

        if update.is_snapshot {
            self.bids.clear();
            self.asks.clear();
        }

        apply_levels(&mut self.bids, &update.bids);
        apply_levels(&mut self.asks, &update.asks);

        self.timestamp = update.timestamp;
        self.update_id = update.update_id;
        self.sequence = update.sequence;
        self.initialized = true;
        Ok(())
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn update_id(&self) -> u64 {
        self.update_id
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn best_bid(&self) -> Option<PriceLevel> {
        self.bids
            .iter()
            .next_back()
            .map(|(price, quantity)| PriceLevel {
                price: price.0,
                quantity: *quantity,
            })
    }

    pub fn best_ask(&self) -> Option<PriceLevel> {
        self.asks.iter().next().map(|(price, quantity)| PriceLevel {
            price: price.0,
            quantity: *quantity,
        })
    }

    pub fn top_n(&self, n: usize) -> BookView {
        let bids = self
            .bids
            .iter()
            .rev()
            .take(n)
            .map(|(price, quantity)| PriceLevel {
                price: price.0,
                quantity: *quantity,
            })
            .collect::<Vec<_>>();
        let asks = self
            .asks
            .iter()
            .take(n)
            .map(|(price, quantity)| PriceLevel {
                price: price.0,
                quantity: *quantity,
            })
            .collect::<Vec<_>>();

        let total_bid_base_quantity = bids.iter().map(|level| level.quantity).sum();
        let total_ask_base_quantity = asks.iter().map(|level| level.quantity).sum();
        let total_bid_quote_quantity =
            bids.iter().map(|level| level.price * level.quantity).sum();
        let total_ask_quote_quantity =
            asks.iter().map(|level| level.price * level.quantity).sum();

        BookView {
            symbol: self.symbol.clone(),
            best_bid: self.best_bid(),
            best_ask: self.best_ask(),
            bids,
            asks,
            total_bid_base_quantity,
            total_ask_base_quantity,
            total_bid_quote_quantity,
            total_ask_quote_quantity,
            timestamp: self.timestamp,
            update_id: self.update_id,
            sequence: self.sequence,
        }
    }

    pub fn buy_with_quote(
        &self,
        quote_quantity: f64,
    ) -> Result<ExecutionEstimate, OrderBookError> {
        validate_amount(quote_quantity)?;

        let best_price = self.best_ask().map(|level| level.price);
        let mut remaining_quote = quote_quantity;
        let mut filled_base = 0.0;
        let mut filled_quote = 0.0;
        let mut worst_price = None;
        let mut levels_consumed = 0;

        for (price, available_base) in &self.asks {
            if remaining_quote <= f64::EPSILON {
                break;
            }

            let price = price.0;
            let level_quote_capacity = price * available_base;
            let quote_taken = remaining_quote.min(level_quote_capacity);
            if quote_taken <= 0.0 {
                continue;
            }

            let base_taken = quote_taken / price;
            filled_base += base_taken;
            filled_quote += quote_taken;
            remaining_quote -= quote_taken;
            worst_price = Some(price);
            levels_consumed += 1;
        }

        Ok(self.build_estimate(
            ExecutionSide::Buy,
            None,
            Some(quote_quantity),
            filled_base,
            filled_quote,
            best_price,
            worst_price,
            levels_consumed,
            remaining_quote <= quote_quantity * 1e-12,
        ))
    }

    pub fn buy_base(
        &self,
        base_quantity: f64,
    ) -> Result<ExecutionEstimate, OrderBookError> {
        validate_amount(base_quantity)?;

        let best_price = self.best_ask().map(|level| level.price);
        let mut remaining_base = base_quantity;
        let mut filled_base = 0.0;
        let mut filled_quote = 0.0;
        let mut worst_price = None;
        let mut levels_consumed = 0;

        for (price, available_base) in &self.asks {
            if remaining_base <= f64::EPSILON {
                break;
            }

            let base_taken = remaining_base.min(*available_base);
            if base_taken <= 0.0 {
                continue;
            }

            let price = price.0;
            filled_base += base_taken;
            filled_quote += base_taken * price;
            remaining_base -= base_taken;
            worst_price = Some(price);
            levels_consumed += 1;
        }

        Ok(self.build_estimate(
            ExecutionSide::Buy,
            Some(base_quantity),
            None,
            filled_base,
            filled_quote,
            best_price,
            worst_price,
            levels_consumed,
            remaining_base <= base_quantity * 1e-12,
        ))
    }

    pub fn sell_base(
        &self,
        base_quantity: f64,
    ) -> Result<ExecutionEstimate, OrderBookError> {
        validate_amount(base_quantity)?;

        let best_price = self.best_bid().map(|level| level.price);
        let mut remaining_base = base_quantity;
        let mut filled_base = 0.0;
        let mut filled_quote = 0.0;
        let mut worst_price = None;
        let mut levels_consumed = 0;

        for (price, available_base) in self.bids.iter().rev() {
            if remaining_base <= f64::EPSILON {
                break;
            }

            let base_taken = remaining_base.min(*available_base);
            if base_taken <= 0.0 {
                continue;
            }

            let price = price.0;
            filled_base += base_taken;
            filled_quote += base_taken * price;
            remaining_base -= base_taken;
            worst_price = Some(price);
            levels_consumed += 1;
        }

        Ok(self.build_estimate(
            ExecutionSide::Sell,
            Some(base_quantity),
            None,
            filled_base,
            filled_quote,
            best_price,
            worst_price,
            levels_consumed,
            remaining_base <= base_quantity * 1e-12,
        ))
    }

    fn build_estimate(
        &self,
        side: ExecutionSide,
        requested_base_quantity: Option<f64>,
        requested_quote_quantity: Option<f64>,
        filled_base_quantity: f64,
        filled_quote_quantity: f64,
        best_price: Option<f64>,
        worst_price: Option<f64>,
        levels_consumed: usize,
        complete: bool,
    ) -> ExecutionEstimate {
        let average_execution_price = if filled_base_quantity > 0.0 {
            Some(filled_quote_quantity / filled_base_quantity)
        } else {
            None
        };

        let slippage_bps = match (side, best_price, average_execution_price) {
            (ExecutionSide::Buy, Some(best), Some(avg)) if best > 0.0 => {
                Some(((avg / best) - 1.0) * 10_000.0)
            }
            (ExecutionSide::Sell, Some(best), Some(avg)) if best > 0.0 => {
                Some((1.0 - (avg / best)) * 10_000.0)
            }
            _ => None,
        };

        ExecutionEstimate {
            symbol: self.symbol.clone(),
            side,
            requested_base_quantity,
            requested_quote_quantity,
            filled_base_quantity,
            filled_quote_quantity,
            average_execution_price,
            best_price,
            worst_price,
            slippage_bps,
            levels_consumed,
            complete,
            timestamp: self.timestamp,
            update_id: self.update_id,
            sequence: self.sequence,
        }
    }
}

#[derive(Debug, Default)]
pub struct OrderBookEngine {
    books: HashMap<String, LocalOrderBook>,
}

impl OrderBookEngine {
    pub fn apply(&mut self, update: BookUpdate) -> Result<(), OrderBookError> {
        let symbol = update.symbol.clone();
        let book = self
            .books
            .entry(symbol.clone())
            .or_insert_with(|| LocalOrderBook::new(symbol));
        book.apply(update)
    }

    pub fn get(&self, symbol: &str) -> Option<&LocalOrderBook> {
        self.books.get(symbol)
    }

    pub fn top_n(&self, symbol: &str, n: usize) -> Result<BookView, OrderBookError> {
        self.get(symbol)
            .map(|book| book.top_n(n))
            .ok_or_else(|| OrderBookError::BookNotFound(symbol.to_string()))
    }

    pub fn buy_with_quote(
        &self,
        symbol: &str,
        quote_quantity: f64,
    ) -> Result<ExecutionEstimate, OrderBookError> {
        self.get(symbol)
            .ok_or_else(|| OrderBookError::BookNotFound(symbol.to_string()))?
            .buy_with_quote(quote_quantity)
    }

    pub fn buy_base(
        &self,
        symbol: &str,
        base_quantity: f64,
    ) -> Result<ExecutionEstimate, OrderBookError> {
        self.get(symbol)
            .ok_or_else(|| OrderBookError::BookNotFound(symbol.to_string()))?
            .buy_base(base_quantity)
    }

    pub fn sell_base(
        &self,
        symbol: &str,
        base_quantity: f64,
    ) -> Result<ExecutionEstimate, OrderBookError> {
        self.get(symbol)
            .ok_or_else(|| OrderBookError::BookNotFound(symbol.to_string()))?
            .sell_base(base_quantity)
    }
}

fn validate_levels(symbol: &str, levels: &[PriceLevel]) -> Result<(), OrderBookError> {
    for level in levels {
        if !level.price.is_finite()
            || !level.quantity.is_finite()
            || level.price <= 0.0
            || level.quantity < 0.0
        {
            return Err(OrderBookError::InvalidLevel {
                symbol: symbol.to_string(),
                price: level.price,
                quantity: level.quantity,
            });
        }
    }
    Ok(())
}

fn validate_amount(amount: f64) -> Result<(), OrderBookError> {
    if amount.is_finite() && amount > 0.0 {
        Ok(())
    } else {
        Err(OrderBookError::InvalidAmount)
    }
}

fn apply_levels(
    side: &mut BTreeMap<OrderedFloat<f64>, f64>,
    levels: &[PriceLevel],
) {
    for level in levels {
        let key = OrderedFloat(level.price);
        if level.quantity == 0.0 {
            side.remove(&key);
        } else {
            side.insert(key, level.quantity);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> BookUpdate {
        BookUpdate {
            symbol: "ETHUSDT".into(),
            bids: vec![
                PriceLevel {
                    price: 2499.50,
                    quantity: 0.10,
                },
                PriceLevel {
                    price: 2499.00,
                    quantity: 1.00,
                },
            ],
            asks: vec![
                PriceLevel {
                    price: 2500.00,
                    quantity: 0.05,
                },
                PriceLevel {
                    price: 2501.00,
                    quantity: 0.20,
                },
                PriceLevel {
                    price: 2502.00,
                    quantity: 1.00,
                },
            ],
            timestamp: 1_790_549_000_000,
            update_id: 10,
            sequence: 100,
            is_snapshot: true,
        }
    }

    #[test]
    fn maintains_best_prices_top_levels_and_metadata() {
        let mut book = LocalOrderBook::new("ETHUSDT");
        book.apply(snapshot()).unwrap();

        let view = book.top_n(2);
        assert_eq!(view.best_bid.unwrap().price, 2499.50);
        assert_eq!(view.best_ask.unwrap().price, 2500.00);
        assert_eq!(view.bids.len(), 2);
        assert_eq!(view.asks.len(), 2);
        assert_eq!(view.timestamp, 1_790_549_000_000);
        assert_eq!(view.update_id, 10);
        assert_eq!(view.sequence, 100);
    }

    #[test]
    fn applies_delta_and_removes_zero_quantity_levels() {
        let mut book = LocalOrderBook::new("ETHUSDT");
        book.apply(snapshot()).unwrap();

        book.apply(BookUpdate {
            symbol: "ETHUSDT".into(),
            bids: vec![PriceLevel {
                price: 2499.50,
                quantity: 0.0,
            }],
            asks: vec![PriceLevel {
                price: 2499.75,
                quantity: 0.50,
            }],
            timestamp: 2,
            update_id: 11,
            sequence: 101,
            is_snapshot: false,
        })
        .unwrap();

        assert_eq!(book.best_bid().unwrap().price, 2499.00);
        assert_eq!(book.best_ask().unwrap().price, 2499.75);
    }

    #[test]
    fn calculates_buy_execution_from_quote_notional() {
        let mut book = LocalOrderBook::new("ETHUSDT");
        book.apply(snapshot()).unwrap();

        let estimate = book.buy_with_quote(400.0).unwrap();

        let first_level_quote = 2500.0 * 0.05;
        let remaining_quote = 400.0 - first_level_quote;
        let expected_base = 0.05 + remaining_quote / 2501.0;
        let expected_average = 400.0 / expected_base;

        assert!(estimate.complete);
        assert_eq!(estimate.levels_consumed, 2);
        assert!((estimate.filled_quote_quantity - 400.0).abs() < 1e-9);
        assert!((estimate.filled_base_quantity - expected_base).abs() < 1e-12);
        assert!((estimate.average_execution_price.unwrap() - expected_average).abs() < 1e-9);
        assert!(estimate.average_execution_price.unwrap() > 2500.0);
        assert_eq!(estimate.best_price, Some(2500.0));
        assert_eq!(estimate.worst_price, Some(2501.0));
    }

    #[test]
    fn calculates_sell_execution_across_bid_depth() {
        let mut book = LocalOrderBook::new("ETHUSDT");
        book.apply(snapshot()).unwrap();

        let estimate = book.sell_base(0.50).unwrap();

        let expected_quote = (0.10 * 2499.50) + (0.40 * 2499.00);
        assert!(estimate.complete);
        assert_eq!(estimate.levels_consumed, 2);
        assert!((estimate.filled_quote_quantity - expected_quote).abs() < 1e-9);
        assert!((estimate.average_execution_price.unwrap() - (expected_quote / 0.50)).abs() < 1e-9);
    }

    #[test]
    fn reports_partial_fill_when_depth_is_insufficient() {
        let mut book = LocalOrderBook::new("ETHUSDT");
        book.apply(snapshot()).unwrap();

        let estimate = book.buy_base(10.0).unwrap();

        assert!(!estimate.complete);
        assert!((estimate.filled_base_quantity - 1.25).abs() < 1e-12);
        assert_eq!(estimate.levels_consumed, 3);
    }

    #[test]
    fn rejects_regressing_sequence() {
        let mut book = LocalOrderBook::new("ETHUSDT");
        book.apply(snapshot()).unwrap();

        let error = book
            .apply(BookUpdate {
                symbol: "ETHUSDT".into(),
                bids: vec![],
                asks: vec![],
                timestamp: 2,
                update_id: 11,
                sequence: 99,
                is_snapshot: false,
            })
            .unwrap_err();

        assert!(matches!(
            error,
            OrderBookError::SequenceRegression { .. }
        ));
    }

    #[test]
    fn fresh_snapshot_can_reset_sequence_and_update_id() {
        let mut book = LocalOrderBook::new("ETHUSDT");
        book.apply(snapshot()).unwrap();

        book.apply(BookUpdate {
            symbol: "ETHUSDT".into(),
            bids: vec![PriceLevel {
                price: 2400.0,
                quantity: 1.0,
            }],
            asks: vec![PriceLevel {
                price: 2401.0,
                quantity: 1.0,
            }],
            timestamp: 3,
            update_id: 1,
            sequence: 1,
            is_snapshot: true,
        })
        .unwrap();

        assert_eq!(book.best_bid().unwrap().price, 2400.0);
        assert_eq!(book.sequence(), 1);
    }
}
