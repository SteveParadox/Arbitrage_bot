use orderbook::OrderBookEngine;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::Instant;

/// A socket alone is not readiness. All acknowledgements and required books must be usable.
pub(crate) fn snapshot(
    required: &[String],
    pending: &HashSet<String>,
    books: &OrderBookEngine,
    receipts: &HashMap<String, Instant>,
    stale_after: Duration,
    now_ms: u64,
) -> Value {
    let symbols: serde_json::Map<String, Value> = required.iter().map(|symbol| {
        let book = books.get(symbol);
        let initialized = book.is_some_and(|book| book.update_id() > 0);
        let synchronized = book.is_some_and(|book| match (book.best_bid(), book.best_ask()) {
            (Some(bid), Some(ask)) => bid.price < ask.price,
            _ => false,
        });
        let exchange_timestamp_ms = book.map(|book| book.timestamp());
        let receive_age_ms = receipts.get(symbol).map(|received| received.elapsed().as_millis() as u64);
        (symbol.clone(), json!({"initialized": initialized, "synchronized": synchronized,
            "exchange_timestamp_ms": exchange_timestamp_ms, "receive_age_ms": receive_age_ms,
            "update_id": book.map(|book| book.update_id()), "sequence": book.map(|book| book.sequence())}))
    }).collect();
    let initialized = !required.is_empty()
        && symbols
            .values()
            .all(|v| v["initialized"] == true && v["synchronized"] == true);
    let fresh = symbols.values().all(|v| {
        let timestamp = v["exchange_timestamp_ms"].as_u64();
        let receipt = v["receive_age_ms"].as_u64();
        timestamp.is_some_and(|ts| ts <= now_ms && now_ms - ts <= stale_after.as_millis() as u64)
            && receipt.is_some_and(|age| age <= stale_after.as_millis() as u64)
    });
    let state = if !pending.is_empty() || !initialized {
        "resynchronizing"
    } else if !fresh {
        "connected_but_stale"
    } else {
        "connected_and_fresh"
    };
    json!({"state": state, "timestamp": now_ms, "required_symbols": required,
        "subscriptions_confirmed": pending.is_empty(), "pending_subscription_batches": pending.len(),
        "symbols": symbols})
}

#[cfg(test)]
mod tests {
    use super::*;
    use orderbook::{BookUpdate, PriceLevel};
    #[test]
    fn readiness_requires_ack_coverage_synchronization_and_both_clocks() {
        let required = vec!["BTCUSDT".into()];
        let mut pending = HashSet::from(["market-data-0".into()]);
        let mut books = OrderBookEngine::default();
        let receipts = HashMap::from([("BTCUSDT".into(), Instant::now())]);
        let status = |p: &HashSet<String>, b: &OrderBookEngine, now| {
            snapshot(&required, p, b, &receipts, Duration::from_secs(10), now)
        };
        assert_eq!(status(&pending, &books, 1000)["state"], "resynchronizing");
        books
            .apply(BookUpdate {
                symbol: "BTCUSDT".into(),
                bids: vec![PriceLevel {
                    price: 99.,
                    quantity: 1.,
                }],
                asks: vec![PriceLevel {
                    price: 100.,
                    quantity: 1.,
                }],
                timestamp: 1000,
                update_id: 10,
                sequence: 10,
                is_snapshot: true,
            })
            .unwrap();
        assert_eq!(status(&pending, &books, 1000)["state"], "resynchronizing");
        pending.clear();
        assert_eq!(
            status(&pending, &books, 1000)["state"],
            "connected_and_fresh"
        );
        assert_eq!(
            status(&pending, &books, 11001)["state"],
            "connected_but_stale"
        );
        assert_eq!(
            status(&pending, &books, 999)["state"],
            "connected_but_stale"
        );
    }
}
