use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use orderbook::{BookUpdate, ExecutionEstimate, OrderBookEngine, OrderBookError};
use serde::{Deserialize, Serialize};

use crate::{
    ProfitabilityBreakdown, ProfitabilityConfig, TradeSide, TriangleConfig, TriangleRoute,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScannerSettings {
    pub version: u32,
    pub start_amounts: HashMap<String, f64>,
    pub record_path: String,
    pub profitability_config_path: String,
    #[serde(default = "default_max_book_age_ms")]
    pub max_book_age_ms: u64,
    #[serde(default = "default_max_book_skew_ms")]
    pub max_book_skew_ms: u64,
}

fn default_max_book_age_ms() -> u64 {
    1_000
}
fn default_max_book_skew_ms() -> u64 {
    100
}

impl ScannerSettings {
    pub fn validate(&self) -> Result<(), String> {
        if self.max_book_age_ms == 0 || self.max_book_skew_ms == 0 {
            return Err("book age and skew limits must be positive".into());
        }
        if self.version != 1 {
            return Err(format!(
                "unsupported scanner settings version {}",
                self.version
            ));
        }
        if self.start_amounts.is_empty() {
            return Err("scanner start_amounts must not be empty".to_string());
        }
        for (asset, amount) in &self.start_amounts {
            if asset.trim().is_empty() {
                return Err("scanner start asset must not be empty".to_string());
            }
            if !amount.is_finite() || *amount <= 0.0 {
                return Err(format!(
                    "scanner start amount for {asset} must be positive and finite"
                ));
            }
        }
        if self.record_path.trim().is_empty() {
            return Err("scanner record_path must not be empty".to_string());
        }
        if self.profitability_config_path.trim().is_empty() {
            return Err("scanner profitability_config_path must not be empty".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScanStatus {
    Complete,
    StaleBook,
    BookTimestampSkew,
    MissingBook,
    InsufficientLiquidity,
    StartAmountNotConfigured,
    CalculationError,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct LegScan {
    pub leg_index: usize,
    pub symbol: String,
    pub side: TradeSide,
    pub input_asset: String,
    pub output_asset: String,
    pub input_amount: f64,
    pub output_amount: f64,
    pub complete: bool,
    pub execution: ExecutionEstimate,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ArbitrageScanRecord {
    pub scan_timestamp: u64,
    pub trigger_symbol: String,
    pub trigger_timestamp: u64,
    pub trigger_update_id: u64,
    pub trigger_sequence: u64,
    pub route_id: String,
    pub triangle_id: String,
    pub start_asset: String,
    pub start_amount: Option<f64>,
    pub final_amount: Option<f64>,
    pub gross_profit: Option<f64>,
    pub gross_return_pct: Option<f64>,
    pub gross_return_bps: Option<f64>,
    pub gross_profitable: Option<bool>,
    pub expected_net_profit: Option<f64>,
    pub expected_net_return_pct: Option<f64>,
    pub expected_net_return_bps: Option<f64>,
    pub expected_final_amount: Option<f64>,
    pub net_profitable: Option<bool>,
    pub profitability: Option<ProfitabilityBreakdown>,
    pub status: ScanStatus,
    pub reason: Option<String>,
    pub oldest_book_timestamp: Option<u64>,
    pub newest_book_timestamp: Option<u64>,
    pub book_timestamp_skew_ms: Option<u64>,
    pub legs: Vec<LegScan>,
    pub fees_included: bool,
    pub execution_enabled: bool,
}

#[derive(Debug)]
pub struct ArbitrageScanner {
    routes: Vec<TriangleRoute>,
    route_indexes_by_symbol: HashMap<String, Vec<usize>>,
    books: OrderBookEngine,
    start_amounts: HashMap<String, f64>,
    profitability: ProfitabilityConfig,
    max_book_age_ms: u64,
    max_book_skew_ms: u64,
}

impl ArbitrageScanner {
    pub fn new(
        config: TriangleConfig,
        settings: ScannerSettings,
        profitability: ProfitabilityConfig,
    ) -> Result<Self, String> {
        config.validate().map_err(|error| error.to_string())?;
        settings.validate()?;
        profitability
            .validate()
            .map_err(|error| error.to_string())?;

        let mut route_indexes_by_symbol: HashMap<String, Vec<usize>> = HashMap::new();
        for (route_index, route) in config.routes.iter().enumerate() {
            for leg in &route.legs {
                route_indexes_by_symbol
                    .entry(leg.symbol.clone())
                    .or_default()
                    .push(route_index);
            }
        }

        for indexes in route_indexes_by_symbol.values_mut() {
            indexes.sort_unstable();
            indexes.dedup();
        }

        Ok(Self {
            routes: config.routes,
            route_indexes_by_symbol,
            books: OrderBookEngine::default(),
            start_amounts: settings
                .start_amounts
                .into_iter()
                .map(|(asset, amount)| (asset.to_uppercase(), amount))
                .collect(),
            profitability,
            max_book_age_ms: settings.max_book_age_ms,
            max_book_skew_ms: settings.max_book_skew_ms,
        })
    }

    pub fn reset_books(&mut self) {
        self.books = OrderBookEngine::default();
    }

    pub fn reload_routes(&mut self, config: TriangleConfig) -> Result<(), String> {
        config.validate().map_err(|error| error.to_string())?;

        let mut route_indexes_by_symbol: HashMap<String, Vec<usize>> =
            HashMap::new();
        for (route_index, route) in config.routes.iter().enumerate() {
            for leg in &route.legs {
                route_indexes_by_symbol
                    .entry(leg.symbol.clone())
                    .or_default()
                    .push(route_index);
            }
        }
        for indexes in route_indexes_by_symbol.values_mut() {
            indexes.sort_unstable();
            indexes.dedup();
        }

        self.routes = config.routes;
        self.route_indexes_by_symbol = route_indexes_by_symbol;
        self.reset_books();
        Ok(())
    }

    pub fn affected_route_count(&self, symbol: &str) -> usize {
        self.route_indexes_by_symbol.get(symbol).map_or(0, Vec::len)
    }

    pub fn on_book_update(
        &mut self,
        update: BookUpdate,
    ) -> Result<Vec<ArbitrageScanRecord>, OrderBookError> {
        let trigger_symbol = update.symbol.clone();
        let trigger_timestamp = update.timestamp;
        let trigger_update_id = update.update_id;
        let trigger_sequence = update.sequence;

        let Some(route_indexes) = self.route_indexes_by_symbol.get(&trigger_symbol) else {
            return Ok(Vec::new());
        };

        if let Err(error) = self.books.apply(update) {
            self.reset_books();
            return Err(error);
        }

        let mut records = Vec::with_capacity(route_indexes.len());
        for &route_index in route_indexes {
            records.push(self.scan_route(
                &self.routes[route_index],
                &trigger_symbol,
                trigger_timestamp,
                trigger_update_id,
                trigger_sequence,
            ));
        }

        Ok(records)
    }

    fn scan_route(
        &self,
        route: &TriangleRoute,
        trigger_symbol: &str,
        trigger_timestamp: u64,
        trigger_update_id: u64,
        trigger_sequence: u64,
    ) -> ArbitrageScanRecord {
        let scan_timestamp = now_ms();
        let Some(start_amount) = self.start_amounts.get(&route.start_asset).copied() else {
            return base_record(
                route,
                scan_timestamp,
                trigger_symbol,
                trigger_timestamp,
                trigger_update_id,
                trigger_sequence,
                None,
                ScanStatus::StartAmountNotConfigured,
                Some(format!(
                    "no configured scanner start amount for {}",
                    route.start_asset
                )),
            );
        };

        let mut amount = start_amount;
        let mut leg_scans = Vec::with_capacity(3);
        let mut book_timestamps = Vec::with_capacity(3);

        for (index, leg) in route.legs.iter().enumerate() {
            let estimate = match leg.side {
                TradeSide::Buy => self.books.buy_with_quote(&leg.symbol, amount),
                TradeSide::Sell => self.books.sell_base(&leg.symbol, amount),
            };

            let estimate = match estimate {
                Ok(value) => value,
                Err(OrderBookError::BookNotFound(_)) => {
                    let mut record = base_record(
                        route,
                        scan_timestamp,
                        trigger_symbol,
                        trigger_timestamp,
                        trigger_update_id,
                        trigger_sequence,
                        Some(start_amount),
                        ScanStatus::MissingBook,
                        Some(format!("order book not initialized for {}", leg.symbol)),
                    );
                    record.legs = leg_scans;
                    apply_book_timestamp_stats(&mut record, &book_timestamps);
                    return record;
                }
                Err(error) => {
                    let mut record = base_record(
                        route,
                        scan_timestamp,
                        trigger_symbol,
                        trigger_timestamp,
                        trigger_update_id,
                        trigger_sequence,
                        Some(start_amount),
                        ScanStatus::CalculationError,
                        Some(error.to_string()),
                    );
                    record.legs = leg_scans;
                    apply_book_timestamp_stats(&mut record, &book_timestamps);
                    return record;
                }
            };

            let output_amount = match leg.side {
                TradeSide::Buy => estimate.filled_base_quantity,
                TradeSide::Sell => estimate.filled_quote_quantity,
            };
            book_timestamps.push(estimate.timestamp);

            let stale = estimate.timestamp > scan_timestamp
                || scan_timestamp.saturating_sub(estimate.timestamp) > self.max_book_age_ms;
            let skew = book_timestamps.iter().max().unwrap_or(&0)
                - book_timestamps.iter().min().unwrap_or(&0);
            if stale || skew > self.max_book_skew_ms {
                let mut record = base_record(
                    route,
                    scan_timestamp,
                    trigger_symbol,
                    trigger_timestamp,
                    trigger_update_id,
                    trigger_sequence,
                    Some(start_amount),
                    if stale {
                        ScanStatus::StaleBook
                    } else {
                        ScanStatus::BookTimestampSkew
                    },
                    Some(format!("unusable book timestamps on {}", leg.symbol)),
                );
                record.legs = leg_scans;
                apply_book_timestamp_stats(&mut record, &book_timestamps);
                return record;
            }

            let complete = estimate.complete;
            leg_scans.push(LegScan {
                leg_index: index + 1,
                symbol: leg.symbol.clone(),
                side: leg.side,
                input_asset: leg.from_asset.clone(),
                output_asset: leg.to_asset.clone(),
                input_amount: amount,
                output_amount,
                complete,
                execution: estimate,
            });

            if !complete {
                let mut record = base_record(
                    route,
                    scan_timestamp,
                    trigger_symbol,
                    trigger_timestamp,
                    trigger_update_id,
                    trigger_sequence,
                    Some(start_amount),
                    ScanStatus::InsufficientLiquidity,
                    Some(format!("insufficient visible depth on {}", leg.symbol)),
                );
                record.legs = leg_scans;
                apply_book_timestamp_stats(&mut record, &book_timestamps);
                return record;
            }

            amount = output_amount;
        }

        let gross_profit = amount - start_amount;
        let gross_return_pct = (gross_profit / start_amount) * 100.0;
        let gross_return_bps = (gross_profit / start_amount) * 10_000.0;

        let profitability = match self.profitability.evaluate_f64(start_amount, amount) {
            Ok(result) => result,
            Err(error) => {
                let mut record = base_record(
                    route,
                    scan_timestamp,
                    trigger_symbol,
                    trigger_timestamp,
                    trigger_update_id,
                    trigger_sequence,
                    Some(start_amount),
                    ScanStatus::CalculationError,
                    Some(format!("profitability calculation failed: {error}")),
                );
                record.final_amount = Some(amount);
                record.gross_profit = Some(gross_profit);
                record.gross_return_pct = Some(gross_return_pct);
                record.gross_return_bps = Some(gross_return_bps);
                record.gross_profitable = Some(gross_profit > 0.0);
                record.legs = leg_scans;
                apply_book_timestamp_stats(&mut record, &book_timestamps);
                return record;
            }
        };

        let breakdown = profitability.breakdown();

        let mut record = base_record(
            route,
            scan_timestamp,
            trigger_symbol,
            trigger_timestamp,
            trigger_update_id,
            trigger_sequence,
            Some(start_amount),
            ScanStatus::Complete,
            None,
        );
        record.final_amount = Some(amount);
        record.gross_profit = Some(gross_profit);
        record.gross_return_pct = Some(gross_return_pct);
        record.gross_return_bps = Some(gross_return_bps);
        record.gross_profitable = Some(gross_profit > 0.0);
        record.expected_net_profit = Some(breakdown.expected_net_profit);
        record.expected_net_return_pct = Some(breakdown.expected_net_return_pct);
        record.expected_net_return_bps = Some(breakdown.expected_net_return_bps);
        record.expected_final_amount = Some(breakdown.expected_final_amount);
        record.net_profitable = Some(breakdown.net_profitable);
        record.fees_included = true;
        record.profitability = Some(breakdown);
        record.legs = leg_scans;
        apply_book_timestamp_stats(&mut record, &book_timestamps);
        record
    }
}

#[allow(clippy::too_many_arguments)]
fn base_record(
    route: &TriangleRoute,
    scan_timestamp: u64,
    trigger_symbol: &str,
    trigger_timestamp: u64,
    trigger_update_id: u64,
    trigger_sequence: u64,
    start_amount: Option<f64>,
    status: ScanStatus,
    reason: Option<String>,
) -> ArbitrageScanRecord {
    ArbitrageScanRecord {
        scan_timestamp,
        trigger_symbol: trigger_symbol.to_string(),
        trigger_timestamp,
        trigger_update_id,
        trigger_sequence,
        route_id: route.id.clone(),
        triangle_id: route.triangle_id.clone(),
        start_asset: route.start_asset.clone(),
        start_amount,
        final_amount: None,
        gross_profit: None,
        gross_return_pct: None,
        gross_return_bps: None,
        gross_profitable: None,
        expected_net_profit: None,
        expected_net_return_pct: None,
        expected_net_return_bps: None,
        expected_final_amount: None,
        net_profitable: None,
        profitability: None,
        status,
        reason,
        oldest_book_timestamp: None,
        newest_book_timestamp: None,
        book_timestamp_skew_ms: None,
        legs: Vec::new(),
        fees_included: false,
        execution_enabled: false,
    }
}

fn apply_book_timestamp_stats(record: &mut ArbitrageScanRecord, timestamps: &[u64]) {
    if timestamps.is_empty() {
        return;
    }

    let oldest = *timestamps.iter().min().expect("timestamps is not empty");
    let newest = *timestamps.iter().max().expect("timestamps is not empty");
    record.oldest_book_timestamp = Some(oldest);
    record.newest_book_timestamp = Some(newest);
    record.book_timestamp_skew_ms = Some(newest.saturating_sub(oldest));
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use orderbook::{BookUpdate, PriceLevel};

    use super::*;
    use crate::{ProfitabilityConfigFile, TriangleLeg, TriangleSource};

    fn config() -> TriangleConfig {
        TriangleConfig {
            version: 1,
            exchange: "bybit".into(),
            market: "spot".into(),
            generated_at: None,
            source: TriangleSource {
                endpoint: "test".into(),
                category: "spot".into(),
                status: "Trading".into(),
                testnet: true,
            },
            start_assets: vec!["USDT".into()],
            instrument_count: 3,
            triangle_count: 1,
            route_count: 1,
            routes: vec![TriangleRoute {
                id: "USDT>BTC>ETH>USDT".into(),
                triangle_id: "BTC-ETH-USDT".into(),
                start_asset: "USDT".into(),
                assets: vec!["USDT".into(), "BTC".into(), "ETH".into(), "USDT".into()],
                pair1: "BTCUSDT".into(),
                pair2: "ETHBTC".into(),
                pair3: "ETHUSDT".into(),
                legs: vec![
                    TriangleLeg {
                        symbol: "BTCUSDT".into(),
                        from_asset: "USDT".into(),
                        to_asset: "BTC".into(),
                        side: TradeSide::Buy,
                        base_asset: "BTC".into(),
                        quote_asset: "USDT".into(),
                    },
                    TriangleLeg {
                        symbol: "ETHBTC".into(),
                        from_asset: "BTC".into(),
                        to_asset: "ETH".into(),
                        side: TradeSide::Buy,
                        base_asset: "ETH".into(),
                        quote_asset: "BTC".into(),
                    },
                    TriangleLeg {
                        symbol: "ETHUSDT".into(),
                        from_asset: "ETH".into(),
                        to_asset: "USDT".into(),
                        side: TradeSide::Sell,
                        base_asset: "ETH".into(),
                        quote_asset: "USDT".into(),
                    },
                ],
            }],
        }
    }

    fn profitability() -> ProfitabilityConfig {
        ProfitabilityConfigFile {
            version: 1,
            fee_profile: "bybit_spot_vip0_reference".into(),
            fee_bps_per_leg: vec!["10".into(), "10".into(), "10".into()],
            expected_slippage_bps: "5".into(),
            rounding_loss_bps: "0".into(),
            latency_buffer_bps: "3".into(),
            safety_margin_bps: "5".into(),
        }
        .try_into()
        .unwrap()
    }

    fn scanner() -> ArbitrageScanner {
        ArbitrageScanner::new(
            config(),
            ScannerSettings {
                version: 1,
                start_amounts: HashMap::from([("USDT".to_string(), 450.0)]),
                record_path: "ignored.ndjson".into(),
                profitability_config_path: "ignored.json".into(),
                max_book_age_ms: 1_000,
                max_book_skew_ms: 100,
            },
            profitability(),
        )
        .unwrap()
    }

    fn snapshot(
        symbol: &str,
        bid: f64,
        bid_qty: f64,
        ask: f64,
        ask_qty: f64,
        timestamp: u64,
    ) -> BookUpdate {
        BookUpdate {
            symbol: symbol.into(),
            bids: vec![PriceLevel {
                price: bid,
                quantity: bid_qty,
            }],
            asks: vec![PriceLevel {
                price: ask,
                quantity: ask_qty,
            }],
            timestamp: now_ms().saturating_sub(100) + timestamp,
            update_id: 1,
            sequence: timestamp,
            is_snapshot: true,
        }
    }

    #[test]
    fn indexes_only_affected_routes() {
        let scanner = scanner();
        assert_eq!(scanner.affected_route_count("BTCUSDT"), 1);
        assert_eq!(scanner.affected_route_count("SOLUSDT"), 0);
    }

    #[test]
    fn stale_or_skewed_books_never_complete() {
        for stale in [true, false] {
            let mut scanner = scanner();
            let mut old = snapshot("BTCUSDT", 99., 10., 100., 10., 1);
            old.timestamp = now_ms() - if stale { 2_000 } else { 500 };
            scanner.on_book_update(old).unwrap();
            scanner
                .on_book_update(snapshot("ETHBTC", 0.049, 100., 0.05, 100., 2))
                .unwrap();
            let scans = scanner
                .on_book_update(snapshot("ETHUSDT", 21., 100., 22., 100., 3))
                .unwrap();
            assert_eq!(
                scans[0].status,
                if stale {
                    ScanStatus::StaleBook
                } else {
                    ScanStatus::BookTimestampSkew
                }
            );
            assert!(scans[0].expected_net_profit.is_none());
        }
    }

    #[test]
    fn reset_requires_all_three_new_snapshots() {
        let mut scanner = scanner();
        scanner
            .on_book_update(snapshot("BTCUSDT", 99., 10., 100., 10., 1))
            .unwrap();
        scanner
            .on_book_update(snapshot("ETHBTC", 0.049, 100., 0.05, 100., 2))
            .unwrap();
        scanner.reset_books();
        let scans = scanner
            .on_book_update(snapshot("ETHUSDT", 21., 100., 22., 100., 3))
            .unwrap();
        assert_eq!(scans[0].status, ScanStatus::MissingBook);
    }

    #[test]
    fn route_reload_resets_books_before_scanning_resumes() {
        let mut scanner = scanner();
        scanner
            .on_book_update(snapshot("BTCUSDT", 99.0, 10.0, 100.0, 10.0, 1))
            .unwrap();
        scanner
            .on_book_update(snapshot("ETHBTC", 0.049, 100.0, 0.05, 100.0, 2))
            .unwrap();

        scanner.reload_routes(config()).unwrap();

        let records = scanner
            .on_book_update(snapshot("ETHUSDT", 21.0, 100.0, 22.0, 100.0, 3))
            .unwrap();
        assert_eq!(records[0].status, ScanStatus::MissingBook);
    }

    #[test]
    fn records_missing_books_until_route_is_fully_initialized() {
        let mut scanner = scanner();
        let records = scanner
            .on_book_update(snapshot("BTCUSDT", 99.0, 10.0, 100.0, 10.0, 1))
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, ScanStatus::MissingBook);
        assert!(records[0].reason.as_deref().unwrap().contains("ETHBTC"));
        assert!(!records[0].fees_included);
    }

    #[test]
    fn calculates_net_profit_after_cost_model() {
        let mut scanner = scanner();
        scanner
            .on_book_update(snapshot("BTCUSDT", 99.0, 10.0, 100.0, 10.0, 1))
            .unwrap();
        scanner
            .on_book_update(snapshot("ETHBTC", 0.049, 100.0, 0.05, 100.0, 2))
            .unwrap();
        let records = scanner
            .on_book_update(snapshot("ETHUSDT", 21.0, 100.0, 22.0, 100.0, 3))
            .unwrap();

        let record = &records[0];
        assert_eq!(record.status, ScanStatus::Complete);
        assert_eq!(record.start_amount, Some(450.0));
        assert!((record.final_amount.unwrap() - 1890.0).abs() < 1e-9);
        assert!((record.gross_profit.unwrap() - 1440.0).abs() < 1e-9);
        assert!(record.expected_net_profit.unwrap() < record.gross_profit.unwrap());
        assert_eq!(record.legs.len(), 3);
        assert!(record.fees_included);
        assert!(record.profitability.is_some());
        assert!(!record.execution_enabled);
    }

    #[test]
    fn records_insufficient_depth_without_net_profitability() {
        let mut scanner = scanner();
        scanner
            .on_book_update(snapshot("BTCUSDT", 99.0, 1.0, 100.0, 1.0, 1))
            .unwrap();
        scanner
            .on_book_update(snapshot("ETHBTC", 0.049, 100.0, 0.05, 100.0, 2))
            .unwrap();
        scanner
            .on_book_update(snapshot("ETHUSDT", 21.0, 100.0, 22.0, 100.0, 3))
            .unwrap();

        let records = scanner
            .on_book_update(snapshot("BTCUSDT", 99.0, 1.0, 100.0, 1.0, 4))
            .unwrap();

        assert_eq!(records[0].status, ScanStatus::InsufficientLiquidity);
        assert_eq!(records[0].legs.len(), 1);
        assert!(!records[0].legs[0].complete);
        assert!(records[0].expected_net_profit.is_none());
        assert!(!records[0].fees_included);
    }
}
