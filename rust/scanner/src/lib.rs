mod engine;
mod profitability;
mod recorder;

pub use engine::{ArbitrageScanRecord, ArbitrageScanner, LegScan, ScanStatus, ScannerSettings};
pub use profitability::{
    load_profitability_config, parse_decimal, CanonicalProfitabilityResult, ProfitabilityBreakdown,
    ProfitabilityConfig, ProfitabilityConfigFile, ProfitabilityError, ProfitabilityResult,
};
pub use recorder::NdjsonRecorder;

use std::{
    collections::{BTreeSet, HashSet},
    fs,
    path::Path,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub fn gross_edge(buy_price: f64, sell_price: f64) -> f64 {
    sell_price - buy_price
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TriangleLeg {
    pub symbol: String,
    pub from_asset: String,
    pub to_asset: String,
    pub side: TradeSide,
    pub base_asset: String,
    pub quote_asset: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TriangleRoute {
    pub id: String,
    pub triangle_id: String,
    pub start_asset: String,
    pub assets: Vec<String>,
    pub pair1: String,
    pub pair2: String,
    pub pair3: String,
    pub legs: Vec<TriangleLeg>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TriangleSource {
    pub endpoint: String,
    pub category: String,
    pub status: String,
    pub testnet: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TriangleConfig {
    pub version: u32,
    pub exchange: String,
    pub market: String,
    pub generated_at: Option<String>,
    pub source: TriangleSource,
    pub start_assets: Vec<String>,
    pub instrument_count: usize,
    pub triangle_count: usize,
    pub route_count: usize,
    pub routes: Vec<TriangleRoute>,
}

#[derive(Debug, Error)]
pub enum TriangleConfigError {
    #[error("failed to read triangle config: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse triangle config: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported triangle config version {0}")]
    UnsupportedVersion(u32),
    #[error("triangle config route_count does not match routes length")]
    RouteCountMismatch,
    #[error("triangle config triangle_count does not match represented unique triangles")]
    TriangleCountMismatch,
    #[error("duplicate triangle route id: {0}")]
    DuplicateRoute(String),
    #[error("invalid triangle route {route_id}: {reason}")]
    InvalidRoute { route_id: String, reason: String },
}

impl TriangleConfig {
    pub fn validate(&self) -> Result<(), TriangleConfigError> {
        if self.exchange != "bybit"
            || self.market != "spot"
            || self.source.category != "spot"
            || self.source.status != "Trading"
        {
            return Err(TriangleConfigError::InvalidRoute {
                route_id: "configuration".into(),
                reason: "only active Bybit spot routes are supported".into(),
            });
        }
        if self.version != 1 {
            return Err(TriangleConfigError::UnsupportedVersion(self.version));
        }
        if self.route_count != self.routes.len() {
            return Err(TriangleConfigError::RouteCountMismatch);
        }

        let represented_triangles = self
            .routes
            .iter()
            .map(|route| route.triangle_id.as_str())
            .collect::<HashSet<_>>();
        if self.triangle_count != represented_triangles.len() {
            return Err(TriangleConfigError::TriangleCountMismatch);
        }

        let mut route_ids = HashSet::new();
        for route in &self.routes {
            if !route_ids.insert(route.id.clone()) {
                return Err(TriangleConfigError::DuplicateRoute(route.id.clone()));
            }
            validate_route(route)?;
        }

        Ok(())
    }

    pub fn required_symbols(&self) -> BTreeSet<String> {
        self.routes
            .iter()
            .flat_map(|route| route.legs.iter().map(|leg| leg.symbol.clone()))
            .collect()
    }
}

pub fn load_triangle_config(path: impl AsRef<Path>) -> Result<TriangleConfig, TriangleConfigError> {
    let raw = fs::read_to_string(path)?;
    let config: TriangleConfig = serde_json::from_str(&raw)?;
    config.validate()?;
    Ok(config)
}

fn validate_route(route: &TriangleRoute) -> Result<(), TriangleConfigError> {
    let invalid = |reason: &str| TriangleConfigError::InvalidRoute {
        route_id: route.id.clone(),
        reason: reason.to_string(),
    };

    if route.assets.len() != 4 {
        return Err(invalid(
            "assets must contain exactly four entries including the return asset",
        ));
    }
    if route.legs.len() != 3 {
        return Err(invalid("route must contain exactly three legs"));
    }
    if route.assets[0] != route.assets[3] {
        return Err(invalid("route must return to its starting asset"));
    }
    if route.start_asset != route.assets[0] {
        return Err(invalid("start_asset must equal assets[0]"));
    }

    let unique_assets = route.assets[..3].iter().collect::<HashSet<_>>();
    if unique_assets.len() != 3 {
        return Err(invalid("triangle must contain three distinct assets"));
    }
    let mut canonical_assets = route.assets[..3].to_vec();
    canonical_assets.sort();
    if route.id != route.assets.join(">") || route.triangle_id != canonical_assets.join("-") {
        return Err(invalid("route and triangle identifiers must match assets"));
    }

    let expected_pairs = [&route.pair1, &route.pair2, &route.pair3];
    for (index, leg) in route.legs.iter().enumerate() {
        if leg.symbol.as_str() != expected_pairs[index].as_str() {
            return Err(invalid("pair fields must match leg symbols"));
        }
        if leg.from_asset != route.assets[index] || leg.to_asset != route.assets[index + 1] {
            return Err(invalid("leg assets are not continuous with route assets"));
        }

        match leg.side {
            TradeSide::Sell => {
                if leg.base_asset != leg.from_asset || leg.quote_asset != leg.to_asset {
                    return Err(invalid("SELL leg must convert base_asset into quote_asset"));
                }
            }
            TradeSide::Buy => {
                if leg.quote_asset != leg.from_asset || leg.base_asset != leg.to_asset {
                    return Err(invalid(
                        "BUY leg must spend quote_asset to acquire base_asset",
                    ));
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_wrong_market_and_forged_route_identity() {
        let mut config: TriangleConfig = serde_json::from_str(valid_config_json()).unwrap();
        config.market = "linear".into();
        assert!(config.validate().is_err());
        config.market = "spot".into();
        config.routes[0].id = "another-route".into();
        assert!(config.validate().is_err());
    }

    fn valid_config_json() -> &'static str {
        r#"{
          "version": 1,
          "exchange": "bybit",
          "market": "spot",
          "generated_at": "2026-09-29T14:00:00+00:00",
          "source": {
            "endpoint": "https://api.bybit.com/v5/market/instruments-info",
            "category": "spot",
            "status": "Trading",
            "testnet": false
          },
          "start_assets": ["USDT"],
          "instrument_count": 3,
          "triangle_count": 1,
          "route_count": 1,
          "routes": [{
            "id": "USDT>BTC>ETH>USDT",
            "triangle_id": "BTC-ETH-USDT",
            "start_asset": "USDT",
            "assets": ["USDT", "BTC", "ETH", "USDT"],
            "pair1": "BTCUSDT",
            "pair2": "ETHBTC",
            "pair3": "ETHUSDT",
            "legs": [
              {
                "symbol": "BTCUSDT",
                "from_asset": "USDT",
                "to_asset": "BTC",
                "side": "BUY",
                "base_asset": "BTC",
                "quote_asset": "USDT"
              },
              {
                "symbol": "ETHBTC",
                "from_asset": "BTC",
                "to_asset": "ETH",
                "side": "BUY",
                "base_asset": "ETH",
                "quote_asset": "BTC"
              },
              {
                "symbol": "ETHUSDT",
                "from_asset": "ETH",
                "to_asset": "USDT",
                "side": "SELL",
                "base_asset": "ETH",
                "quote_asset": "USDT"
              }
            ]
          }]
        }"#
    }

    #[test]
    fn validates_structural_buy_sell_route() {
        let config: TriangleConfig = serde_json::from_str(valid_config_json()).unwrap();
        config.validate().unwrap();

        let route = &config.routes[0];
        assert_eq!(route.legs[0].side, TradeSide::Buy);
        assert_eq!(route.legs[2].side, TradeSide::Sell);
    }

    #[test]
    fn returns_required_market_symbols() {
        let config: TriangleConfig = serde_json::from_str(valid_config_json()).unwrap();
        let symbols = config.required_symbols();

        assert_eq!(
            symbols,
            BTreeSet::from([
                "BTCUSDT".to_string(),
                "ETHBTC".to_string(),
                "ETHUSDT".to_string()
            ])
        );
    }

    #[test]
    fn rejects_incorrect_side_semantics() {
        let raw = valid_config_json().replace(
            "\"side\": \"BUY\",\n                \"base_asset\": \"BTC\"",
            "\"side\": \"SELL\",\n                \"base_asset\": \"BTC\"",
        );
        let config: TriangleConfig = serde_json::from_str(&raw).unwrap();

        assert!(matches!(
            config.validate(),
            Err(TriangleConfigError::InvalidRoute { .. })
        ));
    }
}
