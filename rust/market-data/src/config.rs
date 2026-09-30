use std::{env, time::Duration};

use anyhow::{bail, Context, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    Spot,
    Linear,
    Inverse,
}

impl Category {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Linear => "linear",
            Self::Inverse => "inverse",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub testnet: bool,
    pub category: Category,
    pub symbols: Vec<String>,
    pub orderbook_depth: u16,
    pub subscribe_trades: bool,
    pub subscribe_tickers: bool,
    pub heartbeat_interval: Duration,
    pub stale_after: Duration,
    pub reconnect_min: Duration,
    pub reconnect_max: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let testnet = parse_bool("BYBIT_TESTNET", true)?;
        let category = match env::var("BYBIT_MARKET_CATEGORY")
            .unwrap_or_else(|_| "linear".to_string())
            .to_lowercase()
            .as_str()
        {
            "spot" => Category::Spot,
            "linear" => Category::Linear,
            "inverse" => Category::Inverse,
            other => bail!("unsupported BYBIT_MARKET_CATEGORY={other}; use spot, linear, or inverse"),
        };

        let symbols = env::var("BYBIT_MARKET_SYMBOLS")
            .unwrap_or_else(|_| "BTCUSDT".to_string())
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_uppercase())
            .collect::<Vec<_>>();
        if symbols.is_empty() {
            bail!("BYBIT_MARKET_SYMBOLS must contain at least one symbol");
        }

        let orderbook_depth = parse_u16("BYBIT_ORDERBOOK_DEPTH", 50)?;
        let allowed_depths: &[u16] = match category {
            Category::Spot | Category::Linear | Category::Inverse => &[1, 50, 200, 1000],
        };
        if !allowed_depths.contains(&orderbook_depth) {
            bail!("unsupported BYBIT_ORDERBOOK_DEPTH={orderbook_depth}; use 1, 50, 200, or 1000");
        }

        let subscribe_trades = parse_bool("BYBIT_SUBSCRIBE_TRADES", true)?;
        let subscribe_tickers = parse_bool("BYBIT_SUBSCRIBE_TICKERS", true)?;
        let heartbeat_seconds = parse_u64("BYBIT_HEARTBEAT_SECONDS", 20)?;
        let stale_seconds = parse_u64("BYBIT_STALE_AFTER_SECONDS", 10)?;
        let reconnect_min_ms = parse_u64("BYBIT_RECONNECT_MIN_MS", 500)?;
        let reconnect_max_seconds = parse_u64("BYBIT_RECONNECT_MAX_SECONDS", 30)?;

        if heartbeat_seconds == 0 {
            bail!("BYBIT_HEARTBEAT_SECONDS must be greater than zero");
        }
        if stale_seconds == 0 {
            bail!("BYBIT_STALE_AFTER_SECONDS must be greater than zero");
        }
        if reconnect_min_ms == 0 || reconnect_max_seconds == 0 {
            bail!("reconnect intervals must be greater than zero");
        }

        let reconnect_min = Duration::from_millis(reconnect_min_ms);
        let reconnect_max = Duration::from_secs(reconnect_max_seconds);
        if reconnect_min > reconnect_max {
            bail!("BYBIT_RECONNECT_MIN_MS must not exceed BYBIT_RECONNECT_MAX_SECONDS");
        }

        Ok(Self {
            testnet,
            category,
            symbols,
            orderbook_depth,
            subscribe_trades,
            subscribe_tickers,
            heartbeat_interval: Duration::from_secs(heartbeat_seconds),
            stale_after: Duration::from_secs(stale_seconds),
            reconnect_min,
            reconnect_max,
        })
    }

    pub fn websocket_url(&self) -> String {
        let host = if self.testnet {
            "stream-testnet.bybit.com"
        } else {
            "stream.bybit.com"
        };
        format!("wss://{host}/v5/public/{}", self.category.as_str())
    }

    pub fn rest_base_url(&self) -> &'static str {
        if self.testnet {
            "https://api-testnet.bybit.com"
        } else {
            "https://api.bybit.com"
        }
    }
}

fn parse_bool(name: &str, default: bool) -> Result<bool> {
    match env::var(name) {
        Ok(value) => value
            .parse::<bool>()
            .with_context(|| format!("{name} must be true or false")),
        Err(_) => Ok(default),
    }
}

fn parse_u16(name: &str, default: u16) -> Result<u16> {
    env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<u16>()
        .with_context(|| format!("{name} must be an unsigned integer"))
}

fn parse_u64(name: &str, default: u64) -> Result<u64> {
    env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<u64>()
        .with_context(|| format!("{name} must be an unsigned integer"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_expected_testnet_url() {
        let config = Config {
            testnet: true,
            category: Category::Linear,
            symbols: vec!["BTCUSDT".into()],
            orderbook_depth: 50,
            subscribe_trades: true,
            subscribe_tickers: true,
            heartbeat_interval: Duration::from_secs(20),
            stale_after: Duration::from_secs(10),
            reconnect_min: Duration::from_millis(500),
            reconnect_max: Duration::from_secs(30),
        };
        assert_eq!(
            config.websocket_url(),
            "wss://stream-testnet.bybit.com/v5/public/linear"
        );
    }
}
