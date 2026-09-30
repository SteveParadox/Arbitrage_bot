use std::{env, time::Duration};

use rust_decimal::Decimal;

use crate::ExecutionError;

#[derive(Clone)]
pub struct ExecutionConfig {
    pub testnet: bool,
    pub live_trading_enabled: bool,
    api_key: String,
    api_secret: String,
    pub recv_window_ms: u64,
    pub request_timeout: Duration,
    pub max_retries: u32,
    pub retry_base_delay: Duration,
    pub poll_interval: Duration,
    pub order_timeout: Duration,
    pub cancel_confirmation_timeout: Duration,
    pub fill_confirmation_timeout: Duration,
    pub max_order_notional: Decimal,
    pub max_execution_pages: usize,
    pub account_type: String,
    pub cancel_on_timeout: bool,
}

impl ExecutionConfig {
    pub fn from_env() -> Result<Self, ExecutionError> {
        let config = Self {
            testnet: parse_bool("BYBIT_EXECUTION_TESTNET", parse_bool("BYBIT_TESTNET", true)?)?,
            live_trading_enabled: parse_bool("ARB_LIVE_TRADING_ENABLED", false)?,
            api_key: env::var("BYBIT_API_KEY").unwrap_or_default(),
            api_secret: env::var("BYBIT_API_SECRET").unwrap_or_default(),
            recv_window_ms: parse_u64("BYBIT_EXECUTION_RECV_WINDOW_MS", 5_000)?,
            request_timeout: Duration::from_millis(parse_u64(
                "BYBIT_EXECUTION_REQUEST_TIMEOUT_MS",
                3_000,
            )?),
            max_retries: parse_u32("BYBIT_EXECUTION_MAX_RETRIES", 2)?,
            retry_base_delay: Duration::from_millis(parse_u64(
                "BYBIT_EXECUTION_RETRY_BASE_MS",
                100,
            )?),
            poll_interval: Duration::from_millis(parse_u64(
                "BYBIT_EXECUTION_POLL_INTERVAL_MS",
                50,
            )?),
            order_timeout: Duration::from_millis(parse_u64(
                "BYBIT_EXECUTION_ORDER_TIMEOUT_MS",
                3_000,
            )?),
            cancel_confirmation_timeout: Duration::from_millis(parse_u64(
                "BYBIT_EXECUTION_CANCEL_TIMEOUT_MS",
                2_000,
            )?),
            fill_confirmation_timeout: Duration::from_millis(parse_u64(
                "BYBIT_EXECUTION_FILL_CONFIRM_TIMEOUT_MS",
                2_000,
            )?),
            max_order_notional: parse_decimal(
                "BYBIT_EXECUTION_MAX_ORDER_NOTIONAL",
                "5",
            )?,
            max_execution_pages: parse_usize("BYBIT_EXECUTION_MAX_EXECUTION_PAGES", 20)?,
            account_type: env::var("BYBIT_EXECUTION_ACCOUNT_TYPE")
                .unwrap_or_else(|_| "UNIFIED".to_string()),
            cancel_on_timeout: parse_bool("BYBIT_EXECUTION_CANCEL_ON_TIMEOUT", true)?,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ExecutionError> {
        if self.api_key.trim().is_empty() || self.api_secret.trim().is_empty() {
            return Err(ExecutionError::InvalidConfig(
                "BYBIT_API_KEY and BYBIT_API_SECRET are required for private execution".to_string(),
            ));
        }
        if self.recv_window_ms == 0
            || self.request_timeout.is_zero()
            || self.retry_base_delay.is_zero()
            || self.poll_interval.is_zero()
            || self.order_timeout.is_zero()
            || self.cancel_confirmation_timeout.is_zero()
            || self.fill_confirmation_timeout.is_zero()
        {
            return Err(ExecutionError::InvalidConfig(
                "execution timing values must be greater than zero".to_string(),
            ));
        }
        if self.max_order_notional <= Decimal::ZERO {
            return Err(ExecutionError::InvalidConfig(
                "BYBIT_EXECUTION_MAX_ORDER_NOTIONAL must be positive".to_string(),
            ));
        }
        if self.max_execution_pages == 0 {
            return Err(ExecutionError::InvalidConfig(
                "max execution pages must be greater than zero".to_string(),
            ));
        }
        if self.account_type != "UNIFIED" {
            return Err(ExecutionError::InvalidConfig(
                "Phase 10 currently supports BYBIT_EXECUTION_ACCOUNT_TYPE=UNIFIED only".to_string(),
            ));
        }
        if !self.testnet && !self.live_trading_enabled {
            return Err(ExecutionError::InvalidConfig(
                "mainnet private execution requires ARB_LIVE_TRADING_ENABLED=true".to_string(),
            ));
        }
        Ok(())
    }

    pub fn base_url(&self) -> &'static str {
        if self.testnet {
            "https://api-testnet.bybit.com"
        } else {
            "https://api.bybit.com"
        }
    }

    pub(crate) fn api_key(&self) -> &str {
        &self.api_key
    }

    pub(crate) fn api_secret(&self) -> &str {
        &self.api_secret
    }

    pub fn safe_summary(&self) -> serde_json::Value {
        serde_json::json!({
            "testnet": self.testnet,
            "live_trading_enabled": self.live_trading_enabled,
            "api_key_configured": !self.api_key.is_empty(),
            "api_secret_configured": !self.api_secret.is_empty(),
            "recv_window_ms": self.recv_window_ms,
            "request_timeout_ms": self.request_timeout.as_millis(),
            "max_retries": self.max_retries,
            "poll_interval_ms": self.poll_interval.as_millis(),
            "order_timeout_ms": self.order_timeout.as_millis(),
            "fill_confirmation_timeout_ms": self.fill_confirmation_timeout.as_millis(),
            "max_order_notional": self.max_order_notional.to_string(),
            "account_type": &self.account_type,
            "cancel_on_timeout": self.cancel_on_timeout
        })
    }
}

fn parse_bool(name: &str, default: bool) -> Result<bool, ExecutionError> {
    match env::var(name) {
        Ok(value) => value.parse::<bool>().map_err(|_| {
            ExecutionError::InvalidConfig(format!("{name} must be true or false"))
        }),
        Err(_) => Ok(default),
    }
}

fn parse_u64(name: &str, default: u64) -> Result<u64, ExecutionError> {
    env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<u64>()
        .map_err(|_| ExecutionError::InvalidConfig(format!("{name} must be an unsigned integer")))
}

fn parse_u32(name: &str, default: u32) -> Result<u32, ExecutionError> {
    env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<u32>()
        .map_err(|_| ExecutionError::InvalidConfig(format!("{name} must be an unsigned integer")))
}

fn parse_usize(name: &str, default: usize) -> Result<usize, ExecutionError> {
    env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<usize>()
        .map_err(|_| ExecutionError::InvalidConfig(format!("{name} must be a positive integer")))
}

fn parse_decimal(name: &str, default: &str) -> Result<Decimal, ExecutionError> {
    let value = env::var(name).unwrap_or_else(|_| default.to_string());
    Decimal::from_str_exact(&value)
        .map_err(|_| ExecutionError::InvalidConfig(format!("{name} must be a decimal number")))
}
