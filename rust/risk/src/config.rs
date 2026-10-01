use std::{
    fs,
    path::{Path, PathBuf},
};

use rust_decimal::Decimal;
use serde::Deserialize;

use crate::RiskError;

#[derive(Debug, Clone)]
pub struct RiskConfig {
    pub version: u32,
    pub max_market_data_age_ms: u64,
    pub min_net_edge_bps: Decimal,
    pub max_slippage_bps: Decimal,
    pub min_liquidity_ratio: Decimal,
    pub max_trade_size: Decimal,
    pub max_total_exposure: Decimal,
    pub max_daily_loss: Decimal,
    pub execution_failure_limit: usize,
    pub execution_failure_window_ms: u64,
    pub api_health_max_age_ms: u64,
    pub exchange_health_max_age_ms: u64,
    pub approval_ttl_ms: u64,
    pub emergency_max_market_data_age_ms: u64,
    pub kill_switch_file: PathBuf,
    pub state_file: PathBuf,
    pub trading_control_file: PathBuf,
}

#[derive(Debug, Deserialize)]
struct RiskConfigFile {
    version: u32,
    max_market_data_age_ms: u64,
    min_net_edge_bps: String,
    max_slippage_bps: String,
    min_liquidity_ratio: String,
    max_trade_size: String,
    max_total_exposure: String,
    max_daily_loss: String,
    execution_failure_limit: usize,
    execution_failure_window_ms: u64,
    api_health_max_age_ms: u64,
    exchange_health_max_age_ms: u64,
    approval_ttl_ms: u64,
    emergency_max_market_data_age_ms: u64,
    kill_switch_file: String,
    state_file: String,
    trading_control_file: String,
}

pub fn load_risk_config(path: impl AsRef<Path>) -> Result<RiskConfig, RiskError> {
    let path = path.as_ref();
    let raw = fs::read_to_string(path)?;
    let file: RiskConfigFile = serde_json::from_str(&raw)?;

    let config = RiskConfig {
        version: file.version,
        max_market_data_age_ms: file.max_market_data_age_ms,
        min_net_edge_bps: parse_decimal("min_net_edge_bps", &file.min_net_edge_bps)?,
        max_slippage_bps: parse_decimal("max_slippage_bps", &file.max_slippage_bps)?,
        min_liquidity_ratio: parse_decimal(
            "min_liquidity_ratio",
            &file.min_liquidity_ratio,
        )?,
        max_trade_size: parse_decimal("max_trade_size", &file.max_trade_size)?,
        max_total_exposure: parse_decimal(
            "max_total_exposure",
            &file.max_total_exposure,
        )?,
        max_daily_loss: parse_decimal("max_daily_loss", &file.max_daily_loss)?,
        execution_failure_limit: file.execution_failure_limit,
        execution_failure_window_ms: file.execution_failure_window_ms,
        api_health_max_age_ms: file.api_health_max_age_ms,
        exchange_health_max_age_ms: file.exchange_health_max_age_ms,
        approval_ttl_ms: file.approval_ttl_ms,
        emergency_max_market_data_age_ms: file.emergency_max_market_data_age_ms,
        kill_switch_file: resolve_path(path, &file.kill_switch_file),
        state_file: resolve_path(path, &file.state_file),
        trading_control_file: resolve_path(path, &file.trading_control_file),
    };
    config.validate()?;
    Ok(config)
}

impl RiskConfig {
    pub fn validate(&self) -> Result<(), RiskError> {
        if self.version != 1 {
            return Err(RiskError::InvalidConfig(format!(
                "unsupported risk config version {}",
                self.version
            )));
        }
        if self.max_market_data_age_ms == 0
            || self.execution_failure_window_ms == 0
            || self.api_health_max_age_ms == 0
            || self.exchange_health_max_age_ms == 0
            || self.approval_ttl_ms == 0
            || self.emergency_max_market_data_age_ms == 0
        {
            return Err(RiskError::InvalidConfig(
                "risk timing limits must be greater than zero".to_string(),
            ));
        }
        if self.emergency_max_market_data_age_ms < self.max_market_data_age_ms {
            return Err(RiskError::InvalidConfig(
                "emergency_max_market_data_age_ms must be >= normal freshness limit".to_string(),
            ));
        }
        if self.execution_failure_limit == 0 {
            return Err(RiskError::InvalidConfig(
                "execution_failure_limit must be greater than zero".to_string(),
            ));
        }
        if self.min_net_edge_bps < Decimal::ZERO
            || self.max_slippage_bps < Decimal::ZERO
        {
            return Err(RiskError::InvalidConfig(
                "edge and slippage limits must be non-negative".to_string(),
            ));
        }
        if self.min_liquidity_ratio <= Decimal::ZERO
            || self.min_liquidity_ratio > Decimal::ONE
        {
            return Err(RiskError::InvalidConfig(
                "min_liquidity_ratio must be in (0, 1]".to_string(),
            ));
        }
        if self.max_trade_size <= Decimal::ZERO
            || self.max_total_exposure <= Decimal::ZERO
            || self.max_daily_loss <= Decimal::ZERO
        {
            return Err(RiskError::InvalidConfig(
                "trade, exposure, and daily-loss limits must be positive".to_string(),
            ));
        }
        if self.max_total_exposure < self.max_trade_size {
            return Err(RiskError::InvalidConfig(
                "max_total_exposure must be at least max_trade_size".to_string(),
            ));
        }
        if self.kill_switch_file.as_os_str().is_empty()
            || self.state_file.as_os_str().is_empty()
            || self.trading_control_file.as_os_str().is_empty()
        {
            return Err(RiskError::InvalidConfig(
                "risk state paths must not be empty".to_string(),
            ));
        }
        Ok(())
    }
}

fn parse_decimal(field: &str, value: &str) -> Result<Decimal, RiskError> {
    Decimal::from_str_exact(value).map_err(|_| RiskError::InvalidDecimal {
        field: field.to_string(),
        value: value.to_string(),
    })
}

fn resolve_path(config_path: &Path, configured: &str) -> PathBuf {
    let path = PathBuf::from(configured);
    if path.is_absolute() {
        return path;
    }
    config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(path)
}
