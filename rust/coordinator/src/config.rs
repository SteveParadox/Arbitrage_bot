use std::{
    fs,
    path::Path,
};

use rust_decimal::Decimal;
use serde::Deserialize;

use crate::CoordinatorError;

#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    pub version: u32,
    pub normal_slippage_tolerance_percent: Decimal,
    pub emergency_slippage_tolerance_percent: Decimal,
    pub max_unwind_attempts_per_asset: usize,
    pub max_dust_notional_base: Decimal,
}

#[derive(Debug, Deserialize)]
struct CoordinatorConfigFile {
    version: u32,
    normal_slippage_tolerance_percent: String,
    emergency_slippage_tolerance_percent: String,
    max_unwind_attempts_per_asset: usize,
    max_dust_notional_base: String,
}

pub fn load_coordinator_config(
    path: impl AsRef<Path>,
) -> Result<CoordinatorConfig, CoordinatorError> {
    let raw = fs::read_to_string(path)?;
    let file: CoordinatorConfigFile = serde_json::from_str(&raw)?;
    let config = CoordinatorConfig {
        version: file.version,
        normal_slippage_tolerance_percent: parse_decimal(
            "normal_slippage_tolerance_percent",
            &file.normal_slippage_tolerance_percent,
        )?,
        emergency_slippage_tolerance_percent: parse_decimal(
            "emergency_slippage_tolerance_percent",
            &file.emergency_slippage_tolerance_percent,
        )?,
        max_unwind_attempts_per_asset: file.max_unwind_attempts_per_asset,
        max_dust_notional_base: parse_decimal(
            "max_dust_notional_base",
            &file.max_dust_notional_base,
        )?,
    };
    config.validate()?;
    Ok(config)
}

impl CoordinatorConfig {
    pub fn validate(&self) -> Result<(), CoordinatorError> {
        if self.version != 1 {
            return Err(CoordinatorError::InvalidConfig(format!(
                "unsupported coordinator config version {}",
                self.version
            )));
        }
        if self.normal_slippage_tolerance_percent <= Decimal::ZERO
            || self.normal_slippage_tolerance_percent > Decimal::new(10, 0)
            || self.emergency_slippage_tolerance_percent
                < self.normal_slippage_tolerance_percent
            || self.emergency_slippage_tolerance_percent > Decimal::new(10, 0)
        {
            return Err(CoordinatorError::InvalidConfig(
                "slippage tolerances must be positive, <=10%, and emergency >= normal"
                    .to_string(),
            ));
        }
        if self.max_unwind_attempts_per_asset == 0 {
            return Err(CoordinatorError::InvalidConfig(
                "max_unwind_attempts_per_asset must be positive".to_string(),
            ));
        }
        if self.max_dust_notional_base < Decimal::ZERO {
            return Err(CoordinatorError::InvalidConfig(
                "max_dust_notional_base must be non-negative".to_string(),
            ));
        }
        Ok(())
    }
}

fn parse_decimal(field: &str, value: &str) -> Result<Decimal, CoordinatorError> {
    Decimal::from_str_exact(value).map_err(|_| {
        CoordinatorError::InvalidConfig(format!("{field} must be a decimal string"))
    })
}
