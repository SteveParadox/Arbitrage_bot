use std::{fs, path::Path};

use serde::Deserialize;

use crate::ShadowError;

#[derive(Debug, Clone)]
pub struct ShadowConfig {
    pub version: u32,
    pub base_asset: String,
    pub latency_ms: Vec<u64>,
    pub minimum_observations: usize,
    pub max_pending_observations: usize,
    pub account_refresh_ms: u64,
    pub sample_tick_ms: u64,
    pub history_retention_ms: u64,
}

#[derive(Debug, Deserialize)]
struct ShadowConfigFile {
    version: u32,
    base_asset: String,
    latency_ms: Vec<u64>,
    minimum_observations: usize,
    max_pending_observations: usize,
    account_refresh_ms: u64,
    sample_tick_ms: u64,
    history_retention_ms: u64,
}

pub fn load_shadow_config(
    path: impl AsRef<Path>,
) -> Result<ShadowConfig, ShadowError> {
    let raw = fs::read_to_string(path)?;
    let file: ShadowConfigFile = serde_json::from_str(&raw)?;
    let mut latency_ms = file.latency_ms;
    latency_ms.sort_unstable();
    latency_ms.dedup();

    let config = ShadowConfig {
        version: file.version,
        base_asset: file.base_asset.to_uppercase(),
        latency_ms,
        minimum_observations: file.minimum_observations,
        max_pending_observations: file.max_pending_observations,
        account_refresh_ms: file.account_refresh_ms,
        sample_tick_ms: file.sample_tick_ms,
        history_retention_ms: file.history_retention_ms,
    };
    config.validate()?;
    Ok(config)
}

impl ShadowConfig {
    pub fn validate(&self) -> Result<(), ShadowError> {
        if self.version != 1 {
            return Err(ShadowError::InvalidConfig(format!(
                "unsupported shadow config version {}",
                self.version
            )));
        }
        if self.base_asset != "USDT" {
            return Err(ShadowError::InvalidConfig(
                concat!(
                    "Phase 12 currently requires base_asset=USDT so account ",
                    "USD exposure and route notional use compatible units"
                )
                .to_string(),
            ));
        }
        if self.latency_ms.is_empty() || self.latency_ms.contains(&0) {
            return Err(ShadowError::InvalidConfig(
                "latency_ms must contain positive delays".to_string(),
            ));
        }
        if self.minimum_observations < 3_000 {
            return Err(ShadowError::InvalidConfig(
                "minimum_observations must be at least 3000".to_string(),
            ));
        }
        if self.max_pending_observations < self.minimum_observations {
            return Err(ShadowError::InvalidConfig(
                "max_pending_observations must be >= minimum_observations".to_string(),
            ));
        }
        if self.account_refresh_ms == 0 || self.sample_tick_ms == 0 {
            return Err(ShadowError::InvalidConfig(
                "account_refresh_ms and sample_tick_ms must be positive".to_string(),
            ));
        }
        let max_latency = *self.latency_ms.iter().max().expect("not empty");
        if self.history_retention_ms < max_latency.saturating_add(self.sample_tick_ms * 4) {
            return Err(ShadowError::InvalidConfig(
                "history_retention_ms is too short for configured latency samples".to_string(),
            ));
        }
        Ok(())
    }
}
