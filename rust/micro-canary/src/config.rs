use std::{env, fs, path::Path};

use rust_decimal::Decimal;
use serde::Deserialize;

use crate::MicroCanaryError;

pub const ABSOLUTE_MAX_CYCLE_NOTIONAL: Decimal = Decimal::new(25, 0);

#[derive(Debug, Clone)]
pub struct MicroCanaryConfig {
    pub version: u32,
    pub base_asset: String,
    pub cycle_notional: Decimal,
    pub max_candidates_per_session: usize,
    pub cooldown_ms: u64,
    pub require_shadow_ready_ack: bool,
}

#[derive(Debug, Deserialize)]
struct MicroCanaryConfigFile {
    version: u32,
    base_asset: String,
    cycle_notional: String,
    max_candidates_per_session: usize,
    cooldown_ms: u64,
    require_shadow_ready_ack: bool,
}

pub fn load_micro_canary_config(
    path: impl AsRef<Path>,
) -> Result<MicroCanaryConfig, MicroCanaryError> {
    let raw = fs::read_to_string(path)?;
    let file: MicroCanaryConfigFile = serde_json::from_str(&raw)?;
    let cycle_notional = Decimal::from_str_exact(&file.cycle_notional)
        .map_err(|_| {
            MicroCanaryError::InvalidConfig(
                "cycle_notional must be a decimal string".to_string(),
            )
        })?;
    let config = MicroCanaryConfig {
        version: file.version,
        base_asset: file.base_asset.to_uppercase(),
        cycle_notional,
        max_candidates_per_session: file.max_candidates_per_session,
        cooldown_ms: file.cooldown_ms,
        require_shadow_ready_ack: file.require_shadow_ready_ack,
    };
    config.validate()?;
    Ok(config)
}

impl MicroCanaryConfig {
    pub fn validate(&self) -> Result<(), MicroCanaryError> {
        if self.version != 1 {
            return Err(MicroCanaryError::InvalidConfig(
                "unsupported micro-canary config version".to_string(),
            ));
        }
        if self.base_asset != "USDT" {
            return Err(MicroCanaryError::InvalidConfig(
                "Phase 13 currently requires base_asset=USDT".to_string(),
            ));
        }
        if self.cycle_notional <= Decimal::ZERO
            || self.cycle_notional > ABSOLUTE_MAX_CYCLE_NOTIONAL
        {
            return Err(MicroCanaryError::InvalidConfig(
                "cycle_notional must be positive and <= 25 USDT".to_string(),
            ));
        }
        if self.max_candidates_per_session == 0 || self.cooldown_ms == 0 {
            return Err(MicroCanaryError::InvalidConfig(
                "candidate limit and cooldown must be positive".to_string(),
            ));
        }
        Ok(())
    }

    pub fn validate_environment(&self) -> Result<(), MicroCanaryError> {
        if env::var("ARB_LIVE_TRADING_ENABLED")
            .unwrap_or_else(|_| "false".to_string())
            .parse::<bool>()
            .unwrap_or(false)
        {
            return Err(MicroCanaryError::InvalidConfig(
                "micro-canary refuses to start while live trading is enabled"
                    .to_string(),
            ));
        }
        if self.require_shadow_ready_ack
            && !env::var("ARB_MICRO_CANARY_SHADOW_READY")
                .unwrap_or_else(|_| "false".to_string())
                .parse::<bool>()
                .unwrap_or(false)
        {
            return Err(MicroCanaryError::InvalidConfig(
                "set ARB_MICRO_CANARY_SHADOW_READY=true only after reviewing Phase 12 results"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_full_account_sized_candidate() {
        let config = MicroCanaryConfig {
            version: 1,
            base_asset: "USDT".into(),
            cycle_notional: Decimal::from(500),
            max_candidates_per_session: 10,
            cooldown_ms: 1000,
            require_shadow_ready_ack: true,
        };
        assert!(config.validate().is_err());
    }
}
