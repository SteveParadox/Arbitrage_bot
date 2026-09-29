use std::{fs, path::Path};

use rust_decimal::{prelude::ToPrimitive, Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use thiserror::Error;

fn bps_denominator() -> Decimal {
    Decimal::new(10_000, 0)
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProfitabilityConfigFile {
    pub version: u32,
    pub fee_profile: String,
    pub fee_bps_per_leg: Vec<String>,
    pub expected_slippage_bps: String,
    pub rounding_loss_bps: String,
    pub latency_buffer_bps: String,
    pub safety_margin_bps: String,
}

#[derive(Debug, Clone)]
pub struct ProfitabilityConfig {
    pub version: u32,
    pub fee_profile: String,
    pub fee_bps_per_leg: Vec<Decimal>,
    pub expected_slippage_bps: Decimal,
    pub rounding_loss_bps: Decimal,
    pub latency_buffer_bps: Decimal,
    pub safety_margin_bps: Decimal,
}

#[derive(Debug, Error)]
pub enum ProfitabilityError {
    #[error("failed to read profitability config: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse profitability config JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid decimal value for {field}: {value}")]
    InvalidDecimal { field: String, value: String },
    #[error("{0}")]
    InvalidConfig(String),
    #[error("{0}")]
    InvalidInput(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProfitabilityResult {
    pub fee_profile: String,
    pub fee_bps_per_leg: Vec<Decimal>,
    pub start_amount: Decimal,
    pub gross_final_amount: Decimal,
    pub gross_profit: Decimal,
    pub gross_return_bps: Decimal,
    pub gross_return_pct: Decimal,
    pub fee_multiplier: Decimal,
    pub nominal_fee_bps: Decimal,
    pub fee_amount: Decimal,
    pub fee_bps_on_start: Decimal,
    pub expected_slippage_bps: Decimal,
    pub expected_slippage_amount: Decimal,
    pub rounding_loss_bps: Decimal,
    pub rounding_loss_amount: Decimal,
    pub latency_buffer_bps: Decimal,
    pub latency_buffer_amount: Decimal,
    pub safety_margin_bps: Decimal,
    pub safety_margin_amount: Decimal,
    pub total_cost_amount: Decimal,
    pub total_cost_bps: Decimal,
    pub expected_net_profit: Decimal,
    pub expected_net_return_bps: Decimal,
    pub expected_net_return_pct: Decimal,
    pub expected_final_amount: Decimal,
    pub net_profitable: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CanonicalProfitabilityResult {
    pub start_amount: String,
    pub gross_final_amount: String,
    pub gross_profit: String,
    pub gross_return_bps: String,
    pub gross_return_pct: String,
    pub fee_multiplier: String,
    pub nominal_fee_bps: String,
    pub fee_amount: String,
    pub fee_bps_on_start: String,
    pub expected_slippage_bps: String,
    pub expected_slippage_amount: String,
    pub rounding_loss_bps: String,
    pub rounding_loss_amount: String,
    pub latency_buffer_bps: String,
    pub latency_buffer_amount: String,
    pub safety_margin_bps: String,
    pub safety_margin_amount: String,
    pub total_cost_amount: String,
    pub total_cost_bps: String,
    pub expected_net_profit: String,
    pub expected_net_return_bps: String,
    pub expected_net_return_pct: String,
    pub expected_final_amount: String,
    pub net_profitable: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ProfitabilityBreakdown {
    pub fee_profile: String,
    pub fee_bps_per_leg: Vec<f64>,
    pub nominal_fee_bps: f64,
    pub fee_amount: f64,
    pub fee_bps_on_start: f64,
    pub expected_slippage_bps: f64,
    pub expected_slippage_amount: f64,
    pub rounding_loss_bps: f64,
    pub rounding_loss_amount: f64,
    pub latency_buffer_bps: f64,
    pub latency_buffer_amount: f64,
    pub safety_margin_bps: f64,
    pub safety_margin_amount: f64,
    pub total_cost_amount: f64,
    pub total_cost_bps: f64,
    pub expected_net_profit: f64,
    pub expected_net_return_bps: f64,
    pub expected_net_return_pct: f64,
    pub expected_final_amount: f64,
    pub net_profitable: bool,
}

impl TryFrom<ProfitabilityConfigFile> for ProfitabilityConfig {
    type Error = ProfitabilityError;

    fn try_from(raw: ProfitabilityConfigFile) -> Result<Self, Self::Error> {
        let config = Self {
            version: raw.version,
            fee_profile: raw.fee_profile,
            fee_bps_per_leg: raw
                .fee_bps_per_leg
                .iter()
                .enumerate()
                .map(|(index, value)| parse_decimal(&format!("fee_bps_per_leg[{index}]"), value))
                .collect::<Result<Vec<_>, _>>()?,
            expected_slippage_bps: parse_decimal(
                "expected_slippage_bps",
                &raw.expected_slippage_bps,
            )?,
            rounding_loss_bps: parse_decimal("rounding_loss_bps", &raw.rounding_loss_bps)?,
            latency_buffer_bps: parse_decimal(
                "latency_buffer_bps",
                &raw.latency_buffer_bps,
            )?,
            safety_margin_bps: parse_decimal("safety_margin_bps", &raw.safety_margin_bps)?,
        };
        config.validate()?;
        Ok(config)
    }
}

impl ProfitabilityConfig {
    pub fn validate(&self) -> Result<(), ProfitabilityError> {
        if self.version != 1 {
            return Err(ProfitabilityError::InvalidConfig(format!(
                "unsupported profitability config version {}",
                self.version
            )));
        }
        if self.fee_profile.trim().is_empty() {
            return Err(ProfitabilityError::InvalidConfig(
                "fee_profile must not be empty".to_string(),
            ));
        }
        if self.fee_bps_per_leg.len() != 3 {
            return Err(ProfitabilityError::InvalidConfig(
                "fee_bps_per_leg must contain exactly three triangle-leg fees".to_string(),
            ));
        }

        for value in self
            .fee_bps_per_leg
            .iter()
            .chain([
                &self.expected_slippage_bps,
                &self.rounding_loss_bps,
                &self.latency_buffer_bps,
                &self.safety_margin_bps,
            ])
        {
            if *value < Decimal::ZERO {
                return Err(ProfitabilityError::InvalidConfig(
                    "profitability cost assumptions must be non-negative".to_string(),
                ));
            }
            if *value >= bps_denominator() {
                return Err(ProfitabilityError::InvalidConfig(
                    "a single profitability cost assumption must be below 10000 bps".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub fn evaluate(
        &self,
        start_amount: Decimal,
        gross_final_amount: Decimal,
    ) -> Result<ProfitabilityResult, ProfitabilityError> {
        self.validate()?;
        if start_amount <= Decimal::ZERO {
            return Err(ProfitabilityError::InvalidInput(
                "start_amount must be greater than zero".to_string(),
            ));
        }
        if gross_final_amount < Decimal::ZERO {
            return Err(ProfitabilityError::InvalidInput(
                "gross_final_amount must not be negative".to_string(),
            ));
        }

        let gross_profit = gross_final_amount - start_amount;
        let gross_return_bps = (gross_profit / start_amount) * bps_denominator();
        let gross_return_pct = gross_return_bps / Decimal::new(100, 0);

        let mut fee_multiplier = Decimal::ONE;
        for fee_bps in &self.fee_bps_per_leg {
            fee_multiplier *= Decimal::ONE - (*fee_bps / bps_denominator());
        }

        let nominal_fee_bps = self
            .fee_bps_per_leg
            .iter()
            .copied()
            .fold(Decimal::ZERO, |acc, value| acc + value);
        let fee_amount = gross_final_amount * (Decimal::ONE - fee_multiplier);
        let fee_bps_on_start = (fee_amount / start_amount) * bps_denominator();

        let expected_slippage_amount =
            start_amount * self.expected_slippage_bps / bps_denominator();
        let rounding_loss_amount = start_amount * self.rounding_loss_bps / bps_denominator();
        let latency_buffer_amount = start_amount * self.latency_buffer_bps / bps_denominator();
        let safety_margin_amount = start_amount * self.safety_margin_bps / bps_denominator();

        let total_cost_amount = fee_amount
            + expected_slippage_amount
            + rounding_loss_amount
            + latency_buffer_amount
            + safety_margin_amount;
        let total_cost_bps = (total_cost_amount / start_amount) * bps_denominator();
        let expected_net_profit = gross_profit - total_cost_amount;
        let expected_net_return_bps =
            (expected_net_profit / start_amount) * bps_denominator();
        let expected_net_return_pct = expected_net_return_bps / Decimal::new(100, 0);
        let expected_final_amount = start_amount + expected_net_profit;

        Ok(ProfitabilityResult {
            fee_profile: self.fee_profile.clone(),
            fee_bps_per_leg: self.fee_bps_per_leg.clone(),
            start_amount,
            gross_final_amount,
            gross_profit,
            gross_return_bps,
            gross_return_pct,
            fee_multiplier,
            nominal_fee_bps,
            fee_amount,
            fee_bps_on_start,
            expected_slippage_bps: self.expected_slippage_bps,
            expected_slippage_amount,
            rounding_loss_bps: self.rounding_loss_bps,
            rounding_loss_amount,
            latency_buffer_bps: self.latency_buffer_bps,
            latency_buffer_amount,
            safety_margin_bps: self.safety_margin_bps,
            safety_margin_amount,
            total_cost_amount,
            total_cost_bps,
            expected_net_profit,
            expected_net_return_bps,
            expected_net_return_pct,
            expected_final_amount,
            net_profitable: expected_net_profit > Decimal::ZERO,
        })
    }

    pub fn evaluate_f64(
        &self,
        start_amount: f64,
        gross_final_amount: f64,
    ) -> Result<ProfitabilityResult, ProfitabilityError> {
        let start = parse_decimal("start_amount", &start_amount.to_string())?;
        let gross = parse_decimal("gross_final_amount", &gross_final_amount.to_string())?;
        self.evaluate(start, gross)
    }
}

impl ProfitabilityResult {
    pub fn canonical(&self) -> CanonicalProfitabilityResult {
        CanonicalProfitabilityResult {
            start_amount: canonical_decimal(self.start_amount),
            gross_final_amount: canonical_decimal(self.gross_final_amount),
            gross_profit: canonical_decimal(self.gross_profit),
            gross_return_bps: canonical_decimal(self.gross_return_bps),
            gross_return_pct: canonical_decimal(self.gross_return_pct),
            fee_multiplier: canonical_decimal(self.fee_multiplier),
            nominal_fee_bps: canonical_decimal(self.nominal_fee_bps),
            fee_amount: canonical_decimal(self.fee_amount),
            fee_bps_on_start: canonical_decimal(self.fee_bps_on_start),
            expected_slippage_bps: canonical_decimal(self.expected_slippage_bps),
            expected_slippage_amount: canonical_decimal(self.expected_slippage_amount),
            rounding_loss_bps: canonical_decimal(self.rounding_loss_bps),
            rounding_loss_amount: canonical_decimal(self.rounding_loss_amount),
            latency_buffer_bps: canonical_decimal(self.latency_buffer_bps),
            latency_buffer_amount: canonical_decimal(self.latency_buffer_amount),
            safety_margin_bps: canonical_decimal(self.safety_margin_bps),
            safety_margin_amount: canonical_decimal(self.safety_margin_amount),
            total_cost_amount: canonical_decimal(self.total_cost_amount),
            total_cost_bps: canonical_decimal(self.total_cost_bps),
            expected_net_profit: canonical_decimal(self.expected_net_profit),
            expected_net_return_bps: canonical_decimal(self.expected_net_return_bps),
            expected_net_return_pct: canonical_decimal(self.expected_net_return_pct),
            expected_final_amount: canonical_decimal(self.expected_final_amount),
            net_profitable: self.net_profitable,
        }
    }

    pub fn breakdown(&self) -> ProfitabilityBreakdown {
        ProfitabilityBreakdown {
            fee_profile: self.fee_profile.clone(),
            fee_bps_per_leg: self
                .fee_bps_per_leg
                .iter()
                .map(|value| decimal_to_f64(*value))
                .collect(),
            nominal_fee_bps: decimal_to_f64(self.nominal_fee_bps),
            fee_amount: decimal_to_f64(self.fee_amount),
            fee_bps_on_start: decimal_to_f64(self.fee_bps_on_start),
            expected_slippage_bps: decimal_to_f64(self.expected_slippage_bps),
            expected_slippage_amount: decimal_to_f64(self.expected_slippage_amount),
            rounding_loss_bps: decimal_to_f64(self.rounding_loss_bps),
            rounding_loss_amount: decimal_to_f64(self.rounding_loss_amount),
            latency_buffer_bps: decimal_to_f64(self.latency_buffer_bps),
            latency_buffer_amount: decimal_to_f64(self.latency_buffer_amount),
            safety_margin_bps: decimal_to_f64(self.safety_margin_bps),
            safety_margin_amount: decimal_to_f64(self.safety_margin_amount),
            total_cost_amount: decimal_to_f64(self.total_cost_amount),
            total_cost_bps: decimal_to_f64(self.total_cost_bps),
            expected_net_profit: decimal_to_f64(self.expected_net_profit),
            expected_net_return_bps: decimal_to_f64(self.expected_net_return_bps),
            expected_net_return_pct: decimal_to_f64(self.expected_net_return_pct),
            expected_final_amount: decimal_to_f64(self.expected_final_amount),
            net_profitable: self.net_profitable,
        }
    }
}

pub fn load_profitability_config(
    path: impl AsRef<Path>,
) -> Result<ProfitabilityConfig, ProfitabilityError> {
    let raw = fs::read_to_string(path)?;
    let file: ProfitabilityConfigFile = serde_json::from_str(&raw)?;
    file.try_into()
}

pub fn parse_decimal(field: &str, value: &str) -> Result<Decimal, ProfitabilityError> {
    Decimal::from_str_exact(value).map_err(|_| ProfitabilityError::InvalidDecimal {
        field: field.to_string(),
        value: value.to_string(),
    })
}

fn canonical_decimal(value: Decimal) -> String {
    let rounded = value.round_dp_with_strategy(8, RoundingStrategy::MidpointAwayFromZero);
    let mut text = rounded.normalize().to_string();
    if let Some(dot) = text.find('.') {
        let decimals = text.len() - dot - 1;
        if decimals < 8 {
            text.push_str(&"0".repeat(8 - decimals));
        }
    } else {
        text.push_str(".00000000");
    }
    text
}

fn decimal_to_f64(value: Decimal) -> f64 {
    value
        .to_f64()
        .expect("validated profitability decimals fit in f64")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference_config() -> ProfitabilityConfig {
        ProfitabilityConfigFile {
            version: 1,
            fee_profile: "bybit_spot_vip0_reference".to_string(),
            fee_bps_per_leg: vec!["10".into(), "10".into(), "10".into()],
            expected_slippage_bps: "5".into(),
            rounding_loss_bps: "0".into(),
            latency_buffer_bps: "3".into(),
            safety_margin_bps: "5".into(),
        }
        .try_into()
        .unwrap()
    }

    #[test]
    fn reference_example_is_about_eighteen_basis_points_net() {
        let result = reference_config()
            .evaluate(
                Decimal::from_str_exact("450").unwrap(),
                Decimal::from_str_exact("452.745").unwrap(),
            )
            .unwrap();

        assert_eq!(result.canonical().gross_return_pct, "0.61000000");
        assert_eq!(
            result.canonical().expected_net_return_pct,
            "0.17847173"
        );
        assert!(result.net_profitable);
    }
}
