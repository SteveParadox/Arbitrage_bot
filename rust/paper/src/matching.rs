use anyhow::{bail, Result};
use execution::OrderSide;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub fn decimal(value: f64) -> Result<Decimal> {
    if !value.is_finite() || value < 0.0 {
        bail!("invalid market numeric value");
    }
    Ok(Decimal::from_str_exact(&value.to_string())?)
}
pub fn floor(value: Decimal, step: Decimal) -> Decimal {
    value - value % step
}

#[derive(Clone, Debug)]
pub struct Instrument {
    pub base: String,
    pub quote: String,
    pub step: Decimal,
    pub quote_step: Decimal,
    pub tick: Decimal,
    pub min_amount: Decimal,
    pub max_quantity: Decimal,
    pub max_limit_quantity: Option<Decimal>,
    pub timestamp: u64,
}
#[derive(Clone, Debug)]
pub struct Book {
    pub bids: Vec<(Decimal, Decimal)>,
    pub asks: Vec<(Decimal, Decimal)>,
    pub timestamp: u64,
    pub version: String,
}
#[derive(Clone)]
pub struct MatchSettings {
    pub fee_rate: Decimal,
    pub latency_ms: u64,
    pub max_age_ms: u64,
    pub timeout_ms: u64,
    pub slippage_percent: Decimal,
}
impl MatchSettings {
    pub fn validate(&self) -> Result<()> {
        if self.fee_rate < Decimal::ZERO
            || self.fee_rate >= Decimal::ONE
            || self.max_age_ms == 0
            || self.timeout_ms == 0
            || self.timeout_ms > 5000
            || self.latency_ms > 5000
            || self.slippage_percent < Decimal::ZERO
        {
            bail!("invalid paper matching configuration");
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FillResult {
    pub requested: Decimal,
    pub quantity: Decimal,
    pub quote_value: Decimal,
    pub fee: Decimal,
    pub fee_currency: String,
    pub price: Option<Decimal>,
    pub remaining: Decimal,
    pub status: String,
    pub reason: Option<String>,
    pub executed_at_ms: u64,
    pub book_version: String,
    #[serde(default, skip_serializing_if = "decimal_zero")]
    pub quote_rounding_dust: Decimal,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_reference_price: Option<Decimal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_slippage_bps: Option<Decimal>,
}

pub fn match_order(
    book: &Book,
    instrument: &Instrument,
    side: OrderSide,
    quantity: Decimal,
    budget: Decimal,
    settings: &MatchSettings,
    now: u64,
) -> Result<FillResult> {
    settings.validate()?;
    if quantity <= Decimal::ZERO
        || budget < Decimal::ZERO
        || instrument.step <= Decimal::ZERO
        || instrument.quote_step <= Decimal::ZERO
        || instrument.tick <= Decimal::ZERO
        || quantity % instrument.step != Decimal::ZERO
        || quantity > instrument.max_quantity
    {
        bail!("invalid quantity/precision or market quantity cap");
    }
    let budget = if side == OrderSide::Buy {
        floor(budget, instrument.quote_step)
    } else {
        budget
    };
    let mut result = FillResult {
        requested: quantity,
        quantity: Decimal::ZERO,
        quote_value: Decimal::ZERO,
        fee: Decimal::ZERO,
        fee_currency: if side == OrderSide::Buy {
            instrument.base.clone()
        } else {
            instrument.quote.clone()
        },
        price: None,
        remaining: quantity,
        status: "Rejected".into(),
        reason: None,
        executed_at_ms: now,
        book_version: book.version.clone(),
        quote_rounding_dust: Decimal::ZERO,
        submission_reference_price: None,
        execution_slippage_bps: None,
    };
    if now
        .checked_sub(book.timestamp)
        .is_none_or(|age| age > settings.max_age_ms)
        || now
            .checked_sub(instrument.timestamp)
            .is_none_or(|age| age > 300_000)
    {
        result.reason = Some("STALE_MARKET_OR_METADATA".into());
        return Ok(result);
    }
    let levels = if side == OrderSide::Buy {
        &book.asks
    } else {
        &book.bids
    };
    let Some((best, _)) = levels.first() else {
        result.reason = Some("NO_LIQUIDITY".into());
        return Ok(result);
    };
    let tolerance = settings.slippage_percent / Decimal::from(100);
    for (price, available) in levels {
        if *price <= Decimal::ZERO
            || *available < Decimal::ZERO
            || *price % instrument.tick != Decimal::ZERO
        {
            bail!("invalid book precision");
        }
        if (side == OrderSide::Buy && *price > *best * (Decimal::ONE + tolerance))
            || (side == OrderSide::Sell && *price < *best * (Decimal::ONE - tolerance))
        {
            break;
        }
        let remaining_budget = if side == OrderSide::Buy {
            floor(
                (budget - result.quote_value).max(Decimal::ZERO) / *price,
                instrument.step,
            )
        } else {
            (budget - result.quantity).max(Decimal::ZERO)
        };
        let take = floor(
            (quantity - result.quantity)
                .min(*available)
                .min(remaining_budget),
            instrument.step,
        );
        result.quantity += take;
        result.quote_value += take * *price;
        if result.quantity == quantity {
            break;
        }
    }
    let raw_quote = result.quote_value;
    result.price = if result.quantity > Decimal::ZERO {
        Some(raw_quote / result.quantity)
    } else {
        None
    };
    result.quote_value = floor(raw_quote, instrument.quote_step);
    if side == OrderSide::Buy && result.quote_value < raw_quote {
        result.quote_value += instrument.quote_step;
    }
    result.quote_rounding_dust = (result.quote_value - raw_quote).abs();
    if result.quote_value < instrument.min_amount || result.quantity == Decimal::ZERO {
        result.quantity = Decimal::ZERO;
        result.quote_value = Decimal::ZERO;
        result.price = None;
        result.quote_rounding_dust = Decimal::ZERO;
        result.reason = Some("MINIMUM_AMOUNT_OR_LIQUIDITY".into());
        return Ok(result);
    }
    let gross_output = if side == OrderSide::Buy {
        result.quantity
    } else {
        result.quote_value
    };
    // Fee is rounded up to output precision. Output dust remains explicit in cycle holdings.
    let fee_step = if side == OrderSide::Buy {
        instrument.step
    } else {
        instrument.quote_step
    };
    let fee = gross_output * settings.fee_rate;
    result.fee = if fee % fee_step == Decimal::ZERO {
        fee
    } else {
        floor(fee, fee_step) + fee_step
    };
    if result.fee > gross_output {
        bail!("fee exceeds received asset");
    }
    result.remaining = quantity - result.quantity;
    result.status = if result.remaining == Decimal::ZERO {
        "Filled"
    } else {
        "PartiallyFilledCanceled"
    }
    .into();
    if result.remaining > Decimal::ZERO {
        result.reason = Some("IOC_UNFILLED_REMAINDER".into());
    }
    Ok(result)
}

fn decimal_zero(value: &Decimal) -> bool {
    *value == Decimal::ZERO
}

#[cfg(test)]
mod tests {
    use super::*;
    fn d(s: &str) -> Decimal {
        Decimal::from_str_exact(s).unwrap()
    }
    fn setup() -> (Book, Instrument, MatchSettings) {
        (
            Book {
                bids: vec![(d("9"), d("2")), (d("8"), d("4"))],
                asks: vec![(d("10"), d("2")), (d("11"), d("4"))],
                timestamp: 1000,
                version: "v1".into(),
            },
            Instrument {
                base: "BTC".into(),
                quote: "USDT".into(),
                step: d("0.001"),
                quote_step: d("0.01"),
                tick: d("0.01"),
                min_amount: d("1"),
                max_quantity: d("100"),
                max_limit_quantity: None,
                timestamp: 1000,
            },
            MatchSettings {
                fee_rate: d("0.001"),
                latency_ms: 25,
                max_age_ms: 1000,
                timeout_ms: 1000,
                slippage_percent: d("20"),
            },
        )
    }
    #[test]
    fn depth_buy_and_fee() {
        let (b, i, s) = setup();
        let r = match_order(&b, &i, OrderSide::Buy, d("3"), d("100"), &s, 1001).unwrap();
        assert_eq!(r.quote_value, d("31"));
        assert_eq!(r.fee, d("0.003"));
    }
    #[test]
    fn depth_sell_partial_and_fee() {
        let (b, i, s) = setup();
        let r = match_order(&b, &i, OrderSide::Sell, d("7"), d("7"), &s, 1001).unwrap();
        assert_eq!(r.quote_value, d("50"));
        assert_eq!(r.quantity, d("6"));
        assert_eq!(r.fee, d("0.05"));
        assert_eq!(r.remaining, d("1"));
    }
    #[test]
    fn budget_never_overspent() {
        let (b, i, s) = setup();
        let r = match_order(&b, &i, OrderSide::Buy, d("3"), d("25"), &s, 1001).unwrap();
        assert!(r.quote_value <= d("25"));
        assert_eq!(r.status, "PartiallyFilledCanceled");
    }
    #[test]
    fn stale_minimum_precision_and_slippage() {
        let (b, i, mut s) = setup();
        assert_eq!(
            match_order(&b, &i, OrderSide::Buy, d("3"), d("100"), &s, 3000)
                .unwrap()
                .quantity,
            Decimal::ZERO
        );
        assert!(match_order(&b, &i, OrderSide::Buy, d("0.0001"), d("100"), &s, 1001).is_err());
        assert_eq!(
            match_order(&b, &i, OrderSide::Buy, d("0.001"), d("100"), &s, 1001)
                .unwrap()
                .quantity,
            Decimal::ZERO
        );
        s.slippage_percent = d("1");
        assert_eq!(
            match_order(&b, &i, OrderSide::Buy, d("3"), d("100"), &s, 1001)
                .unwrap()
                .quantity,
            d("2")
        );
    }
    #[test]
    fn fee_rounding_is_conservative_and_invalid_settings_fail() {
        let (b, i, mut s) = setup();
        let r = match_order(&b, &i, OrderSide::Sell, d("0.123"), d("0.123"), &s, 1001).unwrap();
        assert_eq!(r.quote_value, d("1.10"));
        assert_eq!(r.quote_rounding_dust, d("0.007"));
        assert_eq!(r.price, Some(d("9")));
        assert_eq!(r.fee, d("0.01"));
        assert_eq!(r.fee_currency, "USDT");
        s.timeout_ms = 0;
        assert!(s.validate().is_err());
        s.timeout_ms = 1000;
        s.fee_rate = Decimal::ONE;
        assert!(s.validate().is_err());
    }
    #[test]
    fn maximum_quantity_and_metadata_age_are_enforced() {
        let (b, mut i, s) = setup();
        i.max_quantity = d("1");
        assert!(match_order(&b, &i, OrderSide::Buy, d("2"), d("100"), &s, 1001).is_err());
        i.timestamp = 0;
        assert_eq!(
            match_order(&b, &i, OrderSide::Buy, d("1"), d("100"), &s, 2001)
                .unwrap()
                .quantity,
            Decimal::ZERO
        );
    }
    #[test]
    fn quote_rounding_never_overspends_buy_budget() {
        let (mut b, mut i, s) = setup();
        b.asks = vec![(d("10.001"), d("2"))];
        i.tick = d("0.001");
        let r = match_order(&b, &i, OrderSide::Buy, d("0.999"), d("9.999"), &s, 1001).unwrap();
        assert_eq!(r.quantity, d("0.998"));
        assert_eq!(r.quote_value, d("9.99"));
        assert_eq!(r.quote_rounding_dust, d("0.009002"));
        assert!(r.quote_value <= d("9.999"));
    }
}
