use std::{env, time::Duration};

use hmac::{Hmac, Mac};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;
use sha2::Sha256;

use crate::ShadowError;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct ReadOnlyFeeRate {
    pub symbol: String,
    pub maker_fee_rate: Decimal,
    pub taker_fee_rate: Decimal,
}

#[derive(Debug, Clone)]
pub struct ReadOnlyAccountSnapshot {
    pub base_asset: String,
    pub base_available: Decimal,
    pub total_equity_usd: Decimal,
    pub total_wallet_balance_usd: Decimal,
    pub non_base_exposure_usd: Decimal,
    pub synchronized_at_ms: u64,
}

#[derive(Clone)]
pub struct ReadOnlyAccountClient {
    api_key: String,
    api_secret: String,
    recv_window_ms: u64,
    base_asset: String,
    http: Client,
}

impl ReadOnlyAccountClient {
    pub fn from_env(base_asset: &str) -> Result<Self, ShadowError> {
        let api_key = env::var("BYBIT_SHADOW_API_KEY").unwrap_or_default();
        let api_secret = env::var("BYBIT_SHADOW_API_SECRET").unwrap_or_default();
        if api_key.trim().is_empty() || api_secret.trim().is_empty() {
            return Err(ShadowError::Account(
                concat!(
                    "BYBIT_SHADOW_API_KEY and BYBIT_SHADOW_API_SECRET are ",
                    "required for read-only mainnet account sync"
                )
                .to_string(),
            ));
        }

        let recv_window_ms = env::var("BYBIT_SHADOW_RECV_WINDOW_MS")
            .unwrap_or_else(|_| "5000".to_string())
            .parse::<u64>()
            .map_err(|_| {
                ShadowError::Account(
                    "BYBIT_SHADOW_RECV_WINDOW_MS must be an unsigned integer".to_string(),
                )
            })?;
        let timeout_ms = env::var("BYBIT_SHADOW_REQUEST_TIMEOUT_MS")
            .unwrap_or_else(|_| "3000".to_string())
            .parse::<u64>()
            .map_err(|_| {
                ShadowError::Account(
                    "BYBIT_SHADOW_REQUEST_TIMEOUT_MS must be an unsigned integer".to_string(),
                )
            })?;

        if recv_window_ms == 0 || timeout_ms == 0 {
            return Err(ShadowError::Account(
                "shadow account request timing values must be positive".to_string(),
            ));
        }

        let http = Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .map_err(|error| ShadowError::Account(error.to_string()))?;

        Ok(Self {
            api_key,
            api_secret,
            recv_window_ms,
            base_asset: base_asset.to_uppercase(),
            http,
        })
    }

    pub async fn get_spot_fee_rate(&self, symbol: &str) -> Result<ReadOnlyFeeRate, ShadowError> {
        if symbol.trim().is_empty() || symbol != symbol.to_uppercase() {
            return Err(ShadowError::Account(
                "fee-rate symbol must be non-empty uppercase text".to_string(),
            ));
        }
        let query = format!("category=spot&symbol={symbol}");
        let body = self.signed_get("/v5/account/fee-rate", &query).await?;
        let envelope: FeeEnvelope = serde_json::from_str(&body)?;
        if envelope.ret_code != 0 {
            return Err(ShadowError::Account(format!(
                "Bybit fee-rate error {}: {}",
                envelope.ret_code, envelope.ret_msg
            )));
        }
        let row = envelope.result.list.into_iter().next().ok_or_else(|| {
            ShadowError::Account(format!("fee-rate response contained no row for {symbol}"))
        })?;
        Ok(ReadOnlyFeeRate {
            symbol: if row.symbol.is_empty() {
                symbol.to_string()
            } else {
                row.symbol
            },
            maker_fee_rate: parse_decimal("makerFeeRate", &row.maker_fee_rate)?,
            taker_fee_rate: parse_decimal("takerFeeRate", &row.taker_fee_rate)?,
        })
    }

    async fn signed_get(&self, path: &str, query: &str) -> Result<String, ShadowError> {
        let timestamp = current_time_ms();
        let signature = sign(
            &self.api_secret,
            timestamp,
            &self.api_key,
            self.recv_window_ms,
            query,
        )?;
        let url = format!("https://api.bybit.com{path}?{query}");
        let response = self
            .http
            .get(url)
            .header("X-BAPI-API-KEY", &self.api_key)
            .header("X-BAPI-TIMESTAMP", timestamp.to_string())
            .header("X-BAPI-RECV-WINDOW", self.recv_window_ms.to_string())
            .header("X-BAPI-SIGN", signature)
            .send()
            .await
            .map_err(|error| ShadowError::Account(error.to_string()))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| ShadowError::Account(error.to_string()))?;
        if !status.is_success() {
            return Err(ShadowError::Account(format!(
                "{path} HTTP {}: {}",
                status.as_u16(),
                body
            )));
        }
        Ok(body)
    }

    pub async fn sync(&self) -> Result<ReadOnlyAccountSnapshot, ShadowError> {
        let query = "accountType=UNIFIED";
        let body = self.signed_get("/v5/account/wallet-balance", query).await?;

        let envelope: WalletEnvelope = serde_json::from_str(&body)?;
        if envelope.ret_code != 0 {
            return Err(ShadowError::Account(format!(
                "Bybit wallet-balance error {}: {}",
                envelope.ret_code, envelope.ret_msg
            )));
        }

        let account = envelope.result.list.into_iter().next().ok_or_else(|| {
            ShadowError::Account("wallet-balance response contained no account".to_string())
        })?;

        let mut base_available = Decimal::ZERO;
        let mut non_base_exposure_usd = Decimal::ZERO;

        for coin in account.coin {
            let wallet = parse_decimal("walletBalance", &coin.wallet_balance)?;
            let locked = parse_decimal("locked", &coin.locked)?;
            let borrow = parse_decimal("spotBorrow", &coin.spot_borrow)?;
            let usd_value = parse_decimal("usdValue", &coin.usd_value)?;
            if coin.coin == self.base_asset {
                base_available = (wallet - locked - borrow).max(Decimal::ZERO);
            } else {
                non_base_exposure_usd += if usd_value < Decimal::ZERO {
                    -usd_value
                } else {
                    usd_value
                };
            }
        }

        Ok(ReadOnlyAccountSnapshot {
            base_asset: self.base_asset.clone(),
            base_available,
            total_equity_usd: parse_decimal("totalEquity", &account.total_equity)?,
            total_wallet_balance_usd: parse_decimal(
                "totalWalletBalance",
                &account.total_wallet_balance,
            )?,
            non_base_exposure_usd,
            synchronized_at_ms: envelope.time,
        })
    }
}

fn sign(
    secret: &str,
    timestamp_ms: u64,
    api_key: &str,
    recv_window_ms: u64,
    query: &str,
) -> Result<String, ShadowError> {
    let payload = format!("{timestamp_ms}{api_key}{recv_window_ms}{query}");
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|error| ShadowError::Account(error.to_string()))?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn parse_decimal(field: &str, value: &str) -> Result<Decimal, ShadowError> {
    if value.is_empty() {
        return Ok(Decimal::ZERO);
    }
    Decimal::from_str_exact(value)
        .map_err(|_| ShadowError::Account(format!("invalid decimal field {field}: {value}")))
}

fn current_time_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Debug, Deserialize)]
struct FeeEnvelope {
    #[serde(rename = "retCode")]
    ret_code: i64,
    #[serde(rename = "retMsg")]
    ret_msg: String,
    result: FeeResult,
}

#[derive(Debug, Deserialize)]
struct FeeResult {
    #[serde(default)]
    list: Vec<FeeRow>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FeeRow {
    #[serde(default)]
    symbol: String,
    #[serde(default)]
    maker_fee_rate: String,
    #[serde(default)]
    taker_fee_rate: String,
}

#[derive(Debug, Deserialize)]
struct WalletEnvelope {
    #[serde(rename = "retCode")]
    ret_code: i64,
    #[serde(rename = "retMsg")]
    ret_msg: String,
    result: WalletResult,
    time: u64,
}

#[derive(Debug, Deserialize)]
struct WalletResult {
    #[serde(default)]
    list: Vec<WalletAccount>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WalletAccount {
    #[serde(default)]
    total_equity: String,
    #[serde(default)]
    total_wallet_balance: String,
    #[serde(default)]
    coin: Vec<WalletCoin>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WalletCoin {
    coin: String,
    #[serde(default)]
    wallet_balance: String,
    #[serde(default)]
    locked: String,
    #[serde(default)]
    spot_borrow: String,
    #[serde(default)]
    usd_value: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_is_deterministic() {
        let first = sign("secret", 123, "key", 5000, "accountType=UNIFIED").unwrap();
        let second = sign("secret", 123, "key", 5000, "accountType=UNIFIED").unwrap();
        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
    }
}
