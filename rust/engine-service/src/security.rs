//! Production paper/observer startup is fail closed; credentials are never logged.
use anyhow::{bail, Result};
use std::env;

struct ProductionConfig {
    mode: String,
    live: String,
    token: String,
    database: String,
    redis: String,
    forbidden: bool,
    testnet_enabled: bool,
}
fn validate(config: &ProductionConfig) -> Result<()> {
    let valid_mode = match config.mode.as_str() {
        "paper" | "observe" => config.live == "false" && !config.testnet_enabled,
        "testnet" => config.live == "false" && config.testnet_enabled,
        "live" => matches!(config.live.as_str(), "true" | "false") && !config.testnet_enabled,
        _ => false,
    };
    if !valid_mode {
        bail!("production execution mode and deployment flags inconsistent");
    }
    if config.forbidden {
        bail!("development replay/fault hooks and private exchange credentials forbidden in production");
    }
    let token = config.token.to_lowercase();
    if config.token.len() < 32
        || ["example", "changeme", "test-token"]
            .iter()
            .any(|word| token.contains(word))
    {
        bail!("production requires a strong internal control credential");
    }
    for value in [&config.database, &config.redis] {
        let url = url::Url::parse(&value.replace("postgresql+psycopg:", "postgresql:"))
            .map_err(|_| anyhow::anyhow!("invalid data-service URL"))?;
        let password = percent_encoding::percent_decode_str(url.password().unwrap_or(""))
            .decode_utf8()
            .map_err(|_| anyhow::anyhow!("invalid data-service password"))?;
        if password.len() < 16 {
            bail!("production requires authenticated data services");
        }
    }
    Ok(())
}

pub fn initialize() -> Result<()> {
    for name in [
        "ARB_ENGINE_GRPC_TOKEN",
        "ARB_DATABASE_URL",
        "ARB_REDIS_URL",
        "ARB_CONTROL_API_TOKEN",
        "ARB_METRICS_TOKEN",
        "BYBIT_API_KEY",
        "BYBIT_API_SECRET",
    ] {
        infrastructure::load_secret(name)?;
    }
    let mode = env::var("ARB_TRADING_MODE").unwrap_or_else(|_| "observe".into());
    let _: execution::ExecutionMode = mode.parse()?;
    if matches!(mode.as_str(), "testnet" | "live") {
        let prefix = if mode == "testnet" {
            "BYBIT_TESTNET"
        } else {
            "BYBIT_LIVE"
        };
        for suffix in ["API_KEY", "API_SECRET"] {
            infrastructure::load_secret(&format!("{prefix}_{suffix}"))?;
        }
    }
    let environment = env::var("ARB_ENV").unwrap_or_else(|_| "development".into());
    if !matches!(environment.as_str(), "development" | "test" | "production") {
        bail!("unknown deployment environment");
    }
    if environment == "production" {
        if env::var("ARB_DATABASE_SSLMODE").as_deref() != Ok("verify-full") {
            bail!("production PostgreSQL requires verify-full");
        }
        for name in [
            "ARB_DATABASE_TLS_CA_CERT",
            "ARB_REDIS_TLS_CA_CERT",
            "ARB_ENGINE_GRPC_TLS_CA_CERT",
            "ARB_ENGINE_GRPC_TLS_CERT",
            "ARB_ENGINE_GRPC_TLS_KEY",
        ] {
            if env::var(name).is_err() {
                bail!("production verified transport configuration incomplete");
            }
        }
        infrastructure::redis_client(&env::var("ARB_REDIS_URL").unwrap_or_default())?;
        let forbidden = [
            "BYBIT_API_KEY",
            "BYBIT_API_SECRET",
            "ARB_OBSERVER_REPLAY_FILE",
            "ARB_PAPER_CRASH_AT",
            "ARB_PAPER_CRASH_AFTER_LEG",
            "ARB_TEST_GRPC_RESPONSE_DELAY_MS",
            "ARB_EXECUTION_OFFLINE_MOCK",
            "ARB_EXECUTION_MOCK_URL",
            "ARB_EXECUTION_CRASH_AT",
        ]
        .iter()
        .any(|name| env::var(name).is_ok_and(|v| !v.is_empty()))
            || env::var("ARB_REPLAY_CLOCK").as_deref() == Ok("virtual");
        validate(&ProductionConfig {
            mode: env::var("ARB_TRADING_MODE").unwrap_or_default(),
            live: env::var("ARB_LIVE_TRADING_ENABLED").unwrap_or_default(),
            token: env::var("ARB_ENGINE_GRPC_TOKEN").unwrap_or_default(),
            database: env::var("ARB_DATABASE_URL").unwrap_or_default(),
            redis: env::var("ARB_REDIS_URL").unwrap_or_default(),
            forbidden,
            testnet_enabled: env::var("ARB_TESTNET_TRADING_ENABLED").as_deref() == Ok("true"),
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn safe() -> ProductionConfig {
        ProductionConfig {
            mode: "paper".into(),
            live: "false".into(),
            token: "internal-credential-0123456789-abcdefghijk".into(),
            database: "postgresql://runtime:strong-password-012345@db/paper".into(),
            redis: "redis://:strong-password-678901@cache/0".into(),
            forbidden: false,
            testnet_enabled: false,
        }
    }
    #[test]
    fn production_fails_closed() {
        assert!(validate(&safe()).is_ok());
        for mode in ["", "testnet"] {
            let mut c = safe();
            c.mode = mode.into();
            assert!(validate(&c).is_err());
        }
        let mut testnet = safe();
        testnet.mode = "testnet".into();
        testnet.testnet_enabled = true;
        assert!(validate(&testnet).is_ok());
        let mut c = safe();
        c.live = "true".into();
        assert!(validate(&c).is_err());
        let mut c = safe();
        c.forbidden = true;
        assert!(validate(&c).is_err());
        let mut c = safe();
        c.token = "example-token".repeat(4);
        assert!(validate(&c).is_err());
        let mut c = safe();
        c.redis = "redis://cache/0".into();
        assert!(validate(&c).is_err());
        let mut c = safe();
        c.database = "postgresql://runtime:%41%41%41%41%41%41@db/paper".into();
        assert!(validate(&c).is_err());
    }
}
