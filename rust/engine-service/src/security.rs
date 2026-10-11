//! Production paper/observer startup is fail closed; credentials are never logged.
use anyhow::{bail, Context, Result};
use std::{env, fs, path::Path};

fn load_secret(name: &str) -> Result<()> {
    let Ok(source) = env::var(format!("{name}_FILE")) else {
        return Ok(());
    };
    if env::var(name).is_ok_and(|v| !v.is_empty()) {
        bail!("inline and file-based secret sources conflict");
    }
    let path = Path::new(&source);
    let metadata = fs::metadata(path).context("secret file unavailable")?;
    if metadata.len() > 65536 {
        bail!("secret file exceeds size limit");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o022 != 0 {
            bail!("secret file writable by another user");
        }
    }
    let value = fs::read_to_string(path).context("secret file unreadable")?;
    env::set_var(name, value.trim());
    Ok(())
}

struct ProductionConfig {
    mode: String,
    live: String,
    token: String,
    database: String,
    redis: String,
    forbidden: bool,
}
fn validate(config: &ProductionConfig) -> Result<()> {
    if config.live != "false" || !matches!(config.mode.as_str(), "paper" | "observe") {
        bail!("production requires explicit paper/observe mode and live=false");
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
    for name in ["ARB_ENGINE_GRPC_TOKEN", "ARB_DATABASE_URL", "ARB_REDIS_URL"] {
        load_secret(name)?;
    }
    let environment = env::var("ARB_ENV").unwrap_or_else(|_| "development".into());
    if !matches!(environment.as_str(), "development" | "test" | "production") {
        bail!("unknown deployment environment");
    }
    if environment == "production" {
        let forbidden = [
            "BYBIT_API_KEY",
            "BYBIT_API_SECRET",
            "ARB_OBSERVER_REPLAY_FILE",
            "ARB_PAPER_CRASH_AT",
            "ARB_PAPER_CRASH_AFTER_LEG",
            "ARB_TEST_GRPC_RESPONSE_DELAY_MS",
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
        }
    }
    #[test]
    fn production_fails_closed() {
        assert!(validate(&safe()).is_ok());
        for mode in ["", "live", "testnet"] {
            let mut c = safe();
            c.mode = mode.into();
            assert!(validate(&c).is_err());
        }
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
