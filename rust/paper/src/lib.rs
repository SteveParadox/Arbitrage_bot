//! Paper-only execution. No HTTP client, credential provider or exchange order constructor.
mod ledger;
mod matching;
mod venue;
use anyhow::{bail, Context, Result};
pub use ledger::{Ledger, Portfolio};
pub use matching::{decimal, match_order, Book, FillResult, Instrument, MatchSettings};
pub use venue::{PaperBooks, PaperPlanner, PaperVenue, Provider, StopGuard};

/// Deterministic crash injection is confined to explicitly authorized development paper runs.
pub fn crash_checkpoint(name: &str) {
    if std::env::var("ARB_ENV").as_deref() == Ok("development")
        && std::env::var("ARB_LIVE_TRADING_ENABLED").as_deref() == Ok("false")
        && std::env::var("ARB_PAPER_TRADING_ENABLED").as_deref() == Ok("true")
        && std::env::var("ARB_TRADING_MODE").as_deref() == Ok("paper")
        && std::env::var("ARB_PAPER_CRASH_AT").as_deref() == Ok(name)
    {
        std::process::exit(88);
    }
}
pub fn validate_mode(mode: &str, paper_enabled: bool, live_enabled: bool) -> Result<()> {
    if mode != "paper" || !paper_enabled || live_enabled {
        bail!("paper execution requires mode=paper, paper enabled, live disabled");
    }
    Ok(())
}

pub async fn redis_ready() -> Result<()> {
    let url = std::env::var("ARB_REDIS_URL").context("paper requires Redis URL")?;
    tokio::task::spawn_blocking(move || -> Result<()> {
        let timeout = std::time::Duration::from_millis(200);
        let mut connection =
            infrastructure::redis_client(&url)?.get_connection_with_timeout(timeout)?;
        connection.set_read_timeout(Some(timeout))?;
        connection.set_write_timeout(Some(timeout))?;
        let pong: String = redis::cmd("PING").query(&mut connection)?;
        if pong != "PONG" {
            bail!("Redis unhealthy");
        }
        Ok(())
    })
    .await??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn modes_fail_closed() {
        for mode in ["observe", "testnet", "live", "", "PAPER"] {
            assert!(validate_mode(mode, true, false).is_err());
        }
        assert!(validate_mode("paper", false, false).is_err());
        assert!(validate_mode("paper", true, true).is_err());
        assert!(validate_mode("paper", true, false).is_ok());
    }
}
