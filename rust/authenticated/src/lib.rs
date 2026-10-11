//! Managed exchange execution support. This is not another scanner or risk engine.
mod ledger;
mod venue;
pub use ledger::{Ledger, Portfolio};
pub use venue::{ExchangePlanner, Gate, Provider, Venue};

use anyhow::{bail, Result};
use execution::ExecutionMode;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Deployment permission is independent of credentials and runtime commands.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentPermit {
    pub version: u32,
    pub account_id: String,
    pub mode: String,
    pub exchange_uid: String,
    pub expires_at_ms: u64,
    pub offline_tests: bool,
    pub testnet_execution_verified: bool,
    pub reconciliation_verified: bool,
    pub crash_recovery_verified: bool,
    pub security_verified: bool,
    pub soak_verified: bool,
    pub human_mainnet_authorization: bool,
}
impl DeploymentPermit {
    pub fn verify(&self, account: &str, mode: ExecutionMode, now: u64, mock: bool) -> Result<()> {
        let name = match mode {
            ExecutionMode::Testnet => "testnet",
            ExecutionMode::Live => "live",
            _ => bail!("authenticated execution mode required"),
        };
        if self.version != 1
            || self.account_id != account
            || self.mode != name
            || self.exchange_uid.is_empty()
            || self.expires_at_ms <= now
            || self.expires_at_ms - now > 86_400_000
        {
            bail!("deployment authorization missing, stale or inconsistent");
        }
        if mode == ExecutionMode::Live
            && (mock
                || !(self.offline_tests
                    && self.testnet_execution_verified
                    && self.reconciliation_verified
                    && self.crash_recovery_verified
                    && self.security_verified
                    && self.soak_verified
                    && self.human_mainnet_authorization))
        {
            bail!("live operational readiness and human authorization required");
        }
        Ok(())
    }
    pub fn load(path: &Path) -> Result<Self> {
        let metadata = path.metadata()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o022 != 0 {
                bail!("deployment permit writable by another user");
            }
        }
        if metadata.len() > 8192 {
            bail!("oversized deployment permit");
        }
        Ok(serde_json::from_slice(&std::fs::read(path)?)?)
    }
}

pub fn checkpoint(client: &execution::BybitExecutionClient, name: &str) {
    if client.config().is_offline_mock()
        && std::env::var("ARB_ENV").as_deref() == Ok("development")
        && std::env::var("ARB_EXECUTION_CRASH_AT").as_deref() == Ok(name)
    {
        std::process::exit(89);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn permit() -> DeploymentPermit {
        DeploymentPermit {
            version: 1,
            account_id: "account".into(),
            mode: "live".into(),
            exchange_uid: "123".into(),
            expires_at_ms: 2000,
            offline_tests: true,
            testnet_execution_verified: true,
            reconciliation_verified: true,
            crash_recovery_verified: true,
            security_verified: true,
            soak_verified: true,
            human_mainnet_authorization: true,
        }
    }
    #[test]
    fn every_live_readiness_gate_is_mandatory() {
        let p = permit();
        assert!(p
            .verify("account", ExecutionMode::Live, 1000, false)
            .is_ok());
        for key in [
            "offline_tests",
            "testnet_execution_verified",
            "reconciliation_verified",
            "crash_recovery_verified",
            "security_verified",
            "soak_verified",
            "human_mainnet_authorization",
        ] {
            let mut v = serde_json::to_value(&p).unwrap();
            v[key] = serde_json::json!(false);
            let changed: DeploymentPermit = serde_json::from_value(v).unwrap();
            assert!(
                changed
                    .verify("account", ExecutionMode::Live, 1000, false)
                    .is_err(),
                "{key}"
            );
        }
        for (account, mode, now, mock) in [
            ("another", ExecutionMode::Live, 1000, false),
            ("account", ExecutionMode::Testnet, 1000, false),
            ("account", ExecutionMode::Live, 2000, false),
            ("account", ExecutionMode::Live, 1000, true),
            ("account", ExecutionMode::Paper, 1000, false),
        ] {
            assert!(p.verify(account, mode, now, mock).is_err());
        }
    }
}
