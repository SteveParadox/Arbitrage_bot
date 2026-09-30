use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use risk::{current_time_ms, load_risk_config, RiskEngine};

fn main() -> Result<()> {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let config_path = std::env::var("ARB_RISK_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| repo_root.join("shared/config/risk.json"));

    let config = load_risk_config(&config_path)
        .with_context(|| format!("failed to load {}", config_path.display()))?;
    let mut engine = RiskEngine::new(config)?;
    let mut args = std::env::args().skip(1);

    match args.next().as_deref() {
        Some("status") => {
            let status = engine.status(current_time_ms())?;
            println!("{}", serde_json::to_string_pretty(&status)?);
        }
        Some("kill") => {
            let reason = args.collect::<Vec<_>>().join(" ");
            if reason.trim().is_empty() {
                bail!("usage: riskctl kill <reason>");
            }
            engine.engage_manual_kill_switch(&reason, current_time_ms())?;
            println!("manual kill switch engaged");
        }
        Some("clear-kill") => {
            let note = args.collect::<Vec<_>>().join(" ");
            if note.trim().is_empty() {
                bail!("usage: riskctl clear-kill <operator note>");
            }
            engine.clear_manual_kill_switch(&note)?;
            println!("manual kill switch cleared");
        }
        Some("reset-breaker") => {
            let note = args.collect::<Vec<_>>().join(" ");
            if note.trim().is_empty() {
                bail!("usage: riskctl reset-breaker <operator note>");
            }
            engine.reset_circuit_breaker(&note)?;
            println!("circuit breaker reset");
        }
        _ => {
            bail!(
                "usage: riskctl <status|kill|clear-kill|reset-breaker> [reason/note]"
            );
        }
    }

    Ok(())
}
