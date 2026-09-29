use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use scanner::{parse_decimal, ProfitabilityConfig, ProfitabilityConfigFile};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct FixtureCase {
    name: String,
    input: FixtureInput,
}

#[derive(Debug, Deserialize)]
struct FixtureInput {
    start_amount: String,
    gross_final_amount: String,
    config: ProfitabilityConfigFile,
}

#[derive(Debug, Serialize)]
struct FixtureOutput {
    name: String,
    result: scanner::CanonicalProfitabilityResult,
}

fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .context("usage: profitability-fixture <shared fixture JSON>")?;
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let cases: Vec<FixtureCase> = serde_json::from_str(&raw)?;

    let mut outputs = Vec::with_capacity(cases.len());
    for case in cases {
        let config: ProfitabilityConfig = case.input.config.try_into()?;
        let result = config.evaluate(
            parse_decimal("start_amount", &case.input.start_amount)?,
            parse_decimal("gross_final_amount", &case.input.gross_final_amount)?,
        )?;
        outputs.push(FixtureOutput {
            name: case.name,
            result: result.canonical(),
        });
    }

    println!("{}", serde_json::to_string(&outputs)?);
    Ok(())
}
