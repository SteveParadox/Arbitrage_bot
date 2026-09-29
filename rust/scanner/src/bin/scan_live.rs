use std::{
    fs,
    io::{self, BufRead},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use orderbook::BookUpdate;
use scanner::{
    load_triangle_config, ArbitrageScanner, NdjsonRecorder, ScannerSettings,
};

fn main() -> Result<()> {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let triangle_path = std::env::var("ARB_TRIANGLE_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| repo_root.join("shared/config/triangles.json"));
    let scanner_config_path = std::env::var("ARB_SCANNER_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| repo_root.join("shared/config/scanner.json"));

    let triangle_config = load_triangle_config(&triangle_path)?;
    let scanner_settings = load_scanner_settings(&scanner_config_path)?;
    let record_path = resolve_path(&repo_root, &scanner_settings.record_path);
    let mut scanner = ArbitrageScanner::new(triangle_config, scanner_settings)
        .map_err(anyhow::Error::msg)?;
    let mut recorder = NdjsonRecorder::open(&record_path)?;

    eprintln!(
        "scanner ready; records will be appended to {}",
        record_path.display()
    );

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let value: serde_json::Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(error) => {
                eprintln!("ignored malformed market-data line: {error}");
                continue;
            }
        };

        if value.get("type").and_then(|value| value.as_str()) != Some("order_book") {
            continue;
        }

        let update: BookUpdate = match serde_json::from_value(value) {
            Ok(update) => update,
            Err(error) => {
                eprintln!("ignored malformed order_book event: {error}");
                continue;
            }
        };

        let records = scanner.on_book_update(update)?;
        if records.is_empty() {
            continue;
        }

        recorder.record_batch(&records)?;
        for record in records {
            println!("{}", serde_json::to_string(&record)?);
        }
    }

    Ok(())
}

fn load_scanner_settings(path: &Path) -> Result<ScannerSettings> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read scanner config {}", path.display()))?;
    let settings: ScannerSettings = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse scanner config {}", path.display()))?;
    settings.validate().map_err(anyhow::Error::msg)?;
    Ok(settings)
}

fn resolve_path(repo_root: &Path, configured: &str) -> PathBuf {
    let path = PathBuf::from(configured);
    if path.is_absolute() {
        path
    } else {
        repo_root.join(path)
    }
}
