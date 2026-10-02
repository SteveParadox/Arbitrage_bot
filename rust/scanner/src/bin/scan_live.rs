use std::{
    fs,
    io::{self, BufRead},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use event_bus::EventPublisher;
use orderbook::BookUpdate;
use scanner::{
    load_profitability_config, load_triangle_config, ArbitrageScanner, NdjsonRecorder,
    ScanStatus, ScannerSettings,
};

fn main() -> Result<()> {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let triangle_path = std::env::var("ARB_TRIANGLE_CONFIG")
        .map(|value| resolve_path(&repo_root, &value))
        .unwrap_or_else(|_| repo_root.join("shared/config/triangles.json"));
    let scanner_config_path = std::env::var("ARB_SCANNER_CONFIG")
        .map(|value| resolve_path(&repo_root, &value))
        .unwrap_or_else(|_| repo_root.join("shared/config/scanner.json"));

    let triangle_config = load_triangle_config(&triangle_path)?;
    let scanner_settings = load_scanner_settings(&scanner_config_path)?;
    let profitability_path = std::env::var("ARB_PROFITABILITY_CONFIG")
        .map(|value| resolve_path(&repo_root, &value))
        .unwrap_or_else(|_| resolve_path(&repo_root, &scanner_settings.profitability_config_path));
    let profitability = load_profitability_config(&profitability_path)?;

    let record_path = resolve_path(&repo_root, &scanner_settings.record_path);
    let mut scanner = ArbitrageScanner::new(triangle_config, scanner_settings, profitability)
        .map_err(anyhow::Error::msg)?;
    let mut recorder = NdjsonRecorder::open(&record_path)?;
    let events = EventPublisher::try_from_env("scanner")
        .map_err(|error| anyhow::anyhow!("failed to initialize event publisher: {error}"))?;
    let reload_path = std::env::var("ARB_STRATEGY_RELOAD_FILE")
        .map(|value| resolve_path(&repo_root, &value))
        .unwrap_or_else(|_| repo_root.join("data/control/strategy_reload.json"));
    let mut reload_watcher = StrategyReloadWatcher::new(reload_path);

    eprintln!(
        "scanner ready; records will be appended to {}",
        record_path.display()
    );

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line?;
        reload_watcher.maybe_reload(&mut scanner, &triangle_path)?;
        if line.trim().is_empty() {
            continue;
        }

        let value: serde_json::Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(error) => {
                eprintln!("ignored malformed market-data line: {error}");
                scanner.reset_books();
                continue;
            }
        };

        let event_type = value.get("type").and_then(|value| value.as_str());
        if event_type == Some("status")
            && matches!(
                value.get("state").and_then(|v| v.as_str()),
                Some("connected" | "reconnecting" | "metadata_retry")
            )
        {
            scanner.reset_books();
        }
        if event_type != Some("order_book") {
            continue;
        }

        let update: BookUpdate = match serde_json::from_value(value) {
            Ok(update) => update,
            Err(error) => {
                eprintln!("ignored malformed order_book event: {error}");
                scanner.reset_books();
                continue;
            }
        };

        let records = match scanner.on_book_update(update) {
            Ok(records) => records,
            Err(error) => {
                eprintln!("book invalidated; waiting for snapshots: {error}");
                continue;
            }
        };
        if records.is_empty() {
            continue;
        }

        recorder.record_batch(&records)?;
        for record in records {
            if record.status == ScanStatus::Complete {
                events.publish(
                    "opportunity.detected",
                    serde_json::to_value(&record)?,
                );
            }
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


struct StrategyReloadWatcher {
    path: PathBuf,
    generation: Option<String>,
    next_check: Instant,
}

impl StrategyReloadWatcher {
    fn new(path: PathBuf) -> Self {
        let generation = read_generation(&path);
        Self {
            path,
            generation,
            next_check: Instant::now(),
        }
    }

    fn maybe_reload(
        &mut self,
        scanner: &mut ArbitrageScanner,
        triangle_path: &Path,
    ) -> Result<()> {
        let now = Instant::now();
        if now < self.next_check {
            return Ok(());
        }
        self.next_check = now + Duration::from_millis(500);

        let Some(generation) = read_generation(&self.path) else {
            return Ok(());
        };
        if self.generation.as_deref() == Some(generation.as_str()) {
            return Ok(());
        }

        let result = load_triangle_config(triangle_path)
            .with_context(|| "strategy reload could not read triangle config")
            .and_then(|config| {
                scanner
                    .reload_routes(config)
                    .map_err(anyhow::Error::msg)
            });
        self.generation = Some(generation.clone());
        match result {
            Ok(()) => {
                eprintln!("strategy routes reloaded; generation={generation}");
            }
            Err(error) => {
                eprintln!(
                    "strategy reload rejected; generation={generation}: {error}"
                );
            }
        }
        Ok(())
    }
}

fn read_generation(path: &Path) -> Option<String> {
    let raw = fs::read_to_string(path).ok()?;
    let value: serde_json::Value = serde_json::from_str(&raw).ok()?;
    value
        .get("generation")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}
