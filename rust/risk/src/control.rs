//! Shared strict runtime gate used by risk approvals and the gRPC control service.
use anyhow::{bail, Context, Result};
use chrono::Utc;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Read,
    path::Path,
    time::{Duration, Instant},
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlState {
    pub version: u32,
    pub enabled: bool,
    pub updated_at: String,
    pub reason: String,
    pub source: String,
    #[serde(default)]
    pub request_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopIntent {
    pub version: u32,
    pub request_id: String,
    pub reason: String,
    pub status: String,
    pub updated_at: String,
}

pub fn read_stop_intent(path: &Path) -> Result<Option<StopIntent>> {
    let raw = match read_state_file(&path.with_extension("stop.json")) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let intent: StopIntent = serde_json::from_str(&raw)?;
    if intent.version != 1
        || !matches!(
            intent.status.as_str(),
            "STOP_REQUESTED" | "STOP_UNCONFIRMED" | "CONFIRMED_STOPPED"
        )
    {
        bail!("invalid stop intent version or status");
    }
    validate_request_id(Some(&intent.request_id))?;
    validate_reason(&intent.reason)?;
    let timestamp = chrono::DateTime::parse_from_rfc3339(&intent.updated_at)?;
    if timestamp.timestamp_millis() > Utc::now().timestamp_millis() + 5000 {
        bail!("future-dated stop intent");
    }
    Ok(Some(intent))
}

pub fn stop_pending(path: &Path) -> bool {
    match read_stop_intent(path) {
        Ok(Some(intent)) => intent.status != "CONFIRMED_STOPPED",
        Ok(None) => false,
        Err(_) => true,
    }
}

pub fn read_control(path: &Path) -> Result<Option<ControlState>> {
    let raw = match read_state_file(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut state: ControlState = serde_json::from_str(&raw)?;
    if state.version != 1 {
        bail!("unsupported runtime control version");
    }
    validate_reason(&state.reason)?;
    if !matches!(
        state.source.as_str(),
        "fastapi_control" | "rust_grpc_control"
    ) {
        bail!("unsupported runtime control source");
    }
    validate_request_id(state.request_id.as_deref())?;
    let updated = chrono::DateTime::parse_from_rfc3339(&state.updated_at)
        .context("runtime control updated_at must be RFC3339")?;
    let max_age = std::env::var("ARB_CONTROL_STATE_MAX_AGE_SECONDS")
        .unwrap_or_else(|_| "3600".into())
        .parse::<i64>()
        .context("invalid control state max age")?;
    if !(1..=86400).contains(&max_age) {
        bail!("invalid control state max age");
    }
    let age_ms = Utc::now().timestamp_millis() - updated.timestamp_millis();
    if age_ms < -5000 || (state.enabled && age_ms > max_age * 1000) {
        bail!("runtime control timestamp is future-dated or enabled state has expired");
    }
    if stop_pending(path) {
        state.enabled = false;
    }
    Ok(Some(state))
}

fn read_state_file(path: &Path) -> std::io::Result<String> {
    let mut raw = String::new();
    File::open(path)?.take(16385).read_to_string(&mut raw)?;
    if raw.len() > 16384 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "runtime control record exceeds size limit",
        ));
    }
    Ok(raw)
}

fn validate_reason(reason: &str) -> Result<()> {
    if reason.trim().is_empty() || reason.chars().count() > 256 {
        bail!("runtime control reason must contain 1-256 characters");
    }
    Ok(())
}

fn validate_request_id(value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        if value.is_empty()
            || value.len() > 64
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            bail!("invalid control request_id");
        }
    }
    Ok(())
}

/// Advisory lock shared with Python. Never unlink it; the OS releases it on crash.
pub fn lock_control(path: &Path) -> Result<File> {
    let lock_path = path.with_extension("lock");
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)?;
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match file.try_lock_exclusive() {
            Ok(()) => return Ok(file),
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error).context("runtime control lock unavailable"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "control-{name}-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir.join("trading_state.json")
    }
    fn valid() -> serde_json::Value {
        serde_json::json!({"version":1,"enabled":true,"updated_at":Utc::now().to_rfc3339(),
            "reason":"operator approved test","source":"rust_grpc_control","request_id":"test-op"})
    }
    #[test]
    fn strict_control_rejects_malformed_fields_and_expired_activation() {
        let path = path("strict");
        let mut samples = vec![
            serde_json::json!(null),
            serde_json::json!([]),
            serde_json::json!({"version":1,"enabled":true}),
        ];
        for (field, value) in [
            ("version", serde_json::json!(2)),
            ("enabled", serde_json::json!("true")),
            ("reason", serde_json::json!(" ")),
            ("source", serde_json::json!("unknown")),
            ("updated_at", serde_json::json!("invalid")),
            ("updated_at", serde_json::json!("2000-01-01T00:00:00Z")),
            (
                "updated_at",
                serde_json::json!((Utc::now() + chrono::Duration::minutes(1)).to_rfc3339()),
            ),
            ("request_id", serde_json::json!("../bad")),
            ("unknown", serde_json::json!(true)),
        ] {
            let mut payload = valid();
            payload[field] = value;
            samples.push(payload);
        }
        assert!(read_control(&path).unwrap().is_none());
        for payload in samples {
            let raw = serde_json::to_string(&payload).unwrap();
            fs::write(&path, &raw).unwrap();
            assert!(read_control(&path).is_err(), "accepted {raw}");
            assert_eq!(fs::read_to_string(&path).unwrap(), raw);
        }
        fs::write(&path, serde_json::to_vec(&valid()).unwrap()).unwrap();
        assert!(read_control(&path).unwrap().unwrap().enabled);
        fs::write(&path, " ".repeat(16385)).unwrap();
        assert!(read_control(&path).is_err());
        fs::write(&path, r#"{"version":1,"version":1}"#).unwrap();
        assert!(read_control(&path).is_err());
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
    #[test]
    fn durable_stop_latch_blocks_activation_including_corrupt_latch() {
        let path = path("stop");
        fs::write(&path, serde_json::to_vec(&valid()).unwrap()).unwrap();
        fs::write(path.with_extension("stop.json"),serde_json::to_vec(&serde_json::json!({
            "version":1,"request_id":"stop-1","reason":"stop","status":"STOP_REQUESTED","updated_at":Utc::now().to_rfc3339()
        })).unwrap()).unwrap();
        assert!(!read_control(&path).unwrap().unwrap().enabled);
        fs::write(path.with_extension("stop.json"), "{bad").unwrap();
        assert!(!read_control(&path).unwrap().unwrap().enabled);
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
    #[test]
    fn control_locks_release_on_drop() {
        let path = path("lock");
        let first = lock_control(&path).unwrap();
        drop(first);
        drop(lock_control(&path).unwrap());
        fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
}
