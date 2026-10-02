use std::{
    fmt::Write as FmtWrite,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CommandStatus {
    InProgress,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CachedCommandReply {
    pub accepted: bool,
    pub command: String,
    pub request_id: String,
    pub detail: String,
    pub applied_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CachedFailure {
    pub code: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandRecord {
    pub request_id: String,
    pub command_type: String,
    pub request_fingerprint: String,
    pub status: CommandStatus,
    pub event_id: String,
    pub event_accepted: bool,
    pub created_at_ms: u64,
    pub completed_at_ms: Option<u64>,
    pub response: Option<CachedCommandReply>,
    pub failure: Option<CachedFailure>,
}

#[derive(Debug)]
pub enum ClaimOutcome {
    New(CommandRecord),
    Completed(CommandRecord),
    InProgress(CommandRecord),
    Conflict {
        existing_command: String,
        existing_fingerprint: String,
    },
}

#[derive(Debug, Clone)]
pub struct StoreHealth {
    pub healthy: bool,
    pub in_progress: usize,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct IdempotencyStore {
    root: PathBuf,
    retention: Duration,
}

impl IdempotencyStore {
    pub fn open(root: PathBuf, retention_seconds: u64) -> Result<Self> {
        if retention_seconds == 0 {
            bail!("gRPC idempotency retention must be greater than zero");
        }
        fs::create_dir_all(&root)
            .with_context(|| format!("failed to create idempotency store {}", root.display()))?;
        sync_directory(&root)
            .with_context(|| format!("failed to sync idempotency store {}", root.display()))?;
        Ok(Self {
            root,
            retention: Duration::from_secs(retention_seconds),
        })
    }

    pub fn claim(
        &self,
        request_id: &str,
        command_type: &str,
        request_fingerprint: &str,
    ) -> Result<ClaimOutcome> {
        self.prune_completed()?;
        let request_dir = self.request_dir(request_id);
        match fs::create_dir(&request_dir) {
            Ok(()) => {
                sync_directory(&self.root)?;
                let record = CommandRecord {
                    request_id: request_id.to_string(),
                    command_type: command_type.to_string(),
                    request_fingerprint: request_fingerprint.to_string(),
                    status: CommandStatus::InProgress,
                    event_id: stable_event_id(request_id, command_type),
                    event_accepted: false,
                    created_at_ms: now_ms(),
                    completed_at_ms: None,
                    response: None,
                    failure: None,
                };
                if let Err(error) = self.write_record(&record) {
                    let _ = fs::remove_dir_all(&request_dir);
                    let _ = sync_directory(&self.root);
                    return Err(error);
                }
                Ok(ClaimOutcome::New(record))
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let record = self.read_record_retry(&request_dir)?;
                if record.request_id != request_id
                    || record.command_type != command_type
                    || record.request_fingerprint != request_fingerprint
                {
                    return Ok(ClaimOutcome::Conflict {
                        existing_command: record.command_type,
                        existing_fingerprint: record.request_fingerprint,
                    });
                }
                match record.status {
                    CommandStatus::InProgress => Ok(ClaimOutcome::InProgress(record)),
                    CommandStatus::Succeeded | CommandStatus::Failed => {
                        Ok(ClaimOutcome::Completed(record))
                    }
                }
            }
            Err(error) => Err(error)
                .with_context(|| format!("failed to claim request_id {request_id}")),
        }
    }

    pub fn mark_event_accepted(&self, record: &mut CommandRecord) -> Result<()> {
        record.event_accepted = true;
        self.write_record(record)
    }

    pub fn complete_success(
        &self,
        record: &mut CommandRecord,
        response: CachedCommandReply,
    ) -> Result<()> {
        record.status = CommandStatus::Succeeded;
        record.completed_at_ms = Some(now_ms());
        record.response = Some(response);
        record.failure = None;
        self.write_record(record)
    }

    pub fn complete_failure(
        &self,
        record: &mut CommandRecord,
        code: impl Into<String>,
        detail: impl Into<String>,
    ) -> Result<()> {
        record.status = CommandStatus::Failed;
        record.completed_at_ms = Some(now_ms());
        record.response = None;
        record.failure = Some(CachedFailure {
            code: code.into(),
            detail: detail.into(),
        });
        self.write_record(record)
    }

    pub fn reset_in_progress_after_reconciliation(
        &self,
        record: &mut CommandRecord,
    ) -> Result<()> {
        record.status = CommandStatus::InProgress;
        record.completed_at_ms = None;
        record.response = None;
        record.failure = None;
        self.write_record(record)
    }

    pub fn health(&self) -> StoreHealth {
        match self.health_inner() {
            Ok(in_progress) => StoreHealth {
                healthy: true,
                in_progress,
                detail: "durable gRPC idempotency store ready".to_string(),
            },
            Err(error) => StoreHealth {
                healthy: false,
                in_progress: 0,
                detail: error.to_string(),
            },
        }
    }

    fn health_inner(&self) -> Result<usize> {
        fs::create_dir_all(&self.root)?;
        let probe = self.root.join(format!(".health-{}", Uuid::new_v4()));
        {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&probe)?;
            file.write_all(b"ok")?;
            file.sync_all()?;
        }
        fs::remove_file(&probe)?;
        sync_directory(&self.root)?;

        let mut in_progress = 0;
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            if let Ok(record) = self.read_record(&entry.path()) {
                if record.status == CommandStatus::InProgress {
                    in_progress += 1;
                }
            }
        }
        Ok(in_progress)
    }

    fn prune_completed(&self) -> Result<()> {
        let cutoff_ms = now_ms().saturating_sub(self.retention.as_millis() as u64);
        let mut removed = false;
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let path = entry.path();
            let Ok(record) = self.read_record(&path) else {
                continue;
            };
            if record.status == CommandStatus::InProgress {
                continue;
            }
            if record.completed_at_ms.unwrap_or(u64::MAX) < cutoff_ms {
                fs::remove_dir_all(path)?;
                removed = true;
            }
        }
        if removed {
            sync_directory(&self.root)?;
        }
        Ok(())
    }

    fn request_dir(&self, request_id: &str) -> PathBuf {
        self.root.join(hex_digest(request_id.as_bytes()))
    }

    fn read_record_retry(&self, request_dir: &Path) -> Result<CommandRecord> {
        let mut last_error = None;
        for _ in 0..20 {
            match self.read_record(request_dir) {
                Ok(record) => return Ok(record),
                Err(error) => {
                    last_error = Some(error);
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("idempotency record unavailable")))
    }

    fn read_record(&self, request_dir: &Path) -> Result<CommandRecord> {
        let path = request_dir.join("record.json");
        let raw = fs::read(&path)
            .with_context(|| format!("failed to read idempotency record {}", path.display()))?;
        serde_json::from_slice(&raw)
            .with_context(|| format!("failed to decode idempotency record {}", path.display()))
    }

    fn write_record(&self, record: &CommandRecord) -> Result<()> {
        let request_dir = self.request_dir(&record.request_id);
        fs::create_dir_all(&request_dir)?;
        let final_path = request_dir.join("record.json");
        let temp_path = request_dir.join(format!(".tmp-{}.json", Uuid::new_v4()));
        let bytes = serde_json::to_vec_pretty(record)?;

        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temp_path, &final_path)?;
        sync_directory(&request_dir)?;
        sync_directory(&self.root)?;
        Ok(())
    }
}

pub fn fingerprint(command_type: &str, normalized_payload: &str) -> String {
    hex_digest(format!("{command_type}\n{normalized_payload}").as_bytes())
}

pub fn stable_event_id(request_id: &str, command_type: &str) -> String {
    let digest = hex_digest(format!("{command_type}\n{request_id}").as_bytes());
    format!("cmd-{}", &digest[..48])
}

fn hex_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for value in digest {
        write!(&mut encoded, "{value:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "arbitrage-idempotency-{name}-{}-{}",
            std::process::id(),
            Uuid::new_v4()
        ))
    }

    #[test]
    fn sequential_duplicate_returns_completed_record() {
        let path = temp_store("sequential");
        let store = IdempotencyStore::open(path.clone(), 3600).unwrap();
        let fingerprint = fingerprint("reload_strategy", r#"{"reason":"test"}"#);
        let mut record = match store
            .claim("request-a", "reload_strategy", &fingerprint)
            .unwrap()
        {
            ClaimOutcome::New(record) => record,
            other => panic!("unexpected claim outcome: {other:?}"),
        };
        store
            .complete_success(
                &mut record,
                CachedCommandReply {
                    accepted: true,
                    command: "reload_strategy".to_string(),
                    request_id: "request-a".to_string(),
                    detail: "ok".to_string(),
                    applied_at_ms: 123,
                },
            )
            .unwrap();

        match store
            .claim("request-a", "reload_strategy", &fingerprint)
            .unwrap()
        {
            ClaimOutcome::Completed(cached) => {
                assert_eq!(cached.response.unwrap().applied_at_ms, 123);
            }
            other => panic!("unexpected duplicate outcome: {other:?}"),
        }
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn same_request_id_with_different_payload_conflicts() {
        let path = temp_store("conflict");
        let store = IdempotencyStore::open(path.clone(), 3600).unwrap();
        let first = fingerprint("update_limits", r#"{"max_trade_size":"500"}"#);
        let second = fingerprint("update_limits", r#"{"max_trade_size":"5000"}"#);
        assert!(matches!(
            store.claim("request-a", "update_limits", &first).unwrap(),
            ClaimOutcome::New(_)
        ));
        assert!(matches!(
            store.claim("request-a", "update_limits", &second).unwrap(),
            ClaimOutcome::Conflict { .. }
        ));
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn in_progress_survives_store_restart_and_is_not_pruned() {
        let path = temp_store("restart");
        let fingerprint = fingerprint("start_trading", r#"{"reason":"test"}"#);
        {
            let store = IdempotencyStore::open(path.clone(), 1).unwrap();
            assert!(matches!(
                store.claim("request-a", "start_trading", &fingerprint).unwrap(),
                ClaimOutcome::New(_)
            ));
        }
        std::thread::sleep(Duration::from_millis(5));
        let reopened = IdempotencyStore::open(path.clone(), 1).unwrap();
        assert!(matches!(
            reopened
                .claim("request-a", "start_trading", &fingerprint)
                .unwrap(),
            ClaimOutcome::InProgress(_)
        ));
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn concurrent_claim_has_one_winner() {
        let path = temp_store("concurrent");
        let store = std::sync::Arc::new(IdempotencyStore::open(path.clone(), 3600).unwrap());
        let fingerprint = fingerprint("stop_trading", r#"{"reason":"test"}"#);
        let mut handles = Vec::new();
        for _ in 0..8 {
            let store = store.clone();
            let fingerprint = fingerprint.clone();
            handles.push(std::thread::spawn(move || {
                store.claim("request-a", "stop_trading", &fingerprint).unwrap()
            }));
        }
        let outcomes = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, ClaimOutcome::New(_)))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, ClaimOutcome::InProgress(_)))
                .count(),
            7
        );
        let _ = fs::remove_dir_all(path);
    }
}
