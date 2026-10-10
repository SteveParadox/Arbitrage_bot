use std::{
    collections::hash_map::DefaultHasher,
    env, fmt,
    fs::{self, OpenOptions},
    hash::{Hash, Hasher},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError},
        Arc, Mutex, OnceLock,
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
static OUTBOX_SEQUENCE: AtomicU64 = AtomicU64::new(0);
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tracing::{error, warn};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineEvent {
    pub event_id: String,
    pub event_type: String,
    pub occurred_at_ms: u64,
    pub source: String,
    pub schema_version: u32,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventPipelineHealth {
    pub event_pipeline_status: String,
    pub event_queue_depth: usize,
    pub critical_events_pending: usize,
    pub event_publish_failures_total: u64,
    pub event_publish_retries_total: u64,
    pub event_publish_success_total: u64,
    pub event_queue_full_total: u64,
    pub event_outbox_pending: usize,
    pub oldest_pending_event_age_ms: Option<u64>,
    pub best_effort_dropped_total: u64,
    pub publisher_alive: bool,
    pub redis_connected: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct EventPublishError {
    detail: String,
}

impl EventPublishError {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl fmt::Display for EventPublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for EventPublishError {}

#[derive(Clone)]
pub struct EventPublisher {
    sender: SyncSender<PublisherMessage>,
    source: String,
    outbox: Arc<Outbox>,
    metrics: Arc<Metrics>,
    max_pending: usize,
}

#[derive(Debug)]
enum PublisherMessage {
    CriticalWake,
    Telemetry(EngineEvent),
}

#[derive(Debug, Clone, PartialEq)]
struct PublisherConfig {
    redis_url: String,
    stream: String,
    maxlen: usize,
    capacity: usize,
    outbox_path: PathBuf,
    retry_initial_ms: u64,
    retry_max_ms: u64,
    max_pending: usize,
}

#[derive(Debug)]
struct Metrics {
    queue_depth: AtomicUsize,
    publish_failures: AtomicU64,
    publish_retries: AtomicU64,
    publish_success: AtomicU64,
    queue_full: AtomicU64,
    best_effort_dropped: AtomicU64,
    publisher_alive: AtomicBool,
    outbox_available: AtomicBool,
    redis_known: AtomicBool,
    redis_connected: AtomicBool,
}

impl Metrics {
    fn new() -> Self {
        Self {
            queue_depth: AtomicUsize::new(0),
            publish_failures: AtomicU64::new(0),
            publish_retries: AtomicU64::new(0),
            publish_success: AtomicU64::new(0),
            queue_full: AtomicU64::new(0),
            best_effort_dropped: AtomicU64::new(0),
            publisher_alive: AtomicBool::new(false),
            outbox_available: AtomicBool::new(true),
            redis_known: AtomicBool::new(false),
            redis_connected: AtomicBool::new(false),
        }
    }
}

#[derive(Debug)]
struct Outbox {
    pending_dir: PathBuf,
    receipts_dir: PathBuf,
    write_lock: Mutex<()>,
    _instance_lock: fs::File,
    pending_count: AtomicUsize,
}

impl Outbox {
    fn open(path: PathBuf) -> Result<Self, EventPublishError> {
        fs::create_dir_all(&path).map_err(|error| {
            EventPublishError::new(format!(
                "failed to create event outbox {}: {error}",
                path.display()
            ))
        })?;
        let instance_lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.join(".publisher.lock"))
            .map_err(|e| EventPublishError::new(e.to_string()))?;
        instance_lock
            .try_lock_exclusive()
            .map_err(|e| EventPublishError::new(format!("outbox already in use: {e}")))?;
        let receipts_dir = path.join("receipts");
        fs::create_dir_all(&receipts_dir).map_err(|e| EventPublishError::new(e.to_string()))?;
        let pending_count = count_pending_files(&path).map_err(|error| {
            EventPublishError::new(format!(
                "failed to inspect event outbox {}: {error}",
                path.display()
            ))
        })?;
        Ok(Self {
            pending_dir: path,
            receipts_dir,
            write_lock: Mutex::new(()),
            _instance_lock: instance_lock,
            pending_count: AtomicUsize::new(pending_count),
        })
    }

    fn persist(&self, event: &EngineEvent) -> Result<PathBuf, EventPublishError> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| EventPublishError::new("outbox lock poisoned"))?;
        let hash = format!("{:x}", Sha256::digest(event.event_id.as_bytes()));
        let ordinal = OUTBOX_SEQUENCE.fetch_add(1, Ordering::SeqCst);
        let sequence = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let final_path = self
            .pending_dir
            .join(format!("{sequence:030}-{ordinal:020}-{hash}.json"));
        let receipt = self.receipts_dir.join(format!("{hash}.json"));
        let previous = if receipt.exists() {
            Some(receipt)
        } else {
            // A crash after fsync but before Redis delivery must also deduplicate.
            fs::read_dir(&self.pending_dir)
                .map_err(|e| EventPublishError::new(e.to_string()))?
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .find(|path| {
                    path.file_name()
                        .and_then(|v| v.to_str())
                        .is_some_and(|v| v.ends_with(&format!("-{hash}.json")))
                })
        };
        if let Some(path) = previous {
            let existing = self.read_event(&path)?;
            if canonical_event(&existing) != canonical_event(event) {
                return Err(EventPublishError::new(
                    "conflicting critical event_id in durable outbox",
                ));
            }
            return Ok(path);
        }
        let temp_path = self
            .pending_dir
            .join(format!(".tmp-{}.json", Uuid::new_v4()));
        let bytes = serde_json::to_vec(event).map_err(|error| {
            EventPublishError::new(format!(
                "failed to serialize critical event {}: {error}",
                event.event_id
            ))
        })?;

        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .map_err(|error| {
                EventPublishError::new(format!(
                    "failed to create event outbox temp file {}: {error}",
                    temp_path.display()
                ))
            })?;
        file.write_all(&bytes).map_err(|error| {
            EventPublishError::new(format!(
                "failed to write critical event {} to outbox: {error}",
                event.event_id
            ))
        })?;
        file.sync_all().map_err(|error| {
            EventPublishError::new(format!(
                "failed to sync critical event {} to outbox: {error}",
                event.event_id
            ))
        })?;
        fs::rename(&temp_path, &final_path).map_err(|error| {
            let _ = fs::remove_file(&temp_path);
            EventPublishError::new(format!(
                "failed to commit critical event {} to outbox: {error}",
                event.event_id
            ))
        })?;
        sync_directory(&self.pending_dir).map_err(|error| {
            EventPublishError::new(format!(
                "failed to sync event outbox directory {}: {error}",
                self.pending_dir.display()
            ))
        })?;
        self.pending_count.fetch_add(1, Ordering::SeqCst);
        Ok(final_path)
    }

    fn next_pending(&self) -> Result<Option<PathBuf>, EventPublishError> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| EventPublishError::new("outbox lock poisoned"))?;
        let mut entries = fs::read_dir(&self.pending_dir)
            .map_err(|error| {
                EventPublishError::new(format!(
                    "failed to read event outbox {}: {error}",
                    self.pending_dir.display()
                ))
            })?
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_type()
                    .map(|value| value.is_file())
                    .unwrap_or(false)
                    && entry
                        .file_name()
                        .to_str()
                        .map(|name| name.ends_with(".json") && !name.starts_with(".tmp-"))
                        .unwrap_or(false)
            })
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        entries.sort();
        Ok(entries.into_iter().next())
    }

    fn read_event(&self, path: &Path) -> Result<EngineEvent, EventPublishError> {
        let raw = fs::read(path).map_err(|error| {
            EventPublishError::new(format!(
                "failed to read pending event {}: {error}",
                path.display()
            ))
        })?;
        serde_json::from_slice(&raw).map_err(|error| {
            EventPublishError::new(format!(
                "failed to decode pending event {}: {error}",
                path.display()
            ))
        })
    }

    fn mark_delivered(&self, path: &Path) -> Result<(), EventPublishError> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| EventPublishError::new("outbox lock poisoned"))?;
        let event = self.read_event(path)?;
        let hash = format!("{:x}", Sha256::digest(event.event_id.as_bytes()));
        fs::rename(path, self.receipts_dir.join(format!("{hash}.json"))).map_err(|error| {
            EventPublishError::new(format!(
                "failed to remove delivered event {}: {error}",
                path.display()
            ))
        })?;
        sync_directory(&self.receipts_dir)
            .and_then(|_| sync_directory(&self.pending_dir))
            .map_err(|error| EventPublishError::new(error.to_string()))?;
        let _ = self
            .pending_count
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                Some(value.saturating_sub(1))
            });
        Ok(())
    }

    fn pending_count(&self) -> usize {
        self.pending_count.load(Ordering::SeqCst)
    }

    fn oldest_pending_age_ms(&self) -> Option<u64> {
        let path = self.next_pending().ok().flatten()?;
        let event = self.read_event(&path).ok()?;
        Some(now_ms().saturating_sub(event.occurred_at_ms))
    }
}

impl EventPublisher {
    pub fn try_from_env(source: impl Into<String>) -> Result<Self, EventPublishError> {
        let source = source.into();
        let outbox_root = env::var("ARB_EVENT_OUTBOX_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("data/event-outbox"));
        let config = PublisherConfig {
            redis_url: env::var("ARB_REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string()),
            stream: env::var("ARB_EVENT_STREAM").unwrap_or_else(|_| "arb.events".to_string()),
            maxlen: env_usize("ARB_EVENT_STREAM_MAXLEN", 1_000_000),
            capacity: env_usize("ARB_EVENT_QUEUE_CAPACITY", 4_096),
            outbox_path: outbox_root.join(sanitize_source(&source)),
            retry_initial_ms: env_u64("ARB_EVENT_RETRY_INITIAL_MS", 100),
            retry_max_ms: env_u64("ARB_EVENT_RETRY_MAX_MS", 5_000),
            max_pending: env_usize("ARB_EVENT_MAX_PENDING", 100_000),
        };
        // Reuse one publisher per configured outbox in this process. Separate processes
        // remain excluded by the file lock; callers such as risk/coordinator can share it.
        type Cached = std::collections::HashMap<PathBuf, (PublisherConfig, EventPublisher)>;
        static PUBLISHERS: OnceLock<Mutex<Cached>> = OnceLock::new();
        let mut cache = PUBLISHERS
            .get_or_init(|| Mutex::new(Cached::new()))
            .lock()
            .map_err(|_| EventPublishError::new("publisher cache lock poisoned"))?;
        if let Some((existing, publisher)) = cache.get(&config.outbox_path) {
            if existing != &config {
                return Err(EventPublishError::new(
                    "conflicting publisher configuration",
                ));
            }
            return Ok(publisher.clone());
        }
        let publisher = Self::from_config(source, config.clone())?;
        cache.insert(config.outbox_path.clone(), (config, publisher.clone()));
        Ok(publisher)
    }

    pub fn from_env(source: impl Into<String>) -> Self {
        Self::try_from_env(source)
            .expect("critical event outbox and Redis publisher configuration must initialize")
    }

    fn from_config(source: String, config: PublisherConfig) -> Result<Self, EventPublishError> {
        if config.retry_initial_ms == 0
            || config.retry_max_ms == 0
            || config.retry_initial_ms > config.retry_max_ms
        {
            return Err(EventPublishError::new(
                "event retry delays must be positive and initial <= maximum",
            ));
        }
        if config.max_pending == 0 {
            return Err(EventPublishError::new(
                "ARB_EVENT_MAX_PENDING must be greater than zero",
            ));
        }

        let redis_client = redis::Client::open(config.redis_url.as_str()).map_err(|error| {
            EventPublishError::new(format!(
                "invalid ARB_REDIS_URL for event publisher: {error}"
            ))
        })?;
        let outbox = Arc::new(Outbox::open(config.outbox_path.clone())?);
        let metrics = Arc::new(Metrics::new());
        let (sender, receiver) = sync_channel::<PublisherMessage>(config.capacity);

        let worker_outbox = outbox.clone();
        let worker_metrics = metrics.clone();
        let worker_config = config.clone();
        metrics.publisher_alive.store(true, Ordering::SeqCst);
        let spawn_result = thread::Builder::new()
            .name(format!("event-bus-{source}"))
            .spawn(move || {
                publisher_loop(
                    redis_client,
                    worker_config,
                    worker_outbox,
                    worker_metrics.clone(),
                    receiver,
                );
                worker_metrics
                    .publisher_alive
                    .store(false, Ordering::SeqCst);
            });
        if let Err(error) = spawn_result {
            metrics.publisher_alive.store(false, Ordering::SeqCst);
            return Err(EventPublishError::new(format!(
                "failed to start event publisher thread: {error}"
            )));
        }

        Ok(Self {
            sender,
            source,
            outbox,
            metrics,
            max_pending: config.max_pending,
        })
    }

    pub fn publish(&self, event_type: &str, payload: Value) {
        if is_critical_event(event_type) {
            if let Err(error) = self.publish_critical(event_type, payload) {
                error!(
                    event_type,
                    error = %error,
                    "critical event could not be durably accepted"
                );
            }
        } else {
            self.publish_best_effort(event_type, payload);
        }
    }

    pub fn publish_critical(
        &self,
        event_type: &str,
        payload: Value,
    ) -> Result<String, EventPublishError> {
        self.publish_critical_with_id(Uuid::new_v4().to_string(), event_type, payload)
    }

    pub fn publish_critical_with_id(
        &self,
        event_id: String,
        event_type: &str,
        payload: Value,
    ) -> Result<String, EventPublishError> {
        self.publish_critical_at(event_id, event_type, now_ms(), payload)
    }

    /// The caller supplies immutable market time for deterministic replayable events.
    pub fn publish_critical_at(
        &self,
        event_id: String,
        event_type: &str,
        occurred_at_ms: u64,
        payload: Value,
    ) -> Result<String, EventPublishError> {
        if event_id.trim().is_empty() || event_id.len() > 64 || !event_id.is_ascii() {
            return Err(EventPublishError::new(
                "critical event_id must be non-empty ASCII text no longer than 64 bytes",
            ));
        }
        let event = EngineEvent {
            event_id,
            event_type: event_type.to_string(),
            occurred_at_ms,
            source: self.source.clone(),
            schema_version: 1,
            payload,
        };
        if let Err(error) = self.outbox.persist(&event) {
            self.metrics.outbox_available.store(false, Ordering::SeqCst);
            return Err(error);
        }
        self.metrics.outbox_available.store(true, Ordering::SeqCst);

        match self.sender.try_send(PublisherMessage::CriticalWake) {
            Ok(()) => {
                self.metrics.queue_depth.fetch_add(1, Ordering::SeqCst);
            }
            Err(TrySendError::Full(_)) => {
                self.metrics.queue_full.fetch_add(1, Ordering::SeqCst);
                warn!(
                    event_id = %event.event_id,
                    event_type = %event.event_type,
                    "event publisher queue full; critical event remains durable in outbox"
                );
            }
            Err(TrySendError::Disconnected(_)) => {
                self.metrics.publisher_alive.store(false, Ordering::SeqCst);
                warn!(
                    event_id = %event.event_id,
                    event_type = %event.event_type,
                    "event publisher disconnected; critical event remains durable in outbox"
                );
            }
        }
        Ok(event.event_id)
    }

    pub fn publish_best_effort(&self, event_type: &str, payload: Value) {
        let event = self.new_event(event_type, payload);
        match self.sender.try_send(PublisherMessage::Telemetry(event)) {
            Ok(()) => {
                self.metrics.queue_depth.fetch_add(1, Ordering::SeqCst);
            }
            Err(TrySendError::Full(PublisherMessage::Telemetry(event))) => {
                self.metrics.queue_full.fetch_add(1, Ordering::SeqCst);
                self.metrics
                    .best_effort_dropped
                    .fetch_add(1, Ordering::SeqCst);
                warn!(
                    event_id = %event.event_id,
                    event_type = %event.event_type,
                    "event publisher queue full; dropping best-effort telemetry"
                );
            }
            Err(TrySendError::Disconnected(PublisherMessage::Telemetry(event))) => {
                self.metrics.publisher_alive.store(false, Ordering::SeqCst);
                self.metrics
                    .best_effort_dropped
                    .fetch_add(1, Ordering::SeqCst);
                warn!(
                    event_id = %event.event_id,
                    event_type = %event.event_type,
                    "event publisher disconnected; dropping best-effort telemetry"
                );
            }
            Err(TrySendError::Full(PublisherMessage::CriticalWake))
            | Err(TrySendError::Disconnected(PublisherMessage::CriticalWake)) => {
                unreachable!("best-effort publishing never sends a critical wake")
            }
        }
    }

    pub fn ensure_critical_ready(&self) -> Result<(), EventPublishError> {
        if !self.metrics.outbox_available.load(Ordering::SeqCst) {
            return Err(EventPublishError::new(
                "critical event outbox is unavailable",
            ));
        }
        if !self.metrics.publisher_alive.load(Ordering::SeqCst) {
            return Err(EventPublishError::new(
                "critical event publisher is not running",
            ));
        }
        let pending = self.outbox.pending_count();
        if pending >= self.max_pending {
            return Err(EventPublishError::new(format!(
                "critical event backlog {pending} reached safety limit {}",
                self.max_pending
            )));
        }
        Ok(())
    }

    pub fn health_snapshot(&self) -> EventPipelineHealth {
        let pending = self.outbox.pending_count();
        let alive = self.metrics.publisher_alive.load(Ordering::SeqCst);
        let outbox_available = self.metrics.outbox_available.load(Ordering::SeqCst);
        let redis_known = self.metrics.redis_known.load(Ordering::SeqCst);
        let redis_connected = self.metrics.redis_connected.load(Ordering::SeqCst);
        let status = if !alive || !outbox_available || pending >= self.max_pending {
            "unhealthy"
        } else if pending > 0 || (redis_known && !redis_connected) {
            "degraded"
        } else {
            "healthy"
        };

        EventPipelineHealth {
            event_pipeline_status: status.to_string(),
            event_queue_depth: self.metrics.queue_depth.load(Ordering::SeqCst),
            critical_events_pending: pending,
            event_publish_failures_total: self.metrics.publish_failures.load(Ordering::SeqCst),
            event_publish_retries_total: self.metrics.publish_retries.load(Ordering::SeqCst),
            event_publish_success_total: self.metrics.publish_success.load(Ordering::SeqCst),
            event_queue_full_total: self.metrics.queue_full.load(Ordering::SeqCst),
            event_outbox_pending: pending,
            oldest_pending_event_age_ms: self.outbox.oldest_pending_age_ms(),
            best_effort_dropped_total: self.metrics.best_effort_dropped.load(Ordering::SeqCst),
            publisher_alive: alive,
            redis_connected: redis_known.then_some(redis_connected),
        }
    }

    fn new_event(&self, event_type: &str, payload: Value) -> EngineEvent {
        EngineEvent {
            event_id: Uuid::new_v4().to_string(),
            event_type: event_type.to_string(),
            occurred_at_ms: now_ms(),
            source: self.source.clone(),
            schema_version: 1,
            payload,
        }
    }

    #[cfg(test)]
    fn without_worker(
        source: String,
        outbox_path: PathBuf,
        capacity: usize,
        max_pending: usize,
    ) -> Result<(Self, Receiver<PublisherMessage>), EventPublishError> {
        let outbox = Arc::new(Outbox::open(outbox_path)?);
        let metrics = Arc::new(Metrics::new());
        metrics.publisher_alive.store(true, Ordering::SeqCst);
        let (sender, receiver) = sync_channel(capacity);
        Ok((
            Self {
                sender,
                source,
                outbox,
                metrics,
                max_pending,
            },
            receiver,
        ))
    }
}

fn publisher_loop(
    client: redis::Client,
    config: PublisherConfig,
    outbox: Arc<Outbox>,
    metrics: Arc<Metrics>,
    receiver: Receiver<PublisherMessage>,
) {
    let mut connection = None;

    loop {
        match outbox.next_pending() {
            Ok(Some(path)) => {
                let event = match outbox.read_event(&path) {
                    Ok(event) => event,
                    Err(error) => {
                        metrics.outbox_available.store(false, Ordering::SeqCst);
                        error!(error = %error, "critical event outbox contains unreadable data");
                        thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                };
                metrics.outbox_available.store(true, Ordering::SeqCst);
                let mut attempt = 1_u32;
                loop {
                    match publish_to_redis(&client, &mut connection, &config, &event) {
                        Ok(()) => {
                            metrics.redis_known.store(true, Ordering::SeqCst);
                            metrics.redis_connected.store(true, Ordering::SeqCst);
                            metrics.publish_success.fetch_add(1, Ordering::SeqCst);
                            if let Err(error) = outbox.mark_delivered(&path) {
                                metrics.outbox_available.store(false, Ordering::SeqCst);
                                error!(
                                    event_id = %event.event_id,
                                    event_type = %event.event_type,
                                    error = %error,
                                    "Redis confirmed critical event but outbox delivery marker failed; event will be safely redelivered"
                                );
                                thread::sleep(Duration::from_secs(1));
                            }
                            break;
                        }
                        Err(error) => {
                            metrics.redis_known.store(true, Ordering::SeqCst);
                            metrics.redis_connected.store(false, Ordering::SeqCst);
                            metrics.publish_failures.fetch_add(1, Ordering::SeqCst);
                            metrics.publish_retries.fetch_add(1, Ordering::SeqCst);
                            let delay = retry_delay(
                                config.retry_initial_ms,
                                config.retry_max_ms,
                                &event.event_id,
                                attempt,
                            );
                            warn!(
                                event_id = %event.event_id,
                                event_type = %event.event_type,
                                publish_attempt = attempt,
                                retry_delay_ms = delay.as_millis() as u64,
                                pending_duration_ms = now_ms().saturating_sub(event.occurred_at_ms),
                                redis_error = %error,
                                delivery_status = "pending",
                                "critical event publish failed; retrying from durable outbox"
                            );
                            connection = None;
                            thread::sleep(delay);
                            attempt = attempt.saturating_add(1);
                        }
                    }
                }
                continue;
            }
            Ok(None) => {}
            Err(error) => {
                metrics.outbox_available.store(false, Ordering::SeqCst);
                error!(error = %error, "critical event outbox scan failed");
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        }

        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(PublisherMessage::CriticalWake) => {
                decrement_queue_depth(&metrics);
            }
            Ok(PublisherMessage::Telemetry(event)) => {
                decrement_queue_depth(&metrics);
                match publish_to_redis(&client, &mut connection, &config, &event) {
                    Ok(()) => {
                        metrics.redis_known.store(true, Ordering::SeqCst);
                        metrics.redis_connected.store(true, Ordering::SeqCst);
                        metrics.publish_success.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(error) => {
                        metrics.redis_known.store(true, Ordering::SeqCst);
                        metrics.redis_connected.store(false, Ordering::SeqCst);
                        metrics.publish_failures.fetch_add(1, Ordering::SeqCst);
                        metrics.best_effort_dropped.fetch_add(1, Ordering::SeqCst);
                        connection = None;
                        warn!(
                            event_id = %event.event_id,
                            event_type = %event.event_type,
                            redis_error = %error,
                            delivery_status = "dropped_best_effort",
                            "best-effort telemetry publish failed"
                        );
                    }
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                if outbox.pending_count() == 0 {
                    break;
                }
            }
        }
    }
}

fn publish_to_redis(
    client: &redis::Client,
    connection: &mut Option<redis::Connection>,
    config: &PublisherConfig,
    event: &EngineEvent,
) -> redis::RedisResult<()> {
    if connection.is_none() {
        *connection = Some(client.get_connection()?);
    }
    let payload = event.payload.to_string();
    // Never trim an unacknowledged critical record, even when telemetry shares
    // the stream. Full streams push back into the durable outbox until consumers
    // advance. MINID trimming is bounded by every group's oldest pending/delivery ID.
    let script = redis::Script::new(
        r#"
        local function less(a,b)
            local am,as = string.match(a, '(%d+)%-(%d+)')
            local bm,bs = string.match(b, '(%d+)%-(%d+)')
            if #am ~= #bm then return #am < #bm end
            if am ~= bm then return am < bm end
            if #as ~= #bs then return #as < #bs end
            return as < bs
        end
        if redis.call('XLEN',KEYS[1]) >= tonumber(ARGV[1]) then
            local groups = redis.call('XINFO','GROUPS',KEYS[1])
            local cutoff = nil
            for _,fields in ipairs(groups) do
                local group, delivered
                for i=1,#fields,2 do
                    if fields[i] == 'name' then group = fields[i+1] end
                    if fields[i] == 'last-delivered-id' then delivered = fields[i+1] end
                end
                local pending = redis.call('XPENDING',KEYS[1],group)
                local safe = delivered
                if pending[1] > 0 and less(pending[2],safe) then safe = pending[2] end
                if cutoff == nil or less(safe,cutoff) then cutoff = safe end
            end
            if cutoff ~= nil then redis.call('XTRIM',KEYS[1],'MINID',cutoff) end
            if redis.call('XLEN',KEYS[1]) >= tonumber(ARGV[1]) then
                return redis.error_reply('event stream backlog capacity reached')
            end
        end
        return redis.call('XADD',KEYS[1],'*',unpack(ARGV,2))
    "#,
    );
    let result: redis::RedisResult<String> = script
        .key(&config.stream)
        .arg(config.maxlen)
        .arg("event_id")
        .arg(&event.event_id)
        .arg("event_type")
        .arg(&event.event_type)
        .arg("occurred_at_ms")
        .arg(event.occurred_at_ms)
        .arg("source")
        .arg(&event.source)
        .arg("schema_version")
        .arg(event.schema_version)
        .arg("payload")
        .arg(payload)
        .invoke(connection.as_mut().expect("connection is initialized"));
    result.map(|_| ())
}

fn retry_delay(initial_ms: u64, max_ms: u64, event_id: &str, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(16);
    let base = initial_ms.saturating_mul(1_u64 << exponent).min(max_ms);
    let jitter_window = (base / 5).max(1);
    let mut hasher = DefaultHasher::new();
    event_id.hash(&mut hasher);
    attempt.hash(&mut hasher);
    let jitter = hasher.finish() % jitter_window;
    Duration::from_millis(base.saturating_add(jitter).min(max_ms))
}

fn decrement_queue_depth(metrics: &Metrics) {
    let _ = metrics
        .queue_depth
        .try_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
            Some(value.saturating_sub(1))
        });
}

fn is_critical_event(event_type: &str) -> bool {
    event_type.starts_with("trade.")
        || event_type.starts_with("order.")
        || event_type == "balance.updated"
        || event_type.starts_with("risk.")
        || event_type == "engine.state_changed"
        || event_type.starts_with("audit.")
        || event_type.starts_with("security.")
        || event_type.starts_with("execution.")
}

fn count_pending_files(path: &Path) -> io::Result<usize> {
    Ok(fs::read_dir(path)?
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_type()
                .map(|value| value.is_file())
                .unwrap_or(false)
                && entry
                    .file_name()
                    .to_str()
                    .map(|name| name.ends_with(".json") && !name.starts_with(".tmp-"))
                    .unwrap_or(false)
        })
        .count())
}

fn sanitize_source(source: &str) -> String {
    source
        .chars()
        .map(|value| {
            if value.is_ascii_alphanumeric() || matches!(value, '-' | '_') {
                value
            } else {
                '_'
            }
        })
        .collect()
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        fs::File::open(path)?.sync_all()
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

fn canonical_event(event: &EngineEvent) -> Value {
    let mut value = serde_json::to_value(event).expect("validated event");
    if event.source == "engine-service"
        && event.payload["identity_version"] == 2
        && event.payload["candidate_id"] == event.event_id
        && matches!(
            event.event_type.as_str(),
            "opportunity.detected" | "opportunity.rejected"
        )
    {
        if let Some(payload) = value["payload"].as_object_mut() {
            // Only this explicit processing field is mutable. Market timestamps stay strict.
            if payload.get("processing").is_some_and(|p| {
                p.as_object().is_some_and(|p| {
                    p.len() == 1 && p.get("processed_at_ms").is_some_and(Value::is_u64)
                })
            }) {
                payload.remove("processing");
            }
        }
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_outbox(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "arbitrage-event-bus-{name}-{}-{}",
            std::process::id(),
            Uuid::new_v4()
        ))
    }

    #[test]
    fn queue_full_never_drops_critical_event() {
        let path = temp_outbox("queue-full");
        let (publisher, _receiver) =
            EventPublisher::without_worker("test".to_string(), path.clone(), 1, 100).unwrap();

        publisher.publish_best_effort("opportunity.detected", Value::Null);
        let event_id = publisher
            .publish_critical("trade.executed", Value::Null)
            .unwrap();

        assert_eq!(publisher.outbox.pending_count(), 1);
        let pending_path = publisher.outbox.next_pending().unwrap().unwrap();
        let persisted = publisher.outbox.read_event(&pending_path).unwrap();
        assert_eq!(persisted.event_id, event_id);
        assert_eq!(persisted.event_type, "trade.executed");
        assert_eq!(publisher.metrics.queue_full.load(Ordering::SeqCst), 1);
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn disconnected_queue_never_drops_critical_event() {
        let path = temp_outbox("queue-closed");
        let (publisher, receiver) =
            EventPublisher::without_worker("test".to_string(), path.clone(), 1, 100).unwrap();
        drop(receiver);

        let event_id = publisher
            .publish_critical("order.executed", Value::Null)
            .unwrap();

        assert_eq!(publisher.outbox.pending_count(), 1);
        let pending_path = publisher.outbox.next_pending().unwrap().unwrap();
        assert_eq!(
            publisher.outbox.read_event(&pending_path).unwrap().event_id,
            event_id
        );
        assert!(!publisher.metrics.publisher_alive.load(Ordering::SeqCst));
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn high_volume_critical_burst_is_disk_backed_not_memory_bounded() {
        let path = temp_outbox("burst");
        let (publisher, _receiver) =
            EventPublisher::without_worker("test".to_string(), path.clone(), 1, 2_000).unwrap();

        for index in 0..1_000 {
            publisher
                .publish_critical("trade.failed", serde_json::json!({"index": index}))
                .unwrap();
        }

        assert_eq!(publisher.outbox.pending_count(), 1_000);
        assert!(publisher.metrics.queue_full.load(Ordering::SeqCst) > 0);
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn retry_delay_is_bounded_and_deterministic() {
        let first = retry_delay(100, 5_000, "event-1", 1);
        let first_again = retry_delay(100, 5_000, "event-1", 1);
        let late = retry_delay(100, 5_000, "event-1", 30);

        assert_eq!(first, first_again);
        assert!(first >= Duration::from_millis(100));
        assert!(late <= Duration::from_millis(5_000));
    }

    #[test]
    #[ignore = "requires Redis integration service"]
    fn redis_pause_preserves_critical_event_and_recovers() {
        let redis_url =
            env::var("ARB_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
        let stream = format!("arb.events.test.pause.{}", Uuid::new_v4());
        let path = temp_outbox("redis-pause");

        let client = redis::Client::open(redis_url.clone()).unwrap();
        let mut connection = client.get_connection().unwrap();
        let _: String = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(600)
            .arg("ALL")
            .query(&mut connection)
            .unwrap();

        let publisher = EventPublisher::from_config(
            "test-pause".to_string(),
            PublisherConfig {
                redis_url: redis_url.clone(),
                stream: stream.clone(),
                maxlen: 10_000,
                capacity: 1,
                outbox_path: path.clone(),
                retry_initial_ms: 25,
                retry_max_ms: 100,
                max_pending: 100,
            },
        )
        .unwrap();

        let event_id = publisher
            .publish_critical(
                "trade.executed",
                serde_json::json!({"trade_id": "pause-test"}),
            )
            .unwrap();

        thread::sleep(Duration::from_millis(100));
        assert_eq!(publisher.outbox.pending_count(), 1);

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while publisher.outbox.pending_count() != 0 && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(25));
        }
        assert_eq!(publisher.outbox.pending_count(), 0);

        let rows: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
            .arg(&stream)
            .arg("-")
            .arg("+")
            .query(&mut connection)
            .unwrap();
        let delivered = rows
            .iter()
            .filter_map(|(_, fields)| {
                fields
                    .iter()
                    .find(|(key, _)| key == "event_id")
                    .map(|(_, value)| value.clone())
            })
            .collect::<Vec<_>>();
        assert_eq!(delivered, vec![event_id]);

        let _: redis::RedisResult<i64> = redis::cmd("DEL").arg(&stream).query(&mut connection);
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    #[ignore = "requires Redis integration service"]
    fn restart_recovery_delivers_same_event_ids_to_redis_when_enabled() {
        let redis_url =
            env::var("ARB_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
        let stream = format!("arb.events.test.{}", Uuid::new_v4());
        let path = temp_outbox("redis-recovery");
        let outbox = Outbox::open(path.clone()).unwrap();
        let expected = (0..3)
            .map(|index| EngineEvent {
                event_id: Uuid::new_v4().to_string(),
                event_type: "trade.executed".to_string(),
                occurred_at_ms: now_ms().saturating_add(index),
                source: "test".to_string(),
                schema_version: 1,
                payload: serde_json::json!({"index": index}),
            })
            .collect::<Vec<_>>();

        for event in &expected {
            outbox.persist(event).unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        drop(outbox);

        let publisher = EventPublisher::from_config(
            "test".to_string(),
            PublisherConfig {
                redis_url: redis_url.clone(),
                stream: stream.clone(),
                maxlen: 10_000,
                capacity: 1,
                outbox_path: path.clone(),
                retry_initial_ms: 25,
                retry_max_ms: 100,
                max_pending: 100,
            },
        )
        .unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while publisher.outbox.pending_count() != 0 && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(25));
        }
        assert_eq!(publisher.outbox.pending_count(), 0);

        let client = redis::Client::open(redis_url).unwrap();
        let mut connection = client.get_connection().unwrap();
        let rows: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
            .arg(&stream)
            .arg("-")
            .arg("+")
            .query(&mut connection)
            .unwrap();
        let delivered = rows
            .iter()
            .filter_map(|(_, fields)| {
                fields
                    .iter()
                    .find(|(key, _)| key == "event_id")
                    .map(|(_, value)| value.clone())
            })
            .collect::<Vec<_>>();
        let expected_ids = expected
            .iter()
            .map(|event| event.event_id.clone())
            .collect::<Vec<_>>();
        assert_eq!(delivered, expected_ids);

        let _: redis::RedisResult<i64> = redis::cmd("DEL").arg(&stream).query(&mut connection);
        let _ = fs::remove_dir_all(path);
    }
    #[test]
    fn deterministic_events_deduplicate_pending_delivered_and_restart() {
        let path = temp_outbox("canonical");
        let event = EngineEvent {
            event_id: "same".into(),
            event_type: "opportunity.detected".into(),
            occurred_at_ms: 1000,
            source: "engine-service".into(),
            schema_version: 1,
            payload: serde_json::json!({"identity_version":2,"candidate_id":"same",
                "scan_timestamp":1000,"price":100.,"processing":{"processed_at_ms":1100}}),
        };
        let outbox = Outbox::open(path.clone()).unwrap();
        let pending = outbox.persist(&event).unwrap();
        let mut retry = event.clone();
        retry.payload["processing"]["processed_at_ms"] = Value::from(1200);
        assert_eq!(outbox.persist(&retry).unwrap(), pending);
        assert_eq!(outbox.pending_count(), 1);
        let mut conflict = retry.clone();
        conflict.payload["price"] = Value::from(101);
        assert!(outbox.persist(&conflict).is_err());
        outbox.mark_delivered(&pending).unwrap();
        assert_eq!(outbox.pending_count(), 0);
        drop(outbox);
        let reopened = Outbox::open(path.clone()).unwrap();
        reopened.persist(&retry).unwrap();
        assert_eq!(reopened.pending_count(), 0);
        assert!(reopened.persist(&conflict).is_err());
        drop(reopened);
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn concurrent_duplicate_outbox_writers_accept_once_and_fail_on_conflict() {
        let path = temp_outbox("concurrent");
        let outbox = Arc::new(Outbox::open(path.clone()).unwrap());
        let mut threads = Vec::new();
        for _ in 0..8 {
            let outbox = outbox.clone();
            threads.push(thread::spawn(move || {
                outbox
                    .persist(&EngineEvent {
                        event_id: "duplicate".into(),
                        event_type: "trade.executed".into(),
                        occurred_at_ms: 1000,
                        source: "test".into(),
                        schema_version: 1,
                        payload: Value::Null,
                    })
                    .unwrap();
            }));
        }
        for handle in threads {
            handle.join().unwrap();
        }
        assert_eq!(outbox.pending_count(), 1);
        assert!(Outbox::open(path.clone()).is_err());
        drop(outbox);
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn outbox_write_failure_never_reports_acceptance() {
        let path = temp_outbox("write-failure");
        let outbox = Outbox::open(path.clone()).unwrap();
        fs::remove_dir_all(&path).unwrap();
        assert!(outbox
            .persist(&EngineEvent {
                event_id: "fail".into(),
                event_type: "audit.test".into(),
                occurred_at_ms: 1000,
                source: "test".into(),
                schema_version: 1,
                payload: Value::Null
            })
            .is_err());
        assert_eq!(outbox.pending_count(), 0);
    }
    #[test]
    #[ignore = "requires Redis integration service"]
    fn redis_backlog_never_trims_pending_critical_events() {
        let redis_url = env::var("ARB_REDIS_URL").unwrap();
        let client = redis::Client::open(redis_url.clone()).unwrap();
        let mut connection = client.get_connection().unwrap();
        let stream = format!("arb.capacity.{}", Uuid::new_v4());
        let _: String = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&stream)
            .arg("test")
            .arg("0")
            .arg("MKSTREAM")
            .query(&mut connection)
            .unwrap();
        let path = temp_outbox("backpressure");
        let publisher = EventPublisher::from_config(
            "test".into(),
            PublisherConfig {
                redis_url,
                stream: stream.clone(),
                maxlen: 2,
                capacity: 4,
                outbox_path: path.clone(),
                retry_initial_ms: 10,
                retry_max_ms: 20,
                max_pending: 100,
            },
        )
        .unwrap();
        for index in 0..3 {
            publisher
                .publish_critical("trade.executed", serde_json::json!({"index":index}))
                .unwrap();
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let length: usize = redis::cmd("XLEN")
                .arg(&stream)
                .query(&mut connection)
                .unwrap();
            if length == 2 && publisher.outbox.pending_count() == 1 {
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            thread::sleep(Duration::from_millis(10));
        }
        let _: redis::Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg("test")
            .arg("worker")
            .arg("STREAMS")
            .arg(&stream)
            .arg(">")
            .query(&mut connection)
            .unwrap();
        thread::sleep(Duration::from_millis(50));
        let rows: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
            .arg(&stream)
            .arg("-")
            .arg("+")
            .query(&mut connection)
            .unwrap();
        assert_eq!(rows.len(), 2);
        for (id, _) in rows {
            let _: usize = redis::cmd("XACK")
                .arg(&stream)
                .arg("test")
                .arg(id)
                .query(&mut connection)
                .unwrap();
        }
        while publisher.outbox.pending_count() != 0 {
            assert!(std::time::Instant::now() < deadline);
            thread::sleep(Duration::from_millis(10));
        }
        let length: usize = redis::cmd("XLEN")
            .arg(&stream)
            .query(&mut connection)
            .unwrap();
        assert_eq!(length, 2);
        let _: usize = redis::cmd("DEL")
            .arg(&stream)
            .query(&mut connection)
            .unwrap();
        drop(publisher);
        let _ = fs::remove_dir_all(path);
    }
}
