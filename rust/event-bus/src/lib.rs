use std::{
    collections::hash_map::DefaultHasher,
    env,
    fmt,
    fs::{self, File, OpenOptions},
    hash::{Hash, Hasher},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{
            sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError,
        },
        Arc,
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
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

#[derive(Debug, Clone)]
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
        let pending_count = count_pending_files(&path).map_err(|error| {
            EventPublishError::new(format!(
                "failed to inspect event outbox {}: {error}",
                path.display()
            ))
        })?;
        Ok(Self {
            pending_dir: path,
            pending_count: AtomicUsize::new(pending_count),
        })
    }

    fn persist(&self, event: &EngineEvent) -> Result<PathBuf, EventPublishError> {
        let sequence = now_ns();
        let file_name = format!(
            "{sequence:030}-{}.json",
            event.event_id.replace('-', "")
        );
        let final_path = self.pending_dir.join(file_name);
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
        let mut entries = fs::read_dir(&self.pending_dir)
            .map_err(|error| {
                EventPublishError::new(format!(
                    "failed to read event outbox {}: {error}",
                    self.pending_dir.display()
                ))
            })?
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.file_type().map(|value| value.is_file()).unwrap_or(false)
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
        fs::remove_file(path).map_err(|error| {
            EventPublishError::new(format!(
                "failed to remove delivered event {}: {error}",
                path.display()
            ))
        })?;
        sync_directory(&self.pending_dir).map_err(|error| {
            EventPublishError::new(format!(
                "failed to sync event outbox directory {}: {error}",
                self.pending_dir.display()
            ))
        })?;
        let _ = self.pending_count.fetch_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            |value| Some(value.saturating_sub(1)),
        );
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
            stream: env::var("ARB_EVENT_STREAM")
                .unwrap_or_else(|_| "arb.events".to_string()),
            maxlen: env_usize("ARB_EVENT_STREAM_MAXLEN", 1_000_000),
            capacity: env_usize("ARB_EVENT_QUEUE_CAPACITY", 4_096),
            outbox_path: outbox_root.join(sanitize_source(&source)),
            retry_initial_ms: env_u64("ARB_EVENT_RETRY_INITIAL_MS", 100),
            retry_max_ms: env_u64("ARB_EVENT_RETRY_MAX_MS", 5_000),
            max_pending: env_usize("ARB_EVENT_MAX_PENDING", 100_000),
        };
        Self::from_config(source, config)
    }

    pub fn from_env(source: impl Into<String>) -> Self {
        Self::try_from_env(source)
            .expect("critical event outbox and Redis publisher configuration must initialize")
    }

    fn from_config(
        source: String,
        config: PublisherConfig,
    ) -> Result<Self, EventPublishError> {
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

        let redis_client = redis::Client::open(config.redis_url.as_str())
            .map_err(|error| {
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
        let event = self.new_event(event_type, payload);
        if let Err(error) = self.outbox.persist(&event) {
            self.metrics
                .outbox_available
                .store(false, Ordering::SeqCst);
            return Err(error);
        }
        self.metrics
            .outbox_available
            .store(true, Ordering::SeqCst);

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
                self.metrics
                    .publisher_alive
                    .store(false, Ordering::SeqCst);
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
                self.metrics
                    .publisher_alive
                    .store(false, Ordering::SeqCst);
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
            event_publish_failures_total: self
                .metrics
                .publish_failures
                .load(Ordering::SeqCst),
            event_publish_retries_total: self
                .metrics
                .publish_retries
                .load(Ordering::SeqCst),
            event_publish_success_total: self
                .metrics
                .publish_success
                .load(Ordering::SeqCst),
            event_queue_full_total: self.metrics.queue_full.load(Ordering::SeqCst),
            event_outbox_pending: pending,
            oldest_pending_event_age_ms: self.outbox.oldest_pending_age_ms(),
            best_effort_dropped_total: self
                .metrics
                .best_effort_dropped
                .load(Ordering::SeqCst),
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
                    match publish_to_redis(
                        &client,
                        &mut connection,
                        &config,
                        &event,
                    ) {
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
                match publish_to_redis(
                    &client,
                    &mut connection,
                    &config,
                    &event,
                ) {
                    Ok(()) => {
                        metrics.redis_known.store(true, Ordering::SeqCst);
                        metrics.redis_connected.store(true, Ordering::SeqCst);
                        metrics.publish_success.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(error) => {
                        metrics.redis_known.store(true, Ordering::SeqCst);
                        metrics.redis_connected.store(false, Ordering::SeqCst);
                        metrics.publish_failures.fetch_add(1, Ordering::SeqCst);
                        metrics
                            .best_effort_dropped
                            .fetch_add(1, Ordering::SeqCst);
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
    let result: redis::RedisResult<String> = redis::cmd("XADD")
        .arg(&config.stream)
        .arg("MAXLEN")
        .arg("~")
        .arg(config.maxlen)
        .arg("*")
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
        .query(connection.as_mut().expect("connection is initialized"));
    result.map(|_| ())
}

fn retry_delay(
    initial_ms: u64,
    max_ms: u64,
    event_id: &str,
    attempt: u32,
) -> Duration {
    let exponent = attempt.saturating_sub(1).min(16);
    let base = initial_ms
        .saturating_mul(1_u64 << exponent)
        .min(max_ms);
    let jitter_window = (base / 5).max(1);
    let mut hasher = DefaultHasher::new();
    event_id.hash(&mut hasher);
    attempt.hash(&mut hasher);
    let jitter = hasher.finish() % jitter_window;
    Duration::from_millis(base.saturating_add(jitter).min(max_ms))
}

fn decrement_queue_depth(metrics: &Metrics) {
    let _ = metrics.queue_depth.fetch_update(
        Ordering::SeqCst,
        Ordering::SeqCst,
        |value| Some(value.saturating_sub(1)),
    );
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
            entry.file_type().map(|value| value.is_file()).unwrap_or(false)
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

fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
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
            EventPublisher::without_worker("test".to_string(), path.clone(), 1, 100)
                .unwrap();

        publisher.publish_best_effort("opportunity.detected", Value::Null);
        let event_id = publisher
            .publish_critical("trade.executed", Value::Null)
            .unwrap();

        assert_eq!(publisher.outbox.pending_count(), 1);
        let pending_path = publisher.outbox.next_pending().unwrap().unwrap();
        let persisted = publisher.outbox.read_event(&pending_path).unwrap();
        assert_eq!(persisted.event_id, event_id);
        assert_eq!(persisted.event_type, "trade.executed");
        assert_eq!(
            publisher.metrics.queue_full.load(Ordering::SeqCst),
            1
        );
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn disconnected_queue_never_drops_critical_event() {
        let path = temp_outbox("queue-closed");
        let (publisher, receiver) =
            EventPublisher::without_worker("test".to_string(), path.clone(), 1, 100)
                .unwrap();
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
            EventPublisher::without_worker("test".to_string(), path.clone(), 1, 2_000)
                .unwrap();

        for index in 0..1_000 {
            publisher
                .publish_critical(
                    "trade.failed",
                    serde_json::json!({"index": index}),
                )
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
    fn restart_recovery_delivers_same_event_ids_to_redis_when_enabled() {
        if env::var("ARB_RUN_REDIS_INTEGRATION").ok().as_deref() != Some("1") {
            return;
        }

        let redis_url = env::var("ARB_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
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
        while publisher.outbox.pending_count() != 0
            && std::time::Instant::now() < deadline
        {
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

        let _: redis::RedisResult<i64> =
            redis::cmd("DEL").arg(&stream).query(&mut connection);
        let _ = fs::remove_dir_all(path);
    }
}
