use std::{
    env,
    sync::mpsc::{sync_channel, SyncSender, TrySendError},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use serde_json::Value;
use tracing::warn;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
pub struct EngineEvent {
    pub event_id: String,
    pub event_type: String,
    pub occurred_at_ms: u64,
    pub source: String,
    pub schema_version: u32,
    pub payload: Value,
}

#[derive(Clone)]
pub struct EventPublisher {
    sender: SyncSender<EngineEvent>,
    source: String,
}

impl EventPublisher {
    pub fn from_env(source: impl Into<String>) -> Self {
        let source = source.into();
        let redis_url = env::var("ARB_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
        let stream = env::var("ARB_EVENT_STREAM")
            .unwrap_or_else(|_| "arb.events".to_string());
        let maxlen = env::var("ARB_EVENT_STREAM_MAXLEN")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(1_000_000);
        let capacity = env::var("ARB_EVENT_QUEUE_CAPACITY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(4_096);
        let (sender, receiver) = sync_channel::<EngineEvent>(capacity);

        thread::Builder::new()
            .name(format!("event-bus-{source}"))
            .spawn(move || {
                let client = loop {
                    match redis::Client::open(redis_url.as_str()) {
                        Ok(client) => break client,
                        Err(error) => {
                            warn!(%error, "invalid Redis URL; retrying event publisher");
                            thread::sleep(Duration::from_secs(2));
                        }
                    }
                };

                let mut connection = None;
                while let Ok(event) = receiver.recv() {
                    loop {
                        if connection.is_none() {
                            match client.get_connection() {
                                Ok(value) => connection = Some(value),
                                Err(error) => {
                                    warn!(%error, "failed to connect to Redis event stream");
                                    thread::sleep(Duration::from_millis(500));
                                    continue;
                                }
                            }
                        }

                        let payload = event.payload.to_string();
                        let result: redis::RedisResult<String> = redis::cmd("XADD")
                            .arg(&stream)
                            .arg("MAXLEN")
                            .arg("~")
                            .arg(maxlen)
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
                            .query(connection.as_mut().expect("connection exists"));
                        match result {
                            Ok(_) => break,
                            Err(error) => {
                                warn!(%error, "failed to publish Redis event; reconnecting");
                                connection = None;
                                thread::sleep(Duration::from_millis(500));
                            }
                        }
                    }
                }
            })
            .expect("event publisher thread must start");

        Self { sender, source }
    }

    pub fn publish(&self, event_type: &str, payload: Value) {
        let event = EngineEvent {
            event_id: Uuid::new_v4().to_string(),
            event_type: event_type.to_string(),
            occurred_at_ms: now_ms(),
            source: self.source.clone(),
            schema_version: 1,
            payload,
        };

        if let Err(error) = self.sender.try_send(event) {
            match error {
                TrySendError::Full(event) => warn!(
                    event_type = %event.event_type,
                    "event queue full; dropping telemetry event"
                ),
                TrySendError::Disconnected(event) => warn!(
                    event_type = %event.event_type,
                    "event publisher disconnected; dropping telemetry event"
                ),
            }
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
