use std::{
    env,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use chrono::Utc;
use event_bus::EventPublisher;
use risk::{current_time_ms, load_risk_config, RiskEngine};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{sync::Mutex, time::sleep};
use tonic::{transport::Server, Request, Response, Status};
use tracing::info;
use uuid::Uuid;

pub mod proto {
    tonic::include_proto!("arbitrage.engine.v1");
}

use proto::engine_control_server::{EngineControl, EngineControlServer};
use proto::{
    CommandReply, ControlRequest, EngineStatus, ReloadStrategyRequest,
    StatusRequest, UpdateLimitsRequest,
};

#[derive(Clone)]
struct EngineControlService {
    config: Arc<ServiceConfig>,
    events: EventPublisher,
    mutation_lock: Arc<Mutex<()>>,
}

#[derive(Debug)]
struct ServiceConfig {
    token: String,
    master_live_enabled: bool,
    control_file: PathBuf,
    limits_file: PathBuf,
    strategy_reload_file: PathBuf,
    risk_config_file: PathBuf,
    triangle_config_file: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
struct ControlState {
    version: u32,
    enabled: bool,
    updated_at: String,
    reason: String,
    source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RuntimeLimits {
    version: u32,
    min_net_edge_bps: Option<String>,
    max_slippage_bps: Option<String>,
    max_trade_size: Option<String>,
    max_total_exposure: Option<String>,
    max_daily_loss: Option<String>,
}

#[tonic::async_trait]
impl EngineControl for EngineControlService {
    async fn start_trading(
        &self,
        request: Request<ControlRequest>,
    ) -> Result<Response<CommandReply>, Status> {
        self.authorize(&request)?;
        let _guard = self.mutation_lock.lock().await;
        if !self.config.master_live_enabled {
            return Err(Status::failed_precondition(
                "deployment live-trading gate is disabled",
            ));
        }

        let risk_config = load_risk_config(&self.config.risk_config_file)
            .map_err(internal)?;
        let limits = read_limits(&self.config.limits_file).map_err(internal)?;
        validate_runtime_limits(&limits, &risk_config).map_err(|detail| {
            Status::failed_precondition(format!(
                "runtime risk limits are invalid: {detail}"
            ))
        })?;
        let mut risk_engine = RiskEngine::new(risk_config).map_err(internal)?;
        let risk_status = risk_engine.status(current_time_ms()).map_err(internal)?;
        if risk_status.manual_kill_switch_active {
            return Err(Status::failed_precondition(
                "manual kill switch is active",
            ));
        }
        if risk_status.circuit_breaker.is_some() {
            return Err(Status::failed_precondition(
                "risk circuit breaker is active",
            ));
        }

        let message = request.into_inner();
        let now = now_ms();
        write_control(
            &self.config.control_file,
            true,
            clean_reason(&message.reason, "gRPC start_trading"),
        )
        .map_err(internal)?;
        self.events.publish(
            "engine.health",
            json!({
                "runtime_enabled": true,
                "command": "start_trading",
                "request_id": message.request_id.clone(),
            }),
        );

        Ok(Response::new(reply(
            true,
            "start_trading",
            message.request_id,
            "runtime trading gate enabled",
            now,
        )))
    }

    async fn stop_trading(
        &self,
        request: Request<ControlRequest>,
    ) -> Result<Response<CommandReply>, Status> {
        self.authorize(&request)?;
        let _guard = self.mutation_lock.lock().await;
        let message = request.into_inner();
        let now = now_ms();
        write_control(
            &self.config.control_file,
            false,
            clean_reason(&message.reason, "gRPC stop_trading"),
        )
        .map_err(internal)?;
        self.events.publish(
            "engine.health",
            json!({
                "runtime_enabled": false,
                "command": "stop_trading",
                "request_id": message.request_id.clone(),
            }),
        );

        Ok(Response::new(reply(
            true,
            "stop_trading",
            message.request_id,
            "runtime trading gate disabled",
            now,
        )))
    }

    async fn update_limits(
        &self,
        request: Request<UpdateLimitsRequest>,
    ) -> Result<Response<CommandReply>, Status> {
        self.authorize(&request)?;
        let _guard = self.mutation_lock.lock().await;
        let message = request.into_inner();
        let mut limits = read_limits(&self.config.limits_file).map_err(internal)?;
        merge_limit(
            "min_net_edge_bps",
            &message.min_net_edge_bps,
            &mut limits.min_net_edge_bps,
            false,
        )?;
        merge_limit(
            "max_slippage_bps",
            &message.max_slippage_bps,
            &mut limits.max_slippage_bps,
            false,
        )?;
        merge_limit(
            "max_trade_size",
            &message.max_trade_size,
            &mut limits.max_trade_size,
            true,
        )?;
        merge_limit(
            "max_total_exposure",
            &message.max_total_exposure,
            &mut limits.max_total_exposure,
            true,
        )?;
        merge_limit(
            "max_daily_loss",
            &message.max_daily_loss,
            &mut limits.max_daily_loss,
            true,
        )?;

        if [
            &message.min_net_edge_bps,
            &message.max_slippage_bps,
            &message.max_trade_size,
            &message.max_total_exposure,
            &message.max_daily_loss,
        ]
        .iter()
        .all(|value| value.trim().is_empty())
        {
            return Err(Status::invalid_argument(
                "at least one runtime risk limit must be supplied",
            ));
        }

        let static_risk = load_risk_config(&self.config.risk_config_file)
            .map_err(internal)?;
        limits.version = 1;
        validate_runtime_limits(&limits, &static_risk)
            .map_err(Status::invalid_argument)?;
        write_json_atomic(&self.config.limits_file, &limits).map_err(internal)?;
        let now = now_ms();
        self.events.publish(
            "engine.health",
            json!({
                "command": "update_limits",
                "request_id": message.request_id.clone(),
                "runtime_limits": limits,
            }),
        );

        Ok(Response::new(reply(
            true,
            "update_limits",
            message.request_id,
            "runtime risk limits updated",
            now,
        )))
    }

    async fn reload_strategy(
        &self,
        request: Request<ReloadStrategyRequest>,
    ) -> Result<Response<CommandReply>, Status> {
        self.authorize(&request)?;
        let _guard = self.mutation_lock.lock().await;
        let message = request.into_inner();
        scanner::load_triangle_config(&self.config.triangle_config_file)
            .map_err(|error| {
                Status::failed_precondition(format!(
                    "triangle strategy config is invalid: {error}"
                ))
            })?;
        let generation = Uuid::new_v4().to_string();
        let payload = json!({
            "version": 1,
            "generation": generation.clone(),
            "requested_at_ms": now_ms(),
            "reason": clean_reason(&message.reason, "gRPC reload_strategy"),
        });
        write_json_atomic(&self.config.strategy_reload_file, &payload)
            .map_err(internal)?;
        let now = now_ms();
        self.events.publish(
            "engine.health",
            json!({
                "command": "reload_strategy",
                "request_id": message.request_id.clone(),
                "strategy_generation": generation.clone(),
            }),
        );

        Ok(Response::new(reply(
            true,
            "reload_strategy",
            message.request_id,
            "strategy reload requested",
            now,
        )))
    }

    async fn get_status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<EngineStatus>, Status> {
        self.authorize(&request)?;
        let control = read_control(&self.config.control_file);
        let generation = read_strategy_generation(&self.config.strategy_reload_file);
        let (healthy, limits_json, detail) =
            match (
                read_limits(&self.config.limits_file),
                load_risk_config(&self.config.risk_config_file),
            ) {
                (Ok(limits), Ok(static_risk)) => {
                    match validate_runtime_limits(&limits, &static_risk) {
                        Ok(()) => (
                            true,
                            serde_json::to_string(&limits)
                                .unwrap_or_else(|_| "{}".to_string()),
                            "engine control service ready".to_string(),
                        ),
                        Err(error) => (
                            false,
                            serde_json::to_string(&limits)
                                .unwrap_or_else(|_| "{}".to_string()),
                            format!("runtime risk limits invalid: {error}"),
                        ),
                    }
                }
                (Err(error), _) => (
                    false,
                    "{}".to_string(),
                    format!("runtime risk limits unreadable: {error}"),
                ),
                (_, Err(error)) => (
                    false,
                    "{}".to_string(),
                    format!("static risk configuration invalid: {error}"),
                ),
            };
        Ok(Response::new(EngineStatus {
            healthy,
            runtime_enabled: control.as_ref().map(|value| value.enabled).unwrap_or(false),
            control_source: control
                .as_ref()
                .map(|value| value.source.clone())
                .unwrap_or_else(|| "default_fail_closed".to_string()),
            generated_at_ms: now_ms() as i64,
            strategy_generation: generation,
            limits_json,
            detail,
        }))
    }
}

impl EngineControlService {
    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        if self.config.token.len() < 32 {
            return Err(Status::unavailable(
                "ARB_ENGINE_GRPC_TOKEN must contain at least 32 bytes",
            ));
        }
        let supplied = request
            .metadata()
            .get("x-engine-token")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        if !constant_time_eq(supplied.as_bytes(), self.config.token.as_bytes()) {
            return Err(Status::unauthenticated("invalid engine gRPC token"));
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .json()
        .init();

    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let config = Arc::new(ServiceConfig {
        token: env::var("ARB_ENGINE_GRPC_TOKEN").unwrap_or_default(),
        master_live_enabled: env::var("ARB_LIVE_TRADING_ENABLED")
            .unwrap_or_else(|_| "false".to_string())
            .parse::<bool>()
            .context("ARB_LIVE_TRADING_ENABLED must be true or false")?,
        control_file: env_path(
            &repo_root,
            "ARB_CONTROL_STATE_FILE",
            "data/control/trading_state.json",
        ),
        limits_file: env_path(
            &repo_root,
            "ARB_RUNTIME_LIMITS_FILE",
            "data/control/risk_limits.json",
        ),
        strategy_reload_file: env_path(
            &repo_root,
            "ARB_STRATEGY_RELOAD_FILE",
            "data/control/strategy_reload.json",
        ),
        risk_config_file: env_path(
            &repo_root,
            "ARB_RISK_CONFIG",
            "shared/config/risk.json",
        ),
        triangle_config_file: env_path(
            &repo_root,
            "ARB_TRIANGLE_CONFIG",
            "shared/config/triangles.json",
        ),
    });

    let addr: SocketAddr = env::var("ARB_ENGINE_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50051".to_string())
        .parse()
        .context("invalid ARB_ENGINE_GRPC_ADDR")?;
    let events = EventPublisher::from_env("engine-service");
    let heartbeat_events = events.clone();
    let heartbeat_config = config.clone();
    tokio::spawn(async move {
        loop {
            let control = read_control(&heartbeat_config.control_file);
            heartbeat_events.publish(
                "engine.health",
                json!({
                    "healthy": true,
                    "runtime_enabled": control
                        .as_ref()
                        .map(|state| state.enabled)
                        .unwrap_or(false),
                    "grpc_addr": addr.to_string(),
                }),
            );
            sleep(Duration::from_secs(5)).await;
        }
    });

    info!(%addr, "engine gRPC control service listening");
    Server::builder()
        .add_service(EngineControlServer::new(EngineControlService {
            config,
            events,
            mutation_lock: Arc::new(Mutex::new(())),
        }))
        .serve(addr)
        .await?;
    Ok(())
}

fn env_path(repo_root: &Path, name: &str, default: &str) -> PathBuf {
    let value = env::var(name).unwrap_or_else(|_| default.to_string());
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        repo_root.join(path)
    }
}

fn clean_reason(value: &str, fallback: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        fallback.to_string()
    } else {
        value.chars().take(256).collect()
    }
}

fn reply(
    accepted: bool,
    command: &str,
    request_id: String,
    detail: &str,
    applied_at_ms: u64,
) -> CommandReply {
    CommandReply {
        accepted,
        command: command.to_string(),
        request_id,
        detail: detail.to_string(),
        applied_at_ms: applied_at_ms as i64,
    }
}

fn write_control(path: &Path, enabled: bool, reason: String) -> Result<()> {
    let payload = ControlState {
        version: 1,
        enabled,
        updated_at: Utc::now().to_rfc3339(),
        reason,
        source: "rust_grpc_control".to_string(),
    };
    write_json_atomic(path, &payload)
}

fn read_control(path: &Path) -> Option<ControlState> {
    fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .filter(|state: &ControlState| state.version == 1)
}

fn read_limits(path: &Path) -> Result<RuntimeLimits> {
    match fs::read_to_string(path) {
        Ok(raw) => Ok(serde_json::from_str(&raw)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(RuntimeLimits {
                version: 1,
                ..RuntimeLimits::default()
            })
        }
        Err(error) => Err(error.into()),
    }
}

fn read_strategy_generation(path: &Path) -> String {
    fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .and_then(|value| value.get("generation").and_then(Value::as_str).map(str::to_owned))
        .unwrap_or_default()
}

fn merge_limit(
    name: &str,
    raw: &str,
    target: &mut Option<String>,
    strictly_positive: bool,
) -> Result<(), Status> {
    if raw.trim().is_empty() {
        return Ok(());
    }
    let value = Decimal::from_str_exact(raw.trim())
        .map_err(|_| Status::invalid_argument(format!("{name} must be a decimal")))?;
    if (strictly_positive && value <= Decimal::ZERO)
        || (!strictly_positive && value < Decimal::ZERO)
    {
        return Err(Status::invalid_argument(format!(
            "{name} has an invalid value"
        )));
    }
    *target = Some(value.normalize().to_string());
    Ok(())
}

fn validate_runtime_limits(
    limits: &RuntimeLimits,
    static_risk: &risk::RiskConfig,
) -> Result<(), String> {
    if limits.version != 1 {
        return Err(format!(
            "unsupported runtime risk limits version {}",
            limits.version
        ));
    }

    let min_edge = effective_decimal(
        limits.min_net_edge_bps.as_deref(),
        static_risk.min_net_edge_bps,
        "min_net_edge_bps",
    )?;
    let max_slippage = effective_decimal(
        limits.max_slippage_bps.as_deref(),
        static_risk.max_slippage_bps,
        "max_slippage_bps",
    )?;
    let max_trade = effective_decimal(
        limits.max_trade_size.as_deref(),
        static_risk.max_trade_size,
        "max_trade_size",
    )?;
    let max_exposure = effective_decimal(
        limits.max_total_exposure.as_deref(),
        static_risk.max_total_exposure,
        "max_total_exposure",
    )?;
    let max_daily_loss = effective_decimal(
        limits.max_daily_loss.as_deref(),
        static_risk.max_daily_loss,
        "max_daily_loss",
    )?;

    if min_edge < Decimal::ZERO || max_slippage < Decimal::ZERO {
        return Err("edge and slippage limits must be non-negative".to_string());
    }
    if max_trade <= Decimal::ZERO
        || max_exposure <= Decimal::ZERO
        || max_daily_loss <= Decimal::ZERO
    {
        return Err(
            "trade, exposure, and daily-loss limits must be positive".to_string(),
        );
    }
    if max_exposure < max_trade {
        return Err("max_total_exposure must be >= max_trade_size".to_string());
    }
    Ok(())
}

fn effective_decimal(
    raw: Option<&str>,
    default: Decimal,
    field: &str,
) -> Result<Decimal, String> {
    match raw {
        Some(value) => Decimal::from_str_exact(value)
            .map_err(|_| format!("invalid {field}")),
        None => Ok(default),
    }
}

fn write_json_atomic(path: &Path, payload: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temp = path.with_extension(format!("tmp-{}", Uuid::new_v4()));
    let bytes = serde_json::to_vec_pretty(payload)?;
    fs::write(&temp, bytes)?;
    match fs::rename(&temp, path) {
        Ok(()) => Ok(()),
        Err(error) if path.exists() => {
            fs::remove_file(path)?;
            fs::rename(&temp, path)?;
            Ok(())
        }
        Err(error) => {
            let _ = fs::remove_file(&temp);
            Err(error.into())
        }
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
