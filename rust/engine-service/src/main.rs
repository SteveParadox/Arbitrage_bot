// tonic::Status is the required error type at the gRPC service boundary; boxing it would
// complicate the control API without reducing runtime risk.
#![allow(clippy::result_large_err)]

use std::{
    collections::BTreeMap,
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use chrono::Utc;
use event_bus::EventPublisher;
use risk::{current_time_ms, load_risk_config, RiskEngine};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{sync::Mutex, time::sleep};
use tonic::{transport::Server, Code, Request, Response, Status};
use tracing::{info, warn};
use uuid::Uuid;

mod idempotency;

pub mod proto {
    tonic::include_proto!("arbitrage.engine.v1");
}

use idempotency::{
    fingerprint, CachedCommandReply, ClaimOutcome, CommandRecord, CommandStatus, IdempotencyStore,
};

use proto::engine_control_server::{EngineControl, EngineControlServer};
use proto::{
    CommandReply, ControlRequest, EngineStatus, ReloadStrategyRequest, StatusRequest,
    UpdateLimitsRequest,
};

#[derive(Clone)]
struct EngineControlService {
    config: Arc<ServiceConfig>,
    events: EventPublisher,
    idempotency: Arc<IdempotencyStore>,
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
    #[serde(default)]
    request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RuntimeLimits {
    version: u32,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    updated_at_ms: Option<u64>,
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
        validate_request_id(&request.get_ref().request_id)?;
        let message = request.into_inner();
        let normalized_reason = clean_reason(&message.reason, "gRPC start_trading");
        let request_fingerprint = fingerprint(
            "start_trading",
            &serde_json::json!({"reason": normalized_reason.clone()}).to_string(),
        );

        let _guard = self.mutation_lock.lock().await;
        let mut record =
            match self.begin_command(&message.request_id, "start_trading", &request_fingerprint)? {
                BeginCommand::Return(reply) => return Ok(Response::new(reply)),
                BeginCommand::Recover(record) => {
                    return self.recover_control_command(record, true, "start_trading");
                }
                BeginCommand::Execute(record) => record,
            };

        if !self.config.master_live_enabled {
            return Err(self.cache_failure(
                &mut record,
                Status::failed_precondition("deployment live-trading gate is disabled"),
            ));
        }
        if let Err(error) = read_control(&self.config.control_file) {
            return Err(self.cache_failure(
                &mut record,
                Status::failed_precondition(format!(
                    "runtime control state is invalid; issue stop before start: {error}"
                )),
            ));
        }

        let risk_config = match load_risk_config(&self.config.risk_config_file) {
            Ok(value) => value,
            Err(error) => return Err(self.cache_failure(&mut record, internal(error))),
        };
        let limits = match read_limits(&self.config.limits_file) {
            Ok(value) => value,
            Err(error) => return Err(self.cache_failure(&mut record, internal(error))),
        };
        if let Err(detail) = validate_runtime_limits(&limits, &risk_config) {
            return Err(self.cache_failure(
                &mut record,
                Status::failed_precondition(format!("runtime risk limits are invalid: {detail}")),
            ));
        }
        let mut risk_engine = match RiskEngine::new(risk_config) {
            Ok(value) => value,
            Err(error) => return Err(self.cache_failure(&mut record, internal(error))),
        };
        let risk_status = match risk_engine.status(current_time_ms()) {
            Ok(value) => value,
            Err(error) => return Err(self.cache_failure(&mut record, internal(error))),
        };
        if risk_status.manual_kill_switch_active {
            return Err(self.cache_failure(
                &mut record,
                Status::failed_precondition("manual kill switch is active"),
            ));
        }
        if risk_status.circuit_breaker.is_some() {
            return Err(self.cache_failure(
                &mut record,
                Status::failed_precondition("risk circuit breaker is active"),
            ));
        }
        if let Err(error) = self.events.ensure_critical_ready() {
            return Err(self.cache_failure(
                &mut record,
                Status::unavailable(format!("critical event pipeline unavailable: {error}")),
            ));
        }

        if let Err(error) = write_control(
            &self.config.control_file,
            true,
            normalized_reason,
            Some(message.request_id.clone()),
        ) {
            return Err(Status::unavailable(format!(
                "start_trading state write outcome is uncertain; retry the same request_id: {error}"
            )));
        }

        let applied_at_ms = now_ms();
        self.emit_command_event(
            &mut record,
            json!({
                "runtime_enabled": true,
                "command": "start_trading",
                "request_id": message.request_id,
            }),
        )?;
        let response = reply(
            true,
            "start_trading",
            record.request_id.clone(),
            "runtime trading gate enabled",
            applied_at_ms,
        );
        self.finish_success(&mut record, response).await
    }

    async fn stop_trading(
        &self,
        request: Request<ControlRequest>,
    ) -> Result<Response<CommandReply>, Status> {
        self.authorize(&request)?;
        validate_request_id(&request.get_ref().request_id)?;
        let message = request.into_inner();
        let normalized_reason = clean_reason(&message.reason, "gRPC stop_trading");
        let request_fingerprint = fingerprint(
            "stop_trading",
            &serde_json::json!({"reason": normalized_reason.clone()}).to_string(),
        );

        let _guard = self.mutation_lock.lock().await;
        let mut record =
            match self.begin_command(&message.request_id, "stop_trading", &request_fingerprint)? {
                BeginCommand::Return(reply) => return Ok(Response::new(reply)),
                BeginCommand::Recover(record) => {
                    return self.recover_control_command(record, false, "stop_trading");
                }
                BeginCommand::Execute(record) => record,
            };

        if let Err(error) = write_control(
            &self.config.control_file,
            false,
            normalized_reason,
            Some(message.request_id.clone()),
        ) {
            return Err(Status::unavailable(format!(
                "stop_trading state write outcome is uncertain; retry the same request_id: {error}"
            )));
        }

        let applied_at_ms = now_ms();
        self.emit_command_event(
            &mut record,
            json!({
                "runtime_enabled": false,
                "command": "stop_trading",
                "request_id": message.request_id,
            }),
        )?;
        let response = reply(
            true,
            "stop_trading",
            record.request_id.clone(),
            "runtime trading gate disabled",
            applied_at_ms,
        );
        self.finish_success(&mut record, response).await
    }

    async fn update_limits(
        &self,
        request: Request<UpdateLimitsRequest>,
    ) -> Result<Response<CommandReply>, Status> {
        self.authorize(&request)?;
        validate_request_id(&request.get_ref().request_id)?;
        let message = request.into_inner();
        let normalized_payload = normalize_limits_payload(&message)?;

        let _guard = self.mutation_lock.lock().await;
        let mut record = match self.begin_command(
            &message.request_id,
            "update_limits",
            &fingerprint("update_limits", &normalized_payload),
        )? {
            BeginCommand::Return(reply) => return Ok(Response::new(reply)),
            BeginCommand::Recover(record) => {
                return self.recover_limits_command(record);
            }
            BeginCommand::Execute(record) => record,
        };

        let mut limits = match read_limits(&self.config.limits_file) {
            Ok(value) => value,
            Err(error) => return Err(self.cache_failure(&mut record, internal(error))),
        };
        if let Err(status) = merge_limit(
            "min_net_edge_bps",
            &message.min_net_edge_bps,
            &mut limits.min_net_edge_bps,
            false,
        ) {
            return Err(self.cache_failure(&mut record, status));
        }
        if let Err(status) = merge_limit(
            "max_slippage_bps",
            &message.max_slippage_bps,
            &mut limits.max_slippage_bps,
            false,
        ) {
            return Err(self.cache_failure(&mut record, status));
        }
        if let Err(status) = merge_limit(
            "max_trade_size",
            &message.max_trade_size,
            &mut limits.max_trade_size,
            true,
        ) {
            return Err(self.cache_failure(&mut record, status));
        }
        if let Err(status) = merge_limit(
            "max_total_exposure",
            &message.max_total_exposure,
            &mut limits.max_total_exposure,
            true,
        ) {
            return Err(self.cache_failure(&mut record, status));
        }
        if let Err(status) = merge_limit(
            "max_daily_loss",
            &message.max_daily_loss,
            &mut limits.max_daily_loss,
            true,
        ) {
            return Err(self.cache_failure(&mut record, status));
        }

        let static_risk = match load_risk_config(&self.config.risk_config_file) {
            Ok(value) => value,
            Err(error) => return Err(self.cache_failure(&mut record, internal(error))),
        };
        limits.version = 1;
        if let Err(detail) = validate_runtime_limits(&limits, &static_risk) {
            return Err(self.cache_failure(&mut record, Status::invalid_argument(detail)));
        }
        if let Err(error) = self.events.ensure_critical_ready() {
            return Err(self.cache_failure(
                &mut record,
                Status::unavailable(format!("critical event pipeline unavailable: {error}")),
            ));
        }

        let applied_at_ms = now_ms();
        limits.request_id = Some(message.request_id.clone());
        limits.updated_at_ms = Some(applied_at_ms);
        if let Err(error) = write_json_atomic(&self.config.limits_file, &limits) {
            return Err(Status::unavailable(format!(
                "update_limits state write outcome is uncertain; retry the same request_id: {error}"
            )));
        }

        self.emit_command_event(
            &mut record,
            json!({
                "command": "update_limits",
                "request_id": message.request_id,
                "runtime_limits": limits,
            }),
        )?;
        let response = reply(
            true,
            "update_limits",
            record.request_id.clone(),
            "runtime risk limits updated",
            applied_at_ms,
        );
        self.finish_success(&mut record, response).await
    }

    async fn reload_strategy(
        &self,
        request: Request<ReloadStrategyRequest>,
    ) -> Result<Response<CommandReply>, Status> {
        self.authorize(&request)?;
        validate_request_id(&request.get_ref().request_id)?;
        let message = request.into_inner();
        let normalized_reason = clean_reason(&message.reason, "gRPC reload_strategy");
        let request_fingerprint = fingerprint(
            "reload_strategy",
            &serde_json::json!({"reason": normalized_reason.clone()}).to_string(),
        );

        let _guard = self.mutation_lock.lock().await;
        let mut record = match self.begin_command(
            &message.request_id,
            "reload_strategy",
            &request_fingerprint,
        )? {
            BeginCommand::Return(reply) => return Ok(Response::new(reply)),
            BeginCommand::Recover(record) => {
                return self.recover_reload_command(record);
            }
            BeginCommand::Execute(record) => record,
        };

        if let Err(error) = scanner::load_triangle_config(&self.config.triangle_config_file) {
            return Err(self.cache_failure(
                &mut record,
                Status::failed_precondition(format!(
                    "triangle strategy config is invalid: {error}"
                )),
            ));
        }
        if let Err(error) = self.events.ensure_critical_ready() {
            return Err(self.cache_failure(
                &mut record,
                Status::unavailable(format!("critical event pipeline unavailable: {error}")),
            ));
        }

        let generation = Uuid::new_v4().to_string();
        let applied_at_ms = now_ms();
        let payload = json!({
            "version": 1,
            "generation": generation.clone(),
            "requested_at_ms": applied_at_ms,
            "request_id": message.request_id.clone(),
            "reason": normalized_reason,
        });
        if let Err(error) = write_json_atomic(&self.config.strategy_reload_file, &payload) {
            return Err(Status::unavailable(format!(
                "reload_strategy state write outcome is uncertain; retry the same request_id: {error}"
            )));
        }

        self.emit_command_event(
            &mut record,
            json!({
                "command": "reload_strategy",
                "request_id": message.request_id,
                "strategy_generation": generation,
            }),
        )?;
        let response = reply(
            true,
            "reload_strategy",
            record.request_id.clone(),
            "strategy reload requested",
            applied_at_ms,
        );
        self.finish_success(&mut record, response).await
    }

    async fn get_status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<EngineStatus>, Status> {
        self.authorize(&request)?;
        validate_request_id(&request.get_ref().request_id)?;
        let control = read_control(&self.config.control_file);
        let (runtime_enabled, control_source, control_error) = control_snapshot(&control);
        let generation = read_strategy_generation(&self.config.strategy_reload_file);
        let (mut healthy, limits_json, mut summary) = match (
            read_limits(&self.config.limits_file),
            load_risk_config(&self.config.risk_config_file),
        ) {
            (Ok(limits), Ok(static_risk)) => match validate_runtime_limits(&limits, &static_risk) {
                Ok(()) => (
                    true,
                    serde_json::to_string(&limits).unwrap_or_else(|_| "{}".to_string()),
                    "engine control service ready".to_string(),
                ),
                Err(error) => (
                    false,
                    serde_json::to_string(&limits).unwrap_or_else(|_| "{}".to_string()),
                    format!("runtime risk limits invalid: {error}"),
                ),
            },
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
        if let Some(error) = control_error {
            healthy = false;
            summary = format!("runtime control state invalid: {error}; {summary}");
        }

        let event_health = self.events.health_snapshot();
        let idempotency_health = self.idempotency.health();
        if event_health.event_pipeline_status != "healthy" || !idempotency_health.healthy {
            healthy = false;
        }
        let detail = serde_json::to_string(&json!({
            "summary": summary,
            "event_pipeline": event_health,
            "grpc_idempotency_store_status": if idempotency_health.healthy {
                "healthy"
            } else {
                "unhealthy"
            },
            "grpc_idempotency_in_progress": idempotency_health.in_progress,
            "grpc_idempotency_detail": idempotency_health.detail,
        }))
        .unwrap_or_else(|error| {
            format!("{{\"summary\":\"failed to serialize health detail: {error}\"}}")
        });

        Ok(Response::new(EngineStatus {
            healthy,
            runtime_enabled,
            control_source,
            generated_at_ms: now_ms() as i64,
            strategy_generation: generation,
            limits_json,
            detail,
        }))
    }
}

enum BeginCommand {
    Execute(CommandRecord),
    Return(CommandReply),
    Recover(CommandRecord),
}

impl EngineControlService {
    fn begin_command(
        &self,
        request_id: &str,
        command_type: &str,
        request_fingerprint: &str,
    ) -> Result<BeginCommand, Status> {
        let outcome = self
            .idempotency
            .claim(request_id, command_type, request_fingerprint)
            .map_err(|error| {
                Status::unavailable(format!(
                    "gRPC idempotency store unavailable; command not executed: {error}"
                ))
            })?;
        match outcome {
            ClaimOutcome::New(record) => {
                info!(
                    request_id,
                    command_type,
                    request_fingerprint,
                    deduplication_result = "NEW",
                    execution_status = "IN_PROGRESS",
                    "gRPC command claimed"
                );
                Ok(BeginCommand::Execute(record))
            }
            ClaimOutcome::Completed(record) => match record.status {
                CommandStatus::Succeeded => {
                    let cached = record.response.ok_or_else(|| {
                        Status::internal("completed idempotency record is missing response")
                    })?;
                    info!(
                        request_id,
                        command_type,
                        request_fingerprint,
                        deduplication_result = "DUPLICATE_COMPLETED",
                        execution_status = "SUCCEEDED",
                        "returning cached gRPC command response"
                    );
                    Ok(BeginCommand::Return(command_reply_from_cached(cached)))
                }
                CommandStatus::Failed => {
                    let failure = record.failure.ok_or_else(|| {
                        Status::internal("failed idempotency record is missing failure")
                    })?;
                    info!(
                        request_id,
                        command_type,
                        request_fingerprint,
                        deduplication_result = "DUPLICATE_COMPLETED",
                        execution_status = "FAILED",
                        "returning cached gRPC command failure"
                    );
                    Err(status_from_cached(&failure.code, failure.detail))
                }
                CommandStatus::InProgress => unreachable!("completed claim cannot be IN_PROGRESS"),
            },
            ClaimOutcome::InProgress(record) => {
                warn!(
                    request_id,
                    command_type,
                    request_fingerprint,
                    deduplication_result = "DUPLICATE_IN_PROGRESS",
                    execution_status = "IN_PROGRESS",
                    "reconciling interrupted gRPC command"
                );
                Ok(BeginCommand::Recover(record))
            }
            ClaimOutcome::Conflict {
                existing_command,
                existing_fingerprint,
            } => {
                warn!(
                    request_id,
                    command_type,
                    request_fingerprint,
                    existing_command,
                    existing_fingerprint,
                    deduplication_result = "REQUEST_ID_CONFLICT",
                    execution_status = "REJECTED",
                    "request_id reused with a different logical command"
                );
                Err(Status::invalid_argument(
                    "request_id was already used for a different command or payload",
                ))
            }
        }
    }

    fn emit_command_event(&self, record: &mut CommandRecord, payload: Value) -> Result<(), Status> {
        if record.event_accepted {
            return Ok(());
        }
        self.events
            .publish_critical_with_id(
                record.event_id.clone(),
                "engine.state_changed",
                payload,
            )
            .map_err(|error| {
                Status::unavailable(format!(
                    "command state changed but critical event persistence failed; retry the same request_id: {error}"
                ))
            })?;
        self.idempotency
            .mark_event_accepted(record)
            .map_err(|error| {
                Status::unavailable(format!(
                    "critical event was accepted but command journal update failed; retry the same request_id: {error}"
                ))
            })
    }

    async fn finish_success(
        &self,
        record: &mut CommandRecord,
        response: CommandReply,
    ) -> Result<Response<CommandReply>, Status> {
        let cached = CachedCommandReply {
            accepted: response.accepted,
            command: response.command.clone(),
            request_id: response.request_id.clone(),
            detail: response.detail.clone(),
            applied_at_ms: response.applied_at_ms,
        };
        self.idempotency
            .complete_success(record, cached)
            .map_err(|error| {
                Status::unavailable(format!(
                    "command executed but idempotency result could not be committed; retry the same request_id: {error}"
                ))
            })?;
        info!(
            request_id = %record.request_id,
            command_type = %record.command_type,
            request_fingerprint = %record.request_fingerprint,
            deduplication_result = "NEW",
            execution_status = "SUCCEEDED",
            "gRPC command completed"
        );

        #[cfg(debug_assertions)]
        if let Ok(raw) = env::var("ARB_TEST_GRPC_RESPONSE_DELAY_MS") {
            if let Ok(delay_ms) = raw.parse::<u64>() {
                if delay_ms > 0 {
                    sleep(Duration::from_millis(delay_ms)).await;
                }
            }
        }
        Ok(Response::new(response))
    }

    fn cache_failure(&self, record: &mut CommandRecord, status: Status) -> Status {
        let code = code_name(status.code()).to_string();
        let detail = status.message().to_string();
        if let Err(error) = self.idempotency.complete_failure(record, code, detail) {
            return Status::unavailable(format!(
                "command failed before side effects, but idempotency failure could not be committed: {error}"
            ));
        }
        info!(
            request_id = %record.request_id,
            command_type = %record.command_type,
            request_fingerprint = %record.request_fingerprint,
            deduplication_result = "NEW",
            execution_status = "FAILED",
            grpc_code = %code_name(status.code()),
            "gRPC command failed before side effects"
        );
        status
    }

    fn recover_control_command(
        &self,
        mut record: CommandRecord,
        expected_enabled: bool,
        command: &str,
    ) -> Result<Response<CommandReply>, Status> {
        let state = read_control(&self.config.control_file)
            .map_err(|error| Status::aborted(format!(
                "request remains IN_PROGRESS; runtime state cannot be reconciled safely: {error}"
            )))?
            .ok_or_else(|| Status::aborted(
                "request remains IN_PROGRESS; runtime state is missing and cannot be reconciled safely"
            ))?;
        if state.request_id.as_deref() != Some(record.request_id.as_str())
            || state.enabled != expected_enabled
        {
            return Err(Status::aborted(
                "request remains IN_PROGRESS; persisted engine state does not prove this request completed",
            ));
        }

        let request_id = record.request_id.clone();
        self.emit_command_event(
            &mut record,
            json!({
                "runtime_enabled": expected_enabled,
                "command": command,
                "request_id": request_id,
                "recovered_after_restart": true,
            }),
        )?;
        let applied_at_ms = chrono::DateTime::parse_from_rfc3339(&state.updated_at)
            .ok()
            .and_then(|value| value.timestamp_millis().try_into().ok())
            .unwrap_or_else(now_ms);
        let response = reply(
            true,
            command,
            record.request_id.clone(),
            if expected_enabled {
                "runtime trading gate enabled"
            } else {
                "runtime trading gate disabled"
            },
            applied_at_ms,
        );
        let cached = CachedCommandReply {
            accepted: response.accepted,
            command: response.command.clone(),
            request_id: response.request_id.clone(),
            detail: response.detail.clone(),
            applied_at_ms: response.applied_at_ms,
        };
        self.idempotency
            .complete_success(&mut record, cached)
            .map_err(|error| {
                Status::unavailable(format!(
                    "reconciled command result could not be committed: {error}"
                ))
            })?;
        Ok(Response::new(response))
    }

    fn recover_limits_command(
        &self,
        mut record: CommandRecord,
    ) -> Result<Response<CommandReply>, Status> {
        let limits = read_limits(&self.config.limits_file).map_err(|error| {
            Status::aborted(format!(
                "request remains IN_PROGRESS; runtime limits cannot be reconciled safely: {error}"
            ))
        })?;
        if limits.request_id.as_deref() != Some(record.request_id.as_str()) {
            return Err(Status::aborted(
                "request remains IN_PROGRESS; persisted limits do not prove this request completed",
            ));
        }
        let request_id = record.request_id.clone();
        self.emit_command_event(
            &mut record,
            json!({
                "command": "update_limits",
                "request_id": request_id,
                "runtime_limits": limits.clone(),
                "recovered_after_restart": true,
            }),
        )?;
        let response = reply(
            true,
            "update_limits",
            record.request_id.clone(),
            "runtime risk limits updated",
            limits.updated_at_ms.unwrap_or_else(now_ms),
        );
        let cached = CachedCommandReply {
            accepted: response.accepted,
            command: response.command.clone(),
            request_id: response.request_id.clone(),
            detail: response.detail.clone(),
            applied_at_ms: response.applied_at_ms,
        };
        self.idempotency
            .complete_success(&mut record, cached)
            .map_err(|error| {
                Status::unavailable(format!(
                    "reconciled command result could not be committed: {error}"
                ))
            })?;
        Ok(Response::new(response))
    }

    fn recover_reload_command(
        &self,
        mut record: CommandRecord,
    ) -> Result<Response<CommandReply>, Status> {
        let raw = fs::read_to_string(&self.config.strategy_reload_file).map_err(|error| {
            Status::aborted(format!(
                "request remains IN_PROGRESS; strategy state cannot be reconciled safely: {error}"
            ))
        })?;
        let state: Value = serde_json::from_str(&raw).map_err(|error| {
            Status::aborted(format!(
                "request remains IN_PROGRESS; strategy state is invalid: {error}"
            ))
        })?;
        if state.get("request_id").and_then(Value::as_str) != Some(record.request_id.as_str()) {
            return Err(Status::aborted(
                "request remains IN_PROGRESS; persisted strategy state does not prove this request completed",
            ));
        }
        let generation = state
            .get("generation")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if generation.is_empty() {
            return Err(Status::aborted(
                "request remains IN_PROGRESS; persisted strategy generation is missing",
            ));
        }
        let applied_at_ms = state
            .get("requested_at_ms")
            .and_then(Value::as_u64)
            .unwrap_or_else(now_ms);
        let request_id = record.request_id.clone();
        self.emit_command_event(
            &mut record,
            json!({
                "command": "reload_strategy",
                "request_id": request_id,
                "strategy_generation": generation,
                "recovered_after_restart": true,
            }),
        )?;
        let response = reply(
            true,
            "reload_strategy",
            record.request_id.clone(),
            "strategy reload requested",
            applied_at_ms,
        );
        let cached = CachedCommandReply {
            accepted: response.accepted,
            command: response.command.clone(),
            request_id: response.request_id.clone(),
            detail: response.detail.clone(),
            applied_at_ms: response.applied_at_ms,
        };
        self.idempotency
            .complete_success(&mut record, cached)
            .map_err(|error| {
                Status::unavailable(format!(
                    "reconciled command result could not be committed: {error}"
                ))
            })?;
        Ok(Response::new(response))
    }

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
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
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
        risk_config_file: env_path(&repo_root, "ARB_RISK_CONFIG", "shared/config/risk.json"),
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
    let events = EventPublisher::try_from_env("engine-service")
        .map_err(|error| anyhow::anyhow!("failed to initialize durable event pipeline: {error}"))?;
    let idempotency_path = env_path(
        &repo_root,
        "ARB_GRPC_IDEMPOTENCY_STORE",
        "data/control/grpc-idempotency",
    );
    let retention_seconds = env::var("ARB_GRPC_IDEMPOTENCY_RETENTION_SECONDS")
        .unwrap_or_else(|_| "604800".to_string())
        .parse::<u64>()
        .context("ARB_GRPC_IDEMPOTENCY_RETENTION_SECONDS must be a positive integer")?;
    let idempotency = Arc::new(IdempotencyStore::open(idempotency_path, retention_seconds)?);

    let heartbeat_events = events.clone();
    let heartbeat_idempotency = idempotency.clone();
    let heartbeat_config = config.clone();
    tokio::spawn(async move {
        loop {
            let control = read_control(&heartbeat_config.control_file);
            let (runtime_enabled, control_source, _) = control_snapshot(&control);
            let (base_healthy, detail) = service_health(&heartbeat_config);
            let event_health = heartbeat_events.health_snapshot();
            let idempotency_health = heartbeat_idempotency.health();
            let healthy = base_healthy
                && event_health.event_pipeline_status == "healthy"
                && idempotency_health.healthy;
            heartbeat_events.publish(
                "engine.health",
                json!({
                    "component": "engine-control",
                    "healthy": healthy,
                    "detail": detail,
                    "runtime_enabled": runtime_enabled,
                    "control_source": control_source,
                    "grpc_addr": addr.to_string(),
                    "event_pipeline": event_health,
                    "grpc_idempotency_store_status": if idempotency_health.healthy {
                        "healthy"
                    } else {
                        "unhealthy"
                    },
                    "grpc_idempotency_in_progress": idempotency_health.in_progress,
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
            idempotency,
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

fn write_control(
    path: &Path,
    enabled: bool,
    reason: String,
    request_id: Option<String>,
) -> Result<()> {
    let payload = ControlState {
        version: 1,
        enabled,
        updated_at: Utc::now().to_rfc3339(),
        reason,
        source: "rust_grpc_control".to_string(),
        request_id,
    };
    write_json_atomic(path, &payload)
}

fn read_control(path: &Path) -> Result<Option<ControlState>> {
    let raw = match fs::read_to_string(path) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(error) => return Err(error.into()),
    };
    let state: ControlState = serde_json::from_str(&raw)?;
    if state.version != 1 {
        bail!("unsupported runtime control version {}", state.version);
    }
    if state.reason.trim().is_empty() || state.reason.chars().count() > 256 {
        bail!("runtime control reason must contain 1-256 characters");
    }
    if state.source != "fastapi_control" && state.source != "rust_grpc_control" {
        bail!("unsupported runtime control source {}", state.source);
    }
    chrono::DateTime::parse_from_rfc3339(&state.updated_at)
        .context("runtime control updated_at must be RFC3339")?;
    Ok(Some(state))
}

fn control_snapshot(control: &Result<Option<ControlState>>) -> (bool, String, Option<String>) {
    match control {
        Ok(Some(state)) => (state.enabled, state.source.clone(), None),
        Ok(None) => (false, "default_fail_closed".to_string(), None),
        Err(error) => (
            false,
            "invalid_fail_closed".to_string(),
            Some(error.to_string()),
        ),
    }
}

fn read_limits(path: &Path) -> Result<RuntimeLimits> {
    match fs::read_to_string(path) {
        Ok(raw) => Ok(serde_json::from_str(&raw)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(RuntimeLimits {
            version: 1,
            ..RuntimeLimits::default()
        }),
        Err(error) => Err(error.into()),
    }
}

fn read_strategy_generation(path: &Path) -> String {
    fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .and_then(|value| {
            value
                .get("generation")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_default()
}

fn normalize_limits_payload(message: &UpdateLimitsRequest) -> Result<String, Status> {
    let mut values = BTreeMap::new();
    for (name, raw) in [
        ("min_net_edge_bps", message.min_net_edge_bps.as_str()),
        ("max_slippage_bps", message.max_slippage_bps.as_str()),
        ("max_trade_size", message.max_trade_size.as_str()),
        ("max_total_exposure", message.max_total_exposure.as_str()),
        ("max_daily_loss", message.max_daily_loss.as_str()),
    ] {
        let normalized = if raw.trim().is_empty() {
            None
        } else {
            Some(
                Decimal::from_str_exact(raw.trim())
                    .map_err(|_| Status::invalid_argument(format!("{name} must be a decimal")))?
                    .normalize()
                    .to_string(),
            )
        };
        values.insert(name, normalized);
    }
    if values.values().all(Option::is_none) {
        return Err(Status::invalid_argument(
            "at least one runtime risk limit must be supplied",
        ));
    }
    serde_json::to_string(&values).map_err(internal)
}

fn command_reply_from_cached(cached: CachedCommandReply) -> CommandReply {
    CommandReply {
        accepted: cached.accepted,
        command: cached.command,
        request_id: cached.request_id,
        detail: cached.detail,
        applied_at_ms: cached.applied_at_ms,
    }
}

fn code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

fn status_from_cached(code: &str, detail: String) -> Status {
    let code = match code {
        "OK" => Code::Ok,
        "CANCELLED" => Code::Cancelled,
        "INVALID_ARGUMENT" => Code::InvalidArgument,
        "DEADLINE_EXCEEDED" => Code::DeadlineExceeded,
        "NOT_FOUND" => Code::NotFound,
        "ALREADY_EXISTS" => Code::AlreadyExists,
        "PERMISSION_DENIED" => Code::PermissionDenied,
        "RESOURCE_EXHAUSTED" => Code::ResourceExhausted,
        "FAILED_PRECONDITION" => Code::FailedPrecondition,
        "ABORTED" => Code::Aborted,
        "OUT_OF_RANGE" => Code::OutOfRange,
        "UNIMPLEMENTED" => Code::Unimplemented,
        "INTERNAL" => Code::Internal,
        "UNAVAILABLE" => Code::Unavailable,
        "DATA_LOSS" => Code::DataLoss,
        "UNAUTHENTICATED" => Code::Unauthenticated,
        _ => Code::Unknown,
    };
    Status::new(code, detail)
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
        return Err("trade, exposure, and daily-loss limits must be positive".to_string());
    }
    if max_exposure < max_trade {
        return Err("max_total_exposure must be >= max_trade_size".to_string());
    }
    Ok(())
}

fn effective_decimal(raw: Option<&str>, default: Decimal, field: &str) -> Result<Decimal, String> {
    match raw {
        Some(value) => Decimal::from_str_exact(value).map_err(|_| format!("invalid {field}")),
        None => Ok(default),
    }
}

fn write_json_atomic(path: &Path, payload: &impl Serialize) -> Result<()> {
    use std::io::Write as _;

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let temp = path.with_extension(format!("tmp-{}", Uuid::new_v4()));
    let bytes = serde_json::to_vec_pretty(payload)?;
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);

    match fs::rename(&temp, path) {
        Ok(()) => {}
        Err(_error) if path.exists() => {
            fs::remove_file(path)?;
            fs::rename(&temp, path)?;
        }
        Err(error) => {
            let _ = fs::remove_file(&temp);
            return Err(error.into());
        }
    }
    sync_directory(parent)?;
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        fs::File::open(path)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
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

fn validate_request_id(request_id: &str) -> Result<(), Status> {
    let request_id = request_id.trim();
    if request_id.is_empty() {
        return Err(Status::invalid_argument("request_id must not be empty"));
    }
    if request_id.len() > 64 || !request_id.is_ascii() {
        return Err(Status::invalid_argument(
            "request_id must be ASCII text no longer than 64 bytes",
        ));
    }
    Ok(())
}

fn service_health(config: &ServiceConfig) -> (bool, String) {
    if let Err(error) = read_control(&config.control_file) {
        return (false, format!("runtime control state invalid: {error}"));
    }

    match (
        read_limits(&config.limits_file),
        load_risk_config(&config.risk_config_file),
    ) {
        (Ok(limits), Ok(static_risk)) => match validate_runtime_limits(&limits, &static_risk) {
            Ok(()) => (true, "engine control service ready".to_string()),
            Err(error) => (false, format!("runtime risk limits invalid: {error}")),
        },
        (Err(error), _) => (false, format!("runtime risk limits unreadable: {error}")),
        (_, Err(error)) => (false, format!("static risk configuration invalid: {error}")),
    }
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

#[cfg(test)]
mod boundary_tests {
    use super::*;

    #[test]
    fn control_state_distinguishes_missing_from_corrupt() {
        let base = std::env::temp_dir().join(format!(
            "engine-control-state-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let missing = base.join("missing.json");
        assert!(matches!(read_control(&missing), Ok(None)));

        fs::create_dir_all(&base).unwrap();
        let corrupt = base.join("corrupt.json");
        fs::write(&corrupt, "{not-json").unwrap();
        assert!(read_control(&corrupt).is_err());

        let valid = base.join("valid.json");
        let valid_state = ControlState {
            version: 1,
            enabled: false,
            updated_at: "2026-10-01T20:00:00Z".to_string(),
            reason: "test".to_string(),
            source: "rust_grpc_control".to_string(),
            request_id: None,
        };
        fs::write(&valid, serde_json::to_vec(&valid_state).unwrap()).unwrap();
        assert!(matches!(read_control(&valid), Ok(Some(_))));
        let _ = fs::remove_dir_all(base);
    }

    #[test]
    fn request_id_validation_rejects_empty_and_oversized_values() {
        assert!(validate_request_id("").is_err());
        assert!(validate_request_id("   ").is_err());
        assert!(validate_request_id(&"x".repeat(65)).is_err());
        assert!(validate_request_id("request-123").is_ok());
    }
}
