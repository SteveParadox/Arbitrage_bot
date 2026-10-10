"""Canonical safety response schemas, exported for frontend runtime validation."""

from datetime import datetime
from typing import Any, Literal
from pydantic import BaseModel, ConfigDict, model_validator


class ResponseModel(BaseModel):
    model_config = ConfigDict(extra="allow")


class TradingDependencies(BaseModel):
    model_config = ConfigDict(extra="forbid")
    database: bool
    engine: bool
    market_data: bool
    risk: bool
    redis: bool
    event_consumer: bool
    event_outbox: bool
    command_store: bool
    control_auth: bool
    control_state: bool
    stop_resolved: bool


class TradingHealth(ResponseModel):
    deployment_enabled: bool
    runtime_enabled: bool
    risk_allows_new_orders: bool
    effective_enabled: bool
    dependencies: TradingDependencies
    blocking_reasons: list[str]
    stop_outcome: Literal["STOP_REQUESTED", "STOP_UNCONFIRMED", "CONFIRMED_STOPPED"] | None
    stop_request_id: str | None
    updated_at: str | None
    reason: str | None
    source: str


class HealthResponse(ResponseModel):
    status: Literal["ok", "degraded", "unhealthy"]
    generated_at: datetime
    environment: str
    database_status: Literal["online", "offline"]
    market_stream_status: Literal[
        "unknown",
        "disconnected",
        "resynchronizing",
        "connected_but_stale",
        "connected_and_fresh",
        "degraded",
    ]
    last_market_event: datetime | None
    market_data: dict[str, Any]
    event_pipeline: dict[str, Any]
    redis: dict[str, Any]
    risk: dict[str, Any]
    engine_grpc: dict[str, Any]
    event_consumer: dict[str, Any]
    trading: TradingHealth
    control_auth_configured: bool


class TradingControlResponse(ResponseModel):
    status: Literal["started", "stopped", "stop_requested_fallback"]
    effective_enabled: bool | None
    engine_state_confirmed: bool
    stop_outcome: Literal["CONFIRMED_STOPPED", "STOP_UNCONFIRMED"] | None = None
    request_id: str | None = None
    exposure_confirmed_flat: Literal[False] = False

    @model_validator(mode="after")
    def consistent_outcome(self):
        if self.status == "stopped" and (
            self.stop_outcome != "CONFIRMED_STOPPED"
            or not self.engine_state_confirmed
            or self.effective_enabled is not False
        ):
            raise ValueError("stopped requires independent confirmation")
        if self.status == "stop_requested_fallback" and (
            self.stop_outcome != "STOP_UNCONFIRMED"
            or self.engine_state_confirmed
            or self.effective_enabled is not None
        ):
            raise ValueError("uncertain stop cannot report a confirmed outcome")
        return self
