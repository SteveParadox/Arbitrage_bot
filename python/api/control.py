from __future__ import annotations

import hmac
import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy.orm import Session

from analytics.db import get_db
from api.contracts import HealthResponse, TradingControlResponse
from api.engine_client import EngineCommandError, engine_grpc_client
from api.operations import (
    _balance_snapshot,
    _performance_snapshot,
    recent_executions,
    recent_opportunities,
)
from api.runtime_control import (
    read_control_state,
    read_stop_intent,
    request_stop,
    mark_stop_unconfirmed,
    confirm_stop,
)
from api.settings import settings

router = APIRouter(tags=["control-api"])
DatabaseSession = Annotated[Session, Depends(get_db)]
bearer = HTTPBearer(auto_error=False)


class TradingCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    @field_validator("reason")
    @classmethod
    def nonblank_reason(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("reason must not be blank")
        return value.strip()

    reason: str = Field(
        default="operator control request",
        min_length=1,
        max_length=256,
    )
    request_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9_-]+$",
    )


class RiskLimitsCommand(BaseModel):
    request_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9_-]+$",
    )
    min_net_edge_bps: Decimal | None = Field(default=None, ge=0)
    max_slippage_bps: Decimal | None = Field(default=None, ge=0)
    max_trade_size: Decimal | None = Field(default=None, gt=0)
    max_total_exposure: Decimal | None = Field(default=None, gt=0)
    max_daily_loss: Decimal | None = Field(default=None, gt=0)


def require_control_auth(
    credentials: Annotated[
        HTTPAuthorizationCredentials | None,
        Depends(bearer),
    ],
) -> None:
    expected = settings.arb_control_api_token
    if len(expected.encode("utf-8")) < 32:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="control bearer token must be configured with at least 32 bytes",
        )
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing control bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not hmac.compare_digest(
        credentials.credentials.encode("utf-8"),
        expected.encode("utf-8"),
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid control bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )


@router.get("/opportunities")
def get_opportunities(
    db: DatabaseSession,
    limit: int = Query(default=50, ge=1, le=500),
) -> list[dict[str, Any]]:
    return recent_opportunities(db, limit)


@router.get("/trades")
def get_trades(
    db: DatabaseSession,
    limit: int = Query(default=50, ge=1, le=500),
) -> list[dict[str, Any]]:
    return recent_executions(db, limit)


@router.get("/performance")
def get_performance(db: DatabaseSession) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(UTC),
        **_performance_snapshot(db),
    }


@router.get("/balances")
def get_balances(db: DatabaseSession) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(UTC),
        **_balance_snapshot(db),
    }


@router.get("/health", response_model=HealthResponse)
async def get_health(db: DatabaseSession) -> dict[str, Any]:
    from api.health import health_snapshot

    return await health_snapshot(db, engine_grpc_client)


@router.post(
    "/trading/start",
    response_model=TradingControlResponse,
    dependencies=[Depends(require_control_auth)],
)
async def start_trading(command: TradingCommand) -> dict[str, Any]:
    if not settings.arb_live_trading_enabled:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "deployment live-trading gate is disabled; ARB_LIVE_TRADING_ENABLED must be true"
            ),
        )

    intent = read_stop_intent()
    if intent and intent["status"] != "CONFIRMED_STOPPED":
        raise HTTPException(
            status_code=409, detail="emergency stop remains unconfirmed; retry stop before start"
        )
    if not read_control_state().get("valid"):
        raise HTTPException(
            status_code=409,
            detail="authoritative control state is missing or invalid; issue stop before start",
        )

    request_id = command.request_id or uuid.uuid4().hex
    try:
        result = await engine_grpc_client.start(
            command.reason,
            request_id=request_id,
        )
    except EngineCommandError as error:
        if error.code_name not in {"INVALID_ARGUMENT", "FAILED_PRECONDITION"}:
            try:
                request_stop("start outcome unconfirmed", "uncertain-" + uuid.uuid4().hex)
            except (OSError, TimeoutError):
                pass
        raise HTTPException(
            status_code=_command_http_status(error),
            detail={
                "message": f"Rust engine command failed: {error}",
                "request_id": request_id,
            },
        ) from error

    engine = await _verify_engine_state()
    state = read_control_state()
    enabled = bool(result.accepted and engine and engine.runtime_enabled and state["enabled"])
    if not enabled:
        try:
            request_stop("start state could not be verified", "uncertain-" + uuid.uuid4().hex)
        except (OSError, TimeoutError):
            pass
        raise HTTPException(
            status_code=503,
            detail={
                "message": "start outcome unconfirmed or gate disabled",
                "request_id": request_id,
            },
        )
    return {
        "status": "started",
        "effective_enabled": None,
        "runtime_enabled": True,
        "engine_state_confirmed": True,
        "command": result.__dict__,
    }


@router.post(
    "/trading/stop",
    response_model=TradingControlResponse,
    dependencies=[Depends(require_control_auth)],
)
async def stop_trading(command: TradingCommand) -> dict[str, Any]:
    request_id = command.request_id or uuid.uuid4().hex
    local_intent_persisted = False
    persistence_error = False
    try:
        # Do this before dispatch; crashes and lost responses retain disabled intent.
        request_stop(command.reason, request_id)
        local_intent_persisted = True
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    except (OSError, TimeoutError):
        # Still attempt the remote stop when the local volume is unavailable.
        persistence_error = True

    result = None
    warning = "Rust stop command could not be independently confirmed"
    try:
        result = await engine_grpc_client.stop(command.reason, request_id=request_id)
        engine = await _verify_engine_state()
        detail = _engine_detail(engine)
        if (
            result.accepted
            and result.request_id == request_id
            and engine is not None
            and not engine.runtime_enabled
            and detail.get("control_request_id") == request_id
            and confirm_stop(request_id)
        ):
            return {
                "status": "stopped",
                "stop_outcome": "CONFIRMED_STOPPED",
                "effective_enabled": False,
                "engine_state_confirmed": True,
                "request_id": request_id,
                "local_intent_persisted": True,
                "exposure_confirmed_flat": False,
                "command": result.__dict__,
            }
    except (EngineCommandError, OSError, TimeoutError):
        pass
    if persistence_error:
        warning = "local disabled intent could not be persisted; engine stop remains unconfirmed"
    try:
        mark_stop_unconfirmed(request_id)
    except (OSError, TimeoutError):
        pass
    return {
        "status": "stop_requested_fallback",
        "stop_outcome": "STOP_UNCONFIRMED",
        "effective_enabled": None,
        "engine_state_confirmed": False,
        "request_id": request_id,
        "local_intent_persisted": local_intent_persisted,
        "exposure_confirmed_flat": False,
        "warning": warning,
        "control": read_control_state(),
    }


def _engine_detail(engine) -> dict[str, Any]:
    if engine is None:
        return {}
    try:
        detail = json.loads(engine.detail)
        return detail if isinstance(detail, dict) else {}
    except (TypeError, ValueError):
        return {}


async def _verify_engine_state():
    try:
        engine = await engine_grpc_client.status()
        timestamp = getattr(engine, "generated_at_ms", None)
        age = (
            int(datetime.now(UTC).timestamp() * 1000) - timestamp if type(timestamp) is int else -1
        )
        return engine if 0 <= age <= settings.arb_health_event_max_age_ms else None
    except EngineCommandError:
        return None


@router.post(
    "/engine/limits",
    dependencies=[Depends(require_control_auth)],
)
async def update_limits(command: RiskLimitsCommand) -> dict[str, Any]:
    request_id = command.request_id or uuid.uuid4().hex
    values = command.model_dump(exclude={"request_id"})
    if not any(value is not None for value in values.values()):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="at least one risk limit must be supplied",
        )
    try:
        result = await engine_grpc_client.update_limits(
            **{key: "" if value is None else str(value) for key, value in values.items()},
            request_id=request_id,
        )
    except EngineCommandError as error:
        raise HTTPException(
            status_code=_command_http_status(error),
            detail={
                "message": f"Rust engine command failed: {error}",
                "request_id": request_id,
            },
        ) from error
    return {"status": "updated", "command": result.__dict__}


@router.post(
    "/engine/reload-strategy",
    dependencies=[Depends(require_control_auth)],
)
async def reload_strategy(command: TradingCommand) -> dict[str, Any]:
    request_id = command.request_id or uuid.uuid4().hex
    try:
        result = await engine_grpc_client.reload_strategy(
            command.reason,
            request_id=request_id,
        )
    except EngineCommandError as error:
        raise HTTPException(
            status_code=_command_http_status(error),
            detail={
                "message": f"Rust engine command failed: {error}",
                "request_id": request_id,
            },
        ) from error
    return {"status": "reload_requested", "command": result.__dict__}


def _command_http_status(error: EngineCommandError) -> int:
    if error.code_name == "INVALID_ARGUMENT":
        return status.HTTP_422_UNPROCESSABLE_ENTITY
    if error.code_name == "FAILED_PRECONDITION":
        return status.HTTP_409_CONFLICT
    return status.HTTP_503_SERVICE_UNAVAILABLE
