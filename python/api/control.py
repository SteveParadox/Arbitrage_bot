from __future__ import annotations

import hmac
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from analytics.db import get_db
from api.engine_client import EngineCommandError, engine_grpc_client
from api.event_consumer import consumer_metrics_snapshot
from api.operations import (
    _balance_snapshot,
    _performance_snapshot,
    _recent_market_activity,
    _risk_status,
    recent_executions,
    recent_opportunities,
)
from api.runtime_control import read_control_state, write_control_state
from api.settings import settings

router = APIRouter(tags=["control-api"])
DatabaseSession = Annotated[Session, Depends(get_db)]
bearer = HTTPBearer(auto_error=False)


class TradingCommand(BaseModel):
    reason: str = Field(
        default="operator control request",
        min_length=1,
        max_length=256,
    )


class RiskLimitsCommand(BaseModel):
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


@router.get("/health")
async def get_health(db: DatabaseSession) -> dict[str, Any]:
    database_status = "online"
    try:
        db.execute(text("SELECT 1"))
    except SQLAlchemyError:
        database_status = "offline"

    if database_status == "online":
        market_status, last_market_event = _recent_market_activity(db)
    else:
        market_status, last_market_event = "unknown", None

    risk = _risk_status()
    control = read_control_state()
    risk_allows_new_orders = bool(
        risk.get("available") is not False
        and not risk.get("kill_switch_active")
        and not risk.get("circuit_breaker")
    )

    grpc_status: dict[str, Any]
    try:
        engine = await engine_grpc_client.status()
        try:
            detail_payload = json.loads(engine.detail)
        except (TypeError, ValueError):
            detail_payload = {"summary": engine.detail}
        event_pipeline = detail_payload.get("event_pipeline", {})
        grpc_status = {
            "status": "online" if engine.healthy else "degraded",
            "runtime_enabled": engine.runtime_enabled,
            "strategy_generation": engine.strategy_generation,
            "detail": detail_payload.get("summary", engine.detail),
            "event_pipeline_status": event_pipeline.get(
                "event_pipeline_status",
                "unknown",
            ),
            "critical_event_backlog": event_pipeline.get(
                "critical_events_pending",
            ),
            "oldest_pending_event_age_ms": event_pipeline.get(
                "oldest_pending_event_age_ms",
            ),
            "event_pipeline": event_pipeline,
            "grpc_idempotency_store_status": detail_payload.get(
                "grpc_idempotency_store_status",
                "unknown",
            ),
            "grpc_idempotency_in_progress": detail_payload.get(
                "grpc_idempotency_in_progress",
            ),
        }
    except EngineCommandError as error:
        grpc_status = {
            "status": "offline",
            "runtime_enabled": control["enabled"],
            "strategy_generation": "",
            "detail": str(error),
            "event_pipeline_status": "unknown",
            "critical_event_backlog": None,
            "oldest_pending_event_age_ms": None,
            "event_pipeline": {},
            "grpc_idempotency_store_status": "unknown",
            "grpc_idempotency_in_progress": None,
        }

    effective = bool(
        settings.arb_live_trading_enabled
        and grpc_status["status"] == "online"
        and grpc_status["runtime_enabled"]
        and risk_allows_new_orders
        and market_status == "online"
    )
    overall_ok = (
        database_status == "online"
        and market_status == "online"
        and grpc_status["status"] == "online"
        and risk.get("available") is not False
    )

    return {
        "status": "ok" if overall_ok else "degraded",
        "generated_at": datetime.now(UTC),
        "environment": settings.arb_env,
        "database_status": database_status,
        "market_stream_status": market_status,
        "last_market_event": last_market_event,
        "risk": risk,
        "engine_grpc": grpc_status,
        "event_consumer": consumer_metrics_snapshot(),
        "trading": {
            "deployment_enabled": settings.arb_live_trading_enabled,
            "runtime_enabled": grpc_status["runtime_enabled"],
            "risk_allows_new_orders": risk_allows_new_orders,
            "effective_enabled": effective,
            "updated_at": control["updated_at"],
            "reason": control["reason"],
            "source": control["source"],
        },
        "control_auth_configured": (
            len(settings.arb_control_api_token.encode("utf-8")) >= 32
        ),
    }


@router.post(
    "/trading/start",
    dependencies=[Depends(require_control_auth)],
)
async def start_trading(command: TradingCommand) -> dict[str, Any]:
    if not settings.arb_live_trading_enabled:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "deployment live-trading gate is disabled; "
                "ARB_LIVE_TRADING_ENABLED must be true"
            ),
        )

    try:
        result = await engine_grpc_client.start(command.reason)
    except EngineCommandError as error:
        raise HTTPException(
            status_code=_command_http_status(error),
            detail=f"Rust engine command failed: {error}",
        ) from error

    return {
        "status": "started",
        "effective_enabled": result.accepted,
        "command": result.__dict__,
    }


@router.post(
    "/trading/stop",
    dependencies=[Depends(require_control_auth)],
)
async def stop_trading(command: TradingCommand) -> dict[str, Any]:
    try:
        result = await engine_grpc_client.stop(command.reason)
        return {
            "status": "stopped",
            "effective_enabled": False,
            "engine_state_confirmed": True,
            "command": result.__dict__,
        }
    except EngineCommandError as error:
        fallback = write_control_state(
            enabled=False,
            reason=f"gRPC stop fallback: {command.reason}",
        )
        return {
            "status": "stop_requested_fallback",
            "effective_enabled": None,
            "engine_state_confirmed": False,
            "warning": (
                "Rust gRPC did not confirm the stop command. A local fail-closed "
                "control state was written, but engine state remains unconfirmed: "
                f"{error}"
            ),
            "control": fallback,
        }


@router.post(
    "/engine/limits",
    dependencies=[Depends(require_control_auth)],
)
async def update_limits(command: RiskLimitsCommand) -> dict[str, Any]:
    values = command.model_dump()
    if not any(value is not None for value in values.values()):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="at least one risk limit must be supplied",
        )
    try:
        result = await engine_grpc_client.update_limits(
            **{
                key: "" if value is None else str(value)
                for key, value in values.items()
            }
        )
    except EngineCommandError as error:
        raise HTTPException(
            status_code=_command_http_status(error),
            detail=f"Rust engine command failed: {error}",
        ) from error
    return {"status": "updated", "command": result.__dict__}


@router.post(
    "/engine/reload-strategy",
    dependencies=[Depends(require_control_auth)],
)
async def reload_strategy(command: TradingCommand) -> dict[str, Any]:
    try:
        result = await engine_grpc_client.reload_strategy(command.reason)
    except EngineCommandError as error:
        raise HTTPException(
            status_code=_command_http_status(error),
            detail=f"Rust engine command failed: {error}",
        ) from error
    return {"status": "reload_requested", "command": result.__dict__}



def _command_http_status(error: EngineCommandError) -> int:
    if error.code_name == "INVALID_ARGUMENT":
        return status.HTTP_422_UNPROCESSABLE_ENTITY
    if error.code_name == "FAILED_PRECONDITION":
        return status.HTTP_409_CONFLICT
    return status.HTTP_503_SERVICE_UNAVAILABLE
