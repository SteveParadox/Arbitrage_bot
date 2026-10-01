from __future__ import annotations

import hmac
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from analytics.db import get_db
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


def require_control_auth(
    credentials: Annotated[
        HTTPAuthorizationCredentials | None,
        Depends(bearer),
    ],
) -> None:
    expected = settings.arb_control_api_token
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="control authentication is not configured",
        )
    if len(expected.encode("utf-8")) < 32:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="control bearer token must be at least 32 bytes",
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
def get_health(db: DatabaseSession) -> dict[str, Any]:
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
    effective = bool(
        settings.arb_live_trading_enabled
        and control["enabled"]
        and risk_allows_new_orders
    )

    return {
        "status": "ok" if database_status == "online" else "degraded",
        "generated_at": datetime.now(UTC),
        "environment": settings.arb_env,
        "database_status": database_status,
        "market_stream_status": market_status,
        "last_market_event": last_market_event,
        "risk": risk,
        "trading": {
            "deployment_enabled": settings.arb_live_trading_enabled,
            "runtime_enabled": control["enabled"],
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
def start_trading(command: TradingCommand) -> dict[str, Any]:
    if not settings.arb_live_trading_enabled:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "deployment live-trading gate is disabled; "
                "ARB_LIVE_TRADING_ENABLED must be true"
            ),
        )

    risk = _risk_status()
    if risk["available"] is False:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="risk runtime state is unavailable",
        )
    if risk["kill_switch_active"]:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="manual kill switch is active",
        )
    if risk["circuit_breaker"]:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="risk circuit breaker is active",
        )

    state = write_control_state(enabled=True, reason=command.reason)
    return {
        "status": "started",
        "effective_enabled": True,
        "control": state,
    }


@router.post(
    "/trading/stop",
    dependencies=[Depends(require_control_auth)],
)
def stop_trading(command: TradingCommand) -> dict[str, Any]:
    state = write_control_state(enabled=False, reason=command.reason)
    return {
        "status": "stopped",
        "effective_enabled": False,
        "control": state,
    }
