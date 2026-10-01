from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from analytics.db import get_db
from analytics.micro_live_models import MicroLiveCycle
from analytics.models import OpportunityObservation
from api.settings import settings

router = APIRouter(prefix="/operations", tags=["operations-dashboard"])
DatabaseSession = Annotated[Session, Depends(get_db)]

REPO_ROOT = Path(__file__).resolve().parents[2]
RISK_CONFIG_PATH = REPO_ROOT / "shared" / "config" / "risk.json"


def _number(value: Any) -> float | None:
    return None if value is None else float(value)


def _risk_paths() -> tuple[Path, Path]:
    try:
        raw = json.loads(RISK_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return (
            REPO_ROOT / "data" / "risk" / "KILL_SWITCH",
            REPO_ROOT / "data" / "risk" / "risk_state.json",
        )

    def resolve(value: str, fallback: Path) -> Path:
        if not value:
            return fallback
        path = Path(value)
        if path.is_absolute():
            return path
        return (RISK_CONFIG_PATH.parent / path).resolve()

    return (
        resolve(
            str(raw.get("kill_switch_file", "")),
            REPO_ROOT / "data" / "risk" / "KILL_SWITCH",
        ),
        resolve(
            str(raw.get("state_file", "")),
            REPO_ROOT / "data" / "risk" / "risk_state.json",
        ),
    )


def _risk_status() -> dict[str, Any]:
    kill_path, state_path = _risk_paths()
    kill_active = kill_path.exists()
    kill_detail: str | None = None
    if kill_active:
        try:
            payload = json.loads(kill_path.read_text(encoding="utf-8"))
            kill_detail = payload.get("reason") or payload.get("detail")
        except (OSError, json.JSONDecodeError):
            kill_detail = "kill switch file present"

    breaker = None
    recent_failures = 0
    if state_path.exists():
        try:
            payload = json.loads(state_path.read_text(encoding="utf-8"))
            breaker = payload.get("circuit_breaker")
            failures = payload.get("execution_failures_ms") or []
            if isinstance(failures, list):
                recent_failures = len(failures)
        except (OSError, json.JSONDecodeError):
            breaker = {
                "kind": "state_unreadable",
                "detail": "risk state file could not be parsed",
            }

    if kill_active:
        state = "halted"
    elif breaker:
        state = "circuit_breaker"
    else:
        state = "ready"

    return {
        "state": state,
        "kill_switch_active": kill_active,
        "kill_switch_detail": kill_detail,
        "circuit_breaker": breaker,
        "execution_failures_recorded": recent_failures,
    }


def _recent_market_activity(db: Session) -> tuple[str, datetime | None]:
    latest = db.scalar(select(func.max(OpportunityObservation.detected_at)))
    if latest is None:
        return "offline", None

    age = datetime.now(UTC) - latest
    if age <= timedelta(seconds=5):
        return "connected", latest
    if age <= timedelta(seconds=30):
        return "stale", latest
    return "offline", latest


@router.get("/dashboard")
def dashboard(db: DatabaseSession) -> dict[str, Any]:
    now = datetime.now(UTC)
    today = datetime(now.year, now.month, now.day, tzinfo=UTC)
    week = now - timedelta(days=7)
    day_24h = now - timedelta(hours=24)

    latest_cycle = db.scalar(
        select(MicroLiveCycle)
        .order_by(desc(MicroLiveCycle.detected_at))
        .limit(1)
    )

    today_stats = db.execute(
        select(
            func.coalesce(func.sum(MicroLiveCycle.realized_pnl), 0),
            func.count(MicroLiveCycle.trade_id).filter(
                MicroLiveCycle.reconciled_at.is_not(None)
            ),
            func.count(MicroLiveCycle.trade_id).filter(
                MicroLiveCycle.realized_pnl > 0
            ),
            func.coalesce(func.sum(MicroLiveCycle.starting_capital).filter(
                MicroLiveCycle.reconciled_at.is_not(None)
            ), 0),
            func.avg(MicroLiveCycle.execution_time_ms).filter(
                MicroLiveCycle.reconciled_at.is_not(None)
            ),
        ).where(
            MicroLiveCycle.reconciled_at >= today,
        )
    ).one()

    weekly_pnl = db.scalar(
        select(func.coalesce(func.sum(MicroLiveCycle.realized_pnl), 0)).where(
            MicroLiveCycle.reconciled_at >= week,
        )
    )

    opportunity_stats = db.execute(
        select(
            func.count(OpportunityObservation.id),
            func.count(OpportunityObservation.id).filter(
                OpportunityObservation.accepted.is_(False)
            ),
            func.avg(OpportunityObservation.net_edge_bps).filter(
                OpportunityObservation.accepted.is_(True)
            ),
        ).where(OpportunityObservation.detected_at >= day_24h)
    ).one()

    websocket_status, last_market_event = _recent_market_activity(db)
    executed = int(today_stats[1] or 0)
    profitable = int(today_stats[2] or 0)
    capital = float(today_stats[3] or 0)
    pnl_today = float(today_stats[0] or 0)
    success_rate = (profitable / executed * 100.0) if executed else 0.0
    net_return = (pnl_today / capital * 100.0) if capital else 0.0

    return {
        "generated_at": now,
        "account": {
            "balance": _number(
                latest_cycle.account_balance if latest_cycle else None
            ),
            "equity_usd": _number(
                latest_cycle.account_equity_usd if latest_cycle else None
            ),
            "exposure_usd": _number(
                latest_cycle.account_exposure_usd if latest_cycle else None
            ),
            "snapshot_at": latest_cycle.detected_at if latest_cycle else None,
        },
        "performance": {
            "today_pnl": pnl_today,
            "weekly_pnl": float(weekly_pnl or 0),
            "net_return_pct": net_return,
            "detected_opportunities": int(opportunity_stats[0] or 0),
            "executed_trades": executed,
            "rejected_opportunities": int(opportunity_stats[1] or 0),
            "success_rate_pct": success_rate,
            "average_net_edge_bps": _number(opportunity_stats[2]),
            "average_latency_ms": _number(today_stats[4]),
        },
        "system": {
            "api_status": "online",
            "websocket_status": websocket_status,
            "websocket_status_source": "recent opportunity activity proxy",
            "last_market_event": last_market_event,
            "trading_enabled": settings.arb_live_trading_enabled,
            "risk": _risk_status(),
        },
    }


@router.get("/opportunities")
def recent_opportunities(
    db: DatabaseSession,
    limit: int = Query(default=50, ge=1, le=500),
) -> list[dict[str, Any]]:
    rows = db.scalars(
        select(OpportunityObservation)
        .order_by(desc(OpportunityObservation.detected_at))
        .limit(limit)
    ).all()
    return [
        {
            "id": row.id,
            "time": row.detected_at,
            "triangle": row.triangle_id,
            "route_id": row.route_id,
            "gross_edge_pct": _number(row.gross_edge_pct),
            "net_edge_pct": _number(row.net_edge_pct),
            "net_edge_bps": _number(row.net_edge_bps),
            "capital": _number(row.starting_capital),
            "status": "accepted" if row.accepted else "rejected",
            "reason_rejected": row.rejection_reason,
        }
        for row in rows
    ]


@router.get("/executions")
def recent_executions(
    db: DatabaseSession,
    limit: int = Query(default=20, ge=1, le=200),
) -> list[dict[str, Any]]:
    rows = db.scalars(
        select(MicroLiveCycle)
        .order_by(desc(MicroLiveCycle.detected_at))
        .limit(limit)
    ).all()
    return [
        {
            "trade_id": row.trade_id,
            "time": row.detected_at,
            "triangle": row.triangle_id,
            "route_id": row.route_id,
            "starting_capital": _number(row.starting_capital),
            "expected_pnl": _number(row.expected_pnl),
            "realized_pnl": _number(row.realized_pnl),
            "prediction_error": _number(row.prediction_error),
            "expected_net_edge_bps": _number(row.expected_net_edge_bps),
            "actual_slippage_bps": _number(row.actual_slippage_bps),
            "execution_time_ms": row.execution_time_ms,
            "execution_status": row.execution_status or "pending",
            "reconciled": row.reconciled_at is not None,
            "detection_leg_prices": row.detection_leg_prices,
        }
        for row in rows
    ]
