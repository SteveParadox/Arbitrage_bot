from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from analytics.db import get_db
from simulator.models import PaperSimulationResult, PaperSimulationRun

router = APIRouter(prefix="/analytics/paper-trading", tags=["paper-trading"])
DatabaseSession = Annotated[Session, Depends(get_db)]


@router.get("/runs")
def list_runs(
    db: DatabaseSession,
    limit: int = Query(default=20, ge=1, le=200),
) -> list[dict]:
    rows = db.scalars(
        select(PaperSimulationRun)
        .order_by(desc(PaperSimulationRun.started_at))
        .limit(limit)
    ).all()
    return [
        {
            "id": row.id,
            "started_at": row.started_at,
            "completed_at": row.completed_at,
            "status": row.status,
            "opportunity_count": row.opportunity_count,
            "scenario_count": row.scenario_count,
            "config": row.config,
            "summary": row.summary,
        }
        for row in rows
    ]


@router.get("/runs/{run_id}")
def get_run(run_id: str, db: DatabaseSession) -> dict:
    run = db.get(PaperSimulationRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="simulation run not found")

    return {
        "id": run.id,
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "status": run.status,
        "opportunity_count": run.opportunity_count,
        "scenario_count": run.scenario_count,
        "config": run.config,
        "summary": run.summary,
        "failure_reason": run.failure_reason,
    }


@router.get("/runs/{run_id}/failures")
def get_failures(
    run_id: str,
    db: DatabaseSession,
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[dict]:
    if db.get(PaperSimulationRun, run_id) is None:
        raise HTTPException(status_code=404, detail="simulation run not found")

    rows = db.scalars(
        select(PaperSimulationResult)
        .where(
            PaperSimulationResult.run_id == run_id,
            PaperSimulationResult.completed.is_(False),
        )
        .order_by(PaperSimulationResult.latency_ms, PaperSimulationResult.id)
        .limit(limit)
    ).all()
    return [
        {
            "opportunity_id": row.opportunity_id,
            "route_id": row.route_id,
            "latency_ms": row.latency_ms,
            "failure_reason": row.failure_reason,
            "failure_leg": row.failure_leg,
            "fill_ratio": float(row.fill_ratio),
            "opportunity_lifetime_ms": row.opportunity_lifetime_ms,
            "remaining_lifetime_ms": row.remaining_lifetime_ms,
        }
        for row in rows
    ]
