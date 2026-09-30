from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from analytics.db import get_db
from analytics.shadow_analytics import (
    latency_breakdown,
    route_breakdown,
    shadow_summary,
)
from analytics.shadow_models import ShadowRun

router = APIRouter(prefix="/analytics/shadow", tags=["live-shadow"])
DatabaseSession = Annotated[Session, Depends(get_db)]


@router.get("/runs")
def list_shadow_runs(
    db: DatabaseSession,
    limit: int = Query(default=20, ge=1, le=200),
) -> list[dict]:
    rows = db.scalars(
        select(ShadowRun)
        .order_by(desc(ShadowRun.started_at))
        .limit(limit)
    ).all()
    return [
        {
            "id": row.id,
            "started_at": row.started_at,
            "base_asset": row.base_asset,
            "observed_count": row.observed_count,
            "sampled_observation_count": row.sampled_observation_count,
            "minimum_observations": row.minimum_observations,
            "approved_count": row.approved_count,
            "would_execute_count": row.would_execute_count,
            "ready_for_analysis": row.ready_for_analysis,
        }
        for row in rows
    ]


@router.get("/summary")
def get_shadow_summary(
    db: DatabaseSession,
    run_id: str | None = None,
) -> dict:
    return shadow_summary(db, run_id)


@router.get("/latencies")
def get_shadow_latencies(
    db: DatabaseSession,
    run_id: str | None = None,
) -> list[dict]:
    return latency_breakdown(db, run_id)


@router.get("/routes")
def get_shadow_routes(
    db: DatabaseSession,
    run_id: str | None = None,
    limit: int = Query(default=25, ge=1, le=200),
) -> list[dict]:
    return route_breakdown(db, run_id, limit)
