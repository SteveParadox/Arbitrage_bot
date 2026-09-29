from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from analytics.db import get_db
from analytics.opportunity_store import (
    analytics_summary,
    rejection_breakdown,
    triangle_breakdown,
)

router = APIRouter(prefix="/analytics/opportunities", tags=["opportunity-analytics"])
DatabaseSession = Annotated[Session, Depends(get_db)]


@router.get("/summary")
def opportunity_summary(
    db: DatabaseSession,
    hours: int = Query(default=24, ge=1, le=24 * 365),
) -> dict:
    return analytics_summary(db, hours)


@router.get("/rejections")
def opportunity_rejections(
    db: DatabaseSession,
    hours: int = Query(default=24, ge=1, le=24 * 365),
) -> list[dict]:
    return rejection_breakdown(db, hours)


@router.get("/triangles")
def opportunity_triangles(
    db: DatabaseSession,
    hours: int = Query(default=24, ge=1, le=24 * 365),
    limit: int = Query(default=20, ge=1, le=200),
) -> list[dict]:
    return triangle_breakdown(db, hours, limit)
