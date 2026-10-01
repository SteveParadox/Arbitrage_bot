from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from analytics.db import get_db
from analytics.performance_analytics import build_performance_analytics

router = APIRouter(prefix="/analytics/performance", tags=["performance-analytics"])
DatabaseSession = Annotated[Session, Depends(get_db)]


@router.get("")
def get_performance_analytics(
    db: DatabaseSession,
    days: int = Query(default=7, ge=1, le=90),
    base_asset: str = Query(default="USDT", min_length=2, max_length=16),
    bins: int = Query(default=10, ge=5, le=30),
) -> dict[str, Any]:
    return build_performance_analytics(
        db,
        days=days,
        base_asset=base_asset,
        bins=bins,
    )
