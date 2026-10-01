from datetime import datetime, timezone
from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from analytics.db import get_db
from analytics.micro_live_analytics import recent_cycles, summary
from analytics.micro_live_models import MicroLiveCycle, MicroLiveRun
from api.control import require_control_auth

router = APIRouter(prefix="/analytics/micro-live", tags=["micro-live"])
DatabaseSession = Annotated[Session, Depends(get_db)]


class ReconcileRequest(BaseModel):
    realized_pnl: Decimal
    actual_fee_amount_base: Decimal | None = Field(default=None, ge=0)
    actual_fees_by_currency: dict[str, Decimal] = Field(default_factory=dict)
    actual_slippage: Decimal | None = None
    actual_slippage_bps: Decimal | None = None
    execution_time_ms: int = Field(ge=0)
    execution_status: str = Field(min_length=1, max_length=64)
    notes: str | None = Field(default=None, max_length=1024)


@router.get("/summary")
def get_summary(
    db: DatabaseSession,
    session_id: str | None = None,
) -> dict:
    return summary(db, session_id)


@router.get("/cycles")
def get_cycles(
    db: DatabaseSession,
    session_id: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
) -> list[dict]:
    return recent_cycles(db, session_id, limit)


@router.post(
    "/reconcile/{trade_id}",
    dependencies=[Depends(require_control_auth)],
)
def reconcile_cycle(
    trade_id: str,
    payload: ReconcileRequest,
    db: DatabaseSession,
) -> dict:
    cycle = db.scalar(
        select(MicroLiveCycle)
        .where(MicroLiveCycle.trade_id == trade_id)
        .with_for_update()
    )
    if cycle is None:
        raise HTTPException(status_code=404, detail="micro-live candidate not found")
    if cycle.reconciled_at is not None:
        raise HTTPException(
            status_code=409,
            detail="micro-live candidate is already reconciled",
        )

    cycle.realized_pnl = payload.realized_pnl
    cycle.prediction_error = payload.realized_pnl - cycle.expected_pnl
    cycle.actual_fee_amount_base = payload.actual_fee_amount_base
    cycle.actual_fees_by_currency = {
        asset: str(value)
        for asset, value in payload.actual_fees_by_currency.items()
    }
    cycle.actual_slippage = payload.actual_slippage
    cycle.actual_slippage_bps = payload.actual_slippage_bps
    cycle.execution_time_ms = payload.execution_time_ms
    cycle.execution_status = payload.execution_status
    cycle.notes = payload.notes
    cycle.reconciled_at = datetime.now(timezone.utc)

    run = db.get(MicroLiveRun, cycle.session_id)
    if run is not None:
        run.reconciled_cycles += 1
        run.updated_at = datetime.now(timezone.utc)

    db.commit()
    db.refresh(cycle)

    return {
        "trade_id": cycle.trade_id,
        "expected_pnl": cycle.expected_pnl,
        "realized_pnl": cycle.realized_pnl,
        "prediction_error": cycle.prediction_error,
        "definition": "realized_pnl - expected_pnl",
        "reconciled_at": cycle.reconciled_at,
    }
