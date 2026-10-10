import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.orm import Session

from analytics.db import get_db
from analytics.micro_live_analytics import recent_cycles, summary
from analytics.micro_live_models import MicroLiveCycle, MicroLiveRun
from api.control import require_control_auth
from api.settings import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analytics/micro-live", tags=["micro-live"])
DatabaseSession = Annotated[Session, Depends(get_db)]
Money = Annotated[Decimal, Field(max_digits=38, decimal_places=18)]
Fee = Annotated[Decimal, Field(max_digits=38, decimal_places=18, ge=0)]
Bps = Annotated[Decimal, Field(max_digits=20, decimal_places=8)]


class ReconcileRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    request_id: str | None = Field(
        default=None, min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]+$"
    )
    realized_pnl: Money
    actual_fee_amount_base: Fee | None = None
    actual_fees_by_currency: dict[str, Fee] = Field(default_factory=dict, max_length=32)
    actual_slippage: Money | None = None
    actual_slippage_bps: Bps | None = None
    execution_time_ms: int = Field(ge=0, le=9223372036854775807)
    execution_status: str = Field(min_length=1, max_length=64)
    notes: str | None = Field(default=None, max_length=1024)

    @field_validator("actual_fees_by_currency")
    @classmethod
    def valid_fees(cls, values: dict[str, Decimal]) -> dict[str, Decimal]:
        if any(
            not asset or len(asset) > 32 or not fee.is_finite() or fee < 0
            for asset, fee in values.items()
        ):
            raise ValueError("fee assets and amounts must be valid and nonnegative")
        return values


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


def _digest(payload: ReconcileRequest) -> str:
    def normalized(value):
        if isinstance(value, Decimal):
            if value == 0:
                return "0"
            # normalize() uses the active Decimal precision and can lose financial digits.
            exact = format(value, "f")
            return exact.rstrip("0").rstrip(".") if "." in exact else exact
        if isinstance(value, dict):
            return {key: normalized(item) for key, item in value.items()}
        return value

    body = normalized(payload.model_dump(exclude={"request_id"}))
    return hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _response(cycle: MicroLiveCycle, *, duplicate: bool) -> dict:
    return {
        "trade_id": cycle.trade_id,
        "request_id": cycle.reconciliation_request_id,
        "expected_pnl": cycle.expected_pnl,
        "realized_pnl": cycle.realized_pnl,
        "prediction_error": cycle.prediction_error,
        "definition": "realized_pnl - expected_pnl",
        "reconciled_at": cycle.reconciled_at,
        "duplicate": duplicate,
        "source": "operator_reported",
        "exchange_confirmed": False,
    }


@router.post("/reconcile/{trade_id}", dependencies=[Depends(require_control_auth)])
def reconcile_cycle(trade_id: str, payload: ReconcileRequest, db: DatabaseSession) -> dict:
    if not trade_id or len(trade_id) > 96:
        raise HTTPException(status_code=422, detail="invalid trade identity")
    if db.get_bind().dialect.name != "postgresql":
        raise HTTPException(
            status_code=503, detail="reconciliation requires PostgreSQL transaction locks"
        )
    request_id = payload.request_id or uuid.uuid4().hex
    digest = _digest(payload)
    try:
        # Both limits are transaction-local, including pooled connections.
        db.execute(
            text("SELECT set_config('lock_timeout', :timeout, true)"),
            {"timeout": f"{settings.arb_reconciliation_lock_timeout_ms}ms"},
        )
        db.execute(text("SELECT set_config('statement_timeout', '5000ms', true)"))
        cycle = db.scalar(
            select(MicroLiveCycle)
            .where(MicroLiveCycle.trade_id == trade_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if cycle is None:
            raise HTTPException(status_code=404, detail="micro-live candidate not found")
        if cycle.reconciled_at is not None:
            if cycle.reconciliation_digest == digest:
                result = _response(cycle, duplicate=True)
                db.rollback()  # release the row lock without another financial update
                return result
            raise HTTPException(
                status_code=409,
                detail="candidate already reconciled with a different or legacy payload",
            )
        run = db.scalar(
            select(MicroLiveRun)
            .where(MicroLiveRun.id == cycle.session_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if run is None:
            raise HTTPException(status_code=409, detail="candidate session is missing")
        cycle.realized_pnl = payload.realized_pnl
        cycle.prediction_error = payload.realized_pnl - cycle.expected_pnl
        cycle.actual_fee_amount_base = payload.actual_fee_amount_base
        cycle.actual_fees_by_currency = {
            asset: str(value) for asset, value in payload.actual_fees_by_currency.items()
        }
        cycle.actual_slippage = payload.actual_slippage
        cycle.actual_slippage_bps = payload.actual_slippage_bps
        cycle.execution_time_ms = payload.execution_time_ms
        cycle.execution_status = payload.execution_status
        cycle.notes = payload.notes
        cycle.reconciled_at = datetime.now(timezone.utc)
        cycle.reconciliation_digest = digest
        cycle.reconciliation_request_id = request_id
        run.reconciled_cycles += 1
        run.updated_at = datetime.now(timezone.utc)
        # Build before commit; a lost response can retry against the stored digest.
        result = _response(cycle, duplicate=False)
        db.commit()
        logger.info(
            "micro-live reconciliation committed trade_id=%s request_id=%s", trade_id, request_id
        )
        return result
    except HTTPException:
        db.rollback()
        raise
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=409, detail="reconciliation request identity conflict"
        ) from None
    except SQLAlchemyError:
        db.rollback()
        logger.warning(
            "micro-live reconciliation unavailable trade_id=%s request_id=%s", trade_id, request_id
        )
        raise HTTPException(
            status_code=503,
            detail="reconciliation unavailable; retry the same request_id and payload",
        ) from None
