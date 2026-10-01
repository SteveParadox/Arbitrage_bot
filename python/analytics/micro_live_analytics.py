from __future__ import annotations

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from analytics.micro_live_models import MicroLiveCycle, MicroLiveRun


def latest_run(db: Session) -> MicroLiveRun | None:
    return db.scalar(
        select(MicroLiveRun)
        .order_by(desc(MicroLiveRun.started_at))
        .limit(1)
    )


def summary(db: Session, session_id: str | None = None) -> dict:
    run = db.get(MicroLiveRun, session_id) if session_id else latest_run(db)
    if run is None:
        return {"session_id": None, "status": "no_micro_live_run"}

    row = db.execute(
        select(
            func.count(MicroLiveCycle.trade_id),
            func.count(MicroLiveCycle.prediction_error),
            func.avg(MicroLiveCycle.prediction_error),
            func.avg(func.abs(MicroLiveCycle.prediction_error)),
            func.avg(MicroLiveCycle.execution_time_ms),
            func.avg(
                MicroLiveCycle.actual_fee_amount_base
                - MicroLiveCycle.expected_fees
            ),
            func.avg(
                MicroLiveCycle.actual_slippage
                - MicroLiveCycle.expected_slippage
            ),
        ).where(MicroLiveCycle.session_id == run.id)
    ).one()

    return {
        "session_id": run.id,
        "started_at": run.started_at,
        "cycle_notional": run.cycle_notional,
        "hard_cycle_cap": run.hard_cycle_cap,
        "candidates_recorded": int(row[0] or 0),
        "reconciled_cycles": int(row[1] or 0),
        "prediction_error_definition": "realized_pnl - expected_pnl",
        "average_prediction_error": _float(row[2]),
        "mean_absolute_prediction_error": _float(row[3]),
        "average_execution_time_ms": _float(row[4]),
        "average_fee_error": _float(row[5]),
        "average_slippage_error": _float(row[6]),
        "manual_execution_required": run.manual_execution_required,
    }


def recent_cycles(
    db: Session,
    session_id: str | None = None,
    limit: int = 50,
) -> list[dict]:
    run = db.get(MicroLiveRun, session_id) if session_id else latest_run(db)
    if run is None:
        return []
    rows = db.scalars(
        select(MicroLiveCycle)
        .where(MicroLiveCycle.session_id == run.id)
        .order_by(desc(MicroLiveCycle.detected_at))
        .limit(limit)
    ).all()
    return [
        {
            "trade_id": row.trade_id,
            "route_id": row.route_id,
            "detected_at": row.detected_at,
            "starting_capital": row.starting_capital,
            "expected_pnl": row.expected_pnl,
            "realized_pnl": row.realized_pnl,
            "prediction_error": row.prediction_error,
            "expected_fees": row.expected_fees,
            "actual_fee_amount_base": row.actual_fee_amount_base,
            "expected_slippage": row.expected_slippage,
            "actual_slippage": row.actual_slippage,
            "execution_time_ms": row.execution_time_ms,
            "execution_status": row.execution_status,
            "reconciled_at": row.reconciled_at,
        }
        for row in rows
    ]


def _float(value) -> float | None:
    return None if value is None else float(value)
