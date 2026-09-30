from __future__ import annotations

from sqlalchemy import case, desc, func, select
from sqlalchemy.orm import Session

from analytics.shadow_models import ShadowLatencySample, ShadowObservation, ShadowRun


def latest_shadow_run(db: Session) -> ShadowRun | None:
    return db.scalar(
        select(ShadowRun).order_by(desc(ShadowRun.started_at)).limit(1)
    )


def shadow_summary(db: Session, run_id: str | None = None) -> dict:
    run = db.get(ShadowRun, run_id) if run_id else latest_shadow_run(db)
    if run is None:
        return {
            "run_id": None,
            "status": "no_shadow_run",
            "ready_for_analysis": False,
        }

    completion_pct = (
        (run.sampled_observation_count / run.minimum_observations) * 100
        if run.minimum_observations
        else 0.0
    )
    account_health = None
    if run.latest_account_snapshot:
        account_health = {
            "synchronized_at_ms": run.latest_account_snapshot.get(
                "synchronized_at_ms"
            ),
            "healthy": run.latest_account_snapshot.get("healthy"),
            "detail": run.latest_account_snapshot.get("detail"),
        }

    return {
        "run_id": run.id,
        "started_at": run.started_at,
        "base_asset": run.base_asset,
        "latency_ms": run.latency_ms,
        "minimum_observations": run.minimum_observations,
        "observed_count": run.observed_count,
        "sampled_observation_count": run.sampled_observation_count,
        "approved_count": run.approved_count,
        "would_execute_count": run.would_execute_count,
        "approval_rate_pct": _pct(run.approved_count, run.observed_count),
        "would_execute_rate_pct": _pct(
            run.would_execute_count, run.observed_count
        ),
        "completion_pct": round(min(completion_pct, 100.0), 4),
        "ready_for_analysis": run.ready_for_analysis,
        "no_order_endpoints": run.no_order_endpoints,
        "mainnet_market_data": run.mainnet_market_data,
        "mainnet_read_only_account": run.mainnet_read_only_account,
        "account_health": account_health,
    }


def latency_breakdown(db: Session, run_id: str | None = None) -> list[dict]:
    run = db.get(ShadowRun, run_id) if run_id else latest_shadow_run(db)
    if run is None:
        return []

    rows = db.execute(
        select(
            ShadowLatencySample.latency_ms,
            func.count(ShadowLatencySample.id),
            func.sum(
                case((ShadowLatencySample.sample_valid.is_(True), 1), else_=0)
            ),
            func.sum(
                case(
                    (
                        ShadowLatencySample.profitable_after_latency.is_(True),
                        1,
                    ),
                    else_=0,
                )
            ),
            func.sum(
                case(
                    (ShadowLatencySample.still_meets_min_edge.is_(True), 1),
                    else_=0,
                )
            ),
            func.avg(ShadowLatencySample.net_profit),
            func.avg(ShadowLatencySample.net_edge_bps),
            func.avg(ShadowLatencySample.profit_drift_from_detection),
            func.avg(ShadowLatencySample.route_final_drift_bps),
            func.avg(ShadowLatencySample.scheduler_lag_ms),
        )
        .where(ShadowLatencySample.run_id == run.id)
        .group_by(ShadowLatencySample.latency_ms)
        .order_by(ShadowLatencySample.latency_ms)
    ).all()

    output = []
    for row in rows:
        (
            latency_ms,
            total,
            valid,
            profitable,
            meets_edge,
            avg_profit,
            avg_edge,
            avg_drift,
            avg_route_drift_bps,
            avg_scheduler_lag,
        ) = row
        valid = int(valid or 0)
        output.append(
            {
                "latency_ms": int(latency_ms),
                "sample_count": int(total),
                "valid_sample_count": valid,
                "invalid_sample_count": int(total) - valid,
                "valid_rate_pct": _pct(valid, int(total)),
                "profitable_after_latency_count": int(profitable or 0),
                "profitable_after_latency_rate_pct": _pct(
                    int(profitable or 0), valid
                ),
                "still_meets_min_edge_count": int(meets_edge or 0),
                "still_meets_min_edge_rate_pct": _pct(
                    int(meets_edge or 0), valid
                ),
                "average_net_profit": _float(avg_profit),
                "average_net_edge_bps": _float(avg_edge),
                "average_profit_drift_from_detection": _float(avg_drift),
                "average_route_final_drift_bps": _float(
                    avg_route_drift_bps
                ),
                "average_scheduler_lag_ms": _float(avg_scheduler_lag),
            }
        )
    return output


def route_breakdown(
    db: Session,
    run_id: str | None = None,
    limit: int = 25,
) -> list[dict]:
    run = db.get(ShadowRun, run_id) if run_id else latest_shadow_run(db)
    if run is None:
        return []

    rows = db.execute(
        select(
            ShadowObservation.route_id,
            func.count(ShadowObservation.id),
            func.sum(case((ShadowObservation.approved.is_(True), 1), else_=0)),
            func.avg(ShadowObservation.expected_profit),
            func.avg(ShadowObservation.expected_net_edge_bps),
        )
        .where(ShadowObservation.run_id == run.id)
        .group_by(ShadowObservation.route_id)
        .order_by(desc(func.count(ShadowObservation.id)))
        .limit(limit)
    ).all()

    return [
        {
            "route_id": route_id,
            "detected_count": int(detected),
            "approved_count": int(approved or 0),
            "approval_rate_pct": _pct(int(approved or 0), int(detected)),
            "average_expected_profit": _float(avg_profit),
            "average_expected_net_edge_bps": _float(avg_edge),
        }
        for route_id, detected, approved, avg_profit, avg_edge in rows
    ]


def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 4)


def _float(value) -> float | None:
    return None if value is None else float(value)
