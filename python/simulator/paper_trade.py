from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from analytics.db import get_session_factory
from analytics.models import OpportunityObservation, OpportunityWindow
from simulator.book_archive import load_symbol_replays
from simulator.models import PaperSimulationResult, PaperSimulationRun
from simulator.replay import SimulationOutcome, simulate_route
from strategy.profitability import ProfitabilityConfig


DEFAULT_LATENCIES = (25, 50, 100, 200, 500)


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[2]
    default_config = repo_root / "shared" / "config" / "paper-simulation.json"

    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument("--config", type=Path, default=default_config)
    known, _ = bootstrap.parse_known_args()
    config = json.loads(known.config.read_text(encoding="utf-8"))

    parser = argparse.ArgumentParser(description="Replay detected arbitrage opportunities.")
    parser.add_argument("--config", type=Path, default=known.config)
    parser.add_argument("--hours", type=int, default=int(config.get("default_hours", 24)))
    parser.add_argument(
        "--limit",
        type=int,
        default=int(config.get("default_opportunity_limit", 10_000)),
    )
    configured_latencies = config.get("latencies_ms", list(DEFAULT_LATENCIES))
    parser.add_argument(
        "--latencies",
        default=",".join(str(value) for value in configured_latencies),
    )
    parser.add_argument(
        "--max-book-age-ms",
        type=int,
        default=int(config.get("max_book_age_ms", 1_000)),
    )
    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=int(config.get("checkpoint_interval", 100)),
    )
    parser.add_argument(
        "--profitability-config",
        type=Path,
        default=repo_root / "shared" / "config" / "profitability.json",
    )
    parser.add_argument(
        "--include-rejected",
        action="store_true",
        default=not bool(config.get("accepted_only", True)),
        help="Simulate all complete observations instead of accepted observations only.",
    )
    return parser.parse_args()


def _latencies(raw: str) -> tuple[int, ...]:
    values = tuple(sorted({int(value.strip()) for value in raw.split(",") if value.strip()}))
    if not values or any(value <= 0 for value in values):
        raise ValueError("latencies must contain positive millisecond values")
    return values


def _load_opportunities(
    session: Session,
    *,
    hours: int,
    limit: int,
    include_rejected: bool,
) -> list[tuple[OpportunityObservation, OpportunityWindow | None]]:
    if hours <= 0:
        raise ValueError("hours must be greater than zero")
    if limit <= 0:
        raise ValueError("limit must be greater than zero")

    since = datetime.now(UTC) - timedelta(hours=hours)
    statement = (
        select(OpportunityObservation, OpportunityWindow)
        .outerjoin(
            OpportunityWindow,
            OpportunityObservation.opportunity_window_id == OpportunityWindow.id,
        )
        .where(
            OpportunityObservation.detected_at >= since,
            OpportunityObservation.executable.is_(True),
        )
        .order_by(OpportunityObservation.detected_at)
        .limit(limit)
    )
    if not include_rejected:
        statement = statement.where(OpportunityObservation.accepted.is_(True))
    return list(session.execute(statement).all())


def _required_symbols(
    opportunities: list[tuple[OpportunityObservation, OpportunityWindow | None]],
) -> set[str]:
    symbols: set[str] = set()
    for observation, _ in opportunities:
        for leg in observation.raw_scan.get("legs") or []:
            symbol = leg.get("symbol")
            if symbol:
                symbols.add(str(symbol))
    return symbols


def _result_row(
    run_id: str,
    observation: OpportunityObservation,
    window: OpportunityWindow | None,
    latency_ms: int,
    outcome: SimulationOutcome,
) -> PaperSimulationResult:
    lifetime_ms = (
        int(window.duration_ms)
        if window is not None
        else int(observation.opportunity_duration_ms or 0)
    )
    total_execution_time_ms = latency_ms * 3

    return PaperSimulationResult(
        run_id=run_id,
        opportunity_id=observation.id,
        route_id=observation.route_id,
        triangle_id=observation.triangle_id,
        detected_at_ms=observation.scan_timestamp_ms,
        latency_ms=latency_ms,
        total_execution_time_ms=total_execution_time_ms,
        starting_capital=observation.starting_capital,
        expected_profit=observation.net_profit,
        expected_net_edge_bps=observation.net_edge_bps,
        detected_gross_final_amount=observation.gross_final_amount,
        simulated_final_amount=outcome.final_amount,
        simulated_profit=outcome.simulated_profit,
        simulated_net_edge_bps=outcome.simulated_net_edge_bps,
        execution_drift_amount=outcome.execution_drift_amount,
        execution_drift_bps=outcome.execution_drift_bps,
        expectation_error=outcome.expectation_error,
        completed=outcome.completed,
        fill_ratio=outcome.fill_ratio,
        failure_reason=outcome.failure_reason,
        failure_leg=outcome.failure_leg,
        opportunity_lifetime_ms=lifetime_ms,
        outlived_opportunity=lifetime_ms > 0 and total_execution_time_ms > lifetime_ms,
        max_book_age_ms=outcome.max_book_age_ms,
        legs=list(outcome.legs),
    )


def _summary(results: list[PaperSimulationResult]) -> dict[str, Any]:
    by_latency: dict[int, list[PaperSimulationResult]] = defaultdict(list)
    for result in results:
        by_latency[result.latency_ms].append(result)

    scenarios: dict[str, Any] = {}
    for latency, rows in sorted(by_latency.items()):
        completed = [row for row in rows if row.completed]
        failures = Counter(row.failure_reason or "unknown" for row in rows if not row.completed)
        expected = [
            float(row.expected_profit)
            for row in rows
            if row.expected_profit is not None
        ]
        simulated = [
            float(row.simulated_profit)
            for row in completed
            if row.simulated_profit is not None
        ]
        slippage = [
            float(row.execution_drift_bps)
            for row in completed
            if row.execution_drift_bps is not None
        ]
        lifetimes = [row.opportunity_lifetime_ms for row in rows]
        attempts = len(rows)
        fills = len(completed)

        scenarios[str(latency)] = {
            "latency_ms": latency,
            "attempts": attempts,
            "fills": fills,
            "fill_rate_pct": _pct(fills, attempts),
            "failure_rate_pct": _pct(attempts - fills, attempts),
            "avg_expected_profit": _avg(expected),
            "avg_simulated_profit": _avg(simulated),
            "total_expected_profit": round(sum(expected), 8),
            "total_simulated_profit": round(sum(simulated), 8),
            "avg_execution_drift_bps": _avg(slippage),
            "avg_opportunity_lifetime_ms": _avg(lifetimes),
            "outlived_opportunity_count": sum(row.outlived_opportunity for row in rows),
            "failure_reasons": dict(sorted(failures.items())),
        }

    return {
        "opportunity_scenarios": len(results),
        "latency_scenarios": scenarios,
    }


def _pct(numerator: int, denominator: int) -> float:
    return 0.0 if denominator == 0 else round(numerator / denominator * 100.0, 4)


def _avg(values: list[float] | list[int]) -> float | None:
    return None if not values else round(sum(values) / len(values), 8)


def run_simulation(
    session: Session,
    *,
    hours: int,
    limit: int,
    latencies: tuple[int, ...],
    max_book_age_ms: int,
    checkpoint_interval: int,
    profitability_config: Path,
    include_rejected: bool,
) -> PaperSimulationRun:
    opportunities = _load_opportunities(
        session,
        hours=hours,
        limit=limit,
        include_rejected=include_rejected,
    )
    if not opportunities:
        raise RuntimeError("no eligible opportunities found for the requested period")

    fee_config = ProfitabilityConfig.from_path(profitability_config)
    fee_bps = tuple(fee_config.fee_bps_per_leg)
    if len(fee_bps) != 3:
        raise RuntimeError("profitability configuration must contain exactly three leg fees")

    start_ms = min(row.scan_timestamp_ms for row, _ in opportunities)
    end_ms = max(row.scan_timestamp_ms for row, _ in opportunities) + max(latencies) * 3
    symbols = _required_symbols(opportunities)
    replays = load_symbol_replays(
        session,
        symbols,
        start_ms=start_ms,
        end_ms=end_ms,
        checkpoint_interval=checkpoint_interval,
    )

    missing_symbols = sorted(symbols - set(replays))
    if missing_symbols:
        raise RuntimeError(
            "book archive has no replayable history for: " + ", ".join(missing_symbols)
        )

    run = PaperSimulationRun(
        id=str(uuid4()),
        started_at=datetime.now(UTC),
        source_from_ms=start_ms,
        source_to_ms=end_ms,
        requested_limit=limit,
        opportunity_count=len(opportunities),
        scenario_count=len(opportunities) * len(latencies),
        config={
            "hours": hours,
            "latencies_ms": list(latencies),
            "max_book_age_ms": max_book_age_ms,
            "checkpoint_interval": checkpoint_interval,
            "include_rejected": include_rejected,
            "fee_profile": fee_config.fee_profile,
            "fee_bps_per_leg": [str(value) for value in fee_bps],
            "rounding_loss_bps": str(fee_config.rounding_loss_bps),
        },
        status="running",
    )
    session.add(run)
    session.flush()

    result_rows: list[PaperSimulationResult] = []
    for observation, window in opportunities:
        expected_profit = (
            Decimal(observation.net_profit) if observation.net_profit is not None else None
        )
        for latency_ms in latencies:
            outcome = simulate_route(
                observation.raw_scan,
                replays,
                latency_ms=latency_ms,
                fee_bps_per_leg=(fee_bps[0], fee_bps[1], fee_bps[2]),
                max_book_age_ms=max_book_age_ms,
                rounding_loss_bps=fee_config.rounding_loss_bps,
                expected_profit=expected_profit,
            )
            row = _result_row(run.id, observation, window, latency_ms, outcome)
            session.add(row)
            result_rows.append(row)

            if len(result_rows) % 1_000 == 0:
                session.flush()

    run.summary = _summary(result_rows)
    run.completed_at = datetime.now(UTC)
    run.status = "completed"
    session.commit()
    return run


def main() -> int:
    args = parse_args()
    latencies = _latencies(args.latencies)
    session = get_session_factory()()

    try:
        run = run_simulation(
            session,
            hours=args.hours,
            limit=args.limit,
            latencies=latencies,
            max_book_age_ms=args.max_book_age_ms,
            checkpoint_interval=args.checkpoint_interval,
            profitability_config=args.profitability_config,
            include_rejected=args.include_rejected,
        )
        print(json.dumps({"run_id": run.id, "summary": run.summary}, indent=2))
        return 0
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
