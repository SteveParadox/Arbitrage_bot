from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TextIO

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert

from analytics.db import get_session_factory
from analytics.shadow_models import ShadowLatencySample, ShadowObservation, ShadowRun
from api.settings import settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Persist Rust live-shadow NDJSON events to PostgreSQL."
    )
    parser.add_argument(
        "--file",
        type=Path,
        help="Read captured shadow NDJSON instead of stdin.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=settings.arb_shadow_batch_size,
    )
    return parser.parse_args()


def _dt_from_ms(value: int) -> datetime:
    return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)


def _decimal(value: object | None) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"invalid decimal value: {value}") from error


def _opportunity_values(event: dict) -> dict:
    return {
        "id": str(event["observation_id"]),
        "run_id": str(event["run_id"]),
        "detected_at": _dt_from_ms(int(event["detected_at_ms"])),
        "route_id": str(event["route_id"]),
        "triangle_id": str(event["triangle_id"]),
        "start_asset": str(event["start_asset"]),
        "starting_capital": _decimal(event["starting_capital"]),
        "detection_final_amount": _decimal(event["detection_final_amount"]),
        "detection_gross_profit": _decimal(event["detection_gross_profit"]),
        "expected_profit": _decimal(event["expected_profit"]),
        "latency_neutral_detection_profit": _decimal(
            event["latency_neutral_detection_profit"]
        ),
        "expected_net_edge_bps": _decimal(event["expected_net_edge_bps"]),
        "detected": bool(event["detected"]),
        "approved": bool(event["approved"]),
        "would_execute": bool(event["would_execute"]),
        "approval_error": event.get("approval_error"),
        "risk_checks": event.get("risk_checks") or [],
        "account_balance": _decimal(event.get("account_balance")),
        "account_equity_usd": _decimal(event.get("account_equity_usd")),
        "account_exposure_usd": _decimal(event.get("account_exposure_usd")),
        "session_pnl_proxy_usd": _decimal(event.get("session_pnl_proxy_usd")),
        "detection_leg_prices": event.get("detection_leg_prices") or [],
        "oldest_book_timestamp_ms": event.get("oldest_book_timestamp_ms"),
        "newest_book_timestamp_ms": event.get("newest_book_timestamp_ms"),
        "book_timestamp_skew_ms": event.get("book_timestamp_skew_ms"),
        "latency_tracking": bool(event["latency_tracking"]),
        "raw_event": event,
    }


def _sample_values(event: dict) -> dict:
    return {
        "run_id": str(event["run_id"]),
        "observation_id": str(event["observation_id"]),
        "route_id": str(event["route_id"]),
        "latency_ms": int(event["latency_ms"]),
        "target_at_ms": int(event["target_at_ms"]),
        "sampled_at_ms": int(event["sampled_at_ms"]),
        "scheduler_lag_ms": int(event["scheduler_lag_ms"]),
        "sample_valid": bool(event["sample_valid"]),
        "failure_reason": event.get("failure_reason"),
        "final_amount": _decimal(event.get("final_amount")),
        "net_profit": _decimal(event.get("net_profit")),
        "net_edge_bps": _decimal(event.get("net_edge_bps")),
        "profit_drift_from_detection": _decimal(
            event.get("profit_drift_from_detection")
        ),
        "route_final_drift_bps": _decimal(
            event.get("route_final_drift_bps")
        ),
        "leg_price_drift_bps": event.get("leg_price_drift_bps") or [],
        "profitable_after_latency": bool(event["profitable_after_latency"]),
        "still_meets_min_edge": bool(event["still_meets_min_edge"]),
        "leg_average_prices": event.get("leg_average_prices") or [],
        "oldest_book_timestamp_ms": event.get("oldest_book_timestamp_ms"),
        "newest_book_timestamp_ms": event.get("newest_book_timestamp_ms"),
        "book_timestamp_skew_ms": event.get("book_timestamp_skew_ms"),
        "raw_event": event,
    }


def _record_event(session, event: dict) -> bool:
    event_type = event.get("type")

    if event_type == "run_started":
        statement = insert(ShadowRun).values(
            id=str(event["run_id"]),
            started_at=_dt_from_ms(int(event["started_at_ms"])),
            base_asset=str(event["base_asset"]),
            latency_ms=event["latency_ms"],
            minimum_observations=int(event["minimum_observations"]),
            no_order_endpoints=bool(event["no_order_endpoints"]),
            mainnet_market_data=bool(event["mainnet_market_data"]),
            mainnet_read_only_account=bool(event["mainnet_read_only_account"]),
        )
        statement = (
            statement.on_conflict_do_nothing(index_elements=[ShadowRun.id])
            .returning(ShadowRun.id)
        )
        return session.execute(statement).scalar_one_or_none() is not None

    if event_type == "account_snapshot":
        session.execute(
            update(ShadowRun)
            .where(ShadowRun.id == str(event["run_id"]))
            .values(
                latest_account_snapshot=event,
                updated_at=func.now(),
            )
        )
        return True

    if event_type == "opportunity":
        values = _opportunity_values(event)
        statement = (
            insert(ShadowObservation)
            .values(**values)
            .on_conflict_do_nothing(index_elements=[ShadowObservation.id])
            .returning(ShadowObservation.id)
        )
        inserted = session.execute(statement).scalar_one_or_none()
        if inserted is None:
            return False

        session.execute(
            update(ShadowRun)
            .where(ShadowRun.id == values["run_id"])
            .values(
                observed_count=ShadowRun.observed_count + 1,
                approved_count=ShadowRun.approved_count
                + (1 if values["approved"] else 0),
                would_execute_count=ShadowRun.would_execute_count
                + (1 if values["would_execute"] else 0),
                updated_at=func.now(),
            )
        )
        return True

    if event_type == "latency_sample":
        values = _sample_values(event)
        statement = (
            insert(ShadowLatencySample)
            .values(**values)
            .on_conflict_do_nothing(
                constraint="uq_shadow_observation_latency"
            )
            .returning(ShadowLatencySample.id)
        )
        inserted = session.execute(statement).scalar_one_or_none()
        if inserted is None:
            return False

        run = session.get(ShadowRun, values["run_id"])
        if run is not None:
            expected_samples = len(run.latency_ms)
            sample_count = session.scalar(
                select(func.count(ShadowLatencySample.id)).where(
                    ShadowLatencySample.observation_id
                    == values["observation_id"]
                )
            )
            if sample_count == expected_samples:
                next_count = run.sampled_observation_count + 1
                run.sampled_observation_count = next_count
                run.ready_for_analysis = (
                    next_count >= run.minimum_observations
                )
                run.updated_at = datetime.now(timezone.utc)
        return True

    if event_type == "readiness":
        session.execute(
            update(ShadowRun)
            .where(ShadowRun.id == str(event["run_id"]))
            .values(
                observed_count=int(event["observed_count"]),
                sampled_observation_count=int(
                    event["sampled_observation_count"]
                ),
                approved_count=int(event["approved_count"]),
                would_execute_count=int(event["would_execute_count"]),
                ready_for_analysis=bool(event["ready_for_analysis"]),
                updated_at=func.now(),
            )
        )
        return True

    return False


def ingest_stream(stream: TextIO, batch_size: int) -> tuple[int, int, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    session = get_session_factory()()
    processed = 0
    duplicates_or_ignored = 0
    malformed = 0
    pending = 0

    try:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
                recorded = _record_event(session, event)
            except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
                malformed += 1
                print(
                    f"ignored malformed shadow event on line {line_number}: {error}",
                    file=sys.stderr,
                )
                continue

            if recorded:
                processed += 1
                pending += 1
            else:
                duplicates_or_ignored += 1

            if pending >= batch_size:
                session.commit()
                pending = 0

        session.commit()
        return processed, duplicates_or_ignored, malformed
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def main() -> int:
    args = parse_args()
    if args.file:
        with args.file.open("r", encoding="utf-8") as stream:
            result = ingest_stream(stream, args.batch_size)
    else:
        result = ingest_stream(sys.stdin, args.batch_size)

    processed, ignored, malformed = result
    print(
        "shadow ingest complete: "
        f"processed={processed} ignored_or_duplicate={ignored} "
        f"malformed={malformed}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
