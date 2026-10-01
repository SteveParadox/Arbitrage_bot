from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TextIO

from sqlalchemy import func, update
from sqlalchemy.dialects.postgresql import insert

from analytics.db import get_session_factory
from analytics.micro_live_models import MicroLiveCycle, MicroLiveRun
from api.settings import settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Persist Phase 13 micro-canary NDJSON to PostgreSQL."
    )
    parser.add_argument("--file", type=Path)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=settings.arb_micro_live_batch_size,
    )
    return parser.parse_args()


def _dt(value_ms: int) -> datetime:
    return datetime.fromtimestamp(int(value_ms) / 1000, tz=timezone.utc)


def _decimal(value: object | None) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"invalid decimal value: {value}") from error


def _candidate_values(event: dict) -> dict:
    return {
        "trade_id": str(event["trade_id"]),
        "session_id": str(event["session_id"]),
        "detected_at": _dt(int(event["detected_at_ms"])),
        "route_id": str(event["route_id"]),
        "triangle_id": str(event["triangle_id"]),
        "base_asset": str(event["base_asset"]),
        "starting_capital": _decimal(event["starting_capital"]),
        "expected_pnl": _decimal(event["expected_pnl"]),
        "expected_fees": _decimal(event["expected_fees"]),
        "expected_slippage": _decimal(event["expected_slippage"]),
        "expected_slippage_bps": _decimal(
            event["expected_slippage_bps"]
        ),
        "expected_net_edge_bps": _decimal(
            event["expected_net_edge_bps"]
        ),
        "fee_bps_per_leg": event["fee_bps_per_leg"],
        "detection_leg_prices": event["detection_leg_prices"],
        "account_balance": _decimal(event["account_balance"]),
        "account_equity_usd": _decimal(event["account_equity_usd"]),
        "account_exposure_usd": _decimal(event["account_exposure_usd"]),
        "manual_execution_required": bool(
            event["manual_execution_required"]
        ),
        "raw_candidate_event": event,
    }


def _record_event(session, event: dict) -> bool:
    event_type = event.get("type")
    if event_type == "micro_live_run":
        statement = (
            insert(MicroLiveRun)
            .values(
                id=str(event["session_id"]),
                started_at=_dt(int(event["started_at_ms"])),
                base_asset=str(event["base_asset"]),
                cycle_notional=_decimal(event["cycle_notional"]),
                hard_cycle_cap=_decimal(event["hard_cycle_cap"]),
                manual_execution_required=bool(
                    event["manual_execution_required"]
                ),
            )
            .on_conflict_do_nothing(index_elements=[MicroLiveRun.id])
            .returning(MicroLiveRun.id)
        )
        return session.execute(statement).scalar_one_or_none() is not None

    if event_type == "micro_live_candidate":
        values = _candidate_values(event)
        statement = (
            insert(MicroLiveCycle)
            .values(**values)
            .on_conflict_do_nothing(
                index_elements=[MicroLiveCycle.trade_id]
            )
            .returning(MicroLiveCycle.trade_id)
        )
        inserted = session.execute(statement).scalar_one_or_none()
        if inserted is None:
            return False
        session.execute(
            update(MicroLiveRun)
            .where(MicroLiveRun.id == values["session_id"])
            .values(
                candidates_recorded=MicroLiveRun.candidates_recorded + 1,
                updated_at=func.now(),
            )
        )
        return True

    return False


def ingest_stream(stream: TextIO, batch_size: int) -> tuple[int, int, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    session = get_session_factory()()
    processed = ignored = malformed = pending = 0
    try:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
                recorded = _record_event(session, event)
            except (
                json.JSONDecodeError,
                KeyError,
                TypeError,
                ValueError,
            ) as error:
                malformed += 1
                print(
                    f"ignored malformed micro-canary event on line "
                    f"{line_number}: {error}",
                    file=sys.stderr,
                )
                continue

            if recorded:
                processed += 1
                pending += 1
            else:
                ignored += 1
            if pending >= batch_size:
                session.commit()
                pending = 0

        session.commit()
        return processed, ignored, malformed
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
    print(
        "micro-canary ingest complete: "
        f"processed={result[0]} ignored={result[1]} malformed={result[2]}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
