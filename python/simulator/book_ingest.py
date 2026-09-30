from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import TextIO

from sqlalchemy.dialects.postgresql import insert

from analytics.db import get_session_factory
from simulator.models import MarketBookEvent
from simulator.book_archive import _decode_levels


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Archive normalized Rust order-book events for paper-trading replay."
    )
    parser.add_argument(
        "--file",
        type=Path,
        help="Read captured market-data NDJSON instead of stdin.",
    )
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument(
        "--passthrough",
        action="store_true",
        help="Echo every input line to stdout after parsing.",
    )
    return parser.parse_args()


def _book_values(event: dict) -> dict | None:
    if not isinstance(event, dict):
        raise ValueError("market event must be an object")
    if event.get("type") != "order_book":
        return None
    if not isinstance(event.get("is_snapshot"), bool):
        raise ValueError("is_snapshot must be boolean")
    if not isinstance(event.get("symbol"), str) or not event["symbol"].strip():
        raise ValueError("symbol must be nonempty")
    for field in ("timestamp", "update_id", "sequence"):
        value = event[field]
        if type(value) is not int or not 0 <= value < 2**63:
            raise ValueError(f"{field} must be a nonnegative signed-64-bit integer")
    if event["update_id"] == 0:
        raise ValueError("update_id must be positive")
    for side in ("bids", "asks"):
        if not isinstance(event.get(side), list):
            raise ValueError("book sides must be arrays")
        _decode_levels(event[side])
    return {
        "symbol": str(event["symbol"]),
        "event_timestamp_ms": int(event["timestamp"]),
        "update_id": int(event["update_id"]),
        "sequence": int(event["sequence"]),
        "is_snapshot": event["is_snapshot"] or event["update_id"] == 1,
        "bids": event.get("bids") or [],
        "asks": event.get("asks") or [],
    }


def ingest_stream(
    stream: TextIO,
    *,
    batch_size: int,
    passthrough: bool = False,
) -> tuple[int, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    session = get_session_factory()()
    buffered: list[dict] = []
    archived = 0
    malformed = 0

    def flush() -> None:
        nonlocal archived
        if not buffered:
            return
        statement = (
            insert(MarketBookEvent)
            .values(buffered)
            .on_conflict_do_nothing(
                constraint="uq_market_book_event_identity"
            )
            .returning(MarketBookEvent.id)
        )
        archived += len(session.execute(statement).scalars().all())
        session.commit()
        buffered.clear()

    try:
        for line_number, line in enumerate(stream, start=1):
            if passthrough:
                sys.stdout.write(line)
                sys.stdout.flush()

            if not line.strip():
                continue

            try:
                event = json.loads(line)
                values = _book_values(event)
            except (json.JSONDecodeError, KeyError, TypeError, ValueError,
                    IndexError, OverflowError) as error:
                malformed += 1
                print(
                    f"ignored malformed market-data line {line_number}: {error}",
                    file=sys.stderr,
                )
                continue

            if values is None:
                continue

            buffered.append(values)
            if len(buffered) >= batch_size:
                flush()

        flush()
        return archived, malformed
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def main() -> int:
    args = parse_args()
    if args.file:
        with args.file.open("r", encoding="utf-8") as stream:
            archived, malformed = ingest_stream(
                stream,
                batch_size=args.batch_size,
                passthrough=False,
            )
    else:
        archived, malformed = ingest_stream(
            sys.stdin,
            batch_size=args.batch_size,
            passthrough=args.passthrough,
        )

    print(
        f"book archive complete: archived={archived} malformed={malformed}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
