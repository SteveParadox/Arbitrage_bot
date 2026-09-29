from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import TextIO

from sqlalchemy.dialects.postgresql import insert

from analytics.db import get_session_factory
from simulator.models import MarketBookEvent


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
    if event.get("type") != "order_book":
        return None
    return {
        "symbol": str(event["symbol"]),
        "event_timestamp_ms": int(event["timestamp"]),
        "update_id": int(event["update_id"]),
        "sequence": int(event["sequence"]),
        "is_snapshot": bool(event["is_snapshot"]),
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
            except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
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
