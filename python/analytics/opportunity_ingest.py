from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal, DecimalException
from pathlib import Path
from typing import TextIO

from analytics.db import get_session_factory
from analytics.opportunity_store import OpportunityStore
from api.settings import settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Persist Phase 5/6 scanner NDJSON observations to PostgreSQL."
    )
    parser.add_argument(
        "--file",
        type=Path,
        help="Backfill from an NDJSON file. Omit to read the live scanner stream from stdin.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=settings.arb_opportunity_batch_size,
    )
    return parser.parse_args()


def ingest_stream(stream: TextIO, batch_size: int) -> tuple[int, int, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    session = get_session_factory()()
    inserted = 0
    duplicates = 0
    malformed = 0

    try:
        store = OpportunityStore(
            session,
            min_net_edge_bps=Decimal(str(settings.arb_opportunity_min_net_bps)),
            max_continuity_gap_ms=settings.arb_opportunity_max_gap_ms,
        )
        pending = 0

        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue

            try:
                scan = json.loads(line)
            except json.JSONDecodeError as error:
                malformed += 1
                print(
                    f"ignored malformed JSON on line {line_number}: {error}",
                    file=sys.stderr,
                )
                continue

            try:
                was_inserted = store.record_scan(scan)
            except (KeyError, TypeError, ValueError, OverflowError, DecimalException) as error:
                malformed += 1
                print(
                    f"ignored invalid scan on line {line_number}: {error}",
                    file=sys.stderr,
                )
                continue

            if was_inserted:
                inserted += 1
                pending += 1
            else:
                duplicates += 1

            if pending >= batch_size:
                session.commit()
                pending = 0

        session.commit()
        return inserted, duplicates, malformed
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def main() -> int:
    args = parse_args()
    if args.file:
        with args.file.open("r", encoding="utf-8") as stream:
            inserted, duplicates, malformed = ingest_stream(stream, args.batch_size)
    else:
        inserted, duplicates, malformed = ingest_stream(sys.stdin, args.batch_size)

    print(
        f"opportunity ingest complete: inserted={inserted} "
        f"duplicates={duplicates} malformed={malformed}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
