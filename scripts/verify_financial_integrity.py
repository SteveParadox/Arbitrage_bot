"""Read independently persisted financial evidence; never repair or balance discrepancies."""

import argparse
import json
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))
import psycopg
from analytics.integrity import audit
from api.infrastructure import atomic_json, postgres_options
from api.settings import settings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, default=Path(settings.arb_financial_integrity_file)
    )
    parser.add_argument("--watch-seconds", type=float, default=0)
    args = parser.parse_args()
    if args.watch_seconds and not 5 <= args.watch_seconds <= 60:
        parser.error("watch interval must be 5..60 seconds")
    while True:
        try:
            with psycopg.connect(
                settings.arb_database_url.replace("postgresql+psycopg:", "postgresql:"),
                autocommit=True,
                **postgres_options(settings),
            ) as connection:
                report = audit(connection)
        except Exception as error:
            report = {
                "checked_at": time.time(),
                "balanced": False,
                "failures": [{"category": type(error).__name__}],
            }
        atomic_json(args.output, report)
        print(json.dumps(report), flush=True)
        if not args.watch_seconds:
            raise SystemExit(0 if report["balanced"] else 1)
        time.sleep(args.watch_seconds)


if __name__ == "__main__":
    main()
