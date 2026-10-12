"""Clear a restore barrier only after stopped ownership and explicit evidence review.

Never enables execution. The operator must physically fence the previous deployment.
"""

import argparse
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))
import psycopg
from api.settings import settings
from api.infrastructure import read_file, postgres_options
from analytics.integrity import audit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--previous-site-fenced", action="store_true", required=True)
    parser.add_argument(
        "--confirm-reconciliation-reviewed", action="store_true", required=True
    )
    args = parser.parse_args()
    if not args.previous_site_fenced or not args.confirm_reconciliation_reviewed:
        parser.error("explicit disaster-recovery confirmations required")
    marker = Path(settings.arb_restore_marker)
    restored = json.loads(read_file(marker))["restored_at"]
    with psycopg.connect(
        settings.arb_database_url.replace("postgresql+psycopg:", "postgresql:"),
        **postgres_options(settings),
    ) as connection:
        for table, domain in (("paper_accounts", 8149), ("execution_accounts", 9150)):
            for account, enabled in connection.execute(
                f"SELECT id,enabled FROM {table} ORDER BY id FOR UPDATE"
            ):
                if (
                    enabled
                    or not connection.execute(
                        "SELECT pg_try_advisory_xact_lock(hashtextextended(%s,%s))",
                        (account, domain),
                    ).fetchone()[0]
                ):
                    raise RuntimeError(
                        "recovery acknowledgement requires disabled stopped owners"
                    )
        if connection.execute(
            "SELECT EXISTS(SELECT 1 FROM paper_cycles WHERE NOT settled) OR EXISTS(SELECT 1 FROM execution_cycles WHERE NOT settled)"
        ).fetchone()[0]:
            raise RuntimeError("incomplete financial cycles require recovery")
        if connection.execute(
            "SELECT EXISTS(SELECT 1 FROM execution_accounts a WHERE NOT EXISTS(SELECT 1 FROM execution_reconciliation r WHERE r.account_id=a.id AND r.healthy AND r.created_at>to_timestamp(%s)))",
            (restored,),
        ).fetchone()[0]:
            raise RuntimeError("fresh post-restore exchange reconciliation required")
        with psycopg.connect(
            settings.arb_database_url.replace("postgresql+psycopg:", "postgresql:"),
            autocommit=True,
            **postgres_options(settings),
        ) as verifier:
            if not audit(verifier)["balanced"]:
                raise RuntimeError("independent integrity check failed")
        connection.execute("UPDATE paper_accounts SET enabled=false,heartbeat=NULL")
        connection.execute("UPDATE execution_accounts SET enabled=false,heartbeat=NULL")
    marker.replace(marker.with_name("RESTORE_ACKNOWLEDGED_" + str(time.time_ns())))
    print('{"acknowledged":true,"execution_authorized":false}')


if __name__ == "__main__":
    main()
