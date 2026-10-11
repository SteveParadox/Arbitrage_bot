"""Capture fresh financial evidence through the actual autonomous runtime (test-only)."""
import argparse
import json
import os
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path

import psycopg

import test_autonomous_paper as runtime
from paper_reference import read_snapshot, verify


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if os.getenv("ARB_LIVE_TRADING_ENABLED") != "false":
        raise RuntimeError("live trading must explicitly remain disabled")

    class Factory:
        def mktemp(self, name):
            return Path(tempfile.mkdtemp(prefix=name))

    api_fixture = runtime.api.__wrapped__(Factory())
    api = next(api_fixture)
    cases = []
    try:
        for scenario in ("profitable", "completed_loss", "losing_unwind", "partial_recovery", "crash_residual"):
            directory = Factory().mktemp("audit-" + scenario)
            account = "trace-" + uuid.uuid4().hex[:16]
            runtime.create(api, account)
            process, log = runtime.start_engine(directory, account,
                                                partial=scenario == "partial_recovery",
                                                losing=scenario == "completed_loss",
                                                crash=1 if scenario == "crash_residual" else None)
            try:
                runtime.enable(api, account)
                if scenario == "losing_unwind":
                    runtime.wait(lambda: runtime.sql("SELECT o.id FROM paper_orders o JOIN paper_cycles c "
                                 "ON c.id=o.cycle_id WHERE c.account_id=%s AND o.result IS NULL", (account,)))
                    (directory/"GLOBAL_KILL").write_text("audit: recover committed intermediate inventory")
                elif scenario == "crash_residual":
                    runtime.wait(lambda: process.poll() is not None)
                    assert process.returncode == 88
                    log.close()
                    process, log = runtime.start_engine(directory, account)
                runtime.wait(lambda: runtime.sql("SELECT id FROM paper_cycles WHERE account_id=%s AND settled", (account,)))
                with psycopg.connect(runtime.URL) as connection:
                    snapshot = read_snapshot(connection, account)
                reference = verify(snapshot)
                capture = {"scenario": scenario, "account_id": account,
                           "starting_balances": {"USDT": "500"}, "reserved_capital": "100",
                           "snapshot": snapshot, "independent_reference": reference}
                for endpoint in ("status", "cycles", "orders", "performance", "valuation", "reconciliation"):
                    response = api.get(f"/paper/{account}/{endpoint}")
                    response.raise_for_status()
                    capture[endpoint] = response.json()
                assert capture["reconciliation"]["balanced"]
                cases.append(capture)
            finally:
                runtime.stop(process, log)
    finally:
        api_fixture.close()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"captured_at": datetime.now(UTC).isoformat(), "simulated": True,
                                      "market_source": "synthetic executable-depth replay", "cases": cases},
                                     default=str, indent=2))
    print(f"Captured {len(cases)} actual runtime traces with independent reconciliation: {args.output}")


if __name__ == "__main__":
    main()
