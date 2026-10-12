"""Offline tracked-source scan and bounded private-key history guard.

Known synthetic fixture findings require exact path/type/hash entries in the baseline.
No configured production secrets are passed to external verification services.
"""

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        scan = subprocess.run(
            ["detect-secrets", "scan", "--no-verify"],
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=120,
            check=True,
        )
        result = json.loads(scan.stdout)
        baseline = json.loads((ROOT / "scripts/secrets-baseline.json").read_text())
        allowed = {
            (row["path"], row["type"], row["hashed_secret"])
            for row in baseline["findings"]
        }
        unexpected = [
            {"path": path, **row}
            for path, rows in result["results"].items()
            for row in rows
            if (path, row["type"], row["hashed_secret"]) not in allowed
        ]
        history = subprocess.Popen(
            ["git", "log", "-p", "--all", "--format=commit:%H"],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        size = 0
        private_keys = 0
        for line in history.stdout:
            size += len(line)
            if size > 32 * 1024 * 1024:
                history.kill()
                raise RuntimeError("history scan exceeds configured memory/work bound")
            if re.search(
                rb"^\+-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----", line
            ):
                private_keys += 1
        if history.wait(timeout=10):
            raise RuntimeError("history scan failed")
        report = {
            "checked_at": datetime.now(UTC).isoformat(),
            "source_commit": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
            ).strip(),
            "tool": result["version"],
            "offline": True,
            "passed": not unexpected and private_keys == 0,
            "findings": unexpected,
            "baselined_findings": len(allowed),
            "history_private_key_findings": private_keys,
            "history_bytes_scanned": size,
        }
    except Exception as error:
        report = {
            "checked_at": datetime.now(UTC).isoformat(),
            "passed": False,
            "status": "BLOCKED",
            "failure_category": type(error).__name__,
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
