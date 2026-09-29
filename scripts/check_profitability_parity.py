#!/usr/bin/env python3
"""Assert that Python and Rust profitability engines emit identical canonical outputs."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

from strategy.profitability import evaluate_fixture_case  # noqa: E402


def main() -> int:
    fixture_path = ROOT / "shared" / "tests" / "profitability_cases.json"
    cases = json.loads(fixture_path.read_text(encoding="utf-8"))

    python_results = [
        {"name": case["name"], "result": evaluate_fixture_case(case)}
        for case in cases
    ]

    command = [
        "cargo",
        "run",
        "--quiet",
        "--manifest-path",
        str(ROOT / "rust" / "Cargo.toml"),
        "-p",
        "scanner",
        "--bin",
        "profitability-fixture",
        "--",
        str(fixture_path),
    ]
    rust = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    rust_results = json.loads(rust.stdout)

    if python_results != rust_results:
        print("Python/Rust profitability parity failed.", file=sys.stderr)
        print("Python:", json.dumps(python_results, indent=2), file=sys.stderr)
        print("Rust:", json.dumps(rust_results, indent=2), file=sys.stderr)
        return 1

    print(f"profitability parity passed for {len(cases)} shared cases")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
