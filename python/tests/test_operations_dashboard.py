from __future__ import annotations

import json
from pathlib import Path

from api import operations


def _paths(root: Path) -> tuple[Path, Path]:
    return root / "KILL_SWITCH", root / "risk_state.json"


def test_risk_status_reports_unavailable_when_runtime_is_not_mounted(
    monkeypatch,
    tmp_path: Path,
) -> None:
    missing = tmp_path / "not-mounted"
    monkeypatch.setattr(operations, "_risk_paths", lambda: _paths(missing))

    status = operations._risk_status()

    assert status["available"] is False
    assert status["state"] == "unavailable"
    assert status["kill_switch_active"] is False


def test_risk_status_does_not_claim_ready_without_persisted_state(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "risk"
    runtime.mkdir()
    monkeypatch.setattr(operations, "_risk_paths", lambda: _paths(runtime))

    status = operations._risk_status()

    assert status["available"] is True
    assert status["state"] == "no_persisted_state"
    assert status["kill_switch_active"] is False
    assert status["circuit_breaker"] is None


def test_risk_status_reports_ready_with_clear_persisted_state(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "risk"
    runtime.mkdir()
    _, state_path = _paths(runtime)
    state_path.write_text(
        json.dumps(
            {
                "circuit_breaker": None,
                "execution_failures_ms": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(operations, "_risk_paths", lambda: _paths(runtime))

    status = operations._risk_status()

    assert status["available"] is True
    assert status["state"] == "ready"
    assert status["kill_switch_active"] is False


def test_risk_status_reports_kill_switch_and_breaker(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "risk"
    runtime.mkdir()
    kill_path, state_path = _paths(runtime)
    kill_path.write_text(
        json.dumps({"reason": "operator emergency stop"}),
        encoding="utf-8",
    )
    state_path.write_text(
        json.dumps(
            {
                "circuit_breaker": {
                    "kind": "stale_market_data",
                    "tripped_at_ms": 1,
                    "detail": "book age exceeded limit",
                },
                "execution_failures_ms": [1, 2, 3],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        operations,
        "_risk_paths",
        lambda: (kill_path, state_path),
    )

    status = operations._risk_status()

    assert status["available"] is True
    assert status["state"] == "halted"
    assert status["kill_switch_active"] is True
    assert status["kill_switch_detail"] == "operator emergency stop"
    assert status["circuit_breaker"]["kind"] == "stale_market_data"
    assert status["execution_failures_recorded"] == 3
