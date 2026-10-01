from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from api import control, runtime_control


def test_control_state_defaults_fail_closed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "control" / "trading_state.json"
    monkeypatch.setattr(runtime_control, "control_state_path", lambda: path)

    state = runtime_control.read_control_state()

    assert state["enabled"] is False
    assert state["source"] == "default_fail_closed"


def test_control_state_write_is_readable_and_idempotent(
    monkeypatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "control" / "trading_state.json"
    monkeypatch.setattr(runtime_control, "control_state_path", lambda: path)

    first = runtime_control.write_control_state(
        enabled=True,
        reason="operator approved test",
    )
    second = runtime_control.read_control_state()

    assert first["enabled"] is True
    assert second["enabled"] is True
    assert second["reason"] == "operator approved test"

    stopped = runtime_control.write_control_state(
        enabled=False,
        reason="operator stop",
    )
    assert stopped["enabled"] is False
    assert json.loads(path.read_text(encoding="utf-8"))["enabled"] is False


def test_control_auth_rejects_missing_configuration(monkeypatch) -> None:
    monkeypatch.setattr(control.settings, "arb_control_api_token", "")

    with pytest.raises(HTTPException) as error:
        control.require_control_auth(None)

    assert error.value.status_code == 503


def test_control_auth_uses_bearer_token(monkeypatch) -> None:
    monkeypatch.setattr(
        control.settings,
        "arb_control_api_token",
        "this-is-a-test-token-with-more-than-32-characters",
    )

    with pytest.raises(HTTPException) as error:
        control.require_control_auth(
            HTTPAuthorizationCredentials(
                scheme="Bearer",
                credentials="wrong-token",
            )
        )
    assert error.value.status_code == 401

    control.require_control_auth(
        HTTPAuthorizationCredentials(
            scheme="Bearer",
            credentials="this-is-a-test-token-with-more-than-32-characters",
        )
    )


def test_control_auth_rejects_weak_configured_token(monkeypatch) -> None:
    monkeypatch.setattr(control.settings, "arb_control_api_token", "too-short")

    with pytest.raises(HTTPException) as error:
        control.require_control_auth(
            HTTPAuthorizationCredentials(
                scheme="Bearer",
                credentials="too-short",
            )
        )

    assert error.value.status_code == 503


def test_start_requires_deployment_gate_and_clear_risk(
    monkeypatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "control" / "trading_state.json"
    monkeypatch.setattr(runtime_control, "control_state_path", lambda: path)
    monkeypatch.setattr(control.settings, "arb_live_trading_enabled", False)
    monkeypatch.setattr(
        control,
        "_risk_status",
        lambda: {
            "available": True,
            "kill_switch_active": False,
            "circuit_breaker": None,
        },
    )

    with pytest.raises(HTTPException) as error:
        control.start_trading(control.TradingCommand(reason="test start"))

    assert error.value.status_code == 409
    assert runtime_control.read_control_state()["enabled"] is False

    monkeypatch.setattr(control.settings, "arb_live_trading_enabled", True)
    result = control.start_trading(
        control.TradingCommand(reason="test start")
    )

    assert result["status"] == "started"
    assert runtime_control.read_control_state()["enabled"] is True


def test_stop_is_idempotent_and_does_not_require_live_deployment_gate(
    monkeypatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "control" / "trading_state.json"
    monkeypatch.setattr(runtime_control, "control_state_path", lambda: path)
    monkeypatch.setattr(control.settings, "arb_live_trading_enabled", False)

    first = control.stop_trading(
        control.TradingCommand(reason="operator stop")
    )
    second = control.stop_trading(
        control.TradingCommand(reason="operator stop again")
    )

    assert first["status"] == "stopped"
    assert second["status"] == "stopped"
    assert runtime_control.read_control_state()["enabled"] is False
