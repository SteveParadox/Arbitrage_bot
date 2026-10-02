from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from api import control, runtime_control
from api.engine_client import EngineCommandError


class FakeEngineClient:
    async def start(self, _reason: str, *, request_id: str | None = None):
        return SimpleNamespace(
            accepted=True,
            command="start_trading",
            request_id=request_id or "start-1",
            detail="started",
            applied_at_ms=1,
        )

    async def stop(self, _reason: str, *, request_id: str | None = None):
        return SimpleNamespace(
            accepted=True,
            command="stop_trading",
            request_id=request_id or "stop-1",
            detail="stopped",
            applied_at_ms=2,
        )

    async def update_limits(self, **_values):
        request_id = _values.get("request_id")
        return SimpleNamespace(
            accepted=True,
            command="update_limits",
            request_id=request_id or "limits-1",
            detail="updated",
            applied_at_ms=3,
        )

    async def reload_strategy(self, _reason: str, *, request_id: str | None = None):
        return SimpleNamespace(
            accepted=True,
            command="reload_strategy",
            request_id=request_id or "reload-1",
            detail="reloaded",
            applied_at_ms=4,
        )


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
    token = "this-is-a-test-token-with-more-than-32-characters"
    monkeypatch.setattr(control.settings, "arb_control_api_token", token)

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
            credentials=token,
        )
    )


def test_start_requires_deployment_gate_and_clear_risk(monkeypatch) -> None:
    monkeypatch.setattr(control.settings, "arb_live_trading_enabled", False)
    monkeypatch.setattr(control, "engine_grpc_client", FakeEngineClient())
    with pytest.raises(HTTPException) as error:
        asyncio.run(
            control.start_trading(
                control.TradingCommand(reason="test start")
            )
        )
    assert error.value.status_code == 409

    monkeypatch.setattr(control.settings, "arb_live_trading_enabled", True)
    result = asyncio.run(
        control.start_trading(
            control.TradingCommand(reason="test start")
        )
    )
    assert result["status"] == "started"
    assert result["command"]["accepted"] is True


def test_stop_uses_grpc_and_is_idempotent(monkeypatch) -> None:
    monkeypatch.setattr(control, "engine_grpc_client", FakeEngineClient())

    first = asyncio.run(
        control.stop_trading(
            control.TradingCommand(reason="operator stop")
        )
    )
    second = asyncio.run(
        control.stop_trading(
            control.TradingCommand(reason="operator stop again")
        )
    )

    assert first["status"] == "stopped"
    assert second["status"] == "stopped"


def test_stop_falls_back_fail_closed_when_grpc_is_down(
    monkeypatch,
    tmp_path: Path,
) -> None:
    class BrokenEngine:
        async def stop(self, _reason: str, *, request_id: str | None = None):
            raise EngineCommandError("unavailable")

    path = tmp_path / "control" / "trading_state.json"
    monkeypatch.setattr(runtime_control, "control_state_path", lambda: path)
    monkeypatch.setattr(control, "engine_grpc_client", BrokenEngine())

    result = asyncio.run(
        control.stop_trading(
            control.TradingCommand(reason="emergency stop")
        )
    )

    assert result["status"] == "stop_requested_fallback"
    assert result["engine_state_confirmed"] is False
    assert result["effective_enabled"] is None
    assert runtime_control.read_control_state()["enabled"] is False


def test_update_limits_and_reload_use_grpc(monkeypatch) -> None:
    monkeypatch.setattr(control, "engine_grpc_client", FakeEngineClient())

    limits = asyncio.run(
        control.update_limits(
            control.RiskLimitsCommand(max_trade_size="25")
        )
    )
    reload_result = asyncio.run(
        control.reload_strategy(
            control.TradingCommand(reason="new routes")
        )
    )

    assert limits["status"] == "updated"
    assert reload_result["status"] == "reload_requested"


def test_control_preserves_explicit_request_id(monkeypatch) -> None:
    monkeypatch.setattr(control.settings, "arb_live_trading_enabled", True)
    monkeypatch.setattr(control, "engine_grpc_client", FakeEngineClient())
    request_id = "stable-control-request-123"

    start = asyncio.run(
        control.start_trading(
            control.TradingCommand(reason="test start", request_id=request_id)
        )
    )
    stop = asyncio.run(
        control.stop_trading(
            control.TradingCommand(reason="test stop", request_id=request_id)
        )
    )
    limits = asyncio.run(
        control.update_limits(
            control.RiskLimitsCommand(
                max_trade_size="25",
                request_id=request_id,
            )
        )
    )
    reload_result = asyncio.run(
        control.reload_strategy(
            control.TradingCommand(reason="test reload", request_id=request_id)
        )
    )

    assert start["command"]["request_id"] == request_id
    assert stop["command"]["request_id"] == request_id
    assert limits["command"]["request_id"] == request_id
    assert reload_result["command"]["request_id"] == request_id
