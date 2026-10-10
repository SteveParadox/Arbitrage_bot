import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from api import control, runtime_control
from api.engine_client import EngineCommandError
from test_control_api import FakeEngineClient


@pytest.fixture
def state(monkeypatch, tmp_path):
    path = tmp_path / "trading_state.json"
    monkeypatch.setattr(runtime_control, "control_state_path", lambda: path)
    return path


def valid(**updates):
    return {
        "version": 1,
        "enabled": True,
        "reason": "operator enabled",
        "source": "rust_grpc_control",
        "updated_at": datetime.now(UTC).isoformat(),
        "request_id": "test-1",
        **updates,
    }


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        "bad",
        True,
        1,
        {},
        {"version": 1, "enabled": True},
        valid(version=True),
        valid(version=2),
        valid(enabled="true"),
        valid(enabled=1),
        valid(reason=" "),
        valid(reason="x" * 257),
        valid(source="unknown"),
        valid(updated_at="invalid"),
        valid(updated_at="2026-01-01T00:00:00"),
        valid(extra=True),
        valid(request_id="../x"),
        valid(updated_at=(datetime.now(UTC) + timedelta(minutes=1)).isoformat()),
        valid(updated_at=(datetime.now(UTC) - timedelta(days=2)).isoformat()),
    ],
)
def test_invalid_records_never_activate_and_remain_for_diagnosis(state, payload):
    state.write_text(json.dumps(payload))
    original = state.read_bytes()
    assert runtime_control.read_control_state()["enabled"] is False
    assert state.read_bytes() == original


def test_invalid_encoding_and_unreadable_state_are_disabled(state):
    state.write_bytes(b"\xff")
    assert runtime_control.read_control_state()["enabled"] is False
    state.unlink()
    state.mkdir()
    assert runtime_control.read_control_state()["enabled"] is False


def test_partial_invalid_writes_do_not_replace_valid_state(state):
    runtime_control.write_control_state(enabled=False, reason="valid stop")
    original = state.read_bytes()
    with pytest.raises(ValidationError):
        runtime_control.write_control_state(enabled=True, reason=" ")
    assert state.read_bytes() == original


def test_disabled_state_can_age_without_activating(state):
    state.write_text(json.dumps(valid(enabled=False, updated_at="2000-01-01T00:00:00Z")))
    assert runtime_control.read_control_state()["valid"] is True
    assert runtime_control.read_control_state()["enabled"] is False


def test_stop_intent_precedes_dispatch_and_blocks_concurrent_start(state, monkeypatch):
    class TimeoutEngine:
        async def stop(self, reason, *, request_id):
            assert runtime_control.read_control_state()["enabled"] is False
            assert runtime_control.read_stop_intent()["status"] == "STOP_REQUESTED"
            with pytest.raises(HTTPException) as error:
                await control.start_trading(control.TradingCommand(reason="racing start"))
            assert error.value.status_code == 409
            raise EngineCommandError("timeout", code_name="DEADLINE_EXCEEDED")

    state.write_text(json.dumps(valid()))
    monkeypatch.setattr(control.settings, "arb_live_trading_enabled", True)
    monkeypatch.setattr(control, "engine_grpc_client", TimeoutEngine())
    result = asyncio.run(
        control.stop_trading(control.TradingCommand(reason="stop", request_id="pending-1"))
    )
    assert result["stop_outcome"] == "STOP_UNCONFIRMED"
    assert result["effective_enabled"] is None
    assert result["exposure_confirmed_flat"] is False
    assert runtime_control.read_stop_intent()["request_id"] == "pending-1"


def test_retry_after_lost_response_confirms_same_operation(state, monkeypatch):
    class LostResponseEngine(FakeEngineClient):
        attempts = 0

        async def stop(self, reason, *, request_id):
            reply = await super().stop(reason, request_id=request_id)
            self.attempts += 1
            if self.attempts == 1:
                raise EngineCommandError("lost response", code_name="DEADLINE_EXCEEDED")
            return reply

    monkeypatch.setattr(control, "engine_grpc_client", LostResponseEngine())
    command = control.TradingCommand(reason="stop", request_id="stable-stop")
    first = asyncio.run(control.stop_trading(command))
    second = asyncio.run(control.stop_trading(command))
    assert first["engine_state_confirmed"] is False
    assert second["stop_outcome"] == "CONFIRMED_STOPPED"
    assert runtime_control.read_stop_intent()["status"] == "CONFIRMED_STOPPED"


def test_ack_without_matching_actual_engine_state_cannot_confirm(state, monkeypatch):
    class WrongState(FakeEngineClient):
        async def status(self):
            result = await super().status()
            result.runtime_enabled = True
            return result

    monkeypatch.setattr(control, "engine_grpc_client", WrongState())
    result = asyncio.run(
        control.stop_trading(control.TradingCommand(reason="stop", request_id="stop-1"))
    )
    assert result["engine_state_confirmed"] is False
    assert runtime_control.read_control_state()["enabled"] is False


def test_newer_stop_intent_cannot_be_overwritten_by_old_confirmation(state):
    runtime_control.request_stop("old", "old-stop")
    runtime_control.request_stop("new", "new-stop")
    assert runtime_control.confirm_stop("old-stop") is False
    runtime_control.mark_stop_unconfirmed("old-stop")
    assert runtime_control.read_stop_intent()["request_id"] == "new-stop"


def test_corrupt_stop_latch_blocks_valid_enabled_control(state):
    state.write_text(json.dumps(valid()))
    runtime_control.stop_intent_path().write_text("{bad")
    assert runtime_control.read_control_state()["enabled"] is False


def test_future_dated_confirmation_is_not_authoritative(state):
    state.write_text(json.dumps(valid()))
    runtime_control.stop_intent_path().write_text(json.dumps({
        "version": 1, "request_id": "future-stop", "reason": "stop",
        "status": "CONFIRMED_STOPPED",
        "updated_at": (datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
    }))
    assert runtime_control.read_control_state()["enabled"] is False
    assert runtime_control.read_stop_intent()["status"] == "STOP_UNCONFIRMED"


def test_lock_is_shared_across_handles_and_released(state):
    with runtime_control.control_lock():
        with pytest.raises(TimeoutError):
            with runtime_control.control_lock(timeout_seconds=0.01):
                pass
    with runtime_control.control_lock(timeout_seconds=0.01):
        pass


def test_cached_stop_reply_keeps_rust_provenance_for_verification(state):
    runtime_control.request_stop("stop", "cached-stop")
    state.write_text(json.dumps(valid(enabled=False, reason="stop", request_id="cached-stop")))
    runtime_control.request_stop("stop", "cached-stop")
    assert json.loads(state.read_text())["source"] == "rust_grpc_control"
    assert runtime_control.confirm_stop("cached-stop") is True


def test_uncertain_start_retains_disabled_intent(state, monkeypatch):
    class LostStart(FakeEngineClient):
        async def start(self, reason, *, request_id):
            await super().start(reason, request_id=request_id)
            raise EngineCommandError("lost start response", code_name="DEADLINE_EXCEEDED")

    state.write_text(json.dumps(valid(enabled=False)))
    monkeypatch.setattr(control.settings, "arb_live_trading_enabled", True)
    monkeypatch.setattr(control, "engine_grpc_client", LostStart())
    with pytest.raises(HTTPException):
        asyncio.run(control.start_trading(control.TradingCommand(reason="start")))
    assert runtime_control.read_control_state()["enabled"] is False
    assert runtime_control.read_stop_intent()["status"] == "STOP_REQUESTED"


def test_conflicting_duplicate_json_fields_are_not_accepted(state):
    payload = json.dumps(valid())
    state.write_text(payload[:-1] + ', "enabled": false, "enabled": true}')
    assert runtime_control.read_control_state()["valid"] is False
    assert runtime_control.read_control_state()["enabled"] is False


def test_oversized_runtime_record_is_disabled(state):
    state.write_text(json.dumps({**valid(), "reason": "x" * 20000}))
    assert runtime_control.read_control_state()["enabled"] is False
