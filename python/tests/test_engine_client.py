from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from api import engine_client


class FakeChannel:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


def test_reply_rejects_mismatched_request_id() -> None:
    reply = SimpleNamespace(
        accepted=True,
        command="stop_trading",
        request_id="wrong",
        detail="ok",
        applied_at_ms=1,
    )

    with pytest.raises(engine_client.EngineCommandError) as error:
        engine_client._reply(
            reply,
            expected_command="stop_trading",
            request_id="expected",
        )

    assert error.value.code_name == "INVALID_RESPONSE"


def test_reply_rejects_wrong_command_and_negative_ack() -> None:
    wrong_command = SimpleNamespace(
        accepted=True,
        command="start_trading",
        request_id="request-1",
        detail="ok",
        applied_at_ms=1,
    )
    with pytest.raises(engine_client.EngineCommandError):
        engine_client._reply(
            wrong_command,
            expected_command="stop_trading",
            request_id="request-1",
        )

    rejected = SimpleNamespace(
        accepted=False,
        command="stop_trading",
        request_id="request-1",
        detail="not accepted",
        applied_at_ms=1,
    )
    with pytest.raises(engine_client.EngineCommandError) as error:
        engine_client._reply(
            rejected,
            expected_command="stop_trading",
            request_id="request-1",
        )
    assert error.value.code_name == "FAILED_PRECONDITION"


def test_call_passes_configured_timeout(monkeypatch) -> None:
    observed: dict[str, object] = {}

    async def fake_rpc(_request, *, metadata, timeout):
        observed["metadata"] = metadata
        observed["timeout"] = timeout
        return SimpleNamespace()

    class FakeStub:
        def __init__(self, _channel):
            self.GetStatus = fake_rpc

    monkeypatch.setattr(
        engine_client.grpc.aio,
        "insecure_channel",
        lambda _target: FakeChannel(),
    )
    monkeypatch.setattr(
        engine_client.engine_control_pb2_grpc,
        "EngineControlStub",
        FakeStub,
    )
    monkeypatch.setattr(
        engine_client.settings,
        "arb_engine_grpc_timeout_seconds",
        0.25,
    )

    client = engine_client.EngineGrpcClient()
    client._token = "x" * 40
    asyncio.run(client._call("GetStatus", object()))

    assert observed["timeout"] == 0.25
    assert observed["metadata"] == (("x-engine-token", "x" * 40),)


def test_call_rejects_nonpositive_timeout(monkeypatch) -> None:
    monkeypatch.setattr(
        engine_client.settings,
        "arb_engine_grpc_timeout_seconds",
        0,
    )
    client = engine_client.EngineGrpcClient()
    client._token = "x" * 40

    with pytest.raises(engine_client.EngineCommandError) as error:
        asyncio.run(client._call("GetStatus", object()))

    assert error.value.code_name == "INVALID_CONFIGURATION"



def test_deadline_exceeded_is_mapped_to_engine_command_error(
    monkeypatch,
) -> None:
    class FakeRpcError(Exception):
        def details(self):
            return "deadline exceeded"

        def code(self):
            return SimpleNamespace(name="DEADLINE_EXCEEDED")

    async def fake_rpc(_request, *, metadata, timeout):
        assert metadata
        assert timeout > 0
        raise FakeRpcError("deadline")

    class FakeStub:
        def __init__(self, _channel):
            self.GetStatus = fake_rpc

    monkeypatch.setattr(
        engine_client.grpc.aio,
        "AioRpcError",
        FakeRpcError,
    )
    monkeypatch.setattr(
        engine_client.grpc.aio,
        "insecure_channel",
        lambda _target: FakeChannel(),
    )
    monkeypatch.setattr(
        engine_client.engine_control_pb2_grpc,
        "EngineControlStub",
        FakeStub,
    )
    monkeypatch.setattr(
        engine_client.settings,
        "arb_engine_grpc_timeout_seconds",
        0.1,
    )

    client = engine_client.EngineGrpcClient()
    client._token = "x" * 40
    with pytest.raises(engine_client.EngineCommandError) as error:
        asyncio.run(client._call("GetStatus", object()))

    assert error.value.code_name == "DEADLINE_EXCEEDED"
    assert "deadline exceeded" in str(error.value)
