from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import time
import uuid
from dataclasses import replace
from pathlib import Path

import grpc
import pytest
from redis import Redis
from sqlalchemy import delete, func, select

from analytics.db import get_session_factory
from analytics.engine_event_models import EngineEvent
from api import event_consumer
from api.engine_client import EngineCommandError, EngineGrpcClient
from api.grpc import engine_control_pb2, engine_control_pb2_grpc

pytestmark = pytest.mark.skipif(
    os.getenv("ARB_RUN_GRPC_INTEGRATION") != "1",
    reason="requires the Rust engine-service, Redis, and PostgreSQL",
)


def _metadata() -> tuple[tuple[str, str], ...]:
    return (("x-engine-token", os.environ["ARB_ENGINE_GRPC_TOKEN"]),)


async def _wait_for_status(
    client: EngineGrpcClient,
    timeout_seconds: float = 20.0,
):
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return await client.status()
        except EngineCommandError as error:
            last_error = error
            await asyncio.sleep(0.25)
    raise AssertionError(
        f"Rust gRPC service did not become ready: {last_error}"
    )


def _redis() -> Redis:
    return Redis.from_url(
        os.environ["ARB_REDIS_URL"],
        decode_responses=True,
    )


def _command_events(request_id: str) -> list[tuple[str, dict[str, str]]]:
    rows: list[tuple[str, dict[str, str]]] = []
    for stream_id, fields in _redis().xrange(
        os.environ.get("ARB_EVENT_STREAM", "arb.events"),
        min="-",
        max="+",
    ):
        if fields.get("event_type") != "engine.state_changed":
            continue
        payload = json.loads(fields.get("payload", "{}"))
        if payload.get("request_id") == request_id:
            rows.append((stream_id, fields))
    return rows


def _wait_for_command_event(
    request_id: str,
    timeout_seconds: float = 10.0,
) -> tuple[str, dict[str, str]]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        rows = _command_events(request_id)
        if rows:
            return rows[-1]
        time.sleep(0.1)
    raise AssertionError(
        f"Redis event for command request_id={request_id} was not observed"
    )


def _strategy_state() -> dict:
    return json.loads(
        Path(os.environ["ARB_STRATEGY_RELOAD_FILE"]).read_text(
            encoding="utf-8"
        )
    )


async def _raw_reload(
    request_id: str,
    reason: str,
    timeout: float,
):
    async with grpc.aio.insecure_channel(
        os.environ["ARB_ENGINE_GRPC_TARGET"]
    ) as channel:
        stub = engine_control_pb2_grpc.EngineControlStub(channel)
        return await stub.ReloadStrategy(
            engine_control_pb2.ReloadStrategyRequest(
                request_id=request_id,
                reason=reason,
            ),
            metadata=_metadata(),
            timeout=timeout,
        )


def _restart_engine_service() -> None:
    pid_path = Path(os.environ["ARB_ENGINE_PID_FILE"])
    binary = Path(os.environ["ARB_ENGINE_BINARY"])
    old_pid = int(pid_path.read_text(encoding="utf-8").strip())
    os.kill(old_pid, signal.SIGTERM)

    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        try:
            os.kill(old_pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.05)

    log_path = Path(
        os.environ.get("ARB_ENGINE_LOG_FILE", "/tmp/engine-service.log")
    )
    log_handle = log_path.open("ab")
    process = subprocess.Popen(
        [str(binary)],
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        env=os.environ.copy(),
        cwd=Path.cwd(),
    )
    pid_path.write_text(str(process.pid), encoding="utf-8")
    asyncio.run(_wait_for_status(EngineGrpcClient()))


def test_rust_python_grpc_and_redis_boundary() -> None:
    client = EngineGrpcClient()

    initial = asyncio.run(_wait_for_status(client))
    assert initial.healthy is True
    assert initial.runtime_enabled is False

    stopped = asyncio.run(client.stop("cross-language integration stop"))
    assert stopped.accepted is True
    assert stopped.command == "stop_trading"
    assert stopped.request_id
    assert stopped.applied_at_ms > 0

    _stream_id, fields = _wait_for_command_event(stopped.request_id)
    event = json.loads(fields["payload"])
    assert event["command"] == "stop_trading"
    assert event["runtime_enabled"] is False

    with pytest.raises(EngineCommandError) as error:
        asyncio.run(client.start("deployment gate should reject"))
    assert error.value.code_name == "FAILED_PRECONDITION"

    control_path = Path(os.environ["ARB_CONTROL_STATE_FILE"])
    control_path.write_text("{not-json", encoding="utf-8")

    unhealthy = asyncio.run(client.status())
    assert unhealthy.healthy is False
    assert unhealthy.runtime_enabled is False
    assert unhealthy.control_source == "invalid_fail_closed"
    detail = json.loads(unhealthy.detail)
    assert "runtime control state invalid" in detail["summary"]
    assert detail["grpc_idempotency_store_status"] == "healthy"

    repaired = asyncio.run(client.stop("repair invalid control state"))
    assert repaired.accepted is True
    final = asyncio.run(client.status())
    assert final.runtime_enabled is False


def test_duplicate_sequential_request_executes_once() -> None:
    client = EngineGrpcClient()
    request_id = f"seq-{uuid.uuid4().hex}"

    first = asyncio.run(
        client.reload_strategy(
            "sequential deduplication",
            request_id=request_id,
        )
    )
    first_state = _strategy_state()

    second = asyncio.run(
        client.reload_strategy(
            "sequential deduplication",
            request_id=request_id,
        )
    )
    second_state = _strategy_state()

    assert second == first
    assert first_state["generation"] == second_state["generation"]
    assert first_state["request_id"] == request_id
    _wait_for_command_event(request_id)
    assert len(_command_events(request_id)) == 1


def test_duplicate_concurrent_request_executes_once() -> None:
    client = EngineGrpcClient()
    request_id = f"concurrent-{uuid.uuid4().hex}"

    async def run_both():
        return await asyncio.gather(
            client.reload_strategy(
                "concurrent deduplication",
                request_id=request_id,
            ),
            client.reload_strategy(
                "concurrent deduplication",
                request_id=request_id,
            ),
        )

    first, second = asyncio.run(run_both())
    state = _strategy_state()

    assert first == second
    assert state["request_id"] == request_id
    _wait_for_command_event(request_id)
    assert len(_command_events(request_id)) == 1


def test_same_request_id_with_different_payload_is_rejected() -> None:
    client = EngineGrpcClient()
    request_id = f"conflict-{uuid.uuid4().hex}"

    asyncio.run(
        client.reload_strategy(
            "first logical request",
            request_id=request_id,
        )
    )

    with pytest.raises(EngineCommandError) as error:
        asyncio.run(
            client.reload_strategy(
                "different logical request",
                request_id=request_id,
            )
        )

    assert error.value.code_name == "INVALID_ARGUMENT"


def test_lost_response_redis_outage_and_database_duplicate_are_safe() -> None:
    if os.getenv("ARB_RUN_RELIABILITY_INTEGRATION") != "1":
        pytest.fail(
            "reliability integration was selected without "
            "ARB_RUN_RELIABILITY_INTEGRATION=1"
        )
    request_id = f"cross-{uuid.uuid4().hex}"
    reason = "lost response while Redis is paused"

    redis_client = _redis()
    redis_client.execute_command("CLIENT", "PAUSE", 800, "ALL")

    with pytest.raises(grpc.aio.AioRpcError) as error:
        asyncio.run(
            _raw_reload(
                request_id,
                reason,
                timeout=0.05,
            )
        )
    assert error.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED

    retry = asyncio.run(
        _raw_reload(
            request_id,
            reason,
            timeout=2.0,
        )
    )
    assert retry.request_id == request_id
    assert retry.command == "reload_strategy"

    state = _strategy_state()
    assert state["request_id"] == request_id

    stream_id, fields = _wait_for_command_event(
        request_id,
        timeout_seconds=10.0,
    )
    rows = _command_events(request_id)
    assert len(rows) == 1
    event_ids = {row_fields["event_id"] for _, row_fields in rows}
    assert len(event_ids) == 1

    parsed = event_consumer._parse(stream_id, fields)
    duplicate = replace(
        parsed,
        stream_id=f"duplicate-{uuid.uuid4().hex}",
    )
    try:
        assert event_consumer._persist([parsed]) == []
        assert event_consumer._persist([duplicate]) == []
        with get_session_factory()() as session:
            count = session.scalar(
                select(func.count())
                .select_from(EngineEvent)
                .where(EngineEvent.event_id == parsed.event_id)
            )
            assert count == 1
    finally:
        with get_session_factory()() as session:
            session.execute(
                delete(EngineEvent).where(
                    EngineEvent.event_id == parsed.event_id
                )
            )
            session.commit()


def test_completed_request_survives_engine_restart() -> None:
    required = ("ARB_ENGINE_BINARY", "ARB_ENGINE_PID_FILE")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        pytest.fail(
            "restart reliability test requires: " + ", ".join(missing)
        )

    client = EngineGrpcClient()
    request_id = f"restart-{uuid.uuid4().hex}"
    first = asyncio.run(
        client.reload_strategy(
            "restart persistence",
            request_id=request_id,
        )
    )
    before = _strategy_state()

    _restart_engine_service()

    second = asyncio.run(
        client.reload_strategy(
            "restart persistence",
            request_id=request_id,
        )
    )
    after = _strategy_state()

    assert second == first
    assert after["generation"] == before["generation"]
    assert after["request_id"] == request_id
    _wait_for_command_event(request_id)
    assert len(_command_events(request_id)) == 1
