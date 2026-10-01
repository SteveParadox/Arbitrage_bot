from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from redis import Redis
import redis.asyncio as async_redis
from redis.exceptions import ResponseError
from sqlalchemy import select

from analytics.db import get_session_factory
from analytics.engine_event_models import EngineEvent
from api.engine_client import EngineCommandError, EngineGrpcClient
from api.event_consumer import EngineEventConsumer
from api.main import app
from api.settings import settings

pytestmark = pytest.mark.skipif(
    os.getenv("ARB_RUN_GRPC_INTEGRATION") != "1",
    reason="requires the Rust engine-service, Redis, and PostgreSQL",
)


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


def _wait_for_command_event(request_id: str) -> dict:
    client = Redis.from_url(
        os.environ["ARB_REDIS_URL"],
        decode_responses=True,
    )
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        for _stream_id, fields in client.xrevrange(
            os.environ.get("ARB_EVENT_STREAM", "arb.events"),
            count=100,
        ):
            if fields.get("event_type") != "engine.health":
                continue
            payload = json.loads(fields.get("payload", "{}"))
            if payload.get("request_id") == request_id:
                return payload
        time.sleep(0.1)
    raise AssertionError(
        f"Redis event for command request_id={request_id} was not observed"
    )


async def _consume_until_persisted(request_id: str) -> str:
    client = async_redis.from_url(
        os.environ["ARB_REDIS_URL"],
        decode_responses=True,
    )
    consumer = EngineEventConsumer()
    group = settings.arb_event_consumer_group
    stream = settings.arb_event_stream
    try:
        try:
            await client.xgroup_create(stream, group, id="0-0", mkstream=True)
        except ResponseError as error:
            if "BUSYGROUP" not in str(error):
                raise

        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            batches = await client.xreadgroup(
                groupname=group,
                consumername=f"integration-{os.getpid()}",
                streams={stream: ">"},
                count=100,
                block=1000,
            )
            for _stream, messages in batches:
                target_event_id: str | None = None
                for _stream_id, fields in messages:
                    if fields.get("event_type") != "engine.health":
                        continue
                    payload = json.loads(fields.get("payload", "{}"))
                    if payload.get("request_id") == request_id:
                        target_event_id = fields["event_id"]

                await consumer._persist_and_ack(client, messages)
                if target_event_id is not None:
                    return target_event_id

        raise AssertionError(
            f"consumer did not persist request_id={request_id}"
        )
    finally:
        await client.aclose()


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

    event = _wait_for_command_event(stopped.request_id)
    assert event["command"] == "stop_trading"
    assert event["runtime_enabled"] is False

    persisted_event_id = asyncio.run(
        _consume_until_persisted(stopped.request_id)
    )
    with get_session_factory()() as session:
        persisted = session.scalar(
            select(EngineEvent).where(
                EngineEvent.event_id == persisted_event_id
            )
        )
        assert persisted is not None
        assert persisted.payload["request_id"] == stopped.request_id

    with TestClient(app) as api:
        health = api.get("/health")
        assert health.status_code == 200
        event_pipeline = health.json()["event_pipeline"]
        assert event_pipeline["status"] == "online"
        assert event_pipeline["last_event_id"]
        with get_session_factory()() as session:
            visible = session.get(
                EngineEvent,
                event_pipeline["last_event_id"],
            )
            assert visible is not None

    with pytest.raises(EngineCommandError) as error:
        asyncio.run(client.start("deployment gate should reject"))
    assert error.value.code_name == "FAILED_PRECONDITION"

    control_path = Path(os.environ["ARB_CONTROL_STATE_FILE"])
    control_path.write_text("{not-json", encoding="utf-8")

    unhealthy = asyncio.run(client.status())
    assert unhealthy.healthy is False
    assert unhealthy.runtime_enabled is False
    assert unhealthy.control_source == "invalid_fail_closed"
    assert "runtime control state invalid" in unhealthy.detail

    repaired = asyncio.run(client.stop("repair invalid control state"))
    assert repaired.accepted is True
    final = asyncio.run(client.status())
    assert final.healthy is True
    assert final.runtime_enabled is False
