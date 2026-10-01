from __future__ import annotations

import asyncio
import json

import pytest

from api import event_consumer


def test_parse_accepts_versioned_object_payload() -> None:
    event = event_consumer._parse(
        "1710000000000-0",
        {
            "event_id": "event-1",
            "event_type": "engine.health",
            "source": "engine-service",
            "schema_version": "1",
            "occurred_at_ms": "1710000000000",
            "payload": json.dumps({"healthy": True}),
        },
    )

    assert event.event_id == "event-1"
    assert event.schema_version == 1
    assert event.payload == {"healthy": True}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_id", ""),
        ("event_type", ""),
        ("source", ""),
        ("schema_version", "2"),
        ("payload", "[]"),
    ],
)
def test_parse_rejects_invalid_envelope(field: str, value: str) -> None:
    fields = {
        "event_id": "event-1",
        "event_type": "engine.health",
        "source": "engine-service",
        "schema_version": "1",
        "occurred_at_ms": "1710000000000",
        "payload": "{}",
    }
    fields[field] = value

    with pytest.raises((ValueError, TypeError)):
        event_consumer._parse("1710000000000-0", fields)


def test_consumer_advances_xautoclaim_cursor(monkeypatch) -> None:
    calls: list[tuple] = []

    class FakeRedis:
        def __init__(self) -> None:
            self.claim_calls = 0

        async def xgroup_create(self, *_args, **_kwargs):
            return True

        async def execute_command(self, *args):
            calls.append(args)
            self.claim_calls += 1
            if self.claim_calls == 1:
                return ["500-0", []]
            return ["0-0", []]

        async def xreadgroup(self, **_kwargs):
            consumer._stopping.set()
            return []

        async def aclose(self):
            return None

    fake = FakeRedis()
    monkeypatch.setattr(
        event_consumer.redis,
        "from_url",
        lambda *_args, **_kwargs: fake,
    )

    consumer = event_consumer.EngineEventConsumer()
    asyncio.run(consumer._consume_session())

    claim_calls = [call for call in calls if call and call[0] == "XAUTOCLAIM"]
    assert len(claim_calls) == 2
    assert claim_calls[0][5] == "0-0"
    assert claim_calls[1][5] == "500-0"
