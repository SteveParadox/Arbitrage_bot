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
        ("event_id", "x" * 65),
        ("event_type", ""),
        ("event_type", "x" * 65),
        ("source", ""),
        ("source", "x" * 65),
        ("schema_version", "2"),
        ("occurred_at_ms", str(2**63)),
        ("payload", "[]"),
        ("payload", '{"bad":NaN}'),
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



def test_parse_rejects_oversized_stream_id() -> None:
    with pytest.raises(ValueError, match="stream_id exceeds"):
        event_consumer._parse(
            "9" * 65,
            {
                "event_id": "event-1",
                "event_type": "engine.health",
                "source": "engine-service",
                "schema_version": "1",
                "occurred_at_ms": "1710000000000",
                "payload": "{}",
            },
        )



def test_database_rejected_event_is_dead_lettered_and_acked(
    monkeypatch,
) -> None:
    event = event_consumer.StreamEvent(
        stream_id="1710000000000-0",
        event_id="event-db-reject",
        event_type="engine.health",
        source="engine-service",
        schema_version=1,
        occurred_at_ms=1710000000000,
        payload={"healthy": True},
    )

    class FakeRedis:
        def __init__(self) -> None:
            self.dead_letters: list[tuple[str, dict]] = []
            self.acked: list[str] = []

        async def xadd(self, stream, fields):
            self.dead_letters.append((stream, fields))
            return "1-0"

        async def xack(self, _stream, _group, *ids):
            self.acked.extend(ids)
            return len(ids)

    monkeypatch.setattr(
        event_consumer,
        "_persist",
        lambda _events: [(event, "value too long")],
    )
    fake = FakeRedis()
    consumer = event_consumer.EngineEventConsumer()

    asyncio.run(
        consumer._persist_and_ack(
            fake,
            [
                (
                    event.stream_id,
                    {
                        "event_id": event.event_id,
                        "event_type": event.event_type,
                        "source": event.source,
                        "schema_version": "1",
                        "occurred_at_ms": str(event.occurred_at_ms),
                        "payload": json.dumps(event.payload),
                    },
                )
            ],
        )
    )

    assert fake.acked == [event.stream_id]
    assert len(fake.dead_letters) == 1
    assert fake.dead_letters[0][0] == (
        event_consumer.settings.arb_event_dead_letter_stream
    )
    assert "database rejected event" in fake.dead_letters[0][1]["error"]
