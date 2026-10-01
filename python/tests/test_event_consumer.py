from __future__ import annotations

import pytest

from api.event_consumer import _parse


def test_parse_engine_event_envelope() -> None:
    event = _parse(
        "123-0",
        {
            "event_id": "event-1",
            "event_type": "trade.executed",
            "occurred_at_ms": "123456",
            "source": "execution",
            "schema_version": "1",
            "payload": '{"trade_id":"trade-1"}',
        },
    )

    assert event.stream_id == "123-0"
    assert event.event_id == "event-1"
    assert event.event_type == "trade.executed"
    assert event.payload["trade_id"] == "trade-1"


def test_parse_rejects_non_object_payload() -> None:
    with pytest.raises(ValueError):
        _parse(
            "124-0",
            {
                "event_id": "event-2",
                "event_type": "engine.health",
                "occurred_at_ms": "123457",
                "source": "engine-service",
                "schema_version": "1",
                "payload": '["not","an","object"]',
            },
        )



def test_parse_rejects_unknown_schema_version() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        _parse(
            "125-0",
            {
                "event_id": "event-3",
                "event_type": "engine.health",
                "occurred_at_ms": "123458",
                "source": "engine-service",
                "schema_version": "2",
                "payload": "{}",
            },
        )
