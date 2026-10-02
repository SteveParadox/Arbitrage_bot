from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import delete, func, select

from analytics.db import get_session_factory
from analytics.engine_event_models import EngineEvent
from api.event_consumer import StreamEvent, _persist

pytestmark = pytest.mark.skipif(
    os.getenv("ARB_RUN_RELIABILITY_INTEGRATION") != "1",
    reason="requires PostgreSQL reliability integration environment",
)


def test_duplicate_event_id_applies_database_effect_once() -> None:
    event_id = f"it-{uuid.uuid4().hex}"
    first = StreamEvent(
        stream_id=f"it-{uuid.uuid4().hex}",
        event_id=event_id,
        event_type="engine.state_changed",
        source="engine-service",
        schema_version=1,
        occurred_at_ms=1_710_000_000_000,
        payload={"request_id": "request-a", "runtime_enabled": False},
    )
    duplicate = StreamEvent(
        stream_id=f"it-{uuid.uuid4().hex}",
        event_id=event_id,
        event_type=first.event_type,
        source=first.source,
        schema_version=first.schema_version,
        occurred_at_ms=first.occurred_at_ms,
        payload=first.payload,
    )

    try:
        assert _persist([first]) == []
        assert _persist([duplicate]) == []

        with get_session_factory()() as session:
            count = session.scalar(
                select(func.count())
                .select_from(EngineEvent)
                .where(EngineEvent.event_id == event_id)
            )
            assert count == 1
    finally:
        with get_session_factory()() as session:
            session.execute(
                delete(EngineEvent).where(EngineEvent.event_id == event_id)
            )
            session.commit()
