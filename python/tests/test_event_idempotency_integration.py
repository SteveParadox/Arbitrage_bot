from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import delete, func, select

from analytics.db import get_session_factory
from analytics.engine_event_models import EngineEvent
from analytics.models import OpportunityObservation, OpportunityWindow
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


@pytest.mark.parametrize("collision", ["event_id", "stream_id"])
def test_conflicting_event_identity_is_rejected_not_silently_dropped(
    collision: str,
) -> None:
    first = StreamEvent(
        stream_id=f"stream-{uuid.uuid4().hex}",
        event_id=f"event-{uuid.uuid4().hex}",
        event_type="engine.state_changed",
        source="engine-service",
        schema_version=1,
        occurred_at_ms=1_710_000_000_000,
        payload={"request_id": "request-a", "runtime_enabled": False},
    )
    if collision == "event_id":
        conflicting = StreamEvent(
            stream_id=f"stream-{uuid.uuid4().hex}",
            event_id=first.event_id,
            event_type=first.event_type,
            source=first.source,
            schema_version=first.schema_version,
            occurred_at_ms=first.occurred_at_ms,
            payload={"request_id": "request-a", "runtime_enabled": True},
        )
    else:
        conflicting = StreamEvent(
            stream_id=first.stream_id,
            event_id=f"event-{uuid.uuid4().hex}",
            event_type=first.event_type,
            source=first.source,
            schema_version=first.schema_version,
            occurred_at_ms=first.occurred_at_ms,
            payload=first.payload,
        )

    try:
        assert _persist([first]) == []
        rejected = _persist([conflicting])
        assert len(rejected) == 1
        assert rejected[0][0] == conflicting
        assert "conflicting event_id or stream_id" in rejected[0][1]

        with get_session_factory()() as session:
            stored = session.get(EngineEvent, first.event_id)
            assert stored is not None
            assert stored.payload == first.payload
            assert session.scalar(
                select(func.count()).select_from(EngineEvent).where(
                    EngineEvent.stream_id == first.stream_id
                )
            ) == 1
            assert session.get(EngineEvent, conflicting.event_id) is (
                stored if collision == "event_id" else None
            )
    finally:
        with get_session_factory()() as session:
            session.execute(
                delete(EngineEvent).where(EngineEvent.event_id == first.event_id)
            )
            session.commit()


def test_observer_candidates_update_dashboard_ledger_once() -> None:
    route_id = f"test-{uuid.uuid4().hex}"
    timestamp = 1_790_000_000_000
    scan = {
        "candidate_id": f"candidate-{uuid.uuid4().hex}",
        "scan_timestamp": timestamp,
        "trigger_symbol": "ETHUSDT",
        "trigger_update_id": 4,
        "trigger_sequence": 4,
        "route_id": route_id,
        "triangle_id": "BTC-ETH-USDT",
        "start_asset": "USDT",
        "start_amount": 100,
        "final_amount": 101,
        "expected_net_profit": 1,
        "expected_net_return_bps": 100,
        "net_profitable": True,
        "fees_included": True,
        "status": "complete",
        "legs": [{"side": "BUY", "complete": True, "execution": {}}] * 2
        + [{"side": "SELL", "complete": True, "execution": {}}],
    }
    event = StreamEvent(
        stream_id=f"it-{uuid.uuid4().hex}",
        event_id=scan["candidate_id"],
        event_type="opportunity.detected",
        source="engine-service",
        schema_version=1,
        occurred_at_ms=timestamp,
        payload=scan,
    )
    try:
        assert _persist([event]) == []
        assert _persist([event]) == []
        with get_session_factory()() as session:
            observations = session.scalars(
                select(OpportunityObservation).where(
                    OpportunityObservation.route_id == route_id
                )
            ).all()
            assert len(observations) == 1
            assert observations[0].accepted is True
            assert session.scalar(
                select(func.count()).select_from(EngineEvent).where(
                    EngineEvent.event_id == event.event_id
                )
            ) == 1
    finally:
        with get_session_factory()() as session:
            session.execute(delete(OpportunityObservation).where(
                OpportunityObservation.route_id == route_id
            ))
            session.execute(delete(OpportunityWindow).where(
                OpportunityWindow.route_id == route_id
            ))
            session.execute(delete(EngineEvent).where(
                EngineEvent.event_id == event.event_id
            ))
            session.commit()
