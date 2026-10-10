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


@pytest.mark.parametrize("field,value", [
    ("price", 102), ("direction", "SELL"), ("net_profitable", False),
    ("scan_timestamp", 1700000000001), ("processing", {"processed_at_ms": 2000, "extra": True}),
])
def test_versioned_candidate_replay_only_exempts_explicit_processing(field, value):
    from dataclasses import replace
    identity = uuid.uuid4().hex
    first = StreamEvent(f"s-{identity}", identity, "opportunity.rejected", "engine-service", 1,
        1700000000000, {"identity_version": 2, "candidate_id": identity,
        "scan_timestamp": 1700000000000, "price": 101, "direction": "BUY",
        "net_profitable": True, "processing": {"processed_at_ms": 1800}})
    # Use a non-observer source for initial malformed scan? Persist a valid rejected scan.
    payload = dict(first.payload, route_id=f"it-{identity}", triangle_id="BTC-ETH-USDT",
        start_asset="USDT", trigger_symbol="ETHUSDT", trigger_update_id=1, trigger_sequence=1,
        start_amount=100, status="missing_book", legs=[], fees_included=False)
    first = replace(first, payload=payload)
    try:
        assert _persist([first]) == []
        replay = replace(first, stream_id=f"r-{identity}", payload=dict(payload,
            processing={"processed_at_ms": 1900}))
        assert _persist([replay]) == []
        conflict = replace(replay, stream_id=f"c-{identity}", payload=dict(replay.payload, **{field: value}))
        assert len(_persist([conflict])) == 1
        with get_session_factory()() as session:
            assert session.scalar(select(func.count()).select_from(OpportunityObservation).where(
                OpportunityObservation.route_id == payload["route_id"])) == 1
    finally:
        _cleanup_candidate(first)


def _cleanup_candidate(event):
    with get_session_factory()() as session:
        session.execute(delete(OpportunityObservation).where(
            OpportunityObservation.route_id == event.payload["route_id"]))
        session.execute(delete(OpportunityWindow).where(
            OpportunityWindow.route_id == event.payload["route_id"]))
        session.execute(delete(EngineEvent).where(EngineEvent.event_id == event.event_id))
        session.commit()


def test_event_and_stream_identity_resolve_to_different_rows_rejects():
    from dataclasses import replace
    first = StreamEvent(f"s-{uuid.uuid4().hex}", uuid.uuid4().hex,
        "engine.state_changed", "engine-service", 1, 1000, {"runtime_enabled": False})
    second = replace(first, event_id=uuid.uuid4().hex, stream_id=f"s-{uuid.uuid4().hex}")
    try:
        assert _persist([first, second]) == []
        ambiguous = replace(first, stream_id=second.stream_id)
        assert len(_persist([ambiguous])) == 1
    finally:
        with get_session_factory()() as session:
            session.execute(delete(EngineEvent).where(EngineEvent.event_id.in_([first.event_id, second.event_id])))
            session.commit()


def test_concurrent_candidate_delivery_has_one_event_and_observation():
    from concurrent.futures import ThreadPoolExecutor
    from dataclasses import replace
    identity = uuid.uuid4().hex
    event = StreamEvent(f"s-{identity}", identity, "opportunity.rejected", "engine-service", 1, 1700000000000,
        {"candidate_id":identity, "scan_timestamp":1700000000000, "route_id":f"it-{identity}",
         "triangle_id":"BTC-ETH-USDT", "start_asset":"USDT", "trigger_symbol":"ETHUSDT",
         "trigger_update_id":1, "trigger_sequence":1, "start_amount":100, "status":"missing_book",
         "legs":[], "fees_included":False})
    try:
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(_persist, [replace(event,stream_id=f"{index}-{identity}")]) for index in range(8)]
            assert all(f.result() == [] for f in futures)
        with get_session_factory()() as session:
            assert session.scalar(select(func.count()).select_from(EngineEvent).where(
                EngineEvent.event_id == identity)) == 1
            assert session.scalar(select(func.count()).select_from(OpportunityObservation).where(
                OpportunityObservation.route_id == event.payload["route_id"])) == 1
    finally:
        _cleanup_candidate(event)


def test_candidate_savepoint_rolls_back_and_retry_succeeds(monkeypatch):
    from analytics.opportunity_store import OpportunityStore
    identity = uuid.uuid4().hex
    event = StreamEvent(f"s-{identity}", identity,"opportunity.rejected","engine-service",1,1700000000000,
        {"candidate_id":identity,"scan_timestamp":1700000000000,"route_id":f"it-{identity}",
         "triangle_id":"BTC-ETH-USDT","start_asset":"USDT","trigger_symbol":"ETHUSDT",
         "trigger_update_id":1,"trigger_sequence":1,"start_amount":100,"status":"missing_book",
         "legs":[],"fees_included":False})
    original = OpportunityStore.record_scan
    def fail(self, payload):
        original(self, payload)
        raise ValueError("injected failure after ledger write")
    try:
        monkeypatch.setattr(OpportunityStore,"record_scan",fail)
        assert len(_persist([event])) == 1
        with get_session_factory()() as session:
            assert session.get(EngineEvent,identity) is None
            assert session.scalar(select(func.count()).select_from(OpportunityObservation).where(
                OpportunityObservation.route_id == event.payload["route_id"])) == 0
        monkeypatch.setattr(OpportunityStore,"record_scan",original)
        assert _persist([event]) == []
    finally:
        _cleanup_candidate(event)
