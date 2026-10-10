import asyncio
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from analytics.engine_event_models import EngineEvent
from api import health, runtime_control
from api.contracts import HealthResponse
from api.settings import settings


@pytest.fixture
def healthy(monkeypatch, tmp_path):
    engine = create_engine("sqlite://")
    EngineEvent.__table__.create(engine)
    session = Session(engine)
    monkeypatch.setattr(
        runtime_control, "control_state_path", lambda: tmp_path / "trading_state.json"
    )
    runtime_control.write_control_state(enabled=True, reason="test enabled")
    monkeypatch.setattr(settings, "arb_live_trading_enabled", True)
    monkeypatch.setattr(settings, "arb_control_api_token", "health-test-operator-token-0123456789")
    monkeypatch.setattr(
        health,
        "_risk_status",
        lambda: {
            "available": True,
            "state": "ready",
            "kill_switch_active": False,
            "circuit_breaker": None,
        },
    )

    async def redis_ready():
        return {"status": "online", "pending": 0, "lag": 0}

    monkeypatch.setattr(health, "_redis_status", redis_ready)
    now = int(datetime.now(UTC).timestamp() * 1000)
    symbols = {
        s: {
            "initialized": True,
            "synchronized": True,
            "exchange_timestamp_ms": now,
            "receive_age_ms": 0,
        }
        for s in settings.arb_market_required_symbols.split(",")
    }
    session.add_all(
        [
            EngineEvent(
                event_id="engine",
                stream_id="1-0",
                event_type="engine.health",
                source="engine-service",
                schema_version=1,
                occurred_at_ms=now,
                payload={"healthy": True},
            ),
            EngineEvent(
                event_id="market",
                stream_id="2-0",
                event_type="market.health",
                source="scanner",
                schema_version=1,
                occurred_at_ms=now,
                payload={
                    "state": "connected_and_fresh",
                    "subscriptions_confirmed": True,
                    "symbols": symbols,
                },
            ),
        ]
    )
    session.commit()
    client = SimpleNamespace()

    async def status():
        return SimpleNamespace(
            generated_at_ms=int(datetime.now(UTC).timestamp() * 1000),
            healthy=True,
            runtime_enabled=True,
            strategy_generation="test",
            detail=json.dumps(
                {
                    "event_pipeline": {
                        "event_pipeline_status": "healthy",
                        "critical_events_pending": 0,
                    },
                    "grpc_idempotency_store_status": "healthy",
                }
            ),
        )

    client.status = status
    yield session, client
    session.close()
    engine.dispose()


def test_healthy_dependencies_can_report_eligible(healthy):
    result = asyncio.run(health.health_snapshot(*healthy))
    HealthResponse.model_validate(result)
    assert result["trading"]["effective_enabled"] is True
    assert result["status"] == "ok"


@pytest.mark.parametrize(
    "failure",
    [
        "missing_symbols",
        "no_snapshot",
        "unsynchronized",
        "old_exchange",
        "future_exchange",
        "old_receive",
        "old_heartbeat",
        "disconnected",
        "no_telemetry",
        "malformed",
    ],
)
def test_market_pipeline_failure_blocks_eligibility(healthy, failure):
    db, client = healthy
    event = db.get(EngineEvent, "market")
    payload = json.loads(json.dumps(event.payload))
    symbol = next(iter(payload["symbols"]))
    if failure == "missing_symbols":
        payload["symbols"].pop(symbol)
    elif failure == "no_snapshot":
        payload["symbols"][symbol]["initialized"] = False
    elif failure == "unsynchronized":
        payload["symbols"][symbol]["synchronized"] = False
    elif failure in {"old_exchange", "future_exchange"}:
        payload["symbols"][symbol]["exchange_timestamp_ms"] += (
            -60000 if failure == "old_exchange" else 60000
        )
    elif failure == "old_receive":
        payload["symbols"][symbol]["receive_age_ms"] = 60000
    elif failure == "old_heartbeat":
        event.occurred_at_ms -= 60000
    elif failure == "disconnected":
        payload["state"] = "reconnecting"
    elif failure == "no_telemetry":
        db.delete(event)
    elif failure == "malformed":
        payload["symbols"] = []
    event.payload = payload
    db.commit()
    result = asyncio.run(health.health_snapshot(db, client))
    assert result["trading"]["effective_enabled"] is False
    assert result["market_stream_status"] != "connected_and_fresh"


@pytest.mark.parametrize(
    "component",
    [
        "risk", "redis", "consumer", "grpc", "database", "control", "outbox",
        "command_store", "stale_engine", "control_auth",
    ],
)
def test_missing_mandatory_dependency_blocks_eligibility(healthy, monkeypatch, component):
    db, client = healthy
    if component == "risk":
        monkeypatch.setattr(
            health, "_risk_status", lambda: {"available": True, "state": "no_persisted_state"}
        )
    elif component == "redis":

        async def offline():
            return {"status": "offline"}

        monkeypatch.setattr(health, "_redis_status", offline)
    elif component == "consumer":
        db.get(EngineEvent, "engine").occurred_at_ms -= 60000
        db.commit()
    elif component in {"grpc", "outbox", "command_store"}:
        original = client.status

        async def broken():
            result = await original()
            if component == "grpc":
                result.healthy = False
            else:
                payload = json.loads(result.detail)
                if component == "outbox":
                    payload["event_pipeline"]["event_pipeline_status"] = "degraded"
                else:
                    payload["grpc_idempotency_store_status"] = "unknown"
                result.detail = json.dumps(payload)
            return result

        client.status = broken
    elif component == "database":

        def broken(*args):
            raise SQLAlchemyError("secret connection string should not leak")

        monkeypatch.setattr(db, "execute", broken)
    elif component == "stale_engine":
        original = client.status

        async def stale():
            result = await original()
            result.generated_at_ms -= 60000
            return result

        client.status = stale
    elif component == "control_auth":
        monkeypatch.setattr(settings, "arb_control_api_token", "")
    elif component == "control":
        runtime_control.control_state_path().write_text("[]")
    result = asyncio.run(health.health_snapshot(db, client))
    assert result["trading"]["effective_enabled"] is False
    assert result["status"] != "ok"
    assert "secret connection" not in str(result)


@pytest.mark.parametrize("proof", ["matching", "other_operation", "still_enabled", "old_status"])
def test_reconnected_health_only_confirms_matching_verified_stop(healthy, proof):
    db, client = healthy
    runtime_control.request_stop("stop", "recover-stop")
    runtime_control.mark_stop_unconfirmed("recover-stop")
    raw = json.loads(runtime_control.control_state_path().read_text())
    raw.update(source="rust_grpc_control", request_id="recover-stop", enabled=False)
    runtime_control.control_state_path().write_text(json.dumps(raw))
    original = client.status

    async def recovered():
        result = await original()
        result.runtime_enabled = proof == "still_enabled"
        if proof == "old_status":
            result.generated_at_ms -= 60000
        detail = json.loads(result.detail)
        detail["control_request_id"] = "other" if proof == "other_operation" else "recover-stop"
        result.detail = json.dumps(detail)
        return result

    client.status = recovered
    result = asyncio.run(health.health_snapshot(db, client))
    expected = "CONFIRMED_STOPPED" if proof == "matching" else "STOP_UNCONFIRMED"
    assert result["trading"]["stop_outcome"] == expected
    assert result["trading"]["effective_enabled"] is False
