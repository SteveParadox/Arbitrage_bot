"""Actual managed observer, consumer, PostgreSQL and HTTP API; synthetic data only."""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest
from redis import Redis
from sqlalchemy import select, delete

from analytics.db import get_session_factory
from analytics.engine_event_models import EngineEvent
from analytics.models import OpportunityObservation, OpportunityWindow

pytestmark = pytest.mark.skipif(
    os.getenv("ARB_RUN_OBSERVER_INTEGRATION") != "1",
    reason="requires Rust observer executable, PostgreSQL and Redis",
)


def wait_until(check, timeout=15):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (OSError, httpx.HTTPError) as error:
            last_error = error
        time.sleep(0.05)
    raise AssertionError(f"condition timed out: {last_error}")


def test_managed_replay_restart_recovery_and_http_agree(tmp_path):
    root = Path(__file__).resolve().parents[2]
    subprocess.run([sys.executable, str(root / "scripts/make_observer_replay.py"), str(tmp_path)],
                   cwd=root, check=True, capture_output=True)
    config = json.loads((root / "shared/config/scanner.json").read_text())
    config["max_book_age_ms"] = 60000
    (tmp_path / "scanner.json").write_text(json.dumps(config))
    fixture = tmp_path / "market.jsonl"
    events = [json.loads(line) for line in fixture.read_text().splitlines()]
    # Invalid update, late generation-one health and metadata must never resume scans.
    fault = json.loads(json.dumps(events[-1]))
    fault["event"]["update_id"] = 6
    fault["event"]["sequence"] = 6
    fault["event"]["bids"] = [{"price": 7, "quantity": 100}]
    # First discard old ask levels so this delta actually locks/crosses the book.
    fault["event"]["asks"] = [{"price": 4.1, "quantity": 100}]
    recovered = [dict(event, generation=2) for event in events]
    late = [event for event in events if event["event"]["type"] in {"instrument", "health"}]
    all_events = events + [fault] + recovered[:1] + late + recovered[1:]
    fixture.write_text("".join(json.dumps(event)+"\n" for event in all_events))
    stream = f"arb.observer.integration.{tmp_path.name}"
    env = dict(os.environ, ARB_ENV="development", ARB_LIVE_TRADING_ENABLED="false",
        ARB_TRADING_MODE="observe", ARB_ENGINE_AUTOSTART="true",
        ARB_OBSERVER_REPLAY_FILE=str(fixture), ARB_TRIANGLE_CONFIG=str(tmp_path / "triangles.json"),
        ARB_SCANNER_CONFIG=str(tmp_path / "scanner.json"), ARB_EVENT_STREAM=stream,
        ARB_EVENT_CONSUMER_GROUP="observer-integration", ARB_EVENT_DEAD_LETTER_STREAM=stream+".dead",
        ARB_EVENT_OUTBOX_PATH=str(tmp_path / "outbox"), ARB_OBSERVER_LOCK=str(tmp_path / "observer.lock"),
        ARB_GRPC_IDEMPOTENCY_STORE=str(tmp_path / "commands"),
        ARB_CONTROL_STATE_FILE=str(tmp_path / "control.json"),
        ARB_RUNTIME_LIMITS_FILE=str(tmp_path / "limits.json"),
        ARB_ENGINE_GRPC_ADDR="127.0.0.1:55052", ARB_ENGINE_GRPC_TARGET="127.0.0.1:55052",
        ARB_ENGINE_GRPC_TOKEN="observer-test-internal-token-0123456789",
        ARB_CONTROL_API_TOKEN="observer-test-operator-token-0123456789",
        ARB_MARKET_REQUIRED_SYMBOLS="BTCUSDT,ETHBTC,ETHUSDT")
    client = Redis.from_url(env["ARB_REDIS_URL"], decode_responses=True)
    identities = set()
    processes = []
    handles = []

    def start(command, name, overrides=None):
        handle = (tmp_path / f"{name}.log").open("ab")
        handles.append(handle)
        process = subprocess.Popen(command, cwd=root / "python", env=dict(env, **(overrides or {})),
            stdout=handle, stderr=subprocess.STDOUT)
        processes.append(process)
        return process

    def candidate_events():
        with get_session_factory()() as db:
            return db.scalars(select(EngineEvent).where(
                EngineEvent.event_type.in_(["opportunity.detected", "opportunity.rejected"]),
                EngineEvent.payload["configuration_hash"].as_string().is_not(None),
                EngineEvent.occurred_at_ms == events[0]["event"]["timestamp"],
            )).all()

    def health():
        return httpx.get("http://127.0.0.1:58080/health", timeout=2, trust_env=False).json()

    try:
        api = start([sys.executable, "-m", "uvicorn", "api.main:app", "--host", "127.0.0.1",
                     "--port", "58080"], "api", {"ARB_DATABASE_URL": "postgresql+psycopg://audit@127.0.0.1:55439/unavailable"})
        wait_until(lambda: health().get("database_status") == "offline")
        engine = start([os.environ["ARB_ENGINE_BINARY"]], "engine")
        def pending_candidates():
            pending = {row["message_id"] for row in client.xpending_range(
                stream, env["ARB_EVENT_CONSUMER_GROUP"], "-", "+", 1000)}
            return sum(stream_id in pending and fields.get("event_type") in {
                "opportunity.detected", "opportunity.rejected"}
                for stream_id, fields in client.xrange(stream))
        wait_until(lambda: pending_candidates() == 2)
        assert not candidate_events()  # failed database writes must not acknowledge persistence
        api.terminate()
        api.wait(timeout=10)
        api = start([sys.executable,"-m","uvicorn","api.main:app","--host","127.0.0.1","--port","58080"],"api")
        # Real XAUTOCLAIM recovers the failed consumer's unacknowledged records after its lease.
        records = wait_until(lambda: candidate_events() if len(candidate_events()) == 2 else None, timeout=45)
        assert sorted(record.event_type for record in records) == ["opportunity.detected", "opportunity.rejected"]
        identities = {record.event_id for record in records}
        assert all(record.event_id == record.payload["candidate_id"] for record in records)
        with get_session_factory()() as db:
            rows = db.scalars(select(OpportunityObservation).where(
                OpportunityObservation.observation_key.in_(identities))).all()
            assert len(rows) == 2
            assert sorted(row.accepted for row in rows) == [False, True]
        wait_until(lambda: health()["observer"].get("generation") == 2)
        value = health()
        assert value["observer"]["scanner_ready"] is True
        assert value["trading"]["deployment_enabled"] is False
        assert value["trading"]["effective_enabled"] is False
        assert value["observer"]["execution_enabled"] is False
        opportunities = httpx.get("http://127.0.0.1:58080/opportunities", trust_env=False).json()
        assert opportunities
        # Shutdown and restart the actual process using the same immutable fixture/outbox.
        engine.send_signal(signal.SIGINT)
        engine.wait(timeout=10)
        engine = start([os.environ["ARB_ENGINE_BINARY"]], "engine")
        wait_until(lambda: health()["observer"].get("generation") == 2)
        time.sleep(0.3)
        assert {record.event_id for record in candidate_events()} == identities
        assert client.xlen(stream+".dead") == 0
        # Real Redis redelivery under a new stream ID is acknowledged without a side effect.
        record = records[0]
        fields = dict(event_id=record.event_id, event_type=record.event_type, source=record.source,
            schema_version="1", occurred_at_ms=str(record.occurred_at_ms), payload=json.dumps(record.payload))
        client.xadd(stream, fields)
        wait_until(lambda: health()["event_consumer"]["database_duplicate_events_total"] >= 1)
        fields["payload"] = json.dumps(dict(record.payload, expected_net_profit=999))
        client.xadd(stream, fields)
        wait_until(lambda: client.xlen(stream+".dead") == 1)
        assert "conflicting event_id" in client.xrange(stream+".dead")[0][1]["error"]
        assert len(candidate_events()) == 2
        # Restart the real consumer/API; pending group records remain recoverable.
        api.terminate()
        api.wait(timeout=10)
        client.xadd(stream, dict(fields, payload=json.dumps(record.payload)))
        api = start([sys.executable,"-m","uvicorn","api.main:app","--host","127.0.0.1","--port","58080"],"api")
        wait_until(lambda: health()["event_consumer"]["database_duplicate_events_total"] >= 1)
        assert len(candidate_events()) == 2
        print("managed replay: accepted=1 rejected=1; database outage/pending recovery, generation recovery, engine/consumer restart, Redis redelivery, conflict DLQ, HTTP health and ledger agree")
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        for handle in handles:
            handle.close()
        client.delete(stream, stream+".dead")
        if identities:
            with get_session_factory()() as db:
                windows = db.scalars(select(OpportunityObservation.opportunity_window_id).where(
                    OpportunityObservation.observation_key.in_(identities))).all()
                db.execute(delete(OpportunityObservation).where(
                    OpportunityObservation.observation_key.in_(identities)))
                db.execute(delete(OpportunityWindow).where(OpportunityWindow.id.in_([w for w in windows if w])))
                db.execute(delete(EngineEvent).where(EngineEvent.event_id.in_(identities)))
                db.commit()
