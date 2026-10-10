from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from api import health
from api.contracts import ObserverHealth


@pytest.mark.parametrize("state", ["offline", "connecting", "synchronizing", "scanning",
    "degraded", "reconnecting", "stale", "failed", "unrecognized"])
def test_observer_states_are_conservative(monkeypatch, state):
    now = int(datetime.now(UTC).timestamp() * 1000)
    event = SimpleNamespace(occurred_at_ms=now,
        payload={"state":state,"scanner_ready":state == "scanning", "execution_enabled":True})
    monkeypatch.setattr(health,"_latest",lambda *_:event)
    value = health._observer_status(None)
    assert value["state"] == ("unknown" if state == "unrecognized" else state)
    assert value["execution_enabled"] is False
    ObserverHealth.model_validate(value)
    event.occurred_at_ms = now - 999999
    assert health._observer_status(None)["state"] == "stale"


def test_scanning_requires_scanner_readiness(monkeypatch):
    event = SimpleNamespace(occurred_at_ms=int(datetime.now(UTC).timestamp()*1000),
        payload={"state":"scanning"})
    monkeypatch.setattr(health,"_latest",lambda *_:event)
    assert health._observer_status(None)["state"] == "degraded"
    with pytest.raises(ValueError):
        ObserverHealth(state="scanning",execution_enabled=False)
