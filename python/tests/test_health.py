from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)


def test_health() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] in {"ok", "degraded", "unhealthy"}
    assert payload["trading"]["deployment_enabled"] is False
    assert payload["trading"]["effective_enabled"] is False
