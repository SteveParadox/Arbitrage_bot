import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from api import control
from api.main import app
from api.micro_live import ReconcileRequest

PAYLOAD = {"realized_pnl": "0.02", "execution_time_ms": 100, "execution_status": "filled"}


@pytest.mark.parametrize(
    "credentials",
    [None, "Bearer wrong", "Basic wrong", "Bearer expired-token", "Bearer read-only-user"],
)
def test_reconciliation_rejects_unprivileged_callers(monkeypatch, credentials):
    monkeypatch.setattr(
        control.settings, "arb_control_api_token", "operator-token-0123456789-abcdefghijk"
    )
    headers = {"Authorization": credentials} if credentials else {}
    with TestClient(app) as client:
        response = client.post(
            "/analytics/micro-live/reconcile/test-trade", headers=headers, json=PAYLOAD
        )
    assert response.status_code in {401, 403}
    assert "operator-token" not in response.text


def test_query_credentials_do_not_grant_access(monkeypatch):
    token = "operator-token-0123456789-abcdefghijk"
    monkeypatch.setattr(control.settings, "arb_control_api_token", token)
    with TestClient(app) as client:
        response = client.post(
            "/analytics/micro-live/reconcile/test-trade", params={"token": token}, json=PAYLOAD
        )
    assert response.status_code == 401
    assert token not in response.text


@pytest.mark.parametrize(
    "updates",
    [
        {"realized_pnl": "NaN"},
        {"actual_fee_amount_base": "Infinity"},
        {"actual_fees_by_currency": {"USDT": "-1"}},
        {"actual_fees_by_currency": {"USDT": "NaN"}},
        {"actual_fees_by_currency": {str(i): "1" for i in range(33)}},
        {"extra": True},
        {"request_id": "../x"},
        {"realized_pnl": "1e100000"},
        {"actual_slippage_bps": "0.000000001"},
        {"execution_time_ms": 9223372036854775808},
    ],
)
def test_reconciliation_payload_bounds(updates):
    with pytest.raises(ValidationError):
        ReconcileRequest.model_validate({**PAYLOAD, **updates})


def test_retry_fingerprint_preserves_all_database_precision():
    from api.micro_live import _digest

    def digest(pnl):
        return _digest(ReconcileRequest.model_validate({**PAYLOAD, "realized_pnl": pnl}))

    assert digest("12345678901234567890.123456789012345671") != digest(
        "12345678901234567890.123456789012345672"
    )
    assert digest("0.0200") == digest("2e-2")
    assert digest("-0") == digest("0.0")
