import hashlib
import json

from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient
import pytest
from pydantic import ValidationError

from api import security
from api.settings import Settings

VIEWER = "exchange-viewer-"+"v"*32
OPERATOR = "exchange-operator-"+"o"*32
CONTROL = "generic-control-"+"c"*32
PAPER = "paper-operator-"+"p"*32


@pytest.fixture
def client(monkeypatch):
    grants = {hashlib.sha256(token.encode()).hexdigest(): {"subject": role, "role": role, "accounts": ["mine"], "modes": ["testnet"]} for token, role in [(VIEWER, "viewer"), (OPERATOR, "operator")]}
    monkeypatch.setattr(security.settings, "arb_execution_api_keys_json", json.dumps(grants))
    monkeypatch.setattr(security.settings, "arb_control_api_token", CONTROL)
    monkeypatch.setattr(security.settings, "arb_paper_api_keys_json", json.dumps({hashlib.sha256(PAPER.encode()).hexdigest(): {"subject": "paper", "role": "operator", "accounts": ["mine"]}}))
    monkeypatch.setattr(security.settings, "arb_api_rate_limit_per_minute", 0)
    app = FastAPI()
    app.add_middleware(security.SecurityMiddleware)

    @app.api_route("/execution/{account}/control/{action}", methods=["GET", "POST"], dependencies=[Depends(security.require_execution_auth)])
    def endpoint(request: Request):
        return request.state.execution_grant

    return TestClient(app)


@pytest.mark.parametrize("token,status", [(CONTROL, 401), (PAPER, 401), (VIEWER, 403), (OPERATOR, 200), ("invalid", 401)])
def test_explicit_execution_permission(client, token, status):
    response = client.post("/execution/mine/control/start", headers={"Authorization": "Bearer "+token})
    assert response.status_code == status


def test_account_access_cannot_be_forged(client):
    assert client.get("/execution/another/control/start", headers={"Authorization": "Bearer "+OPERATOR}).status_code == 403
    result = client.get("/execution/mine/control/start", headers={"Authorization": "Bearer "+VIEWER})
    assert result.status_code == 200
    assert result.json()["modes"] == ["testnet"]


@pytest.mark.parametrize("field,value", [("role", "superuser"), ("modes", ["paper"]), ("accounts", ["*"]), ("subject", "")])
def test_invalid_exchange_grants_rejected(field, value):
    grant = {"subject": "operator", "role": "operator", "accounts": ["mine"], "modes": ["testnet"]}
    grant[field] = value
    with pytest.raises(ValidationError):
        Settings(_env_file=None, arb_execution_api_keys_json=json.dumps({hashlib.sha256(OPERATOR.encode()).hexdigest(): grant}))


def test_paper_credential_cannot_be_promoted_to_exchange_scope():
    digest = hashlib.sha256(PAPER.encode()).hexdigest()
    with pytest.raises(ValidationError):
        Settings(_env_file=None, arb_paper_api_keys_json=json.dumps({digest: {"subject": "paper", "role": "operator", "accounts": ["mine"]}}),
                 arb_execution_api_keys_json=json.dumps({digest: {"subject": "paper", "role": "operator", "accounts": ["mine"], "modes": ["live"]}}))


@pytest.mark.parametrize("values",[{"arb_trading_mode":"unknown"},
    {"arb_trading_mode":"testnet","arb_live_trading_enabled":True},
    {"arb_trading_mode":"live","arb_testnet_trading_enabled":True},
    {"arb_trading_mode":"live","arb_paper_trading_enabled":True}])
def test_conflicting_execution_modes_fail_api_startup(values):
    with pytest.raises(ValidationError):
        Settings(_env_file=None,**values)
