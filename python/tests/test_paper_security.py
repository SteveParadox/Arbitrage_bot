from __future__ import annotations

import hashlib
import json

import pytest
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient
from pydantic import ValidationError

from api import security
from api.settings import Settings


ADMIN = "admin-credential-" + "a"*32
VIEWER = "viewer-credential-" + "v"*32
OPERATOR = "operator-credential-" + "o"*32


@pytest.fixture
def secured(monkeypatch):
    grants = {hashlib.sha256(token.encode()).hexdigest():
              {"subject": role, "role": role, "accounts": ["mine"]}
              for token, role in ((VIEWER, "viewer"), (OPERATOR, "operator"))}
    monkeypatch.setattr(security.settings, "arb_control_api_token", ADMIN)
    monkeypatch.setattr(security.settings, "arb_paper_api_keys_json", json.dumps(grants))
    monkeypatch.setattr(security.settings, "arb_api_rate_limit_per_minute", 0)
    monkeypatch.setattr(security.settings, "arb_api_max_body_bytes", 1024)
    app = FastAPI()
    app.add_middleware(security.SecurityMiddleware)

    @app.get("/paper/{account}/status", dependencies=[Depends(security.require_paper_auth)])
    def status(request: Request):
        return {"role": request.state.paper_role}

    @app.post("/paper/{account}/control/{action}", dependencies=[Depends(security.require_paper_auth)])
    def command():
        return {"accepted": True}

    @app.post("/paper/accounts", dependencies=[Depends(security.require_paper_auth)])
    def create():
        return {"accepted": True}

    return TestClient(app)


def headers(token):
    return {"Authorization": "Bearer "+token}


def test_roles_are_scoped_and_viewer_cannot_control(secured):
    assert secured.get("/paper/mine/status", headers=headers(VIEWER)).status_code == 200
    assert secured.get("/paper/other/status", headers=headers(VIEWER)).status_code == 403
    assert secured.post("/paper/mine/control/start", headers=headers(VIEWER)).status_code == 403
    assert secured.post("/paper/mine/control/start", headers=headers(OPERATOR)).status_code == 200
    assert secured.post("/paper/other/control/stop", headers=headers(OPERATOR)).status_code == 403
    assert secured.post("/paper/accounts", headers=headers(OPERATOR)).status_code == 403
    assert secured.post("/paper/accounts", headers=headers(ADMIN)).status_code == 200
    assert secured.get("/paper/mine/status", headers=headers("forged")).status_code == 401


def test_requests_are_bounded_and_headers_do_not_cache(secured):
    response = secured.get("/paper/mine/status", headers=headers(VIEWER))
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert secured.post("/paper/mine/control/start", headers=headers(OPERATOR), content=b"x"*1025).status_code == 413


def test_rate_outage_fails_closed_except_verified_emergency_stop(secured, monkeypatch):
    monkeypatch.setattr(security.settings, "arb_api_rate_limit_per_minute", 2)
    def unavailable(_identity):
        raise ConnectionError("injected Redis outage")
    monkeypatch.setattr(security, "_rate_count", unavailable)
    assert secured.get("/paper/mine/status", headers=headers(OPERATOR)).status_code == 503
    assert secured.post("/paper/mine/control/start", headers=headers(OPERATOR)).status_code == 503
    assert secured.post("/paper/mine/control/stop", headers=headers(OPERATOR)).status_code == 200
    assert secured.post("/paper/other/control/stop", headers=headers(OPERATOR)).status_code != 200


def test_forged_tokens_share_source_quota(secured, monkeypatch):
    monkeypatch.setattr(security.settings, "arb_api_rate_limit_per_minute", 2)
    identities = []
    def count(identity):
        identities.append(identity)
        return len(identities)
    monkeypatch.setattr(security, "_rate_count", count)
    results = [secured.get("/paper/mine/status", headers=headers("forged-"+str(i))).status_code for i in range(3)]
    assert results == [401, 401, 429]
    assert len(set(identities)) == 1


def production(monkeypatch):
    monkeypatch.setenv("ARB_LIVE_TRADING_ENABLED", "false")
    return dict(_env_file=None, arb_env="production", arb_trading_mode="paper",
                arb_control_api_token=ADMIN, arb_engine_grpc_token="grpc-"+"g"*40,
                arb_cors_origins="https://dashboard.example.org", arb_trusted_hosts="api.example.org",
                arb_api_rate_limit_per_minute=100,
                arb_database_url="postgresql+psycopg://runtime:strong-password-012345@db/paper",
                arb_redis_url="redis://:strong-password-678901@cache/0")


@pytest.mark.parametrize("override", [
    {"arb_live_trading_enabled": True}, {"arb_trading_mode": "live"},
    {"arb_control_api_token": "short"}, {"arb_engine_grpc_token": ADMIN},
    {"arb_api_rate_limit_per_minute": 0}, {"bybit_api_key": "forbidden"},
    {"arb_cors_origins": "http://dashboard.example.org"}, {"arb_trusted_hosts": "*"},
    {"arb_redis_url": "redis://cache/0"},
])
def test_production_rejects_unsafe_configuration(monkeypatch, override):
    with pytest.raises(ValidationError):
        Settings(**(production(monkeypatch)|override))


def test_production_accepts_explicit_paper_configuration(monkeypatch):
    assert Settings(**production(monkeypatch)).arb_live_trading_enabled is False


def test_invalid_grants_and_secrets_are_not_echoed(monkeypatch):
    secret = "unexposed-"+"s"*40
    with pytest.raises(ValidationError) as error:
        Settings(_env_file=None, arb_paper_api_keys_json=json.dumps({"bad": secret}))
    assert secret not in str(error.value)
