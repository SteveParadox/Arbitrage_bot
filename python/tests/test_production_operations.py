"""Independent seeded accounting, encrypted restore and live protected exporter tests."""

import json
import os
import subprocess
import sys
import time
import uuid
from urllib.parse import urlsplit, urlunsplit

from fastapi import FastAPI
from fastapi.testclient import TestClient
import psycopg
from psycopg.types.json import Jsonb
import pytest
from redis import Redis
from sqlalchemy import create_engine, text

from analytics.integrity import audit
from api import metrics
from api.infrastructure import postgres_options, redis_options, production_readiness
from api.security import SecurityMiddleware
import test_infrastructure_transport as transport

services = transport.services
ROOT = transport.ROOT
pytestmark = pytest.mark.skipif(
    os.getenv("ARB_RUN_TLS_INTEGRATION") != "1",
    reason="requires real disposable TLS PostgreSQL and Redis fixtures",
)


def seed(connection):
    connection.execute("INSERT INTO paper_accounts(id) VALUES('dr')")
    connection.execute(
        "INSERT INTO paper_balances(account_id,asset,available,reserved) VALUES('dr','USDT',401.694305898,100),('dr','BTC',0,0),('dr','ETH',0,0)"
    )
    for identity, state, holdings, pnl in [
        ("closed", "COMPLETED", {"USDT": "101.694305898", "BTC": "0", "ETH": "0"}, "1.694305898"),
        ("pending", "RESERVED", {"USDT": "100"}, None),
    ]:
        connection.execute(
            "INSERT INTO paper_cycles(id,account_id,candidate_id,evidence_key,route_id,start_asset,starting,expected_profit,epoch,state,holdings,route,risk,settled,realized_pnl,cash_delta,completed_at) VALUES(%s,'dr',%s,%s,'USDT>BTC>ETH>USDT','USDT',100,1,0,%s,%s,'{}','{}',%s,%s,%s,CASE WHEN %s THEN now() ELSE NULL END)",
            (
                identity,
                identity,
                identity,
                state,
                Jsonb(holdings),
                pnl is not None,
                pnl,
                pnl,
                pnl is not None,
            ),
        )

    def movement(cycle, operation, asset, debit, credit, amount):
        connection.execute(
            "INSERT INTO paper_ledger(account_id,cycle_id,operation_id,asset,debit_scope,credit_scope,amount) VALUES('dr',%s,%s,%s,%s,%s,%s)",
            (cycle, operation, asset, debit, credit, amount),
        )

    movement(None, "opening1", "USDT", "opening", "available", "250")
    movement(None, "opening2", "USDT", "opening", "available", "250")
    movement("closed", "closed-reserve", "USDT", "available", "cycle", "100")
    for order, side, source, target, quantity, value, fee, price in [
        ("one", "Buy", "USDT", "BTC", "10", "100", "0.01", "10"),
        ("two", "Sell", "BTC", "ETH", "9.99", "19.98", "0.01998", "2"),
        ("three", "Sell", "ETH", "USDT", "19.96002", "101.796102", "0.101796102", "5.1"),
    ]:
        intent = {"side": side, "from": source, "to": target, "quantity": quantity}
        fill = {
            "quantity": quantity,
            "quote_value": value,
            "fee": fee,
            "fee_currency": target,
            "requested": quantity,
            "remaining": "0",
            "price": price,
            "status": "Filled",
        }
        connection.execute(
            "INSERT INTO paper_orders(id,cycle_id,intent,result,state,completed_at) VALUES(%s,'closed',%s,%s,'TERMINAL',now())",
            (order, Jsonb(intent), Jsonb(fill)),
        )
        spent, received = (value, quantity) if side == "Buy" else (quantity, value)
        movement("closed", order, source, "cycle", "simulated_venue", spent)
        movement("closed", order, target, "simulated_venue", "cycle", received)
        movement("closed", order, target, "cycle", "fees", fee)
    movement("closed", "closed-settle", "USDT", "cycle", "available", "101.694305898")
    movement("pending", "pending-reserve", "USDT", "available", "cycle", "100")


@pytest.fixture
def database(services):
    config, ca, *_ = services
    names = ["ops_" + uuid.uuid4().hex for _ in range(2)]
    uri = urlsplit(config.arb_database_url)
    urls = [urlunsplit(uri._replace(path="/" + name)) for name in names]
    env = {k: v for k, v in os.environ.items() if not k.startswith(("ARB_", "BYBIT_"))}
    env.update(
        ARB_ENV="development",
        ARB_LIVE_TRADING_ENABLED="false",
        ARB_DATABASE_URL=urls[0],
        ARB_DATABASE_SSLMODE="verify-full",
        ARB_DATABASE_TLS_CA_CERT=str(ca / "ca.pem"),
        PYTHONPATH=str(ROOT / "python"),
    )
    with psycopg.connect(
        config.arb_database_url, autocommit=True, **postgres_options(config)
    ) as admin:
        for name in names:
            admin.execute('CREATE DATABASE "' + name + '"')
        try:
            subprocess.run(
                [sys.executable, "-m", "alembic", "upgrade", "head"],
                cwd=ROOT / "python",
                env=env,
                check=True,
                capture_output=True,
            )
            yield config, urls, env
        finally:
            for name in names:
                admin.execute('DROP DATABASE "' + name + '" WITH (FORCE)')


def test_encrypted_restore_preserves_pending_capital_and_valid_disabled_controls(
    database, tmp_path
):
    config, urls, env = database
    with psycopg.connect(urls[0], autocommit=True, **postgres_options(config)) as connection:
        seed(connection)
        assert audit(connection)["balanced"]
    state = tmp_path / "state"
    state.mkdir()
    (state / "outbox").mkdir()
    (state / "outbox/pending.json").write_text('{"critical_event":"retained"}')
    key = tmp_path / "backup-key"
    key.write_text(os.urandom(32).hex())
    key.chmod(0o600)
    base = [sys.executable, str(ROOT / "scripts/backup_restore.py")]
    result = subprocess.run(
        base
        + [
            "backup",
            "--key-file",
            str(key),
            "--state",
            str(state),
            "--output",
            str(tmp_path / "backups"),
            "--status-file",
            str(tmp_path / "backup.json"),
            "--source-commit",
            "independent-vector",
        ],
        env=env,
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    backup = json.loads(result.stdout)
    archive = next((tmp_path / "backups").glob("*.arbbackup"))
    assert b"critical_event" not in archive.read_bytes()
    env["ARB_DATABASE_URL"] = urls[1]
    restored = tmp_path / "restored"
    # Empty subdirectories from volume initialization are allowed; populated state is not.
    (restored / "control").mkdir(parents=True)
    result = subprocess.run(
        base
        + [
            "restore",
            "--key-file",
            str(key),
            "--state",
            str(restored),
            "--backup",
            str(archive),
            "--status-file",
            str(tmp_path / "restore.json"),
        ],
        env=env,
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    restore = json.loads(result.stdout)
    assert restore["integrity"]["balanced"] and restore["authorized"] is False
    with psycopg.connect(urls[1], autocommit=True, **postgres_options(config)) as connection:
        assert audit(connection)["balanced"]
        assert connection.execute("SELECT count(*) FROM paper_ledger").fetchone()[0] == 14
        assert connection.execute("SELECT count(*) FROM paper_orders").fetchone()[0] == 3
        assert connection.execute(
            "SELECT enabled,blocked,recovering,epoch FROM paper_accounts"
        ).fetchone() == (False, True, True, 1)
        assert (
            connection.execute("SELECT reserved FROM paper_balances WHERE asset='USDT'").fetchone()[
                0
            ]
            == 100
        )
    control = json.loads((restored / "control/trading_state.json").read_text())
    assert set(control) == {"version", "enabled", "updated_at", "reason", "source", "request_id"}
    assert control["version"] == 1 and control["enabled"] is False
    assert (restored / "RESTORE_REQUIRED").exists()
    assert (restored / "outbox/pending.json").read_bytes() == (
        state / "outbox/pending.json"
    ).read_bytes()
    (tmp_path / "disaster-recovery-evidence.json").write_text(
        json.dumps(
            {
                "checked_at": time.time(),
                "backup": backup,
                "restore": restore,
                "fills": 3,
                "ledger_entries": 14,
                "pending_capital": "100",
                "live_orders": 0,
            },
            indent=2,
        )
    )


def test_protected_exporter_reports_real_dependency_failure(database, tmp_path, monkeypatch):
    config, urls, env = database
    engine = create_engine(
        urls[0].replace("postgresql:", "postgresql+psycopg:"),
        connect_args=postgres_options(config),
        pool_size=2,
        max_overflow=0,
        pool_timeout=2,
    )
    try:
        with engine.begin() as connection:
            for kind, payload in [
                (
                    "engine.health",
                    {"healthy": True, "event_pipeline": {"event_pipeline_status": "healthy"}},
                ),
                ("engine.observer", {"scanner_ready": True}),
            ]:
                connection.execute(
                    text(
                        "INSERT INTO engine_events(event_id,stream_id,event_type,source,schema_version,occurred_at_ms,payload) VALUES(:id,:stream,:kind,'isolated-fixture',1,:stamp,CAST(:payload AS jsonb))"
                    ),
                    {
                        "id": uuid.uuid4().hex,
                        "stream": uuid.uuid4().hex,
                        "kind": kind,
                        "stamp": int(time.time() * 1000),
                        "payload": json.dumps(payload),
                    },
                )
        token = "synthetic-monitor-" + uuid.uuid4().hex
        admin = "synthetic-admin-" + uuid.uuid4().hex
        values = {
            "arb_env": "production",
            "arb_database_url": urls[0],
            "arb_database_sslmode": "verify-full",
            "arb_database_tls_ca_cert": config.arb_database_tls_ca_cert,
            "arb_redis_url": config.arb_redis_url,
            "arb_redis_tls_ca_cert": config.arb_redis_tls_ca_cert,
            "arb_metrics_token": token,
            "arb_control_api_token": admin,
            "arb_api_rate_limit_per_minute": 0,
            "arb_restore_marker": str(tmp_path / "RESTORE_REQUIRED"),
            "arb_financial_integrity_file": str(tmp_path / "integrity.json"),
            "arb_monitoring_readiness_file": str(tmp_path / "monitoring.json"),
            "arb_engine_metrics_file": str(tmp_path / "engine.json"),
            "arb_backup_status_file": str(tmp_path / "backup.json"),
            "arb_event_stream": "metrics-" + uuid.uuid4().hex,
            "arb_event_consumer_group": "metrics-fixture",
        }
        for field, value in values.items():
            monkeypatch.setattr(metrics.settings, field, value)
        (tmp_path / "integrity.json").write_text(
            json.dumps({"checked_at": time.time(), "balanced": True})
        )
        with Redis.from_url(config.arb_redis_url, **redis_options(config)) as redis:
            redis.xgroup_create(
                values["arb_event_stream"],
                values["arb_event_consumer_group"],
                id="0",
                mkstream=True,
            )
        monkeypatch.setattr(metrics, "get_engine", lambda: engine)
        app = FastAPI()
        app.add_middleware(SecurityMiddleware)
        app.include_router(metrics.router)
        with TestClient(app) as client:
            assert client.get("/metrics").status_code == 401
            assert (
                client.get("/metrics", headers={"Authorization": "Bearer " + admin}).status_code
                == 401
            )
            response = client.get("/metrics", headers={"Authorization": "Bearer " + token})
            assert response.status_code == 200
            assert 'arb_dependency_up{service="postgresql"} 1.0' in response.text
            assert token not in response.text and admin not in response.text
            assert production_readiness(metrics.settings)[0]

            def disconnected():
                raise RuntimeError(admin)

            monkeypatch.setattr(metrics, "get_engine", disconnected)
            response = client.get("/metrics", headers={"Authorization": "Bearer " + token})
            assert 'arb_dependency_up{service="postgresql"} 0.0' in response.text
            assert admin not in response.text
            assert not production_readiness(metrics.settings)[0]
    finally:
        engine.dispose()
