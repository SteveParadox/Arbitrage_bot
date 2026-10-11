"""Actual observer -> scanner -> risk -> PostgreSQL -> V5 HTTP -> reconciliation -> API."""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid
from decimal import Decimal
from datetime import UTC, datetime
from fractions import Fraction

import httpx
import psycopg
import pytest
import redis

from bybit_mock import BybitMock

pytestmark = pytest.mark.skipif(os.getenv("ARB_RUN_EXCHANGE_INTEGRATION") != "1", reason="requires isolated PostgreSQL/Redis and engine binary")
ROOT = Path(__file__).resolve().parents[2]
URL = os.getenv("ARB_DATABASE_URL", "").replace("postgresql+psycopg:", "postgresql:")
TOKEN = "exchange-admin-credential-"+"x"*32
VIEWER = "exchange-viewer-credential-"+"v"*32
CASES = ["profit", "loss", "partial", "reject", "lost_ack", "unknown", "foreign_fee", "after_reservation", "after_intent", "before_submission", "after_acknowledgement", "after_exchange_before_journal", "after_l10", "before_settlement", "after_settlement", "insufficient", "reserve2", "reserve10", "reserve50", "reserve100"]
CASES += ["delayed_fill", "rate_limit", "cancel_unknown", "conflicting_fill", "extra_fees", "wrong_uid", "overprivileged", "borrowed", "clock_skew", "emergency"]
CASES += ["db_disconnect", "redis_pause"]
CASES += ["missing_control"]
CASES += ["wrong_side", "wrong_quantity", "startup_discrepancy", "after_outbox_fsync", "regressed_fill"]
CASES += ["fee_tier", "fee_change"]
PREFIX = uuid.uuid4().hex[:10]


def sql(statement, params=()):
    with psycopg.connect(URL, autocommit=True) as connection:
        cursor = connection.execute(statement, params)
        return cursor.fetchall() if cursor.description else []


def wait(check, timeout=15):
    deadline = time.monotonic()+timeout
    while time.monotonic()<deadline:
        result = check()
        if result:
            return result
        time.sleep(.02)
    raise AssertionError("exchange condition timed out")


@pytest.fixture(scope="module")
def api(tmp_path_factory):
    directory = tmp_path_factory.mktemp("exchange-api")
    accounts = [f"{case}-{PREFIX}" for case in CASES]
    grants = {hashlib.sha256(token.encode()).hexdigest(): {"subject": role, "role": role, "accounts": accounts, "modes": ["testnet"]} for token, role in [(TOKEN, "admin"), (VIEWER, "viewer")]}
    env = dict(os.environ, ARB_TRADING_MODE="testnet", ARB_TESTNET_TRADING_ENABLED="true", ARB_PAPER_TRADING_ENABLED="false", ARB_LIVE_TRADING_ENABLED="false", ARB_EXECUTION_API_KEYS_JSON=json.dumps(grants), ARB_EXECUTION_DEPLOYMENT_PERMIT=str(directory/"permit.json"))
    with (directory/"api.log").open("w") as log:
        process = subprocess.Popen([sys.executable, "-m", "uvicorn", "api.main:app", "--host", "127.0.0.1", "--port", "58085"], cwd=ROOT/"python", env=env, stdout=log, stderr=subprocess.STDOUT)
        client = httpx.Client(base_url="http://127.0.0.1:58085", headers={"Authorization": "Bearer "+TOKEN}, trust_env=False)
        try:
            wait(lambda: ready(client))
            yield client, directory
        finally:
            client.close()
            process.terminate()
            process.wait(timeout=10)


def ready(client):
    try:
        return client.get("/livez").status_code == 200
    except httpx.HTTPError:
        return False


def market_fixture(directory, loss=False):
    subprocess.run([sys.executable, str(ROOT/"scripts/make_observer_replay.py"), str(directory)], check=True, capture_output=True)
    original = [json.loads(line) for line in (directory/"market.jsonl").read_text().splitlines()]
    events = []
    for raw in original[:9]:
        raw["replay_clock"] = True
        if raw["event"]["type"] == "instrument":
            raw["event"].update(qty_step=1e-8 if raw["event"]["symbol"] == "BTCUSDT" else 1e-5,
                                min_order_amt=1e-8, quote_precision=1e-8)
            raw["event"]["execution_filters"]={"base_precision":"0.00000001" if raw["event"]["symbol"]=="BTCUSDT" else "0.00001","quote_precision":"0.00000001","tick_size":"0.00001","min_order_amount":"0.00000001","max_market_quantity":"1000"}
        events.append(raw)
    # The original fixture has health at index 8; a fresh update creates the candidate.
    trigger = json.loads(json.dumps(original[9]))
    trigger.update(replay_clock=True, delay_ms=500)
    events.append(trigger)
    if loss:
        adverse = json.loads(json.dumps(trigger))
        adverse.update(wait_for_execution_leg=3, delay_ms=0)
        adverse["event"].update(update_id=5, sequence=5, is_snapshot=True, bids=[{"price": 4.9, "quantity": 100}], asks=[{"price": 5, "quantity": 100}])
        events.append(adverse)
    # A finite tail keeps health available for controls; it cannot submit without a new book candidate.
    for _ in range(30):
        health = json.loads(json.dumps(original[8]))
        health.update(replay_clock=True, delay_ms=100)
        events.append(health)
    (directory/"market.jsonl").write_text("".join(json.dumps(e)+"\n" for e in events))
    scanner = json.loads((ROOT/"shared/config/scanner.json").read_text())
    scanner.update(start_amounts={"USDT": 100}, max_book_age_ms=2000, max_book_skew_ms=2000)
    (directory/"scanner.json").write_text(json.dumps(scanner))
    risk = json.loads((ROOT/"shared/config/risk.json").read_text())
    risk.update(max_market_data_age_ms=2000, emergency_max_market_data_age_ms=2000, approval_ttl_ms=1000,
                kill_switch_file=str(directory/"KILL"), trading_control_file=str(directory/"global-control.json"),
                runtime_limits_file=str(directory/"limits.json"), state_file=str(directory/"risk.json"))
    (directory/"risk.json").write_text(json.dumps(risk))
    # Explicitly known disabled global state, with no emergency-stop intent.
    # This is not account execution authorization; that still requires API start.
    (directory/"global-control.json").write_text(json.dumps({"version":1,"enabled":False,"updated_at":datetime.now(UTC).isoformat(),"reason":"offline fixture global state","source":"rust_grpc_control","request_id":None}))


def start(directory, account, mock, permit_path, crash=None):
    env = dict(os.environ, ARB_ENV="development", ARB_TRADING_MODE="testnet", ARB_TESTNET_TRADING_ENABLED="true", ARB_PAPER_TRADING_ENABLED="false", ARB_LIVE_TRADING_ENABLED="false", ARB_EXECUTION_ACCOUNT=account, ARB_EXECUTION_OFFLINE_MOCK="true", ARB_EXECUTION_MOCK_URL=mock.url, ARB_EXECUTION_DEPLOYMENT_PERMIT=str(permit_path), ARB_EXECUTION_STATE_DIR=str(directory/"state"), ARB_ENGINE_AUTOSTART="true", ARB_REPLAY_CLOCK="wall", ARB_ENGINE_GRPC_ADDR="127.0.0.1:55055", ARB_ENGINE_GRPC_TOKEN="exchange-internal-token-"+"g"*32, ARB_OBSERVER_REPLAY_FILE=str(directory/"market.jsonl"), ARB_TRIANGLE_CONFIG=str(directory/"triangles.json"), ARB_SCANNER_CONFIG=str(directory/"scanner.json"), ARB_RISK_CONFIG=str(directory/"risk.json"), ARB_CONTROL_STATE_FILE=str(directory/"global-control.json"), ARB_RUNTIME_LIMITS_FILE=str(directory/"limits.json"), ARB_EVENT_OUTBOX_PATH=str(directory/"outbox"), ARB_OBSERVER_LOCK=str(directory/"observer.lock"), ARB_GRPC_IDEMPOTENCY_STORE=str(directory/"commands"), ARB_EVENT_STREAM="arb.exchange."+PREFIX, BYBIT_EXECUTION_MAX_ORDER_NOTIONAL="200")
    for key in ["BYBIT_API_KEY", "BYBIT_API_SECRET", "BYBIT_TESTNET_API_KEY", "BYBIT_TESTNET_API_SECRET", "BYBIT_LIVE_API_KEY", "BYBIT_LIVE_API_SECRET"]:
        env.pop(key, None)
    env.update(BYBIT_EXECUTION_ORDER_TIMEOUT_MS="200", BYBIT_EXECUTION_CANCEL_TIMEOUT_MS="200", BYBIT_EXECUTION_FILL_CONFIRM_TIMEOUT_MS="200", BYBIT_EXECUTION_POLL_INTERVAL_MS="20")
    if crash:
        env["ARB_EXECUTION_CRASH_AT"] = crash
    else:
        env.pop("ARB_EXECUTION_CRASH_AT", None)
    log = (directory/"engine.log").open("a")
    process = subprocess.Popen([str(ROOT/"rust/target/debug/engine-service")], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
    return process, log


def stop(process, log):
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    log.close()


def provision(api, name, uid):
    client, directory = api
    permit = {"version": 1, "account_id": name, "mode": "testnet", "exchange_uid": uid, "expires_at_ms": int(time.time()*1000)+60_000,
              **{key: False for key in ("offline_tests", "testnet_execution_verified", "reconciliation_verified", "crash_recovery_verified", "security_verified", "soak_verified", "human_mainnet_authorization")}}
    (directory/"permit.json").write_text(json.dumps(permit))
    result = client.post("/execution/accounts", json={"account_id": name, "mode": "testnet", "exchange_uid": uid, "mock": True, "request_id": uuid.uuid4().hex,
                                                     "limits": {"max_trade": "200", "max_exposure": "500", "max_asset_exposure": "500", "max_daily_loss": "50", "max_drawdown": "50", "max_consecutive_losses": 3}})
    assert result.status_code == 200, result.text
    return directory/"permit.json"


def verify_accounting(name, mock):
    # Independent rational arithmetic; does not import production calculations.
    cycles = sql("SELECT id,holdings,starting::text,realized_pnl::text,settled FROM execution_cycles WHERE account_id=%s", (name,))
    movements = sql("SELECT asset,amount::text FROM execution_movements WHERE account_id=%s", (name,))
    expected = {a: Fraction(v) for a, v in mock.opening.items()}
    for asset, amount in movements:
        expected[asset] = expected.get(asset, Fraction(0))+Fraction(amount)
    assert expected == {a: Fraction(v) for a, v in mock.wallet.items()}
    for cycle, holdings, starting, pnl, settled in cycles:
        actual = {"USDT": Fraction(starting)}
        for asset, amount in sql("SELECT m.asset,m.amount::text FROM execution_movements m JOIN execution_fills f USING(account_id,execution_id) WHERE f.cycle_id=%s", (cycle,)):
            actual[asset] = actual.get(asset, Fraction(0))+Fraction(amount)
        assert actual == {a: Fraction(v) for a, v in holdings.items()}
        if settled:
            assert all(a == "USDT" or v == 0 for a, v in actual.items())
            assert Fraction(pnl) == actual["USDT"]-Fraction(starting)
    return cycles


@pytest.mark.parametrize("scenario", ["profit", "loss", "partial", "reject", "lost_ack", "foreign_fee", "insufficient", "delayed_fill"])
def test_autonomous_mock_cycle(api, tmp_path, scenario):
    name = f"{scenario}-{PREFIX}"
    uid = str(uuid.uuid4().int % 10**15)
    client, _ = api
    market_fixture(tmp_path, loss=scenario == "loss")
    permit = provision(api, name, uid)
    with BybitMock(uid, scenario, capital="50" if scenario == "insufficient" else "1000") as mock:
        process, log = start(tmp_path, name, mock, permit)
        try:
            wait(lambda: sql("SELECT id FROM execution_accounts WHERE id=%s AND state='READY'", (name,)))
            # Paper/control tokens cannot activate exchange execution; neither can a viewer.
            denied = client.post(f"/execution/{name}/control/start", headers={"Authorization": "Bearer "+VIEWER}, json={"request_id": "denied", "reason": "viewer denied", "authorization": "authorize_testnet"})
            assert denied.status_code == 403
            payload = {"request_id": "start-once", "reason": "explicit offline fixture authorization", "authorization": "authorize_testnet"}
            response = client.post(f"/execution/{name}/control/start", json=payload)
            assert response.status_code == 200, response.text
            assert client.post(f"/execution/{name}/control/start", json=payload).json() == response.json()
            if scenario == "insufficient":
                wait(lambda: sql("SELECT id FROM execution_events WHERE account_id=%s AND kind='execution.candidate_rejected'", (name,)))
                time.sleep(.8)
                assert mock.posts == []
                assert sql("SELECT id FROM execution_cycles WHERE account_id=%s", (name,)) == []
                return
            wait(lambda: sql("SELECT state FROM execution_cycles WHERE account_id=%s AND (settled OR state='RECOVERY_REQUIRED')", (name,)))
            client.post(f"/execution/{name}/control/stop", json={"request_id": "stop-once", "reason": "fixture bounded completion", "authorization": "stop"})
            rows = verify_accounting(name, mock)
            assert not mock.errors, mock.errors
            assert len(mock.posts) == len({p["orderLinkId"] for p in mock.posts})
            assert len(rows) == 1
            if scenario == "foreign_fee":
                assert rows[0][3] is None and not rows[0][4]
            else:
                assert rows[0][4], (rows, sql("SELECT state,reason FROM execution_cycles WHERE account_id=%s", (name,)),mock.posts)
                assert (Decimal(rows[0][3]) < 0) == (scenario in {"loss", "reject"})
                if scenario in {"partial", "reject"}:
                    assert sql("SELECT state FROM execution_cycles WHERE account_id=%s", (name,))[0][0] == "RECOVERED"
            fills = client.get(f"/execution/{name}/fills").json()
            assert len(fills) == len(mock.fills)
            status = client.get(f"/execution/{name}/status").json()
            assert status["environment"] == "offline_mock" and not status["exchange_confirmed"]
            persisted = {"cycles": rows, "orders": sql("SELECT intent,evidence FROM execution_orders WHERE account_id=%s ORDER BY created_at", (name,)), "fills": fills, "opening": mock.opening, "ending": mock.wallet, "independent_rational_reconciliation": True}
            (tmp_path/"financial-trace.json").write_text(json.dumps(persisted, default=str, indent=2))
        finally:
            stop(process, log)
            if process.returncode not in (0,):
                print((tmp_path/"engine.log").read_text())


@pytest.mark.parametrize("boundary", ["after_reservation", "after_intent", "before_submission", "after_acknowledgement", "after_exchange_before_journal", "after_l10", "before_settlement", "after_settlement"])
def test_crash_does_not_replay_exchange_order(api, tmp_path, boundary):
    name = f"{boundary}-{PREFIX}"
    uid = str(uuid.uuid4().int % 10**15)
    client, _ = api
    market_fixture(tmp_path)
    permit = provision(api, name, uid)
    with BybitMock(uid) as mock:
        process, log = start(tmp_path, name, mock, permit, crash=boundary)
        try:
            wait(lambda: sql("SELECT id FROM execution_accounts WHERE id=%s AND state='READY'", (name,)))
            result = client.post(f"/execution/{name}/control/start", json={"request_id": "start", "reason": "offline crash injection", "authorization": "authorize_testnet"})
            assert result.status_code == 200, result.text
            assert process.wait(timeout=15) == 89
        finally:
            stop(process, log)
        post_count = len(mock.posts)
        restarted, log = start(tmp_path, name, mock, permit)
        try:
            wait(lambda: sql("SELECT id FROM execution_accounts WHERE id=%s AND epoch=2 AND NOT recovering", (name,)))
            time.sleep(.8)
            assert len(mock.posts) == post_count
            assert not sql("SELECT id FROM execution_accounts WHERE id=%s AND enabled", (name,))
            if boundary in {"after_intent", "before_submission"}:
                assert sql("SELECT state FROM execution_cycles WHERE account_id=%s", (name,))[0][0] == "RECOVERY_REQUIRED"
            else:
                verify_accounting(name, mock)
            assert len({p["orderLinkId"] for p in mock.posts}) == len(mock.posts)
        finally:
            stop(restarted, log)


def test_ambiguous_submission_never_retries_post(api, tmp_path):
    name = f"unknown-{PREFIX}"
    uid = str(uuid.uuid4().int % 10**15)
    client, _ = api
    market_fixture(tmp_path)
    permit = provision(api, name, uid)
    with BybitMock(uid, "unknown") as mock:
        process, log = start(tmp_path, name, mock, permit)
        try:
            wait(lambda: sql("SELECT id FROM execution_accounts WHERE id=%s AND state='READY'", (name,)))
            result = client.post(f"/execution/{name}/control/start", json={"request_id": "start", "reason": "uncertain HTTP outcome fixture", "authorization": "authorize_testnet"})
            assert result.status_code == 200, result.text
            wait(lambda: sql("SELECT id FROM execution_cycles WHERE account_id=%s AND state='RECOVERY_REQUIRED'", (name,)))
            assert len(mock.posts) == 1
            assert not sql("SELECT id FROM execution_cycles WHERE account_id=%s AND settled", (name,))
            assert sql("SELECT committed FROM execution_cycles WHERE account_id=%s", (name,)) == [(True,)]
        finally:
            stop(process, log)
        mock.hide = False
        restarted, log = start(tmp_path, name, mock, permit)
        try:
            wait(lambda: sql("SELECT id FROM execution_accounts WHERE id=%s AND epoch=2 AND NOT recovering", (name,)))
            assert len(mock.posts) == 1
            verify_accounting(name, mock)
            assert sql("SELECT blocked,enabled FROM execution_accounts WHERE id=%s", (name,)) == [(True, False)]
        finally:
            stop(restarted, log)


@pytest.mark.parametrize("count", [2, 10, 50, 100])
def test_atomic_exchange_reservation_sessions(api, tmp_path, count):
    name = f"reserve{count}-{PREFIX}"
    uid = str(uuid.uuid4().int % 10**15)
    market_fixture(tmp_path)
    provision(api, name, uid)
    result = subprocess.run([str(ROOT/"rust/target/debug/exchange_reservation_probe"), name, uid, str(tmp_path/"triangles.json"), str(count)], cwd=ROOT, env={**os.environ, "ARB_ENV": "development", "ARB_LIVE_TRADING_ENABLED": "false"}, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    evidence = json.loads(result.stdout)
    assert evidence["accepted"] == 1 and evidence["rejected"] == count-1
    assert evidence["competing_owner_denied"] and evidence["stale_owner_denied"]
    assert sql("SELECT sum(starting)::text FROM execution_cycles WHERE account_id=%s AND committed", (name,)) == [("100",)]


@pytest.mark.parametrize("scenario", ["rate_limit", "cancel_unknown", "conflicting_fill", "extra_fees"])
def test_uncertain_exchange_evidence_preserves_commitment(api, tmp_path, scenario):
    name=f"{scenario}-{PREFIX}"
    uid=str(uuid.uuid4().int % 10**15)
    client,_=api
    market_fixture(tmp_path)
    permit=provision(api,name,uid)
    with BybitMock(uid,scenario) as mock:
        process,log=start(tmp_path,name,mock,permit)
        try:
            wait(lambda:sql("SELECT id FROM execution_accounts WHERE id=%s AND state='READY'",(name,)))
            response=client.post(f"/execution/{name}/control/start",json={"request_id":"start","reason":"bounded fault fixture","authorization":"authorize_testnet"})
            assert response.status_code==200,response.text
            wait(lambda:sql("SELECT id FROM execution_cycles WHERE account_id=%s AND state='RECOVERY_REQUIRED'",(name,)))
            assert sql("SELECT committed,settled,realized_pnl FROM execution_cycles WHERE account_id=%s",(name,))==[(True,False,None)]
            assert len(mock.posts)==(2 if scenario=="cancel_unknown" else 1)
            assert len({p["orderLinkId"] for p in mock.posts})==len(mock.posts)
            assert sql("SELECT blocked,enabled FROM execution_accounts WHERE id=%s",(name,))==[(True,False)]
            assert not mock.errors,mock.errors
        finally:
            stop(process,log)


@pytest.mark.parametrize("scenario",["wrong_uid","overprivileged","borrowed","clock_skew"])
def test_account_preflight_blocks_unsafe_exchange_evidence(api,tmp_path,scenario):
    name=f"{scenario}-{PREFIX}"
    uid=str(uuid.uuid4().int % 10**15)
    market_fixture(tmp_path)
    permit=provision(api,name,uid)
    with BybitMock(uid,scenario) as mock:
        process,log=start(tmp_path,name,mock,permit)
        try:
            assert process.wait(timeout=15)!=0
            assert not mock.posts
            assert sql("SELECT enabled FROM execution_accounts WHERE id=%s",(name,))==[(False,)]
            assert not sql("SELECT id FROM execution_cycles WHERE account_id=%s",(name,))
            assert not mock.errors,mock.errors
        finally:
            stop(process,log)


def test_emergency_stop_after_exchange_fill_blocks_next_exposure(api,tmp_path):
    name=f"emergency-{PREFIX}"
    uid=str(uuid.uuid4().int % 10**15)
    client,_=api
    market_fixture(tmp_path)
    permit=provision(api,name,uid)
    with BybitMock(uid) as mock:
        def halt(link):
            if link.endswith("-l10"):
                response=client.post(f"/execution/{name}/control/emergency-stop",json={"request_id":"halt","reason":"halt after fixture fill","authorization":"stop"})
                assert response.status_code==200,response.text
        mock.on_fill=halt
        process,log=start(tmp_path,name,mock,permit)
        try:
            wait(lambda:sql("SELECT id FROM execution_accounts WHERE id=%s AND state='READY'",(name,)))
            response=client.post(f"/execution/{name}/control/start",json={"request_id":"start","reason":"stop fault fixture","authorization":"authorize_testnet"})
            assert response.status_code==200,response.text
            wait(lambda:sql("SELECT id FROM execution_cycles WHERE account_id=%s AND (settled OR state='RECOVERY_REQUIRED')",(name,)))
            # Only the first exposure and a separately authorized unwind may occur.
            assert not any(p["orderLinkId"].endswith(("-l20","-l30")) for p in mock.posts)
            assert sql("SELECT emergency_stop,enabled FROM execution_accounts WHERE id=%s",(name,))==[(True,False)]
            verify_accounting(name,mock)
            assert not mock.errors,mock.errors
        finally:
            stop(process,log)


@pytest.mark.parametrize("dependency",["db_disconnect","redis_pause"])
def test_dependency_failure_after_fill_retains_financial_ownership(api,tmp_path,dependency):
    name=f"{dependency}-{PREFIX}"
    uid=str(uuid.uuid4().int % 10**15)
    client,_=api
    market_fixture(tmp_path)
    permit=provision(api,name,uid)
    paused=False
    with BybitMock(uid) as mock:
        def disrupt(link):
            nonlocal paused
            if link.endswith("-l10"):
                if dependency=="db_disconnect":
                    result=sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name=%s",("exchange:"+name,))
                    assert result==[(True,)],result
                else:
                    redis.Redis.from_url(os.environ["ARB_REDIS_URL"]).execute_command("CLIENT","PAUSE",3000,"ALL")
                    paused=True
        mock.on_fill=disrupt
        process,log=start(tmp_path,name,mock,permit)
        try:
            wait(lambda:sql("SELECT id FROM execution_accounts WHERE id=%s AND state='READY'",(name,)))
            response=client.post(f"/execution/{name}/control/start",json={"request_id":"start","reason":"dependency disruption fixture","authorization":"authorize_testnet"})
            assert response.status_code==200,response.text
            wait(lambda:len(mock.posts)==1)
            if dependency=="db_disconnect":
                assert process.wait(timeout=15)!=0
            else:
                wait(lambda:sql("SELECT id FROM execution_cycles WHERE account_id=%s AND state='RECOVERY_REQUIRED'",(name,)))
                assert sql("SELECT count(*) FROM execution_events WHERE account_id=%s",(name,))[0][0]>0
            assert len(mock.posts)==1
            assert sql("SELECT committed,settled FROM execution_cycles WHERE account_id=%s",(name,))==[(True,False)]
        finally:
            stop(process,log)
            if paused:
                redis.Redis.from_url(os.environ["ARB_REDIS_URL"],socket_timeout=5).ping()
        mock.on_fill=None
        restarted,log=start(tmp_path,name,mock,permit)
        try:
            wait(lambda:sql("SELECT id FROM execution_accounts WHERE id=%s AND epoch=2 AND NOT recovering",(name,)))
            verify_accounting(name,mock)
            assert len(mock.posts)==1
            assert sql("SELECT enabled,blocked FROM execution_accounts WHERE id=%s",(name,))==[(False,True)]
            assert not mock.errors,mock.errors
        finally:
            stop(restarted,log)


def test_missing_global_control_is_not_execution_permission(api,tmp_path):
    name=f"missing_control-{PREFIX}"
    uid=str(uuid.uuid4().int % 10**15)
    client,_=api
    market_fixture(tmp_path)
    (tmp_path/"global-control.json").unlink()
    permit=provision(api,name,uid)
    with BybitMock(uid) as mock:
        process,log=start(tmp_path,name,mock,permit)
        try:
            wait(lambda:sql("SELECT id FROM execution_accounts WHERE id=%s AND NOT recovering AND state='BLOCKED'",(name,)))
            response=client.post(f"/execution/{name}/control/start",json={"request_id":"denied","reason":"unknown global stop state","authorization":"authorize_testnet"})
            assert response.status_code==409,response.text
            assert not mock.posts
            assert not sql("SELECT id FROM execution_cycles WHERE account_id=%s",(name,))
        finally:
            stop(process,log)


def test_startup_discrepancy_cannot_release_recent_reservation(api,tmp_path):
    name=f"startup_discrepancy-{PREFIX}"
    uid=str(uuid.uuid4().int % 10**15)
    client,_=api
    market_fixture(tmp_path)
    permit=provision(api,name,uid)
    with BybitMock(uid) as mock:
        process,log=start(tmp_path,name,mock,permit,crash="after_reservation")
        try:
            wait(lambda:sql("SELECT id FROM execution_accounts WHERE id=%s AND state='READY'",(name,)))
            response=client.post(f"/execution/{name}/control/start",json={"request_id":"start","reason":"reservation crash fixture","authorization":"authorize_testnet"})
            assert response.status_code==200,response.text
            assert process.wait(timeout=15)==89
        finally:
            stop(process,log)
        mock.wallet["USDT"]+=Decimal(1)
        restarted,log=start(tmp_path,name,mock,permit)
        try:
            wait(lambda:sql("SELECT id FROM execution_accounts WHERE id=%s AND epoch=2 AND NOT recovering",(name,)))
            assert sql("SELECT committed,settled,realized_pnl FROM execution_cycles WHERE account_id=%s",(name,))==[(True,False,None)]
            assert sql("SELECT blocked,enabled FROM execution_accounts WHERE id=%s",(name,))==[(True,False)]
            assert not mock.posts
        finally:
            stop(restarted,log)


@pytest.mark.parametrize("scenario",["wrong_side","wrong_quantity"])
def test_exchange_order_must_match_durable_economic_intent(api,tmp_path,scenario):
    name=f"{scenario}-{PREFIX}"
    uid=str(uuid.uuid4().int % 10**15)
    client,_=api
    market_fixture(tmp_path)
    permit=provision(api,name,uid)
    with BybitMock(uid,scenario) as mock:
        process,log=start(tmp_path,name,mock,permit)
        try:
            wait(lambda:sql("SELECT id FROM execution_accounts WHERE id=%s AND state='READY'",(name,)))
            response=client.post(f"/execution/{name}/control/start",json={"request_id":"start","reason":"order identity mismatch fixture","authorization":"authorize_testnet"})
            assert response.status_code==200,response.text
            wait(lambda:sql("SELECT id FROM execution_cycles WHERE account_id=%s AND state='RECOVERY_REQUIRED'",(name,)))
            assert len(mock.posts)==1
            assert sql("SELECT committed,settled,realized_pnl FROM execution_cycles WHERE account_id=%s",(name,))==[(True,False,None)]
            assert not sql("SELECT execution_id FROM execution_fills WHERE account_id=%s",(name,))
        finally:
            stop(process,log)


def test_financial_outbox_retry_keeps_the_original_event(api,tmp_path):
    name=f"after_outbox_fsync-{PREFIX}"
    uid=str(uuid.uuid4().int % 10**15)
    client,_=api
    market_fixture(tmp_path)
    permit=provision(api,name,uid)
    with BybitMock(uid) as mock:
        process,log=start(tmp_path,name,mock,permit,crash="after_outbox_fsync")
        try:
            wait(lambda:sql("SELECT id FROM execution_accounts WHERE id=%s AND state='READY'",(name,)))
            response=client.post(f"/execution/{name}/control/start",json={"request_id":"start","reason":"outbox crash fixture","authorization":"authorize_testnet"})
            assert response.status_code==200,response.text
            assert process.wait(timeout=15)==89
            assert sql("SELECT id FROM execution_events WHERE account_id=%s AND kind='execution.settlement' AND NOT published",(name,))
        finally:
            stop(process,log)
        count=len(mock.posts)
        restarted,log=start(tmp_path,name,mock,permit)
        try:
            wait(lambda:not sql("SELECT id FROM execution_events WHERE account_id=%s AND NOT published",(name,)))
            assert restarted.poll() is None
            assert len(mock.posts)==count
            verify_accounting(name,mock)
        finally:
            stop(restarted,log)


def test_restart_rejects_regressed_fill_evidence(api,tmp_path):
    name=f"regressed_fill-{PREFIX}"
    uid=str(uuid.uuid4().int % 10**15)
    client,_=api
    market_fixture(tmp_path)
    permit=provision(api,name,uid)
    with BybitMock(uid) as mock:
        process,log=start(tmp_path,name,mock,permit,crash="after_l10")
        try:
            wait(lambda:sql("SELECT id FROM execution_accounts WHERE id=%s AND state='READY'",(name,)))
            response=client.post(f"/execution/{name}/control/start",json={"request_id":"start","reason":"persisted fill fixture","authorization":"authorize_testnet"})
            assert response.status_code==200,response.text
            assert process.wait(timeout=15)==89
        finally:
            stop(process,log)
        before=sql("SELECT holdings,committed,settled FROM execution_cycles WHERE account_id=%s",(name,))
        order=next(iter(mock.orders.values()))
        order.update(cumExecQty="0",leavesQty=order["qty"],orderStatus="Cancelled")
        mock.fills[order["orderId"]]=[]
        restarted,log=start(tmp_path,name,mock,permit)
        try:
            assert restarted.wait(timeout=15)!=0
            assert len(mock.posts)==1
            assert sql("SELECT holdings,committed,settled FROM execution_cycles WHERE account_id=%s",(name,))==before
            assert sql("SELECT enabled FROM execution_accounts WHERE id=%s",(name,))==[(False,)]
        finally:
            stop(restarted,log)


@pytest.mark.parametrize("scenario",["fee_tier","fee_change"])
def test_account_fee_evidence_is_required_before_each_order(api,tmp_path,scenario):
    name=f"{scenario}-{PREFIX}"
    uid=str(uuid.uuid4().int % 10**15)
    client,_=api
    market_fixture(tmp_path)
    permit=provision(api,name,uid)
    with BybitMock(uid,scenario) as mock:
        process,log=start(tmp_path,name,mock,permit)
        try:
            wait(lambda:sql("SELECT id FROM execution_accounts WHERE id=%s AND state='READY'",(name,)))
            response=client.post(f"/execution/{name}/control/start",json={"request_id":"start","reason":"account fee rate fixture","authorization":"authorize_testnet"})
            assert response.status_code==200,response.text
            if scenario=="fee_tier":
                wait(lambda:sql("SELECT id FROM execution_events WHERE account_id=%s AND kind='execution.candidate_rejected' AND payload->>'reason' LIKE '%%taker fee%%'",(name,)))
            else:
                wait(lambda:sql("SELECT id FROM execution_cycles WHERE account_id=%s AND state='RECOVERY_REQUIRED'",(name,)))
            assert len(mock.posts)==(0 if scenario=="fee_tier" else 1)
            if scenario=="fee_tier":
                assert not sql("SELECT id FROM execution_cycles WHERE account_id=%s",(name,))
            else:
                assert sql("SELECT committed,settled,realized_pnl FROM execution_cycles WHERE account_id=%s",(name,))==[(True,False,None)]
                verify_accounting(name,mock)
            assert not mock.errors,mock.errors
        finally:
            stop(process,log)
