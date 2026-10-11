"""Real Rust entry point + public fixture events + PostgreSQL + Redis + authenticated API."""
from __future__ import annotations
import concurrent.futures
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid
from decimal import Decimal

import httpx
import psycopg
import pytest

RUN = os.getenv("ARB_RUN_PAPER_INTEGRATION") == "1"
pytestmark = pytest.mark.skipif(not RUN, reason="requires isolated PostgreSQL, Redis and Rust binaries")
ROOT = Path(__file__).resolve().parents[2]
URL = os.getenv("ARB_DATABASE_URL", "").replace("postgresql+psycopg:", "postgresql:")
TOKEN = "paper-test-control-token-0123456789012345"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}


def wait(check, timeout=15):
    end = time.monotonic()+timeout
    last = None
    while time.monotonic() < end:
        try:
            result = check()
            if result:
                return result
        except (httpx.HTTPError, psycopg.Error) as error:
            last = error
        time.sleep(.02)
    raise AssertionError(f"condition timed out: {last}")


def sql(query, params=()):
    with psycopg.connect(URL, autocommit=True) as c:
        cur = c.execute(query, params)
        return cur.fetchall() if cur.description else []


@pytest.fixture(scope="module")
def api(tmp_path_factory):
    directory = tmp_path_factory.mktemp("paper-api")
    env = dict(os.environ, ARB_TRADING_MODE="paper", ARB_PAPER_TRADING_ENABLED="true",
               ARB_LIVE_TRADING_ENABLED="false", ARB_CONTROL_API_TOKEN=TOKEN,
               ARB_EVENT_STREAM="arb.paper.integration.api", ARB_EVENT_CONSUMER_GROUP="paper-api")
    with (directory/"api.log").open("w") as log:
        process = subprocess.Popen([sys.executable, "-m", "uvicorn", "api.main:app", "--host",
                                    "127.0.0.1", "--port", "58081"], cwd=ROOT/"python",
                                   env=env, stdout=log, stderr=subprocess.STDOUT)
        client = httpx.Client(base_url="http://127.0.0.1:58081", headers=HEADERS, trust_env=False)
        try:
            wait(lambda: client.get("/health").status_code == 200)
            yield client
        finally:
            client.close()
            process.terminate()
            process.wait(timeout=10)


def fixture(directory, *, fail=False, partial=False, adverse=False):
    subprocess.run([sys.executable, str(ROOT/"scripts/make_observer_replay.py"), str(directory)],
                   env={**os.environ, "PYTHONPATH": str(ROOT/"python")}, check=True,
                   capture_output=True)
    path = directory/"market.jsonl"
    events = [json.loads(line) for line in path.read_text().splitlines()]
    for e in events:
        e["replay_clock"] = True
        if e["event"]["type"] == "instrument":
            e["event"]["quote_precision"] = .00001
            e["event"]["min_order_amt"] = .0001
    events[0]["delay_ms"] = 1200
    # Give the three orders time to fill before the rejected market regime arrives.
    events[-1]["delay_ms"] = 700
    if fail:
        events = events[:-1]
        events[-1]["event"]["is_snapshot"] = True
        events[-1]["event"]["bids"] = [{"price": 4.0, "quantity": 100}]
        events[-1]["event"]["asks"] = [{"price": 6.1, "quantity": 0}, {"price": 4.1, "quantity": 100}]
    if adverse:
        move = json.loads(json.dumps(events[-2]))
        move["delay_ms"] = 100
        move["event"].update(symbol="BTCUSDT", bids=[{"price":109,"quantity":10}],
                             asks=[{"price":110,"quantity":10}], update_id=30,
                             sequence=30, is_snapshot=True)
        events.insert(-1, move)
    if partial:
        # Liquidity deteriorates after validation and before leg two matching.
        move = json.loads(json.dumps(events[-2]))
        move["wait_for_paper_leg"] = 2
        move["delay_ms"] = 20
        move["event"].update(symbol="ETHBTC", bids=[{"price": .049, "quantity": 100}],
                              asks=[{"price": .05, "quantity": 10}], update_id=30,
                              sequence=30, is_snapshot=True)
        events.insert(-1, move)
    path.write_text("".join(json.dumps(e)+"\n" for e in events))
    config = json.loads((ROOT/"shared/config/scanner.json").read_text())
    config["start_amounts"] = {"USDT": 100.0}
    if adverse or partial:
        config["max_book_skew_ms"] = 1000
    (directory/"scanner.json").write_text(json.dumps(config))
    return path


def start_engine(directory, account, *, crash=None, fail=False, partial=False, risk_config=None, adverse=False, port=55053, expect_ready=True):
    path = fixture(directory, fail=fail, partial=partial, adverse=adverse)
    env = dict(os.environ, ARB_ENV="development", ARB_TRADING_MODE="paper",
               ARB_PAPER_TRADING_ENABLED="true", ARB_LIVE_TRADING_ENABLED="false",
               ARB_PAPER_ACCOUNT=account, ARB_ENGINE_AUTOSTART="true",
               ARB_ENGINE_GRPC_ADDR=f"127.0.0.1:{port}",
               ARB_ENGINE_GRPC_TOKEN="paper-test-internal-token-0123456789",
               ARB_OBSERVER_REPLAY_FILE=str(path), ARB_TRIANGLE_CONFIG=str(directory/"triangles.json"),
               ARB_SCANNER_CONFIG=str(directory/"scanner.json"), ARB_PAPER_LATENCY_MS="200" if adverse or partial else "50",
               ARB_EVENT_OUTBOX_PATH=str(directory/"outbox"), ARB_OBSERVER_LOCK=str(directory/"observer.lock"),
               ARB_GRPC_IDEMPOTENCY_STORE=str(directory/"commands"),
               ARB_EVENT_STREAM="arb.paper.integration.api",
               ARB_CONTROL_STATE_FILE=str(directory/"control.json"),
               ARB_RUNTIME_LIMITS_FILE=str(directory/"limits.json"))
    if risk_config:
        env["ARB_RISK_CONFIG"] = str(risk_config)
    else:
        # Each test owns its global controls, as well as its account ledger.
        # Never inherit another test's emergency-stop file through the shell.
        config = json.loads((ROOT/"shared/config/risk.json").read_text())
        if partial:
            # Keep the books fresh for barrier-controlled IOC/unwind latency.
            config["max_market_data_age_ms"] = 2000
            config["emergency_max_market_data_age_ms"] = 2000
        config.update(kill_switch_file=str(directory/"GLOBAL_KILL"),
                      trading_control_file=str(directory/"global-control.json"),
                      runtime_limits_file=str(directory/"global-limits.json"))
        path = directory/"isolated-risk.json"
        path.write_text(json.dumps(config))
        env["ARB_RISK_CONFIG"] = str(path)
    if isinstance(crash, int):
        env["ARB_PAPER_CRASH_AFTER_LEG"] = str(crash)
    elif crash:
        env["ARB_PAPER_CRASH_AT"] = crash
    prior_epoch = sql("SELECT epoch FROM paper_accounts WHERE id=%s", (account,))[0][0]
    log = (directory/"engine.log").open("a")
    process = subprocess.Popen([os.getenv("ARB_ENGINE_BINARY", str(ROOT/"rust/target/debug/engine-service"))], cwd=ROOT,
                               env=env, stdout=log, stderr=subprocess.STDOUT)
    if not expect_ready:
        return process, log
    try:
        wait(lambda: sql("SELECT epoch FROM paper_accounts WHERE id=%s AND epoch>%s AND heartbeat IS NOT NULL AND NOT recovering", (account, prior_epoch)))
    except Exception:
        stop(process, log)
        print((directory/"engine.log").read_text())
        raise
    return process, log


def create(api, account, amount="500"):
    response = api.post("/paper/accounts", json={"account_id": account, "amount": amount,
                                                "request_id": str(uuid.uuid4())})
    assert response.status_code == 200, response.text


def enable(api, account):
    wait(lambda: api.get(f"/paper/{account}/status").json().get("runtime_online"))
    response = api.post(f"/paper/{account}/control/start", json={"request_id": str(uuid.uuid4()),
                                                              "reason": "explicit test paper authorization"})
    assert response.status_code == 200, response.text


def stop(process, log):
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    log.close()


def check_ledger(account):
    balances = sql("SELECT asset,available,reserved FROM paper_balances WHERE account_id=%s", (account,))
    for asset, available, reserved in balances:
        assert available >= 0 and reserved >= 0
        journal = sql("SELECT COALESCE(sum(CASE WHEN credit_scope='available' THEN amount ELSE -amount END),0) "
                      "FROM paper_ledger WHERE account_id=%s AND asset=%s AND "
                      "(debit_scope='available' OR credit_scope='available')", (account, asset))[0][0]
        assert available == journal
    assert not sql("SELECT id FROM paper_cycles WHERE account_id=%s AND NOT settled", (account,))


@pytest.mark.parametrize("scenario", ["profitable", "unprofitable", "capital", "partial", "crash"])
def test_autonomous_entry_point(api, tmp_path, scenario):
    account = "test-"+uuid.uuid4().hex[:12]
    create(api, account, "50" if scenario == "capital" else "500")
    process, log = start_engine(tmp_path, account, crash=1 if scenario == "crash" else None,
                               fail=scenario == "unprofitable", partial=scenario == "partial")
    try:
        enable(api, account)
        if scenario in {"unprofitable", "capital"}:
            wait(lambda: sql("SELECT id FROM paper_events WHERE account_id=%s AND kind='candidate.rejected'", (account,)))
            assert not sql("SELECT id FROM paper_cycles WHERE account_id=%s", (account,))
            assert sql("SELECT available FROM paper_balances WHERE account_id=%s", (account,))[0][0] == Decimal("50" if scenario == "capital" else "500")
        elif scenario == "crash":
            wait(lambda: process.poll() is not None)
            assert process.returncode == 88, (tmp_path/"engine.log").read_text()
            assert len(sql("SELECT o.id FROM paper_orders o JOIN paper_cycles c ON c.id=o.cycle_id WHERE c.account_id=%s", (account,))) == 1
            log.close()
            process, log = start_engine(tmp_path, account)
            wait(lambda: sql("SELECT id FROM paper_cycles WHERE account_id=%s AND settled", (account,)))
            result = api.get(f"/paper/{account}/cycles").json()[0]
            assert result["state"] == "RECOVERY_REQUIRED"
            assert result["realized_pnl"] is None
            assert Decimal(result["holdings"]["BTC"]) == Decimal("0.999")
            assert not api.get(f"/paper/{account}/status").json()["authorized"]
            assert len(api.get(f"/paper/{account}/orders").json()) == 1
        else:
            wait(lambda: sql("SELECT id FROM paper_cycles WHERE account_id=%s AND settled", (account,)))
            result = api.get(f"/paper/{account}/cycles").json()[0]
            orders = api.get(f"/paper/{account}/orders").json()
            assert all(o["result"] is not None for o in orders)
            if scenario == "profitable":
                assert result["state"] == "COMPLETED", result
                assert Decimal(result["realized_pnl"]) == Decimal("19.64035")
                assert len(orders) == 3
                wait(lambda: sql("SELECT event_id FROM engine_events WHERE event_id=%s", (result["candidate_id"],)))
                assert {a:Decimal(v) for a,v in result["paper_metrics"]["fees_by_asset"].items()} == {"BTC":Decimal("0.001"), "ETH":Decimal("0.01998"), "USDT":Decimal("0.11977")}
                assert Decimal(result["paper_metrics"]["return_pct"]) == Decimal("19.64035")
                assert {o["intent"]["leg_number"] for o in orders} == {1,2,3}
                marked = api.get(f"/paper/{account}/valuation").json()
                assert Decimal(marked["estimated_equity"]) == Decimal("519.64035")
                assert Decimal(marked["estimated_unrealized_pnl"]) == 0
                assert sql("SELECT available FROM paper_balances WHERE account_id=%s AND asset='USDT'", (account,))[0][0] == Decimal("519.64035")
                assert all("paper" in o["id"] or o["intent"]["simulated"] for o in orders)
            else:
                assert any(o["result"]["status"] == "PartiallyFilledCanceled" for o in orders), orders
                assert result["state"] in {"RECOVERED", "RECOVERY_REQUIRED"}
        check_ledger(account)
    except Exception:
        print((tmp_path/"engine.log").read_text())
        raise
    finally:
        stop(process, log)


@pytest.mark.parametrize("competitors", [2, 10, 50, 100])
def test_independent_process_reservations_and_duplicate_settlement(api, tmp_path, competitors):
    account = "race-"+uuid.uuid4().hex[:12]
    create(api, account)
    fixture(tmp_path)
    from strategy.triangle_discovery import Instrument, build_config, discover_triangles, write_config
    instruments = [Instrument("BTCUSDT", "BTC", "USDT"), Instrument("ETHBTC", "ETH", "BTC"), Instrument("ETHUSDT", "ETH", "USDT")]
    write_config(build_config(discover_triangles(instruments, {"USDT"}),testnet=True,start_assets={"USDT"}), tmp_path/"triangles.json")
    sql("UPDATE paper_accounts SET enabled=true WHERE id=%s", (account,))
    binary = ROOT/"rust/target/debug/reservation_probe"
    def reserve(i):
        return subprocess.run([str(binary), account, "0", str(tmp_path/"triangles.json"),
                               "reserve", f"{account}-{i}", str(i%2)], cwd=ROOT,
                              env={**os.environ, "ARB_ENV": "development", "ARB_TRADING_MODE":"paper", "ARB_PAPER_TRADING_ENABLED":"true", "ARB_LIVE_TRADING_ENABLED":"false"}, capture_output=True, text=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(reserve, range(competitors)))
    assert sum(r.returncode == 0 for r in results) == 1
    cycle = sql("SELECT id FROM paper_cycles WHERE account_id=%s", (account,))[0][0]
    assert sql("SELECT available,reserved FROM paper_balances WHERE account_id=%s", (account,))[0] == (Decimal(400), Decimal(100))
    for _ in range(2):
        result = subprocess.run([str(binary), account, "0", str(tmp_path/"triangles.json"), "settle", cycle], cwd=ROOT,
                                env={**os.environ, "ARB_ENV": "development", "ARB_TRADING_MODE":"paper", "ARB_PAPER_TRADING_ENABLED":"true", "ARB_LIVE_TRADING_ENABLED":"false"}, capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
    assert sql("SELECT available,reserved FROM paper_balances WHERE account_id=%s", (account,))[0] == (Decimal(500), Decimal(0))
    check_ledger(account)


def test_mutation_auth_and_request_conflicts(api):
    account = "auth-"+uuid.uuid4().hex[:12]
    command = {"account_id": account, "amount": "500", "request_id": "first-request"}
    assert httpx.post(str(api.base_url)+"/paper/accounts", json=command, trust_env=False).status_code == 401
    first = api.post("/paper/accounts", json=command)
    assert first.status_code == 200
    assert api.post("/paper/accounts", json=command).json() == first.json()
    command["amount"] = "600"
    assert api.post("/paper/accounts", json=command).status_code == 409
    assert api.post(f"/paper/{account}/control/start", json={"request_id": "start", "reason": "test"}).status_code == 409


@pytest.mark.parametrize("checkpoint", ["before_reservation", "after_reservation", "after_intent",
                                        2, 3, "before_settlement", "after_settlement"])
def test_crash_boundaries_preserve_financial_identity(api, tmp_path, checkpoint):
    account = "crash-"+uuid.uuid4().hex[:12]
    create(api, account)
    process, log = start_engine(tmp_path, account, crash=checkpoint)
    try:
        enable(api, account)
        wait(lambda: process.poll() is not None)
        assert process.returncode == 88, (tmp_path/"engine.log").read_text()
        before = len(api.get(f"/paper/{account}/orders").json())
        log.close()
        process, log = start_engine(tmp_path, account)
        wait(lambda: api.get(f"/paper/{account}/status").json().get("runtime_online"))
        check_ledger(account)
        assert len(api.get(f"/paper/{account}/orders").json()) == before
        status = api.get(f"/paper/{account}/status").json()
        assert not status["authorized"]
        if checkpoint == 2:
            assert status["blocked"]
            assert api.get(f"/paper/{account}/cycles").json()[0]["realized_pnl"] is None
        if checkpoint == "after_settlement":
            enable(api, account)
            time.sleep(2)
            assert len(api.get(f"/paper/{account}/cycles").json()) == 1
            assert len(api.get(f"/paper/{account}/orders").json()) == 3
    finally:
        stop(process, log)


def test_data_outage_prevents_new_dispatch(api, tmp_path):
    account = "stale-"+uuid.uuid4().hex[:12]
    create(api, account)
    process, log = start_engine(tmp_path, account)
    try:
        enable(api, account)
        wait(lambda: sql("SELECT id FROM paper_cycles WHERE account_id=%s AND settled", (account,)))
        time.sleep(1.2)
        assert len(api.get(f"/paper/{account}/cycles").json()) == 1
        assert api.get(f"/paper/{account}/status").json()["active_cycles"] == 0
        check_ledger(account)
    finally:
        stop(process, log)


def test_database_session_outage_recovers_without_duplicate_fill(api, tmp_path):
    account = "db-loss-"+uuid.uuid4().hex[:12]
    create(api, account)
    process, log = start_engine(tmp_path, account)
    try:
        enable(api, account)
        wait(lambda: sql("SELECT o.id FROM paper_orders o JOIN paper_cycles c ON c.id=o.cycle_id WHERE c.account_id=%s AND o.result IS NOT NULL", (account,)))
        sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name=%s", (f"paper:{account}",))
        time.sleep(.3)
        orders = api.get(f"/paper/{account}/orders").json()
        assert len(orders) <= 3
        stop(process, log)
        process, log = start_engine(tmp_path, account)
        wait(lambda: api.get(f"/paper/{account}/status").json().get("runtime_online"))
        check_ledger(account)
        assert len(api.get(f"/paper/{account}/orders").json()) == len(orders)
    finally:
        stop(process, log)


def test_redis_unavailable_blocks_new_financial_activity(api, tmp_path):
    from redis import Redis
    account = "redis-loss-"+uuid.uuid4().hex[:12]
    create(api, account)
    directory = tmp_path
    # A proxy endpoint that cannot connect is a deterministic service unavailability condition.
    path = fixture(directory)
    del path
    old_url = os.environ["ARB_REDIS_URL"]
    os.environ["ARB_REDIS_URL"] = "redis://127.0.0.1:59999/0"
    try:
        process, log = start_engine(directory, account)
    finally:
        os.environ["ARB_REDIS_URL"] = old_url
    try:
        enable(api, account)
        wait(lambda: sql("SELECT id FROM paper_events WHERE account_id=%s AND kind='candidate.rejected'", (account,)))
        assert not api.get(f"/paper/{account}/orders").json()
        assert not api.get(f"/paper/{account}/cycles").json()
        check_ledger(account)
        stop(process, log)
        process, log = start_engine(directory, account)
        enable(api, account)
        wait(lambda: sql("SELECT id FROM paper_cycles WHERE account_id=%s AND settled", (account,)))
        check_ledger(account)
        assert Redis.from_url(old_url).ping()
    finally:
        stop(process, log)


def test_global_kill_between_legs_unwinds_actual_holding_at_a_loss(api, tmp_path):
    account = "kill-"+uuid.uuid4().hex[:12]
    create(api, account)
    config = json.loads((ROOT/"shared/config/risk.json").read_text())
    config.update(kill_switch_file=str(tmp_path/"GLOBAL_KILL"),
                  trading_control_file=str(tmp_path/"global-control.json"),
                  runtime_limits_file=str(tmp_path/"global-limits.json"))
    risk_path = tmp_path/"risk.json"
    risk_path.write_text(json.dumps(config))
    process, log = start_engine(tmp_path, account, risk_config=risk_path)
    try:
        enable(api, account)
        wait(lambda: sql("SELECT o.id FROM paper_orders o JOIN paper_cycles c ON c.id=o.cycle_id WHERE c.account_id=%s AND o.result IS NULL", (account,)))
        (tmp_path/"GLOBAL_KILL").write_text("explicit emergency stop")
        wait(lambda: sql("SELECT id FROM paper_cycles WHERE account_id=%s AND settled", (account,)))
        cycle = api.get(f"/paper/{account}/cycles").json()[0]
        orders = api.get(f"/paper/{account}/orders").json()
        assert cycle["state"] == "RECOVERED", (cycle, orders)
        assert Decimal(cycle["realized_pnl"]) == Decimal("-1.19791")
        assert len(orders) == 2
        assert any(o["intent"]["risk_reducing"] for o in orders)
        assert sql("SELECT available FROM paper_balances WHERE account_id=%s AND asset='USDT'", (account,))[0][0] == Decimal("498.80209")
        check_ledger(account)
    finally:
        stop(process, log)


def test_reconciliation_discrepancy_latches_account_block(api, tmp_path):
    account = "broken-"+uuid.uuid4().hex[:12]
    create(api, account)
    sql("UPDATE paper_balances SET available=available+1 WHERE account_id=%s", (account,))
    # Recovery must reconcile before publishing an eligible heartbeat.
    fixture(tmp_path)
    with pytest.raises(AssertionError, match="condition timed out"):
        start_engine(tmp_path, account)
    assert sql("SELECT blocked,enabled FROM paper_accounts WHERE id=%s", (account,))[0] == (True, False)
    assert not sql("SELECT id FROM paper_cycles WHERE account_id=%s", (account,))
    assert api.get(f"/paper/{account}/reconciliation").json()["balanced"] is False


def test_account_start_asset_isolation(api, tmp_path):
    account = "asset-"+uuid.uuid4().hex[:12]
    response = api.post("/paper/accounts", json={"account_id":account,"asset":"ETH", "amount":"500", "request_id":str(uuid.uuid4())})
    assert response.status_code == 200
    process, log = start_engine(tmp_path, account)
    try:
        enable(api, account)
        wait(lambda: sql("SELECT id FROM paper_events WHERE account_id=%s AND kind='candidate.rejected'", (account,)))
        assert not api.get(f"/paper/{account}/orders").json()
        assert not api.get(f"/paper/{account}/cycles").json()
        assert sql("SELECT asset,available,reserved FROM paper_balances WHERE account_id=%s", (account,))[0] == ("ETH", Decimal(500), Decimal(0))
    finally:
        stop(process, log)


@pytest.mark.parametrize("limit", ["drawdown", "loss_streak", "asset_exposure", "total_exposure"])
def test_risk_portfolio_limits_are_mandatory_before_reservation(api, tmp_path, limit):
    account = "limit-"+uuid.uuid4().hex[:12]
    create(api, account)
    config = json.loads((ROOT/"shared/config/risk.json").read_text())
    config.update(kill_switch_file=str(tmp_path/"GLOBAL_KILL"),
                  trading_control_file=str(tmp_path/"global-control.json"),
                  runtime_limits_file=str(tmp_path/"global-limits.json"))
    config["paper_portfolio_limits"]["max_asset_exposure"] = "1" if limit == "asset_exposure" else "1000"
    path = tmp_path/"risk.json"
    if limit == "total_exposure":
        # The route starts at the permitted 100 USDT trade size but reaches a
        # larger projected intermediate notional. This isolates the
        # portfolio-wide peak-exposure check from the per-trade-size check.
        config["max_trade_size"] = "100"
        config["max_total_exposure"] = "110"
    if limit not in {"asset_exposure", "total_exposure"}:
        # An actual recorded losing unwind establishes history; do not fabricate a P&L row.
        path.write_text(json.dumps(config))
        process, log = start_engine(tmp_path, account, risk_config=path)
        try:
            enable(api, account)
            wait(lambda: sql("SELECT o.id FROM paper_orders o JOIN paper_cycles c ON c.id=o.cycle_id WHERE c.account_id=%s AND o.result IS NULL", (account,)))
            (tmp_path/"GLOBAL_KILL").write_text("test stop")
            wait(lambda: sql("SELECT id FROM paper_cycles WHERE account_id=%s AND settled", (account,)))
        finally:
            stop(process, log)
        (tmp_path/"GLOBAL_KILL").unlink()
        config["paper_portfolio_limits"]["max_drawdown"] = "1" if limit == "drawdown" else "50"
        config["paper_portfolio_limits"]["max_consecutive_losses"] = 1 if limit == "loss_streak" else 3
    path.write_text(json.dumps(config))
    previous = len(api.get(f"/paper/{account}/cycles").json())
    process, log = start_engine(tmp_path, account, risk_config=path)
    try:
        enable(api, account)
        wait(lambda: sql("SELECT id FROM paper_events WHERE account_id=%s AND kind='candidate.rejected' AND payload->>'reason_code'='RUST_RISK_DENIED'", (account,)))
        assert len(api.get(f"/paper/{account}/cycles").json()) == previous
        assert not sql("SELECT id FROM paper_cycles WHERE account_id=%s AND NOT settled", (account,))
        check_ledger(account)
    finally:
        stop(process, log)


def test_two_independent_engines_cannot_own_the_same_account(api, tmp_path):
    account = "owner-"+uuid.uuid4().hex[:12]
    create(api, account)
    first_dir = tmp_path/"first"
    second_dir = tmp_path/"second"
    first_dir.mkdir()
    second_dir.mkdir()
    first, first_log = start_engine(first_dir, account)
    second, second_log = start_engine(second_dir, account, port=55055, expect_ready=False)
    try:
        second.wait(timeout=10)
        assert second.returncode != 0
        assert "already has an authoritative runtime" in (second_dir/"engine.log").read_text()
        assert sql("SELECT epoch FROM paper_accounts WHERE id=%s", (account,))[0][0] == 1
        enable(api, account)
        wait(lambda: sql("SELECT id FROM paper_cycles WHERE account_id=%s AND settled", (account,)))
        assert len(api.get(f"/paper/{account}/orders").json()) == 3
        check_ledger(account)
    finally:
        stop(second, second_log)
        stop(first, first_log)


def test_submission_price_cap_blocks_adverse_movement_during_latency(api, tmp_path):
    account = "movement-"+uuid.uuid4().hex[:12]
    create(api, account)
    process, log = start_engine(tmp_path, account, adverse=True)
    try:
        enable(api, account)
        wait(lambda: sql("SELECT id FROM paper_cycles WHERE account_id=%s AND settled", (account,)))
        orders = api.get(f"/paper/{account}/orders").json()
        assert len(orders) == 1
        assert Decimal(orders[0]["result"]["quantity"]) == 0
        assert orders[0]["result"]["reason"] == "SUBMISSION_SLIPPAGE_CAP_OR_NO_LIQUIDITY"
        assert sql("SELECT available,reserved FROM paper_balances WHERE account_id=%s AND asset='USDT'", (account,))[0] == (Decimal(500),Decimal(0))
        assert Decimal(api.get(f"/paper/{account}/cycles").json()[0]["realized_pnl"]) == 0
        check_ledger(account)
    finally:
        stop(process, log)


@pytest.mark.parametrize("boundary", ["partial_fill", "unwind"])
def test_restart_during_partial_fill_and_unwind_preserves_inventory(api, tmp_path, boundary):
    account = "partial-crash-"+uuid.uuid4().hex[:12]
    create(api, account)
    process, log = start_engine(tmp_path, account, partial=True,
                               crash=2 if boundary == "partial_fill" else "after_unwind_fill")
    try:
        enable(api, account)
        wait(lambda: process.poll() is not None)
        assert process.returncode == 88
        before = len(api.get(f"/paper/{account}/orders").json())
        assert before == (2 if boundary == "partial_fill" else 3)
        log.close()
        process, log = start_engine(tmp_path, account)
        result = api.get(f"/paper/{account}/cycles").json()[0]
        assert result["state"] == "RECOVERY_REQUIRED"
        assert result["realized_pnl"] is None
        assert Decimal(result["holdings"]["BTC"]) == Decimal("0.499")
        assert Decimal(result["holdings"]["ETH"]) == (Decimal("9.99") if boundary == "partial_fill" else Decimal(0))
        assert Decimal(result["holdings"]["USDT"]) == (Decimal(0) if boundary == "partial_fill" else Decimal("59.88006"))
        assert len(api.get(f"/paper/{account}/orders").json()) == before
        check_ledger(account)
    finally:
        stop(process, log)
