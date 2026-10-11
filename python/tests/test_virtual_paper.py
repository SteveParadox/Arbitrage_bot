"""Actual managed observer → scanner → Rust risk → SQL → paper venue replay."""
from __future__ import annotations
import json
import os
import subprocess
import sys
import time
import uuid
from decimal import Decimal
import psycopg
import pytest
from paper_reference import read_snapshot, verify
from test_autonomous_paper import ROOT, URL, create, enable, fixture, sql, start_engine, stop, wait
from test_autonomous_paper import api as api
pytestmark = pytest.mark.skipif(os.getenv("ARB_RUN_PAPER_INTEGRATION") != "1", reason="requires disposable PostgreSQL, Redis and actual Rust runtime")

def frozen_fixture(directory, scenario="profitable"):
    fixture(directory, partial=scenario=="partial", losing=scenario=="losing")
    path=directory/"market.jsonl"
    frames=[json.loads(line) for line in path.read_text().splitlines()]
    for frame in frames:
        frame["event"]["timestamp"]=1_700_000_000_000
    path.write_text("".join(json.dumps(frame)+"\n" for frame in frames))

def capture(account):
    with psycopg.connect(URL) as connection:
        snapshot=read_snapshot(connection,account)
    assert verify(snapshot)["balanced"]
    cycle=snapshot["cycles"][0]
    return {
        "state":cycle["state"],"holdings":cycle["holdings"],"pnl":cycle["realized_pnl"],
        "created":cycle["created_at"],"completed":cycle["completed_at"],
        "balances":sorted((r["asset"],r["available"],r["reserved"]) for r in snapshot["balances"]),
        "orders":[(o["created_at"],o["completed_at"],o["result"]) for o in snapshot["orders"]],
    },snapshot

@pytest.mark.parametrize("scenario,pnl,orders",[("profitable","19.64035",3),("losing","-0.04048",3),("partial","9.23165",4)])
def test_repeated_replay_accounting_ignores_wall_and_sql_delay(api,tmp_path,scenario,pnl,orders):
    frozen_fixture(tmp_path,scenario)
    results=[]
    isolated="isolated-"+uuid.uuid4().hex[:12]
    create(api,isolated)
    for repeat in range(2):
        account="virtual-"+uuid.uuid4().hex[:12]
        create(api,account)
        if repeat:
            time.sleep(.2)
            sql("CREATE OR REPLACE FUNCTION replay_slow_orders() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.12); RETURN NEW; END $$")
            sql("CREATE TRIGGER replay_slow_orders BEFORE INSERT ON paper_orders FOR EACH ROW EXECUTE FUNCTION replay_slow_orders()")
        process,log=start_engine(tmp_path,account,clock="virtual",fixture_ready=True,partial=scenario=="partial",losing=scenario=="losing")
        try:
            enable(api,account)
            wait(lambda:sql("SELECT id FROM paper_events WHERE account_id=%s AND kind='paper.replay_finished'",(account,)))
            normalized,snapshot=capture(account)
            assert normalized["pnl"]==Decimal(pnl)
            assert len(snapshot["orders"])==orders
            assert all(o["intent"]["simulated"] for o in snapshot["orders"])
            status=api.get(f"/paper/{account}/status").json()
            assert status["clock"]=="virtual" and not status["authorized"]
            assert sql("SELECT config FROM paper_accounts WHERE id=%s",(isolated,))[0][0] == {"start_asset":"USDT"}
            valuation=api.get(f"/paper/{account}/valuation").json()
            assert valuation["clock"]=="virtual" and valuation["valued_at_ms"]<1_700_000_010_000
            assert Decimal(api.get(f"/paper/{account}/performance").json()["net_realized_pnl"])==Decimal(pnl)
            results.append(normalized)
        finally:
            stop(process,log)
            if repeat:
                sql("DROP TRIGGER replay_slow_orders ON paper_orders")
                sql("DROP FUNCTION replay_slow_orders()")
    assert results[0]==results[1],"wall scheduling changed committed financial replay"

@pytest.mark.parametrize("latency",[25,50,100,200,500])
def test_virtual_ioc_latency_is_exact(api,tmp_path,latency):
    frozen_fixture(tmp_path)
    path=tmp_path/"market.jsonl"
    path.write_text("\n".join(path.read_text().splitlines()[:-1])+"\n")
    account="latency-"+uuid.uuid4().hex[:12]
    create(api,account)
    process,log=start_engine(tmp_path,account,clock="virtual",fixture_ready=True,partial=True,latency_ms=latency)
    try:
        enable(api,account)
        wait(lambda:sql("SELECT id FROM paper_events WHERE account_id=%s AND kind='paper.replay_finished'",(account,)))
        _,snapshot=capture(account)
        assert snapshot["cycles"][0]["state"]=="COMPLETED" and len(snapshot["orders"])==3
        for order in snapshot["orders"]:
            assert (order["completed_at"]-order["created_at"]).total_seconds()*1000==latency
            assert order["result"]["executed_at_ms"]==int(order["completed_at"].timestamp()*1000)
    finally:
        stop(process,log)

@pytest.mark.parametrize("checkpoint",["after_reservation","after_intent","before_settlement"])
def test_virtual_restart_preserves_committed_history_without_refeed(api,tmp_path,checkpoint):
    frozen_fixture(tmp_path)
    account="restart-"+uuid.uuid4().hex[:12]
    create(api,account)
    process,log=start_engine(tmp_path,account,clock="virtual",fixture_ready=True,crash=checkpoint)
    try:
        enable(api,account)
        wait(lambda:process.poll() is not None)
        assert process.returncode==88
    finally:
        stop(process,log)
    count=sql("SELECT count(*) FROM paper_orders o JOIN paper_cycles c ON c.id=o.cycle_id WHERE c.account_id=%s",(account,))[0][0]
    process,log=start_engine(tmp_path,account,clock="virtual",fixture_ready=True)
    try:
        wait(lambda:sql("SELECT id FROM paper_events WHERE account_id=%s AND kind='paper.replay_restart_quarantined'",(account,)))
        capture(account)
        assert sql("SELECT count(*) FROM paper_orders o JOIN paper_cycles c ON c.id=o.cycle_id WHERE c.account_id=%s",(account,))[0][0]==count
        assert not api.get(f"/paper/{account}/status").json()["authorized"]
    finally:
        stop(process,log)

@pytest.mark.parametrize("field,value",[("delay_ms",-1),("delay_ms",.5),("wait_for_paper_leg",4),("replay_clock","yes")])
def test_invalid_schedule_fails_startup_before_orders(api,tmp_path,field,value):
    frozen_fixture(tmp_path)
    path=tmp_path/"market.jsonl"
    frames=[json.loads(line) for line in path.read_text().splitlines()]
    frames[0][field]=value
    path.write_text("".join(json.dumps(frame)+"\n" for frame in frames))
    account="invalid-"+uuid.uuid4().hex[:12]
    create(api,account)
    process,log=start_engine(tmp_path,account,clock="virtual",fixture_ready=True,expect_ready=False)
    try:
        wait(lambda:process.poll() is not None)
        assert process.returncode!=0
        assert not sql("SELECT id FROM paper_cycles WHERE account_id=%s",(account,))
    finally:
        stop(process,log)

def test_synthetic_generator_refreshes_metadata_for_logical_long_replay(api,tmp_path):
    subprocess.run([sys.executable,str(ROOT/"scripts/make_paper_replay.py"),str(tmp_path),"--cycles","310"],check=True,capture_output=True)
    for line in (tmp_path/"market.jsonl").read_text().splitlines():
        event=json.loads(line)["event"]
        if event["type"]=="order_book" and event["symbol"]=="ETHUSDT":
            assert all(Decimal(str(level["price"])) % Decimal("0.001") == 0
                       for side in ("bids","asks") for level in event[side])
    account="catalog-"+uuid.uuid4().hex[:12]
    create(api,account)
    process,log=start_engine(tmp_path,account,clock="virtual",fixture_ready=True)
    try:
        enable(api,account)
        # This is 310 real fsync-backed cycles, not a mocked clock microbenchmark.
        # Measured local throughput completed ~250 cycles in the former 120s budget.
        wait(lambda:sql("SELECT id FROM paper_events WHERE account_id=%s AND kind='paper.replay_finished'",(account,)),timeout=300)
        with psycopg.connect(URL) as connection:
            snapshot=read_snapshot(connection,account)
        verify(snapshot)
        assert len(snapshot["cycles"])==310
        assert all(c["state"]=="COMPLETED" for c in snapshot["cycles"])
    finally:
        stop(process,log)

def test_market_batch_at_ioc_deadline_is_fully_applied_before_fill(api,tmp_path):
    frozen_fixture(tmp_path,"partial")
    path=tmp_path/"market.jsonl"
    frames=[json.loads(line) for line in path.read_text().splitlines()]
    index=next(i for i,f in enumerate(frames) if f.get("wait_for_paper_leg")==2)
    marker=json.loads(json.dumps(next(f for f in frames if f["event"]["type"]=="health")))
    marker.update(wait_for_paper_leg=2,delay_ms=200)
    frames[index].pop("wait_for_paper_leg")
    frames[index]["delay_ms"]=0
    frames.insert(index,marker)
    path.write_text("".join(json.dumps(frame)+"\n" for frame in frames))
    account="batch-"+uuid.uuid4().hex[:12]
    create(api,account)
    process,log=start_engine(tmp_path,account,clock="virtual",fixture_ready=True,partial=True)
    try:
        enable(api,account)
        wait(lambda:sql("SELECT id FROM paper_events WHERE account_id=%s AND kind='paper.replay_finished'",(account,)))
        _,snapshot=capture(account)
        second=next(o for o in snapshot["orders"] if o["intent"].get("leg_number")==2)
        assert second["result"]["status"]=="PartiallyFilledCanceled"
        assert Decimal(second["result"]["quantity"])==10
        assert snapshot["cycles"][0]["state"]=="RECOVERED"
    finally:
        stop(process,log)
