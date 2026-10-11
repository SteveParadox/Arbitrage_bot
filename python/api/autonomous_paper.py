"""Authoritative paper ledger controls and reads; never invokes an exchange API."""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from analytics.db import get_db
from api.security import require_paper_auth
from api.settings import settings

router = APIRouter(prefix="/paper", tags=["autonomous-paper"],
                   dependencies=[Depends(require_paper_auth)])
DB = Annotated[Session, Depends(get_db)]
Identity = Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")]


def get_read_db(db: DB):
    # All queries in one response must see the same financial commit.
    db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
    db.execute(text("SET TRANSACTION READ ONLY"))
    _bounded_transaction(db)
    yield db


ReadDB = Annotated[Session, Depends(get_read_db)]


def _runtime_online(heartbeat):
    return heartbeat is not None and 0 <= (datetime.now(UTC) - heartbeat).total_seconds() <= 5


def _bounded_transaction(db):
    db.execute(text("SET LOCAL lock_timeout = '2s'"))
    db.execute(text("SET LOCAL statement_timeout = '5s'"))


class AccountCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")
    account_id: Identity
    asset: Annotated[str, Field(min_length=1, max_length=24, pattern=r"^[A-Z0-9]+$")] = "USDT"
    amount: Decimal = Field(gt=0, le=1_000_000)
    request_id: Identity


class ControlCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: Identity
    reason: str = Field(min_length=1, max_length=256)


def _query(db: Session, sql: str, **params):
    return db.execute(text(sql), params)


def _account(db: Session, account: str, lock: bool = False):
    if lock:
        _bounded_transaction(db)
    row = _query(db, "SELECT * FROM paper_accounts WHERE id=:id" +
                 (" FOR UPDATE" if lock else ""), id=account).mappings().first()
    if row is None:
        raise HTTPException(404, "paper account not found")
    return row


def _fingerprint(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def _prior(db: Session, account: str, request_id: str, fingerprint: str):
    row = _query(db, "SELECT fingerprint,response FROM paper_commands "
                 "WHERE account_id=:a AND request_id=:r", a=account, r=request_id).first()
    if row is None:
        return None
    if row.fingerprint != fingerprint:
        raise HTTPException(409, "request ID reused with different payload")
    return row.response


def _record(db: Session, account: str, request_id: str, fingerprint: str, response: dict):
    _query(db, "INSERT INTO paper_commands(account_id,request_id,fingerprint,response) "
           "VALUES(:a,:r,:f,CAST(:v AS jsonb))", a=account, r=request_id, f=fingerprint,
           v=json.dumps(response))
    _query(db, "INSERT INTO paper_events(account_id,kind,payload) "
           "VALUES(:a,'paper.control',CAST(:v AS jsonb))", a=account,
           v=json.dumps({**response, "request_id": request_id}))
    db.commit()
    return response


@router.post("/accounts")
def create_account(command: AccountCommand, db: DB):
    _bounded_transaction(db)
    # Transaction-level lock serializes first creation, including retries before account exists.
    _query(db, "SELECT pg_advisory_xact_lock(hashtextextended(:a,8148))", a=command.account_id)
    fingerprint = _fingerprint(command.model_dump(mode="json"))
    if _query(db, "SELECT id FROM paper_accounts WHERE id=:a", a=command.account_id).first():
        prior = _prior(db, command.account_id, command.request_id, fingerprint)
        if prior:
            return prior
        raise HTTPException(409, "account exists; balances cannot be reinitialized")
    _query(db, "INSERT INTO paper_accounts(id,config) VALUES(:a,CAST(:v AS jsonb))",
           a=command.account_id, v=json.dumps({"start_asset": command.asset}))
    _query(db, "INSERT INTO paper_balances(account_id,asset,available) VALUES(:a,:s,:v)",
           a=command.account_id, s=command.asset, v=command.amount)
    _query(db, "INSERT INTO paper_ledger(account_id,operation_id,asset,debit_scope,"
           "credit_scope,amount) VALUES(:a,:r,:s,'virtual_funding','available',:v)",
           a=command.account_id, r=command.request_id, s=command.asset, v=command.amount)
    return _record(db, command.account_id, command.request_id, fingerprint,
                   {"account_id": command.account_id, "mode": "paper", "enabled": False})


@router.post("/{account}/control/{action}")
def control(account: str, action: Literal["start", "stop"], command: ControlCommand, db: DB):
    row = _account(db, account, True)
    fingerprint = _fingerprint({"action": action, **command.model_dump()})
    prior = _prior(db, account, command.request_id, fingerprint)
    if prior:
        return prior
    if action == "start":
        if (settings.arb_trading_mode != "paper" or not settings.arb_paper_trading_enabled
                or settings.arb_live_trading_enabled):
            raise HTTPException(409, "deployment must explicitly authorize paper and disable live")
        if row["recovering"]:
            raise HTTPException(409, "paper runtime recovery is still in progress")
        if row["blocked"]:
            raise HTTPException(409, "account has reconciliation or residual inventory block")
        if not _runtime_online(row["heartbeat"]):
            raise HTTPException(409, "authoritative paper runtime offline or recovery incomplete")
    _query(db, "UPDATE paper_accounts SET enabled=:v WHERE id=:a", a=account, v=action == "start")
    return _record(db, account, command.request_id, fingerprint,
                   {"account_id": account, "mode": "paper", "enabled": action == "start",
                    "detail": "stop prevents new exposure; active cycle retains recovery policy"})


def _decimal_rows(rows):
    return [{k: format(v, "f") if isinstance(v, Decimal) else v for k, v in row.items()} for row in rows]


@router.get("/{account}/status")
def status(account: str, db: ReadDB, request: Request = None):
    row = _account(db, account)
    balances = _decimal_rows(_query(db, "SELECT asset,available,reserved,version,reconciled_at "
                                    "FROM paper_balances WHERE account_id=:a ORDER BY asset",
                                    a=account).mappings())
    active = _query(db, "SELECT count(*) FROM paper_cycles WHERE account_id=:a AND NOT settled",
                    a=account).scalar_one()
    pending = _query(db, "SELECT count(*) FROM paper_orders o JOIN paper_cycles c ON c.id=o.cycle_id "
                     "WHERE c.account_id=:a AND o.state='SUBMITTING'", a=account).scalar_one()
    online = _runtime_online(row["heartbeat"])
    return {"account_id": account, "mode": "paper", "simulated": True,
            "clock": row["config"].get("runtime_clock", "wall"),
            "can_control": request is not None and getattr(request.state, "paper_role", "viewer") != "viewer",
            "state": "recovering" if row["recovering"] else "blocked" if row["blocked"] else "offline" if not online else
            "executing" if active else "running" if row["enabled"] else "stopped",
            "authorized": row["enabled"] and online and not row["blocked"] and not row["recovering"], "blocked": row["blocked"], "runtime_online": online,
            "heartbeat": row["heartbeat"], "epoch": row["epoch"], "active_cycles": active,
            "pending_orders": pending, "balances": balances}


@router.get("/{account}/cycles")
def cycles(account: str, db: ReadDB, limit: int = Query(100, ge=1, le=500)):
    _account(db, account)
    result = _decimal_rows(_query(db, "SELECT * FROM paper_cycles WHERE account_id=:a "
                                 "ORDER BY created_at DESC LIMIT :n", a=account, n=limit).mappings())
    ids = [c["id"] for c in result]
    by_cycle: dict[str, list] = {}
    if ids:
        for order in _query(db, "SELECT cycle_id,intent,result FROM paper_orders "
                            "WHERE cycle_id=ANY(CAST(:ids AS TEXT[])) AND result IS NOT NULL", ids=ids).mappings():
            by_cycle.setdefault(order["cycle_id"], []).append(order)
    for cycle in result:
        fees: dict[str, Decimal] = {}
        gross = Decimal(0)
        for order in by_cycle.get(cycle["id"], []):
            fill = order["result"]
            asset = fill["fee_currency"]
            fees[asset] = fees.get(asset, Decimal(0))+Decimal(fill["fee"])
            if order["intent"]["to"] == cycle["start_asset"]:
                gross += Decimal(fill["quote_value"] if order["intent"]["side"] == "Sell" else fill["quantity"])
        pnl = Decimal(cycle["realized_pnl"]) if cycle["realized_pnl"] is not None else None
        start = Decimal(cycle["starting"])
        expected = cycle["risk"].get("candidate", {})
        cycle["paper_metrics"] = {"simulated": True, "expected_gross_profit": expected.get("gross_profit"),
                                  "expected_net_profit": cycle["expected_profit"], "simulated_gross_start_asset_received": str(gross),
                                  "fees_by_asset": {a: str(v) for a, v in fees.items()},
                                  "return_pct": str(pnl/start*100) if pnl is not None else None,
                                  "return_bps": str(pnl/start*10000) if pnl is not None else None,
                                  "capital_committed": cycle["starting"],
                                  "execution_duration_ms": (cycle["completed_at"]-cycle["created_at"]).total_seconds()*1000 if cycle["completed_at"] else None,
                                  "expected_minus_realized_net": str(Decimal(cycle["expected_profit"])-pnl) if pnl is not None else None}
    return result


@router.get("/{account}/orders")
def orders(account: str, db: ReadDB, limit: int = Query(100, ge=1, le=500)):
    _account(db, account)
    return _decimal_rows(_query(db, "SELECT o.* FROM paper_orders o JOIN paper_cycles c "
                                "ON c.id=o.cycle_id WHERE c.account_id=:a "
                                "ORDER BY o.created_at DESC LIMIT :n", a=account, n=limit).mappings())


@router.get("/{account}/events")
def events(account: str, db: ReadDB, limit: int = Query(100, ge=1, le=500)):
    _account(db, account)
    return _decimal_rows(_query(db, "SELECT * FROM paper_events WHERE account_id=:a "
                                "ORDER BY id DESC LIMIT :n", a=account, n=limit).mappings())


@router.get("/{account}/performance")
def performance(account: str, db: ReadDB):
    _account(db, account)
    rows = _query(db, "SELECT state,realized_pnl,expected_profit,starting,created_at,completed_at "
                  "FROM paper_cycles WHERE account_id=:a ORDER BY completed_at,id", a=account).mappings().all()
    closed = [r for r in rows if r["realized_pnl"] is not None]
    pnl = [r["realized_pnl"] for r in closed]
    wins = [v for v in pnl if v > 0]
    losses = [v for v in pnl if v < 0]
    total_win, total_loss = sum(wins, Decimal(0)), -sum(losses, Decimal(0))
    equity = peak = drawdown = Decimal(0)
    for v in pnl:
        equity += v
        peak = max(peak, equity)
        drawdown = max(drawdown, peak-equity)
    daily: dict[str, Decimal] = {}
    for r in closed:
        key = r["completed_at"].astimezone(UTC).date().isoformat()
        daily[key] = daily.get(key, Decimal(0)) + r["realized_pnl"]
    duration = [(r["completed_at"]-r["created_at"]).total_seconds()*1000 for r in closed]
    return {"mode": "paper", "simulated": True, "closed_cycles": len(closed),
            "total_cycles": len(rows), "net_realized_pnl": str(sum(pnl, Decimal(0))),
            "daily_pnl": {k: str(v) for k, v in daily.items()},
            "win_rate_pct": str(Decimal(len(wins))*100/len(pnl)) if pnl else None,
            "average_win": str(total_win/len(wins)) if wins else None,
            "average_loss": str(-total_loss/len(losses)) if losses else None,
            "profit_factor": str(total_win/total_loss) if total_loss else None,
            "max_drawdown_start_asset": str(drawdown),
            "average_duration_ms": sum(duration)/len(duration) if duration else None,
            "unresolved_cycles": sum(r["state"] == "RECOVERY_REQUIRED" for r in rows),
            "valuation": "Residual inventory is excluded from realized P&L; no fresh mark is fabricated"}


@router.get("/{account}/fills")
def fills(account: str, db: ReadDB, limit: int = Query(100, ge=1, le=500)):
    return [{"fill_id": "paper:"+order["id"]+":fill", "order_id": order["id"],
             "cycle_id": order["cycle_id"], "simulated": True, **order["result"]}
            for order in orders(account, db, limit)
            if order["result"] is not None and Decimal(order["result"]["quantity"]) > 0]


@router.get("/{account}/reservations")
def reservations(account: str, db: ReadDB, limit: int = Query(100, ge=1, le=500)):
    return [{"reservation_id": "paper:"+cycle["id"]+":reservation", "cycle_id": cycle["id"],
             "asset": cycle["start_asset"], "amount": cycle["starting"],
             "state": "SETTLED" if cycle["settled"] else "CONSUMING" if
             cycle["state"] == "EXECUTING" else "RESERVED"}
            for cycle in cycles(account, db, limit)]


@router.get("/{account}/reconciliation")
def reconciliation(account: str, db: ReadDB):
    _account(db, account)
    rows = _query(db, "WITH assets AS (SELECT asset FROM paper_balances WHERE account_id=:a "
                  "UNION SELECT asset FROM paper_ledger WHERE account_id=:a "
                  "UNION SELECT start_asset FROM paper_cycles WHERE account_id=:a AND NOT settled) "
                  "SELECT s.asset,COALESCE(b.available,0) AS available,COALESCE(b.reserved,0) AS reserved,COALESCE((SELECT sum(CASE WHEN "
                  "credit_scope='available' THEN amount ELSE -amount END) FROM paper_ledger l "
                  "WHERE l.account_id=:a AND l.asset=s.asset AND "
                  "(debit_scope='available' OR credit_scope='available')),0) AS ledger_available,"
                  "COALESCE((SELECT sum(starting) FROM paper_cycles c WHERE c.account_id=:a "
                  "AND c.start_asset=s.asset AND NOT settled),0) AS expected_reserved "
                  "FROM assets s LEFT JOIN paper_balances b ON b.account_id=:a AND b.asset=s.asset "
                  "ORDER BY s.asset", a=account).mappings().all()
    checks = [{"asset": r["asset"], "available_difference": str(r["available"]-r["ledger_available"]),
               "reservation_difference": str(r["reserved"]-r["expected_reserved"])} for r in rows]
    return {"mode": "paper", "balanced": all(Decimal(r["available_difference"]) == 0 and
            Decimal(r["reservation_difference"]) == 0 for r in checks), "checks": checks}


@router.get("/{account}/valuation")
def valuation(account: str, db: ReadDB):
    row = _account(db, account)
    start_asset = row["config"]["start_asset"]
    actual = {r["asset"]: r["available"] for r in _query(db,
              "SELECT asset,available FROM paper_balances WHERE account_id=:a", a=account).mappings()}
    for r in _query(db, "SELECT holdings FROM paper_cycles WHERE account_id=:a AND NOT settled",
                    a=account).mappings():
        for asset, amount in r["holdings"].items():
            actual[asset] = actual.get(asset, Decimal(0))+Decimal(amount)
    marks = {r["asset"]: r for r in _query(db, "SELECT * FROM paper_valuations WHERE account_id=:a",
                                        a=account).mappings()}
    clock = row["config"].get("runtime_clock", "wall")
    now_ms = int(datetime.now(UTC).timestamp()*1000)
    if clock == "virtual":
        audit = _query(db, "SELECT payload FROM paper_events WHERE account_id=:a "
                       "AND kind IN ('paper.replay_finished','paper.replay_restart_quarantined') "
                       "ORDER BY id DESC LIMIT 1", a=account).scalar_one_or_none()
        now_ms = (audit.get("completed_at_ms", audit.get("restored_at_ms")) if audit else None)
    checks = []
    equity = Decimal(0)
    complete = True
    for asset, amount in actual.items():
        mark = marks.get(asset)
        reason = None
        if asset == start_asset or amount == 0:
            value, details = amount, {"valuation_source": "paper_ledger_cash", "market_timestamp_ms": now_ms}
        elif mark is None:
            value, details, reason = None, {}, "MISSING_EVIDENCE"
        else:
            value, details = mark["estimated_value"], mark["details"]
            timestamp = details.get("market_timestamp_ms")
            if mark["amount"] != amount:
                value, reason = None, "AMOUNT_CHANGED_SINCE_VALUATION"
            elif now_ms is None or timestamp is None or not 0 <= now_ms-timestamp <= details.get("max_age_ms", 0):
                value, reason = None, "STALE_OR_MISSING_MARKET_EVIDENCE"
            elif value is None:
                reason = "INSUFFICIENT_EXECUTABLE_EVIDENCE"
        if value is None:
            complete = False
        else:
            equity += value
        checks.append({"asset": asset, "amount": str(amount), "estimated_value": str(value) if value is not None else None,
                       "details": details, "missing_reason": reason})
    funded = _query(db, "SELECT COALESCE(sum(amount),0) FROM paper_ledger WHERE account_id=:a "
                   "AND asset=:s AND debit_scope='virtual_funding'", a=account, s=start_asset).scalar_one()
    realized = _query(db, "SELECT COALESCE(sum(realized_pnl),0) FROM paper_cycles WHERE account_id=:a",
                     a=account).scalar_one()
    return {"mode": "paper", "simulated": True, "start_asset": start_asset, "complete_evidence": complete,
            "estimated_equity": str(equity) if complete else None,
            "estimated_unrealized_pnl": str(equity-funded-realized) if complete else None,
            "realized_pnl": str(realized), "clock": clock, "valued_at_ms": now_ms, "inventory": checks,
            "policy": "Executable depth after modeled liquidation fees; unavailable or stale marks are excluded and total equity is null"}
