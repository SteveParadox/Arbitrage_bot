"""Exchange-scoped controls. No exchange credentials or order HTTP client in the API."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text

from api.autonomous_paper import DB, Identity, ReadDB, _bounded_transaction, _decimal_rows, _fingerprint, _runtime_online
from api.security import require_execution_auth
from api.settings import settings

router = APIRouter(prefix="/execution", tags=["authenticated-execution"], dependencies=[Depends(require_execution_auth)])


class Limits(BaseModel):
    model_config = ConfigDict(extra="forbid")
    max_trade: Decimal = Field(gt=0, le=500)
    max_exposure: Decimal = Field(gt=0, le=1000)
    max_asset_exposure: Decimal = Field(gt=0, le=1000)
    max_daily_loss: Decimal = Field(gt=0, le=50)
    max_drawdown: Decimal = Field(gt=0, le=50)
    max_consecutive_losses: int = Field(ge=1, le=3)


class Account(BaseModel):
    model_config = ConfigDict(extra="forbid")
    account_id: Identity
    mode: Literal["testnet", "live"]
    exchange_uid: Annotated[str, Field(pattern=r"^[0-9]{1,24}$")]
    mock: bool = False
    limits: Limits
    request_id: Identity


class Command(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: Identity
    reason: str = Field(min_length=1, max_length=256)
    authorization: Literal["authorize_testnet", "authorize_mainnet", "stop"]


def query(db, sql, **params):
    return db.execute(text(sql), params)


def account_row(db, account, request, lock=False):
    if lock:
        _bounded_transaction(db)
    row = query(db, "SELECT * FROM execution_accounts WHERE id=:a" + (" FOR UPDATE" if lock else ""), a=account).mappings().first()
    if row is None:
        raise HTTPException(404, "execution account not found")
    if row["mode"] not in request.state.execution_grant["modes"]:
        raise HTTPException(403, "execution mode permission denied")
    return row


def record(db, account, command, payload, request):
    fingerprint = _fingerprint(payload)
    old = query(db, "SELECT fingerprint,response FROM execution_commands WHERE account_id=:a AND request_id=:r", a=account, r=command.request_id).first()
    if old:
        if old.fingerprint != fingerprint:
            raise HTTPException(409, "request ID conflicts with prior durable command")
        return old.response, fingerprint
    return None, fingerprint


def commit_command(db, account, command, fingerprint, response, request):
    subject = request.state.execution_grant["subject"]
    query(db, "INSERT INTO execution_commands(account_id,request_id,fingerprint,subject,response) VALUES(:a,:r,:f,:s,CAST(:v AS jsonb))", a=account, r=command.request_id, f=fingerprint, s=subject, v=json.dumps(response))
    query(db, "INSERT INTO execution_events(account_id,kind,payload) VALUES(:a,'execution.control',CAST(:v AS jsonb))", a=account, v=json.dumps({**response, "request_id": command.request_id, "subject": subject}))
    db.commit()
    return response


@router.post("/accounts")
def create(command: Account, request: Request, db: DB):
    grant = request.state.execution_grant
    if grant["role"] != "admin" or command.account_id not in grant["accounts"] or command.mode not in grant["modes"]:
        raise HTTPException(403, "exchange account provisioning permission required")
    if command.mock and (settings.arb_env != "development" or command.mode != "testnet"):
        raise HTTPException(409, "mock account allowed only for development testnet")
    _bounded_transaction(db)
    query(db, "SELECT pg_advisory_xact_lock(hashtextextended(:a,9151))", a=command.account_id)
    fingerprint = _fingerprint(command.model_dump(mode="json"))
    if query(db, "SELECT id FROM execution_accounts WHERE id=:a", a=command.account_id).first():
        prior, _ = record(db, command.account_id, command, command.model_dump(mode="json"), request)
        if prior:
            return prior
        raise HTTPException(409, "exchange account identity and limits are immutable")
    if query(db, "SELECT id FROM execution_accounts WHERE mode=:m AND exchange_uid=:u AND mock=:k", m=command.mode, u=command.exchange_uid, k=command.mock).first():
        raise HTTPException(409, "exchange identity already owned by another account")
    query(db, "INSERT INTO execution_accounts(id,mode,exchange_uid,mock,limits) VALUES(:a,:m,:u,:k,CAST(:v AS jsonb))", a=command.account_id, m=command.mode, u=command.exchange_uid, k=command.mock, v=command.limits.model_dump_json())
    return commit_command(db, command.account_id, command, fingerprint, {"account_id": command.account_id, "mode": command.mode, "authorized": False}, request)


@router.get("/accounts")
def accounts(request: Request, db: ReadDB):
    grant = request.state.execution_grant
    return list(query(db, "SELECT id,mode,mock,state FROM execution_accounts WHERE id=ANY(CAST(:a AS text[])) AND mode=ANY(CAST(:m AS text[])) ORDER BY id", a=grant["accounts"], m=grant["modes"]).mappings())


def deployment_allowed(row):
    if row["mode"] != settings.arb_trading_mode or settings.arb_paper_trading_enabled:
        raise HTTPException(409, "execution deployment mode mismatch")
    if row["mode"] == "testnet" and (not settings.arb_testnet_trading_enabled or settings.arb_live_trading_enabled):
        raise HTTPException(409, "testnet deployment not authorized")
    if row["mode"] == "live" and not settings.arb_live_trading_enabled:
        raise HTTPException(409, "live deployment gate remains disabled")
    try:
        permit = json.loads(Path(settings.arb_execution_deployment_permit).read_text())
        now = int(datetime.now(UTC).timestamp()*1000)
        valid = (permit["version"] == 1 and permit["account_id"] == row["id"] and
                 permit["mode"] == row["mode"] and permit["exchange_uid"] == row["exchange_uid"] and
                 0 < permit["expires_at_ms"]-now <= 86_400_000)
        if row["mode"] == "live":
            valid = valid and not row["mock"] and all(permit[k] is True for k in (
                "offline_tests", "testnet_execution_verified", "reconciliation_verified",
                "crash_recovery_verified", "security_verified", "soak_verified", "human_mainnet_authorization"))
        if not valid:
            raise ValueError("invalid permit")
    except (OSError, ValueError, KeyError, TypeError):
        raise HTTPException(409, "deployment permit missing, stale or incomplete") from None


@router.post("/{account}/control/{action}")
def control(account: str, action: Literal["start", "stop", "emergency-stop"], command: Command, request: Request, db: DB):
    row = account_row(db, account, request, True)
    prior, fingerprint = record(db, account, command, {"action": action, **command.model_dump()}, request)
    if prior:
        return prior
    if action == "start":
        if command.authorization != ("authorize_mainnet" if row["mode"] == "live" else "authorize_testnet"):
            raise HTTPException(403, "explicit mode-specific execution authorization required")
        deployment_allowed(row)
        age = int(datetime.now(UTC).timestamp()*1000)-(row["snapshot_ms"] or 0)
        active = query(db, "SELECT EXISTS(SELECT 1 FROM execution_cycles WHERE account_id=:a AND committed)", a=account).scalar_one()
        market_age=int(datetime.now(UTC).timestamp()*1000)-(row["market_ms"] or 0)
        if row["state"] != "READY" or row["blocked"] or row["recovering"] or row["emergency_stop"] or active or not row["market_ready"] or not 0<=market_age<=1000 or not _runtime_online(row["heartbeat"]) or not 0<=age<=1000:
            raise HTTPException(409, "runtime, reconciliation or capital ownership gate blocked")
    elif command.authorization != "stop":
        raise HTTPException(400, "stop authorization required")
    query(db, "UPDATE execution_accounts SET enabled=:v,emergency_stop=emergency_stop OR :e WHERE id=:a", a=account, v=action == "start", e=action == "emergency-stop")
    # This is a committed authorization change, not a claim that an exchange
    # cancellation or active-cycle stop has been confirmed.
    response = {"account_id": account, "mode": row["mode"], "authorized": action == "start",
                "durable": True, "active_cycle_stop_confirmed": False,
                "detail": "new exposure permission committed; risk-approved recovery remains separate"}
    return commit_command(db, account, command, fingerprint, response, request)


@router.get("/{account}/status")
def status(account: str, request: Request, db: ReadDB):
    row = account_row(db, account, request)
    active = query(db, "SELECT count(*) FROM execution_cycles WHERE account_id=:a AND committed", a=account).scalar_one()
    online = _runtime_online(row["heartbeat"]) and row["state"] not in {"STOPPED", "FAILED"}
    evidence = row["balance_evidence"] or {}
    reserved = query(db, "SELECT COALESCE(sum(starting),0) FROM execution_cycles WHERE account_id=:a AND committed", a=account).scalar_one()
    return {"account_id": account, "mode": row["mode"], "environment": "offline_mock" if row["mock"] else row["mode"],
            "exchange_confirmed": not row["mock"] and row["snapshot_ms"] is not None, "mock_evidence": row["mock"], "state": row["state"] if online or row["state"] in {"STOPPED", "FAILED", "BLOCKED"} else "DEGRADED",
            "runtime_online": online, "authorized": row["enabled"] and online and not row["blocked"] and not row["recovering"] and not row["emergency_stop"], "blocked": row["blocked"],
            "emergency_stop": row["emergency_stop"], "recovering": row["recovering"], "epoch": row["epoch"], "active_cycles": active,
            "can_control": request.state.execution_grant["role"] != "viewer", "snapshot_ms": row["snapshot_ms"],
            "market_ready": row["market_ready"] and online, "market_ms":row["market_ms"],
            "balances": evidence, "committed_usdt": str(reserved), "risk_limits": row["limits"], "heartbeat": row["heartbeat"]}


@router.get("/{account}/health")
def health(account: str, request: Request, db: ReadDB):
    return status(account, request, db)


@router.get("/{account}/{entity}")
def records(account: str, entity: Literal["cycles", "orders", "fills", "reservations", "balances", "reconciliation", "performance", "events"], request: Request, db: ReadDB, limit: int = Query(100, ge=1, le=500)):
    row = account_row(db, account, request)
    if entity == "balances":
        return row["balance_evidence"] or {}
    if entity == "performance":
        rows = query(db, "SELECT state,realized_pnl FROM execution_cycles WHERE account_id=:a", a=account).mappings().all()
        return {"mode": row["mode"], "environment": "offline_mock" if row["mock"] else row["mode"],
                "realized_pnl_usdt": str(sum((r["realized_pnl"] for r in rows if r["realized_pnl"] is not None), Decimal(0))),
                "closed_cycles": sum(r["realized_pnl"] is not None for r in rows), "unresolved_cycles": sum(r["state"] == "RECOVERY_REQUIRED" for r in rows),
                "unrealized_pnl": None, "valuation_policy": "No unsupported residual-inventory valuation"}
    if entity == "reservations":
        return _decimal_rows(query(db, "SELECT id,starting,start_asset,committed,settled,state FROM execution_cycles WHERE account_id=:a ORDER BY created_at DESC LIMIT :n", a=account, n=limit).mappings())
    table = {"cycles": "execution_cycles", "orders": "execution_orders", "fills": "execution_fills", "events": "execution_events", "reconciliation": "execution_reconciliation"}[entity]
    return _decimal_rows(query(db, f"SELECT * FROM {table} WHERE account_id=:a ORDER BY created_at DESC LIMIT :n", a=account, n=limit).mappings())
