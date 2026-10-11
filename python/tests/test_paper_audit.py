"""Paper audit regressions against independent PostgreSQL sessions."""
import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from api import autonomous_paper as paper

pytestmark = pytest.mark.skipif(not os.getenv("ARB_DATABASE_URL"), reason="requires PostgreSQL")


@pytest.fixture
def account():
    engine = create_engine(os.environ["ARB_DATABASE_URL"])
    name = "audit-" + uuid.uuid4().hex[:16]
    with Session(engine) as db:
        paper.create_account(paper.AccountCommand(account_id=name, amount="500", request_id="fund"), db)
    yield engine, name
    engine.dispose()


def test_missing_balance_row_is_a_reconciliation_failure(account):
    engine, name = account
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM paper_balances WHERE account_id=:a"), {"a": name})
    with Session(engine) as db:
        result = paper.reconciliation(name, db)
    assert not result["balanced"], result
    assert result["checks"][0]["available_difference"] == "-500"


def test_future_heartbeat_does_not_authorize_start(account, monkeypatch):
    engine, name = account
    monkeypatch.setattr(paper.settings, "arb_trading_mode", "paper")
    monkeypatch.setattr(paper.settings, "arb_paper_trading_enabled", True)
    monkeypatch.setattr(paper.settings, "arb_live_trading_enabled", False)
    with engine.begin() as connection:
        connection.execute(text("UPDATE paper_accounts SET heartbeat=:h WHERE id=:a"),
                           {"a": name, "h": datetime.now(UTC) + timedelta(hours=1)})
    with Session(engine) as db:
        assert not paper.status(name, db)["runtime_online"]
    with Session(engine) as db, pytest.raises(HTTPException) as error:
        paper.control(name, "start", paper.ControlCommand(request_id="start", reason="audit"), db)
    assert error.value.status_code == 409


@pytest.mark.parametrize("value", ["NaN", "Infinity"])
def test_nonfinite_balance_is_rejected_by_postgresql(account, value):
    engine, name = account
    with engine.begin() as connection, pytest.raises(IntegrityError):
        connection.execute(text("UPDATE paper_balances SET available=CAST(:v AS numeric) WHERE account_id=:a"),
                           {"a": name, "v": value})


def test_financial_response_uses_one_committed_snapshot(account, monkeypatch):
    engine, name = account
    original = paper._query
    changed = False

    def query(db, sql, **params):
        nonlocal changed
        result = original(db, sql, **params)
        if sql.startswith("SELECT * FROM paper_accounts") and not changed:
            changed = True
            with engine.begin() as connection:
                connection.execute(text("UPDATE paper_balances SET available=501 WHERE account_id=:a"), {"a": name})
                connection.execute(text("INSERT INTO paper_ledger(account_id,operation_id,asset,debit_scope,credit_scope,amount) "
                                        "VALUES(:a,'extra-funding','USDT','virtual_funding','available',1)"), {"a": name})
        return result

    monkeypatch.setattr(paper, "_query", query)
    with Session(engine) as db:
        reader = paper.get_read_db(db)
        next(reader)
        try:
            assert paper.status(name, db)["balances"][0]["available"] == "500"
        finally:
            reader.close()
    with Session(engine) as db:
        assert paper.status(name, db)["balances"][0]["available"] == "501"
