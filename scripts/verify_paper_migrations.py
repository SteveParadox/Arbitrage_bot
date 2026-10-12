"""Exercise upgrade/rollback/re-upgrade in an isolated, disposable PostgreSQL database."""
from __future__ import annotations

import os
import subprocess
import sys
import uuid
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import psycopg


def main():
    root = Path(__file__).resolve().parents[1]
    source = os.environ["ARB_DATABASE_URL"].replace("postgresql+psycopg:", "postgresql:")
    parsed = urlsplit(source)
    admin = urlunsplit(parsed._replace(path="/postgres"))
    name = "arb_migration_"+uuid.uuid4().hex
    isolated = urlunsplit(parsed._replace(path="/"+name))
    env = {**os.environ, "ARB_DATABASE_URL": isolated.replace("postgresql:", "postgresql+psycopg:")}
    with psycopg.connect(admin, autocommit=True) as connection:
        connection.execute('CREATE DATABASE "'+name+'"')
        try:
            for target in ("head", "0007_reconciliation_idempotency", "head"):
                action = "downgrade" if target.startswith("0007") else "upgrade"
                subprocess.run([sys.executable,"-m","alembic",action,target],cwd=root/"python",env=env,check=True)
                with psycopg.connect(isolated) as db:
                    tables = {r[0] for r in db.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")}
                    expected = {"paper_accounts","paper_balances","paper_cycles","paper_orders","paper_ledger","paper_events","paper_commands","paper_valuations"}
                    exchange = {"execution_accounts", "execution_cycles", "execution_orders", "execution_fills", "execution_movements", "execution_reconciliation", "execution_events", "execution_commands"}
                    if action == "upgrade":
                        assert expected <= tables, tables
                        assert exchange <= tables, tables
                        db.execute("INSERT INTO execution_accounts(id,mode,exchange_uid,limits) VALUES('exchange-test','testnet','123','{}')")
                        assert db.execute("SELECT enabled,blocked,recovering,market_ready FROM execution_accounts").fetchone() == (False, True, True, False)
                        assert db.execute("SELECT 1 FROM pg_indexes WHERE indexname='execution_one_active'").fetchone() == (1,)
                        db.execute("INSERT INTO paper_accounts(id,config) VALUES('migration-test','{\"start_asset\":\"USDT\"}')")
                        assert db.execute("SELECT mode,enabled,blocked FROM paper_accounts").fetchone() == ("paper",False,False)
                        try:
                            with db.transaction():
                                db.execute("INSERT INTO paper_balances(account_id,asset,available) VALUES('migration-test','USDT',-1)")
                        except psycopg.errors.CheckViolation:
                            pass
                        else:
                            raise AssertionError("negative balance constraint missing")
                    else:
                        assert not (expected & tables), tables
                        assert not (exchange & tables), tables
                        assert "engine_events" in tables
            print("Migration fresh upgrade, rollback to 0007, re-upgrade and negative balance constraint: PASS")
        finally:
            connection.execute('DROP DATABASE "'+name+'" WITH (FORCE)')


if __name__ == "__main__":
    main()
