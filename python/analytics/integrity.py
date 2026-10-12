"""Independent read-only accounting oracle: exact fractions from persisted evidence."""

from collections import defaultdict
from fractions import Fraction
import time

from psycopg.rows import dict_row
from analytics.paper_integrity import read_snapshot, verify


def audit(connection):
    report = {
        "checked_at": time.time(),
        "balanced": True,
        "paper_accounts": 0,
        "execution_accounts": 0,
        "failures": [],
    }

    def fail(account, error):
        report["balanced"] = False
        report["failures"].append({"account": account, "detail": str(error)[:512]})

    accounts = connection.execute("SELECT id FROM paper_accounts").fetchall()
    for (account,) in accounts:
        report["paper_accounts"] += 1
        try:
            verify(read_snapshot(connection, account))
        except (AssertionError, ValueError, TypeError, KeyError, ZeroDivisionError) as error:
            fail(account, error)
    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        with connection.cursor(row_factory=dict_row) as cursor:

            def rows(table, account):
                cursor.execute(f"SELECT * FROM {table} WHERE account_id=%s", (account,))
                return cursor.fetchall()

            cursor.execute("SELECT * FROM execution_accounts")
            accounts = cursor.fetchall()
            for account in accounts:
                identity = account["id"]
                report["execution_accounts"] += 1
                try:
                    cycles = rows("execution_cycles", identity)
                    orders = {row["link_id"]: row for row in rows("execution_orders", identity)}
                    fills = rows("execution_fills", identity)
                    movements = rows("execution_movements", identity)
                    reconciliations = rows("execution_reconciliation", identity)
                    transfer = {(row["execution_id"], row["role"]): row for row in movements}
                    assert len(transfer) == len(movements), "duplicate economic movement"
                    totals = defaultdict(lambda: defaultdict(Fraction))
                    seen = set()
                    for fill in fills:
                        execution_id = fill["execution_id"]
                        assert execution_id not in seen, "duplicate exchange execution identity"
                        seen.add(execution_id)
                        evidence = fill["evidence"]
                        order = orders[fill["link_id"]]
                        intent = order["intent"]
                        quantity, value, fee = (
                            Fraction(str(evidence[k])) for k in ("quantity", "value", "fee")
                        )
                        assert quantity > 0 and value > 0 and fee >= 0
                        assert evidence["order_id"] == order["exchange_order_id"]
                        spent, received = (
                            (value, quantity) if intent["side"] == "Buy" else (quantity, value)
                        )
                        for asset, role, amount in (
                            (intent["from"], "input", -spent),
                            (intent["to"], "output", received),
                            (evidence["fee_currency"], "fee", -fee),
                        ):
                            if amount == 0:
                                assert (execution_id, role) not in transfer
                            else:
                                row = transfer[(execution_id, role)]
                                assert (
                                    row["asset"] == asset and Fraction(str(row["amount"])) == amount
                                ), f"fill/movement mismatch: {execution_id}:{role}"
                                totals[fill["cycle_id"]][asset] += amount
                    assert all(row["execution_id"] in seen for row in movements), (
                        "movement without confirmed fill"
                    )
                    for cycle in cycles:
                        held = defaultdict(
                            Fraction, {cycle["start_asset"]: Fraction(str(cycle["starting"]))}
                        )
                        for asset, delta in totals[cycle["id"]].items():
                            held[asset] += delta
                        recorded = {a: Fraction(str(v)) for a, v in cycle["holdings"].items()}
                        assert all(v >= 0 for v in held.values()), (
                            f"negative cycle inventory: {cycle['id']}"
                        )
                        assert all(
                            held[a] == recorded.get(a, 0) for a in held.keys() | recorded.keys()
                        ), f"inventory mismatch: {cycle['id']}"
                        if cycle["settled"]:
                            assert not cycle["committed"] and cycle["completed_at"] is not None
                            assert all(
                                v == 0 for a, v in held.items() if a != cycle["start_asset"]
                            ), "settled residual inventory"
                            assert Fraction(str(cycle["realized_pnl"])) == held[
                                cycle["start_asset"]
                            ] - Fraction(str(cycle["starting"])), "P&L mismatch"
                            assert all(
                                o["terminal"] and (o["confirmed"] or o["state"] == "REJECTED")
                                for o in orders.values()
                                if o["cycle_id"] == cycle["id"]
                            ), "settled uncertain order"
                        else:
                            assert cycle["realized_pnl"] is None, "unsettled realized P&L"
                    healthy = sorted(
                        (row for row in reconciliations if row["healthy"]),
                        key=lambda row: row["id"],
                    )
                    if account["expected_wallet"] is not None:
                        assert healthy, "wallet lacks opening exchange evidence"
                        expected = defaultdict(
                            Fraction,
                            {
                                a: Fraction(str(v))
                                for a, v in healthy[0]["evidence"]["wallet"].items()
                            },
                        )
                        for row in movements:
                            expected[row["asset"]] += Fraction(str(row["amount"]))
                        recorded = {
                            a: Fraction(str(v)) for a, v in account["expected_wallet"].items()
                        }
                        assert all(v >= 0 for v in expected.values()), (
                            "negative reconstructed wallet"
                        )
                        assert all(
                            expected[a] == recorded.get(a, 0)
                            for a in expected.keys() | recorded.keys()
                        ), "wallet/movement mismatch"
                    assert sum(c["committed"] for c in cycles) <= 1, (
                        "multiple active exchange capital owners"
                    )
                except (
                    AssertionError,
                    ValueError,
                    TypeError,
                    KeyError,
                    ZeroDivisionError,
                ) as error:
                    fail(identity, error)
    return report
