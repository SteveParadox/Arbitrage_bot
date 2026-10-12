"""Independent persisted paper accounting oracle; no execution calculations reused."""
from collections import defaultdict
from fractions import Fraction


def read_snapshot(connection, account):
    from psycopg.rows import dict_row
    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        with connection.cursor(row_factory=dict_row) as cursor:
            snapshot = {}
            for table in ("balances", "cycles", "ledger"):
                cursor.execute(f"SELECT * FROM paper_{table} WHERE account_id=%s", (account,))
                snapshot[table] = cursor.fetchall()
            cursor.execute("SELECT o.* FROM paper_orders o JOIN paper_cycles c ON c.id=o.cycle_id "
                           "WHERE c.account_id=%s ORDER BY o.created_at,o.id", (account,))
            snapshot["orders"] = cursor.fetchall()
            return snapshot


def verify(snapshot):
    def number(value):
        return Fraction(str(value))
    available = defaultdict(Fraction)
    cycle_ledger = defaultdict(lambda: defaultdict(Fraction))
    transfers = {}
    for row in snapshot["ledger"]:
        amount = number(row["amount"])
        assert amount > 0
        key = (row["operation_id"], row["asset"], row["debit_scope"], row["credit_scope"])
        assert key not in transfers, f"duplicate ledger operation: {key}"
        transfers[key] = amount
        for scope, sign in ((row["debit_scope"], -1), (row["credit_scope"], 1)):
            if scope == "available":
                available[row["asset"]] += sign * amount
            if scope == "cycle":
                cycle_ledger[row["cycle_id"]][row["asset"]] += sign * amount
    reserved = defaultdict(Fraction)
    results = {}
    orders_by_cycle = defaultdict(list)
    for order in snapshot["orders"]:
        orders_by_cycle[order["cycle_id"]].append(order)
    for cycle in snapshot["cycles"]:
        identity, start = cycle["id"], cycle["start_asset"]
        initial = number(cycle["starting"])
        assets = defaultdict(Fraction, {start: initial})
        for order in (o for o in orders_by_cycle[identity] if o["result"] is not None):
            fill, intent = order["result"], order["intent"]
            quantity, quote, fee = (number(fill[k]) for k in ("quantity", "quote_value", "fee"))
            assert quantity >= 0 and quote >= 0 and fee >= 0
            assert number(fill["requested"]) == number(intent["quantity"])
            assert quantity + number(fill["remaining"]) == number(fill["requested"])
            assert intent["side"] in {"Buy", "Sell"}
            spent, received = (quote, quantity) if intent["side"] == "Buy" else (quantity, quote)
            source, target = intent["from"], intent["to"]
            assert source != target and fill["fee_currency"] == target
            assert spent <= assets[source] and fee <= received
            if quantity == 0:
                assert quote == fee == 0 and fill["price"] is None
            else:
                assert number(fill["price"]) > 0
            assets[source] -= spent
            assets[target] += received - fee
            for asset, debit, credit, amount in (
                (source, "cycle", "simulated_venue", spent),
                (target, "simulated_venue", "cycle", received),
                (target, "cycle", "fees", fee),
            ):
                assert transfers.get((order["id"], asset, debit, credit), 0) == amount
        recorded = {a: number(v) for a, v in cycle["holdings"].items()}
        for asset in assets.keys() | recorded.keys():
            assert assets[asset] == recorded.get(asset, 0), f"receipt/holding mismatch: {identity}:{asset}"
        if not cycle["settled"]:
            reserved[start] += initial
        ledger = cycle_ledger[identity]
        for asset in assets.keys() | ledger.keys():
            assert ledger[asset] == (0 if cycle["settled"] else assets[asset]), f"cycle ledger mismatch: {identity}:{asset}"
        cash_delta = assets[start] - initial
        residual = any(amount > 0 for asset, amount in assets.items() if asset != start)
        realized = None if residual or not cycle["settled"] else cash_delta
        assert (None if cycle["realized_pnl"] is None else number(cycle["realized_pnl"])) == realized
        if cycle["settled"]:
            assert number(cycle["cash_delta"]) == cash_delta
        results[identity] = {"holdings": {a: str(v) for a, v in assets.items()},
                             "realized_pnl": None if realized is None else str(realized),
                             "residual_inventory": residual}
    balances = {row["asset"]: row for row in snapshot["balances"]}
    for asset in available.keys() | reserved.keys() | balances.keys():
        assert asset in balances, f"missing balance row: {asset}"
        row = balances[asset]
        assert number(row["available"]) == available[asset] >= 0, f"available mismatch: {asset}"
        assert number(row["reserved"]) == reserved[asset] >= 0, f"reserved mismatch: {asset}"
    return {"balanced": True, "cycles": results}
