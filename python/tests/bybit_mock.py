"""Loopback-only V5 protocol fixture. All wallet changes derive from its fill journal."""
from __future__ import annotations
import hashlib
import hmac
import json
import threading
import time
from decimal import Decimal, ROUND_DOWN
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit


class BybitMock:
    def __init__(self, uid, scenario="profit", capital="1000"):
        self.uid = uid
        self.scenario = scenario
        self.wallet = {"USDT": Decimal(capital), "BTC": Decimal(0), "ETH": Decimal(0), "MNT": Decimal(1)}
        self.opening = self.wallet.copy()
        self.orders = {}
        self.fills = {}
        self.posts = []
        self.requests = []
        self.errors = []
        self.hide = scenario == "unknown"
        self.delay = .08
        self.lock = threading.RLock()
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def respond(self, result, code=0, http=200):
                body = json.dumps({"retCode": code, "retMsg": "OK" if not code else "mock rejection", "result": result, "time": int(time.time()*1000)}).encode()
                self.send_response(http)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def verify(self, payload):
                timestamp = self.headers.get("X-BAPI-TIMESTAMP", "")
                window = self.headers.get("X-BAPI-RECV-WINDOW", "")
                expected = hmac.new(b"mock-secret", (timestamp+"mock-key"+window+payload).encode(), hashlib.sha256).hexdigest()
                assert self.headers.get("X-BAPI-API-KEY") == "mock-key"
                assert hmac.compare_digest(self.headers.get("X-BAPI-SIGN", ""), expected)
                assert abs(int(time.time()*1000)-int(timestamp)) <= int(window)

            def do_GET(self):
                try:
                    path = urlsplit(self.path)
                    query = parse_qs(path.query)
                    if path.path != "/v5/market/time":
                        self.verify(path.query)
                    with outer.lock:
                        outer.requests.append(("GET", path.path, query))
                        if path.path == "/v5/market/time":
                            return self.respond({"timeSecond": str(int(time.time()))})
                        if path.path == "/v5/user/query-api":
                            return self.respond({"userID": outer.uid, "readOnly": 0, "permissions": {"Spot": ["SpotTrade"], "Wallet": []}})
                        if path.path == "/v5/account/wallet-balance":
                            assert query["accountType"] == ["UNIFIED"]
                            coins = [{"coin": a, "walletBalance": format(v,"f"), "locked": "0", "spotBorrow": "0", "borrowAmount": "0", "spotHedgingQty": "0", "bonus": "0", "totalPositionIM": "0", "equity": format(v,"f"), "usdValue": "0"} for a, v in outer.wallet.items()]
                            return self.respond({"list": [{"accountType": "UNIFIED", "coin": coins, "totalInitialMargin": "0", "totalPerpUPL": "0", "totalEquity": "1000", "totalWalletBalance": "1000", "totalAvailableBalance": "1000"}]})
                        if path.path in {"/v5/order/realtime", "/v5/order/history"}:
                            link = query.get("orderLinkId", [None])[0]
                            order_id = query.get("orderId", [None])[0]
                            orders = [o for o in outer.orders.values() if (link is None or o["orderLinkId"] == link) and (order_id is None or o["orderId"] == order_id)]
                            if outer.hide or (link is None and order_id is None):
                                orders = []
                            return self.respond({"category": "spot", "list": orders, "nextPageCursor": ""})
                        if path.path == "/v5/execution/list":
                            order_id = query["orderId"][0]
                            fills = outer.fills.get(order_id, [])
                            # Exact replay across pages proves pagination deduplication.
                            cursor = "" if "cursor" in query or not fills else "page-2"
                            return self.respond({"category": "spot", "list": fills, "nextPageCursor": cursor})
                        raise AssertionError(f"unexpected GET {path.path}")
                except Exception as error:
                    outer.errors.append(str(error))
                    self.respond({}, http=500)

            def do_POST(self):
                try:
                    payload = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode()
                    self.verify(payload)
                    request = json.loads(payload)
                    time.sleep(outer.delay)
                    with outer.lock:
                        outer.requests.append(("POST", self.path, request))
                        if self.path == "/v5/order/cancel":
                            return self.respond({"orderId": request["orderId"], "orderLinkId": request["orderLinkId"]})
                        assert self.path == "/v5/order/create"
                        assert request["category"] == "spot" and request["isLeverage"] == 0
                        assert request["orderType"] == "Market" and request["timeInForce"] == "IOC"
                        assert request["marketUnit"] == "baseCoin"
                        link = request["orderLinkId"]
                        assert len(link) <= 36 and link.replace("-", "").replace("_", "").isalnum()
                        outer.posts.append(request)
                        if link in outer.orders:
                            return self.respond({}, code=10014)
                        if outer.scenario == "reject" and link.endswith("-l20"):
                            return self.respond({}, code=170140)
                        quantity = Decimal(request["qty"])
                        assert quantity > 0
                        symbol = request["symbol"]
                        base, quote, bid, ask = {"BTCUSDT": ("BTC", "USDT", "99", "100"), "ETHBTC": ("ETH", "BTC", ".049", ".05"), "ETHUSDT": ("ETH", "USDT", "6", "6.1")}[symbol]
                        price = Decimal(ask if request["side"] == "Buy" else bid)
                        if outer.scenario == "loss" and symbol == "ETHUSDT":
                            price = Decimal("4.9")
                        if outer.scenario == "partial" and link.endswith("-l20"):
                            quantity = (quantity/2).quantize(Decimal(".00001"), rounding=ROUND_DOWN)
                        value = quantity*price
                        # Both base/quote and a supported third route currency
                        # are exercised without inventing exchange confirmation.
                        fee_asset, fee = "USDT", Decimal(".001")
                        if outer.scenario == "foreign_fee":
                            fee_asset = "MNT"
                        from_asset, to_asset = (quote, base) if request["side"] == "Buy" else (base, quote)
                        outer.wallet[from_asset] -= value if request["side"] == "Buy" else quantity
                        outer.wallet[to_asset] += quantity if request["side"] == "Buy" else value
                        outer.wallet[fee_asset] -= fee
                        assert all(v >= 0 for v in outer.wallet.values()), outer.wallet
                        order_id = "mock-"+link
                        remaining = Decimal(request["qty"])-quantity
                        order = {"symbol": symbol, "orderId": order_id, "orderLinkId": link, "qty": request["qty"], "cumExecQty": format(quantity,"f"), "leavesQty": format(remaining,"f"), "avgPrice": str(price), "orderStatus": "Filled" if remaining == 0 else "PartiallyFilledCanceled", "createdTime": str(int(time.time()*1000)), "updatedTime": str(int(time.time()*1000))}
                        outer.orders[link] = order
                        outer.fills[order_id] = [{"execId": "fill-"+link, "orderId": order_id, "execPrice": str(price), "execQty": format(quantity,"f"), "execValue": format(value,"f"), "execFee": str(fee), "feeCurrency": fee_asset, "isMaker": False, "execTime": str(int(time.time()*1000))}]
                        if outer.scenario in {"lost_ack", "unknown"} and link.endswith("-l10"):
                            return self.respond({}, http=500)
                        return self.respond({"orderId": order_id, "orderLinkId": link})
                except Exception as error:
                    outer.errors.append(str(error))
                    self.respond({}, http=500)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server.server_port}"
