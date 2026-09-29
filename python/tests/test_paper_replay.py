from decimal import Decimal

from simulator.book_archive import ReplayEvent, SymbolReplay
from simulator.replay import simulate_route


def replay(symbol: str, prices: list[tuple[int, float, float]]) -> SymbolReplay:
    events = []
    for index, (timestamp, bid, ask) in enumerate(prices):
        events.append(
            ReplayEvent(
                symbol=symbol,
                timestamp_ms=timestamp,
                update_id=index + 1,
                sequence=index + 1,
                is_snapshot=index == 0,
                bids=((bid, 100.0),),
                asks=((ask, 100.0),),
            )
        )
    return SymbolReplay(symbol, events, checkpoint_interval=2)


def scan() -> dict:
    return {
        "scan_timestamp": 1_000,
        "start_amount": 450.0,
        "final_amount": 472.5,
        "legs": [
            {
                "symbol": "BTCUSDT",
                "side": "BUY",
                "execution": {"average_execution_price": 100.0},
            },
            {
                "symbol": "ETHBTC",
                "side": "BUY",
                "execution": {"average_execution_price": 0.05},
            },
            {
                "symbol": "ETHUSDT",
                "side": "SELL",
                "execution": {"average_execution_price": 21.0},
            },
        ],
    }


def test_25ms_replay_uses_delayed_books_for_each_leg() -> None:
    replays = {
        "BTCUSDT": replay("BTCUSDT", [(1_000, 99.0, 100.0), (1_025, 100.0, 101.0)]),
        "ETHBTC": replay("ETHBTC", [(1_000, 0.049, 0.05), (1_050, 0.049, 0.05)]),
        "ETHUSDT": replay("ETHUSDT", [(1_000, 21.0, 22.0), (1_075, 20.5, 22.0)]),
    }

    result = simulate_route(
        scan(),
        replays,
        latency_ms=25,
        fee_bps_per_leg=(Decimal("10"), Decimal("10"), Decimal("10")),
        max_book_age_ms=100,
        expected_profit=Decimal("5"),
    )

    assert result.completed is True
    assert result.simulated_profit is not None
    assert result.simulated_profit < Decimal("22.5")
    assert [leg["execution_timestamp_ms"] for leg in result.legs] == [1025, 1050, 1075]
    assert result.execution_drift_bps is not None


def test_replay_fails_when_delayed_book_is_stale() -> None:
    replays = {
        "BTCUSDT": replay("BTCUSDT", [(1_000, 99.0, 100.0)]),
        "ETHBTC": replay("ETHBTC", [(1_000, 0.049, 0.05)]),
        "ETHUSDT": replay("ETHUSDT", [(1_000, 21.0, 22.0)]),
    }

    result = simulate_route(
        scan(),
        replays,
        latency_ms=500,
        fee_bps_per_leg=(Decimal("10"), Decimal("10"), Decimal("10")),
        max_book_age_ms=200,
    )

    assert result.completed is False
    assert result.failure_reason == "stale_book_state"
    assert result.failure_leg == 1


def test_replay_detects_insufficient_liquidity() -> None:
    events = [
        ReplayEvent(
            symbol="BTCUSDT",
            timestamp_ms=1_025,
            update_id=1,
            sequence=1,
            is_snapshot=True,
            bids=((99.0, 1.0),),
            asks=((100.0, 1.0),),
        )
    ]
    replays = {
        "BTCUSDT": SymbolReplay("BTCUSDT", events),
        "ETHBTC": replay("ETHBTC", [(1_000, 0.049, 0.05)]),
        "ETHUSDT": replay("ETHUSDT", [(1_000, 21.0, 22.0)]),
    }

    result = simulate_route(
        scan(),
        replays,
        latency_ms=25,
        fee_bps_per_leg=(Decimal("10"), Decimal("10"), Decimal("10")),
        max_book_age_ms=100,
    )

    assert result.completed is False
    assert result.failure_reason == "insufficient_liquidity"
    assert result.failure_leg == 1
    assert result.fill_ratio < Decimal("1")


def test_symbol_replay_applies_snapshot_and_delta_at_requested_time() -> None:
    events = [
        ReplayEvent(
            symbol="BTCUSDT",
            timestamp_ms=1_000,
            update_id=10,
            sequence=10,
            is_snapshot=True,
            bids=((100.0, 2.0), (99.0, 1.0)),
            asks=((101.0, 3.0),),
        ),
        ReplayEvent(
            symbol="BTCUSDT",
            timestamp_ms=1_050,
            update_id=11,
            sequence=11,
            is_snapshot=False,
            bids=((100.0, 0.0), (100.5, 1.0)),
            asks=((101.0, 2.0), (102.0, 1.0)),
        ),
    ]
    timeline = SymbolReplay("BTCUSDT", events, checkpoint_interval=100)

    before = timeline.state_at(1_025)
    after = timeline.state_at(1_050)

    assert before is not None
    assert before.best_bid == 100.0
    assert before.best_ask == 101.0

    assert after is not None
    assert after.best_bid == 100.5
    assert after.best_ask == 101.0
    assert after.bids[0] == (100.5, 1.0)


def test_missing_symbol_history_is_a_simulation_failure() -> None:
    replays = {
        "BTCUSDT": replay("BTCUSDT", [(1_000, 99.0, 100.0)]),
        "ETHUSDT": replay("ETHUSDT", [(1_000, 21.0, 22.0)]),
    }

    result = simulate_route(
        scan(),
        replays,
        latency_ms=25,
        fee_bps_per_leg=(Decimal("10"), Decimal("10"), Decimal("10")),
        max_book_age_ms=100,
    )

    assert result.completed is False
    assert result.failure_reason == "missing_book_history"
    assert result.failure_leg == 2
