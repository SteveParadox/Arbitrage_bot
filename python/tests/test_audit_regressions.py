from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from analytics.opportunity_store import classify_scan
from simulator.book_archive import ReplayEvent, SymbolReplay
from simulator.book_ingest import _book_values
from simulator.paper_trade import _result_row, _summary
from simulator.replay import simulate_route
from strategy.profitability import ProfitabilityConfig, evaluate_profitability
from test_paper_replay import replay, scan
from test_paper_summary import row


def event(ts=1000, seq=10, snapshot=True, bid=99.0):
    return ReplayEvent("BTCUSDT", ts, seq, seq, snapshot, ((bid, 100.0),), ((100., 100.),))


def test_checkpoint_never_selects_future_state():
    events = [event(1000 + i, 10 + i, i == 0, 90.0 + i / 100) for i in range(250)]
    timeline = SymbolReplay("BTCUSDT", events, checkpoint_interval=100)
    for i in (0, 99, 100, 101, 199, 200, 249):
        state = timeline.state_at(1000 + i)
        assert state.timestamp_ms == 1000 + i
        assert state.best_bid == 90.0 + i / 100
    assert timeline.state_at(999) is None


def test_sequence_regression_invalidates_until_snapshot():
    timeline = SymbolReplay("BTCUSDT", [
        event(), event(1010, 9, False), event(1020, 11, False), event(1030, 1, True),
    ])
    assert timeline.state_at(1005) is not None
    assert timeline.state_at(1015) is None
    assert timeline.state_at(1025) is None
    assert timeline.state_at(1030).update_id == 1


def test_same_timestamp_reset_preserves_arrival_order():
    timeline = SymbolReplay("BTCUSDT", [
        event(), event(1010, 11, False, 99.5), event(1010, 1, False, 80),
    ])
    assert timeline.state_at(1010).bids == ((80, 100.0),)


def test_delta_without_snapshot_is_unusable():
    assert SymbolReplay("BTCUSDT", [event(snapshot=False)]).state_at(1000) is None


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -1., 0.])
def test_invalid_archive_price_cannot_fill(bad):
    invalid = replace(event(), asks=((bad, 100.),))
    assert SymbolReplay("BTCUSDT", [invalid]).state_at(1000) is None


@pytest.mark.parametrize("latency", [25, 50, 100, 200, 500])
def test_all_latency_scenarios_use_asof_prices_and_fee_reduced_quantities(latency):
    histories = {
        "BTCUSDT": replay("BTCUSDT", [(1000, 99., 100.), (1000 + latency, 100., 101.)]),
        "ETHBTC": replay("ETHBTC", [(1000, .049, .05)]),
        "ETHUSDT": replay("ETHUSDT", [
            (1000, 5.25, 5.3), (1000 + latency * 3 + 1, 999., 1000.),
        ]),
    }
    result = simulate_route(scan(), histories, latency_ms=latency,
                            fee_bps_per_leg=(Decimal("10"),) * 3, max_book_age_ms=2000)
    expected = Decimal("450") / 101 / Decimal(".05") * Decimal("5.25")
    expected *= Decimal(".999") ** 3
    assert result.completed
    assert abs(result.final_amount - expected) < Decimal("1e-20")
    assert [leg["execution_timestamp_ms"] for leg in result.legs] == [
        1000 + latency * i for i in (1, 2, 3)
    ]
    for i in (1, 2):
        assert result.legs[i]["input_amount"] == result.legs[i-1]["net_output_amount"]


def test_drift_is_compounded_route_cash_not_sum_of_leg_percentages():
    histories = {
        "BTCUSDT": replay("BTCUSDT", [(1000, 109., 110.)]),
        "ETHBTC": replay("ETHBTC", [(1000, .049, .05)]),
        "ETHUSDT": replay("ETHUSDT", [(1000, 5.775, 6.)]),
    }
    result = simulate_route(scan(), histories, latency_ms=25,
                            fee_bps_per_leg=(Decimal("0"),) * 3, max_book_age_ms=1000)
    # +10% buy price and +10% sell price cancel exactly in multiplicative conversions.
    assert abs(result.execution_drift_amount) < Decimal("1e-20")


@pytest.mark.parametrize("payload", [None, [], "x", {"legs": [None]}])
def test_invalid_scan_envelopes_are_rejected_cleanly(payload):
    with pytest.raises(ValueError):
        classify_scan(payload, Decimal("0"))


def test_archive_ingest_rejects_string_boolean_and_invalid_prices():
    valid = {"type": "order_book", "symbol": "BTCUSDT", "timestamp": 1,
             "update_id": 2, "sequence": 2, "is_snapshot": True,
             "bids": [[99, 1]], "asks": [[100, 1]]}
    assert _book_values(valid)["is_snapshot"] is True
    for bad in ({**valid, "is_snapshot": "false"}, {**valid, "asks": [[0, 1]]}, None, []):
        with pytest.raises(ValueError):
            _book_values(bad)


def test_open_window_is_censored_and_closed_window_uses_remaining_lifetime():
    end = datetime(2026, 9, 30, tzinfo=UTC)
    observation = SimpleNamespace(id=1, route_id="r", triangle_id="t",
        scan_timestamp_ms=int((end - timedelta(milliseconds=200)).timestamp() * 1000),
        starting_capital=450, net_profit=1, net_edge_bps=10, gross_final_amount=452)
    outcome = simulate_route(scan(), {}, latency_ms=100,
        fee_bps_per_leg=(Decimal("10"),) * 3, max_book_age_ms=1000)
    window = SimpleNamespace(duration_ms=800, ended_at=None, last_seen_at=end)
    result = _result_row("run", observation, window, 100, outcome)
    assert result.remaining_lifetime_ms is None
    assert not result.outlived_opportunity
    window.ended_at = end
    result = _result_row("run", observation, window, 100, outcome)
    assert result.remaining_lifetime_ms == 200
    assert result.outlived_opportunity


def test_summary_exposes_different_denominators():
    filled, failed = row(25, True, 1), row(25, False, None)
    failed.expected_profit = Decimal("8")
    metrics = _summary([filled, failed])["latency_scenarios"]["25"]
    assert metrics["avg_expected_profit_all_attempts"] == 5
    assert metrics["avg_expected_profit_among_fills"] == 2
    assert metrics["simulated_profitable_rate_pct"] == 50
    assert metrics["simulated_profitable_among_fills_pct"] == 100
    assert metrics["failed_attempts_unvalued"] == 1


@pytest.mark.parametrize("bad", ["NaN", "Infinity", "-Infinity"])
def test_nonfinite_profitability_inputs_rejected(bad):
    config = ProfitabilityConfig.from_dict({"fee_profile": "test", "fee_bps_per_leg": [0]*3,
        "expected_slippage_bps": 0, "rounding_loss_bps": 0,
        "latency_buffer_bps": 0, "safety_margin_bps": 0})
    with pytest.raises(ValueError):
        evaluate_profitability(bad, 450, config)


def test_rounding_allowance_is_not_realized_cash():
    with pytest.raises(ValueError, match="prediction rounding"):
        simulate_route(scan(), {}, latency_ms=25, fee_bps_per_leg=(Decimal("0"),)*3,
                       max_book_age_ms=1000, rounding_loss_bps=Decimal("1"))


def test_crossed_book_is_rejected_and_favorable_movement_is_profitable():
    histories = {
        "BTCUSDT": replay("BTCUSDT", [(1000, 99., 100.)]),
        "ETHBTC": replay("ETHBTC", [(1000, .049, .05)]),
        "ETHUSDT": replay("ETHUSDT", [(1000, 5.5, 6.)]),
    }
    result = simulate_route(scan(), histories, latency_ms=25,
        fee_bps_per_leg=(Decimal("0"),)*3, max_book_age_ms=1000)
    assert result.simulated_profit == Decimal("45")
    assert result.execution_drift_amount == Decimal("-22.5")
    histories["BTCUSDT"] = replay("BTCUSDT", [(1000, 101., 100.)])
    result = simulate_route(scan(), histories, latency_ms=25,
        fee_bps_per_leg=(Decimal("0"),)*3, max_book_age_ms=1000)
    assert not result.completed and result.failure_reason == "crossed_book_state"


@pytest.mark.parametrize("capital", [-1, 0, "NaN", "Infinity"])
def test_impossible_capital_cannot_be_classified_executable(capital):
    with pytest.raises(ValueError):
        classify_scan({"start_amount": capital}, Decimal("0"))
