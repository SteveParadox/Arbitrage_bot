from decimal import Decimal

from simulator.models import PaperSimulationResult
from simulator.paper_trade import _summary


def row(latency: int, completed: bool, profit: float | None) -> PaperSimulationResult:
    return PaperSimulationResult(
        run_id="run",
        opportunity_id=1,
        route_id="USDT>BTC>ETH>USDT",
        triangle_id="BTC-ETH-USDT",
        detected_at_ms=1000,
        latency_ms=latency,
        total_execution_time_ms=latency * 3,
        starting_capital=Decimal("450"),
        expected_profit=Decimal("2"),
        expected_net_edge_bps=Decimal("44"),
        detected_gross_final_amount=Decimal("452"),
        simulated_final_amount=Decimal(str(450 + profit)) if profit is not None else None,
        simulated_profit=Decimal(str(profit)) if profit is not None else None,
        simulated_net_edge_bps=Decimal("10") if profit is not None else None,
        execution_drift_amount=Decimal("1") if profit is not None else None,
        execution_drift_bps=Decimal("22") if profit is not None else None,
        expectation_error=Decimal("-1") if profit is not None else None,
        completed=completed,
        fill_ratio=Decimal("1") if completed else Decimal("0.5"),
        failure_reason=None if completed else "insufficient_liquidity",
        failure_leg=None if completed else 2,
        opportunity_lifetime_ms=200,
        remaining_lifetime_ms=100,
        outlived_opportunity=latency * 3 > 100,
        max_book_age_ms=10,
        legs=[],
    )


def test_summary_reports_fill_failure_and_profit_by_latency() -> None:
    summary = _summary(
        [
            row(25, True, 1.0),
            row(25, False, None),
            row(100, True, -0.5),
        ]
    )

    fast = summary["latency_scenarios"]["25"]
    assert fast["attempts"] == 2
    assert fast["fills"] == 1
    assert fast["fill_rate_pct"] == 50.0
    assert fast["failure_rate_pct"] == 50.0
    assert fast["failure_reasons"] == {"insufficient_liquidity": 1}
    assert fast["avg_simulated_profit"] == 1.0
    assert fast["avg_remaining_lifetime_ms"] == 100.0
    assert fast["simulated_profitable"] == 1
    assert fast["simulated_profitable_rate_pct"] == 50.0
    assert fast["outlived_opportunity_count"] == 0

    slow = summary["latency_scenarios"]["100"]
    assert slow["outlived_opportunity_count"] == 1
    assert slow["outlived_opportunity_rate_pct"] == 100.0
