from __future__ import annotations

from decimal import Decimal

from analytics.performance_analytics import (
    ActualCycle,
    _decimal,
    _distribution,
    _quantile,
)


def test_distribution_reports_quantiles_and_histogram() -> None:
    distribution = _distribution(
        [1.0, 2.0, 3.0, 4.0],
        bins=2,
        total_count=4,
        unit="bps",
    )

    assert distribution["count"] == 4
    assert distribution["sample_count"] == 4
    assert distribution["median"] == 2.5
    assert distribution["p25"] == 1.75
    assert sum(item["count"] for item in distribution["bins"]) == 4


def test_distribution_handles_constant_values() -> None:
    distribution = _distribution(
        [5.0, 5.0, 5.0],
        bins=10,
        total_count=3,
        unit="ms",
    )

    assert distribution["mean"] == 5.0
    assert distribution["bins"] == [
        {"lower": 5.0, "upper": 5.0, "count": 3}
    ]


def test_quantile_single_value_is_stable() -> None:
    assert _quantile([9.0], 0.95) == 9.0


def test_decimal_parser_rejects_invalid_values() -> None:
    assert _decimal("1.25") == Decimal("1.25")
    assert _decimal("not-a-number") is None
    assert _decimal(None) is None


def test_actual_cycle_is_immutable_value_object() -> None:
    cycle = ActualCycle(
        trade_id="trade-1",
        occurred_at=__import__("datetime").datetime.now(
            __import__("datetime").UTC
        ),
        base_asset="USDT",
        starting_amount=Decimal("10"),
        pnl=Decimal("0.10"),
        execution_time_ms=50,
        estimated_turnover=Decimal("30"),
        source="engine",
        successful=True,
    )
    assert cycle.pnl == Decimal("0.10")



def test_distribution_count_can_exceed_sample_count() -> None:
    distribution = _distribution(
        [1.0, 2.0],
        bins=2,
        total_count=10,
        unit="bps",
    )

    assert distribution["count"] == 10
    assert distribution["sample_count"] == 2
