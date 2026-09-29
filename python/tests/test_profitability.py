import json
from pathlib import Path

import pytest

from strategy.profitability import (
    ProfitabilityConfig,
    evaluate_fixture_case,
    evaluate_profitability,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = REPO_ROOT / "shared" / "tests" / "profitability_cases.json"


def test_shared_profitability_cases_match_exact_canonical_results() -> None:
    cases = json.loads(FIXTURES.read_text(encoding="utf-8"))
    for case in cases:
        assert evaluate_fixture_case(case) == case["expected"], case["name"]


def test_reference_example_is_about_eighteen_basis_points_net() -> None:
    config = ProfitabilityConfig.from_dict(
        {
            "version": 1,
            "fee_profile": "bybit_spot_vip0_reference",
            "fee_bps_per_leg": ["10", "10", "10"],
            "expected_slippage_bps": "5",
            "rounding_loss_bps": "0",
            "latency_buffer_bps": "3",
            "safety_margin_bps": "5",
        }
    )
    result = evaluate_profitability("450", "452.745", config)

    assert float(result.gross_return_pct) == pytest.approx(0.61)
    assert float(result.expected_net_return_pct) == pytest.approx(0.17847172939)
    assert result.net_profitable is True


def test_invalid_cost_assumption_is_rejected() -> None:
    with pytest.raises(ValueError):
        ProfitabilityConfig.from_dict(
            {
                "version": 1,
                "fee_profile": "bad",
                "fee_bps_per_leg": ["10", "-1", "10"],
                "expected_slippage_bps": "5",
                "rounding_loss_bps": "0",
                "latency_buffer_bps": "3",
                "safety_margin_bps": "5",
            }
        )
