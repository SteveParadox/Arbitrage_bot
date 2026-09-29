from decimal import Decimal

from analytics.opportunity_store import classify_scan


def complete_scan(net_bps: float = 18.0, net_profitable: bool = True) -> dict:
    return {
        "status": "complete",
        "start_amount": 450.0,
        "final_amount": 452.8,
        "fees_included": True,
        "net_profitable": net_profitable,
        "expected_net_return_bps": net_bps,
        "legs": [
            {
                "side": "BUY",
                "complete": True,
                "input_amount": 450.0,
                "execution": {
                    "requested_quote_quantity": 450.0,
                    "filled_quote_quantity": 450.0,
                },
            },
            {
                "side": "BUY",
                "complete": True,
                "input_amount": 0.0065,
                "execution": {
                    "requested_quote_quantity": 0.0065,
                    "filled_quote_quantity": 0.0065,
                },
            },
            {
                "side": "SELL",
                "complete": True,
                "input_amount": 0.119,
                "execution": {
                    "requested_base_quantity": 0.119,
                    "filled_base_quantity": 0.119,
                },
            },
        ],
    }


def test_complete_net_profitable_scan_is_accepted() -> None:
    decision = classify_scan(complete_scan(), Decimal("0"))

    assert decision.executable is True
    assert decision.accepted is True
    assert decision.rejection_reason is None
    assert decision.available_liquidity == Decimal("450.0")
    assert decision.available_liquidity_ratio == Decimal("1")


def test_complete_but_net_negative_is_executable_and_rejected() -> None:
    decision = classify_scan(complete_scan(net_bps=-2.0, net_profitable=False), Decimal("0"))

    assert decision.executable is True
    assert decision.accepted is False
    assert decision.rejection_reason == "net_not_profitable"


def test_minimum_net_edge_rejects_thin_profit() -> None:
    decision = classify_scan(complete_scan(net_bps=3.0), Decimal("5"))

    assert decision.executable is True
    assert decision.accepted is False
    assert decision.rejection_reason == "below_min_net_edge"


def test_partial_liquidity_is_recorded_not_accepted() -> None:
    scan = complete_scan()
    scan["status"] = "insufficient_liquidity"
    scan["legs"] = [
        {
            "side": "BUY",
            "complete": False,
            "execution": {
                "requested_quote_quantity": 450.0,
                "filled_quote_quantity": 90.0,
            },
        }
    ]

    decision = classify_scan(scan, Decimal("0"))

    assert decision.executable is False
    assert decision.accepted is False
    assert decision.rejection_reason == "insufficient_liquidity"
    assert decision.available_liquidity_ratio == Decimal("0.2")
    assert decision.available_liquidity == Decimal("90.00")
