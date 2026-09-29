"""Reference profitability model shared conceptually with the Rust scanner."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

BPS_DENOMINATOR = Decimal("10000")
OUTPUT_QUANTUM = Decimal("0.00000001")


def _decimal(value: Decimal | str | int | float) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _canonical(value: Decimal) -> str:
    return format(value.quantize(OUTPUT_QUANTUM, rounding=ROUND_HALF_UP), "f")


@dataclass(frozen=True)
class ProfitabilityConfig:
    version: int
    fee_profile: str
    fee_bps_per_leg: tuple[Decimal, ...]
    expected_slippage_bps: Decimal
    rounding_loss_bps: Decimal
    latency_buffer_bps: Decimal
    safety_margin_bps: Decimal

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ProfitabilityConfig":
        config = cls(
            version=int(payload.get("version", 1)),
            fee_profile=str(payload.get("fee_profile", "custom")),
            fee_bps_per_leg=tuple(
                _decimal(value) for value in payload["fee_bps_per_leg"]
            ),
            expected_slippage_bps=_decimal(payload["expected_slippage_bps"]),
            rounding_loss_bps=_decimal(payload["rounding_loss_bps"]),
            latency_buffer_bps=_decimal(payload["latency_buffer_bps"]),
            safety_margin_bps=_decimal(payload["safety_margin_bps"]),
        )
        config.validate()
        return config

    @classmethod
    def from_path(cls, path: Path) -> "ProfitabilityConfig":
        return cls.from_dict(json.loads(path.read_text(encoding="utf-8")))

    def validate(self) -> None:
        if self.version != 1:
            raise ValueError(f"unsupported profitability config version {self.version}")
        if not self.fee_profile.strip():
            raise ValueError("fee_profile must not be empty")
        if len(self.fee_bps_per_leg) != 3:
            raise ValueError("fee_bps_per_leg must contain exactly three triangle-leg fees")

        all_bps = (
            *self.fee_bps_per_leg,
            self.expected_slippage_bps,
            self.rounding_loss_bps,
            self.latency_buffer_bps,
            self.safety_margin_bps,
        )
        for value in all_bps:
            if value < 0:
                raise ValueError("profitability cost assumptions must be non-negative")
            if value >= BPS_DENOMINATOR:
                raise ValueError("a single profitability cost assumption must be below 10000 bps")


@dataclass(frozen=True)
class ProfitabilityResult:
    start_amount: Decimal
    gross_final_amount: Decimal
    gross_profit: Decimal
    gross_return_bps: Decimal
    gross_return_pct: Decimal
    fee_multiplier: Decimal
    nominal_fee_bps: Decimal
    fee_amount: Decimal
    fee_bps_on_start: Decimal
    expected_slippage_bps: Decimal
    expected_slippage_amount: Decimal
    rounding_loss_bps: Decimal
    rounding_loss_amount: Decimal
    latency_buffer_bps: Decimal
    latency_buffer_amount: Decimal
    safety_margin_bps: Decimal
    safety_margin_amount: Decimal
    total_cost_amount: Decimal
    total_cost_bps: Decimal
    expected_net_profit: Decimal
    expected_net_return_bps: Decimal
    expected_net_return_pct: Decimal
    expected_final_amount: Decimal
    net_profitable: bool

    def canonical(self) -> dict[str, str | bool]:
        return {
            "start_amount": _canonical(self.start_amount),
            "gross_final_amount": _canonical(self.gross_final_amount),
            "gross_profit": _canonical(self.gross_profit),
            "gross_return_bps": _canonical(self.gross_return_bps),
            "gross_return_pct": _canonical(self.gross_return_pct),
            "fee_multiplier": _canonical(self.fee_multiplier),
            "nominal_fee_bps": _canonical(self.nominal_fee_bps),
            "fee_amount": _canonical(self.fee_amount),
            "fee_bps_on_start": _canonical(self.fee_bps_on_start),
            "expected_slippage_bps": _canonical(self.expected_slippage_bps),
            "expected_slippage_amount": _canonical(self.expected_slippage_amount),
            "rounding_loss_bps": _canonical(self.rounding_loss_bps),
            "rounding_loss_amount": _canonical(self.rounding_loss_amount),
            "latency_buffer_bps": _canonical(self.latency_buffer_bps),
            "latency_buffer_amount": _canonical(self.latency_buffer_amount),
            "safety_margin_bps": _canonical(self.safety_margin_bps),
            "safety_margin_amount": _canonical(self.safety_margin_amount),
            "total_cost_amount": _canonical(self.total_cost_amount),
            "total_cost_bps": _canonical(self.total_cost_bps),
            "expected_net_profit": _canonical(self.expected_net_profit),
            "expected_net_return_bps": _canonical(self.expected_net_return_bps),
            "expected_net_return_pct": _canonical(self.expected_net_return_pct),
            "expected_final_amount": _canonical(self.expected_final_amount),
            "net_profitable": self.net_profitable,
        }


def evaluate_profitability(
    start_amount: Decimal | str | int | float,
    gross_final_amount: Decimal | str | int | float,
    config: ProfitabilityConfig,
) -> ProfitabilityResult:
    config.validate()
    start = _decimal(start_amount)
    gross_final = _decimal(gross_final_amount)

    if start <= 0:
        raise ValueError("start_amount must be greater than zero")
    if gross_final < 0:
        raise ValueError("gross_final_amount must not be negative")

    gross_profit = gross_final - start
    gross_return_bps = (gross_profit / start) * BPS_DENOMINATOR
    gross_return_pct = gross_return_bps / Decimal("100")

    fee_multiplier = Decimal("1")
    for fee_bps in config.fee_bps_per_leg:
        fee_multiplier *= Decimal("1") - (fee_bps / BPS_DENOMINATOR)

    nominal_fee_bps = sum(config.fee_bps_per_leg, Decimal("0"))
    fee_amount = gross_final * (Decimal("1") - fee_multiplier)
    fee_bps_on_start = (fee_amount / start) * BPS_DENOMINATOR

    expected_slippage_amount = (
        start * config.expected_slippage_bps / BPS_DENOMINATOR
    )
    rounding_loss_amount = start * config.rounding_loss_bps / BPS_DENOMINATOR
    latency_buffer_amount = start * config.latency_buffer_bps / BPS_DENOMINATOR
    safety_margin_amount = start * config.safety_margin_bps / BPS_DENOMINATOR

    total_cost_amount = (
        fee_amount
        + expected_slippage_amount
        + rounding_loss_amount
        + latency_buffer_amount
        + safety_margin_amount
    )
    total_cost_bps = (total_cost_amount / start) * BPS_DENOMINATOR
    expected_net_profit = gross_profit - total_cost_amount
    expected_net_return_bps = (expected_net_profit / start) * BPS_DENOMINATOR
    expected_net_return_pct = expected_net_return_bps / Decimal("100")
    expected_final_amount = start + expected_net_profit

    return ProfitabilityResult(
        start_amount=start,
        gross_final_amount=gross_final,
        gross_profit=gross_profit,
        gross_return_bps=gross_return_bps,
        gross_return_pct=gross_return_pct,
        fee_multiplier=fee_multiplier,
        nominal_fee_bps=nominal_fee_bps,
        fee_amount=fee_amount,
        fee_bps_on_start=fee_bps_on_start,
        expected_slippage_bps=config.expected_slippage_bps,
        expected_slippage_amount=expected_slippage_amount,
        rounding_loss_bps=config.rounding_loss_bps,
        rounding_loss_amount=rounding_loss_amount,
        latency_buffer_bps=config.latency_buffer_bps,
        latency_buffer_amount=latency_buffer_amount,
        safety_margin_bps=config.safety_margin_bps,
        safety_margin_amount=safety_margin_amount,
        total_cost_amount=total_cost_amount,
        total_cost_bps=total_cost_bps,
        expected_net_profit=expected_net_profit,
        expected_net_return_bps=expected_net_return_bps,
        expected_net_return_pct=expected_net_return_pct,
        expected_final_amount=expected_final_amount,
        net_profitable=expected_net_profit > 0,
    )


def evaluate_fixture_case(case: dict[str, Any]) -> dict[str, str | bool]:
    inputs = case["input"]
    config = ProfitabilityConfig.from_dict(inputs["config"])
    return evaluate_profitability(
        inputs["start_amount"],
        inputs["gross_final_amount"],
        config,
    ).canonical()
