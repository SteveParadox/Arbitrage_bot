from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from simulator.book_archive import BookState, SymbolReplay

BPS = Decimal("10000")


def _d(value: Any) -> Decimal:
    return Decimal(str(value))


@dataclass(frozen=True)
class ExecutionFill:
    complete: bool
    input_amount: Decimal
    gross_output_amount: Decimal
    net_output_amount: Decimal
    average_price: Decimal | None
    best_price: Decimal | None
    worst_price: Decimal | None
    fill_ratio: Decimal
    fee_bps: Decimal
    fee_quantity: Decimal
    adverse_slippage_bps: Decimal | None
    book_timestamp_ms: int
    book_age_ms: int
    update_id: int
    sequence: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "complete": self.complete,
            "input_amount": float(self.input_amount),
            "gross_output_amount": float(self.gross_output_amount),
            "net_output_amount": float(self.net_output_amount),
            "average_price": _float_or_none(self.average_price),
            "best_price": _float_or_none(self.best_price),
            "worst_price": _float_or_none(self.worst_price),
            "fill_ratio": float(self.fill_ratio),
            "fee_bps": float(self.fee_bps),
            "fee_quantity": float(self.fee_quantity),
            "adverse_slippage_bps": _float_or_none(self.adverse_slippage_bps),
            "book_timestamp_ms": self.book_timestamp_ms,
            "book_age_ms": self.book_age_ms,
            "update_id": self.update_id,
            "sequence": self.sequence,
        }


@dataclass(frozen=True)
class SimulationOutcome:
    completed: bool
    final_amount: Decimal | None
    simulated_profit: Decimal | None
    simulated_net_edge_bps: Decimal | None
    execution_drift_amount: Decimal | None
    execution_drift_bps: Decimal | None
    expectation_error: Decimal | None
    fill_ratio: Decimal
    failure_reason: str | None
    failure_leg: int | None
    max_book_age_ms: int | None
    legs: tuple[dict[str, Any], ...]


def execute_buy(
    state: BookState,
    quote_amount: Decimal,
    *,
    fee_bps: Decimal,
    detected_average_price: Decimal | None,
    execution_timestamp_ms: int,
) -> ExecutionFill:
    remaining = quote_amount
    filled_quote = Decimal("0")
    filled_base = Decimal("0")
    best_price = _d(state.asks[0][0]) if state.asks else None
    worst_price: Decimal | None = None

    for raw_price, raw_quantity in state.asks:
        if remaining <= 0:
            break
        price = _d(raw_price)
        available_base = _d(raw_quantity)
        quote_capacity = price * available_base
        quote_taken = min(remaining, quote_capacity)
        if quote_taken <= 0:
            continue
        filled_quote += quote_taken
        filled_base += quote_taken / price
        remaining -= quote_taken
        worst_price = price

    average = filled_quote / filled_base if filled_base > 0 else None
    fee_quantity = filled_base * fee_bps / BPS
    net_output = filled_base - fee_quantity
    fill_ratio = filled_quote / quote_amount if quote_amount > 0 else Decimal("0")
    adverse = None
    if average is not None and detected_average_price and detected_average_price > 0:
        adverse = ((average / detected_average_price) - Decimal("1")) * BPS

    return ExecutionFill(
        complete=remaining <= quote_amount * Decimal("1e-12"),
        input_amount=quote_amount,
        gross_output_amount=filled_base,
        net_output_amount=net_output,
        average_price=average,
        best_price=best_price,
        worst_price=worst_price,
        fill_ratio=min(Decimal("1"), fill_ratio),
        fee_bps=fee_bps,
        fee_quantity=fee_quantity,
        adverse_slippage_bps=adverse,
        book_timestamp_ms=state.timestamp_ms,
        book_age_ms=max(0, execution_timestamp_ms - state.timestamp_ms),
        update_id=state.update_id,
        sequence=state.sequence,
    )


def execute_sell(
    state: BookState,
    base_amount: Decimal,
    *,
    fee_bps: Decimal,
    detected_average_price: Decimal | None,
    execution_timestamp_ms: int,
) -> ExecutionFill:
    remaining = base_amount
    filled_base = Decimal("0")
    filled_quote = Decimal("0")
    best_price = _d(state.bids[0][0]) if state.bids else None
    worst_price: Decimal | None = None

    for raw_price, raw_quantity in state.bids:
        if remaining <= 0:
            break
        price = _d(raw_price)
        available_base = _d(raw_quantity)
        base_taken = min(remaining, available_base)
        if base_taken <= 0:
            continue
        filled_base += base_taken
        filled_quote += base_taken * price
        remaining -= base_taken
        worst_price = price

    average = filled_quote / filled_base if filled_base > 0 else None
    fee_quantity = filled_quote * fee_bps / BPS
    net_output = filled_quote - fee_quantity
    fill_ratio = filled_base / base_amount if base_amount > 0 else Decimal("0")
    adverse = None
    if average is not None and detected_average_price and detected_average_price > 0:
        adverse = (Decimal("1") - (average / detected_average_price)) * BPS

    return ExecutionFill(
        complete=remaining <= base_amount * Decimal("1e-12"),
        input_amount=base_amount,
        gross_output_amount=filled_quote,
        net_output_amount=net_output,
        average_price=average,
        best_price=best_price,
        worst_price=worst_price,
        fill_ratio=min(Decimal("1"), fill_ratio),
        fee_bps=fee_bps,
        fee_quantity=fee_quantity,
        adverse_slippage_bps=adverse,
        book_timestamp_ms=state.timestamp_ms,
        book_age_ms=max(0, execution_timestamp_ms - state.timestamp_ms),
        update_id=state.update_id,
        sequence=state.sequence,
    )


def simulate_route(
    raw_scan: dict[str, Any],
    replays: dict[str, SymbolReplay],
    *,
    latency_ms: int,
    fee_bps_per_leg: tuple[Decimal, Decimal, Decimal],
    max_book_age_ms: int,
    rounding_loss_bps: Decimal = Decimal("0"),
    expected_profit: Decimal | None = None,
) -> SimulationOutcome:
    if latency_ms <= 0:
        raise ValueError("latency_ms must be greater than zero")
    if max_book_age_ms <= 0:
        raise ValueError("max_book_age_ms must be greater than zero")

    start_amount = _d(raw_scan["start_amount"])
    detection_timestamp_ms = int(raw_scan["scan_timestamp"])
    route_legs = raw_scan.get("legs") or []

    if len(route_legs) != 3:
        return SimulationOutcome(
            completed=False,
            final_amount=None,
            simulated_profit=None,
            simulated_net_edge_bps=None,
            execution_drift_amount=None,
            execution_drift_bps=None,
            expectation_error=None,
            fill_ratio=Decimal("0"),
            failure_reason="invalid_route_legs",
            failure_leg=None,
            max_book_age_ms=None,
            legs=(),
        )

    amount = start_amount
    leg_results: list[dict[str, Any]] = []
    overall_fill = Decimal("1")
    observed_book_ages: list[int] = []

    for index, leg in enumerate(route_legs):
        leg_number = index + 1
        execution_timestamp_ms = detection_timestamp_ms + latency_ms * leg_number
        symbol = str(leg["symbol"])
        replay = replays.get(symbol)

        if replay is None:
            return _failed(
                leg_results,
                Decimal("0"),
                "missing_book_history",
                leg_number,
                observed_book_ages,
            )

        state = replay.state_at(execution_timestamp_ms)
        if state is None:
            return _failed(
                leg_results,
                Decimal("0"),
                "missing_book_state",
                leg_number,
                observed_book_ages,
            )

        book_age = execution_timestamp_ms - state.timestamp_ms
        if book_age > max_book_age_ms:
            return _failed(
                leg_results,
                Decimal("0"),
                "stale_book_state",
                leg_number,
                observed_book_ages + [book_age],
            )

        detected_execution = leg.get("execution") or {}
        detected_average = (
            _d(detected_execution["average_execution_price"])
            if detected_execution.get("average_execution_price") is not None
            else None
        )
        side = str(leg["side"]).upper()
        if side == "BUY":
            fill = execute_buy(
                state,
                amount,
                fee_bps=fee_bps_per_leg[index],
                detected_average_price=detected_average,
                execution_timestamp_ms=execution_timestamp_ms,
            )
        elif side == "SELL":
            fill = execute_sell(
                state,
                amount,
                fee_bps=fee_bps_per_leg[index],
                detected_average_price=detected_average,
                execution_timestamp_ms=execution_timestamp_ms,
            )
        else:
            return _failed(
                leg_results,
                Decimal("0"),
                "invalid_leg_side",
                leg_number,
                observed_book_ages,
            )

        overall_fill = min(overall_fill, fill.fill_ratio)
        observed_book_ages.append(fill.book_age_ms)
        leg_results.append(
            {
                "leg_index": leg_number,
                "symbol": symbol,
                "side": side,
                "execution_timestamp_ms": execution_timestamp_ms,
                **fill.to_dict(),
            }
        )

        if not fill.complete:
            return _failed(
                leg_results,
                overall_fill,
                "insufficient_liquidity",
                leg_number,
                observed_book_ages,
            )

        amount = fill.net_output_amount

    rounding_loss = start_amount * rounding_loss_bps / BPS
    final_amount = amount - rounding_loss
    simulated_profit = final_amount - start_amount
    simulated_net_edge_bps = simulated_profit / start_amount * BPS
    execution_drift_bps = sum(
        (
            _d(leg["adverse_slippage_bps"])
            for leg in leg_results
            if leg.get("adverse_slippage_bps") is not None
        ),
        Decimal("0"),
    )
    execution_drift_amount = start_amount * execution_drift_bps / BPS
    expectation_error = (
        simulated_profit - expected_profit if expected_profit is not None else None
    )

    return SimulationOutcome(
        completed=True,
        final_amount=final_amount,
        simulated_profit=simulated_profit,
        simulated_net_edge_bps=simulated_net_edge_bps,
        execution_drift_amount=execution_drift_amount,
        execution_drift_bps=execution_drift_bps,
        expectation_error=expectation_error,
        fill_ratio=overall_fill,
        failure_reason=None,
        failure_leg=None,
        max_book_age_ms=max(observed_book_ages) if observed_book_ages else None,
        legs=tuple(leg_results),
    )


def _failed(
    legs: list[dict[str, Any]],
    fill_ratio: Decimal,
    reason: str,
    failure_leg: int | None,
    book_ages: list[int],
) -> SimulationOutcome:
    return SimulationOutcome(
        completed=False,
        final_amount=None,
        simulated_profit=None,
        simulated_net_edge_bps=None,
        execution_drift_amount=None,
        execution_drift_bps=None,
        expectation_error=None,
        fill_ratio=fill_ratio,
        failure_reason=reason,
        failure_leg=failure_leg,
        max_book_age_ms=max(book_ages) if book_ages else None,
        legs=tuple(legs),
    )


def _float_or_none(value: Decimal | None) -> float | None:
    return None if value is None else float(value)
