"""Discover triangular spot trading routes from Bybit instruments."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from itertools import combinations
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class TradeSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True)
class Instrument:
    symbol: str
    base_asset: str
    quote_asset: str
    status: str = "Trading"

    @classmethod
    def from_bybit(cls, payload: dict[str, Any]) -> "Instrument":
        return cls(
            symbol=str(payload["symbol"]).upper(),
            base_asset=str(payload["baseCoin"]).upper(),
            quote_asset=str(payload["quoteCoin"]).upper(),
            status=str(payload.get("status", "")),
        )


@dataclass(frozen=True)
class TriangleLeg:
    symbol: str
    from_asset: str
    to_asset: str
    side: TradeSide
    base_asset: str
    quote_asset: str

    def to_dict(self) -> dict[str, str]:
        return {
            "symbol": self.symbol,
            "from_asset": self.from_asset,
            "to_asset": self.to_asset,
            "side": self.side.value,
            "base_asset": self.base_asset,
            "quote_asset": self.quote_asset,
        }


@dataclass(frozen=True)
class TriangleRoute:
    id: str
    triangle_id: str
    start_asset: str
    assets: tuple[str, str, str, str]
    pair1: str
    pair2: str
    pair3: str
    legs: tuple[TriangleLeg, TriangleLeg, TriangleLeg]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "triangle_id": self.triangle_id,
            "start_asset": self.start_asset,
            "assets": list(self.assets),
            "pair1": self.pair1,
            "pair2": self.pair2,
            "pair3": self.pair3,
            "legs": [leg.to_dict() for leg in self.legs],
        }


@dataclass(frozen=True)
class DiscoveryResult:
    instruments: tuple[Instrument, ...]
    unique_triangle_count: int
    routes: tuple[TriangleRoute, ...]


def fetch_bybit_spot_instruments(testnet: bool = False, timeout: float = 15.0) -> list[Instrument]:
    host = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"
    query = urlencode({"category": "spot", "status": "Trading"})
    url = f"{host}/v5/market/instruments-info?{query}"
    request = Request(url, headers={"User-Agent": "arbitrage-bot/0.1"})

    with urlopen(request, timeout=timeout) as response:
        payload = json.load(response)

    if payload.get("retCode") != 0:
        raise RuntimeError(
            f"Bybit instruments request failed: "
            f"{payload.get('retCode')} {payload.get('retMsg')}"
        )

    raw = payload.get("result", {}).get("list", [])
    return normalize_instruments(Instrument.from_bybit(item) for item in raw)


def normalize_instruments(instruments: Any) -> list[Instrument]:
    unique_by_symbol: dict[str, Instrument] = {}
    for instrument in instruments:
        normalized = Instrument(
            symbol=instrument.symbol.upper(),
            base_asset=instrument.base_asset.upper(),
            quote_asset=instrument.quote_asset.upper(),
            status=instrument.status,
        )
        if normalized.status != "Trading":
            continue
        if not normalized.symbol or normalized.base_asset == normalized.quote_asset:
            continue
        unique_by_symbol[normalized.symbol] = normalized

    return sorted(unique_by_symbol.values(), key=lambda item: item.symbol)


def discover_triangles(
    instruments: list[Instrument],
    start_assets: set[str] | None = None,
) -> DiscoveryResult:
    instruments = normalize_instruments(instruments)
    pair_index = build_pair_index(instruments)
    assets = sorted({asset for pair in pair_index for asset in pair})

    normalized_starts = None
    if start_assets:
        normalized_starts = {asset.upper() for asset in start_assets}

    routes: list[TriangleRoute] = []
    unique_triangle_count = 0

    for first, second, third in combinations(assets, 3):
        asset_set = (first, second, third)
        if not triangle_is_connected(asset_set, pair_index):
            continue

        starts = asset_set
        if normalized_starts is not None:
            starts = tuple(asset for asset in asset_set if asset in normalized_starts)
            if not starts:
                continue

        unique_triangle_count += 1
        for start in starts:
            remaining = sorted(asset for asset in asset_set if asset != start)
            route_orders = (
                (start, remaining[0], remaining[1], start),
                (start, remaining[1], remaining[0], start),
            )
            for route_assets in route_orders:
                routes.append(build_route(route_assets, pair_index))

    routes.sort(key=lambda route: route.id)
    return DiscoveryResult(
        instruments=tuple(instruments),
        unique_triangle_count=unique_triangle_count,
        routes=tuple(routes),
    )


def build_pair_index(instruments: list[Instrument]) -> dict[tuple[str, str], Instrument]:
    pair_index: dict[tuple[str, str], Instrument] = {}
    for instrument in instruments:
        key = pair_key(instrument.base_asset, instrument.quote_asset)
        existing = pair_index.get(key)
        if existing is not None and existing.symbol != instrument.symbol:
            raise ValueError(
                "multiple spot instruments connect the same assets: "
                f"{existing.symbol} and {instrument.symbol}"
            )
        pair_index[key] = instrument
    return pair_index


def triangle_is_connected(
    assets: tuple[str, str, str],
    pair_index: dict[tuple[str, str], Instrument],
) -> bool:
    first, second, third = assets
    return all(
        key in pair_index
        for key in (
            pair_key(first, second),
            pair_key(second, third),
            pair_key(third, first),
        )
    )


def build_route(
    assets: tuple[str, str, str, str],
    pair_index: dict[tuple[str, str], Instrument],
) -> TriangleRoute:
    legs = tuple(
        build_leg(assets[index], assets[index + 1], pair_index)
        for index in range(3)
    )
    typed_legs = (legs[0], legs[1], legs[2])
    triangle_id = "-".join(sorted(set(assets[:3])))
    route_id = ">".join(assets)

    return TriangleRoute(
        id=route_id,
        triangle_id=triangle_id,
        start_asset=assets[0],
        assets=assets,
        pair1=typed_legs[0].symbol,
        pair2=typed_legs[1].symbol,
        pair3=typed_legs[2].symbol,
        legs=typed_legs,
    )


def build_leg(
    from_asset: str,
    to_asset: str,
    pair_index: dict[tuple[str, str], Instrument],
) -> TriangleLeg:
    instrument = pair_index.get(pair_key(from_asset, to_asset))
    if instrument is None:
        raise ValueError(f"no instrument connects {from_asset} and {to_asset}")

    if instrument.base_asset == from_asset and instrument.quote_asset == to_asset:
        side = TradeSide.SELL
    elif instrument.quote_asset == from_asset and instrument.base_asset == to_asset:
        side = TradeSide.BUY
    else:
        raise ValueError(f"instrument {instrument.symbol} does not match requested conversion")

    return TriangleLeg(
        symbol=instrument.symbol,
        from_asset=from_asset,
        to_asset=to_asset,
        side=side,
        base_asset=instrument.base_asset,
        quote_asset=instrument.quote_asset,
    )


def pair_key(first: str, second: str) -> tuple[str, str]:
    return tuple(sorted((first.upper(), second.upper())))


def build_config(
    result: DiscoveryResult,
    *,
    testnet: bool,
    start_assets: set[str] | None,
) -> dict[str, Any]:
    host = "api-testnet.bybit.com" if testnet else "api.bybit.com"
    return {
        "version": 1,
        "exchange": "bybit",
        "market": "spot",
        "generated_at": datetime.now(UTC).isoformat(),
        "source": {
            "endpoint": f"https://{host}/v5/market/instruments-info",
            "category": "spot",
            "status": "Trading",
            "testnet": testnet,
        },
        "start_assets": sorted(asset.upper() for asset in (start_assets or set())),
        "instrument_count": len(result.instruments),
        "triangle_count": result.unique_triangle_count,
        "route_count": len(result.routes),
        "routes": [route.to_dict() for route in result.routes],
    }


def write_config(config: dict[str, Any], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(config, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )


def load_instruments_file(path: Path) -> list[Instrument]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        raw = payload.get("result", {}).get("list", payload.get("instruments", []))
    elif isinstance(payload, list):
        raw = payload
    else:
        raise ValueError("instrument file must contain a Bybit response, list, or instruments key")

    instruments = []
    for item in raw:
        if "baseCoin" in item:
            instruments.append(Instrument.from_bybit(item))
        else:
            instruments.append(
                Instrument(
                    symbol=item["symbol"],
                    base_asset=item["base_asset"],
                    quote_asset=item["quote_asset"],
                    status=item.get("status", "Trading"),
                )
            )
    return normalize_instruments(instruments)


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[2]
    default_output = repo_root / "shared" / "config" / "triangles.json"

    parser = argparse.ArgumentParser(description="Discover Bybit spot triangular routes")
    parser.add_argument("--output", type=Path, default=default_output)
    parser.add_argument(
        "--start-assets",
        default="",
        help="Comma-separated starting assets. Empty means every possible start asset.",
    )
    parser.add_argument(
        "--testnet",
        action="store_true",
        default=os.getenv("BYBIT_TESTNET", "false").lower() == "true",
    )
    parser.add_argument(
        "--instruments-file",
        type=Path,
        help="Use a local instrument JSON file instead of calling Bybit.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start_assets = {
        asset.strip().upper()
        for asset in args.start_assets.split(",")
        if asset.strip()
    }

    instruments = (
        load_instruments_file(args.instruments_file)
        if args.instruments_file
        else fetch_bybit_spot_instruments(testnet=args.testnet)
    )
    result = discover_triangles(instruments, start_assets or None)
    config = build_config(
        result,
        testnet=args.testnet,
        start_assets=start_assets or None,
    )

    print(
        f"discovered {config['triangle_count']} unique triangles, "
        f"{config['route_count']} directed routes from "
        f"{config['instrument_count']} trading spot instruments"
    )

    if not args.dry_run:
        write_config(config, args.output)
        print(f"wrote {args.output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
