from strategy.triangle_discovery import (
    Instrument,
    TradeSide,
    build_config,
    discover_triangles,
)


def fixture_instruments() -> list[Instrument]:
    return [
        Instrument("BTCUSDT", "BTC", "USDT"),
        Instrument("ETHBTC", "ETH", "BTC"),
        Instrument("ETHUSDT", "ETH", "USDT"),
    ]


def test_discovers_all_six_directed_routes_for_one_triangle() -> None:
    result = discover_triangles(fixture_instruments())

    assert result.unique_triangle_count == 1
    assert len(result.routes) == 6
    assert {route.start_asset for route in result.routes} == {"BTC", "ETH", "USDT"}


def test_usdt_btc_eth_route_has_correct_buy_sell_directions() -> None:
    result = discover_triangles(fixture_instruments(), {"USDT"})
    route = next(
        route
        for route in result.routes
        if route.assets == ("USDT", "BTC", "ETH", "USDT")
    )

    assert route.pair1 == "BTCUSDT"
    assert route.pair2 == "ETHBTC"
    assert route.pair3 == "ETHUSDT"
    assert [leg.side for leg in route.legs] == [
        TradeSide.BUY,
        TradeSide.BUY,
        TradeSide.SELL,
    ]


def test_reverse_usdt_route_has_correct_directions() -> None:
    result = discover_triangles(fixture_instruments(), {"USDT"})
    route = next(
        route
        for route in result.routes
        if route.assets == ("USDT", "ETH", "BTC", "USDT")
    )

    assert [leg.side for leg in route.legs] == [
        TradeSide.BUY,
        TradeSide.SELL,
        TradeSide.SELL,
    ]


def test_start_asset_filter_keeps_only_requested_start() -> None:
    result = discover_triangles(fixture_instruments(), {"USDT"})

    assert len(result.routes) == 2
    assert all(route.start_asset == "USDT" for route in result.routes)


def test_missing_cross_pair_does_not_form_triangle() -> None:
    instruments = [
        Instrument("BTCUSDT", "BTC", "USDT"),
        Instrument("ETHUSDT", "ETH", "USDT"),
    ]

    result = discover_triangles(instruments)

    assert result.unique_triangle_count == 0
    assert result.routes == ()


def test_non_trading_instruments_are_excluded() -> None:
    instruments = fixture_instruments()
    instruments[1] = Instrument("ETHBTC", "ETH", "BTC", status="Closed")

    result = discover_triangles(instruments)

    assert result.unique_triangle_count == 0


def test_config_contains_structural_triangle_fields() -> None:
    result = discover_triangles(fixture_instruments(), {"USDT"})
    config = build_config(result, testnet=True, start_assets={"USDT"})

    assert config["version"] == 1
    assert config["market"] == "spot"
    assert config["triangle_count"] == 1
    assert config["route_count"] == 2

    route = config["routes"][0]
    assert {"pair1", "pair2", "pair3", "legs", "assets"} <= route.keys()
    assert len(route["legs"]) == 3
