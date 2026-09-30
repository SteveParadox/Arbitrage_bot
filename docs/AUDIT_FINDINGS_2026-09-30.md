# Pre-correction findings — 2026-09-30

Overall audit status: NEEDS MAJOR FIXES

Baseline: `2a5a6df657e8aa1402d923964a6ea33a81f19bbf`. This finding log was written before changing application source.

## Demonstrated by source trace and baseline tests

1. High — `rust/scanner/src/engine.rs::scan_route`: timestamp skew is recorded but no age/skew rejection exists. Old books can produce accepted opportunities. `scan_live.rs::main` ignores reconnect status, mixing sessions.
2. High — `rust/orderbook/src/lib.rs::LocalOrderBook::apply`: a delta can initialize a book with no snapshot; the engine does not itself interpret `u=1`. Invalid stream continuity can look executable. Absolute EPSILON also prevents tiny positive requests from traversing depth.
3. High — `rust/market-data/src/connector.rs::run_connection`: one market event resets connection-wide staleness. All topics are sent in one subscription, exceeding Bybit's 10-argument spot limit at four symbols. Missing timestamps are replaced with local time. Default category in `config.rs` is linear, despite a spot strategy.
4. High — `python/simulator/book_archive.py::SymbolReplay`: sequence regression, malformed levels and resets are not validated. Same-timestamp resets are sorted by sequence, which can invert session order. Checkpoint as-of selection itself correctly uses only indexes at/before the requested timestamp.
5. High — `python/analytics/opportunity_store.py::record_scan`: windows mutate before duplicate detection; late accepted observations can rewind active windows. In-memory window caches are unsynchronized across writers. Exact same NDJSON deduplicates, but rescanning a market event changes scan_timestamp and creates a different key.
6. High — exchange constraints are absent end-to-end: `market-data/src/model.rs::LotSizeFilter` misses spot basePrecision, quotePrecision, minOrderAmt and maxMarketOrderQty; scanner/replay cannot enforce precision/minimum notional. A complete visible-depth fill is not exchange-order feasibility.
7. High — `python/simulator/book_archive.py::load_symbol_replays` loads from the last snapshot without a bound; long sessions require hours of deltas for even one chunk. `paper_trade.py::run_simulation` also retains every opportunity and result. Chunking is not a memory bound.
8. Medium — `python/simulator/replay.py::simulate_route`: drift sums per-leg percentages and converts that sum to cash; that is not a compounded route cash difference. A configured rounding buffer is subtracted as cash although actual rounding is not modeled.
9. Medium — `python/simulator/paper_trade.py::_result_row`: open windows are treated as ended at last_seen, giving false expiry certainty. `_summary` omits fill-only expected-profit and profitable-among-fill denominators; totals from different starting assets are potentially mixed.
10. High — `book_ingest.py::_book_values` accepts malformed levels, string booleans and invalid numbers; JSON arrays/null can crash both ingestion paths. Archive decode errors may abort a complete experiment.
11. Medium — `python/api/settings.py`: relative `.env` path depends on cwd. Docker omits shared config; migration CLI reads a different configuration mechanism. CI migration targets default `arbitrage`, but its service creates `arbitrage_test` (only TEST_DATABASE_URL is set).
12. Medium — `python/tests/test_paper_replay.py`: the 450 → BTC at 100 → ETH at .05 → USDT at 21 fixture actually returns 1890 before fees, not 472.5. Price replacements represented as deltas leave old best levels in the book. Baseline: 1 failing, 20 passing, 1 PostgreSQL skip; the separate PostgreSQL test passes.
13. Medium — `rust/scanner/src/lib.rs::TriangleConfig::validate`: validates direction but not spot/exchange/source identity or canonical route identity. An arbitrary symbol is not checked against active metadata. Discovery is cubic in asset count.
14. Medium — `python/strategy/profitability.py`: non-finite values are not rejected explicitly. Aggregate fee multiplication is exact for linear fixed-price conversions, but not a substitute for fee-reduced depth traversal on subsequent legs.
15. Medium — CI baseline Rust formatting fails. GitHub run 36603756887 did not run any job: all five checks explicitly report account locked due to billing, not syntax/runners. Resolving billing will not resolve the separate source/test/workflow defects.

## Confirmed positives / limits

- No private Bybit order-entry implementation or credential-bearing request found. Execution crate is a stub; its boolean helper should fail closed until live execution exists.
- BUY spends quote across asks; SELL consumes base across bids. Outputs feed the next leg in correct source/destination units. Symbol-to-route indexing limits recalculation to affected routes.
- Decimal profitability model compounds three fees, uses bps/10000, and labels extra slippage as future-execution allowance. Replay does not subtract the slippage, latency or safety buffers again.
- PostgreSQL 16 baseline upgrades 0001→0002→0003, downgrades to base, and upgrades again successfully. Alembic's default comparison sees no schema drift; models omit several migration server defaults (not checked by default).
- Frontend lint/build pass, but the UI is a static Phase 1 scaffold, not an analytics dashboard.
- Current data lacks exchange/network/session identity and receive timestamps. No proof of live-feed uptime, clock alignment, complete archives, sustainable throughput, capital reservation, unwind economics or profitable trading exists.

## Correction scope

Apply regression-tested fail-closed book/scanner/ingest/replay corrections, window ordering/deduplication corrections, exact route-drift arithmetic, censored-lifetime and explicit metric semantics, and demonstrated startup/CI defects. Preserve the existing architecture. Document remaining exchange realism and scaling work rather than pretend these require only small fixes. No private order API or live-trading enablement will be added.
