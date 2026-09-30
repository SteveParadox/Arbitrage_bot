# Overall audit status: NEEDS MAJOR FIXES

Repository: `SteveParadox/Arbitrage_bot`. Audited baseline: `2a5a6df657e8aa1402d923964a6ea33a81f19bbf` (Phases 1–8). Date: 2026-09-30. Findings distinguish the original implementation from corrections in this audit branch. This report does not validate live trading or any subsequently developed phases.

## 1. Executive Summary

The system separates detected, executable and accepted observations, walks visible depth, compounds fees, reconstructs historical books and schedules three delayed legs. Those are useful foundations. They do **not yet establish that accepted observations represent opportunities that would actually execute profitably on Bybit**.

Demonstrated defects included stale and mixed-session scanner books, initialization from deltas, replay of invalid sequences, duplicate ingestion changing opportunity windows, out-of-order window mutation, open-window lifetime misclassification, malformed input failures, an artificial rounding cash charge, and a financially incorrect replay fixture. Targeted corrections and regression tests accompany this report; investigation findings were recorded before source changes in `AUDIT_FINDINGS_2026-09-30.md`.

Major unresolved risks are exchange quantity/notional/IOC constraints, archive session and capture provenance, unknown missing deltas, incomplete treatment of partial-execution inventory, overlapping opportunity capital, and memory growth. Continue controlled paper research with these limitations explicitly reported. Do not use aggregate simulated profit as a portfolio return or claim exchange feasibility from the accepted flag.

### Verification evidence

| Check | Observed result |
|---|---|
| Python, including PostgreSQL tests | 57 passed; one dependency deprecation warning |
| Rust workspace | 27 tests passed at the last completed full run; subsequently added crossed-book Rust regression awaits a repeat run |
| Rust clippy | Passed with `-D warnings` before that final crossed-book regression |
| Rust formatting | Applied; final repeat unavailable after runtime replacement |
| Python lint | Passed with explicit baseline Ruff rules |
| Python/Rust profitability parity | 10 shared cases passed |
| PostgreSQL migrations | Fresh upgrade through 0003, downgrade to base, and re-upgrade passed on PostgreSQL 16 |
| Alembic/model comparison | Passed including server-default comparison after corrections |
| Frontend | TypeScript lint and production build passed |
| JSON | 10 files parsed; five schemas checked; route/profitability config validation passed |
| End-to-end regression | Python discovery → Rust scanner → PostgreSQL deduplication → archive → five replay latencies passed |
| Synthetic 10,000 × 5 experiment | 50,000 persisted simulations, 23.09 seconds, 560.01 MiB process peak RSS |
| Docker runtime and live feed soak | Not verified |
| Hosted GitHub Actions | Blocked by account billing lock; jobs did not start |

The synthetic benchmark uses shallow, regular, favorable books. It proves that this fixture completes, not market realism or bounded memory. The execution runtime was replaced before the final repeat checks; Rust/PostgreSQL executables were no longer available. Results above report completed checks without treating the unavailable repeat as a pass.

## 2. Phase-by-Phase Results

| Phase | Status after corrections | Evidence and remaining issues |
|---|---|---|
| 1 Foundation | PASS WITH ISSUES | Python/Rust/frontend boundaries and local builds work. Environment/config paths and Python Docker shared-config layout corrected. CI database URL corrected. Container startup remains untested; hosted CI billing-blocked. |
| 2 Market data | PASS WITH ISSUES | Public spot default, subscription batching, per-symbol book freshness, timestamp checks and session clearing corrected. Real socket fault/soak tests, acknowledgement deadlines, pong timeout and healthy-session backoff reset remain incomplete. |
| 3 Local books | PASS WITH ISSUES | Snapshot/delta/delete/depth arithmetic verified. Initial deltas rejected; `u == 1` resets; tiny inputs and crossed books addressed. Floating arithmetic and exchange steps remain limitations. |
| 4 Discovery | PASS WITH ISSUES | Directed BUY/SELL triangles and affected-route contracts tested. Reverse cycles are distinct economically and must be retained. Rust now rejects wrong venue/category/status and inconsistent IDs. Runtime metadata validity is still not enforced. |
| 5 Scanner | PASS WITH ISSUES | Symbol-to-route index avoids scanning all routes per update. Quantity propagation, missing books and partial liquidity covered. Age/skew gates and reconnect clearing added. No exchange-rule feasibility gate. |
| 6 Profitability | PASS WITH ISSUES | Basis points, multiplicative three-leg fees and decision buffers consistent in ten cross-language cases. Aggregate fee model is an approximation with nonlinear depth. Exact step rounding absent. |
| 7 Ledger/analytics | PASS WITH ISSUES | Detected/executable/accepted funnel preserved. Duplicate and late-window mutation corrected; writers serialized. Late backfills need ordered window rebuild; liquidity fields are proxies. |
| 8 Simulation | FAIL | Timestamp-as-of replay, delayed scheduling and fee propagation are testable and corrected. Archive continuity/provenance and exchange execution constraints remain insufficient for a realistic profitability claim. Memory is not bounded by opportunity chunk size alone. |

Actual data flow: normalized public messages enter Rust books; affected routes consume asks for quote-funded BUYs and bids for base-funded SELLs; scanner records carry gross amounts and profitability decisions; Python ingests these into observations/windows. A separate normalized book stream is archived and reconstructed for paper execution. That separate stream is the weakest integration boundary: it lacks sufficient session, venue/network and capture-integrity metadata to prove that the history used by the simulator is the history assumed by the detector.

## 3. Critical Findings

### F01 — stale and mixed-session scanner state

**Severity:** High. **File/function:** `rust/scanner/src/engine.rs`, `scan_route`; `rust/scanner/src/bin/scan_live.rs`, `main`.

**Problem:** Original scanner recorded timestamp skew without rejecting old/inconsistent books and retained state across connection status changes. **Impact/example:** A newly updated BTC book could combine with an old ETH book and produce artificial profit after reconnect. **Fix implemented:** configurable age/skew gates, future-time rejection, and clearing on reconnect/status/book-integrity errors. Regression tests cover stale/skewed and reset state. **Remaining:** thresholds are research defaults (1,000 ms age, 100 ms skew), not proof of high-frequency simultaneity; measure clock offset and publish skew distributions.

### F02 — invalid local-book initialization and executable states

**Severity:** High. **File/class:** `rust/orderbook/src/lib.rs`, `LocalOrderBook`.

**Problem:** A delta could initialize an incomplete book; absolute epsilon terminated extremely small executions; crossed books could be walked. **Impact/example:** Missing resting asks can inflate proceeds or apparent liquidity. **Fix implemented:** require snapshot, recognize service-reset `u == 1`, use positive remainder termination, reject crossed spread during execution. Tests cover initialization/reset/tiny amounts/crossing; the final added Rust crossing test still needs rerunning in a Rust-enabled runtime. Ordinary non-contiguous update IDs are not treated as missing messages merely because they skip numbers.

### F03 — feed health and subscription integrity

**Severity:** High. **File/function:** `rust/market-data/src/connector.rs`, `run_connection`; `rust/market-data/src/config.rs`.

**Problem:** Activity on another symbol/topic could hide a stale order book; subscriptions exceeded the documented spot argument limit; default category was linear. **Impact/example:** Four symbols × three topics generates twelve subscription arguments; active trades can mask a dead book. **Fix implemented:** spot default, batches of ten arguments, independent book receipt clocks, required timestamps and configured-symbol checks. Regression tests cover batching and ticker activity not refreshing books.

**Remaining recommendation:** track connection liveness, per-symbol order-book freshness and per-topic diagnostics separately. Tickers/trades must not validate book freshness. Add acknowledgement deadlines and heartbeat-response timeout; reset exponential backoff after a genuinely healthy session. Skip malformed noncritical trade/ticker messages with counters; invalidate and resynchronize an affected book after malformed book data. Current broad reconnect behavior is conservative but can cause unnecessary outages. Never keep using a book after silently losing a delta.

### F04 — historical integrity and source provenance

**Severity:** High. **File/class:** `python/simulator/book_archive.py`, `SymbolReplay`; `python/simulator/models.py`, `MarketBookEvent`; `python/simulator/book_ingest.py`, `_book_values`.

**Problem:** Original replay lacked adequate sequence and numeric-level validation; equal-timestamp sequence sorting could reorder resets. Archive rows do not identify network, session, capture continuity or local receive time. **Impact/example:** A skipped corrupt delta, or testnet and mainnet events sharing a symbol, can yield a plausible but false book. **Fix implemented:** stable arrival order for equal timestamps, sequence/update regression invalidation until snapshot, strict finite levels and explicit invalid-history failures. **Still open:** persist capture/session/network/category and integrity markers, enforce isolation, and resnapshot after gaps. Until then use separate databases/captures per source. Non-contiguous IDs alone do not establish a loss.

### F05 — duplicate, late and concurrent ingestion changed windows

**Severity:** High. **File/class:** `python/analytics/opportunity_store.py`, `OpportunityStore.record_scan`.

**Problem:** Window mutation preceded deduplication; late observations could rewind state; separate store instances cached outdated active windows. **Impact/example:** Replaying one record twice could close or extend a profitable window despite inserting only one observation. **Fix implemented:** validate before writes, insert/deduplicate first, transaction-scoped advisory serialization, reload active state, and store late observations without rewriting windows. Tests cover duplicates, late arrivals, silent gaps and two store instances. **Remaining:** historical late observations need an explicit ordered rebuild to receive accurate window membership. The lock is global and deliberately trades throughput for correctness. Simultaneous independent transaction stress testing remains necessary.

### F06 — accepted does not mean exchange executable

**Severity:** High. **Files/functions:** `rust/scanner/src/engine.rs`, `scan_route`; `python/simulator/replay.py`, `simulate_opportunity`; metadata handling in `rust/market-data/src/connector.rs`.

**Problem:** Instrument precision, minimum amount, maximum market quantity and market-order IOC/slippage behavior are not enforced downstream. Account fee tiers are configured assumptions. **Impact/example:** A positive small triangle may leave a second-leg quantity below the minimum notional or an untradable residual. **Recommended fix:** version spot instrument rules with observations, quantize every order and received quantity by asset/rule, enforce all per-leg limits, and model IOC limits. Keep this open rather than implement speculative exchange behavior. Require mainnet/testnet agreement in discovery, scanner and archive.

### F07 — partial executions and overlapping capital are not portfolio accounting

**Severity:** High. **Files/functions:** `python/simulator/replay.py`, `simulate_opportunity`; `python/simulator/paper_trade.py`, `run_simulation`, summary calculation.

**Problem:** Failed cases retain diagnostic legs but do not value or liquidate residual inventory. Repeated detections are independent attempts, not capital-reserved executions. **Impact/example:** Leg 1 fills, leg 2 fails, and an asset remains exposed; summing only completed cycles ignores that economic outcome. Ten observations of one window can reuse the same hypothetical capital. **Recommended fix:** report residual holdings and liquidation assumptions; add a separate capital/inventory-aware experiment mode before interpreting totals as return. Metadata now explicitly labels independent scenarios and summaries count failed unvalued attempts. Mixed starting assets are rejected to avoid summing incompatible currency units.

### F08 — replay financial attribution and lifetime

**Severity:** Medium. **Files/functions:** `python/simulator/replay.py`, `simulate_opportunity`; `python/simulator/paper_trade.py`, `_result_row` and `_summary`.

**Problem:** Predicted rounding allowance was charged as realized cash; drift was a sum of leg basis points; open windows appeared to expire at their last observation. **Impact/example:** Detection 600 ms into an 800 ms closed window has only 200 ms remaining, while an open window has unknown remaining life. **Fix implemented:** no prediction rounding haircut, reject nonzero direct replay allowance, compounded drift, closed-window remaining lifetime and censored open-window value. Tests cover 800/600/200 timing, open windows, compounded drift, favorable prices and denominator differences. Actual quantity rounding remains absent and is explicitly labeled.

### F09 — malformed ingestion and per-case failure isolation

**Severity:** High. **Files/functions:** `python/analytics/opportunity_store.py`, `classify_scan`; `python/analytics/opportunity_ingest.py`; `python/simulator/book_ingest.py`, `_book_values`; `python/simulator/book_archive.py`, `load_symbol_replays`.

**Problem:** JSON arrays/nulls, string booleans and invalid numeric values could crash ingestion or be interpreted incorrectly; archive decode failure could abort an experiment. **Impact/example:** `"complete": "false"` is truthy in Python unless strictly checked. **Fix implemented:** object/type/finite validation, strict booleans and IDs, recoverable malformed-line handling, explicit invalid-history and invalid-case results. **Remaining:** skipped corrupt book events need persistent invalidation markers; unbounded line sizes and unexpected database errors still require operational handling, transaction rollback and quarantine. Do not treat successful parsing as capture completeness.

### F10 — memory and archive reconstruction

**Severity:** High. **File/function:** `python/simulator/book_archive.py`, `load_symbol_replays`; `python/simulator/paper_trade.py`, `run_simulation`.

**Problem:** A chunk can require replay from an arbitrarily old snapshot, while all opportunities/results remain retained. **Impact/example:** A 500-opportunity chunk may load a day's deltas for one symbol. **Fix implemented:** 100,000-event budget per load; oversized histories produce `history_limit_exceeded`, never silent truncation. **Remaining:** this is an event-count guard, not a byte bound. Persist periodic validated checkpoints; stream opportunities/results and batch writes; account for checkpoint and ORM overhead. Test deep books and sparse snapshots before increasing scale.

### F11 — configuration, schema defaults and test oracle defects

**Severity:** Medium. **Files/locations:** `python/api/settings.py`, `Settings`; `python/alembic/env.py`; `python/alembic.ini`; `python/analytics/models.py`; `python/simulator/models.py`; `docker/Dockerfile.python`; `.github/workflows/ci.yml`; `python/tests/test_paper_replay.py`.

**Problem:** CWD-sensitive paths, absent Docker shared configuration, CI migration URL mismatch and model/server-default drift undermined reproducibility. Replay fixture arithmetic asserted 472.5 for a route yielding 1,890 and changed price without deleting a better old delta level. **Fix implemented:** repository-relative settings/config, Alembic configuration anchoring, matching server defaults, explicit CI database URL, Docker shared copy and financially correct replacement snapshots. Verified migration cycle/model comparison and corrected tests. Docker execution and a final external-CWD rerun remain unverified. Ruff baseline rules are explicit; Cargo lockfile records resolved dependencies. Existing Rust formatting changes satisfy the formatter rather than redesigning code.

### F12 — precision and profitability approximation

**Severity:** Medium. **Files/classes:** `rust/orderbook/src/lib.rs`, `LocalOrderBook`; `rust/scanner/src/profitability.rs`; `python/strategy/profitability.py`.

**Problem:** `f64` thresholds can differ near zero; fee-compounding gross output assumes fixed effective conversion rates even when fees reduce the next leg's depth requirement. **Impact/example:** A fee-reduced leg may consume a better VWAP than the original full-size leg. **Fix implemented:** finite-value checks and expanded parity boundaries; this does not remove nonlinear-depth approximation or exact rounding risk. **Recommendation:** keep fast estimates for candidate scanning, store price ticks and quantity steps as checked integers, use widened fixed-point products and explicit rounding at executable boundaries, and validate finalists with an exact reference. Decimal belongs in accounting/reference paths; introducing it indiscriminately into every hot-loop operation is unnecessary.

### F13 — liquidity and identity semantics

**Severity:** Medium. **File/functions:** `python/analytics/opportunity_store.py`, observation construction/key generation; `rust/scanner/src/lib.rs`, route validation.

**Problem:** `available_liquidity`/ratio derive from route fill information and are not a solved maximum executable starting capital. Keys deduplicate identical scans but include scan timestamp, so recomputing the same market event later creates a new detection. **Impact/example:** A backfill that reruns the scanner is different from replaying its original NDJSON and can increase detection counts. **Fix implemented:** stricter canonical route IDs and spot/venue/status validation. **Recommended fix:** label liquidity as a proxy and define market-event identity separately if re-scan deduplication is required. Validate symbols against contemporaneous active metadata. Reverse route directions are not duplicates.

### F14 — CI external failure and incomplete operational hardening

**Severity:** High for CI availability; Medium for operational hardening. **Files/locations:** `.github/workflows/ci.yml`; `docker/docker-compose.yml`; `rust/execution/src/lib.rs`, `live_execution_allowed`; `shared/schemas/opportunity.schema.json`.

**Problem/evidence:** All five jobs in run [36603756887](https://github.com/SteveParadox/Arbitrage_bot/actions/runs/36603756887) had no executed steps and reported: “The job was not started because your account is locked due to a billing issue.” This is an account issue, not evidence of runner exhaustion, YAML failure or token permissions. Local lint/config/test issues were separate defects. **Fix:** code defects corrected; account owner must resolve GitHub billing. No account settings were modified.

Source inspection found public market-data requests, no private Bybit order-entry calls and no hardcoded production Bybit credentials. The execution gate stub now always returns false with a regression test; `ARB_LIVE_TRADING_ENABLED` remains false by default. Environment-driven DB credentials and masked settings summaries are present. Ignore rules reduce accidental commits but cannot guarantee secrets never enter history; full historical secret scanning was not performed. Development database credentials/published ports and unauthenticated API exposure are unsuitable production assumptions. CLI file paths are trusted local inputs, not an exposed file API. SQLAlchemy parameterizes values; no unsafe dynamic JSON execution was found. The old opportunity schema and unused config/helpers need removal or clear legacy labeling. The frontend remains a scaffold, not a verified analytics dashboard.

## 4. Financial Correctness

BUY consumes quote currency over ascending asks and produces base units. SELL consumes base over descending bids and produces quote units. A complete gross path passes each destination amount into the next source amount. Partial fills terminate the cycle. Average price is quote/base; worst price is the last consumed level. Depth slippage is already reflected in these cash flows.

For an independently checkable constant-price example: 450 USDT / 100 USDT per BTC = 4.5 BTC; / 0.05 BTC per ETH = 90 ETH; × 5.25 USDT per ETH = 472.5 USDT. Gross profit is 22.5 USDT, gross return 5%, or 500 bps. With 10 bps received-asset fee on each leg, final amount is `472.5 × 0.999³ = 471.0839170275` USDT and profit is 21.0839170275 USDT. The aggregate fee-equivalent cost is 1.4160829725 USDT. This aggregate conversion is exact for this fixed-rate example; nonlinear-depth routes require fee-reduced rewalking.

Prediction model: gross final amount × the three fee factors, minus starting-capital-based additional slippage, latency, rounding and safety allowances. A basis point is 1/10,000; percentages are separate units. Slippage allowance must mean additional future execution uncertainty, since the book walk already includes visible depth. These buffers influence expected net edge and acceptance, not actual exchange debits.

Replay uses delayed depth and deducts received-asset fees between legs. It already avoided charging latency/slippage/safety prediction buffers again. The remaining prediction rounding haircut has now been removed. **No double charge of prediction slippage/latency/safety is present in the corrected replay.** Exact lot-step rounding and residual inventory accounting are still missing, so this is not a complete realized-cash model. Expected and simulated profits intentionally have different buffer semantics. Fee overrides must reflect the actual account tier and product.

Ten parity fixtures cover ordinary profitable and rejected cases, exact break-even, tiny edge/capital, large capital, fractional fees, high fees and nonzero rounding. Agreement is numerical within the parity check tolerance, not bit-identical Decimal/f64 arithmetic or proof of exchange accuracy.

## 5. Simulation Integrity

| Question | Answer |
|---|---|
| Is there lookahead in timestamp selection? | No future timestamp selection reproduced: `bisect_right` uses events at or before the target and checkpoints no later than that event. Boundary tests pass. This does not prove capture completeness or clock correctness. |
| Are delayed books selected correctly? | Yes for validated archived streams. Equal-timestamp events use recorded arrival order. Receive-time visibility cannot be reconstructed because receive timestamps are absent. |
| Are 25/50/100/200/500 ms modeled? | Yes. Each scenario uses T+L, T+2L and T+3L, giving completion at 75/150/300/600/1500 ms. |
| Are quantities propagated? | Yes: each actual leg output, less its received-asset fee, becomes the next leg input. |
| Are fees correct? | Mechanically yes for the configured three fee rates. Account tier and rounding are not independently enforced. |
| Are failures/partial fills recorded? | Yes as failed cases with reasons and leg diagnostics; one bad history does not have to terminate the run. Residual exposure remains unvalued. |
| Is lifetime correct? | Closed-window remaining lifetime uses end minus detection, not total duration. Open windows are censored. Observed window end is not proof of a continuously profitable interval. |

Windows end on observed rejection or gap handling; a rejection timestamp is an upper bound on when profitability disappeared. Silent gaps close at the last accepted observation rather than bridging the gap. No idle timer guarantees every dormant DB window promptly becomes closed. Exchange generation timestamp and detection wall clock require measured clock alignment before millisecond economic claims.

Metrics now separate expected profit across all attempts, expected profit among fills, simulated profit among fills, profitable rate over attempts and over fills, failed unvalued attempts, and known-lifetime denominators. `profit_delta` compares predicted and simulated outputs on compatible filled cases; missing values must not become zero-profit fills. Compounded drift compares effective route conversion rates and is a diagnostic, not pure adverse-selection cost: fee-dependent size can also change VWAP.

## 6. Database Integrity

The chain `0001_opportunity_ledger → 0002_paper_simulation → 0003_paper_remaining_lifetime` successfully upgraded, downgraded and upgraded on disposable PostgreSQL 16 databases. Model comparison passed after matching server defaults. Monetary Numeric(38,18) and edge Numeric(20,8) definitions match the inspected migrations. Existing nullable/FK deletion semantics were reviewed; simulation child deletion cascades and nullable observation/window references avoid dangling references as designed.

Observation uniqueness makes exact NDJSON ingestion idempotent. Book event uniqueness prevents identical archive replays. Insert-first window processing avoids duplicate side effects. Transactions encompass observation/window changes; global advisory locking protects participating ledger writers, but external writers bypassing this code are not protected. Consider a partial unique constraint for one open window per route and supporting route/time or FK indexes where query plans justify them. Do not add redundant indexes without measurements.

The funnel is: detected = scanner evaluations; executable = three complete visible-depth legs with valid scanner status; accepted = executable plus fee model and profitability thresholds. Accepted is not traded. Thus 1,000/70/4 correctly means 7% executable, 0.4% accepted/detected, and about 5.714% accepted/executable. Rejected means detected minus accepted, including execution failures. Rejection handling includes missing book, insufficient liquidity, missing start amount, calculation/incomplete-route/model failures, nonprofitable/below-threshold results, plus new stale/skew states. Raw scan retention preserves diagnostics.

PostgreSQL integration fixtures use model-created tables; the migration chain was tested separately, not inferred from those fixtures. No production database was migrated. History tables lack provenance constraints, and regenerated scans/backfills need an explicit identity/window-rebuild policy before claiming arbitrary replay equivalence.

## 7. Performance Risks

| Opportunities × five scenarios | Assessment |
|---|---|
| 100 | Functional testing size; database setup and query overhead likely dominate. No dedicated timing claim. |
| 1,000 | More symbol history, per-row ORM work and repeated reconstruction matter; chunk size does not bound oldest-snapshot lookback. |
| 10,000 | Synthetic run completed 50,000 cases in 23.09 s at 560.01 MiB peak RSS. Deep or sparse-snapshot history may be much heavier. |
| 100,000 | Not benchmarked. Full result/opportunity retention makes multi-GB use plausible; do not extrapolate the shallow fixture as a capacity guarantee. |

Rust's symbol index is appropriate; state is largely single-owner, so no demonstrated hot lock contention warrants redesign. Material costs are duplicate book representations, per-message JSON serialization/cloning, scan allocations, line-oriented stdout/file writes, and bounded asynchronous channels that eventually block producers. The 4,096-capacity channel is a buffer, not unlimited throughput; a slow database pipeline can delay socket reads and heartbeats. Capture timestamped events independently, measure queue age and scanner lag, and avoid synchronous persistence in the feed path.

Python uses ORM objects and per-observation work; commit batching is not equivalent to bulk SQL insertion. The new global writer lock is a correctness safeguard with a throughput cost. Persistent checkpoints, streaming result batches and query-plan-guided indexes are justified next steps. No microsecond latency, production throughput or real-market capacity claim was measured.

## 8. Missing Tests and Added Regression Coverage

Added coverage is in `python/tests/test_audit_regressions.py`, `python/tests/test_audit_postgres.py`, Rust module tests and the shared profitability cases. It exercises invalid envelopes/booleans/numbers, snapshot requirements, service reset, tiny amounts, crossed books, stale/skew rejection, batching/freshness separation, duplicate/late/gap windows, invalid archive order, same-time reset order, checkpoint as-of boundaries, five latency schedules, fees, favorable movement, compounded drift, open/remaining lifetime and bounded-history failure. The end-to-end regression invokes the actual Rust scanner when its binary is available; it can skip in a Python-only environment. `scripts/benchmark_paper_audit.py` reproducibly seeds a dedicated empty benchmark database; it is not a market backtest.

Still required:

1. Real socket harness for reconnect/ack/pong deadlines, malformed ticker/trade isolation, prolonged stalls and healthy-session backoff reset.
2. Capture discontinuity/session/network mixing tests with persisted integrity markers and clock offsets.
3. Instrument-rule tests for every BUY/SELL direction, min amount, max market size, quantity precision and IOC partial cancellation.
4. Concurrent independent PostgreSQL transactions, rollback after partial batch failure and ordered historical window rebuilding.
5. Fee-reduced multi-level rewalk parity and threshold tests at exact integer-step boundaries.
6. Residual inventory valuation, unwind failures, capital reservations and repeated observations from one window.
7. Deep-book/sparse-snapshot runs, streaming memory limits and 100,000-opportunity workloads.
8. Container startup smoke tests, production-like service settings and hosted CI rerun after billing repair.
9. Finish the final Rust formatter/test/clippy rerun, including the latest crossing regression.

## 9. Recommended Fix Order

| Priority | Work |
|---|---|
| P0 — correctness/safety | Preserve session/source/capture integrity; invalidate history on lost/corrupt book updates; validate clock alignment. Retain fail-closed scanner and execution controls. |
| P1 — financial accuracy | Enforce exchange instrument rules and actual quantization; rewalk fee-reduced sizes; model IOC limits, residual holdings and capital reuse. |
| P2 — reliability | Resolve GitHub billing, finish CI/runtime checks, implement ack/pong/backoff behavior, quarantine bad inputs and rebuild late-data windows deterministically. |
| P3 — performance | Persistent validated checkpoints; stream candidates/results; batch database operations; measure pipeline backpressure and deep-book memory. |
| P4 — maintainability | Retire legacy schemas/config/helpers, clarify liquidity naming and analytical denominators, pin remaining dependency/tool versions and build an actual analytics UI. |

## 10. Final Paper-Trading Readiness Checklist

“No” includes prerequisites not established by evidence; it does not assert every component is broken.

| Prerequisite | Yes/No | Scope |
|---|---|---|
| Market feed stable under real faults | No | Unit checks only; no sustained live soak |
| Order-book arithmetic trustworthy | Yes | Tested synthetic snapshots/deltas/depth; final Rust regression rerun outstanding |
| End-to-end book/capture integrity established | No | Missing provenance and gap markers |
| Triangle directions verified | Yes | Discovery/consumer direction and route tests |
| Financial model internally consistent | Yes | Constant-rate reference and ten parity cases |
| Exchange execution constraints modeled | No | Precision, notional, IOC and residual accounting incomplete |
| Opportunity persistence trustworthy for ordered valid input | Yes | PostgreSQL dedup/window regressions |
| Arbitrary backfill/window equivalence verified | No | Late data needs explicit rebuild |
| Historical archive trustworthy for economic conclusions | No | Capture/session/clock limitations |
| Replay timestamp selection free of future-state lookup | Yes | As-of and checkpoint boundary tests |
| Complete absence of economic lookahead established | No | Receive-time availability is not represented |
| All five latency scenarios verified | Yes | Exact three-leg timing and fees |
| Database migrations verified | Yes | Disposable PostgreSQL upgrade/downgrade/comparison |
| Completed local Python/parity/frontend checks passing | Yes | Results above |
| Entire final edited Rust tree reverified | No | Runtime replacement prevented last repeat |
| Hosted CI functioning | No | Account billing lock |
| Real trading introduced or enabled | No | No private order calls; gate false |

The central answer is therefore **not yet**: the funnel meaningfully narrows theoretical detections, but the remaining execution and historical-integrity gaps prevent interpreting its accepted or simulated-profitable counts as opportunities that would survive actual Bybit execution.

### Exchange sources checked

Official documentation consulted for material exchange assumptions:

- [Bybit public order book](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook): snapshots replace state, zero-size deletions, `u == 1` service reset, update/sequence semantics, timestamps and depth frequencies; RPI liquidity is excluded.
- [WebSocket connection](https://bybit-exchange.github.io/docs/v5/ws/connect): public network endpoints, heartbeat guidance and spot subscription argument limit.
- [Instrument information](https://bybit-exchange.github.io/docs/v5/market/instrument): current spot precision and amount/market-size constraints; spot pagination differs from derivatives.
- [Order creation semantics](https://bybit-exchange.github.io/docs/v5/order/create-order): used only to audit realism; market IOC/slippage behavior and spot market quantity units. No private API was added or invoked.
- [Spot fees explained](https://www.bybit.com/en/help-center/article/Bybit-Spot-Fees-Explained): fees apply to the received asset and vary with the applicable schedule; configured defaults are not account verification.
