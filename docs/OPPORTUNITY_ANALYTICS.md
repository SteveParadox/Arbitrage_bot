# Phase 7: Opportunity Database and Analytics

Every scanner observation is evidence. Phase 7 stores both attractive and unattractive route
evaluations in PostgreSQL so detection volume can be separated from actual executability.

## Funnel definitions

The ledger deliberately separates three concepts:

```text
detected
  = every route evaluation emitted by the scanner

executable
  = all three books existed, all three legs had enough visible depth, and the route completed

accepted
  = executable + fee model present + expected net return above the configured threshold
```

This makes questions such as:

```text
1,000 detected
70 executable
4 accepted
```

direct database queries rather than guesses from log files.

Accepted does **not** mean traded. Phase 7 still has no order execution.

## Stored observations

`opportunity_observations` stores every unique scanner evaluation, including:

- detection timestamp and trigger sequence;
- route / triangle;
- starting capital;
- gross final amount, gross profit, gross edge;
- fee cost;
- extra slippage allowance;
- rounding allowance;
- latency buffer;
- safety margin;
- total modeled cost;
- expected final amount and net edge;
- visible liquidity estimate;
- scanner status;
- executable flag;
- accepted / rejected decision;
- rejection reason;
- opportunity duration;
- complete raw scanner JSON.

Duplicate replay is safe. An observation key built from route + scan timestamp + triggering update is
unique, and PostgreSQL `ON CONFLICT DO NOTHING` prevents a backfill from duplicating live data.

## Rejection reasons

Current normalized reasons are:

- `missing_book`;
- `insufficient_liquidity`;
- `start_amount_not_configured`;
- `calculation_error`;
- `incomplete_route`;
- `fee_model_missing`;
- `net_not_profitable`;
- `below_min_net_edge`.

`ARB_OPPORTUNITY_MIN_NET_BPS` controls the acceptance threshold and defaults to zero.

## Available liquidity

For complete routes, available liquidity is the configured starting capital because all three legs
were fully fillable.

For an insufficient-liquidity scan, Phase 7 calculates the limiting fill ratio from the actual
Phase 3 execution fields and expresses it both as:

- `available_liquidity_ratio`;
- approximate `available_liquidity` in starting-capital units.

Unknown downstream books are left as null rather than pretending their liquidity is zero or full.

## Opportunity duration

Accepted scans are grouped into continuous `opportunity_windows`.

A window starts on the first accepted scan and remains active while qualifying scans continue. It
closes when the route is rejected or when the gap between accepted observations exceeds
`ARB_OPPORTUNITY_MAX_GAP_MS` (default 2000 ms).

Each window stores:

- start / last-seen / end time;
- duration;
- observation count;
- maximum net edge;
- maximum net profit;
- close reason.

This avoids treating hundreds of updates from one 900 ms market event as hundreds of independent
opportunities.

## PostgreSQL setup

Docker development:

```bash
cp .env.example .env
docker compose -f docker/docker-compose.yml up -d postgres
cd python
alembic upgrade head
```

The default local URL is:

```text
postgresql+psycopg://arbitrage:arbitrage@localhost:5432/arbitrage
```

Deployment credentials must come from environment/secrets, not source control.

## Live ingestion

The Rust scanner emits NDJSON. Pipe it into the Python ledger:

```bash
cargo run -p market-data \
  | cargo run -p scanner --bin scan-live \
  | python -m analytics.opportunity_ingest
```

The ingester commits in configurable batches and prints operational messages to stderr.

## Backfill

Existing Phase 5 journals can be replayed safely:

```bash
cd python
python -m analytics.opportunity_ingest \
  --file ../data/scans/arbitrage_scans.ndjson
```

Duplicate observations are ignored by the unique observation key.

## Analytics API

The FastAPI service exposes:

```text
GET /analytics/opportunities/summary?hours=24
GET /analytics/opportunities/rejections?hours=24
GET /analytics/opportunities/triangles?hours=24&limit=20
```

The summary includes detected, executable, accepted, rejected, gross-profitable, net-profitable,
conversion rates, edge statistics, opportunity-window count, and duration statistics.

The rejection endpoint explains where the funnel collapsed. The triangle endpoint shows which
structural routes are producing useful or useless signal volume.
