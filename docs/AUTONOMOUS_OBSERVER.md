# Managed market observation

`engine-service` can supervise the existing Bybit public feed and scanner in
the same process as gRPC control. Set `ARB_ENGINE_AUTOSTART=true` after
generating a nonempty spot triangle configuration. The default is false.
The configured triangle environment must match `BYBIT_TESTNET`.

The process derives subscriptions from the configured routes, validates each
instrument against its conversion legs, waits for acknowledged subscriptions
and fresh synchronized books, then scans only affected routes. Connection
resets invalidate scanner books. Each evaluated candidate is durably accepted
by the event outbox before scanning continues. The Python event consumer writes
the candidate event and the opportunity observation in one PostgreSQL
transaction, and `/health` reports the observer state. The dashboard shows
that state separately from trading controls.

The connector reloads instrument metadata before each WebSocket connection,
including reconnects. Spot metadata records `basePrecision` as the quantity
increment and retains `minOrderAmt` and `maxMarketOrderQty` separately; Bybit
deprecates spot `minOrderQty`. These public-data fields do not authorize an
order. A future execution path must recheck current filters and account state
before submission.

Example after provisioning route configuration, PostgreSQL, Redis, migrations,
and the existing internal gRPC token:

```sh
ARB_ENGINE_AUTOSTART=true BYBIT_TESTNET=true \
  cargo run --manifest-path rust/Cargo.toml -p engine-service
```

This integration is **observation only**. It does not reserve capital, approve
an autonomous execution, submit orders, reconcile fills, or prove a paper
three-leg cycle. The existing gRPC start control remains a runtime control
flag for the older manual paths; its state is not proof of an autonomous
execution process. Keep `ARB_LIVE_TRADING_ENABLED=false` while developing
the durable reservation and recovery path. Do not enable real orders based on
observer health.

For a local process replay, generate fresh timestamps immediately before
starting the binary:

```sh
python scripts/make_observer_replay.py /tmp/arb-observer-replay
ARB_ENV=development ARB_LIVE_TRADING_ENABLED=false ARB_ENGINE_AUTOSTART=true \
  ARB_OBSERVER_REPLAY_FILE=/tmp/arb-observer-replay/market.jsonl \
  ARB_TRIANGLE_CONFIG=/tmp/arb-observer-replay/triangles.json \
  cargo run --manifest-path rust/Cargo.toml -p engine-service
```

The normal Redis, outbox, and gRPC configuration still applies. Replay refuses
to start outside development with the live deployment gate explicitly false.
The fixture emits an accepted candidate followed by a rejected candidate.
Its prices are synthetic and cannot be used to estimate real returns.
