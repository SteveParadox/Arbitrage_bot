# Phase 16: Rust ↔ Python Service Boundary

Phase 16 separates Python and Rust behind explicit service interfaces.

~~~text
React / TypeScript
        |
        v
     FastAPI
        |
        | gRPC commands
        v
Rust engine-control
        |
        +---- shared runtime controls ----> Rust risk/scanner/execution

Rust scanner/execution/engine-control
        |
        | Redis Streams events
        v
     FastAPI consumer
        |
        v
    PostgreSQL
~~~

Python does not embed Rust, and the browser still talks only to FastAPI.

## Command plane: gRPC

The shared protobuf contract is:

~~~text
shared/proto/engine_control.proto
~~~

Service:

~~~text
arbitrage.engine.v1.EngineControl
~~~

Commands:

~~~text
StartTrading
StopTrading
UpdateLimits
ReloadStrategy
GetStatus
~~~

FastAPI uses generated protobuf bindings under python/api/grpc/ and calls Rust through
python/api/engine_client.py. Command acknowledgements are accepted only when the returned
request_id matches the request, the command name matches the RPC, accepted=true, and
applied_at_ms is positive. The configured RPC timeout must also be greater than zero.

### Authentication

The public HTTP control secret and the internal gRPC secret are deliberately different:

~~~env
ARB_CONTROL_API_TOKEN=<operator-to-FastAPI secret>
ARB_ENGINE_GRPC_TOKEN=<FastAPI-to-Rust secret>
~~~

Each should contain at least 32 high-entropy bytes.

The internal token is sent as gRPC metadata named x-engine-token.

Neither secret belongs in a Vite environment variable.

## Trading commands

Phase 15's HTTP surface remains:

~~~text
POST /trading/start
POST /trading/stop
~~~

but FastAPI now sends the command to Rust rather than opening the runtime gate itself.

Start still requires:

~~~text
ARB_LIVE_TRADING_ENABLED=true
manual kill switch clear
circuit breaker clear
valid gRPC authentication
~~~

Rust writes the authoritative runtime state:

~~~text
data/control/trading_state.json
~~~

The Phase 9 risk engine independently reads the same state before new live approval and before an
approval crosses the execution gate.

If the Rust gRPC service is unavailable during an authenticated stop request, FastAPI performs a
local fail-closed write to the shared control file as an emergency fallback. Start has no fallback.

## Runtime risk limits

FastAPI exposes:

~~~text
POST /engine/limits
~~~

Supported runtime overrides:

~~~text
min_net_edge_bps
max_slippage_bps
max_trade_size
max_total_exposure
max_daily_loss
~~~

Rust validates the command and writes:

~~~text
data/control/risk_limits.json
~~~

The risk engine reads that overlay on each new risk evaluation. Missing values fall back to static
Phase 9 limits. Invalid JSON, invalid decimal values, or internally inconsistent limits fail
closed.

The gRPC service validates the effective pair:

~~~text
max_total_exposure >= max_trade_size
~~~

even when only one side of the pair is being changed.

## Strategy reload

FastAPI exposes:

~~~text
POST /engine/reload-strategy
~~~

Rust first validates the current triangle configuration. Only a valid strategy produces a new
generation token:

~~~text
data/control/strategy_reload.json
~~~

The scanner checks for a new generation at most twice per second. When one appears it reloads the
triangle route graph, resets all local books, and waits for fresh snapshots before a route can
become complete again.

A bad reload signal is logged and does not terminate the scanner.

## Event plane: Redis Streams

The default stream is:

~~~text
arb.events
~~~

Every event uses this envelope:

~~~text
event_id
event_type
occurred_at_ms
source
schema_version
payload
~~~

Current cycle-level event types:

~~~text
opportunity.detected
trade.executed
trade.failed
balance.updated
engine.health
~~~

Individual exchange-order telemetry is separate:

~~~text
order.executed
order.failed
~~~

trade.executed and trade.failed are emitted by the three-leg coordinator, so one trade event
represents one complete triangular cycle outcome rather than one leg.

opportunity.detected is emitted only for complete route evaluations. Incomplete book
initialization and malformed scan attempts are not mislabeled as opportunities.

Execution events are emitted from a background publisher queue. Redis network I/O therefore does
not sit on the order-submission hot path.

Defaults:

~~~env
ARB_EVENT_QUEUE_CAPACITY=4096
ARB_EVENT_STREAM_MAXLEN=1000000
~~~

Redis Stream trimming is approximate. If Redis is unavailable, the publisher retries. If the local
bounded queue fills, telemetry may be dropped and Rust logs a warning. Trading safety never depends
on Redis delivery.

## Python consumer

FastAPI starts a Redis consumer-group worker in application lifespan.

Defaults:

~~~env
ARB_EVENT_STREAM=arb.events
ARB_EVENT_CONSUMER_GROUP=python-api
ARB_EVENT_DEAD_LETTER_STREAM=arb.events.dlq
ARB_EVENT_BATCH_SIZE=100
~~~

The consumer:

1. creates the consumer group if necessary;
2. reclaims stale pending entries with XAUTOCLAIM and follows the returned scan cursor until the
   pending-entry list has been scanned;
3. parses the event envelope;
4. commits the event to PostgreSQL;
5. acknowledges Redis only after the database commit.

Both event_id and Redis stream_id are unique in PostgreSQL. Replay inserts use conflict-ignore
semantics for either unique key so an already-seen stream record cannot wedge the consumer group.

Malformed envelopes are copied to the dead-letter stream before they are acknowledged, so one bad
message cannot permanently block the consumer group. Envelope validation also enforces the
PostgreSQL identity-column lengths, signed BIGINT timestamp range, and strict finite JSON values.

If PostgreSQL rejects an individual event with a permanent data error despite those checks, the
consumer isolates that insert with a savepoint, commits the other valid events, copies the rejected
event to the dead-letter stream, and then acknowledges it. Infrastructure failures such as a
database outage still propagate, so those events remain pending for replay rather than being
misclassified as bad data.

Events are stored in:

~~~text
engine_events
~~~

Migration:

~~~text
0006_engine_events
~~~

event_id is the PostgreSQL primary key, so replay is idempotent.

## Health

GET /health now also checks the Rust gRPC control service. Missing runtime control state is
treated as a healthy fail-closed stopped state, while malformed, unsupported, or otherwise corrupt
control state makes the Rust service unhealthy and keeps runtime trading disabled.

The periodic engine.health stream event uses the same static-risk/runtime-limit validation as the
control service status instead of merely reporting that the heartbeat task is alive. Its payload
sets component=engine-control so consumers do not confuse control-plane health with liveness of
every independently launched scanner/coordinator/execution process.

If gRPC is unavailable:

~~~text
engine_grpc.status = offline
overall API health = degraded
~~~

This does not invent a healthy Rust engine merely because FastAPI itself is responding.

## Docker Compose

Phase 16 adds:

~~~text
redis
engine-control
~~~

Default ports:

~~~text
6379   Redis, bound to host localhost by default
50051  Rust gRPC control, bound to host localhost by default
8000   FastAPI
5173   frontend
~~~

Redis uses append-only persistence. The API and Rust control service share the control directory.
The Rust control service mounts risk state and the repository shared configuration read-only, so
reload validation sees the same triangle configuration as host-side scanner processes.

Any separately launched scanner or execution process must use the same Redis URL and the same
control directory to participate in the service boundary.

## Configuration

Generate two independent secrets:

~~~bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
python -c "import secrets; print(secrets.token_urlsafe(48))"
~~~

Example:

~~~env
ARB_CONTROL_API_TOKEN=<first-secret>
ARB_ENGINE_GRPC_TOKEN=<second-secret>

ARB_ENGINE_GRPC_TARGET=127.0.0.1:50051
ARB_REDIS_URL=redis://127.0.0.1:6379/0
ARB_EVENT_STREAM=arb.events
~~~

## Failure semantics

- gRPC unavailable: start, update-limits and reload fail; stop uses the fail-closed fallback.
- Redis unavailable: command/control and trading safety continue; telemetry retries asynchronously.
- PostgreSQL unavailable: Redis messages remain pending and are reclaimed later.
- runtime limit file malformed: new risk evaluation fails closed.
- runtime control file malformed: normal live approval fails closed.
- strategy reload invalid: command is rejected before generation changes.
- scanner reload unexpectedly fails: old routes remain active and the scanner stays alive.

Commands and events intentionally use different transports. Commands need immediate request/response
semantics and explicit failure. Events need buffering, replay and consumer groups.


## Network security

The default Compose file exposes Redis and the gRPC control port only on 127.0.0.1 while services
communicate over the private Compose network. The gRPC metadata token is not a substitute for
transport encryption across an untrusted network.

If Python and Rust are deployed on different hosts, place the service boundary on a private
network or add TLS/mTLS before exposing port 50051. Do not publish Redis directly to the public
Internet.


## Cross-language CI verification

CI now contains a dedicated `rust-python-boundary` job. It starts a real Rust
`engine-service` and Redis instance, then exercises the generated Python gRPC client against
that Rust server.

The integration test verifies:

~~~text
Python GetStatus -> Rust response
Python StopTrading -> Rust acknowledgement
matching request_id / command / applied_at_ms
Rust command -> Redis engine.health event
deployment-disabled StartTrading -> FAILED_PRECONDITION
corrupt control state -> unhealthy + fail closed
StopTrading repairs corrupt control state
~~~

The ordinary Python unit tests also verify that the configured gRPC timeout is actually passed to
the RPC, non-positive timeouts fail before network I/O, and mismatched/negative command
acknowledgements are rejected.
