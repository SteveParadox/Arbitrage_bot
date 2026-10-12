CREATE TABLE execution_accounts (
 id text PRIMARY KEY CHECK (id ~ '^[A-Za-z0-9_-]{1,64}$'),
 mode text NOT NULL CHECK (mode IN ('testnet','live')),
 exchange_uid text NOT NULL CHECK (exchange_uid ~ '^[0-9]{1,24}$'),
 mock boolean NOT NULL DEFAULT false CHECK (NOT mock OR mode='testnet'),
 enabled boolean NOT NULL DEFAULT false,
 blocked boolean NOT NULL DEFAULT true,
 recovering boolean NOT NULL DEFAULT true,
 emergency_stop boolean NOT NULL DEFAULT false,
 state text NOT NULL DEFAULT 'STOPPED' CHECK (state IN ('STOPPED','STARTING','SYNCHRONIZING','RECOVERING','READY','RUNNING','DEGRADED','STOPPING','BLOCKED','FAILED')),
 epoch bigint NOT NULL DEFAULT 0 CHECK (epoch>=0), heartbeat timestamptz,
 market_ready boolean NOT NULL DEFAULT false, market_ms bigint,
 snapshot_ms bigint, balance_evidence jsonb, expected_wallet jsonb,
 limits jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(mode,exchange_uid,mock)
);
CREATE TABLE execution_cycles (
 id text PRIMARY KEY, account_id text NOT NULL REFERENCES execution_accounts(id),
 candidate_id text NOT NULL, route jsonb NOT NULL, risk jsonb NOT NULL,
 start_asset text NOT NULL CHECK (start_asset='USDT'),
 starting numeric NOT NULL CHECK (starting>0 AND starting<'Infinity'::numeric),
 holdings jsonb NOT NULL,
 state text NOT NULL CHECK (state IN ('RESERVED','EXECUTING','RECOVERY_REQUIRED','COMPLETED','RECOVERED','NOT_STARTED')),
 committed boolean NOT NULL DEFAULT true, settled boolean NOT NULL DEFAULT false,
 realized_pnl numeric CHECK (realized_pnl>'-Infinity'::numeric AND realized_pnl<'Infinity'::numeric),
 reason text, epoch bigint NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), completed_at timestamptz,
 UNIQUE(account_id,candidate_id), UNIQUE(id,account_id),
 CHECK (NOT settled OR (NOT committed AND realized_pnl IS NOT NULL AND completed_at IS NOT NULL)),
 CHECK (settled OR realized_pnl IS NULL)
);
-- Deliberately one active cycle per exchange account: unknown exposure owns it.
CREATE UNIQUE INDEX execution_one_active ON execution_cycles(account_id) WHERE committed;
CREATE UNIQUE INDEX execution_evidence_once ON execution_cycles(account_id,(risk->>'market_fingerprint')) WHERE risk ? 'market_fingerprint';
CREATE TABLE execution_orders (
 account_id text NOT NULL, link_id text NOT NULL CHECK (link_id ~ '^[A-Za-z0-9_-]{1,36}$'),
 cycle_id text NOT NULL, intent jsonb NOT NULL,
 state text NOT NULL CHECK (state IN ('INTENT_RECORDED','SUBMITTING','SUBMISSION_UNKNOWN','ACKNOWLEDGED','PARTIALLY_FILLED','FILLED','CANCEL_PENDING','CANCELLED','REJECTED','EXPIRED','RECONCILIATION_REQUIRED')),
 exchange_order_id text, evidence jsonb, terminal boolean NOT NULL DEFAULT false, confirmed boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(account_id,link_id), UNIQUE(account_id,exchange_order_id),
 FOREIGN KEY(cycle_id,account_id) REFERENCES execution_cycles(id,account_id)
);
CREATE TABLE execution_fills (
 account_id text NOT NULL, execution_id text NOT NULL, link_id text NOT NULL,
 cycle_id text NOT NULL, evidence jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(account_id,execution_id),
 FOREIGN KEY(account_id,link_id) REFERENCES execution_orders(account_id,link_id),
 FOREIGN KEY(cycle_id,account_id) REFERENCES execution_cycles(id,account_id)
);
CREATE TABLE execution_movements (
 account_id text NOT NULL, execution_id text NOT NULL, asset text NOT NULL,
 role text NOT NULL CHECK (role IN ('input','output','fee')),
 amount numeric NOT NULL CHECK (amount>'-Infinity'::numeric AND amount<'Infinity'::numeric),
 PRIMARY KEY(account_id,execution_id,role),
 FOREIGN KEY(account_id,execution_id) REFERENCES execution_fills(account_id,execution_id)
);
CREATE TABLE execution_reconciliation (
 id bigserial PRIMARY KEY, account_id text NOT NULL REFERENCES execution_accounts(id),
 healthy boolean NOT NULL, evidence jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE execution_events (
 id bigserial PRIMARY KEY, account_id text NOT NULL REFERENCES execution_accounts(id),
 kind text NOT NULL, payload jsonb NOT NULL, published boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX execution_outbox_pending ON execution_events(id) WHERE NOT published;
CREATE TABLE execution_commands (
 account_id text NOT NULL REFERENCES execution_accounts(id), request_id text NOT NULL,
 fingerprint text NOT NULL, subject text NOT NULL, response jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(account_id,request_id)
);
