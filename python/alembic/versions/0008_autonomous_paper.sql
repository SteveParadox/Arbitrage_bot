CREATE TABLE paper_accounts (
 id TEXT PRIMARY KEY, mode TEXT NOT NULL DEFAULT 'paper' CHECK(mode='paper'),
 recovering BOOLEAN NOT NULL DEFAULT FALSE,
 enabled BOOLEAN NOT NULL DEFAULT FALSE, blocked BOOLEAN NOT NULL DEFAULT FALSE,
 test_account BOOLEAN NOT NULL DEFAULT TRUE, epoch BIGINT NOT NULL DEFAULT 0,
 heartbeat TIMESTAMPTZ, config JSONB NOT NULL DEFAULT '{}', created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE paper_balances (
 account_id TEXT NOT NULL REFERENCES paper_accounts(id), asset TEXT NOT NULL,
 available NUMERIC NOT NULL DEFAULT 0 CHECK(available>=0),
 reserved NUMERIC NOT NULL DEFAULT 0 CHECK(reserved>=0), version BIGINT NOT NULL DEFAULT 0,
 reconciled_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(account_id,asset)
);
CREATE TABLE paper_cycles (
 id TEXT PRIMARY KEY, account_id TEXT NOT NULL REFERENCES paper_accounts(id),
 candidate_id TEXT NOT NULL, evidence_key TEXT NOT NULL, route_id TEXT NOT NULL, start_asset TEXT NOT NULL,
 starting NUMERIC NOT NULL CHECK(starting>0), expected_profit NUMERIC NOT NULL,
 epoch BIGINT NOT NULL, state TEXT NOT NULL CHECK(state IN ('RESERVED','EXECUTING','RECONCILING','COMPLETED','RECOVERED','RECOVERY_REQUIRED','FAILED')),
 holdings JSONB NOT NULL, route JSONB NOT NULL, risk JSONB NOT NULL,
 settled BOOLEAN NOT NULL DEFAULT FALSE, realized_pnl NUMERIC,
 cash_delta NUMERIC, failure_reason TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 completed_at TIMESTAMPTZ, UNIQUE(account_id,candidate_id), UNIQUE(account_id,evidence_key),
 CHECK(realized_pnl IS NULL OR settled)
);
CREATE INDEX paper_cycles_lifecycle ON paper_cycles(account_id,state,created_at);
CREATE TABLE paper_orders (
 id TEXT PRIMARY KEY, cycle_id TEXT NOT NULL REFERENCES paper_cycles(id),
 intent JSONB NOT NULL, result JSONB, state TEXT NOT NULL CHECK(state IN ('SUBMITTING','TERMINAL','EXPIRED')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), completed_at TIMESTAMPTZ
);
CREATE INDEX paper_orders_cycle ON paper_orders(cycle_id);
CREATE TABLE paper_ledger (
 id BIGSERIAL PRIMARY KEY, account_id TEXT NOT NULL REFERENCES paper_accounts(id),
 cycle_id TEXT REFERENCES paper_cycles(id), operation_id TEXT NOT NULL,
 asset TEXT NOT NULL, debit_scope TEXT NOT NULL, credit_scope TEXT NOT NULL,
 amount NUMERIC NOT NULL CHECK(amount>0), created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(account_id,operation_id,asset,debit_scope,credit_scope), CHECK(debit_scope<>credit_scope)
);
CREATE INDEX paper_ledger_cycle_asset ON paper_ledger(cycle_id,asset);
CREATE INDEX paper_ledger_account_asset ON paper_ledger(account_id,asset);
CREATE TABLE paper_events (
 id BIGSERIAL PRIMARY KEY, account_id TEXT NOT NULL REFERENCES paper_accounts(id),
 cycle_id TEXT REFERENCES paper_cycles(id), kind TEXT NOT NULL, payload JSONB NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX paper_events_account ON paper_events(account_id,id);
CREATE TABLE paper_commands (
 account_id TEXT NOT NULL REFERENCES paper_accounts(id), request_id TEXT NOT NULL,
 fingerprint TEXT NOT NULL, response JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY(account_id,request_id)
);
