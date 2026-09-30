-- Up Migration

CREATE TABLE invoices (
  id            SERIAL PRIMARY KEY,
  customer_name TEXT        NOT NULL,
  -- Money is stored as integer cents to avoid floating point rounding issues
  amount_cents  INTEGER     NOT NULL CHECK (amount_cents >= 0),
  currency      CHAR(3)     NOT NULL DEFAULT 'USD',
  status        TEXT        NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'sent', 'paid', 'void')),
  due_date      DATE        NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Soft delete, so the deletion can still be pushed to QuickBooks
  deleted_at    TIMESTAMPTZ,
  -- Client-provided Idempotency-Key header: a retried POST returns the invoice already created
  idempotency_key TEXT      UNIQUE,

  -- QuickBooks sync. The invoices table is also the sync queue (outbox): every local change sets
  -- sync_status = 'pending' and the worker pushes pending rows.
  quickbooks_id         TEXT        UNIQUE,
  -- QuickBooks' version of the invoice, required on updates (optimistic concurrency)
  quickbooks_sync_token TEXT,
  sync_status           TEXT        NOT NULL DEFAULT 'pending' CHECK (sync_status IN ('pending', 'synced', 'failed')),
  sync_attempts         INTEGER     NOT NULL DEFAULT 0,
  next_attempt_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  sync_error            TEXT
);

CREATE INDEX invoices_pending_sync_idx ON invoices (next_attempt_at) WHERE sync_status = 'pending';

-- The connected QuickBooks company (a single row), shared by the API and the worker
CREATE TABLE quickbooks_connection (
  realm_id   TEXT        PRIMARY KEY,
  token      JSONB       NOT NULL,
  -- Last "time" returned by the QuickBooks Change Data Capture API; next pull starts from here
  cdc_cursor TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Down Migration

DROP TABLE quickbooks_connection;
DROP TABLE invoices;
