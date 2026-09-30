-- Up Migration

-- Money: NUMERIC (exact decimals, like QuickBooks' amounts) instead of integer cents
ALTER TABLE invoices RENAME COLUMN amount_cents TO amount;
ALTER TABLE invoices ALTER COLUMN amount TYPE NUMERIC(14, 2) USING amount / 100.0;
ALTER TABLE invoices RENAME CONSTRAINT invoices_amount_cents_check TO invoices_amount_check;

-- Amount still to be paid. Owned by QuickBooks, where payments are recorded.
ALTER TABLE invoices ADD COLUMN balance NUMERIC(14, 2);
UPDATE invoices SET balance = CASE WHEN status = 'paid' THEN 0 ELSE amount END;
ALTER TABLE invoices
  ALTER COLUMN balance SET NOT NULL,
  ADD CONSTRAINT invoices_balance_check CHECK (balance >= 0);

ALTER TABLE quickbooks_accounts RENAME COLUMN current_balance_cents TO current_balance;
ALTER TABLE quickbooks_accounts ALTER COLUMN current_balance TYPE NUMERIC(16, 2) USING current_balance / 100.0;

-- Invoice synchronization metadata
ALTER TABLE invoices
  -- QuickBooks company the invoice belongs to; its QuickBooks id is only unique within that company
  ADD COLUMN quickbooks_realm_id  TEXT,
  -- Incremented on every local change; synced_version is the last one QuickBooks has
  ADD COLUMN version              INTEGER NOT NULL DEFAULT 1,
  ADD COLUMN synced_version       INTEGER,
  -- Content both sides agreed on at the last sync: the base for detecting who changed what
  ADD COLUMN last_synced_snapshot JSONB,
  ADD COLUMN last_synced_at       TIMESTAMPTZ,
  -- Local and remote versions when both changed; the invoice isn't synced until resolved
  ADD COLUMN sync_conflict        JSONB,
  ADD COLUMN voided_at            TIMESTAMPTZ;

UPDATE invoices
SET quickbooks_realm_id = (SELECT realm_id FROM quickbooks_connection LIMIT 1)
WHERE quickbooks_id IS NOT NULL;

UPDATE invoices
SET synced_version = version,
    last_synced_at = updated_at,
    last_synced_snapshot = jsonb_build_object(
      'customer_name', customer_name, 'amount', amount::text, 'currency', currency, 'due_date', due_date::text
    )
WHERE sync_status = 'synced' AND quickbooks_id IS NOT NULL;

ALTER TABLE invoices
  DROP CONSTRAINT invoices_quickbooks_id_key,
  ADD CONSTRAINT invoices_quickbooks_ref_key UNIQUE (quickbooks_realm_id, quickbooks_id),
  ADD CONSTRAINT invoices_quickbooks_ref_check CHECK ((quickbooks_id IS NULL) = (quickbooks_realm_id IS NULL)),
  DROP CONSTRAINT invoices_sync_status_check,
  ADD CONSTRAINT invoices_sync_status_check
    CHECK (sync_status IN ('pending', 'synced', 'conflict', 'unknown', 'failed'));

-- Notifications received from QuickBooks (webhooks) or found by reconciliation, kept for auditing
CREATE TABLE sync_events (
  id                 BIGSERIAL   PRIMARY KEY,
  source             TEXT        NOT NULL CHECK (source IN ('webhook', 'reconciliation')),
  realm_id           TEXT        NOT NULL,
  entity_type        TEXT        NOT NULL,
  external_entity_id TEXT        NOT NULL,
  operation          TEXT        NOT NULL,
  -- Identifies one notification, so a redelivery isn't processed twice
  event_key          TEXT        NOT NULL UNIQUE,
  payload            JSONB       NOT NULL,
  received_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  processing_status  TEXT        NOT NULL DEFAULT 'pending'
                     CHECK (processing_status IN ('pending', 'processed', 'failed', 'ignored')),
  processed_at       TIMESTAMPTZ,
  error              TEXT
);

CREATE INDEX sync_events_entity_idx ON sync_events (realm_id, entity_type, external_entity_id);

-- The synchronization queue
CREATE TABLE sync_jobs (
  id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  direction        TEXT        NOT NULL CHECK (direction IN ('INBOUND', 'OUTBOUND')),
  entity_type      TEXT        NOT NULL CHECK (entity_type IN ('invoice', 'payment', 'account')),
  -- Local invoice id (OUTBOUND) or QuickBooks id (INBOUND)
  entity_id        TEXT        NOT NULL,
  -- Jobs with the same key run one at a time, oldest first
  entity_key       TEXT        NOT NULL,
  realm_id         TEXT,
  operation        TEXT        NOT NULL,
  event_id         BIGINT      REFERENCES sync_events (id),
  payload          JSONB       NOT NULL DEFAULT '{}',
  status           TEXT        NOT NULL DEFAULT 'PENDING'
                   CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'UNKNOWN')),
  attempts         INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  max_attempts     INTEGER     NOT NULL DEFAULT 8 CHECK (max_attempts > 0),
  next_run_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Ownership: only the worker holding claim_token may finish the job, until the lease expires
  locked_at        TIMESTAMPTZ,
  locked_by        TEXT,
  claim_token      UUID,
  lease_expires_at TIMESTAMPTZ,
  -- Set just before a create is sent to QuickBooks: from then on, a lost response is ambiguous
  create_sent_at   TIMESTAMPTZ,
  last_error       TEXT,
  last_error_class TEXT,
  duration_ms      INTEGER,
  completed_at     TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (status <> 'PROCESSING' OR (claim_token IS NOT NULL AND lease_expires_at IS NOT NULL))
);

CREATE INDEX sync_jobs_runnable_idx ON sync_jobs (next_run_at, created_at) WHERE status = 'PENDING';
CREATE INDEX sync_jobs_active_entity_idx ON sync_jobs (entity_key, created_at)
  WHERE status IN ('PENDING', 'PROCESSING', 'UNKNOWN');
CREATE INDEX sync_jobs_lease_idx ON sync_jobs (lease_expires_at) WHERE status IN ('PROCESSING', 'UNKNOWN');

-- Work queued in the old columns moves to the new queue
INSERT INTO sync_jobs (direction, entity_type, entity_id, entity_key, operation, payload)
SELECT 'OUTBOUND', 'invoice', id::text, 'invoice:' || id,
       CASE WHEN deleted_at IS NOT NULL THEN 'delete' ELSE 'upsert' END,
       jsonb_build_object('version', version)
FROM invoices
WHERE sync_status IN ('pending', 'failed');

DROP INDEX invoices_pending_sync_idx;
ALTER TABLE invoices DROP COLUMN sync_attempts, DROP COLUMN next_attempt_at;

-- Down Migration

ALTER TABLE invoices
  ADD COLUMN sync_attempts   INTEGER     NOT NULL DEFAULT 0,
  ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();
UPDATE invoices SET sync_status = 'pending' WHERE sync_status IN ('conflict', 'unknown');
CREATE INDEX invoices_pending_sync_idx ON invoices (next_attempt_at) WHERE sync_status = 'pending';

DROP TABLE sync_jobs;
DROP TABLE sync_events;

ALTER TABLE invoices
  DROP CONSTRAINT invoices_sync_status_check,
  ADD CONSTRAINT invoices_sync_status_check CHECK (sync_status IN ('pending', 'synced', 'failed')),
  DROP CONSTRAINT invoices_quickbooks_ref_check,
  DROP CONSTRAINT invoices_quickbooks_ref_key,
  ADD CONSTRAINT invoices_quickbooks_id_key UNIQUE (quickbooks_id),
  DROP COLUMN quickbooks_realm_id,
  DROP COLUMN version,
  DROP COLUMN synced_version,
  DROP COLUMN last_synced_snapshot,
  DROP COLUMN last_synced_at,
  DROP COLUMN sync_conflict,
  DROP COLUMN voided_at,
  DROP COLUMN balance;

ALTER TABLE quickbooks_accounts ALTER COLUMN current_balance TYPE BIGINT USING round(current_balance * 100);
ALTER TABLE quickbooks_accounts RENAME COLUMN current_balance TO current_balance_cents;

ALTER TABLE invoices ALTER COLUMN amount TYPE INTEGER USING round(amount * 100);
ALTER TABLE invoices RENAME COLUMN amount TO amount_cents;
ALTER TABLE invoices RENAME CONSTRAINT invoices_amount_check TO invoices_amount_cents_check;
