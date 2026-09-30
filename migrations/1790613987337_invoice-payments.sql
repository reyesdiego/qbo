-- Up Migration

-- Payments recorded through the API (full or partial). Each one is pushed to QuickBooks as a Payment
-- applied to its invoice; the invoice's balance then comes back from QuickBooks.
CREATE TABLE invoice_payments (
  id              SERIAL        PRIMARY KEY,
  invoice_id      INTEGER       NOT NULL REFERENCES invoices (id) ON DELETE CASCADE,
  amount          NUMERIC(14, 2) NOT NULL CHECK (amount > 0),
  paid_on         DATE          NOT NULL DEFAULT CURRENT_DATE,
  -- Client-provided Idempotency-Key header: a retried POST returns the payment already recorded
  idempotency_key TEXT          UNIQUE,
  quickbooks_id   TEXT,
  sync_status     TEXT          NOT NULL DEFAULT 'pending'
                  CHECK (sync_status IN ('pending', 'synced', 'unknown', 'failed')),
  sync_error      TEXT,
  created_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX invoice_payments_invoice_idx ON invoice_payments (invoice_id);
CREATE INDEX invoice_payments_quickbooks_idx ON invoice_payments (quickbooks_id);

-- Down Migration

DROP TABLE invoice_payments;
