-- Up Migration

-- A payment deleted through the API stays (marked) until its deletion reaches QuickBooks
ALTER TABLE invoice_payments ADD COLUMN deleted_at TIMESTAMPTZ;

-- Down Migration

ALTER TABLE invoice_payments DROP COLUMN deleted_at;
