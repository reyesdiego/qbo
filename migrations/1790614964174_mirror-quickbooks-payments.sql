-- Up Migration

-- invoice_payments also mirrors the payments made in QuickBooks: one row per invoice a payment pays
-- (a QuickBooks payment can pay several invoices)
CREATE UNIQUE INDEX invoice_payments_quickbooks_key ON invoice_payments (invoice_id, quickbooks_id)
  WHERE quickbooks_id IS NOT NULL;
DROP INDEX invoice_payments_quickbooks_idx;
CREATE INDEX invoice_payments_quickbooks_idx ON invoice_payments (quickbooks_id);

-- Down Migration

DROP INDEX invoice_payments_quickbooks_key;
