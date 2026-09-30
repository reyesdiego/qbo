-- Up Migration
-- Whether QuickBooks applies sales tax to the invoice. The local model is one line with one amount, so a
-- taxed invoice's content (amount with tax, customer, due date) is owned by QuickBooks: not editable here.
ALTER TABLE invoices ADD COLUMN taxed_in_quickbooks BOOLEAN NOT NULL DEFAULT false;

-- Down Migration
ALTER TABLE invoices DROP COLUMN taxed_in_quickbooks;
