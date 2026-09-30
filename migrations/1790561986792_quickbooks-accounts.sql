-- Up Migration

-- Local copy of the QuickBooks chart of accounts, kept up to date by webhooks and the worker
CREATE TABLE quickbooks_accounts (
  id                    TEXT        PRIMARY KEY, -- QuickBooks Id
  name                  TEXT        NOT NULL,
  fully_qualified_name  TEXT        NOT NULL,
  account_type          TEXT        NOT NULL,
  account_sub_type      TEXT,
  classification        TEXT,
  current_balance_cents BIGINT      NOT NULL DEFAULT 0,
  currency              CHAR(3),
  active                BOOLEAN     NOT NULL,
  sync_token            TEXT        NOT NULL,
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Down Migration

DROP TABLE quickbooks_accounts;
