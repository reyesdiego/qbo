import { sql, type CommonQueryMethods } from 'slonik';
import { z } from 'zod';
import { getPool } from './db';
import { moneyFromNumber } from './money';

// Account as returned by the API (local copy of the QuickBooks chart of accounts)
export const Account = z.object({
  id: z.string(),
  name: z.string(),
  fully_qualified_name: z.string(),
  account_type: z.string(),
  account_sub_type: z.string().nullable(),
  classification: z.string().nullable(),
  current_balance: z.string(), // decimal string, e.g. "1234.56"
  currency: z.string().nullable(),
  active: z.boolean(),
  updated_at: z.string(),
});
export type Account = z.infer<typeof Account>;

// The QuickBooks account fields we use
export type QboAccount = {
  Id: string;
  SyncToken?: string;
  status?: 'Deleted'; // only set on deleted entities returned by CDC
  Name?: string;
  FullyQualifiedName?: string;
  AccountType?: string;
  AccountSubType?: string;
  Classification?: string;
  CurrentBalance?: number;
  CurrencyRef?: { value: string };
  Active?: boolean;
};

export const list = async (): Promise<readonly Account[]> => {
  const pool = await getPool();
  return pool.any(sql.type(Account)`SELECT * FROM quickbooks_accounts ORDER BY fully_qualified_name`);
};

export const isEmpty = async (): Promise<boolean> => {
  const pool = await getPool();
  return !(await pool.exists(sql.type(z.object({ id: z.string() }))`SELECT id FROM quickbooks_accounts`));
};

// Inserts or updates the account from its QuickBooks version (skipped if we already have that
// version), or removes it if deleted in QuickBooks
export const applyFromQuickBooks = async (qb: QboAccount, db?: CommonQueryMethods) => {
  const pool = db ?? (await getPool());

  if (qb.status === 'Deleted') {
    await pool.query(sql.unsafe`DELETE FROM quickbooks_accounts WHERE id = ${qb.Id}`);
    return;
  }

  await pool.query(sql.unsafe`
    INSERT INTO quickbooks_accounts
      (id, name, fully_qualified_name, account_type, account_sub_type, classification,
       current_balance, currency, active, sync_token)
    VALUES
      (${qb.Id}, ${qb.Name ?? ''}, ${qb.FullyQualifiedName ?? qb.Name ?? ''}, ${qb.AccountType ?? ''},
       ${qb.AccountSubType ?? null}, ${qb.Classification ?? null},
       ${moneyFromNumber(qb.CurrentBalance)}, ${qb.CurrencyRef?.value ?? null},
       ${qb.Active ?? true}, ${qb.SyncToken ?? ''})
    ON CONFLICT (id) DO UPDATE SET
      name = EXCLUDED.name,
      fully_qualified_name = EXCLUDED.fully_qualified_name,
      account_type = EXCLUDED.account_type,
      account_sub_type = EXCLUDED.account_sub_type,
      classification = EXCLUDED.classification,
      current_balance = EXCLUDED.current_balance,
      currency = EXCLUDED.currency,
      active = EXCLUDED.active,
      sync_token = EXCLUDED.sync_token,
      updated_at = now()
    -- Unless it's an older version. The same SyncToken is applied too: QuickBooks doesn't change it when
    -- the account's balance moves (checked in the sandbox), and the data always comes from a fresh read
    WHERE coalesce(nullif(EXCLUDED.sync_token, '')::bigint, 0) >= coalesce(nullif(quickbooks_accounts.sync_token, '')::bigint, 0)
  `);
};
