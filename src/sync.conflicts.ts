// Resolving a conflict (an invoice changed locally and in QuickBooks, differently):
// - keep "remote": the local invoice takes QuickBooks' content. Anything else changed locally meanwhile
//   (a deletion, a void, "paid", "sent") is still pushed: an outbound job checks it against QuickBooks
// - keep "local": QuickBooks' version becomes the base, so the local version is pushed as a normal
//   change (and if QuickBooks changed again meanwhile, that's detected as a new conflict)
import { sql } from 'slonik';
import { getPool } from './db';
import { SyncInvoice } from './invoices.schema';
import type { ConflictDetails } from './sync.outbound';
import { enqueueInvoicePush } from './sync.repository';

export const resolveConflict = async (invoiceId: number, keep: 'local' | 'remote') => {
  const pool = await getPool();
  return pool.transaction(async (tx) => {
    const invoice = await tx.maybeOne(sql.type(SyncInvoice)`SELECT * FROM invoices WHERE id = ${invoiceId} FOR UPDATE`);
    if (!invoice) return 'not_found' as const;
    if (invoice.sync_status !== 'conflict' || !invoice.sync_conflict) return 'no_conflict' as const;

    const { remote } = invoice.sync_conflict as Pick<ConflictDetails, 'remote'>;
    const content = keep === 'remote'
      ? sql.fragment`customer_name = ${remote.customer_name}, amount = ${remote.amount}, currency = ${remote.currency},
          due_date = ${remote.due_date}, updated_at = now(),`
      : sql.fragment``;
    await tx.query(sql.unsafe`
      UPDATE invoices
      SET ${content} last_synced_snapshot = ${JSON.stringify(remote)}::jsonb, version = version + 1,
          sync_status = 'pending', sync_conflict = NULL, sync_error = NULL
      WHERE id = ${invoiceId}
    `);
    await enqueueInvoicePush(tx, invoiceId);
    return 'resolved' as const;
  });
};
