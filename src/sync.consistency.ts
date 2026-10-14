// Consistency check (anti-entropy): compares every invoice in QuickBooks with the local invoices linked to
// it, and repairs the differences through the normal sync queue. It catches what the other paths can't:
// a row deleted directly in the database, a restore from an old backup, a bug, or a change older than the
// 30 days CDC keeps. Run by the worker every SYNC_CONSISTENCY_INTERVAL_HOURS, and on demand with
// POST /sync/consistency-check.
import { randomUUID } from 'node:crypto';
import { sql } from 'slonik';
import { z } from 'zod';
import { getPool } from './db';
import { classifyError, NotConnectedError } from './errors';
import type { QboApi } from './quickbooks.client';
import { recordEventAndEnqueue } from './sync.repository';

const PAGE_SIZE = 1000; // QuickBooks' maximum

type Options = { repair?: boolean };

export const checkConsistency = async (qbo: QboApi, { repair = true }: Options = {}) => {
  const realmId = await qbo.realmId();
  if (!realmId) throw new NotConnectedError();
  const pool = await getPool();

  // Local first: an invoice the worker links to QuickBooks while this runs isn't in this list yet,
  // so it can't be mistaken for one that disappeared from QuickBooks
  const local = await pool.any(sql.type(z.object({ id: z.number(), quickbooks_id: z.string(), deleted_at: z.string().nullable() }))`
    SELECT id, quickbooks_id, deleted_at FROM invoices WHERE quickbooks_realm_id = ${realmId}
  `);

  // Every QuickBooks invoice, only Id and SyncToken (cheap)
  const remote = new Map<string, string>();
  for (let start = 1; ; start += PAGE_SIZE) {
    const data = await qbo.query(`SELECT Id, SyncToken FROM Invoice STARTPOSITION ${start} MAXRESULTS ${PAGE_SIZE}`);
    const page: { Id: string; SyncToken: string }[] = data.QueryResponse.Invoice ?? [];
    for (const invoice of page) remote.set(invoice.Id, invoice.SyncToken);
    if (page.length < PAGE_SIZE) break;
  }

  const linked = new Set(local.map((invoice) => invoice.quickbooks_id));
  const missingLocally = [...remote.keys()].filter((id) => !linked.has(id));

  // Not in QuickBooks' list: confirmed with a direct GET before treating it as deleted (two signals,
  // never one failed request; the list can also lag right after a create)
  const missingInQuickBooks: { invoice_id: number; quickbooks_id: string }[] = [];
  for (const invoice of local.filter((l) => !l.deleted_at && !remote.has(l.quickbooks_id))) {
    try {
      await qbo.request(() => `invoice/${invoice.quickbooks_id}`);
    } catch (err) {
      if (classifyError(err).errorClass !== 'not_found') throw err;
      missingInQuickBooks.push({ invoice_id: invoice.id, quickbooks_id: invoice.quickbooks_id });
    }
  }

  // Every QuickBooks payment id (paginated). Like invoices above, a missing id is confirmed
  // with a direct GET before treating it as deleted.
  const remotePayments = new Set<string>();
  for (let start = 1; ; start += PAGE_SIZE) {
    const data = await qbo.query(`SELECT * FROM Payment STARTPOSITION ${start} MAXRESULTS ${PAGE_SIZE}`);
    const page: { Id: string }[] = data.QueryResponse.Payment ?? [];
    for (const payment of page) remotePayments.add(payment.Id);
    if (page.length < PAGE_SIZE) break;
  }

  const localPayments = await pool.any(sql.type(z.object({
    quickbooks_id: z.string(),
    invoice_id: z.number(),
    invoice_quickbooks_id: z.string(),
  }))`
    SELECT p.quickbooks_id, p.invoice_id, i.quickbooks_id AS invoice_quickbooks_id
    FROM invoice_payments p JOIN invoices i ON i.id = p.invoice_id
    WHERE i.quickbooks_realm_id = ${realmId} AND p.quickbooks_id IS NOT NULL AND p.deleted_at IS NULL
  `);

  // A payment deleted in QuickBooks while disconnected (CDC can't list it anymore): confirmed
  // with a direct GET, like invoices above.
  const missingPaymentsInQuickBooks: { quickbooks_id: string; invoice_id: number; invoice_quickbooks_id: string }[] = [];
  for (const payment of localPayments.filter((p) => !remotePayments.has(p.quickbooks_id))) {
    try {
      await qbo.request(() => `payment/${payment.quickbooks_id}`);
    } catch (err) {
      if (classifyError(err).errorClass !== 'not_found') throw err;
      missingPaymentsInQuickBooks.push(payment);
    }
  }

  // Repairs go through the queue, exactly like notifications: the worker imports what's missing
  // (with its payments) and deletes what's gone. Keys are unique per run, so a later run can repair
  // the same invoice again if needed.
  let jobsEnqueued = 0;
  if (repair) {
    const run = randomUUID();
    const enqueue = (entity: 'invoice' | 'payment', id: string, operation: 'update' | 'delete', reason: string) =>
      pool.transaction((tx) =>
        recordEventAndEnqueue(tx, {
          source: 'reconciliation',
          realmId,
          entity,
          id,
          operation,
          eventKey: `consistency:${run}:${entity}:${id}:${operation}`,
          payload: { reason },
        }),
      );
    for (const id of missingLocally) {
      if ((await enqueue('invoice', id, 'update', 'missing locally')) === 'enqueued') jobsEnqueued++;
    }
    for (const { quickbooks_id } of missingInQuickBooks) {
      if ((await enqueue('invoice', quickbooks_id, 'delete', 'not in QuickBooks')) === 'enqueued') jobsEnqueued++;
    }
    for (const { quickbooks_id, invoice_quickbooks_id } of missingPaymentsInQuickBooks) {
      if ((await enqueue('payment', quickbooks_id, 'delete', 'payment not in QuickBooks')) === 'enqueued') jobsEnqueued++;
      // The invoice's balance changed with the deletion: refresh it like its own notification would
      if ((await enqueue('invoice', invoice_quickbooks_id, 'update', 'payment deleted in QuickBooks')) === 'enqueued') jobsEnqueued++;
    }
  }

  return {
    quickbooks_invoices: remote.size,
    local_linked_invoices: local.length,
    missing_locally: missingLocally,
    missing_in_quickbooks: missingInQuickBooks,
    missing_payments_in_quickbooks: missingPaymentsInQuickBooks.map(({ quickbooks_id, invoice_id }) => ({ quickbooks_id, invoice_id })),
    repaired: repair,
    jobs_enqueued: jobsEnqueued,
  };
};
