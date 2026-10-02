import { sql, type CommonQueryMethods } from 'slonik';
import { z } from 'zod';
import { getPool } from './db';
import { shouldFail } from './faults';
import { Invoice, type CreateInvoiceInput, type InvoiceStatus, type SyncStatus, type UpdateInvoiceInput, touchesContent } from './invoices.schema';
import { INVOICE_STATUS, SYNC_STATUS } from './statuses';
import { enqueueJob, invoiceEntityKey } from './sync.repository';

// Every local change is saved together with its sync job (transactional outbox): both commit or
// neither does, so a change can't be saved without being queued for QuickBooks.
const enqueueSync = (db: CommonQueryMethods, invoice: { id: number }, operation: 'upsert' | 'delete') => {
  if (shouldFail('invoice.enqueue')) throw new Error('Injected fault: invoice.enqueue');
  return enqueueJob(
    db,
    {
      direction: 'OUTBOUND',
      entityType: 'invoice',
      entityId: String(invoice.id),
      entityKey: invoiceEntityKey(invoice.id),
      operation,
    },
    { skipIfPending: true },
  );
};

type ListOptions = {
  status?: InvoiceStatus;
  limit: number;
  offset: number;
};

export const list = async ({ status, limit, offset }: ListOptions): Promise<readonly Invoice[]> => {
  const pool = await getPool();
  const statusFilter = status ? sql.fragment`AND status = ${status}` : sql.fragment``;

  return pool.any(sql.type(Invoice)`
    SELECT * FROM invoices
    WHERE deleted_at IS NULL ${statusFilter}
    ORDER BY id
    LIMIT ${limit} OFFSET ${offset}
  `);
};

export const findById = async (id: number): Promise<Invoice | null> => {
  const pool = await getPool();
  return pool.maybeOne(sql.type(Invoice)`
    SELECT * FROM invoices WHERE id = ${id} AND deleted_at IS NULL
  `);
};

// With an idempotency key, retrying the same request returns the invoice created the first time
// (created: false) instead of creating another one. ON CONFLICT makes this safe for concurrent
// retries too: the second insert waits for the first and then does nothing.
export const create = async (
  input: CreateInvoiceInput,
  idempotencyKey?: string,
): Promise<{ invoice: Invoice; created: boolean }> => {
  const pool = await getPool();
  return pool.transaction(async (tx) => {
    const inserted = await tx.maybeOne(sql.type(Invoice)`
      INSERT INTO invoices (customer_name, amount, balance, currency, status, due_date, idempotency_key)
      VALUES (
        ${input.customer_name},
        ${input.amount},
        ${input.amount},
        ${input.currency},
        ${input.status},
        ${input.due_date},
        ${idempotencyKey ?? null}
      )
      ON CONFLICT (idempotency_key) DO NOTHING
      RETURNING *
    `);
    if (inserted) {
      await enqueueSync(tx, inserted, 'upsert');
      return { invoice: inserted, created: true };
    }

    const existing = await tx.one(sql.type(Invoice)`
      SELECT * FROM invoices WHERE idempotency_key = ${idempotencyKey ?? null}
    `);
    return { invoice: existing, created: false };
  });
};

// Groups of live invoices with the same customer, amount and due date: likely duplicates,
// e.g. from a client that sent a new Idempotency-Key when retrying
export const findLikelyDuplicates = async () => {
  const pool = await getPool();
  return pool.any(sql.type(
    z.object({
      customer_name: z.string(),
      amount: z.string(),
      due_date: z.string(),
      invoice_ids: z.array(z.number()),
    }),
  )`
    SELECT customer_name, amount, due_date, array_agg(id ORDER BY id) AS invoice_ids
    FROM invoices
    WHERE deleted_at IS NULL
    GROUP BY customer_name, amount, due_date
    HAVING count(*) > 1
  `);
};

// Including soft-deleted ones: used to match QuickBooks invoices back to the local invoice they came from
export const findLinksByIds = async (ids: number[]) => {
  const pool = await getPool();
  return pool.any(sql.type(Invoice.pick({ id: true, created_at: true, quickbooks_id: true }))`
    SELECT id, created_at, quickbooks_id FROM invoices WHERE id = ANY(${sql.array(ids, 'int4')})
  `);
};

// A local change bumps the version and queues a sync, unless the invoice has an unresolved conflict
// (then the change is kept, and the conflict resolution decides what QuickBooks gets)
const LOCAL_CHANGE = sql.fragment`
  version = version + 1,
  updated_at = now(),
  sync_status = CASE WHEN sync_status = ${SYNC_STATUS.CONFLICT} THEN ${SYNC_STATUS.CONFLICT} ELSE ${SYNC_STATUS.PENDING} END,
  sync_error = CASE WHEN sync_status = ${SYNC_STATUS.CONFLICT} THEN sync_error ELSE NULL END
`;

// Only updates the fields that were provided. Keys are already whitelisted by zod. Returns null, changing
// nothing, if the invoice doesn't exist or can't change this way: it's voided (QuickBooks doesn't allow
// editing it), it's paid and the change un-pays it (its payment has to be deleted instead), or it's taxed
// in QuickBooks and the change touches its content (QuickBooks owns it).
export const update = async (id: number, changes: UpdateInvoiceInput): Promise<Invoice | null> => {
  const pool = await getPool();
  const assignments = Object.entries(changes)
    .filter(([, value]) => value !== undefined)
    .map(([column, value]) => sql.fragment`${sql.identifier([column])} = ${value}`);
  const unpays = changes.status === INVOICE_STATUS.DRAFT || changes.status === INVOICE_STATUS.SENT;
  const guards = [
    ...(unpays ? [sql.fragment`status <> ${INVOICE_STATUS.PAID}`] : []),
    ...(touchesContent(changes) ? [sql.fragment`NOT taxed_in_quickbooks`] : []),
  ];
  // Not in QuickBooks yet, so no payments there: the balance follows the amount
  if (changes.amount !== undefined) {
    assignments.push(sql.fragment`balance = CASE WHEN quickbooks_id IS NULL THEN ${changes.amount} ELSE balance END`);
  }

  return pool.transaction(async (tx) => {
    const invoice = await tx.maybeOne(sql.type(Invoice)`
      UPDATE invoices
      SET ${sql.join(assignments, sql.fragment`, `)}, ${LOCAL_CHANGE}
      WHERE id = ${id} AND deleted_at IS NULL AND status <> ${INVOICE_STATUS.VOID} ${guards.length ? sql.fragment`AND ${sql.join(guards, sql.fragment` AND `)}` : sql.fragment``}
      RETURNING *
    `);
    if (invoice && invoice.sync_status !== SYNC_STATUS.CONFLICT) await enqueueSync(tx, invoice, 'upsert');
    return invoice;
  });
};

// Soft delete: the row stays, so the deletion can be synced to QuickBooks
export const remove = async (id: number): Promise<boolean> => {
  const pool = await getPool();
  return pool.transaction(async (tx) => {
    const deleted = await tx.maybeOne(sql.type(Invoice.pick({ id: true, sync_status: true }))`
      UPDATE invoices
      SET deleted_at = now(), ${LOCAL_CHANGE}
      WHERE id = ${id} AND deleted_at IS NULL
      RETURNING id, sync_status
    `);
    if (deleted && deleted.sync_status !== SYNC_STATUS.CONFLICT) await enqueueSync(tx, deleted, 'delete');
    return deleted !== null;
  });
};

// Sync state shown on the invoice when its job fails, waits or can't be resolved
export const setSyncState = async (id: number, syncStatus: SyncStatus, error: string | null) => {
  const pool = await getPool();
  await pool.query(sql.unsafe`
    UPDATE invoices SET sync_status = ${syncStatus}, sync_error = ${error}
    WHERE id = ${id} AND sync_status <> ${SYNC_STATUS.CONFLICT}
  `);
};
