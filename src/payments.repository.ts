import { sql } from 'slonik';
import { z } from 'zod';
import { getPool } from './db';
import { shouldFail } from './faults';
import { SyncInvoice } from './invoices.schema';
import { MoneyInput } from './money';
import { paidLocallyUnpushed } from './quickbooks.mapping';
import { enqueueJob, invoiceEntityKey } from './sync.repository';

// A payment recorded through the API, as returned by it
export const Payment = z.object({
  id: z.number(),
  invoice_id: z.number(),
  amount: z.string(), // decimal string, e.g. "30.00"
  paid_on: z.string(),
  quickbooks_id: z.string().nullable(),
  sync_status: z.enum(['pending', 'synced', 'unknown', 'failed']),
  sync_error: z.string().nullable(),
  created_at: z.string(),
});
export type Payment = z.infer<typeof Payment>;

// With the column the sync needs (deleted through the API, deletion not synced yet)
export const SyncPayment = Payment.extend({ deleted_at: z.string().nullable() });
export type SyncPayment = z.infer<typeof SyncPayment>;

export const CreatePaymentInput = z.object({
  amount: MoneyInput.refine((amount) => Number(amount) > 0, 'Must be greater than 0'),
  paid_on: z.string().date().optional(), // YYYY-MM-DD, defaults to today
});
export type CreatePaymentInput = z.infer<typeof CreatePaymentInput>;

type CreateResult =
  | { outcome: 'created' | 'existing'; payment: Payment }
  | { outcome: 'invoice_not_found' | 'invoice_void' | 'exceeds_balance' | 'key_reused'; available?: string };

// Records a payment together with its sync job, in one transaction (outbox). The invoice row is
// locked, so two concurrent payments can't together pay more than what's left. What's left is the
// invoice's current amount (including a local change not synced yet) minus its payments, synced or not
// (payments made in QuickBooks are listed too). An invoice marked paid locally has nothing left: its
// own job pays the whole balance in QuickBooks.
export const create = async (invoiceId: number, input: CreatePaymentInput, idempotencyKey?: string): Promise<CreateResult> => {
  const pool = await getPool();
  return pool.transaction(async (tx) => {
    const findExisting = () =>
      idempotencyKey ? tx.maybeOne(sql.type(Payment)`SELECT * FROM invoice_payments WHERE idempotency_key = ${idempotencyKey}`) : null;
    const existing = await findExisting();
    // The same key for another invoice is another request: not answered with this payment
    if (existing) return existing.invoice_id === invoiceId ? { outcome: 'existing', payment: existing } : { outcome: 'key_reused' };

    const invoice = await tx.maybeOne(sql.type(SyncInvoice)`
      SELECT * FROM invoices WHERE id = ${invoiceId} AND deleted_at IS NULL FOR UPDATE
    `);
    if (!invoice) return { outcome: 'invoice_not_found' };
    if (invoice.status === 'void') return { outcome: 'invoice_void' };

    const available = paidLocallyUnpushed(invoice)
      ? '0.00'
      : await tx.oneFirst(sql.type(z.object({ available: z.string() }))`
          SELECT greatest(0, ${invoice.amount}::numeric - coalesce(sum(amount), 0))::numeric(14, 2)::text AS available
          FROM invoice_payments
          WHERE invoice_id = ${invoiceId} AND deleted_at IS NULL
        `);
    if (Number(input.amount) > Number(available)) return { outcome: 'exceeds_balance', available };

    // A concurrent retry with the same key may have inserted it meanwhile (the check above ran before it
    // committed): then that one is returned, instead of failing on the unique key
    const payment = await tx.maybeOne(sql.type(Payment)`
      INSERT INTO invoice_payments (invoice_id, amount, paid_on, idempotency_key)
      VALUES (${invoiceId}, ${input.amount}, ${input.paid_on ?? sql.fragment`CURRENT_DATE`}, ${idempotencyKey ?? null})
      ON CONFLICT (idempotency_key) DO NOTHING
      RETURNING *
    `);
    if (!payment) return { outcome: 'existing', payment: (await findExisting())! };
    if (shouldFail('invoice.enqueue')) throw new Error('Injected fault: invoice.enqueue');
    // Same entity key as the invoice: it runs after the invoice's own jobs (e.g. its create)
    await enqueueJob(tx, {
      direction: 'OUTBOUND',
      entityType: 'payment',
      entityId: String(payment.id),
      entityKey: invoiceEntityKey(invoiceId),
      operation: 'create',
    });
    return { outcome: 'created', payment };
  });
};

export const listForInvoice = async (invoiceId: number): Promise<readonly Payment[]> => {
  const pool = await getPool();
  return pool.any(sql.type(Payment)`
    SELECT * FROM invoice_payments WHERE invoice_id = ${invoiceId} AND deleted_at IS NULL ORDER BY id
  `);
};

// Marks the payment as deleted and queues its deletion, in one transaction. The row stays until the
// worker has deleted it in QuickBooks (or finds it never got there); the job shares the invoice's
// entity key, so it runs after the payment's create job. A QuickBooks payment that also pays other
// invoices isn't deleted from here: that would change those invoices too.
export const remove = async (invoiceId: number, paymentId: number): Promise<'deleted' | 'not_found' | 'shared'> => {
  const pool = await getPool();
  return pool.transaction(async (tx) => {
    const payment = await tx.maybeOne(sql.type(SyncPayment)`
      SELECT * FROM invoice_payments WHERE id = ${paymentId} AND invoice_id = ${invoiceId} AND deleted_at IS NULL FOR UPDATE
    `);
    if (!payment) return 'not_found';
    if (payment.quickbooks_id) {
      const shared = await tx.exists(sql.type(z.object({ id: z.number() }))`
        SELECT id FROM invoice_payments WHERE quickbooks_id = ${payment.quickbooks_id} AND id <> ${payment.id}
      `);
      if (shared) return 'shared';
    }
    await tx.query(sql.unsafe`
      UPDATE invoice_payments SET deleted_at = now(), sync_status = 'pending', sync_error = NULL WHERE id = ${paymentId}
    `);
    await enqueueJob(tx, {
      direction: 'OUTBOUND',
      entityType: 'payment',
      entityId: String(paymentId),
      entityKey: invoiceEntityKey(invoiceId),
      operation: 'delete',
    });
    return 'deleted';
  });
};
