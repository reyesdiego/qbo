// OUTBOUND payments: a payment recorded through the API -> a QuickBooks Payment applied to the invoice,
// and a payment deleted through the API -> deleted in QuickBooks.
//
// Like an invoice create, a payment must never be created twice: it carries a stable reference (sent as
// the requestid and written in its PrivateNote), the job records create_sent_at right before sending it,
// and a lost response makes the job UNKNOWN until reconciliation finds out whether QuickBooks has it.
import { sql, type CommonQueryMethods } from 'slonik';
import { z } from 'zod';
import { getPool } from './db';
import { AmbiguousWriteError, classifyError, errorMessage, NotConnectedError, PermanentError } from './errors';
import { shouldFail } from './faults';
import { SyncInvoice } from './invoices.schema';
import { SyncPayment } from './payments.repository';
import { moneyToNumber } from './money';
import { quote, type QboApi } from './quickbooks.client';
import {
  localPaymentKey,
  paidInvoiceLines,
  parseLocalPaymentKey,
  remoteBalance,
  syncedStatus,
  type QboInvoice,
  type QboPayment,
} from './quickbooks.mapping';
import { fetchInvoice } from './sync.outbound';
import { completeJob, markCreateSent, type SyncJob } from './sync.repository';

type QboPaymentRecord = { Id: string; PrivateNote?: string };

const loadPayment = (db: CommonQueryMethods, id: number) =>
  db.maybeOne(sql.type(SyncPayment)`SELECT * FROM invoice_payments WHERE id = ${id}`);

// The invoice's balance and paid status after a payment changed in QuickBooks. The SyncToken is left
// alone: the fetched version may also carry an edit made in QuickBooks, which only the inbound job
// applies (it would skip it as "already up to date" if the token moved here).
const refreshInvoiceBalance = async (db: CommonQueryMethods, invoiceId: number, invoice: QboInvoice) => {
  const current = await db.maybeOne(sql.type(SyncInvoice)`SELECT * FROM invoices WHERE id = ${invoiceId} FOR UPDATE`);
  if (!current) return;
  const balance = remoteBalance(invoice);
  await db.query(sql.unsafe`
    UPDATE invoices SET balance = ${balance}, status = ${syncedStatus(current, invoice, balance, current.amount)}
    WHERE id = ${current.id}
  `);
};

// Saves the QuickBooks payment id and the invoice's new balance, and completes the job, in one transaction
const saveSyncedPayment = async (job: SyncJob, payment: SyncPayment, quickbooksPaymentId: string, invoice: QboInvoice | null, result: string) => {
  const pool = await getPool();
  await pool.transaction(async (tx) => {
    await tx.query(sql.unsafe`
      UPDATE invoice_payments SET quickbooks_id = ${quickbooksPaymentId}, sync_status = 'synced', sync_error = NULL
      WHERE id = ${payment.id}
    `);
    if (invoice) await refreshInvoiceBalance(tx, payment.invoice_id, invoice);
    await completeJob(tx, job, result);
  });
};

// Deleting is idempotent, so no UNKNOWN state is needed: if a retry finds the payment gone from
// QuickBooks, the deletion already happened
const deletePayment = async (job: SyncJob, payment: SyncPayment, qbo: QboApi) => {
  const pool = await getPool();
  if (!payment.quickbooks_id) {
    // Never reached QuickBooks (its create job ran first and skipped it): nothing to delete there
    return pool.transaction(async (tx) => {
      await tx.query(sql.unsafe`DELETE FROM invoice_payments WHERE id = ${payment.id}`);
      await completeJob(tx, job, 'never reached QuickBooks: deleted locally');
    });
  }

  let result = 'deleted in QuickBooks';
  try {
    const current = (await qbo.request(() => `payment/${payment.quickbooks_id}`)).Payment;
    await qbo.request(() => 'payment', {
      method: 'POST',
      params: { operation: 'delete' },
      body: { Id: payment.quickbooks_id, SyncToken: current.SyncToken },
    });
  } catch (err) {
    if (classifyError(err).errorClass !== 'not_found') throw err;
    result = 'already deleted in QuickBooks';
  }

  const invoice = await pool.maybeOne(sql.type(SyncInvoice)`SELECT * FROM invoices WHERE id = ${payment.invoice_id}`);
  const remoteInvoice = invoice?.quickbooks_id ? await fetchInvoice(qbo, invoice.quickbooks_id) : null;
  await pool.transaction(async (tx) => {
    await tx.query(sql.unsafe`DELETE FROM invoice_payments WHERE id = ${payment.id}`);
    if (remoteInvoice) await refreshInvoiceBalance(tx, payment.invoice_id, remoteInvoice);
    await completeJob(tx, job, result);
  });
};

export const processOutboundPayment = async (job: SyncJob, qbo: QboApi) => {
  const pool = await getPool();
  const payment = await loadPayment(pool, Number(job.entity_id));
  if (!payment) return completeJob(pool, job, 'payment no longer exists');
  if (job.operation === 'delete') return deletePayment(job, payment, qbo);
  if (payment.deleted_at) return completeJob(pool, job, 'deleted before reaching QuickBooks');
  if (payment.quickbooks_id) return completeJob(pool, job, 'already in QuickBooks');

  const invoice = await pool.maybeOne(sql.type(SyncInvoice)`SELECT * FROM invoices WHERE id = ${payment.invoice_id}`);
  if (!invoice) return completeJob(pool, job, 'invoice no longer exists');
  if (invoice.deleted_at) {
    // Deleted locally before its payment was sent: the payment goes with it (sending it would also make
    // QuickBooks void the invoice instead of deleting it)
    return pool.transaction(async (tx) => {
      await tx.query(sql.unsafe`DELETE FROM invoice_payments WHERE id = ${payment.id}`);
      await completeJob(tx, job, 'invoice deleted: payment not sent');
    });
  }
  const realmId = await qbo.realmId();
  if (!realmId) throw new NotConnectedError();
  // The invoice's own jobs run first; if its create failed, retry later (and FAILED eventually)
  if (!invoice.quickbooks_id) throw new Error('The invoice is not in QuickBooks yet');
  if (invoice.quickbooks_realm_id !== realmId) throw new PermanentError('The invoice belongs to another QuickBooks company');

  const remote = await fetchInvoice(qbo, invoice.quickbooks_id);
  const balance = remote.Balance ?? remote.TotalAmt ?? 0;
  if (moneyToNumber(payment.amount) > balance) {
    throw new PermanentError(`The payment (${payment.amount}) exceeds the invoice's balance in QuickBooks (${balance.toFixed(2)})`);
  }

  const reference = localPaymentKey(payment);
  const customerId = remote.CustomerRef?.value;
  // From here on, if we don't get the response we can't tell whether QuickBooks recorded the payment
  await markCreateSent(job, { reference, customer_id: customerId });

  let created: QboPaymentRecord;
  try {
    const data = await qbo.request(() => 'payment', {
      method: 'POST',
      params: { requestid: reference },
      body: {
        CustomerRef: { value: customerId },
        TotalAmt: moneyToNumber(payment.amount),
        TxnDate: payment.paid_on,
        PrivateNote: `Payment from local payment #${payment.id} (${reference})`,
        Line: [{ Amount: moneyToNumber(payment.amount), LinkedTxn: [{ TxnId: remote.Id, TxnType: 'Invoice' }] }],
      },
    });
    if (shouldFail('qbo.payment.response-lost')) throw new AmbiguousWriteError('Injected fault: payment response lost');
    created = data.Payment;
  } catch (err) {
    if (err instanceof AmbiguousWriteError || classifyError(err).ambiguous) {
      throw new AmbiguousWriteError(`Payment result unknown (${errorMessage(err)}); reference ${reference}`);
    }
    throw err;
  }
  // The payment exists now: its id is saved even if reading the invoice's new balance fails (a retry would
  // see the balance already paid and reject the payment). The balance then comes with QuickBooks' notification.
  const invoiceAfter = await fetchInvoice(qbo, remote.Id).catch(() => null);
  await saveSyncedPayment(job, payment, created.Id, invoiceAfter, 'payment recorded in QuickBooks');
};

// Reconciliation of an UNKNOWN payment: search the customer's recent payments for its reference.
// Returns what happened, so reconciliation can update the job.
export const reconcileUnknownPayment = async (job: SyncJob, qbo: QboApi, since: string) => {
  const pool = await getPool();
  const payment = await loadPayment(pool, Number(job.entity_id));
  if (!payment) {
    await completeJob(pool, job, 'payment no longer exists');
    return 'resolved' as const;
  }

  const reference = String(job.payload.reference ?? '');
  const customerId = String(job.payload.customer_id ?? '');
  const data = await qbo.query(
    `SELECT * FROM Payment WHERE CustomerRef = ${quote(customerId)} AND MetaData.CreateTime >= ${quote(since)}`,
  );
  const matches = ((data.QueryResponse.Payment ?? []) as QboPaymentRecord[]).filter((p) => p.PrivateNote?.includes(reference));

  if (matches.length === 1) {
    const invoice = await pool.one(sql.type(SyncInvoice)`SELECT * FROM invoices WHERE id = ${payment.invoice_id}`);
    await saveSyncedPayment(job, payment, matches[0].Id, await fetchInvoice(qbo, invoice.quickbooks_id!), 'reconciled: found in QuickBooks');
    return 'resolved' as const;
  }
  return matches.length === 0 ? ('not_found' as const) : ('ambiguous' as const);
};

export const setPaymentSyncState = async (paymentId: number, syncStatus: 'pending' | 'unknown' | 'failed', error: string | null) => {
  const pool = await getPool();
  await pool.query(sql.unsafe`
    UPDATE invoice_payments SET sync_status = ${syncStatus}, sync_error = ${error} WHERE id = ${paymentId}
  `);
};

// A payment deleted in QuickBooks is deleted locally too, for every invoice it paid (their balances
// come back with the invoices' own notifications)
export const deleteLocalPayment = async (db: CommonQueryMethods, quickbooksPaymentId: string) => {
  const deleted = await db.any(sql.type(z.object({ id: z.number() }))`
    DELETE FROM invoice_payments WHERE quickbooks_id = ${quickbooksPaymentId} RETURNING id
  `);
  return deleted.map((row) => row.id);
};

// Lists the invoice's QuickBooks payments that aren't listed locally yet (e.g. after the invoice was
// imported from QuickBooks, or re-imported by the consistency check). Returns how many were added.
export const mirrorMissingPayments = async (qbo: QboApi, realmId: string, invoice: QboInvoice) => {
  const paymentIds = (invoice.LinkedTxn ?? []).filter((txn) => txn.TxnType === 'Payment').map((txn) => txn.TxnId);
  if (!paymentIds.length) return 0;
  const pool = await getPool();
  const listed = await pool.any(sql.type(z.object({ quickbooks_id: z.string() }))`
    SELECT p.quickbooks_id FROM invoice_payments p JOIN invoices i ON i.id = p.invoice_id
    WHERE i.quickbooks_realm_id = ${realmId} AND i.quickbooks_id = ${invoice.Id}
      AND p.quickbooks_id = ANY(${sql.array(paymentIds, 'text')})
  `);
  const missing = paymentIds.filter((id) => !listed.some((row) => row.quickbooks_id === id));
  for (const id of missing) {
    const payment: QboPayment = (await qbo.request(() => `payment/${id}`)).Payment;
    await pool.transaction((tx) => mirrorQuickBooksPayment(tx, realmId, payment));
  }
  return missing.length;
};

// Mirrors a QuickBooks payment in invoice_payments: one row per local invoice it pays, with that
// invoice's share. A payment recorded here (our reference in its PrivateNote) links its existing row
// instead of adding one, even if QuickBooks notifies it before the worker saved its id. Invoices the
// payment no longer pays lose their row.
export const mirrorQuickBooksPayment = async (db: CommonQueryMethods, realmId: string, payment: QboPayment) => {
  const reference = parseLocalPaymentKey(payment.PrivateNote);
  const paidInvoices: number[] = [];

  for (const line of paidInvoiceLines(payment)) {
    const invoice = await db.maybeOne(sql.type(z.object({ id: z.number() }))`
      SELECT id FROM invoices WHERE quickbooks_realm_id = ${realmId} AND quickbooks_id = ${line.invoiceId}
    `);
    if (!invoice) continue; // an invoice we don't have locally
    paidInvoices.push(invoice.id);

    if (reference) {
      const origin = await db.maybeOne(sql.type(SyncPayment)`
        SELECT * FROM invoice_payments WHERE id = ${reference.id} AND invoice_id = ${invoice.id} FOR UPDATE
      `);
      if (origin && localPaymentKey(origin) === reference.key) {
        await db.query(sql.unsafe`
          UPDATE invoice_payments
          SET quickbooks_id = ${payment.Id}, amount = ${line.amount},
              sync_status = CASE WHEN deleted_at IS NULL THEN 'synced' ELSE sync_status END, sync_error = NULL
          WHERE id = ${origin.id}
        `);
        continue;
      }
    }

    await db.query(sql.unsafe`
      INSERT INTO invoice_payments (invoice_id, amount, paid_on, quickbooks_id, sync_status)
      VALUES (${invoice.id}, ${line.amount}, ${payment.TxnDate ?? sql.fragment`CURRENT_DATE`}, ${payment.Id}, 'synced')
      ON CONFLICT (invoice_id, quickbooks_id) WHERE quickbooks_id IS NOT NULL
      DO UPDATE SET amount = EXCLUDED.amount, paid_on = EXCLUDED.paid_on
    `);
  }

  await db.query(sql.unsafe`
    DELETE FROM invoice_payments
    WHERE quickbooks_id = ${payment.Id} AND NOT (invoice_id = ANY(${sql.array(paidInvoices, 'int4')}))
  `);
  return paidInvoices.length;
};
