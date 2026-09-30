// OUTBOUND: local invoice changes -> QuickBooks.
//
// The job only says which invoice changed; the worker always loads the invoice's latest state, so
// several quick edits end up as one QuickBooks update. HTTP calls happen outside any transaction;
// the result is saved together with the job completion in one short transaction.
import { createHash } from 'node:crypto';
import { sql, type CommonQueryMethods } from 'slonik';
import { z } from 'zod';
import { getPool } from './db';
import { AmbiguousWriteError, classifyError, errorMessage, NotConnectedError, PermanentError } from './errors';
import { shouldFail } from './faults';
import { SyncInvoice, type Snapshot } from './invoices.schema';
import { moneyToNumber } from './money';
import { quote, type QboApi } from './quickbooks.client';
import {
  changedFields,
  isNewerToken,
  isTaxedInQuickBooks,
  localInvoiceKey,
  localSnapshot,
  paidLocallyUnpushed,
  remoteBalance,
  statusLeftToPush,
  isSentInQuickBooks,
  remoteSnapshot,
  sameSnapshot,
  syncedStatus,
  type QboInvoice,
} from './quickbooks.mapping';
import { applyRemoteInvoice } from './sync.inbound';
import { completeJob, enqueueInvoicePush, markCreateSent, type SyncJob } from './sync.repository';

const loadInvoice = (db: CommonQueryMethods, id: number) =>
  db.maybeOne(sql.type(SyncInvoice)`SELECT * FROM invoices WHERE id = ${id}`);

export const fetchInvoice = async (qbo: QboApi, id: string): Promise<QboInvoice> =>
  (await qbo.request(() => `invoice/${id}`)).Invoice;

const complete = async (job: SyncJob, result: string) => {
  const pool = await getPool();
  await completeJob(pool, job, result);
};

// ---------------------------------------------------------------------------
// Customers and items (a QuickBooks invoice needs both)
// ---------------------------------------------------------------------------

const findOrCreateCustomer = async (qbo: QboApi, displayName: string): Promise<string> => {
  const found = await qbo.query(`SELECT Id FROM Customer WHERE DisplayName = ${quote(displayName)}`);
  const existing = found.QueryResponse.Customer?.[0];
  if (existing) return existing.Id;

  // Idempotency key per name, so retries and concurrent creates make one customer.
  // Hashed because names can exceed the 50 char limit.
  const requestid = `customer-${createHash('sha256').update(displayName).digest('hex').slice(0, 40)}`;
  const created = await qbo.request(() => 'customer', {
    method: 'POST',
    params: { requestid },
    body: { DisplayName: displayName },
  });
  return created.Customer.Id;
};

// A QuickBooks invoice line needs an item: use the company's first service item
const getServiceItemId = async (qbo: QboApi): Promise<string> => {
  const items = await qbo.query(`SELECT Id FROM Item WHERE Type = 'Service' MAXRESULTS 1`);
  const itemId = items.QueryResponse.Item?.[0]?.Id;
  if (!itemId) throw new PermanentError('The QuickBooks company has no service item to invoice with');
  return itemId;
};

// The invoice content we own: customer, due date and one line with the amount
const invoiceFields = async (qbo: QboApi, invoice: SyncInvoice) => {
  const amount = moneyToNumber(invoice.amount);
  return {
    CustomerRef: { value: await findOrCreateCustomer(qbo, invoice.customer_name) },
    DueDate: invoice.due_date,
    Line: [
      {
        DetailType: 'SalesItemLineDetail',
        Amount: amount,
        Description: `Invoice #${invoice.id}`,
        SalesItemLineDetail: { ItemRef: { value: await getServiceItemId(qbo) }, Qty: 1, UnitPrice: amount },
      },
    ],
  };
};

// Marked paid locally (not a "paid" that came from QuickBooks) while QuickBooks still shows a balance to pay
const needsPayment = (invoice: SyncInvoice, qb: QboInvoice) => paidLocallyUnpushed(invoice) && statusLeftToPush('paid', qb);

// A payment recorded for a "paid" invoice, and the invoice as QuickBooks has it afterwards
type RecordedPayment = { payment: { quickbooksId: string; amount: string }; invoice: QboInvoice };

// Records a payment in QuickBooks for the invoice's whole balance; returns the payment (saved in
// invoice_payments, so it can be deleted from here like any payment) and the invoice afterwards.
// Safe to retry: the balance is always read before paying (a payment that went through isn't
// repeated), and the requestid makes QuickBooks return the same payment for a resent request. It
// includes the local version, so paying again after a payment was deleted in QuickBooks works.
const recordPayment = async (qbo: QboApi, invoice: SyncInvoice, qb: QboInvoice): Promise<RecordedPayment> => {
  const amount = qb.Balance ?? qb.TotalAmt ?? 0;
  const data = await qbo.request(() => 'payment', {
    method: 'POST',
    params: { requestid: `${localInvoiceKey(invoice)}-pay${invoice.version}` },
    body: {
      CustomerRef: { value: qb.CustomerRef?.value },
      TotalAmt: amount,
      Line: [{ Amount: amount, LinkedTxn: [{ TxnId: qb.Id, TxnType: 'Invoice' }] }],
    },
  });
  return { payment: { quickbooksId: data.Payment.Id, amount: amount.toFixed(2) }, invoice: await fetchInvoice(qbo, qb.Id) };
};

// ---------------------------------------------------------------------------
// Saving results (short transactions, together with the job completion)
// ---------------------------------------------------------------------------

// Records that QuickBooks now has the invoice as `qb`, the result of our own write (or the version we
// found matching). syncedVersion is the local version that matches it; if the invoice changed locally
// since, it stays pending and gets a job. With a payment recorded, the balance and status come from the
// invoice read after it, but the SyncToken and snapshot don't: that read may carry an edit made in
// QuickBooks meanwhile, which only the inbound job applies.
export const saveSynced = async (
  job: SyncJob,
  invoiceId: number,
  qb: QboInvoice,
  realmId: string,
  syncedVersion: number,
  result: string,
  paid?: RecordedPayment,
) => {
  const pool = await getPool();
  await pool.transaction(async (tx) => {
    const current = await tx.one(sql.type(SyncInvoice)`SELECT * FROM invoices WHERE id = ${invoiceId} FOR UPDATE`);
    if (paid) {
      await tx.query(sql.unsafe`
        INSERT INTO invoice_payments (invoice_id, amount, quickbooks_id, sync_status)
        VALUES (${invoiceId}, ${paid.payment.amount}, ${paid.payment.quickbooksId}, 'synced')
        ON CONFLICT (invoice_id, quickbooks_id) WHERE quickbooks_id IS NOT NULL DO NOTHING
      `); // already there if QuickBooks' notification of it was faster
    }
    const balanceFrom = paid?.invoice ?? qb;
    const balance = remoteBalance(balanceFrom);
    // Never go back to an older SyncToken (e.g. a payment was applied while we were pushing)
    const syncToken = isNewerToken(qb.SyncToken, current.quickbooks_sync_token) ? qb.SyncToken : current.quickbooks_sync_token;
    const upToDate = current.version === syncedVersion;
    // Taxed in QuickBooks: its content is QuickBooks' (e.g. the amount with the tax it added)
    const taxed = isTaxedInQuickBooks(qb);
    const theirs = remoteSnapshot(qb);
    const content = taxed
      ? sql.fragment`customer_name = ${theirs.customer_name}, amount = ${theirs.amount}, currency = ${theirs.currency}, due_date = ${theirs.due_date},`
      : sql.fragment``;

    await tx.query(sql.unsafe`
      UPDATE invoices
      SET ${content} taxed_in_quickbooks = ${taxed},
          quickbooks_realm_id = ${realmId}, quickbooks_id = ${qb.Id}, quickbooks_sync_token = ${syncToken},
          last_synced_snapshot = ${JSON.stringify(remoteSnapshot(qb))}::jsonb, last_synced_at = now(),
          synced_version = ${syncedVersion},
          balance = ${balance}, status = ${syncedStatus(current, balanceFrom, balance, current.amount)},
          sync_status = ${upToDate ? 'synced' : 'pending'}, sync_error = NULL
      WHERE id = ${invoiceId}
    `);
    await completeJob(tx, job, result);
    if (!upToDate) await enqueueInvoicePush(tx, invoiceId);
  });
};

export type ConflictDetails = {
  reason: string;
  fields: string[];
  local: Snapshot;
  remote: Snapshot;
  base: Snapshot | null;
  remote_sync_token: string | null;
};

// Both sides changed the invoice differently: keep both versions and stop syncing it until someone
// resolves it (POST /sync/conflicts/:invoiceId/resolve). Financial data is never overwritten silently.
export const saveConflict = async (db: CommonQueryMethods, invoiceId: number, details: ConflictDetails) => {
  await db.query(sql.unsafe`
    UPDATE invoices
    SET sync_status = 'conflict',
        sync_conflict = ${JSON.stringify({ ...details, detected_at: new Date().toISOString() })}::jsonb,
        sync_error = ${`${details.reason}${details.fields.length ? `: ${details.fields.join(', ')}` : ''}`}
    WHERE id = ${invoiceId}
  `);
};

// Nothing (more) to push for this version: deleted or voided in QuickBooks, never there, or a change
// that can't be pushed (a voided invoice isn't editable)
const saveDone = async (job: SyncJob, invoice: SyncInvoice, voided: boolean, result: string) => {
  const pool = await getPool();
  await pool.transaction(async (tx) => {
    const saved = await tx.maybeOne(sql.type(z.object({ version: z.number() }))`
      UPDATE invoices
      SET voided_at = CASE WHEN ${voided} THEN now() ELSE voided_at END,
          status = CASE WHEN ${voided} THEN 'void' ELSE status END,
          synced_version = ${invoice.version}, last_synced_at = now(), sync_error = NULL,
          sync_status = CASE WHEN version = ${invoice.version} THEN 'synced' ELSE 'pending' END
      WHERE id = ${invoice.id}
      RETURNING version
    `);
    await completeJob(tx, job, result);
    await pushIfChanged(tx, invoice.id, saved?.version, invoice.version);
  });
};

// A job only pushes the version it loaded. If the invoice changed meanwhile, the API may not have queued
// a job for it (it saw this one still waiting), so it's queued here
const pushIfChanged = async (tx: CommonQueryMethods, invoiceId: number, currentVersion: number | undefined, loadedVersion: number) => {
  if (currentVersion !== undefined && currentVersion > loadedVersion) await enqueueInvoicePush(tx, invoiceId);
};

// ---------------------------------------------------------------------------
// Create, update, delete/void
// ---------------------------------------------------------------------------

const createInQuickBooks = async (job: SyncJob, invoice: SyncInvoice, realmId: string, qbo: QboApi) => {
  const fields = await invoiceFields(qbo, invoice);
  const reference = localInvoiceKey(invoice);

  // From here on, if we don't get the response we can't tell whether QuickBooks created the invoice
  await markCreateSent(job, { reference, customer_id: fields.CustomerRef.value });

  let created: QboInvoice;
  try {
    const data = await qbo.request(() => 'invoice', {
      method: 'POST',
      params: { requestid: reference },
      body: {
        ...fields,
        CurrencyRef: { value: invoice.currency },
        PrivateNote: `Created from local invoice #${invoice.id} (${reference})`,
        ...(invoice.status === 'sent' ? { EmailStatus: 'EmailSent' } : {}),
      },
    });
    if (shouldFail('qbo.create.response-lost')) throw new AmbiguousWriteError('Injected fault: create response lost');
    created = data.Invoice;
  } catch (err) {
    // Timeouts, dropped connections, 5xx: the invoice may exist. Don't resend; reconcile.
    if (err instanceof AmbiguousWriteError || classifyError(err).ambiguous) {
      throw new AmbiguousWriteError(`Create result unknown (${errorMessage(err)}); reference ${reference}`);
    }
    throw err;
  }
  // Created as paid: record its payment too (if this fails, the retry gets the same invoice back
  // through the create's requestid, and pays it then)
  const paid = needsPayment(invoice, created) ? await recordPayment(qbo, invoice, created) : undefined;
  await saveSynced(job, invoice.id, created, realmId, invoice.version, paid ? 'created in QuickBooks, payment recorded' : 'created in QuickBooks', paid);
};

const updateInQuickBooks = async (job: SyncJob, invoice: SyncInvoice, realmId: string, qbo: QboApi): Promise<unknown> => {
  const remote = await fetchInvoice(qbo, invoice.quickbooks_id!);
  const local = localSnapshot(invoice);
  const theirs = remoteSnapshot(remote);
  const base = invoice.last_synced_snapshot;
  const contentChanged = !sameSnapshot(theirs, local);
  // Sent locally but not in QuickBooks yet ("sent" only moves forward, it's never unset)
  const markSent = invoice.status === 'sent' && !isSentInQuickBooks(remote);

  const remoteOwnsContent = !base || sameSnapshot(local, base) || isTaxedInQuickBooks(remote);
  if (contentChanged && base && remoteOwnsContent && isNewerToken(remote.SyncToken, invoice.quickbooks_sync_token)) {
    // Only QuickBooks changed the content (its notification isn't applied yet), and the local change is
    // something else, e.g. a status; or it's taxed there, so its content is QuickBooks'. Take QuickBooks'
    // content as the inbound job would, then push the rest against it. Not a conflict.
    await applyRemoteInvoice(null, realmId, remote);
    return processOutboundInvoice(job, qbo);
  }
  if (contentChanged && base && !sameSnapshot(theirs, base)) {
    // Also changed in QuickBooks since the last sync, and differently: don't overwrite it
    const pool = await getPool();
    return pool.transaction(async (tx) => {
      await saveConflict(tx, invoice.id, {
        reason: 'Changed locally and in QuickBooks',
        fields: changedFields(local, theirs),
        local,
        remote: theirs,
        base,
        remote_sync_token: remote.SyncToken,
      });
      await completeJob(tx, job, 'conflict registered');
    });
  }
  if (contentChanged && theirs.currency !== local.currency) {
    throw new PermanentError('The currency of an invoice already in QuickBooks cannot be changed');
  }

  let latest = remote;
  const done: string[] = [];
  if (contentChanged || markSent) {
    // Sparse update with QuickBooks' current SyncToken. If the invoice changes in between, QuickBooks
    // rejects it (stale object) and the job is retried: the next attempt fetches the new version.
    const data = await qbo.request(() => 'invoice', {
      method: 'POST',
      body: {
        Id: remote.Id,
        SyncToken: remote.SyncToken,
        sparse: true,
        ...(contentChanged ? await invoiceFields(qbo, invoice) : {}),
        ...(markSent ? { EmailStatus: 'EmailSent' } : {}),
      },
    });
    latest = data.Invoice;
    done.push(contentChanged ? 'updated in QuickBooks' : 'marked as sent in QuickBooks');
  }
  const paid = needsPayment(invoice, latest) ? await recordPayment(qbo, invoice, latest) : undefined;
  if (paid) done.push('payment recorded in QuickBooks');
  await saveSynced(job, invoice.id, latest, realmId, invoice.version, done.join(', ') || 'QuickBooks already matched', paid);
};

// QuickBooks can't delete an invoice with payments applied: void it instead (keeps the payment history)
const removeFromQuickBooks = async (job: SyncJob, invoice: SyncInvoice, qbo: QboApi) => {
  if (!invoice.quickbooks_id) {
    // Never reached QuickBooks (jobs of one invoice run in order, so a create that was running or ended
    // UNKNOWN is resolved before this runs). A local delete then has nothing to sync: the row is removed.
    // A local void just stays as it is.
    if (!invoice.deleted_at) return saveDone(job, invoice, false, 'never reached QuickBooks, nothing to void');
    const pool = await getPool();
    return pool.transaction(async (tx) => {
      await tx.query(sql.unsafe`DELETE FROM invoices WHERE id = ${invoice.id}`);
      await completeJob(tx, job, 'never reached QuickBooks: deleted locally');
    });
  }

  let remote: QboInvoice;
  try {
    remote = await fetchInvoice(qbo, invoice.quickbooks_id);
  } catch (err) {
    // We want it gone from QuickBooks, and it already is (e.g. deleted there too): nothing to do.
    // (Inbound never deletes local data based on a failed GET; this only settles our own request.)
    if (classifyError(err).errorClass === 'not_found') {
      return saveDone(job, invoice, false, 'already removed from QuickBooks');
    }
    throw err;
  }
  const hasPayments = (remote.Balance ?? remote.TotalAmt ?? 0) < (remote.TotalAmt ?? 0);
  const operation = invoice.deleted_at && !hasPayments ? 'delete' : 'void';

  await qbo.request(() => 'invoice', {
    method: 'POST',
    params: { operation },
    body: { Id: remote.Id, SyncToken: remote.SyncToken },
  });
  await saveDone(job, invoice, operation === 'void', operation === 'void' ? 'voided in QuickBooks' : 'deleted in QuickBooks');
};

export const processOutboundInvoice = async (job: SyncJob, qbo: QboApi): Promise<unknown> => {
  const pool = await getPool();
  const invoice = await loadInvoice(pool, Number(job.entity_id));
  if (!invoice) return complete(job, 'invoice no longer exists');
  if (invoice.sync_status === 'conflict') return complete(job, 'skipped: the invoice has a conflict to resolve');

  const realmId = await qbo.realmId();
  if (!realmId) throw new NotConnectedError();
  if (invoice.quickbooks_realm_id && invoice.quickbooks_realm_id !== realmId) {
    throw new PermanentError('The invoice belongs to another QuickBooks company');
  }

  // A later job already synced this version (quick successive edits share one sync)
  if (invoice.synced_version !== null && invoice.version <= invoice.synced_version) {
    return pool.transaction(async (tx) => {
      await completeJob(tx, job, 'already synced');
      // FOR UPDATE: waits for a local change still being committed (the API updates the row before
      // deciding whether to queue a job, so if it saw this one waiting, it holds the row)
      const current = await tx.maybeOneFirst(sql.type(z.object({ version: z.number() }))`
        SELECT version FROM invoices WHERE id = ${invoice.id} AND sync_status <> 'conflict' FOR UPDATE
      `);
      await pushIfChanged(tx, invoice.id, current ?? undefined, invoice.version);
    });
  }
  // Voided (here or in QuickBooks): QuickBooks doesn't allow editing it, so only a deletion is pushed
  if (invoice.voided_at && !invoice.deleted_at) return saveDone(job, invoice, false, 'voided: changes are not pushed');
  if (invoice.deleted_at || (invoice.status === 'void' && !invoice.voided_at)) {
    return removeFromQuickBooks(job, invoice, qbo);
  }
  if (!invoice.quickbooks_id) return createInQuickBooks(job, invoice, realmId, qbo);
  return updateInQuickBooks(job, invoice, realmId, qbo);
};
