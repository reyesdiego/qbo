// INBOUND: QuickBooks changes -> local database.
//
// Notifications (webhooks or reconciliation) only say what changed; the worker fetches the entity's
// current state from QuickBooks, so a stale or out-of-order notification can't overwrite newer data.
// Applying the same change twice is harmless: a version we already have (same SyncToken) is skipped.
import { sql, type DatabaseTransactionConnection } from 'slonik';
import { z } from 'zod';
import * as accounts from './accounts.repository';
import { getPool } from './db';
import { classifyError, DeferError, NotConnectedError } from './errors';
import { SyncInvoice } from './invoices.schema';
import { log } from './logger';
import type { QboApi } from './quickbooks.client';
import {
  changedFields,
  isNewerToken,
  isSentInQuickBooks,
  isTaxedInQuickBooks,
  isVoidedInQuickBooks,
  localInvoiceKey,
  localSnapshot,
  matchesQuickBooks,
  paidInvoiceIds,
  parseLocalInvoiceKey,
  remoteBalance,
  remoteSnapshot,
  sameSnapshot,
  statusFromQuickBooks,
  statusLeftToPush,
  syncedStatus,
  type QboInvoice,
  type QboPayment,
} from './quickbooks.mapping';
import { fetchInvoice, saveConflict } from './sync.outbound';
import { deleteLocalPayment, mirrorMissingPayments, mirrorQuickBooksPayment } from './sync.payments';
import { completeJob, enqueueInvoicePush, hasLaterDeleteNotification, invoiceEntityKey, type SyncJob } from './sync.repository';

type Tx = DatabaseTransactionConnection;

// ---------------------------------------------------------------------------
// Invoices
// ---------------------------------------------------------------------------

// A QuickBooks invoice we have no mapping for: either one we created whose create result we never
// saved (its PrivateNote carries our local key), or one created in QuickBooks, which we import
const linkOrImport = async (tx: Tx, realmId: string, remote: QboInvoice, voided: boolean) => {
  const marker = parseLocalInvoiceKey(remote.PrivateNote);
  const origin = marker
    ? await tx.maybeOne(sql.type(SyncInvoice)`SELECT * FROM invoices WHERE id = ${marker.id} FOR UPDATE`)
    : null;

  if (origin && localInvoiceKey(origin) === marker!.key) {
    if (origin.quickbooks_id) {
      // The local invoice is already linked to another QuickBooks invoice: this one is a duplicate
      log.warn('sync.inbound.duplicate_remote_invoice', { quickbooks_id: remote.Id, invoice_id: origin.id });
      return 'skipped: duplicate of a QuickBooks invoice already linked (see GET /quickbooks/duplicates)';
    }

    // Before the mapping exists, the invoice's create job may still be waiting for QuickBooks'
    // response: let it finish first. If it ended UNKNOWN, this notification resolves it.
    const createJob = await tx.maybeOne(sql.type(z.object({ id: z.string(), status: z.string() }))`
      SELECT id, status FROM sync_jobs
      WHERE entity_key = ${invoiceEntityKey(origin.id)} AND entity_type = 'invoice' AND direction = 'OUTBOUND'
        AND status IN ('PROCESSING', 'UNKNOWN')
      FOR UPDATE
    `);
    if (createJob?.status === 'PROCESSING') throw new DeferError(`Local invoice #${origin.id} is being created in QuickBooks`);

    // A voided invoice has nothing left to push, unless it was deleted locally (the deletion still is).
    // Otherwise a change made after the create (content, a deletion, a void, "paid") still has to be
    // pushed: its job is waiting, so the version stays unsynced. Taxed in QuickBooks: its content is
    // QuickBooks' (taken below), so only the status can be left to push.
    const taxed = isTaxedInQuickBooks(remote);
    const theirs = remoteSnapshot(remote);
    const matches = !origin.deleted_at && (voided || matchesQuickBooks(taxed ? { ...origin, ...theirs } : origin, remote));
    const content = taxed
      ? sql.fragment`customer_name = ${theirs.customer_name}, amount = ${theirs.amount}, currency = ${theirs.currency}, due_date = ${theirs.due_date},`
      : sql.fragment``;
    const balance = remoteBalance(remote);
    await tx.query(sql.unsafe`
      UPDATE invoices
      SET ${content} taxed_in_quickbooks = ${taxed},
          quickbooks_realm_id = ${realmId}, quickbooks_id = ${remote.Id}, quickbooks_sync_token = ${remote.SyncToken},
          last_synced_snapshot = ${JSON.stringify(theirs)}::jsonb, last_synced_at = now(),
          synced_version = CASE WHEN ${matches} THEN version ELSE synced_version END,
          sync_status = ${matches ? 'synced' : 'pending'}, sync_error = NULL, balance = ${balance},
          status = ${voided ? 'void' : syncedStatus(origin, remote, balance, origin.amount)},
          voided_at = ${voided ? sql.fragment`now()` : sql.fragment`NULL`}
      WHERE id = ${origin.id}
    `);
    if (createJob) {
      await tx.query(sql.unsafe`
        UPDATE sync_jobs
        SET status = 'COMPLETED', completed_at = now(), claim_token = NULL, lease_expires_at = NULL,
            payload = payload || '{"result": "reconciled: found in QuickBooks"}'::jsonb, updated_at = now()
        WHERE id = ${createJob.id}
      `);
    }
    if (!matches) await enqueueInvoicePush(tx, origin.id); // changed locally after the create: push it
    return 'linked to the local invoice that created it';
  }

  const remoteContent = remoteSnapshot(remote);
  const balance = remoteBalance(remote);
  await tx.query(sql.unsafe`
    INSERT INTO invoices
      (customer_name, amount, balance, currency, status, due_date, quickbooks_realm_id, quickbooks_id,
       quickbooks_sync_token, last_synced_snapshot, last_synced_at, synced_version, sync_status, voided_at, taxed_in_quickbooks)
    VALUES
      (${remoteContent.customer_name}, ${remoteContent.amount}, ${balance}, ${remoteContent.currency},
       ${voided ? 'void' : statusFromQuickBooks(remote, balance, remoteContent.amount, 'draft')}, ${remoteContent.due_date},
       ${realmId}, ${remote.Id}, ${remote.SyncToken}, ${JSON.stringify(remoteContent)}::jsonb, now(), 1, 'synced',
       ${voided ? sql.fragment`now()` : sql.fragment`NULL`}, ${isTaxedInQuickBooks(remote)})
  `);
  return voided ? 'imported from QuickBooks (voided)' : 'imported from QuickBooks';
};

// Three-way comparison between the last synced snapshot (base), the local invoice and QuickBooks:
// - only QuickBooks changed -> apply it
// - only the local invoice changed -> keep it (its outbound job pushes it)
// - both made the same change -> nothing to do
// - both changed differently -> conflict, nothing overwritten
// Balance and paid status always come from QuickBooks (payments are recorded there).
// Voided in QuickBooks -> voided locally: status "void", balance 0, and the invoiced amounts are kept
// (QuickBooks zeroes them). A void wins over unpushed local changes: a voided invoice can't be edited.
const applyInTransaction = async (tx: Tx, realmId: string, remote: QboInvoice, voidNotified: boolean) => {
  const voided = voidNotified || isVoidedInQuickBooks(remote);
  const local = await tx.maybeOne(sql.type(SyncInvoice)`
    SELECT * FROM invoices WHERE quickbooks_realm_id = ${realmId} AND quickbooks_id = ${remote.Id} FOR UPDATE
  `);
  if (!local) return linkOrImport(tx, realmId, remote, voided);

  if (local.deleted_at) return 'ignored: deleted locally (the deletion is being synced)';

  // Duplicate or out-of-order notification, or our own push coming back: nothing newer to apply.
  // Except "sent": marking an invoice as sent in QuickBooks doesn't change its SyncToken.
  if (!isNewerToken(remote.SyncToken, local.quickbooks_sync_token)) {
    if (local.status === 'draft' && isSentInQuickBooks(remote)) {
      await tx.query(sql.unsafe`UPDATE invoices SET status = 'sent', updated_at = now() WHERE id = ${local.id}`);
      return 'marked as sent (sent in QuickBooks)';
    }
    return 'already up to date';
  }

  if (voided) {
    await tx.query(sql.unsafe`
      UPDATE invoices
      SET status = 'void', voided_at = coalesce(voided_at, now()), balance = '0.00',
          quickbooks_sync_token = ${remote.SyncToken}, synced_version = version, last_synced_at = now(),
          sync_status = 'synced', sync_error = NULL, sync_conflict = NULL, updated_at = now()
      WHERE id = ${local.id}
    `);
    return local.voided_at ? 'already voided' : 'voided locally (invoiced amounts kept)';
  }

  const base = local.last_synced_snapshot;
  const mine = localSnapshot(local);
  const theirs = remoteSnapshot(remote);
  const remoteChanged = !base || !sameSnapshot(theirs, base);
  const localChanged = !base || !sameSnapshot(mine, base);
  // Taxed in QuickBooks: its content is QuickBooks' (it can't be edited here)
  const taxed = isTaxedInQuickBooks(remote);
  const takeRemote = remoteChanged && (!localChanged || sameSnapshot(theirs, mine) || taxed);

  const balance = remoteBalance(remote);
  // A local "paid" not pushed yet stays paid (its outbound job records the payment in QuickBooks)
  const status = syncedStatus(local, remote, balance, takeRemote ? theirs.amount : mine.amount);
  // A local status QuickBooks doesn't have yet (paid, sent, void) still has to be pushed, even when the
  // content is taken from QuickBooks: the invoice isn't fully synced then
  const statusToPush = statusLeftToPush(status, remote);
  const qbOwned = sql.fragment`balance = ${balance}, status = ${status}, quickbooks_sync_token = ${remote.SyncToken}, taxed_in_quickbooks = ${taxed}`;

  if (!remoteChanged) {
    await tx.query(sql.unsafe`UPDATE invoices SET ${qbOwned} WHERE id = ${local.id}`);
    return 'no content change (balance/status refreshed)';
  }
  if (takeRemote) {
    await tx.query(sql.unsafe`
      UPDATE invoices
      SET customer_name = ${theirs.customer_name}, amount = ${theirs.amount}, currency = ${theirs.currency},
          due_date = ${theirs.due_date}, ${qbOwned},
          last_synced_snapshot = ${JSON.stringify(theirs)}::jsonb, last_synced_at = now(),
          synced_version = CASE WHEN ${statusToPush} THEN synced_version ELSE version END,
          sync_status = ${statusToPush ? 'pending' : 'synced'}, sync_error = NULL, sync_conflict = NULL, updated_at = now()
      WHERE id = ${local.id}
    `);
    // Local changes made during a conflict queue no job: clearing it queues the push of what's left
    if (statusToPush && local.sync_status === 'conflict') await enqueueInvoicePush(tx, local.id);
    return localChanged ? 'converged: the same change was made on both sides' : 'applied QuickBooks changes';
  }

  await tx.query(sql.unsafe`UPDATE invoices SET ${qbOwned} WHERE id = ${local.id}`);
  await saveConflict(tx, local.id, {
    reason: 'Changed locally and in QuickBooks',
    fields: changedFields(mine, theirs),
    local: mine,
    remote: theirs,
    base,
    remote_sync_token: remote.SyncToken,
  });
  return 'conflict registered';
};

// Applies the current QuickBooks invoice; with a job, completes it in the same transaction
export const applyRemoteInvoice = async (job: SyncJob | null, realmId: string, remote: QboInvoice, { voided = false } = {}) => {
  const pool = await getPool();
  return pool.transaction(async (tx) => {
    const result = await applyInTransaction(tx, realmId, remote, voided);
    if (job) await completeJob(tx, job, result);
    return result;
  });
};

// Deleted in QuickBooks -> deleted locally (the row is removed). Only a delete notification does this:
// a failed GET is never taken as proof of deletion. Unpushed local changes can't be synced to a deleted
// invoice, so they're discarded; the job's result and a warning log record which fields they were.
const applyRemoteDeletion = async (job: SyncJob, realmId: string, quickbooksId: string) => {
  const pool = await getPool();
  await pool.transaction(async (tx) => {
    const local = await tx.maybeOne(sql.type(SyncInvoice)`
      DELETE FROM invoices WHERE quickbooks_realm_id = ${realmId} AND quickbooks_id = ${quickbooksId}
      RETURNING *
    `);
    let result = 'nothing to delete locally';
    if (local) {
      const base = local.last_synced_snapshot;
      const discarded = base && !local.deleted_at ? changedFields(localSnapshot(local), base) : [];
      result = discarded.length
        ? `deleted locally; unpushed local changes discarded (${discarded.join(', ')})`
        : 'deleted locally';
      if (discarded.length) {
        log.warn('sync.inbound.deleted_with_local_changes', {
          invoice_id: local.id,
          quickbooks_id: quickbooksId,
          local: localSnapshot(local),
          base,
        });
      }
    }
    await completeJob(tx, job, result);
  });
};

// ---------------------------------------------------------------------------
// Payments and accounts
// ---------------------------------------------------------------------------

const applyPayment = async (job: SyncJob, realmId: string, qbo: QboApi) => {
  const pool = await getPool();
  if (job.operation === 'delete') {
    // Deleted in QuickBooks -> the local record of it (if it was recorded here) is deleted too. A
    // deleted payment can't be fetched, so its invoices are refreshed by their own notifications.
    return pool.transaction(async (tx) => {
      const deleted = await deleteLocalPayment(tx, job.entity_id);
      await completeJob(tx, job, deleted.length ? `payment deleted locally (#${deleted.join(', #')})` : 'payment deleted in QuickBooks');
    });
  }
  const payment: QboPayment = (await qbo.request(() => `payment/${job.entity_id}`)).Payment;
  const invoiceIds = paidInvoiceIds(payment);
  for (const invoiceId of invoiceIds) {
    await applyRemoteInvoice(null, realmId, await fetchInvoice(qbo, invoiceId));
  }
  // The payment itself, in each paid invoice's payment list
  await pool.transaction(async (tx) => {
    const mirrored = await mirrorQuickBooksPayment(tx, realmId, payment);
    await completeJob(tx, job, `refreshed ${invoiceIds.length} paid invoice(s), payment listed for ${mirrored}`);
  });
};

const applyAccount = async (job: SyncJob, qbo: QboApi) => {
  const account = job.operation === 'delete'
    ? { Id: job.entity_id, status: 'Deleted' as const }
    : (await qbo.request(() => `account/${job.entity_id}`)).Account;
  const pool = await getPool();
  await pool.transaction(async (tx) => {
    await accounts.applyFromQuickBooks(account, tx);
    await completeJob(tx, job, job.operation === 'delete' ? 'account removed' : 'account updated');
  });
};

export const processInboundJob = async (job: SyncJob, qbo: QboApi) => {
  const realmId = await qbo.realmId();
  if (!realmId) throw new NotConnectedError();
  if (job.realm_id !== realmId) {
    const pool = await getPool();
    return completeJob(pool, job, 'ignored: not the connected company');
  }

  switch (job.entity_type) {
    case 'invoice': {
      if (job.operation === 'delete') return applyRemoteDeletion(job, realmId, job.entity_id);
      let remote: QboInvoice;
      try {
        remote = await fetchInvoice(qbo, job.entity_id);
      } catch (err) {
        // "Not found" alone is no proof of deletion (the job is retried). But if QuickBooks also sent
        // a delete notification for it, this older notification is superseded: the delete job applies it.
        if (classifyError(err).errorClass === 'not_found' && (await hasLaterDeleteNotification(job))) {
          const pool = await getPool();
          return completeJob(pool, job, 'superseded: deleted in QuickBooks afterwards');
        }
        throw err;
      }
      const result = await applyRemoteInvoice(null, realmId, remote, { voided: job.operation === 'void' });
      // Its payments too, when some aren't listed locally yet (e.g. an invoice imported from QuickBooks).
      // Applying and listing are idempotent, so a retry after a failure here just completes them.
      const listed = await mirrorMissingPayments(qbo, realmId, remote);
      const pool = await getPool();
      await completeJob(pool, job, listed ? `${result}, ${listed} payment(s) listed` : result);
      return;
    }
    case 'payment':
      return applyPayment(job, realmId, qbo);
    case 'account':
      return applyAccount(job, qbo);
  }
};
