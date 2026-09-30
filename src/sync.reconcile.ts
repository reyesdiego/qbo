// Reconciliation, run periodically by the worker:
// 1. Full import, only when needed: on the first run after connecting a company, or when there's no
//    local invoice of that company (e.g. the local data was wiped). CDC can't cover that: it only lists
//    changes, for at most 30 days.
// 2. Otherwise, catches QuickBooks changes that webhooks missed: QuickBooks' Change Data Capture (CDC) API
//    lists what changed since the last run; each change is queued like a webhook notification (same queue,
//    same processing). Safe to rerun: changes already recorded are skipped by their event_key.
// 3. Resolves creates whose result is UNKNOWN (e.g. a timeout after sending them), by searching
//    QuickBooks for the invoice carrying the local invoice's reference.
import { sql } from 'slonik';
import { z } from 'zod';
import * as accounts from './accounts.repository';
import type { QboAccount } from './accounts.repository';
import { getPool } from './db';
import { errorMessage, NotConnectedError } from './errors';
import { setSyncState } from './invoices.repository';
import { SyncInvoice } from './invoices.schema';
import { log } from './logger';
import { getCdcCursor, quote, saveCdcCursor, type QboApi } from './quickbooks.client';
import { matchesQuickBooks, parseLocalInvoiceKey, type QboInvoice, type QboPayment } from './quickbooks.mapping';
import { checkConsistency } from './sync.consistency';
import { applyRemoteInvoice } from './sync.inbound';
import { saveSynced } from './sync.outbound';
import { mirrorQuickBooksPayment, reconcileUnknownPayment, setPaymentSyncState } from './sync.payments';
import {
  claimUnknownJob,
  failJob,
  recordEventAndEnqueue,
  releaseUnknown,
  requeueUnknown,
} from './sync.repository';

type CdcEntity = { Id: string; SyncToken?: string; status?: 'Deleted'; MetaData?: { LastUpdatedTime?: string } };
type CdcResponse = {
  time: string;
  CDCResponse: {
    QueryResponse?: { Invoice?: QboInvoice[]; Payment?: QboPayment[]; Account?: QboAccount[] }[];
  }[];
};

// The local chart of accounts starts with a full import; afterwards only changes are applied
const importAllAccounts = async (qbo: QboApi) => {
  const data = await qbo.query('SELECT * FROM Account MAXRESULTS 1000');
  const all: QboAccount[] = data.QueryResponse.Account ?? [];
  for (const account of all) await accounts.applyFromQuickBooks(account);
  return all.length;
};

const PAGE_SIZE = 1000; // QuickBooks' maximum

const hasLocalInvoices = async (realmId: string) => {
  const pool = await getPool();
  return pool.exists(sql.type(z.object({ id: z.number() }))`SELECT id FROM invoices WHERE quickbooks_realm_id = ${realmId}`);
};

// Every QuickBooks invoice (paginated), applied like a notification: the query returns the current
// entities, so no extra fetch is needed. Invoices this system created are linked, not duplicated, and
// versions already present are skipped, so repeating it is harmless. CDC continues from when it started.
const importAllInvoices = async (qbo: QboApi, realmId: string) => {
  let startedAt: string | undefined;
  let imported = 0;
  for (let start = 1; ; start += PAGE_SIZE) {
    const data = await qbo.query(
      `SELECT * FROM Invoice ORDERBY MetaData.CreateTime STARTPOSITION ${start} MAXRESULTS ${PAGE_SIZE}`,
    );
    startedAt ??= data.time;
    const page: QboInvoice[] = data.QueryResponse.Invoice ?? [];
    for (const invoice of page) {
      const outcome = await applyRemoteInvoice(null, realmId, invoice);
      if (outcome !== 'already up to date') imported++;
    }
    if (page.length < PAGE_SIZE) break;
  }
  await importAllPayments(qbo, realmId);
  await saveCdcCursor(startedAt ?? new Date().toISOString());
  return imported;
};

// Every QuickBooks payment, listed in the payments of the invoices it pays. Part of the full import;
// safe to rerun (rows are matched by invoice and QuickBooks payment id).
export const importAllPayments = async (qbo: QboApi, realmId: string) => {
  const pool = await getPool();
  let mirrored = 0;
  for (let start = 1; ; start += PAGE_SIZE) {
    const data = await qbo.query(`SELECT * FROM Payment STARTPOSITION ${start} MAXRESULTS ${PAGE_SIZE}`);
    const page: QboPayment[] = data.QueryResponse.Payment ?? [];
    for (const payment of page) mirrored += await pool.transaction((tx) => mirrorQuickBooksPayment(tx, realmId, payment));
    if (page.length < PAGE_SIZE) return mirrored;
  }
};

// CDC's limits: it only goes back 30 days (a margin is kept), and returns at most 1000 objects per entity
const CDC_MAX_AGE_MS = 29 * 24 * 60 * 60 * 1000;
const CDC_MAX_PER_ENTITY = 1000;
const CDC_ENTITIES = ['Invoice', 'Payment', 'Account'] as const;

// Reads everything instead of the changes: accounts, invoices and payments. Returns 0 (nothing queued).
const fullImport = async (qbo: QboApi, realmId: string, reason: string) => {
  await importAllAccounts(qbo);
  const imported = await importAllInvoices(qbo, realmId);
  log.info('sync.reconcile.full_import', { realm_id: realmId, reason, invoices_applied: imported });
  return 0;
};

const remoteInvoiceCount = async (qbo: QboApi): Promise<number> =>
  (await qbo.query('SELECT COUNT(*) FROM Invoice')).QueryResponse.totalCount ?? 0;

// Instead of CDC's changes: a full import, which brings what changed but not what was deleted, then the
// consistency check, which queues the deletions (each confirmed with a GET)
const catchUp = async (qbo: QboApi, realmId: string, reason: string) => {
  await fullImport(qbo, realmId, reason);
  await checkConsistency(qbo);
  return 0;
};

export const enqueueRemoteChanges = async (qbo: QboApi) => {
  const realmId = await qbo.realmId();
  if (!realmId) throw new NotConnectedError();

  const since = await getCdcCursor();
  if (since === null) return fullImport(qbo, realmId, 'first run');
  // No local invoice of the company (e.g. the local data was wiped): imported again, if QuickBooks has any
  // (a company without invoices just uses CDC)
  if (!(await hasLocalInvoices(realmId)) && (await remoteInvoiceCount(qbo)) > 0) return fullImport(qbo, realmId, 'no local invoices');
  // Stopped for longer than CDC remembers: the changes can't be asked for
  if (Date.parse(since) < Date.now() - CDC_MAX_AGE_MS) return catchUp(qbo, realmId, 'cursor older than CDC allows');
  if (await accounts.isEmpty()) await importAllAccounts(qbo);

  const data: CdcResponse = await qbo.request(() => 'cdc', { params: { entities: CDC_ENTITIES.join(','), changedSince: since } });
  const results = data.CDCResponse.flatMap((r) => r.QueryResponse ?? []);
  const byEntity = CDC_ENTITIES.map((name) => ({
    entity: name.toLowerCase(),
    items: results.flatMap((q) => (q[name] ?? []) as CdcEntity[]),
  }));
  // Cut at its maximum: more may have changed than it returned
  if (byEntity.some(({ items }) => items.length >= CDC_MAX_PER_ENTITY)) return catchUp(qbo, realmId, 'CDC returned its maximum');
  const changes = byEntity.flatMap(({ entity, items }) => items.map((item) => ({ entity, item })));

  const pool = await getPool();
  let enqueued = 0;
  for (const { entity, item } of changes) {
    // LastUpdatedTime, not SyncToken: some changes (e.g. marking an invoice as sent) keep the SyncToken
    const version = item.MetaData?.LastUpdatedTime ?? item.SyncToken ?? 'deleted';
    const outcome = await pool.transaction((tx) =>
      recordEventAndEnqueue(tx, {
        source: 'reconciliation',
        realmId,
        entity,
        id: item.Id,
        operation: item.status === 'Deleted' ? 'delete' : 'update',
        eventKey: `cdc:${realmId}:${entity}:${item.Id}:${version}`,
        payload: { Id: item.Id, SyncToken: item.SyncToken, status: item.status, MetaData: item.MetaData },
      }),
    );
    if (outcome === 'enqueued') enqueued++;
  }

  // QuickBooks' clock, not ours, so no change is missed because of clock differences
  await saveCdcCursor(data.time);
  return enqueued;
};

// QuickBooks query datetime literal
const qboDateTime = (ms: number) => new Date(ms).toISOString().replace(/\.\d{3}Z$/, '+00:00');

// For each UNKNOWN create: search the customer's invoices created around that time for the one
// carrying our reference.
// - exactly one: it was created; link it and complete the job
// - none: it wasn't created; send the create again (same requestid, so QuickBooks can't duplicate it)
// - several: can't decide safely; FAILED for manual investigation
export const reconcileUnknownJobs = async (qbo: QboApi, { workerId, leaseSeconds, graceSeconds, entityKeys }: {
  workerId: string;
  leaseSeconds: number;
  graceSeconds: number;
  entityKeys?: string[]; // only these entities (tests share one database)
}) => {
  const realmId = await qbo.realmId();
  if (!realmId) throw new NotConnectedError();
  const pool = await getPool();
  let checked = 0;

  for (;;) {
    const job = await claimUnknownJob({ workerId, leaseSeconds, graceSeconds, entityKeys });
    if (!job) return checked;
    checked++;

    const reference = String(job.payload.reference ?? '');
    const customerId = String(job.payload.customer_id ?? '');
    const since = qboDateTime(Date.parse(job.create_sent_at!) - 10 * 60 * 1000);

    if (job.entity_type === 'payment') {
      try {
        const outcome = await reconcileUnknownPayment(job, qbo, since);
        if (outcome === 'not_found') {
          await requeueUnknown(job, `Payment not found in QuickBooks: it is sent again (reference ${reference})`);
          await setPaymentSyncState(Number(job.entity_id), 'pending', null);
        } else if (outcome === 'ambiguous') {
          const message = `Several QuickBooks payments carry reference ${reference}: investigate manually`;
          await failJob(job, 'ambiguous', message);
          await setPaymentSyncState(Number(job.entity_id), 'failed', message);
        }
        log.info('sync.reconcile.unknown_payment', { job_id: job.id, reference, outcome });
      } catch (err) {
        await releaseUnknown(job, `Reconciliation failed: ${errorMessage(err)}`, 60);
        log.warn('sync.reconcile.unknown_retry_later', { job_id: job.id, error: errorMessage(err) });
      }
      continue;
    }

    try {
      const data = await qbo.query(
        `SELECT * FROM Invoice WHERE CustomerRef = ${quote(customerId)} AND MetaData.CreateTime >= ${quote(since)}`,
      );
      const matches = ((data.QueryResponse.Invoice ?? []) as QboInvoice[]).filter(
        (invoice) => parseLocalInvoiceKey(invoice.PrivateNote)?.key === reference,
      );

      if (matches.length === 1) {
        const invoice = await pool.one(sql.type(SyncInvoice)`SELECT * FROM invoices WHERE id = ${Number(job.entity_id)}`);
        // The created invoice has the version that was sent. If the invoice changed since (content, a
        // deletion, a void) or still needs its payment ("paid"), saveSynced keeps it pending and queues a job
        const syncedVersion = matchesQuickBooks(invoice, matches[0]) ? invoice.version : (invoice.synced_version ?? 0);
        await saveSynced(job, invoice.id, matches[0], realmId, syncedVersion, 'reconciled: found in QuickBooks');
        log.info('sync.reconcile.unknown_resolved', { job_id: job.id, invoice_id: invoice.id, quickbooks_id: matches[0].Id });
      } else if (matches.length === 0) {
        await requeueUnknown(job, `Not found in QuickBooks: the create is sent again (reference ${reference})`);
        await setSyncState(Number(job.entity_id), 'pending', null);
        log.info('sync.reconcile.unknown_not_created', { job_id: job.id, reference });
      } else {
        const message = `${matches.length} QuickBooks invoices carry reference ${reference}: investigate manually`;
        await failJob(job, 'ambiguous', message);
        await setSyncState(Number(job.entity_id), 'failed', message);
        log.error('sync.reconcile.unknown_ambiguous', { job_id: job.id, reference, quickbooks_ids: matches.map((m) => m.Id) });
      }
    } catch (err) {
      await releaseUnknown(job, `Reconciliation failed: ${errorMessage(err)}`, 60);
      log.warn('sync.reconcile.unknown_retry_later', { job_id: job.id, error: errorMessage(err) });
    }
  }
};
