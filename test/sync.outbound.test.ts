// OUTBOUND sync (local -> QuickBooks) against an in-memory fake QuickBooks, to reproduce failures
// like "QuickBooks created the invoice but the response was lost"
import { test, after, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { sql } from 'slonik';
import { getPool } from '../src/db';
import { armFault, disarmFaults } from '../src/faults';
import * as invoices from '../src/invoices.repository';
import { SyncInvoice } from '../src/invoices.schema';
import { localInvoiceKey } from '../src/quickbooks.mapping';
import { reconcileUnknownJobs } from '../src/sync.reconcile';
import { enqueueJob, invoiceEntityKey } from '../src/sync.repository';
import { FakeQuickBooks, jobsFor, loadInvoice, qboErrors, runJobs, runNow, uniqueName } from './helpers';

after(async () => {
  await (await getPool()).end();
});
beforeEach(() => disarmFaults());

const createLocal = async (amount = '100.00') => {
  const { invoice } = await invoices.create({
    customer_name: uniqueName('Outbound Co'),
    amount,
    currency: 'USD',
    status: 'sent',
    due_date: '2026-12-31',
  });
  return { id: invoice.id, key: invoiceEntityKey(invoice.id) };
};

// Creates a local invoice and syncs it to the fake QuickBooks
const createSynced = async (qbo: FakeQuickBooks, amount = '100.00') => {
  const local = await createLocal(amount);
  await runJobs([local.key], qbo);
  const invoice = await loadInvoice(local.id);
  assert.equal(invoice.sync_status, 'synced');
  return { ...local, quickbooksId: invoice.quickbooks_id! };
};

// Pretends the create was sent long enough ago for reconciliation to look for it
const ageCreate = async (key: string) => {
  const pool = await getPool();
  await pool.query(sql.unsafe`UPDATE sync_jobs SET create_sent_at = now() - interval '10 minutes' WHERE entity_key = ${key}`);
};

// Only this test's jobs: the test files share one database and run at the same time
const reconcile = (qbo: FakeQuickBooks, key: string) =>
  reconcileUnknownJobs(qbo, { workerId: 'test-worker', leaseSeconds: 60, graceSeconds: 60, entityKeys: [key] });

test('creates the invoice in QuickBooks and stores the mapping', async () => {
  const qbo = new FakeQuickBooks();
  const local = await createLocal('1234.56');
  await runJobs([local.key], qbo);

  const invoice = await loadInvoice(local.id);
  assert.equal(invoice.sync_status, 'synced');
  assert.ok(invoice.quickbooks_id);
  assert.equal(invoice.quickbooks_sync_token, '0');
  assert.equal(invoice.synced_version, invoice.version);
  assert.deepEqual(invoice.last_synced_snapshot, { customer_name: invoice.customer_name, amount: '1234.56', currency: 'USD', due_date: '2026-12-31' });

  const remote = qbo.invoices.get(invoice.quickbooks_id!)!;
  assert.equal(remote.TotalAmt, 1234.56);
  assert.ok(remote.PrivateNote?.includes(localInvoiceKey(invoice))); // stable integration reference
  assert.equal((await jobsFor(local.key))[0].status, 'COMPLETED');
});

test('ambiguous create (QuickBooks created it, the response was lost)', async (t) => {
  const qbo = new FakeQuickBooks();
  const local = await createLocal();

  await t.test('the job becomes UNKNOWN and the create is not resent', async () => {
    armFault('qbo.create.response-lost');
    await runJobs([local.key], qbo);

    assert.equal((await jobsFor(local.key))[0].status, 'UNKNOWN');
    assert.equal((await loadInvoice(local.id)).sync_status, 'unknown');
    await invoices.update(local.id, { amount: '120.00' }); // a later change must not trigger a second create
    await runJobs([local.key], qbo);
    assert.equal(qbo.countCalls('POST invoice'), 1);
  });

  await t.test('reconciliation finds it by its reference and links it', async () => {
    await ageCreate(local.key);
    await reconcile(qbo, local.key);

    const jobs = await jobsFor(local.key);
    assert.equal(jobs[0].status, 'COMPLETED');
    const invoice = await loadInvoice(local.id);
    assert.equal(invoice.quickbooks_id, [...qbo.invoices.keys()][0]);

    // The change made meanwhile is then pushed as an update, not a second create
    await runJobs([local.key], qbo);
    assert.equal(qbo.invoices.size, 1);
    assert.equal([...qbo.invoices.values()][0].TotalAmt, 120);
    assert.equal((await loadInvoice(local.id)).sync_status, 'synced');
  });
});

test('ambiguous create that never reached QuickBooks is sent again after reconciliation', async () => {
  const qbo = new FakeQuickBooks();
  const local = await createLocal();
  qbo.failNext('POST invoice', qboErrors.timeout); // times out before QuickBooks creates it
  await runJobs([local.key], qbo);
  assert.equal((await jobsFor(local.key))[0].status, 'UNKNOWN');

  await ageCreate(local.key);
  await reconcile(qbo, local.key); // not found: back to PENDING
  await runJobs([local.key], qbo);

  assert.equal(qbo.invoices.size, 1);
  assert.equal((await loadInvoice(local.id)).sync_status, 'synced');
});

test('reconciliation fails the job when several QuickBooks invoices carry the reference', async () => {
  const qbo = new FakeQuickBooks();
  const local = await createLocal();
  armFault('qbo.create.response-lost');
  await runJobs([local.key], qbo);

  const [created] = qbo.invoices.values();
  qbo.addInvoice({ ...created, Id: 'copy' }); // e.g. someone created it again by hand
  await ageCreate(local.key);
  await reconcile(qbo, local.key);

  assert.equal((await jobsFor(local.key))[0].status, 'FAILED');
  assert.equal((await loadInvoice(local.id)).sync_status, 'failed');
});

test('a QuickBooks version conflict is retried with the latest version', async () => {
  const qbo = new FakeQuickBooks();
  const synced = await createSynced(qbo);
  await invoices.update(synced.id, { amount: '150.00' });

  qbo.failNext('POST invoice', qboErrors.staleObject);
  await runJobs([synced.key], qbo);
  const retrying = (await jobsFor(synced.key)).at(-1)!;
  assert.equal(retrying.status, 'PENDING');
  assert.equal(retrying.last_error_class, 'version_conflict');

  await runNow(synced.key);
  await runJobs([synced.key], qbo);
  assert.equal(qbo.invoices.get(synced.quickbooksId)!.TotalAmt, 150);
  assert.equal((await loadInvoice(synced.id)).sync_status, 'synced');
});

test('a local change does not overwrite a different change made in QuickBooks', async () => {
  const qbo = new FakeQuickBooks();
  const synced = await createSynced(qbo);
  qbo.edit(synced.quickbooksId, { TotalAmt: 999, Balance: 999 }); // changed in QuickBooks
  await invoices.update(synced.id, { amount: '150.00' }); // and locally

  await runJobs([synced.key], qbo);

  const invoice = await loadInvoice(synced.id);
  assert.equal(invoice.sync_status, 'conflict');
  assert.equal(invoice.sync_conflict?.reason, 'Changed locally and in QuickBooks');
  assert.deepEqual(invoice.sync_conflict?.fields, ['amount']);
  assert.equal(qbo.invoices.get(synced.quickbooksId)!.TotalAmt, 999); // QuickBooks untouched
  assert.equal(invoice.amount, '150.00'); // local untouched
});

test('deleting: deleted in QuickBooks, or voided if it has payments', async (t) => {
  await t.test('without payments it is deleted', async () => {
    const qbo = new FakeQuickBooks();
    const synced = await createSynced(qbo);
    await invoices.remove(synced.id);
    await runJobs([synced.key], qbo);
    assert.equal(qbo.invoices.has(synced.quickbooksId), false);
  });

  await t.test('with a payment applied it is voided', async () => {
    const qbo = new FakeQuickBooks();
    const synced = await createSynced(qbo);
    qbo.edit(synced.quickbooksId, { Balance: 40 }); // 60 paid
    await invoices.remove(synced.id);
    await runJobs([synced.key], qbo);

    assert.ok(qbo.invoices.get(synced.quickbooksId)!.PrivateNote?.startsWith('Voided'));
    const invoice = await loadInvoice(synced.id);
    assert.ok(invoice.voided_at);
    assert.equal(invoice.status, 'void');
  });
});

test('an invoice deleted before it reached QuickBooks is removed locally, without calling QuickBooks', async () => {
  const qbo = new FakeQuickBooks();
  const local = await createLocal();
  await invoices.remove(local.id); // before the worker ran: the pending create job covers the deletion too
  await runJobs([local.key], qbo);

  const pool = await getPool();
  const rows = await pool.any(sql.type(SyncInvoice)`SELECT * FROM invoices WHERE id = ${local.id}`);
  assert.equal(rows.length, 0);
  assert.equal((await jobsFor(local.key)).at(-1)!.payload.result, 'never reached QuickBooks: deleted locally');
  assert.equal(qbo.calls.length, 0);
});

test('deleting an invoice already deleted in QuickBooks completes without retrying', async () => {
  const qbo = new FakeQuickBooks();
  const synced = await createSynced(qbo);
  qbo.invoices.delete(synced.quickbooksId); // deleted in QuickBooks too
  await invoices.remove(synced.id);
  await runJobs([synced.key], qbo);

  const job = (await jobsFor(synced.key)).at(-1)!;
  assert.equal(job.status, 'COMPLETED');
  assert.equal(job.payload.result, 'already removed from QuickBooks');
});

test('a job for an already synced version does nothing (no loop)', async () => {
  const qbo = new FakeQuickBooks();
  const synced = await createSynced(qbo);
  const pool = await getPool();
  await enqueueJob(pool, { direction: 'OUTBOUND', entityType: 'invoice', entityId: String(synced.id), entityKey: synced.key, operation: 'upsert' });

  const callsBefore = qbo.calls.length;
  await runJobs([synced.key], qbo);
  assert.equal(qbo.calls.length, callsBefore);
  assert.equal((await jobsFor(synced.key)).at(-1)!.payload.result, 'already synced');
});

test('a validation error from QuickBooks fails the job right away', async () => {
  const qbo = new FakeQuickBooks();
  const local = await createLocal();
  qbo.failNext('POST invoice', qboErrors.validation);
  await runJobs([local.key], qbo);

  const [job] = await jobsFor(local.key);
  assert.equal(job.status, 'FAILED');
  assert.equal(job.attempts, 1);
  const invoice = await loadInvoice(local.id);
  assert.equal(invoice.sync_status, 'failed');
  assert.equal(invoice.sync_error, 'Invalid Reference Id');
});
