// Races between a local change and a sync job that is running or waiting (found in code review).
// Each test reproduces one scenario end to end against the fake QuickBooks.
import { test, after, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { AsyncResource } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { sql } from 'slonik';
import { z } from 'zod';
import { getPool } from '../src/db';
import { armFault, disarmFaults } from '../src/faults';
import * as invoices from '../src/invoices.repository';
import * as payments from '../src/payments.repository';
import { isVoidedInQuickBooks } from '../src/quickbooks.mapping';
import { resolveConflict } from '../src/sync.conflicts';
import { reconcileUnknownJobs } from '../src/sync.reconcile';
import { processJob } from '../src/sync.processor';
import { claimNextJob, enqueueInvoicePush, invoiceEntityKey, recordEventAndEnqueue, recoverExpiredLeases } from '../src/sync.repository';
import { FakeQuickBooks, jobsFor, loadInvoice, qboErrors, runJobs, TEST_REALM, uniqueName } from './helpers';

after(async () => {
  await (await getPool()).end();
});
beforeEach(() => disarmFaults());

const createLocal = async (status: 'draft' | 'sent' | 'paid' = 'sent', amount = '100.00') => {
  const { invoice } = await invoices.create({ customer_name: uniqueName('Race Co'), amount, currency: 'USD', status, due_date: '2026-12-31' });
  return { id: invoice.id, key: invoiceEntityKey(invoice.id) };
};

const createSynced = async (qbo: FakeQuickBooks) => {
  const local = await createLocal();
  await runJobs([local.key], qbo);
  return { ...local, quickbooksId: (await loadInvoice(local.id)).quickbooks_id! };
};

const notify = async (id: string, operation = 'update') => {
  const pool = await getPool();
  await pool.transaction((tx) =>
    recordEventAndEnqueue(tx, { source: 'webhook', realmId: TEST_REALM, entity: 'invoice', id, operation, eventKey: randomUUID(), payload: {} }),
  );
};

const qbKey = (id: string) => `qbo-invoice:${TEST_REALM}:${id}`;

// The create reaches QuickBooks but its response is lost: the job ends UNKNOWN
const createWithLostResponse = async (qbo: FakeQuickBooks, status: 'sent' | 'paid' = 'sent') => {
  const local = await createLocal(status);
  armFault('qbo.create.response-lost');
  await runJobs([local.key], qbo);
  assert.equal((await jobsFor(local.key))[0].status, 'UNKNOWN');
  const quickbooksId = [...qbo.invoices.keys()].at(-1)!;
  return { ...local, quickbooksId };
};

const reconcile = async (qbo: FakeQuickBooks, key: string) => {
  const pool = await getPool();
  await pool.query(sql.unsafe`UPDATE sync_jobs SET create_sent_at = now() - interval '10 minutes' WHERE entity_key = ${key}`);
  await reconcileUnknownJobs(qbo, { workerId: 'test-worker', leaseSeconds: 60, graceSeconds: 60, entityKeys: [key] });
};

test('1. marked "paid" while the worker pushes an earlier change: the payment still reaches QuickBooks', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key, quickbooksId } = await createSynced(qbo);

  await invoices.update(id, { amount: '120.00' });
  qbo.duringNext('POST invoice', () => invoices.update(id, { status: 'paid' })); // while pushing the amount
  await runJobs([key], qbo);

  assert.equal(qbo.invoices.get(quickbooksId)!.Balance, 0, 'no payment recorded in QuickBooks');
  const invoice = await loadInvoice(id);
  assert.equal(invoice.status, 'paid');
  assert.equal(invoice.sync_status, 'synced');
});

test('2. a payment synced before a pending QuickBooks notification: the QuickBooks edit is still applied', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key, quickbooksId } = await createSynced(qbo);

  await payments.create(id, { amount: '30.00' }); // its job is queued first
  qbo.edit(quickbooksId, { DueDate: '2027-03-01' }); // edited in QuickBooks; its notification comes after
  await notify(quickbooksId);
  await runJobs([key], qbo);

  const invoice = await loadInvoice(id);
  assert.equal(invoice.balance, '70.00');
  assert.equal(invoice.due_date, '2027-03-01', 'the QuickBooks edit was skipped');
});

test('3. deleted locally while the create was UNKNOWN, then linked by a notification: deleted in QuickBooks too', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key, quickbooksId } = await createWithLostResponse(qbo);

  await invoices.remove(id); // its job waits behind the UNKNOWN create
  await notify(quickbooksId, 'create'); // QuickBooks notifies the created invoice: links it
  await runJobs([qbKey(quickbooksId)], qbo);
  await runJobs([key], qbo);

  assert.ok(!qbo.invoices.has(quickbooksId), 'still in QuickBooks');
  const pool = await getPool();
  assert.equal(await pool.maybeOne(sql.unsafe`SELECT id FROM invoices WHERE id = ${id} AND deleted_at IS NULL`), null);
});

test('4a. deleted locally while the create was UNKNOWN, then resolved by reconciliation: deleted in QuickBooks too', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key, quickbooksId } = await createWithLostResponse(qbo);

  await invoices.remove(id);
  await reconcile(qbo, key);
  await runJobs([key], qbo);

  assert.ok(!qbo.invoices.has(quickbooksId), 'still in QuickBooks');
});

test('4b. created as "paid", response lost, resolved by reconciliation: the payment is recorded', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key, quickbooksId } = await createWithLostResponse(qbo, 'paid');

  await reconcile(qbo, key);
  await runJobs([key], qbo);

  assert.equal(qbo.invoices.get(quickbooksId)!.Balance, 0, 'no payment recorded in QuickBooks');
  assert.equal((await loadInvoice(id)).status, 'paid');
});

test('5. voided locally while a QuickBooks edit is applied first: still voided in QuickBooks', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key, quickbooksId } = await createSynced(qbo);

  qbo.edit(quickbooksId, { DueDate: '2027-03-01' });
  await notify(quickbooksId); // its job runs before the void's
  await invoices.update(id, { status: 'void' });
  await runJobs([key], qbo);

  assert.ok(isVoidedInQuickBooks(qbo.invoices.get(quickbooksId)!), 'not voided in QuickBooks');
  const invoice = await loadInvoice(id);
  assert.equal(invoice.status, 'void');
  assert.equal(invoice.due_date, '2027-03-01');
});

test('6. deleted during a conflict, resolved with "remote": still deleted in QuickBooks', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key, quickbooksId } = await createSynced(qbo);

  qbo.edit(quickbooksId, { DueDate: '2027-03-01' });
  await notify(quickbooksId);
  await invoices.update(id, { due_date: '2027-04-01' }); // changed differently on both sides
  await runJobs([key], qbo);
  assert.equal((await loadInvoice(id)).sync_status, 'conflict');

  await invoices.remove(id); // kept while the conflict is open (no job)
  assert.equal(await resolveConflict(id, 'remote'), 'resolved');
  await runJobs([key], qbo);

  assert.ok(!qbo.invoices.has(quickbooksId), 'still in QuickBooks');
});

test('7. only the status changed locally, the content only in QuickBooks: no conflict, the status is pushed', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key, quickbooksId } = await createSynced(qbo);

  await invoices.update(id, { status: 'paid' }); // its job runs before the QuickBooks edit's notification
  qbo.edit(quickbooksId, { DueDate: '2027-03-01' });
  await notify(quickbooksId);
  await runJobs([key], qbo);

  const invoice = await loadInvoice(id);
  assert.equal(invoice.sync_status, 'synced', invoice.sync_error ?? undefined);
  assert.equal(invoice.due_date, '2027-03-01');
  assert.equal(invoice.status, 'paid');
  assert.equal(qbo.invoices.get(quickbooksId)!.Balance, 0, 'no payment recorded in QuickBooks');
});

// Not reachable through the normal flow today (a payment job can't run while its invoice's create is
// UNKNOWN), so the state is set up directly: the lookup must only consider the invoice's own jobs
test('8. linking an invoice finds its create job, not a payment job of the same invoice', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key, quickbooksId } = await createWithLostResponse(qbo);
  const payment = await payments.create(id, { amount: '10.00' });
  assert.ok('payment' in payment);
  const pool = await getPool();
  await pool.query(sql.unsafe`
    UPDATE sync_jobs SET status = 'UNKNOWN', create_sent_at = now()
    WHERE entity_key = ${key} AND entity_type = 'payment'
  `);

  await notify(quickbooksId, 'create');
  await runJobs([qbKey(quickbooksId)], qbo);

  const jobs = await jobsFor(key);
  assert.equal(jobs.find((job) => job.entity_type === 'invoice')!.status, 'COMPLETED');
  assert.equal(jobs.find((job) => job.entity_type === 'payment')!.status, 'UNKNOWN'); // left to its own reconciliation
  assert.equal((await loadInvoice(id)).quickbooks_id, quickbooksId);
});

test('9. a conflict that converges (QuickBooks ends up like the local version) is cleared, and what is left is pushed', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key, quickbooksId } = await createSynced(qbo);

  qbo.edit(quickbooksId, { DueDate: '2027-03-01' });
  await notify(quickbooksId);
  await invoices.update(id, { due_date: '2027-04-01' });
  await runJobs([key], qbo);
  assert.equal((await loadInvoice(id)).sync_status, 'conflict');

  await invoices.update(id, { status: 'paid' }); // during the conflict: kept, no job queued
  qbo.edit(quickbooksId, { DueDate: '2027-04-01' }); // someone makes QuickBooks match the local version
  await notify(quickbooksId);
  await runJobs([key], qbo);

  const invoice = await loadInvoice(id);
  assert.equal(invoice.sync_conflict, null, 'stale conflict details left');
  assert.equal(invoice.sync_status, 'synced', invoice.sync_error ?? undefined);
  assert.equal(invoice.status, 'paid');
  assert.equal(qbo.invoices.get(quickbooksId)!.Balance, 0, 'the payment was never pushed');
});

// A paid invoice, synced: its payment is in QuickBooks and the balance is 0 on both sides
const paidSynced = async (qbo: FakeQuickBooks) => {
  const synced = await createSynced(qbo);
  await invoices.update(synced.id, { status: 'paid' });
  await runJobs([synced.key], qbo);
  assert.equal(qbo.invoices.get(synced.quickbooksId)!.Balance, 0);
  return synced;
};
const paymentsIn = (qbo: FakeQuickBooks, quickbooksId: string) =>
  (qbo.invoices.get(quickbooksId)!.LinkedTxn ?? []).filter((txn) => txn.TxnType === 'Payment').length;

test('10a. a payment deleted in QuickBooks is not paid again by an unrelated local edit', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key, quickbooksId } = await paidSynced(qbo);

  const [payment] = qbo.payments.keys();
  await qbo.request(() => 'payment', { method: 'POST', params: { operation: 'delete' }, body: { Id: payment, SyncToken: '0' } }); // its notification is pending
  await invoices.update(id, { due_date: '2027-08-01' });
  await runJobs([key], qbo);

  assert.equal(paymentsIn(qbo, quickbooksId), 0, 'a payment nobody made was recorded');
  assert.equal(qbo.invoices.get(quickbooksId)!.Balance, 100);
  const invoice = await loadInvoice(id);
  assert.equal(invoice.status, 'sent'); // QuickBooks has a balance again
  assert.equal(invoice.balance, '100.00');
});

test('10b. raising the amount of a paid invoice does not pay the difference', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key, quickbooksId } = await paidSynced(qbo);

  await invoices.update(id, { amount: '150.00' });
  await runJobs([key], qbo);

  assert.equal(paymentsIn(qbo, quickbooksId), 1, 'a payment nobody made was recorded');
  const invoice = await loadInvoice(id);
  assert.equal(invoice.balance, '50.00');
  assert.equal(invoice.status, 'sent');
});

test('11. a worker whose lease expired mid-request does not mark the invoice failed after another worker synced it', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key } = await createSynced(qbo);
  await invoices.update(id, { amount: '130.00' });

  const jobA = (await claimNextJob({ workerId: 'worker-a', leaseSeconds: 60, entityKeys: [key] }))!;
  qbo.duringNext('POST invoice', async () => {
    // Worker A's request takes longer than its lease: the job is recovered and worker B syncs it
    const pool = await getPool();
    await pool.query(sql.unsafe`UPDATE sync_jobs SET lease_expires_at = now() - interval '1 second' WHERE id = ${jobA.id}`);
    await recoverExpiredLeases();
    await runJobs([key], qbo, 'worker-b');
    qbo.failNext('POST invoice', qboErrors.validation); // then A's request fails
  });
  await processJob(jobA, qbo);

  const invoice = await loadInvoice(id);
  assert.equal(invoice.sync_status, 'synced', invoice.sync_error ?? undefined);
  assert.equal((await jobsFor(key)).at(-1)!.status, 'COMPLETED');
});

test('12. a voided invoice cannot change, not even bypassing the API (so it never waits for a push that cannot happen)', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key } = await createSynced(qbo);
  await invoices.update(id, { status: 'void' });
  await runJobs([key], qbo);
  const voided = await loadInvoice(id);
  assert.ok(voided.voided_at);

  assert.equal(await invoices.update(id, { due_date: '2027-09-01' }), null);
  const after = await loadInvoice(id);
  assert.equal(after.version, voided.version);
  assert.equal(after.sync_status, 'synced');
});

test('13. an edit made in QuickBooks between our payment and the read after it is still applied', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key, quickbooksId } = await createSynced(qbo);

  await invoices.update(id, { status: 'paid' });
  qbo.duringNext('POST payment', async () => {
    // Right after the payment, before the worker reads the invoice again: edited in QuickBooks
    qbo.duringNext('GET invoice/', async () => {
      qbo.edit(quickbooksId, { DueDate: '2027-10-01' });
      await notify(quickbooksId);
    });
  });
  await runJobs([key], qbo);

  const invoice = await loadInvoice(id);
  assert.equal(invoice.status, 'paid');
  assert.equal(invoice.due_date, '2027-10-01', 'the QuickBooks edit was skipped as already applied');
});

test('14. deleted locally while the create was UNKNOWN, then voided in QuickBooks: the deletion still reaches QuickBooks', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key, quickbooksId } = await createWithLostResponse(qbo);

  await invoices.remove(id); // waits behind the UNKNOWN create
  const remote = qbo.invoices.get(quickbooksId)!;
  await qbo.request(() => 'invoice', { method: 'POST', params: { operation: 'void' }, body: { Id: quickbooksId, SyncToken: remote.SyncToken } });
  await notify(quickbooksId, 'void'); // links it, voided
  await runJobs([qbKey(quickbooksId)], qbo);
  await runJobs([key], qbo);

  assert.ok(!qbo.invoices.has(quickbooksId), 'still in QuickBooks (voided, not deleted)');
});

test('15. the payment reached QuickBooks but the read after it failed: the retry does not fail it', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key } = await createSynced(qbo);

  const created = await payments.create(id, { amount: '100.00' }); // the whole balance
  assert.ok('payment' in created);
  qbo.duringNext('POST payment', async () => {
    qbo.failNext('GET invoice/', () => Object.assign(new Error('Service Unavailable'), { code: '503' }));
  });
  await runJobs([key], qbo);
  const pool = await getPool();
  await pool.query(sql.unsafe`UPDATE sync_jobs SET next_run_at = now() WHERE entity_key = ${key} AND status = 'PENDING'`);
  await runJobs([key], qbo);

  const [payment] = await payments.listForInvoice(id);
  assert.equal(payment.sync_status, 'synced', payment.sync_error ?? undefined);
  assert.equal(qbo.payments.size, 1);
});

test('16. changing the amount of an invoice not in QuickBooks yet changes its balance too', async () => {
  const { id } = await createLocal('sent', '100.00');
  const updated = await invoices.update(id, { amount: '50.00' });
  assert.equal(updated?.balance, '50.00');
});

test('17. a change committed while its predecessor job runs is still pushed (the API skipped queuing it)', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key, quickbooksId } = await createSynced(qbo);
  await invoices.update(id, { status: 'void' });
  await runJobs([key], qbo); // voided in QuickBooks
  const pool = await getPool();
  await enqueueInvoicePush(pool, id); // a job for it is waiting

  let processing: Promise<unknown> | undefined;
  // The worker runs outside the API's transaction (another process): its async scope is taken before it
  const outside = new AsyncResource('worker');
  await pool.transaction(async (tx) => {
    // The user deletes it: the API's enqueue finds that job waiting and skips...
    await tx.query(sql.unsafe`UPDATE invoices SET deleted_at = now(), version = version + 1, sync_status = 'pending' WHERE id = ${id}`);
    await enqueueInvoicePush(tx, id);
    // ...and before that commits, a worker claims the waiting job and loads the invoice (not deleted yet)
    processing = outside.runInAsyncScope(async () => {
      const job = (await claimNextJob({ workerId: 'worker', leaseSeconds: 60, entityKeys: [key] }))!;
      await processJob(job, qbo);
    });
    await sleep(300);
  });
  await processing;
  await runJobs([key], qbo);

  assert.ok(!qbo.invoices.has(quickbooksId), 'the deletion was never pushed');
});

test('18. a payment recorded and then its invoice deleted: nothing is paid in QuickBooks, and the invoice is deleted there', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key, quickbooksId } = await createSynced(qbo);
  await payments.create(id, { amount: '10.00' });
  await invoices.remove(id);
  await runJobs([key], qbo);

  assert.equal(qbo.payments.size, 0, 'the payment of a deleted invoice was recorded');
  assert.ok(!qbo.invoices.has(quickbooksId), 'voided instead of deleted');
});

test('19. a job failed by lease recovery (attempts used up) marks its notification failed too', async () => {
  const qbo = new FakeQuickBooks();
  const { key, quickbooksId } = await createSynced(qbo);
  await notify(quickbooksId);
  const job = (await claimNextJob({ workerId: 'dies', leaseSeconds: 60, entityKeys: [key] }))!;
  const pool = await getPool();
  await pool.query(sql.unsafe`UPDATE sync_jobs SET attempts = max_attempts, lease_expires_at = now() - interval '1 second' WHERE id = ${job.id}`);
  await recoverExpiredLeases();

  const status = await pool.oneFirst(sql.type(z.object({ processing_status: z.string() }))`
    SELECT processing_status FROM sync_events WHERE id = ${job.event_id}
  `);
  assert.equal(status, 'failed');
});

test('20. QuickBooks adds tax to an invoice we push: its content follows QuickBooks, no endless conflict', async () => {
  const qbo = new FakeQuickBooks();
  qbo.taxRate = 0.08;
  const { id, key, quickbooksId } = await createSynced(qbo); // 100.00 + 8.00 tax
  const pushed = await loadInvoice(id);
  assert.equal(pushed.amount, '108.00'); // what QuickBooks charges
  assert.equal(pushed.sync_status, 'synced');

  qbo.edit(quickbooksId, { DueDate: '2027-11-01' });
  await notify(quickbooksId);
  await runJobs([key], qbo);
  const after = await loadInvoice(id);
  assert.equal(after.sync_status, 'synced', after.sync_error ?? undefined);
  assert.equal(after.due_date, '2027-11-01');
});

test('21. an invoice with tax in QuickBooks: its content can\'t be edited here, its status still can', async () => {
  const qbo = new FakeQuickBooks();
  const quickbooksId = uniqueName('taxed').replace(/\W/g, '');
  qbo.addInvoice({ Id: quickbooksId, TotalAmt: 387, Balance: 387, TxnTaxDetail: { TotalTax: 22 } });
  await notify(quickbooksId, 'create');
  await runJobs([qbKey(quickbooksId)], qbo);
  const pool = await getPool();
  const local = await pool.one(sql.type(z.object({ id: z.number(), amount: z.string() }))`
    SELECT id, amount FROM invoices WHERE quickbooks_realm_id = ${TEST_REALM} AND quickbooks_id = ${quickbooksId}
  `);
  assert.equal(local.amount, '387.00');

  assert.equal(await invoices.update(local.id, { amount: '400.00' }), null); // would replace its taxed lines
  assert.ok(await invoices.update(local.id, { status: 'sent' }));
});

