// Integration tests against the real QuickBooks Developer Sandbox (`npm run test:sandbox`).
//
// Needs the API connected to a sandbox company (/quickbooks/connect), and no worker running: the
// tests process their own jobs. If webhooks are set up, Intuit's real notifications for these
// invoices can arrive meanwhile; they're queued for the same invoice and processed too.
// The tests use a dedicated customer and uniquely named invoices, delete the invoices they create,
// and never touch the sandbox's sample data. The customer is kept (QuickBooks doesn't delete
// customers) and reused on the next run.
// The consistency check compares the whole sandbox company with the local database: if they differ in
// something besides these tests' invoices, its repairs are queued too (for the worker, as it would do).
// The CDC test moves the main database's cursor back 40 days, so reconciliation runs a full import of the
// company (what the worker would do after such a stop); the cursor then moves forward again.
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import type { Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { sql } from 'slonik';
import { z } from 'zod';
import app from '../../src/app';
import { getPool } from '../../src/db';
import { classifyError } from '../../src/errors';
import { armFault } from '../../src/faults';
import * as invoices from '../../src/invoices.repository';
import * as payments from '../../src/payments.repository';
import { SyncInvoice } from '../../src/invoices.schema';
import { qbo, quote, type QboApi } from '../../src/quickbooks.client';
import { isTaxedInQuickBooks, isVoidedInQuickBooks, localInvoiceKey, parseLocalInvoiceKey, type QboInvoice } from '../../src/quickbooks.mapping';
import { checkConsistency } from '../../src/sync.consistency';
import { fetchInvoice } from '../../src/sync.outbound';
import { processJob } from '../../src/sync.processor';
import { enqueueRemoteChanges, reconcileUnknownJobs } from '../../src/sync.reconcile';
import { claimNextJob, invoiceEntityKey, recordEventAndEnqueue, SyncJob } from '../../src/sync.repository';

const CUSTOMER = 'Sync Sandbox Test Customer';
const created: number[] = [];

const loadInvoice = async (id: number) => {
  const pool = await getPool();
  return pool.one(sql.type(SyncInvoice)`SELECT * FROM invoices WHERE id = ${id}`);
};

// Processes all of the entity's pending jobs with the real QuickBooks API; returns them as they ended
const runJobsFor = async (entityKey: string, api: QboApi = qbo) => {
  const processed: string[] = [];
  for (;;) {
    const job = await claimNextJob({ workerId: 'sandbox-test', leaseSeconds: 300, entityKeys: [entityKey] });
    if (!job) break;
    await processJob(job, api);
    processed.push(job.id);
  }
  assert.ok(processed.length, 'No job to run: is `npm run worker` running? Stop it during the sandbox tests');
  const pool = await getPool();
  return pool.any(sql.type(SyncJob)`SELECT * FROM sync_jobs WHERE id = ANY(${sql.array(processed, 'uuid')}) ORDER BY created_at`);
};
const runJobs = (id: number, api?: QboApi) => runJobsFor(invoiceEntityKey(id), api);

// The real QuickBooks API, running `run` right before the first request that `matches` is sent: a
// local change made while the worker is talking to QuickBooks
const withChangeDuring = (matches: (path: string, method: string) => boolean, run: () => Promise<unknown>): QboApi => {
  let done = false;
  return {
    ...qbo,
    request: async (buildPath, options) => {
      if (!done && matches(buildPath((await qbo.realmId())!), options?.method ?? 'GET')) {
        done = true;
        await run();
      }
      return qbo.request(buildPath, options);
    },
  };
};

// An edit made in QuickBooks (not through this app)
const editDueDateInQuickBooks = async (quickbooksId: string, dueDate: string) => {
  const remote = await fetchRemote(quickbooksId);
  await qbo.request(() => 'invoice', { method: 'POST', body: { Id: quickbooksId, SyncToken: remote.SyncToken, sparse: true, DueDate: dueDate } });
};

// Deletes the invoice's payments (here and in QuickBooks), so the cleanup deletes the invoice instead
// of voiding it and no test payment is left in the sandbox
const deletePayments = async (id: number) => {
  for (const payment of await payments.listForInvoice(id)) assert.equal(await payments.remove(id, payment.id), 'deleted');
  await runJobs(id);
};

// What the webhook endpoint does with Intuit's notification
const notify = async (realmId: string, quickbooksId: string) => {
  const pool = await getPool();
  await pool.transaction((tx) =>
    recordEventAndEnqueue(tx, { source: 'webhook', realmId, entity: 'invoice', id: quickbooksId, operation: 'update', eventKey: randomUUID(), payload: {} }),
  );
};

const createLocal = async (amount: string) => {
  const { invoice } = await invoices.create({ customer_name: CUSTOMER, amount, currency: 'USD', status: 'sent', due_date: '2026-12-31' });
  created.push(invoice.id);
  return invoice.id;
};

// The local invoice is gone (not by the tests' cleanup): stop tracking it
const forget = (id: number) => created.splice(created.indexOf(id), 1);

const localIdFor = async (realmId: string, quickbooksId: string) => {
  const pool = await getPool();
  return pool.maybeOneFirst(sql.type(z.object({ id: z.number() }))`
    SELECT id FROM invoices WHERE quickbooks_realm_id = ${realmId} AND quickbooks_id = ${quickbooksId}
  `);
};

// The real API on a random port, for what's checked at the HTTP level (the required Idempotency-Key)
let server: Server;
let baseUrl = '';
before(() => {
  server = app.listen(0);
  baseUrl = `http://localhost:${(server.address() as AddressInfo).port}`;
});

const post = async (path: string, body: unknown, idempotencyKey?: string) => {
  const res = await fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...(idempotencyKey === undefined ? {} : { 'Idempotency-Key': idempotencyKey }) },
    body: JSON.stringify(body),
  });
  return { status: res.status, body: await res.json() };
};

const fetchRemote = (quickbooksId: string) => fetchInvoice(qbo, quickbooksId);

after(async () => {
  server.close();
  // Delete the test invoices, locally and in QuickBooks
  for (const id of created) {
    await invoices.remove(id);
    await runJobs(id);
  }
  await (await getPool()).end();
});

test('real QuickBooks sandbox', async (t) => {
  const realmId = await qbo.realmId();
  assert.ok(realmId, 'Not connected: open /quickbooks/connect first');
  let invoiceId = 0;
  let quickbooksId = '';

  await t.test('a local invoice is created in QuickBooks', async () => {
    invoiceId = await createLocal('123.45');
    await runJobs(invoiceId);

    const invoice = await loadInvoice(invoiceId);
    assert.equal(invoice.sync_status, 'synced', invoice.sync_error ?? undefined);
    quickbooksId = invoice.quickbooks_id!;
    const remote = await fetchRemote(quickbooksId);
    assert.equal(remote.TotalAmt, 123.45);
    assert.equal(remote.CustomerRef?.name, CUSTOMER);
    assert.equal(parseLocalInvoiceKey(remote.PrivateNote)?.key, localInvoiceKey(invoice));
  });

  await t.test('a local change is pushed', async () => {
    await invoices.update(invoiceId, { amount: '150.10' });
    await runJobs(invoiceId);
    assert.equal((await fetchRemote(quickbooksId)).TotalAmt, 150.1);
    assert.equal((await loadInvoice(invoiceId)).sync_status, 'synced');
  });

  await t.test('a change made in QuickBooks is applied locally (webhook path)', async () => {
    const remote = await fetchRemote(quickbooksId);
    await qbo.request(() => 'invoice', {
      method: 'POST',
      body: { Id: quickbooksId, SyncToken: remote.SyncToken, sparse: true, DueDate: '2027-02-01' },
    });
    await notify(realmId!, quickbooksId);
    await runJobs(invoiceId);

    const invoice = await loadInvoice(invoiceId);
    assert.equal(invoice.due_date, '2027-02-01');
    assert.equal(invoice.sync_status, 'synced');
  });

  await t.test('a duplicate notification changes nothing (no loop)', async () => {
    const before = await loadInvoice(invoiceId);
    await notify(realmId!, quickbooksId);
    const jobs = await runJobs(invoiceId);

    assert.ok(jobs.every((job) => job.direction === 'INBOUND' && job.payload.result === 'already up to date'));
    const after = await loadInvoice(invoiceId);
    assert.equal(after.quickbooks_sync_token, before.quickbooks_sync_token);
    assert.equal(after.version, before.version); // nothing to push back
  });

  await t.test('a create whose response is lost is reconciled, not duplicated', async () => {
    const id = await createLocal('77.77');
    armFault('qbo.create.response-lost'); // QuickBooks creates it, we never see the response
    const [createJob] = await runJobs(id);
    assert.equal(createJob.status, 'UNKNOWN');

    const pool = await getPool();
    await pool.query(sql.unsafe`UPDATE sync_jobs SET create_sent_at = now() - interval '10 minutes' WHERE id = ${createJob.id}`);
    await reconcileUnknownJobs(qbo, { workerId: 'sandbox-test', leaseSeconds: 300, graceSeconds: 60, entityKeys: [invoiceEntityKey(id)] });

    const invoice = await loadInvoice(id);
    assert.equal(invoice.sync_status, 'synced', invoice.sync_error ?? undefined);
    const job = await pool.one(sql.type(SyncJob)`SELECT * FROM sync_jobs WHERE id = ${createJob.id}`);
    assert.equal(job.status, 'COMPLETED');

    // Exactly one QuickBooks invoice carries its reference
    const remote = await fetchRemote(invoice.quickbooks_id!);
    const all = await qbo.query(`SELECT * FROM Invoice WHERE CustomerRef = ${quote(remote.CustomerRef!.value!)}`);
    const withReference = ((all.QueryResponse.Invoice ?? []) as QboInvoice[]).filter(
      (i) => parseLocalInvoiceKey(i.PrivateNote)?.key === localInvoiceKey(invoice),
    );
    assert.equal(withReference.length, 1);
  });

  await t.test('consistency check: an invoice deleted by hand in the database comes back from QuickBooks', async () => {
    const id = await createLocal('88.88');
    await runJobs(id);
    const quickbooksId = (await loadInvoice(id)).quickbooks_id!;

    const pool = await getPool();
    await pool.query(sql.unsafe`DELETE FROM invoices WHERE id = ${id}`); // the manual delete
    forget(id);

    const report = await checkConsistency(qbo);
    assert.ok(report.missing_locally.includes(quickbooksId), JSON.stringify(report));
    await runJobsFor(`qbo-invoice:${realmId}:${quickbooksId}`);

    const back = await localIdFor(realmId!, quickbooksId);
    assert.ok(back, 'not re-imported');
    created.push(back); // the cleanup deletes it, here and in QuickBooks
    const invoice = await loadInvoice(back);
    assert.equal(invoice.amount, '88.88');
    assert.equal(invoice.customer_name, CUSTOMER);
    assert.equal(invoice.sync_status, 'synced');
  });

  await t.test('consistency check: an invoice deleted in QuickBooks without a notification is deleted locally', async () => {
    const id = await createLocal('66.66');
    await runJobs(id);
    const invoice = await loadInvoice(id);
    const remote = await fetchRemote(invoice.quickbooks_id!);
    await qbo.request(() => 'invoice', {
      method: 'POST',
      params: { operation: 'delete' },
      body: { Id: remote.Id, SyncToken: remote.SyncToken },
    }); // deleted in QuickBooks; no notify(): as if the webhook was missed

    const report = await checkConsistency(qbo);
    assert.ok(report.missing_in_quickbooks.some((m) => m.invoice_id === id), JSON.stringify(report));
    await runJobs(id);
    assert.equal(await localIdFor(realmId!, invoice.quickbooks_id!), null);
    forget(id);
  });

  await t.test('consistency check: afterwards nothing of these tests is missing on either side', async () => {
    const report = await checkConsistency(qbo, { repair: false });
    const ours = await Promise.all(created.map(async (id) => (await loadInvoice(id)).quickbooks_id!));
    assert.ok(!ours.some((q) => report.missing_locally.includes(q)), JSON.stringify(report));
    assert.ok(!report.missing_in_quickbooks.some((m) => created.includes(m.invoice_id)), JSON.stringify(report));
  });

  await t.test('POST /invoices: no Idempotency-Key → 400; a retry with the same key → one invoice in QuickBooks', async () => {
    const body = { customer_name: CUSTOMER, amount: '44.44', status: 'sent', due_date: '2026-12-31' };
    const withoutKey = await post('/invoices', body);
    assert.equal(withoutKey.status, 400);
    assert.match(withoutKey.body.error, /Idempotency-Key/);

    const key = randomUUID();
    const first = await post('/invoices', body, key);
    assert.equal(first.status, 201);
    created.push(first.body.id);
    const retry = await post('/invoices', body, key); // e.g. after a timeout
    assert.equal(retry.status, 200);
    assert.equal(retry.body.id, first.body.id);

    await runJobs(first.body.id);
    const invoice = await loadInvoice(first.body.id);
    assert.equal(invoice.sync_status, 'synced', invoice.sync_error ?? undefined);
    const remote = await fetchRemote(invoice.quickbooks_id!);
    const all = await qbo.query(`SELECT * FROM Invoice WHERE CustomerRef = ${quote(remote.CustomerRef!.value!)}`);
    const withReference = ((all.QueryResponse.Invoice ?? []) as QboInvoice[]).filter(
      (i) => parseLocalInvoiceKey(i.PrivateNote)?.key === localInvoiceKey(invoice),
    );
    assert.equal(withReference.length, 1);
  });

  await t.test('POST /invoices/:id/payments: no Idempotency-Key → 400; a retry with the same key → one payment in QuickBooks', async () => {
    const id = await createLocal('20.00');
    await runJobs(id);

    const withoutKey = await post(`/invoices/${id}/payments`, { amount: '5.00' });
    assert.equal(withoutKey.status, 400);
    assert.match(withoutKey.body.error, /Idempotency-Key/);

    const key = randomUUID();
    const first = await post(`/invoices/${id}/payments`, { amount: '5.00' }, key);
    assert.equal(first.status, 201);
    const retry = await post(`/invoices/${id}/payments`, { amount: '5.00' }, key);
    assert.equal(retry.status, 200);
    assert.equal(retry.body.id, first.body.id);

    await runJobs(id);
    const remote = await fetchRemote((await loadInvoice(id)).quickbooks_id!);
    assert.equal((remote.LinkedTxn ?? []).filter((txn) => txn.TxnType === 'Payment').length, 1);
    assert.equal(remote.Balance, 15);

    // Delete the payment, so the cleanup can delete the invoice in QuickBooks (not just void it)
    const res = await fetch(`${baseUrl}/invoices/${id}/payments/${first.body.id}`, { method: 'DELETE' });
    assert.equal(res.status, 204);
    await runJobs(id);
  });

  // Races between a local change and a job that is running or waiting (see test/sync.races.test.ts)
  await t.test('race: marked "paid" while the worker pushes an earlier change → the payment reaches QuickBooks', async () => {
    const id = await createLocal('50.00');
    await runJobs(id);
    const quickbooksId = (await loadInvoice(id)).quickbooks_id!;

    await invoices.update(id, { amount: '60.00' });
    const api = withChangeDuring((path, method) => method === 'POST' && path === 'invoice', () => invoices.update(id, { status: 'paid' }));
    await runJobs(id, api);

    assert.equal((await fetchRemote(quickbooksId)).Balance, 0);
    const invoice = await loadInvoice(id);
    assert.equal(invoice.status, 'paid');
    assert.equal(invoice.sync_status, 'synced', invoice.sync_error ?? undefined);
    await deletePayments(id);
  });

  await t.test('race: a payment synced before a pending QuickBooks edit → the edit is still applied', async () => {
    const id = await createLocal('40.00');
    await runJobs(id);
    const quickbooksId = (await loadInvoice(id)).quickbooks_id!;

    await payments.create(id, { amount: '10.00' }); // its job runs first
    await editDueDateInQuickBooks(quickbooksId, '2027-05-01');
    await notify(realmId!, quickbooksId);
    await runJobs(id);

    const invoice = await loadInvoice(id);
    assert.equal(invoice.balance, '30.00');
    assert.equal(invoice.due_date, '2027-05-01');
    await deletePayments(id);
  });

  await t.test('race: deleted while the create was UNKNOWN, resolved by reconciliation → deleted in QuickBooks', async () => {
    const id = await createLocal('33.33');
    armFault('qbo.create.response-lost');
    const [createJob] = await runJobs(id);
    assert.equal(createJob.status, 'UNKNOWN');

    await invoices.remove(id); // waits behind the UNKNOWN create
    const pool = await getPool();
    await pool.query(sql.unsafe`UPDATE sync_jobs SET create_sent_at = now() - interval '10 minutes' WHERE id = ${createJob.id}`);
    await reconcileUnknownJobs(qbo, { workerId: 'sandbox-test', leaseSeconds: 300, graceSeconds: 60, entityKeys: [invoiceEntityKey(id)] });
    const quickbooksId = (await loadInvoice(id)).quickbooks_id!;
    assert.ok(quickbooksId, 'not found by reconciliation');
    await runJobs(id);
    forget(id); // already deleted

    await assert.rejects(fetchRemote(quickbooksId), (err) => classifyError(err).errorClass === 'not_found');
  });

  await t.test('race: voided locally while a QuickBooks edit is applied first → voided in QuickBooks', async () => {
    const id = await createLocal('22.22');
    await runJobs(id);
    const quickbooksId = (await loadInvoice(id)).quickbooks_id!;

    await editDueDateInQuickBooks(quickbooksId, '2027-06-01');
    await notify(realmId!, quickbooksId); // its job runs before the void's
    await invoices.update(id, { status: 'void' });
    await runJobs(id);

    assert.ok(isVoidedInQuickBooks(await fetchRemote(quickbooksId)), 'not voided in QuickBooks');
    const invoice = await loadInvoice(id);
    assert.equal(invoice.status, 'void');
    assert.equal(invoice.due_date, '2027-06-01');
  });

  await t.test('race: only the status changed locally, the content only in QuickBooks → no conflict, the payment is pushed', async () => {
    const id = await createLocal('11.11');
    await runJobs(id);
    const quickbooksId = (await loadInvoice(id)).quickbooks_id!;

    await invoices.update(id, { status: 'paid' }); // its job runs before the edit's notification
    await editDueDateInQuickBooks(quickbooksId, '2027-07-01');
    await notify(realmId!, quickbooksId);
    await runJobs(id);

    const invoice = await loadInvoice(id);
    assert.equal(invoice.sync_status, 'synced', invoice.sync_error ?? undefined);
    assert.equal(invoice.due_date, '2027-07-01');
    assert.equal(invoice.status, 'paid');
    assert.equal((await fetchRemote(quickbooksId)).Balance, 0);
    await deletePayments(id);
  });

  // Fixes from the second code review
  const paidInvoice = async (amount: string) => {
    const id = await createLocal(amount);
    await runJobs(id);
    await invoices.update(id, { status: 'paid' });
    await runJobs(id);
    const quickbooksId = (await loadInvoice(id)).quickbooks_id!;
    assert.equal((await fetchRemote(quickbooksId)).Balance, 0);
    return { id, quickbooksId };
  };
  const paymentsInQuickBooks = async (quickbooksId: string) =>
    ((await fetchRemote(quickbooksId)).LinkedTxn ?? []).filter((txn) => txn.TxnType === 'Payment').length;

  await t.test('review: a payment deleted in QuickBooks is not paid again by a local edit', async () => {
    const { id, quickbooksId } = await paidInvoice('25.00');
    const [local] = await payments.listForInvoice(id);
    const payment = (await qbo.request(() => `payment/${local.quickbooks_id}`)).Payment;
    await qbo.request(() => 'payment', { method: 'POST', params: { operation: 'delete' }, body: { Id: payment.Id, SyncToken: payment.SyncToken } });

    await invoices.update(id, { due_date: '2027-08-01' }); // before the deletion's notification is applied
    await runJobs(id);

    assert.equal(await paymentsInQuickBooks(quickbooksId), 0, 'a payment nobody made was recorded');
    const invoice = await loadInvoice(id);
    assert.equal(invoice.status, 'sent');
    assert.equal(invoice.balance, '25.00');
    await deletePayments(id); // the local row of the deleted payment (already gone in QuickBooks)
  });

  await t.test('review: raising the amount of a paid invoice does not pay the difference', async () => {
    const { id, quickbooksId } = await paidInvoice('20.00');

    await invoices.update(id, { amount: '30.00' });
    await runJobs(id);

    assert.equal(await paymentsInQuickBooks(quickbooksId), 1, 'a payment nobody made was recorded');
    assert.equal((await fetchRemote(quickbooksId)).Balance, 10);
    const invoice = await loadInvoice(id);
    assert.equal(invoice.status, 'sent');
    assert.equal(invoice.balance, '10.00');
    await deletePayments(id);
  });

  await t.test('review: QuickBooks rejects a CDC cursor older than 30 days, and reconciliation catches up anyway', async () => {
    const old = new Date(Date.now() - 40 * 24 * 60 * 60 * 1000).toISOString();
    await assert.rejects(qbo.request(() => 'cdc', { params: { entities: 'Invoice', changedSince: old } }));

    const pool = await getPool();
    await pool.query(sql.unsafe`UPDATE quickbooks_connection SET cdc_cursor = ${old}::timestamptz`);
    await enqueueRemoteChanges(qbo); // a full import instead of CDC
    const cursor = await pool.oneFirst(sql.type(z.object({ cdc_cursor: z.string() }))`SELECT cdc_cursor FROM quickbooks_connection`);
    assert.ok(Date.parse(cursor) > Date.now() - 60 * 60 * 1000, 'the cursor did not move forward');
  });

  // From the cleanup: the SyncToken only comes from our own write, never from the invoice read after a payment
  await t.test('cleanup: an edit made in QuickBooks right after our payment is still applied locally', async () => {
    const id = await createLocal('15.00');
    await runJobs(id);
    const quickbooksId = (await loadInvoice(id)).quickbooks_id!;

    await invoices.update(id, { status: 'paid' });
    // Someone edits the invoice in QuickBooks between our payment and the read that follows it
    let paymentSent = false;
    const api = withChangeDuring(
      (path, method) => {
        if (method === 'POST' && path === 'payment') paymentSent = true;
        return paymentSent && method === 'GET' && path === `invoice/${quickbooksId}`;
      },
      async () => {
        await editDueDateInQuickBooks(quickbooksId, '2027-10-01');
        await notify(realmId!, quickbooksId);
      },
    );
    await runJobs(id, api); // the push, then the edit's notification

    const invoice = await loadInvoice(id);
    assert.equal(invoice.status, 'paid');
    assert.equal(invoice.due_date, '2027-10-01', 'the QuickBooks edit was skipped as already applied');
    assert.equal(invoice.sync_status, 'synced', invoice.sync_error ?? undefined);
    await deletePayments(id);
  });

  // Fixes from the fourth code review
  await t.test('review 4: a taxed QuickBooks invoice is recognized (real TxnTaxDetail) and its content is read-only here', async () => {
    // A sample invoice with sales tax, only read: the sandbox's sample data isn't changed
    const taxed = ((await qbo.query('SELECT * FROM Invoice MAXRESULTS 100')).QueryResponse.Invoice as QboInvoice[]).find(isTaxedInQuickBooks);
    assert.ok(taxed, 'the sandbox company has no invoice with sales tax');
    const localId = await localIdFor(realmId!, taxed.Id);
    assert.ok(localId, `QuickBooks invoice ${taxed.Id} isn't imported locally`);
    const local = await loadInvoice(localId);
    assert.equal(local.taxed_in_quickbooks, true);
    assert.equal(Number(local.amount), taxed.TotalAmt); // the amount with the tax

    const res = await fetch(`${baseUrl}/invoices/${localId}`, {
      method: 'PATCH',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ amount: '1.00' }),
    });
    assert.equal(res.status, 409);
    assert.equal((await loadInvoice(localId)).version, local.version); // nothing changed, nothing queued
  });

  await t.test('review 4: a payment recorded and then its invoice deleted → nothing paid, the invoice deleted (not voided)', async () => {
    const id = await createLocal('18.00');
    await runJobs(id);
    const quickbooksId = (await loadInvoice(id)).quickbooks_id!;

    await payments.create(id, { amount: '5.00' });
    await invoices.remove(id);
    await runJobs(id);
    forget(id); // already deleted

    await assert.rejects(fetchRemote(quickbooksId), (err) => classifyError(err).errorClass === 'not_found');
  });

  await t.test('review 4: a QuickBooks payment with two lines for the same invoice is listed with their sum', async () => {
    const id = await createLocal('50.00');
    await runJobs(id);
    const invoice = await loadInvoice(id);
    const remote = await fetchRemote(invoice.quickbooks_id!);
    const payment = (await qbo.request(() => 'payment', {
      method: 'POST',
      body: {
        CustomerRef: { value: remote.CustomerRef!.value },
        TotalAmt: 30,
        Line: [
          { Amount: 20, LinkedTxn: [{ TxnId: remote.Id, TxnType: 'Invoice' }] },
          { Amount: 10, LinkedTxn: [{ TxnId: remote.Id, TxnType: 'Invoice' }] },
        ],
      },
    })).Payment; // made in QuickBooks, not through this API
    const pool = await getPool();
    await pool.transaction((tx) =>
      recordEventAndEnqueue(tx, { source: 'webhook', realmId: realmId!, entity: 'payment', id: payment.Id, operation: 'create', eventKey: randomUUID(), payload: {} }),
    );
    await runJobsFor(`qbo-payment:${realmId}:${payment.Id}`);

    assert.deepEqual((await payments.listForInvoice(id)).map((p) => p.amount), ['30.00']);
    await deletePayments(id);
  });
});

