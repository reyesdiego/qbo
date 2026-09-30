// Integration tests: require Postgres running (`npm run db:up`).
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import type { Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { sql } from 'slonik';
import { z } from 'zod';
import app from '../src/app';
import { getPool } from '../src/db';
import { armFault } from '../src/faults';
import { invoiceEntityKey } from '../src/sync.repository';
import { jobsFor, uniqueName } from './helpers';

let server: Server;
let baseUrl: string;

before(() => {
  server = app.listen(0);
  baseUrl = `http://localhost:${(server.address() as AddressInfo).port}`;
});

after(async () => {
  server.close();
  await (await getPool()).end();
});

const request = async (
  method: string,
  path: string,
  body?: unknown,
  headers: Record<string, string> = {},
) => {
  const res = await fetch(`${baseUrl}${path}`, {
    method,
    headers: { 'content-type': 'application/json', ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : null };
};

// POSTs that create something require an Idempotency-Key: a new one per request
const createInvoice = (body: unknown) => request('POST', '/invoices', body, { 'Idempotency-Key': randomUUID() });
const pay = (invoiceId: number, body: unknown) =>
  request('POST', `/invoices/${invoiceId}/payments`, body, { 'Idempotency-Key': randomUUID() });

test('full CRUD lifecycle of an invoice', async () => {
  const created = await createInvoice({
    customer_name: 'Acme Corp',
    amount: '1500',
    due_date: '2026-12-31',
  });
  assert.equal(created.status, 201);
  assert.equal(created.body.customer_name, 'Acme Corp');
  assert.equal(created.body.amount, '1500.00'); // exact decimal string, never a float
  assert.equal(created.body.balance, '1500.00');
  assert.equal(created.body.status, 'draft');
  assert.equal(created.body.currency, 'USD');
  assert.equal(created.body.quickbooks_id, null);
  assert.equal(created.body.sync_status, 'pending'); // queued for the sync worker
  const { id } = created.body;

  const fetched = await request('GET', `/invoices/${id}`);
  assert.equal(fetched.status, 200);
  assert.deepEqual(fetched.body, created.body);

  // The shared test database grows with each run, so page through the list
  let found = false;
  for (let offset = 0; !found; offset += 100) {
    const page = await request('GET', `/invoices?limit=100&offset=${offset}`);
    assert.equal(page.status, 200);
    if (page.body.length === 0) break;
    found = page.body.some((invoice: { id: number }) => invoice.id === id);
  }
  assert.ok(found);

  // quickbooks_id is only set by the sync worker, so clients can't change it
  const updated = await request('PATCH', `/invoices/${id}`, { status: 'sent', amount: '1499.99', quickbooks_id: '99' });
  assert.equal(updated.status, 200);
  assert.equal(updated.body.status, 'sent');
  assert.equal(updated.body.amount, '1499.99');
  assert.equal(updated.body.quickbooks_id, null);

  const deleted = await request('DELETE', `/invoices/${id}`);
  assert.equal(deleted.status, 204);

  const missing = await request('GET', `/invoices/${id}`);
  assert.equal(missing.status, 404);
});

test('an invoice and its sync job are saved atomically', async (t) => {
  await t.test('creating an invoice queues one OUTBOUND job in the same transaction', async () => {
    const created = await createInvoice({ customer_name: uniqueName('Outbox'), amount: '10.00', due_date: '2026-12-31' });
    const jobs = await jobsFor(invoiceEntityKey(created.body.id));
    assert.equal(jobs.length, 1);
    assert.equal(jobs[0].direction, 'OUTBOUND');
    assert.equal(jobs[0].status, 'PENDING');

    // Quick successive edits share the pending job: it loads the latest state when it runs
    await request('PATCH', `/invoices/${created.body.id}`, { amount: '11.00' });
    assert.equal((await jobsFor(invoiceEntityKey(created.body.id))).length, 1);
  });

  await t.test('if queuing the job fails, the invoice is not saved either', async () => {
    const customer = uniqueName('Rolled back');
    armFault('invoice.enqueue');
    const res = await createInvoice({ customer_name: customer, amount: '10.00', due_date: '2026-12-31' });
    assert.equal(res.status, 500);

    const pool = await getPool();
    const saved = await pool.maybeOne(sql.type(z.object({ id: z.number() }))`
      SELECT id FROM invoices WHERE customer_name = ${customer}
    `);
    assert.equal(saved, null);
  });
});

test('Idempotency-Key prevents duplicate invoices on retries', async () => {
  const body = { customer_name: 'Retry Co', amount: '50.00', due_date: '2026-12-31' };
  const headers = { 'Idempotency-Key': `test-${Date.now()}` };

  const first = await request('POST', '/invoices', body, headers);
  const retry = await request('POST', '/invoices', body, headers);
  assert.equal(first.status, 201);
  assert.equal(retry.status, 200); // not created again
  assert.equal(retry.body.id, first.body.id);
  assert.equal((await jobsFor(invoiceEntityKey(first.body.id))).length, 1); // and not queued twice

  // A different key is a different invoice
  const other = await createInvoice(body);
  assert.equal(other.status, 201);
  assert.notEqual(other.body.id, first.body.id);

  await request('DELETE', `/invoices/${first.body.id}`);
  await request('DELETE', `/invoices/${other.body.id}`);
});

test('POST /invoices without an Idempotency-Key: 400, nothing created', async () => {
  const customer = uniqueName('No key');
  const res = await request('POST', '/invoices', { customer_name: customer, amount: '10.00', due_date: '2026-12-31' });
  assert.equal(res.status, 400);
  assert.match(res.body.error, /Idempotency-Key/);

  const blank = await request('POST', '/invoices', { customer_name: customer, amount: '10.00', due_date: '2026-12-31' }, { 'Idempotency-Key': '' });
  assert.equal(blank.status, 400);

  const pool = await getPool();
  const saved = await pool.maybeOne(sql.type(z.object({ id: z.number() }))`SELECT id FROM invoices WHERE customer_name = ${customer}`);
  assert.equal(saved, null);
});

test('a paid invoice cannot be un-paid locally (the payment is deleted in QuickBooks)', async () => {
  const created = await createInvoice({ customer_name: uniqueName('Paid'), amount: '10.00', due_date: '2026-12-31', status: 'paid' });
  assert.equal(created.status, 201);
  assert.equal(created.body.status, 'paid');

  const unpaid = await request('PATCH', `/invoices/${created.body.id}`, { status: 'sent' });
  assert.equal(unpaid.status, 409);
  await request('DELETE', `/invoices/${created.body.id}`);
});

test('recording payments: POST /invoices/:id/payments', async (t) => {
  const created = await createInvoice({ customer_name: uniqueName('Payments'), amount: '100.00', due_date: '2026-12-31' });
  const id = created.body.id;

  await t.test('records a partial payment and queues it in the same transaction', async () => {
    const paid = await pay(id, { amount: '40' });
    assert.equal(paid.status, 201);
    assert.equal(paid.body.amount, '40.00');
    assert.equal(paid.body.sync_status, 'pending');
    const jobs = await jobsFor(invoiceEntityKey(id));
    assert.ok(jobs.some((job) => job.entity_type === 'payment' && job.entity_id === String(paid.body.id)));
  });

  await t.test('Idempotency-Key: a retry returns the same payment', async () => {
    const headers = { 'Idempotency-Key': `pay-${Date.now()}` };
    const first = await request('POST', `/invoices/${id}/payments`, { amount: '10.00' }, headers);
    const retry = await request('POST', `/invoices/${id}/payments`, { amount: '10.00' }, headers);
    assert.equal(first.status, 201);
    assert.equal(retry.status, 200);
    assert.equal(retry.body.id, first.body.id);
  });

  await t.test('more than what is left to pay: 422', async () => {
    const res = await pay(id, { amount: '50.01' }); // 100 - 40 - 10 = 50 left
    assert.equal(res.status, 422);
  });

  await t.test('if queuing the job fails, the payment is not saved either', async () => {
    armFault('invoice.enqueue');
    assert.equal((await pay(id, { amount: '1.00' })).status, 500);
    const listed = await request('GET', `/invoices/${id}/payments`);
    assert.deepEqual(listed.body.map((p: { amount: string }) => p.amount), ['40.00', '10.00']);
  });

  await t.test('DELETE /invoices/:id/payments/:paymentId', async () => {
    const paid = await pay(id, { amount: '5.00' });
    assert.equal((await request('DELETE', `/invoices/${id}/payments/${paid.body.id}`)).status, 204);
    const listed = await request('GET', `/invoices/${id}/payments`);
    assert.ok(!listed.body.some((p: { id: number }) => p.id === paid.body.id)); // hidden right away
    assert.equal((await request('DELETE', `/invoices/${id}/payments/${paid.body.id}`)).status, 404);
    const jobs = await jobsFor(invoiceEntityKey(id));
    assert.ok(jobs.some((job) => job.entity_type === 'payment' && job.operation === 'delete' && job.entity_id === String(paid.body.id)));
  });

  await t.test('without an Idempotency-Key: 400, nothing recorded', async () => {
    const before = (await request('GET', `/invoices/${id}/payments`)).body.length;
    const res = await request('POST', `/invoices/${id}/payments`, { amount: '1.00' });
    assert.equal(res.status, 400);
    assert.match(res.body.error, /Idempotency-Key/);
    assert.equal((await request('GET', `/invoices/${id}/payments`)).body.length, before);
  });

  await t.test('void or missing invoice, invalid amount', async () => {
    await request('PATCH', `/invoices/${id}`, { status: 'void' });
    assert.equal((await pay(id, { amount: '1.00' })).status, 409);
    assert.equal((await pay(2147483647, { amount: '1.00' })).status, 404);
    assert.equal((await pay(id, { amount: '0.001' })).status, 400);
  });
});

test('a voided invoice cannot be changed (409): QuickBooks does not allow editing it', async () => {
  const created = await createInvoice({ customer_name: uniqueName('Voided'), amount: '10.00', due_date: '2026-12-31' });
  assert.equal((await request('PATCH', `/invoices/${created.body.id}`, { status: 'void' })).status, 200);
  const edit = await request('PATCH', `/invoices/${created.body.id}`, { amount: '12.00' });
  assert.equal(edit.status, 409);
  assert.equal((await request('GET', `/invoices/${created.body.id}`)).body.amount, '10.00');
});

test('an invoice with tax in QuickBooks: its content can\'t be changed here (409), its status can', async () => {
  const created = await createInvoice({ customer_name: uniqueName('Taxed'), amount: '10.00', due_date: '2026-12-31' });
  const pool = await getPool();
  await pool.query(sql.unsafe`UPDATE invoices SET taxed_in_quickbooks = true WHERE id = ${created.body.id}`);

  const edit = await request('PATCH', `/invoices/${created.body.id}`, { amount: '12.00' });
  assert.equal(edit.status, 409);
  assert.match(edit.body.error, /tax/);
  assert.equal((await request('PATCH', `/invoices/${created.body.id}`, { status: 'sent' })).status, 200);
});

test('rejects invalid input with 400', async () => {
  const res = await createInvoice({ customer_name: '', amount: '-5' });
  assert.equal(res.status, 400);
  assert.equal(res.body.error, 'Validation failed');

  const tooManyDecimals = await createInvoice({ customer_name: 'A', amount: '10.001', due_date: '2026-12-31' });
  assert.equal(tooManyDecimals.status, 400);

  const floatAmount = await createInvoice({ customer_name: 'A', amount: 10.5, due_date: '2026-12-31' });
  assert.equal(floatAmount.status, 400); // amounts are decimal strings


  const emptyPatch = await request('PATCH', '/invoices/1', {});
  assert.equal(emptyPatch.status, 400);

  const badId = await request('GET', '/invoices/abc');
  assert.equal(badId.status, 400);
});
