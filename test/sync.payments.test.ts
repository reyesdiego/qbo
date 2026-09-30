// Payments recorded locally (full or partial) -> QuickBooks, against an in-memory fake QuickBooks
import { test, after, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { sql } from 'slonik';
import { z } from 'zod';
import { getPool } from '../src/db';
import { armFault, disarmFaults } from '../src/faults';
import * as invoices from '../src/invoices.repository';
import * as payments from '../src/payments.repository';
import { localPaymentKey } from '../src/quickbooks.mapping';
import { enqueueRemoteChanges, reconcileUnknownJobs } from '../src/sync.reconcile';
import { invoiceEntityKey, listJobs, recordEventAndEnqueue } from '../src/sync.repository';
import { FakeQuickBooks, jobsFor, loadInvoice, qboErrors, runJobs, TEST_REALM, uniqueName, ensureTestConnection } from './helpers';

after(async () => {
  await (await getPool()).end();
});
beforeEach(() => disarmFaults());

// A local invoice of 80.00, synced to the fake QuickBooks
const syncedInvoice = async (qbo: FakeQuickBooks) => {
  const { invoice } = await invoices.create({ customer_name: uniqueName('Partial'), amount: '80.00', currency: 'USD', status: 'sent', due_date: '2026-12-31' });
  const key = invoiceEntityKey(invoice.id);
  await runJobs([key], qbo);
  return { id: invoice.id, key, quickbooksId: (await loadInvoice(invoice.id)).quickbooks_id! };
};

const pay = async (invoiceId: number, amount: string) => {
  const result = await payments.create(invoiceId, { amount });
  assert.equal(result.outcome, 'created');
  return (result as { payment: payments.Payment }).payment;
};

const loadPayment = async (id: number) => (await payments.listForInvoice(0)).find((p) => p.id === id) ?? null;
const paymentsOf = async (invoiceId: number) => payments.listForInvoice(invoiceId);

test('partial payments recorded locally reach QuickBooks, and the balance comes back', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key, quickbooksId } = await syncedInvoice(qbo);

  const first = await pay(id, '30');
  assert.equal(first.amount, '30.00');
  assert.equal(first.sync_status, 'pending');
  await runJobs([key], qbo);

  assert.equal(qbo.invoices.get(quickbooksId)!.Balance, 50);
  const [synced] = await paymentsOf(id);
  assert.equal(synced.sync_status, 'synced');
  assert.ok(synced.quickbooks_id);
  let invoice = await loadInvoice(id);
  assert.equal(invoice.balance, '50.00');
  assert.equal(invoice.status, 'sent');

  await pay(id, '50.00');
  await runJobs([key], qbo);
  invoice = await loadInvoice(id);
  assert.equal(invoice.balance, '0.00');
  assert.equal(invoice.status, 'paid');
  assert.equal(qbo.countCalls('POST payment'), 2);
});

test('a payment larger than what is left to pay is rejected (unsynced payments count)', async () => {
  const qbo = new FakeQuickBooks();
  const { id } = await syncedInvoice(qbo);
  await pay(id, '30.00'); // not synced yet

  const result = await payments.create(id, { amount: '60.00' });
  assert.equal(result.outcome, 'exceeds_balance');
  assert.equal((result as { available: string }).available, '50.00');
});

test('a payment recorded before the invoice reached QuickBooks waits for it', async () => {
  const qbo = new FakeQuickBooks();
  const { invoice } = await invoices.create({ customer_name: uniqueName('Early'), amount: '20.00', currency: 'USD', status: 'sent', due_date: '2026-12-31' });
  await pay(invoice.id, '20.00');
  await runJobs([invoiceEntityKey(invoice.id)], qbo); // the invoice's create runs first, then the payment

  const synced = await loadInvoice(invoice.id);
  assert.equal(qbo.invoices.get(synced.quickbooks_id!)!.Balance, 0);
  assert.equal(synced.status, 'paid');
});

test('ambiguous payment (QuickBooks recorded it, the response was lost)', async (t) => {
  const qbo = new FakeQuickBooks();
  const { id, key, quickbooksId } = await syncedInvoice(qbo);
  const reconcile = async () => {
    const pool = await getPool();
    await pool.query(sql.unsafe`UPDATE sync_jobs SET create_sent_at = now() - interval '10 minutes' WHERE entity_key = ${key} AND status = 'UNKNOWN'`);
    await reconcileUnknownJobs(qbo, { workerId: 'test-worker', leaseSeconds: 60, graceSeconds: 60, entityKeys: [key] });
  };

  await t.test('the job becomes UNKNOWN and the payment is not resent', async () => {
    const payment = await pay(id, '25.00');
    armFault('qbo.payment.response-lost');
    await runJobs([key], qbo);

    assert.equal((await jobsFor(key)).at(-1)!.status, 'UNKNOWN');
    assert.equal((await paymentsOf(id)).find((p) => p.id === payment.id)!.sync_status, 'unknown');
    await runJobs([key], qbo);
    assert.equal(qbo.countCalls('POST payment'), 1);
  });

  await t.test('reconciliation finds it by its reference: no second payment', async () => {
    await reconcile();
    assert.equal((await jobsFor(key)).at(-1)!.status, 'COMPLETED');
    const [payment] = await paymentsOf(id);
    assert.equal(payment.sync_status, 'synced');
    assert.equal(qbo.countCalls('POST payment'), 1);
    assert.equal(qbo.invoices.get(quickbooksId)!.Balance, 55);
    assert.equal((await loadInvoice(id)).balance, '55.00');
  });

  await t.test('a payment that never reached QuickBooks is sent again after reconciliation', async () => {
    await pay(id, '5.00');
    qbo.failNext('POST payment', qboErrors.timeout); // times out before QuickBooks records it
    await runJobs([key], qbo);
    await reconcile(); // not found: back to PENDING
    await runJobs([key], qbo);

    assert.equal(qbo.invoices.get(quickbooksId)!.Balance, 50);
    assert.ok((await paymentsOf(id)).every((p) => p.sync_status === 'synced'));
  });
});

test('a payment deleted in QuickBooks is deleted locally', async () => {
  await ensureTestConnection();
  const qbo = new FakeQuickBooks();
  const { id, key } = await syncedInvoice(qbo);
  await pay(id, '10.00');
  await runJobs([key], qbo);
  const [payment] = await paymentsOf(id);

  const pool = await getPool();
  await pool.transaction((tx) =>
    recordEventAndEnqueue(tx, { source: 'webhook', realmId: TEST_REALM, entity: 'payment', id: payment.quickbooks_id!, operation: 'delete', eventKey: uniqueName('evt'), payload: {} }),
  );
  await runJobs([`qbo-payment:${TEST_REALM}:${payment.quickbooks_id}`], qbo);
  assert.equal((await paymentsOf(id)).length, 0);
  assert.equal(await loadPayment(payment.id), null);
});

test('GET /sync/jobs?invoice_id= includes the invoice\'s payment jobs', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key } = await syncedInvoice(qbo);
  const payment = await pay(id, '10.00');
  await runJobs([key], qbo);

  const jobs = await listJobs({ invoiceId: id, limit: 50 });
  const paymentJob = jobs.find((job) => job.entity_type === 'payment')!;
  assert.equal(paymentJob.entity_id, String(payment.id));
  assert.equal(paymentJob.local_invoice_id, id);
  assert.equal(paymentJob.quickbooks_id, (await paymentsOf(id))[0].quickbooks_id);
});

test('a pending payment job does not swallow a change of its invoice', async () => {
  const qbo = new FakeQuickBooks();
  const { id, key, quickbooksId } = await syncedInvoice(qbo);
  const payment = await pay(id, '10.00');
  await runJobs([key], qbo);
  await payments.remove(id, payment.id); // payment job pending, same entity key as the invoice
  await invoices.remove(id); // must still queue the invoice's own deletion

  const pending = (await jobsFor(key)).filter((job) => job.status === 'PENDING');
  assert.deepEqual(pending.map((job) => `${job.entity_type} ${job.operation}`), ['payment delete', 'invoice delete']);
  await runJobs([key], qbo);
  assert.equal(qbo.invoices.has(quickbooksId), false);
});

test('deleting payments locally', async (t) => {
  const qbo = new FakeQuickBooks();

  await t.test('a synced payment is deleted in QuickBooks and locally, and the balance comes back', async () => {
    const { id, key, quickbooksId } = await syncedInvoice(qbo);
    const payment = await pay(id, '80.00');
    await runJobs([key], qbo);
    assert.equal((await loadInvoice(id)).status, 'paid');
    const quickbooksPaymentId = (await paymentsOf(id))[0].quickbooks_id!;

    assert.equal(await payments.remove(id, payment.id), 'deleted');
    assert.equal((await paymentsOf(id)).length, 0); // hidden right away
    await runJobs([key], qbo);

    assert.equal(qbo.payments.has(quickbooksPaymentId), false);
    assert.equal(qbo.invoices.get(quickbooksId)!.Balance, 80);
    const invoice = await loadInvoice(id);
    assert.equal(invoice.balance, '80.00');
    assert.equal(invoice.status, 'sent');
    const pool = await getPool();
    const rows = await pool.any(sql.type(payments.SyncPayment)`SELECT * FROM invoice_payments WHERE id = ${payment.id}`);
    assert.equal(rows.length, 0);
    assert.equal((await jobsFor(key)).at(-1)!.payload.result, 'deleted in QuickBooks');
  });

  await t.test('a payment deleted before it reached QuickBooks is never sent', async () => {
    const { id, key } = await syncedInvoice(qbo);
    const before = qbo.countCalls('POST payment');
    const payment = await pay(id, '10.00');
    await payments.remove(id, payment.id); // its create job is still pending
    await runJobs([key], qbo);

    assert.equal(qbo.countCalls('POST payment'), before);
    const results = (await jobsFor(key)).slice(-2).map((job) => job.payload.result);
    assert.deepEqual(results, ['deleted before reaching QuickBooks', 'never reached QuickBooks: deleted locally']);
  });

  await t.test('a payment already deleted in QuickBooks completes without retrying', async () => {
    const { id, key } = await syncedInvoice(qbo);
    const payment = await pay(id, '10.00');
    await runJobs([key], qbo);
    qbo.payments.delete((await paymentsOf(id))[0].quickbooks_id!);

    await payments.remove(id, payment.id);
    await runJobs([key], qbo);
    const job = (await jobsFor(key)).at(-1)!;
    assert.equal(job.status, 'COMPLETED');
    assert.equal(job.payload.result, 'already deleted in QuickBooks');
  });

  await t.test('the payment of an invoice marked "paid" can be deleted too', async () => {
    const { id, key, quickbooksId } = await syncedInvoice(qbo);
    await invoices.update(id, { status: 'paid' });
    await runJobs([key], qbo);
    const [recorded] = await paymentsOf(id); // recorded locally when the worker paid it
    assert.equal(recorded.amount, '80.00');
    assert.equal(recorded.sync_status, 'synced');

    await payments.remove(id, recorded.id);
    await runJobs([key], qbo);
    assert.equal(qbo.invoices.get(quickbooksId)!.Balance, 80);
    assert.equal((await loadInvoice(id)).status, 'sent');
  });
});

test('payments made in QuickBooks are listed locally', async (t) => {
  await ensureTestConnection();
  const qbo = new FakeQuickBooks();
  // A payment made in QuickBooks (not through this API)
  const payInQuickBooks = async (lines: { quickbooksId: string; amount: number }[], note?: string) =>
    (await qbo.request(() => 'payment', {
      method: 'POST',
      body: {
        CustomerRef: { value: '1' },
        TxnDate: '2026-09-15',
        PrivateNote: note,
        Line: lines.map((l) => ({ Amount: l.amount, LinkedTxn: [{ TxnId: l.quickbooksId, TxnType: 'Invoice' }] })),
      },
    })).Payment;
  const notifyPayment = async (id: string, operation = 'create') => {
    const pool = await getPool();
    await pool.transaction((tx) =>
      recordEventAndEnqueue(tx, { source: 'webhook', realmId: TEST_REALM, entity: 'payment', id, operation, eventKey: uniqueName('evt'), payload: {} }),
    );
    await runJobs([`qbo-payment:${TEST_REALM}:${id}`], qbo);
  };

  await t.test('a partial payment made in QuickBooks appears in the invoice\'s payments', async () => {
    const { id, quickbooksId } = await syncedInvoice(qbo);
    const payment = await payInQuickBooks([{ quickbooksId, amount: 30 }]);
    await notifyPayment(payment.Id);

    const [listed] = await paymentsOf(id);
    assert.deepEqual(
      { amount: listed.amount, paid_on: listed.paid_on, quickbooks_id: listed.quickbooks_id, sync_status: listed.sync_status },
      { amount: '30.00', paid_on: '2026-09-15', quickbooks_id: payment.Id, sync_status: 'synced' },
    );
    assert.equal((await loadInvoice(id)).balance, '50.00');

    // Changed in QuickBooks: the listed amount follows
    qbo.payments.set(payment.Id, { ...qbo.payments.get(payment.Id)!, Line: [{ Amount: 35, LinkedTxn: [{ TxnId: quickbooksId, TxnType: 'Invoice' }] }] });
    await notifyPayment(payment.Id, 'update');
    assert.equal((await paymentsOf(id))[0].amount, '35.00');
  });

  await t.test('a payment of several invoices is listed on each, and can only be deleted in QuickBooks', async () => {
    const a = await syncedInvoice(qbo);
    const b = await syncedInvoice(qbo);
    const payment = await payInQuickBooks([{ quickbooksId: a.quickbooksId, amount: 20 }, { quickbooksId: b.quickbooksId, amount: 60 }]);
    await notifyPayment(payment.Id);

    assert.equal((await paymentsOf(a.id))[0].amount, '20.00');
    assert.equal((await paymentsOf(b.id))[0].amount, '60.00');
    assert.equal(await payments.remove(a.id, (await paymentsOf(a.id))[0].id), 'shared');

    // Deleted in QuickBooks: gone from both
    qbo.payments.delete(payment.Id);
    await notifyPayment(payment.Id, 'delete');
    assert.equal((await paymentsOf(a.id)).length, 0);
    assert.equal((await paymentsOf(b.id)).length, 0);
  });

  await t.test('GET /sync/jobs lists the job of a payment of several invoices once', async () => {
    const a = await syncedInvoice(qbo);
    const b = await syncedInvoice(qbo);
    const payment = await payInQuickBooks([{ quickbooksId: a.quickbooksId, amount: 10 }, { quickbooksId: b.quickbooksId, amount: 10 }]);
    await notifyPayment(payment.Id);

    const jobs = (await listJobs({ limit: 1000 })).filter((job) => job.direction === 'INBOUND' && job.entity_id === payment.Id);
    assert.equal(jobs.length, 1);
    // Filtered by invoice, it shows that invoice's share
    assert.equal((await listJobs({ invoiceId: b.id, limit: 50 })).filter((job) => job.entity_id === payment.Id)[0]?.local_invoice_id, b.id);
  });

  await t.test('a payment with two lines for the same invoice is listed with their sum', async () => {
    const { id, quickbooksId } = await syncedInvoice(qbo);
    const payment = await payInQuickBooks([{ quickbooksId, amount: 30 }, { quickbooksId, amount: 20 }]);
    await notifyPayment(payment.Id);
    assert.deepEqual((await paymentsOf(id)).map((p) => p.amount), ['50.00']);
  });

  await t.test('our own payment notified back is not listed twice', async () => {
    const { id, key } = await syncedInvoice(qbo);
    await pay(id, '10.00');
    await runJobs([key], qbo);
    await notifyPayment((await paymentsOf(id))[0].quickbooks_id!);
    assert.equal((await paymentsOf(id)).length, 1);
  });

  await t.test('notified before the worker saved it: the existing row is linked, not duplicated', async () => {
    const { id, key, quickbooksId } = await syncedInvoice(qbo);
    const local = await pay(id, '15.00'); // not pushed yet
    // QuickBooks has it (with our reference) and notifies it first
    const remote = await payInQuickBooks([{ quickbooksId, amount: 15 }], `Payment from local payment #${local.id} (${localPaymentKey(local)})`);
    await notifyPayment(remote.Id);

    const listed = await paymentsOf(id);
    assert.equal(listed.length, 1);
    assert.equal(listed[0].id, local.id);
    assert.equal(listed[0].quickbooks_id, remote.Id);

    // The payment's own job then finds it already in QuickBooks
    const before = qbo.countCalls('POST payment');
    await runJobs([key], qbo);
    assert.equal(qbo.countCalls('POST payment'), before);
  });

  await t.test('the full import lists the existing QuickBooks payments', async () => {
    const realm = uniqueName('realm').replace(/\W/g, '');
    const other = new FakeQuickBooks(realm);
    other.addInvoice({ Id: '1', TotalAmt: 100, Balance: 100 });
    await other.request(() => 'payment', { method: 'POST', body: { CustomerRef: { value: '1' }, Line: [{ Amount: 40, LinkedTxn: [{ TxnId: '1', TxnType: 'Invoice' }] }] } });

    await enqueueRemoteChanges(other);
    const pool = await getPool();
    const imported = await pool.one(sql.type(z.object({ id: z.number(), balance: z.string() }))`
      SELECT id, balance FROM invoices WHERE quickbooks_realm_id = ${realm} AND quickbooks_id = '1'
    `);
    assert.equal(imported.balance, '60.00');
    assert.deepEqual((await paymentsOf(imported.id)).map((p) => p.amount), ['40.00']);
  });
});

test('what is left to pay follows local changes not synced yet', async (t) => {
  const qbo = new FakeQuickBooks();

  await t.test('marked paid locally: nothing left (its job pays the whole balance in QuickBooks)', async () => {
    const { id } = await syncedInvoice(qbo);
    await invoices.update(id, { status: 'paid' });
    assert.equal((await payments.create(id, { amount: '80.00' })).outcome, 'exceeds_balance');
  });

  await t.test('amount lowered locally: the new amount is the limit', async () => {
    const { id } = await syncedInvoice(qbo);
    await pay(id, '10.00');
    await invoices.update(id, { amount: '40.00' });
    assert.equal((await payments.create(id, { amount: '30.01' })).outcome, 'exceeds_balance'); // 40 - 10 left
    assert.equal((await payments.create(id, { amount: '30.00' })).outcome, 'created');
  });

  await t.test('amount raised locally: the new amount is the limit too', async () => {
    const { id } = await syncedInvoice(qbo);
    await invoices.update(id, { amount: '120.00' });
    assert.equal((await payments.create(id, { amount: '120.00' })).outcome, 'created');
  });
});

test('two concurrent retries with the same Idempotency-Key: one payment, no error', async () => {
  const { id } = await syncedInvoice(new FakeQuickBooks());
  const key = uniqueName('pay-key');
  const pool = await getPool();
  let retry: Promise<Awaited<ReturnType<typeof payments.create>>> | undefined;
  // The first request has inserted its payment but not committed yet when the retry arrives: the retry
  // doesn't see it, and its insert waits for the first one to commit
  await pool.transaction(async (tx) => {
    await tx.query(sql.unsafe`INSERT INTO invoice_payments (invoice_id, amount, idempotency_key) VALUES (${id}, '10.00', ${key})`);
    retry = payments.create(id, { amount: '10.00' }, key);
    retry.catch(() => {}); // awaited below
    await new Promise((resolve) => setTimeout(resolve, 300));
  });
  const result = await retry!;
  assert.equal(result.outcome, 'existing');
  assert.equal((await paymentsOf(id)).length, 1);
});

test('a payment of 0.00 is rejected by validation (400, not a database error)', () => {
  for (const amount of ['0', '0.00', '0.0']) assert.equal(payments.CreatePaymentInput.safeParse({ amount }).success, false, amount);
  assert.equal(payments.CreatePaymentInput.safeParse({ amount: '0.01' }).success, true);
});

test('an Idempotency-Key reused on another invoice is rejected, not answered with the other invoice\'s payment', async () => {
  const qbo = new FakeQuickBooks();
  const a = await syncedInvoice(qbo);
  const b = await syncedInvoice(qbo);
  const key = uniqueName('pay-key');
  assert.equal((await payments.create(a.id, { amount: '10.00' }, key)).outcome, 'created');
  assert.equal((await payments.create(b.id, { amount: '10.00' }, key)).outcome, 'key_reused');
  assert.equal((await paymentsOf(b.id)).length, 0);
});

