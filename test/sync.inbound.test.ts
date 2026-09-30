// INBOUND sync (QuickBooks -> local) against an in-memory fake QuickBooks
import { test, after, before } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { sql } from 'slonik';
import * as accounts from '../src/accounts.repository';
import type { QboAccount } from '../src/accounts.repository';
import { getPool } from '../src/db';
import * as invoices from '../src/invoices.repository';
import { SyncInvoice } from '../src/invoices.schema';
import { localInvoiceKey } from '../src/quickbooks.mapping';
import { resolveConflict } from '../src/sync.conflicts';
import { claimNextJob, invoiceEntityKey, listJobs, markUnknown, recordEventAndEnqueue } from '../src/sync.repository';
import { ensureTestConnection, FakeQuickBooks, jobsFor, loadInvoice, qboErrors, runJobs, runNow, TEST_REALM, uniqueName } from './helpers';

before(ensureTestConnection);
after(async () => {
  await (await getPool()).end();
});

// A webhook notification, recorded and queued like the webhook endpoint does
const notify = async (entity: string, id: string, operation = 'update', eventKey = randomUUID()) => {
  const pool = await getPool();
  return pool.transaction((tx) =>
    recordEventAndEnqueue(tx, { source: 'webhook', realmId: TEST_REALM, entity, id, operation, eventKey, payload: {} }),
  );
};

const findByQuickbooksId = async (quickbooksId: string) => {
  const pool = await getPool();
  return pool.maybeOne(sql.type(SyncInvoice)`
    SELECT * FROM invoices WHERE quickbooks_realm_id = ${TEST_REALM} AND quickbooks_id = ${quickbooksId}
  `);
};

const qbKey = (id: string) => `qbo-invoice:${TEST_REALM}:${id}`;

// Keeps the invoice's pending outbound job from running (its entity key is set aside), so the
// inbound side can be tested while a local change hasn't been pushed yet
const holdOutbound = async (key: string) => {
  const pool = await getPool();
  await pool.query(sql.unsafe`
    UPDATE sync_jobs SET entity_key = ${`${key}-held`}
    WHERE entity_key = ${key} AND direction = 'OUTBOUND' AND status = 'PENDING'
  `);
};
const releaseOutbound = async (key: string) => {
  const pool = await getPool();
  await pool.query(sql.unsafe`UPDATE sync_jobs SET entity_key = ${key} WHERE entity_key = ${`${key}-held`}`);
};

// An invoice in QuickBooks that's already synced locally
const syncedRemote = async (qbo: FakeQuickBooks) => {
  const id = uniqueName('qb').replace(/\W/g, '');
  qbo.addInvoice({ Id: id, CustomerRef: { value: '1', name: 'Remote Co' }, TotalAmt: 100, Balance: 100 });
  await notify('invoice', id, 'create');
  await runJobs([qbKey(id)], qbo);
  const local = (await findByQuickbooksId(id))!;
  return { id, local, key: invoiceEntityKey(local.id) };
};

test('an invoice created in QuickBooks is imported', async () => {
  const qbo = new FakeQuickBooks();
  const { local } = await syncedRemote(qbo);
  assert.equal(local.customer_name, 'Remote Co');
  assert.equal(local.amount, '100.00');
  assert.equal(local.sync_status, 'synced');
  assert.equal(local.quickbooks_sync_token, '0');
});

test('duplicate, out-of-order and self-caused notifications change nothing', async (t) => {
  const qbo = new FakeQuickBooks();

  await t.test('a redelivered notification (same event key) is not queued again', async () => {
    const eventKey = randomUUID();
    assert.equal(await notify('invoice', 'x', 'update', eventKey), 'enqueued');
    assert.equal(await notify('invoice', 'x', 'update', eventKey), 'duplicate');
  });

  await t.test('our own push coming back is skipped (no sync loop)', async () => {
    const { invoice } = await invoices.create({ customer_name: uniqueName('Loop'), amount: '10.00', currency: 'USD', status: 'sent', due_date: '2026-12-31' });
    const key = invoiceEntityKey(invoice.id);
    await runJobs([key], qbo); // pushed to QuickBooks
    const pushed = await loadInvoice(invoice.id);

    await notify('invoice', pushed.quickbooks_id!, 'create'); // QuickBooks notifies about our create
    await runJobs([key], qbo);
    assert.equal((await jobsFor(key)).at(-1)!.payload.result, 'already up to date');
    assert.equal((await jobsFor(key)).filter((j) => j.direction === 'OUTBOUND').length, 1); // nothing pushed back
  });

  await t.test('an older version never overwrites a newer one', async () => {
    const { id, local, key } = await syncedRemote(qbo);
    const pool = await getPool();
    await pool.query(sql.unsafe`UPDATE invoices SET quickbooks_sync_token = '5' WHERE id = ${local.id}`);
    qbo.edit(id, { TotalAmt: 1, Balance: 1 }); // SyncToken 1 < 5

    await notify('invoice', id);
    await runJobs([key], qbo);
    assert.equal((await loadInvoice(local.id)).amount, '100.00');
  });
});

test('three-way comparison with the last synced snapshot', async (t) => {
  const qbo = new FakeQuickBooks();

  await t.test('only changed in QuickBooks: applied', async () => {
    const { id, local, key } = await syncedRemote(qbo);
    qbo.edit(id, { DueDate: '2027-01-15' });
    await notify('invoice', id);
    await runJobs([key], qbo);

    const updated = await loadInvoice(local.id);
    assert.equal(updated.due_date, '2027-01-15');
    assert.equal(updated.sync_status, 'synced');
    assert.equal(updated.last_synced_snapshot?.due_date, '2027-01-15');
  });

  await t.test('only changed locally: kept, and pushed later against the latest version', async () => {
    const { id, local, key } = await syncedRemote(qbo);
    await invoices.update(local.id, { amount: '150.00' });
    await holdOutbound(key);
    qbo.edit(id, {}); // new SyncToken (e.g. emailed), same content

    await notify('invoice', id);
    await runJobs([key], qbo);
    const kept = await loadInvoice(local.id);
    assert.equal(kept.amount, '150.00');
    assert.equal(kept.quickbooks_sync_token, '1');

    await releaseOutbound(key);
    await runJobs([key], qbo);
    assert.equal(qbo.invoices.get(id)!.TotalAmt, 150);
    assert.equal((await loadInvoice(local.id)).sync_status, 'synced');
  });

  await t.test('changed on both sides differently: conflict, nothing overwritten', async () => {
    const { id, local, key } = await syncedRemote(qbo);
    await invoices.update(local.id, { amount: '150.00' });
    await holdOutbound(key);
    qbo.edit(id, { TotalAmt: 175, Balance: 175 });

    await notify('invoice', id);
    await runJobs([key], qbo);

    const updated = await loadInvoice(local.id);
    assert.equal(updated.sync_status, 'conflict');
    assert.equal(updated.amount, '150.00');
    assert.equal((updated.sync_conflict as { remote: { amount: string } }).remote.amount, '175.00');

    // Resolving it: keep QuickBooks' version. An outbound job checks nothing else is left to push.
    assert.equal(await resolveConflict(local.id, 'remote'), 'resolved');
    assert.equal((await loadInvoice(local.id)).amount, '175.00');
    await runJobs([key], qbo);
    const resolved = await loadInvoice(local.id);
    assert.equal(resolved.sync_status, 'synced');
    assert.equal(qbo.invoices.get(id)!.TotalAmt, 175); // QuickBooks' version wasn't overwritten
  });

  await t.test('same change on both sides: converges without a conflict', async () => {
    const { id, local, key } = await syncedRemote(qbo);
    await invoices.update(local.id, { amount: '130.00' });
    await holdOutbound(key);
    qbo.edit(id, { TotalAmt: 130, Balance: 130 });

    await notify('invoice', id);
    await runJobs([key], qbo);
    const updated = await loadInvoice(local.id);
    assert.equal(updated.sync_status, 'synced');
    assert.equal(updated.synced_version, updated.version);
  });
});

test('sent status', async (t) => {
  const qbo = new FakeQuickBooks();
  const draft = async () => {
    const { invoice } = await invoices.create({ customer_name: uniqueName('Sent'), amount: '20.00', currency: 'USD', status: 'draft', due_date: '2026-12-31' });
    const key = invoiceEntityKey(invoice.id);
    await runJobs([key], qbo);
    return { id: invoice.id, key, quickbooksId: (await loadInvoice(invoice.id)).quickbooks_id! };
  };

  await t.test('marked as sent in QuickBooks: the local draft becomes sent, even with the same SyncToken', async () => {
    const { id, key, quickbooksId } = await draft();
    // What "Mark as sent" in the QuickBooks app does: EmailStatus changes, the SyncToken doesn't
    qbo.invoices.set(quickbooksId, { ...qbo.invoices.get(quickbooksId)!, EmailStatus: 'EmailSent' });
    await notify('invoice', quickbooksId, 'emailed');
    await runJobs([key], qbo);

    assert.equal((await loadInvoice(id)).status, 'sent');
    assert.equal((await jobsFor(key)).at(-1)!.payload.result, 'marked as sent (sent in QuickBooks)');
  });

  await t.test('marked as sent locally: QuickBooks gets EmailStatus EmailSent', async () => {
    const { id, key, quickbooksId } = await draft();
    assert.equal(qbo.invoices.get(quickbooksId)!.EmailStatus, 'NotSet');
    await invoices.update(id, { status: 'sent' });
    await runJobs([key], qbo);

    assert.equal(qbo.invoices.get(quickbooksId)!.EmailStatus, 'EmailSent');
    assert.equal((await jobsFor(key)).at(-1)!.payload.result, 'marked as sent in QuickBooks');
    assert.equal((await loadInvoice(id)).sync_status, 'synced');
  });

  await t.test('created as sent: created in QuickBooks as sent', async () => {
    const { invoice } = await invoices.create({ customer_name: uniqueName('Sent'), amount: '20.00', currency: 'USD', status: 'sent', due_date: '2026-12-31' });
    await runJobs([invoiceEntityKey(invoice.id)], qbo);
    assert.equal(qbo.invoices.get((await loadInvoice(invoice.id)).quickbooks_id!)!.EmailStatus, 'EmailSent');
  });
});

test('paid status, both ways', async (t) => {
  const qbo = new FakeQuickBooks();
  const synced = async () => {
    const { invoice } = await invoices.create({ customer_name: uniqueName('Pay'), amount: '80.00', currency: 'USD', status: 'sent', due_date: '2026-12-31' });
    const key = invoiceEntityKey(invoice.id);
    await runJobs([key], qbo);
    return { id: invoice.id, key, quickbooksId: (await loadInvoice(invoice.id)).quickbooks_id! };
  };
  const payments = () => qbo.countCalls('POST payment');

  await t.test('marked paid locally: a payment for the balance is recorded in QuickBooks', async () => {
    const { id, key, quickbooksId } = await synced();
    const before = payments();
    await invoices.update(id, { status: 'paid' });
    await runJobs([key], qbo);

    assert.equal(payments() - before, 1);
    assert.equal(qbo.invoices.get(quickbooksId)!.Balance, 0);
    const paid = await loadInvoice(id);
    assert.equal(paid.status, 'paid');
    assert.equal(paid.balance, '0.00');
    assert.equal(paid.sync_status, 'synced');
    assert.equal((await jobsFor(key)).at(-1)!.payload.result, 'payment recorded in QuickBooks');
  });

  await t.test('a payment whose response was lost is not recorded twice on retry', async () => {
    const { id, key, quickbooksId } = await synced();
    const before = payments();
    await invoices.update(id, { status: 'paid' });
    qbo.failAfterNext('POST payment', qboErrors.timeout); // QuickBooks records it, we don't hear back
    await runJobs([key], qbo);
    assert.equal((await jobsFor(key)).at(-1)!.status, 'PENDING'); // retried later

    await runNow(key);
    await runJobs([key], qbo);
    assert.equal(payments() - before, 1); // the retry saw the balance was 0: no second payment
    assert.equal(qbo.invoices.get(quickbooksId)!.Balance, 0);
    assert.equal((await loadInvoice(id)).status, 'paid');
  });

  await t.test('created as paid: created and paid in QuickBooks', async () => {
    const { invoice } = await invoices.create({ customer_name: uniqueName('Pay'), amount: '15.00', currency: 'USD', status: 'paid', due_date: '2026-12-31' });
    await runJobs([invoiceEntityKey(invoice.id)], qbo);
    const created = await loadInvoice(invoice.id);
    assert.equal(qbo.invoices.get(created.quickbooks_id!)!.Balance, 0);
    assert.equal(created.status, 'paid');
    assert.equal(created.balance, '0.00');
  });

  await t.test('a QuickBooks notification before the push does not revert a local "paid"', async () => {
    const { id, key, quickbooksId } = await synced();
    await invoices.update(id, { status: 'paid' });
    await holdOutbound(key);
    qbo.edit(quickbooksId, { DueDate: '2027-03-01' }); // unrelated change in QuickBooks, still unpaid there
    await notify('invoice', quickbooksId);
    await runJobs([key], qbo);
    assert.equal((await loadInvoice(id)).status, 'paid');

    await releaseOutbound(key);
    await runJobs([key], qbo);
    assert.equal(qbo.invoices.get(quickbooksId)!.Balance, 0);
  });

  await t.test('payment deleted in QuickBooks: the invoice goes back to sent', async () => {
    const { id, key, quickbooksId } = await synced();
    await invoices.update(id, { status: 'paid' });
    await runJobs([key], qbo);
    qbo.edit(quickbooksId, { Balance: 80 }); // what deleting the payment does to the invoice
    await notify('invoice', quickbooksId);
    await runJobs([key], qbo);

    const unpaid = await loadInvoice(id);
    assert.equal(unpaid.status, 'sent');
    assert.equal(unpaid.balance, '80.00');
  });
});

test('a payment updates the balance and paid status of the invoices it pays', async () => {
  const qbo = new FakeQuickBooks();
  const { id, local } = await syncedRemote(qbo);
  const paymentId = uniqueName('pay').replace(/\W/g, '');
  qbo.edit(id, { Balance: 0 });
  qbo.payments.set(paymentId, { Id: paymentId, Line: [{ LinkedTxn: [{ TxnId: id, TxnType: 'Invoice' }] }] });

  await notify('payment', paymentId, 'create');
  await runJobs([`qbo-payment:${TEST_REALM}:${paymentId}`], qbo);

  const paid = await loadInvoice(local.id);
  assert.equal(paid.balance, '0.00');
  assert.equal(paid.status, 'paid');
});

test('deletions in QuickBooks', async (t) => {
  const qbo = new FakeQuickBooks();

  await t.test('a delete notification deletes the local invoice (the row is removed)', async () => {
    const { id, key } = await syncedRemote(qbo);
    qbo.invoices.delete(id);
    await notify('invoice', id, 'delete');
    await runJobs([key], qbo);
    assert.equal(await findByQuickbooksId(id), null);
    assert.equal((await jobsFor(key)).at(-1)!.payload.result, 'deleted locally');
  });

  await t.test('unpushed local changes are discarded, and the job records which ones', async () => {
    const { id, local, key } = await syncedRemote(qbo);
    await invoices.update(local.id, { amount: '150.00' });
    await holdOutbound(key);
    qbo.invoices.delete(id);
    await notify('invoice', id, 'delete');
    await runJobs([key], qbo);

    assert.equal(await findByQuickbooksId(id), null);
    assert.equal((await jobsFor(key)).at(-1)!.payload.result, 'deleted locally; unpushed local changes discarded (amount)');

    // The held outbound job then finds nothing to push
    await releaseOutbound(key);
    await runJobs([key], qbo);
    const outbound = (await jobsFor(key)).filter((job) => job.direction === 'OUTBOUND').at(-1)!;
    assert.equal(outbound.payload.result, 'invoice no longer exists');
  });

  await t.test('an update notification for an invoice deleted afterwards is superseded by the delete', async () => {
    const { id, local, key } = await syncedRemote(qbo);
    await notify('invoice', id, 'update'); // queued before the deletion...
    qbo.invoices.delete(id); // ...which happens before the update is processed
    await notify('invoice', id, 'delete');
    await runJobs([key], qbo);

    const [update, remove] = (await jobsFor(key)).slice(-2);
    assert.equal(update.status, 'COMPLETED');
    assert.equal(update.payload.result, 'superseded: deleted in QuickBooks afterwards');
    assert.equal(remove.status, 'COMPLETED');
    assert.equal(await findByQuickbooksId(local.quickbooks_id!), null);
  });

  await t.test('a failed GET is not taken as a deletion: the job is retried', async () => {
    const { id, local, key } = await syncedRemote(qbo);
    qbo.failNext(`GET invoice/${id}`, qboErrors.notFound);
    await notify('invoice', id, 'update');
    await runJobs([key], qbo);

    const job = (await jobsFor(key)).at(-1)!;
    assert.equal(job.status, 'PENDING');
    assert.equal(job.last_error_class, 'not_found');
    assert.equal((await loadInvoice(local.id)).deleted_at, null);
  });
});

test('voided in QuickBooks: voided locally, not deleted', async (t) => {
  const qbo = new FakeQuickBooks();
  // What QuickBooks does to a voided invoice (checked against the real sandbox)
  const voidInQuickBooks = (id: string) =>
    qbo.edit(id, { TotalAmt: 0, Balance: 0, PrivateNote: `Voided - ${qbo.invoices.get(id)!.PrivateNote ?? ''}` });

  await t.test('from a webhook with the Void operation', async () => {
    const { id, local, key } = await syncedRemote(qbo);
    voidInQuickBooks(id);
    await notify('invoice', id, 'void');
    await runJobs([key], qbo);

    const voided = await loadInvoice(local.id);
    assert.equal(voided.status, 'void');
    assert.ok(voided.voided_at);
    assert.equal(voided.deleted_at, null);
    assert.equal(voided.amount, '100.00'); // the invoiced amount is kept (QuickBooks zeroes it)
    assert.equal(voided.balance, '0.00');
    assert.equal(voided.sync_status, 'synced');
  });

  await t.test('also recognized when the notification only says "update" (e.g. reconciliation)', async () => {
    const { id, local, key } = await syncedRemote(qbo);
    voidInQuickBooks(id);
    await notify('invoice', id, 'update');
    await runJobs([key], qbo);

    const voided = await loadInvoice(local.id);
    assert.equal(voided.status, 'void');
    assert.equal(voided.amount, '100.00');
  });

  await t.test('a void wins over unpushed local changes, which are then not pushed', async () => {
    const { id, local, key } = await syncedRemote(qbo);
    await invoices.update(local.id, { amount: '150.00' });
    await holdOutbound(key);
    voidInQuickBooks(id);
    await notify('invoice', id, 'void');
    await runJobs([key], qbo);
    assert.equal((await loadInvoice(local.id)).status, 'void');

    await releaseOutbound(key);
    await runJobs([key], qbo);
    assert.equal(qbo.invoices.get(id)!.TotalAmt, 0); // nothing pushed to the voided invoice
  });

  await t.test('an invoice already voided in QuickBooks is imported as void', async () => {
    const id = uniqueName('voided').replace(/\W/g, '');
    qbo.addInvoice({ Id: id, TotalAmt: 0, Balance: 0, PrivateNote: 'Voided - ' });
    await notify('invoice', id, 'create');
    await runJobs([qbKey(id)], qbo);

    const imported = (await findByQuickbooksId(id))!;
    assert.equal(imported.status, 'void');
    assert.ok(imported.voided_at);
  });
});

test('before the mapping exists: an invoice we created is linked, not imported twice', async () => {
  const qbo = new FakeQuickBooks();
  const { invoice } = await invoices.create({ customer_name: uniqueName('Link'), amount: '10.00', currency: 'USD', status: 'sent', due_date: '2026-12-31' });
  const key = invoiceEntityKey(invoice.id);
  const created = await loadInvoice(invoice.id);

  // The worker is creating it right now (PROCESSING) and QuickBooks already notifies about it
  const createJob = (await claimNextJob({ workerId: 'creator', leaseSeconds: 60, entityKeys: [key] }))!;
  const quickbooksId = uniqueName('linked').replace(/\W/g, '');
  // As our create makes it: same content, marked as sent (the local status), our reference in PrivateNote
  qbo.addInvoice({ Id: quickbooksId, TotalAmt: 10, Balance: 10, CustomerRef: { value: '1', name: created.customer_name }, EmailStatus: 'EmailSent', PrivateNote: `(${localInvoiceKey(created)})` });
  await notify('invoice', quickbooksId, 'create');

  // Deferred while the create job runs
  await runJobs([qbKey(quickbooksId)], qbo);
  const deferred = (await jobsFor(qbKey(quickbooksId)))[0];
  assert.equal(deferred.status, 'PENDING');
  assert.equal(deferred.last_error_class, 'deferred');
  assert.equal(deferred.attempts, 0);

  // The create ends UNKNOWN (response lost): the notification resolves it
  await markUnknown(createJob, 'response lost');
  await runNow(qbKey(quickbooksId));
  await runJobs([qbKey(quickbooksId)], qbo);

  const linked = await loadInvoice(invoice.id);
  assert.equal(linked.quickbooks_id, quickbooksId);
  assert.equal(linked.sync_status, 'synced');
  assert.equal((await jobsFor(key))[0].status, 'COMPLETED');
  const pool = await getPool();
  const copies = await pool.any(sql.type(SyncInvoice)`SELECT * FROM invoices WHERE quickbooks_id = ${quickbooksId}`);
  assert.equal(copies.length, 1);
});

test('GET /sync/jobs?invoice_id= lists an invoice\'s jobs in both directions, with both ids', async () => {
  const qbo = new FakeQuickBooks();
  const { invoice } = await invoices.create({ customer_name: uniqueName('Ids'), amount: '10.00', currency: 'USD', status: 'sent', due_date: '2026-12-31' });
  const key = invoiceEntityKey(invoice.id);
  await runJobs([key], qbo);
  const quickbooksId = (await loadInvoice(invoice.id)).quickbooks_id!;
  await notify('invoice', quickbooksId); // INBOUND: its entity_id is the QuickBooks id

  const jobs = await listJobs({ invoiceId: invoice.id, limit: 50 });
  assert.ok(jobs.every((job) => job.local_invoice_id === invoice.id)); // only this invoice's jobs
  const outbound = jobs.find((job) => job.direction === 'OUTBOUND')!;
  const inbound = jobs.find((job) => job.direction === 'INBOUND')!;
  assert.equal(outbound.entity_id, String(invoice.id));
  assert.equal(inbound.entity_id, quickbooksId);
  for (const job of [outbound, inbound]) {
    assert.equal(job.local_invoice_id, invoice.id);
    assert.equal(job.quickbooks_id, quickbooksId);
  }
});

test('keeping the local chart of accounts in sync', async (t) => {
  const accountId = uniqueName('account').replace(/\W/g, '');
  const qboAccount = (overrides: Partial<QboAccount> = {}): QboAccount => ({
    Id: accountId,
    SyncToken: '0',
    Name: 'Office Supplies',
    FullyQualifiedName: 'Expenses:Office Supplies',
    AccountType: 'Expense',
    Classification: 'Expense',
    CurrentBalance: 12.34,
    CurrencyRef: { value: 'USD' },
    Active: true,
    ...overrides,
  });
  const findAccount = async () => (await accounts.list()).find((a) => a.id === accountId);

  await t.test('adds a new account', async () => {
    await accounts.applyFromQuickBooks(qboAccount());
    assert.equal((await findAccount())?.current_balance, '12.34');
  });

  await t.test('updates it when QuickBooks has a newer version', async () => {
    await accounts.applyFromQuickBooks(qboAccount({ SyncToken: '1', Name: 'Supplies', Active: false }));
    const account = await findAccount();
    assert.equal(account?.name, 'Supplies');
    assert.equal(account?.active, false);
  });

  await t.test('same SyncToken, new balance: updated (QuickBooks keeps the SyncToken when the balance moves)', async () => {
    // Checked in the sandbox: posting an invoice changed Accounts Receivable's balance, not its SyncToken
    await accounts.applyFromQuickBooks(qboAccount({ SyncToken: '1', Name: 'Supplies', Active: false, CurrentBalance: 99.5 }));
    assert.equal((await findAccount())?.current_balance, '99.50');
  });

  await t.test('ignores an older version', async () => {
    await accounts.applyFromQuickBooks(qboAccount({ SyncToken: '0', Name: 'Stale copy' }));
    assert.equal((await findAccount())?.name, 'Supplies');
  });

  await t.test('removes it when deleted in QuickBooks', async () => {
    await accounts.applyFromQuickBooks({ Id: accountId, status: 'Deleted' });
    assert.equal(await findAccount(), undefined);
  });
});
