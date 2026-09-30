// Reconciliation: full import when needed, CDC otherwise
import { test, after, before } from 'node:test';
import assert from 'node:assert/strict';
import { sql } from 'slonik';
import { getPool } from '../src/db';
import { SyncInvoice } from '../src/invoices.schema';
import { enqueueRemoteChanges } from '../src/sync.reconcile';
import { invoiceEntityKey } from '../src/sync.repository';
import { ensureTestConnection, FakeQuickBooks, runJobs, uniqueName } from './helpers';

before(ensureTestConnection);
after(async () => {
  await (await getPool()).end();
});

const invoicesOf = async (realmId: string) => {
  const pool = await getPool();
  return pool.any(sql.type(SyncInvoice)`SELECT * FROM invoices WHERE quickbooks_realm_id = ${realmId} ORDER BY quickbooks_id`);
};

test('with no local invoice of the company (first run, or wiped data), everything is imported once', async () => {
  // A company of its own, with no local invoices
  const realm = uniqueName('realm').replace(/\W/g, '');
  const qbo = new FakeQuickBooks(realm);
  qbo.addInvoice({ Id: '1', TotalAmt: 100, Balance: 100 });
  qbo.addInvoice({ Id: '2', TotalAmt: 250.5, Balance: 0 }); // paid

  await enqueueRemoteChanges(qbo);
  const imported = await invoicesOf(realm);
  assert.deepEqual(imported.map((i) => [i.quickbooks_id, i.amount, i.balance, i.status, i.sync_status]), [
    ['1', '100.00', '100.00', 'draft', 'synced'], // not sent in QuickBooks (EmailStatus NotSet)
    ['2', '250.50', '0.00', 'paid', 'synced'],
  ]);

  // Next run: the company has local invoices, so only changes are asked for (CDC), no full import
  qbo.calls.length = 0;
  await enqueueRemoteChanges(qbo);
  assert.equal(qbo.countCalls('GET cdc'), 1);
  assert.equal(qbo.countCalls('QUERY SELECT * FROM Invoice'), 0);
  assert.equal((await invoicesOf(realm)).length, 2);
});

// A company of its own with one invoice already imported, so the next runs use CDC
const importedCompany = async () => {
  const realm = uniqueName('realm').replace(/\W/g, '');
  const qbo = new FakeQuickBooks(realm);
  qbo.addInvoice({ Id: '1' });
  await enqueueRemoteChanges(qbo);
  return { realm, qbo };
};

test('the worker was off for more than 30 days (CDC rejects the cursor): a full import catches up', async () => {
  const { realm, qbo } = await importedCompany();
  const pool = await getPool();
  await pool.query(sql.unsafe`UPDATE quickbooks_connection SET cdc_cursor = now() - interval '40 days'`);
  qbo.addInvoice({ Id: '2' }); // created while the worker was off

  await enqueueRemoteChanges(qbo);
  assert.deepEqual((await invoicesOf(realm)).map((i) => i.quickbooks_id), ['1', '2']);
});

test('CDC returns its maximum (1000 per entity, more may have changed): a full import catches up', async () => {
  const { realm, qbo } = await importedCompany();
  qbo.addInvoice({ Id: '2' }); // changed, but beyond what CDC returned
  qbo.cdcInvoices = Array.from({ length: 1000 }, (_, i) => ({ Id: `cdc-${i}`, SyncToken: '0' }));

  await enqueueRemoteChanges(qbo);
  assert.deepEqual((await invoicesOf(realm)).map((i) => i.quickbooks_id), ['1', '2']);
});

test('a full import instead of CDC also applies deletions made in QuickBooks meanwhile', async () => {
  const { realm, qbo } = await importedCompany();
  const [local] = await invoicesOf(realm);
  qbo.invoices.delete('1'); // deleted in QuickBooks, its notification missed
  qbo.cdcInvoices = Array.from({ length: 1000 }, (_, i) => ({ Id: `cdc-${i}`, SyncToken: '0' }));

  await enqueueRemoteChanges(qbo);
  await runJobs([invoiceEntityKey(local.id)], qbo);
  assert.deepEqual((await invoicesOf(realm)).map((i) => i.quickbooks_id), []);
});

test('a company without invoices uses CDC, not a full import every time', async () => {
  const realm = uniqueName('realm').replace(/\W/g, '');
  const qbo = new FakeQuickBooks(realm);
  await enqueueRemoteChanges(qbo);
  qbo.calls.length = 0;
  await enqueueRemoteChanges(qbo);
  assert.equal(qbo.countCalls('QUERY SELECT * FROM Invoice'), 0);
  assert.equal(qbo.countCalls('GET cdc'), 1);
});

