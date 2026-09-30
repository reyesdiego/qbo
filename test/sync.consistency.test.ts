// Consistency check: repairs what the other sync paths can't see (rows deleted by hand, missed changes)
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { sql } from 'slonik';
import { z } from 'zod';
import { getPool } from '../src/db';
import * as invoices from '../src/invoices.repository';
import * as payments from '../src/payments.repository';
import { checkConsistency } from '../src/sync.consistency';
import { invoiceEntityKey } from '../src/sync.repository';
import { FakeQuickBooks, loadInvoice, runJobs, uniqueName } from './helpers';

after(async () => {
  await (await getPool()).end();
});

// Each test uses its own QuickBooks company, so the check only sees that test's invoices
const company = () => {
  const realm = uniqueName('realm').replace(/\W/g, '');
  return { realm, qbo: new FakeQuickBooks(realm) };
};

const syncedInvoice = async (qbo: FakeQuickBooks, amount = '100.00') => {
  const { invoice } = await invoices.create({ customer_name: uniqueName('Consistency'), amount, currency: 'USD', status: 'sent', due_date: '2026-12-31' });
  await runJobs([invoiceEntityKey(invoice.id)], qbo);
  return { id: invoice.id, quickbooksId: (await loadInvoice(invoice.id)).quickbooks_id! };
};

const localRowFor = async (realm: string, quickbooksId: string) => {
  const pool = await getPool();
  return pool.maybeOne(sql.type(z.object({ id: z.number(), amount: z.string(), balance: z.string() }))`
    SELECT id, amount, balance FROM invoices WHERE quickbooks_realm_id = ${realm} AND quickbooks_id = ${quickbooksId}
  `);
};

test('everything in sync: nothing reported, nothing queued', async () => {
  const { qbo } = company();
  await syncedInvoice(qbo);
  const report = await checkConsistency(qbo);
  assert.deepEqual([report.missing_locally, report.missing_in_quickbooks, report.jobs_enqueued], [[], [], 0]);
});

test('an invoice deleted by hand in the database comes back, with its payments', async () => {
  const { realm, qbo } = company();
  const { id, quickbooksId } = await syncedInvoice(qbo);
  await payments.create(id, { amount: '30.00' });
  await runJobs([invoiceEntityKey(id)], qbo);

  const pool = await getPool();
  await pool.query(sql.unsafe`DELETE FROM invoices WHERE id = ${id}`); // the manual delete

  const report = await checkConsistency(qbo);
  assert.deepEqual(report.missing_locally, [quickbooksId]);
  assert.equal(report.jobs_enqueued, 1);

  await runJobs([`qbo-invoice:${realm}:${quickbooksId}`], qbo);
  const back = (await localRowFor(realm, quickbooksId))!;
  assert.equal(back.amount, '100.00');
  assert.equal(back.balance, '70.00');
  assert.deepEqual((await payments.listForInvoice(back.id)).map((p) => p.amount), ['30.00']);
});

test('an invoice deleted in QuickBooks whose notification was missed is deleted locally', async () => {
  const { realm, qbo } = company();
  const { quickbooksId } = await syncedInvoice(qbo);
  qbo.invoices.delete(quickbooksId); // deleted in QuickBooks, no webhook

  const report = await checkConsistency(qbo);
  assert.deepEqual(report.missing_in_quickbooks.map((m) => m.quickbooks_id), [quickbooksId]);

  const local = (await localRowFor(realm, quickbooksId))!;
  await runJobs([invoiceEntityKey(local.id)], qbo);
  assert.equal(await localRowFor(realm, quickbooksId), null);
});

test('missing from QuickBooks\' list but found by a direct GET: not deleted', async () => {
  const { realm, qbo } = company();
  const { quickbooksId } = await syncedInvoice(qbo);
  qbo.hiddenFromQueries.add(quickbooksId); // e.g. QuickBooks' search lagging right after a create

  const report = await checkConsistency(qbo);
  assert.deepEqual(report.missing_in_quickbooks, []);
  assert.ok(await localRowFor(realm, quickbooksId));
});

test('repair: false only reports', async () => {
  const { qbo } = company();
  const { id } = await syncedInvoice(qbo);
  const pool = await getPool();
  await pool.query(sql.unsafe`DELETE FROM invoices WHERE id = ${id}`);

  const report = await checkConsistency(qbo, { repair: false });
  assert.equal(report.missing_locally.length, 1);
  assert.equal(report.jobs_enqueued, 0);
  assert.equal(report.repaired, false);
});
