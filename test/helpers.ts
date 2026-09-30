// Test support: an in-memory fake QuickBooks (to simulate failures that can't be produced reliably
// against the real sandbox) and helpers to run the sync worker's steps.
import { sql } from 'slonik';
import { getPool } from '../src/db';
import { SyncInvoice } from '../src/invoices.schema';
import type { QboApi } from '../src/quickbooks.client';
import type { QboInvoice, QboPayment } from '../src/quickbooks.mapping';
import { processJob } from '../src/sync.processor';
import { claimNextJob, SyncJob } from '../src/sync.repository';

export const TEST_REALM = 'test-realm';

// Errors shaped like the ones intuit-oauth throws
export const qboErrors = {
  timeout: () => Object.assign(new Error('Request timeout of 30000ms exceeded'), { code: 'TIMEOUT_ERROR' }),
  rateLimited: () => Object.assign(new Error('Rate limit exceeded'), { code: 'RATE_LIMIT_EXCEEDED' }),
  staleObject: () =>
    Object.assign(new Error('Stale Object Error'), {
      code: '5010',
      fault: { errors: [{ code: '5010', message: 'Stale Object Error', detail: 'SyncToken mismatch' }] },
    }),
  notFound: () =>
    Object.assign(new Error('Object Not Found'), {
      code: '610',
      fault: { errors: [{ code: '610', message: 'Object Not Found' }] },
    }),
  validation: () =>
    Object.assign(new Error('Invalid Reference Id'), {
      code: '2500',
      fault: { errors: [{ code: '2500', message: 'Invalid Reference Id' }] },
    }),
};

type Options = { method?: 'GET' | 'POST'; params?: Record<string, string>; body?: any };

export class FakeQuickBooks implements QboApi {
  invoices = new Map<string, QboInvoice>();
  payments = new Map<string, QboPayment>();
  customers = new Map<string, string>(); // id -> DisplayName
  // Invoices that exist but queries don't return yet (QuickBooks' search can lag behind a create)
  hiddenFromQueries = new Set<string>();
  // What CDC returns (by default nothing changed). Like QuickBooks, it rejects a cursor older than 30 days.
  cdcInvoices: { Id: string; SyncToken?: string }[] = [];
  // Sales tax QuickBooks adds to the lines we send (0: none), like a company with taxable items
  taxRate = 0;
  private withTax = (lineAmount: number) => {
    const tax = Math.round(lineAmount * this.taxRate * 100) / 100;
    return { TotalAmt: lineAmount + tax, TxnTaxDetail: { TotalTax: tax } };
  };
  calls: string[] = [];
  private failures: { call: string; error: () => unknown; afterSuccess: boolean }[] = [];
  private hooks: { call: string; run: () => Promise<unknown> }[] = [];
  private requestIds = new Map<string, QboInvoice>();
  // Unique across tests and runs, since they share the test database
  private idPrefix = `${process.pid}${Date.now()}`;
  private counter = 0;
  private nextId = () => `${this.idPrefix}${this.counter++}`;

  // A separate company per test when a test needs data no other test touches
  constructor(private realm = TEST_REALM) {}

  realmId = async () => this.realm;

  // The next call starting with `call` (e.g. "POST invoice") throws `error`
  failNext(call: string, error: () => unknown) {
    this.failures.push({ call, error, afterSuccess: false });
  }

  // The next such call is applied by QuickBooks, but the response is lost (throws `error` after it)
  failAfterNext(call: string, error: () => unknown) {
    this.failures.push({ call, error, afterSuccess: true });
  }

  // Runs `run` while the next call starting with `call` is in flight, before QuickBooks applies it
  // (e.g. a local change made while the worker is pushing)
  duringNext(call: string, run: () => Promise<unknown>) {
    this.hooks.push({ call, run });
  }

  // Simulates an edit made in QuickBooks
  edit(id: string, changes: Partial<QboInvoice>) {
    const invoice = this.invoices.get(id)!;
    this.invoices.set(id, { ...invoice, ...changes, SyncToken: String(Number(invoice.SyncToken) + 1) });
  }

  addInvoice(invoice: Partial<QboInvoice> & { Id: string }) {
    this.invoices.set(invoice.Id, { SyncToken: '0', CustomerRef: { value: '1', name: 'Remote Co' }, TotalAmt: 100, Balance: 100, CurrencyRef: { value: 'USD' }, DueDate: '2026-12-31', ...invoice });
  }

  request: QboApi['request'] = async (buildPath: (realmId: string) => string, { method = 'GET', params = {}, body }: Options = {}) => {
    const path = buildPath(this.realm);
    const call = `${method} ${path}`;
    this.calls.push(call);

    const hook = this.hooks.findIndex((h) => call.startsWith(h.call));
    if (hook !== -1) await this.hooks.splice(hook, 1)[0].run();

    const index = this.failures.findIndex((f) => call.startsWith(f.call));
    const failure = index === -1 ? null : this.failures.splice(index, 1)[0];
    if (failure && !failure.afterSuccess) throw failure.error();
    const result = await this.handle(method, path, params, body);
    if (failure) throw failure.error();
    return result;
  };

  private async handle(method: string, path: string, params: Record<string, string>, body: any) {
    const call = `${method} ${path}`;

    if (method === 'GET' && path.startsWith('invoice/')) {
      const invoice = this.invoices.get(path.slice('invoice/'.length));
      if (!invoice) throw qboErrors.notFound();
      return { Invoice: invoice };
    }
    if (method === 'GET' && path.startsWith('payment/')) {
      const payment = this.payments.get(path.slice('payment/'.length));
      if (!payment) throw qboErrors.notFound();
      return { Payment: payment };
    }
    if (method === 'POST' && path === 'customer') {
      const id = this.nextId();
      this.customers.set(id, body.DisplayName);
      return { Customer: { Id: id, DisplayName: body.DisplayName } };
    }
    if (method === 'POST' && path === 'invoice') return this.writeInvoice(params, body);
    if (method === 'POST' && path === 'payment') return this.createPayment(params, body);
    if (method === 'GET' && path === 'cdc') {
      if (Date.parse(params.changedSince) < Date.now() - 30 * 24 * 60 * 60 * 1000) {
        throw Object.assign(new Error('Invalid changedSince'), { fault: { errors: [{ code: '4000', message: 'changedSince must be within 30 days' }] } });
      }
      const invoices = this.cdcInvoices.length ? [{ Invoice: this.cdcInvoices }] : [];
      return { CDCResponse: [{ QueryResponse: invoices }], time: new Date().toISOString() };
    }
    throw new Error(`FakeQuickBooks: unsupported call ${call}`);
  }

  // A payment lowers the balance of the invoices it pays (bumping their SyncToken, like QuickBooks);
  // a repeated requestid returns the payment created the first time
  private paymentRequestIds = new Map<string, QboPayment>();
  private createPayment(params: Record<string, string>, body: any) {
    if (params.operation === 'delete') return this.deletePayment(body);
    if (params.requestid && this.paymentRequestIds.has(params.requestid)) return { Payment: this.paymentRequestIds.get(params.requestid) };
    const payment = {
      Id: this.nextId(),
      SyncToken: '0',
      Line: body.Line,
      CustomerRef: body.CustomerRef,
      PrivateNote: body.PrivateNote,
      TxnDate: body.TxnDate ?? new Date().toISOString().slice(0, 10),
      MetaData: { CreateTime: new Date().toISOString() },
    } as QboPayment;
    for (const line of body.Line) {
      for (const txn of line.LinkedTxn) {
        const invoice = this.invoices.get(txn.TxnId)!;
        this.invoices.set(txn.TxnId, {
          ...invoice,
          Balance: (invoice.Balance ?? 0) - line.Amount,
          SyncToken: String(Number(invoice.SyncToken) + 1),
          LinkedTxn: [...(invoice.LinkedTxn ?? []), { TxnId: payment.Id, TxnType: 'Payment' }],
        });
      }
    }
    this.payments.set(payment.Id, payment);
    if (params.requestid) this.paymentRequestIds.set(params.requestid, payment);
    return { Payment: payment };
  }

  // Deleting a payment gives its amount back to the invoices it paid (bumping their SyncToken)
  private deletePayment(body: any) {
    const payment = this.payments.get(body.Id);
    if (!payment) throw qboErrors.notFound();
    if ((payment.SyncToken ?? '0') !== body.SyncToken) throw qboErrors.staleObject();
    for (const line of payment.Line ?? []) {
      for (const txn of line.LinkedTxn ?? []) {
        const invoice = this.invoices.get(txn.TxnId);
        if (invoice) {
          const amount = (line as { Amount?: number }).Amount ?? 0;
          this.invoices.set(txn.TxnId, {
            ...invoice,
            Balance: (invoice.Balance ?? 0) + amount,
            SyncToken: String(Number(invoice.SyncToken) + 1),
            LinkedTxn: (invoice.LinkedTxn ?? []).filter((t) => t.TxnId !== body.Id),
          });
        }
      }
    }
    this.payments.delete(body.Id);
    return { Payment: { Id: body.Id, status: 'Deleted' } };
  }

  private writeInvoice(params: Record<string, string>, body: any) {
    if (params.operation === 'delete' || params.operation === 'void' || body.Id) {
      const current = this.invoices.get(body.Id);
      if (!current) throw qboErrors.notFound();
      if (current.SyncToken !== body.SyncToken) throw qboErrors.staleObject();
      if (params.operation === 'delete') {
        this.invoices.delete(body.Id);
        return { Invoice: { Id: body.Id, status: 'Deleted' } };
      }
      const next = String(Number(current.SyncToken) + 1);
      // A sparse update only changes the fields it sends
      const updated: QboInvoice = params.operation === 'void'
        ? { ...current, SyncToken: next, TotalAmt: 0, Balance: 0, PrivateNote: `Voided - ${current.PrivateNote ?? ''}` }
        : {
            ...current,
            SyncToken: next,
            ...(body.CustomerRef ? { CustomerRef: { value: body.CustomerRef.value, name: this.customers.get(body.CustomerRef.value) } } : {}),
            ...(body.DueDate ? { DueDate: body.DueDate } : {}),
            ...(body.Line
              ? { ...this.withTax(body.Line[0].Amount), Balance: this.withTax(body.Line[0].Amount).TotalAmt - ((current.TotalAmt ?? 0) - (current.Balance ?? 0)) }
              : {}),
            ...(body.EmailStatus ? { EmailStatus: body.EmailStatus } : {}),
          };
      this.invoices.set(body.Id, updated);
      return { Invoice: updated };
    }

    // Create: a repeated requestid returns the invoice created the first time, like QuickBooks
    if (params.requestid && this.requestIds.has(params.requestid)) return { Invoice: this.requestIds.get(params.requestid) };
    const created: QboInvoice = {
      Id: this.nextId(),
      SyncToken: '0',
      CustomerRef: { value: body.CustomerRef.value, name: this.customers.get(body.CustomerRef.value) },
      ...this.withTax(body.Line[0].Amount),
      Balance: this.withTax(body.Line[0].Amount).TotalAmt,
      CurrencyRef: body.CurrencyRef,
      DueDate: body.DueDate,
      PrivateNote: body.PrivateNote,
      EmailStatus: body.EmailStatus ?? 'NotSet',
      MetaData: { CreateTime: new Date().toISOString() },
    };
    this.invoices.set(created.Id, created);
    if (params.requestid) this.requestIds.set(params.requestid, created);
    return { Invoice: created };
  }

  query = async (query: string) => {
    this.calls.push(`QUERY ${query}`);
    const customer = query.match(/FROM Customer WHERE DisplayName = '(.*)'$/);
    if (customer) {
      const name = customer[1].replace(/\\'/g, "'").replace(/\\\\/g, '\\');
      const found = [...this.customers].find(([, n]) => n === name);
      return { QueryResponse: found ? { Customer: [{ Id: found[0] }] } : {} };
    }
    if (query.includes('FROM Item')) return { QueryResponse: { Item: [{ Id: '1' }] } };
    if (query.includes('FROM Account')) return { QueryResponse: {}, time: new Date().toISOString() };
    if (query === 'SELECT COUNT(*) FROM Invoice') return { QueryResponse: { totalCount: this.invoices.size } };
    const ids = query.match(/SELECT Id, SyncToken FROM Invoice STARTPOSITION (\d+) MAXRESULTS (\d+)/);
    if (ids) {
      const visible = [...this.invoices.values()].filter((i) => !this.hiddenFromQueries.has(i.Id));
      const all = visible.slice(Number(ids[1]) - 1, Number(ids[1]) - 1 + Number(ids[2])).map((i) => ({ Id: i.Id, SyncToken: i.SyncToken }));
      return { QueryResponse: all.length ? { Invoice: all } : {} };
    }
    const page = query.match(/FROM Invoice ORDERBY \S+ STARTPOSITION (\d+) MAXRESULTS (\d+)/);
    if (page) {
      const start = Number(page[1]) - 1;
      const all = [...this.invoices.values()].slice(start, start + Number(page[2]));
      return { QueryResponse: all.length ? { Invoice: all } : {}, time: new Date().toISOString() };
    }
    const paymentPage = query.match(/FROM Payment STARTPOSITION (\d+) MAXRESULTS (\d+)/);
    if (paymentPage) {
      const start = Number(paymentPage[1]) - 1;
      const all = [...this.payments.values()].slice(start, start + Number(paymentPage[2]));
      return { QueryResponse: all.length ? { Payment: all } : {} };
    }
    const paymentsOf = query.match(/FROM Payment WHERE CustomerRef = '([^']*)'/);
    if (paymentsOf) {
      const found = [...this.payments.values()].filter((p) => (p as { CustomerRef?: { value: string } }).CustomerRef?.value === paymentsOf[1]);
      return { QueryResponse: found.length ? { Payment: found } : {} };
    }
    const byCustomer = query.match(/FROM Invoice WHERE CustomerRef = '([^']*)'/);
    if (byCustomer) {
      return { QueryResponse: { Invoice: [...this.invoices.values()].filter((i) => i.CustomerRef?.value === byCustomer[1]) } };
    }
    throw new Error(`FakeQuickBooks: unsupported query ${query}`);
  };

  countCalls(prefix: string) {
    return this.calls.filter((c) => c.startsWith(prefix)).length;
  }
}

// The tests share the test database with each other, so they only claim jobs of their own entities
export const runJobs = async (entityKeys: string[], qbo: QboApi, workerId = 'test-worker') => {
  const processed: SyncJob[] = [];
  for (;;) {
    const job = await claimNextJob({ workerId, leaseSeconds: 60, entityKeys });
    if (!job) return processed;
    await processJob(job, qbo);
    processed.push(job);
  }
};

export const jobsFor = async (entityKey: string) => {
  const pool = await getPool();
  return pool.any(sql.type(SyncJob)`SELECT * FROM sync_jobs WHERE entity_key = ${entityKey} ORDER BY created_at, id`);
};

export const loadInvoice = async (id: number) => {
  const pool = await getPool();
  return pool.one(sql.type(SyncInvoice)`SELECT * FROM invoices WHERE id = ${id}`);
};

// Makes jobs in backoff runnable now (instead of waiting in the test)
export const runNow = async (entityKey: string) => {
  const pool = await getPool();
  await pool.query(sql.unsafe`UPDATE sync_jobs SET next_run_at = now() WHERE entity_key = ${entityKey} AND status = 'PENDING'`);
};

// A QuickBooks connection row, so the code that reads the connected company works in the test database
export const ensureTestConnection = async () => {
  const pool = await getPool();
  await pool.query(sql.unsafe`
    INSERT INTO quickbooks_connection (realm_id, token) VALUES (${TEST_REALM}, '{}'::jsonb)
    ON CONFLICT (realm_id) DO NOTHING
  `);
};

let counter = 0;
export const uniqueName = (prefix: string) => `${prefix} ${process.pid}-${Date.now()}-${counter++}`;
