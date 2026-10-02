// Pure mapping between QuickBooks entities and local invoices (no I/O)
import type { InvoiceStatus, Snapshot, SyncInvoice } from './invoices.schema';
import { moneyFromNumber } from './money';
import { INVOICE_STATUS } from './statuses';

// The QuickBooks invoice fields we use
export type QboInvoice = {
  Id: string;
  SyncToken: string;
  status?: 'Deleted'; // only on deleted entities returned by CDC
  CustomerRef?: { value?: string; name?: string };
  TotalAmt?: number;
  Balance?: number;
  CurrencyRef?: { value: string };
  DueDate?: string;
  TxnDate?: string;
  PrivateNote?: string;
  EmailStatus?: string; // 'NotSet' | 'NeedToSend' | 'EmailSent'
  LinkedTxn?: { TxnId: string; TxnType: string }[]; // e.g. the payments applied to it
  TxnTaxDetail?: { TotalTax?: number }; // sales tax, included in TotalAmt
  MetaData?: { CreateTime?: string; LastUpdatedTime?: string };
};

// A payment changes the balance (and so the paid status) of the invoices it pays
export type QboPayment = {
  Id: string;
  SyncToken?: string;
  status?: 'Deleted';
  TxnDate?: string;
  PrivateNote?: string;
  CustomerRef?: { value?: string };
  Line?: { Amount?: number; LinkedTxn?: { TxnId: string; TxnType: string }[] }[];
};

// How much of the payment goes to each invoice (a payment can pay several)
// What a payment pays of each invoice: its lines added up per invoice (several lines can pay the same one)
export const paidInvoiceLines = (payment: QboPayment) => {
  const byInvoice = new Map<string, number>();
  for (const line of payment.Line ?? []) {
    for (const txn of line.LinkedTxn ?? []) {
      if (txn.TxnType === 'Invoice') byInvoice.set(txn.TxnId, (byInvoice.get(txn.TxnId) ?? 0) + (line.Amount ?? 0));
    }
  }
  return [...byInvoice].map(([invoiceId, amount]) => ({ invoiceId, amount: moneyFromNumber(Math.round(amount * 100) / 100) }));
};

export const paidInvoiceIds = (payment: QboPayment) => paidInvoiceLines(payment).map((line) => line.invoiceId);

// Stable integration reference for a payment recorded here (requestid and PrivateNote), like invoices'
export const localPaymentKey = (payment: { id: number; created_at: string }) =>
  `local-payment-${payment.id}-${Date.parse(payment.created_at)}`;

export const parseLocalPaymentKey = (note: string | undefined) => {
  const match = note?.match(/local-payment-(\d+)-(\d+)/);
  return match ? { key: match[0], id: Number(match[1]) } : null;
};

// Stable integration reference for a local invoice. Sent as the create's requestid and written into
// the QuickBooks invoice's PrivateNote, so we can recognize invoices we created even when we never got
// the create response. created_at is included because local ids restart if the database is recreated.
// Max 50 chars (requestid limit).
export const localInvoiceKey = (invoice: { id: number; created_at: string }) =>
  `local-invoice-${invoice.id}-${Date.parse(invoice.created_at)}`;

export const parseLocalInvoiceKey = (note: string | undefined) => {
  const match = note?.match(/local-invoice-(\d+)-(\d+)/);
  return match ? { key: match[0], id: Number(match[1]) } : null;
};

export const remoteSnapshot = (qb: QboInvoice): Snapshot => ({
  customer_name: qb.CustomerRef?.name ?? 'Unknown customer',
  amount: moneyFromNumber(qb.TotalAmt),
  currency: qb.CurrencyRef?.value ?? 'USD',
  due_date: qb.DueDate ?? qb.TxnDate ?? '',
});

export const localSnapshot = (invoice: SyncInvoice): Snapshot => ({
  customer_name: invoice.customer_name,
  amount: invoice.amount,
  currency: invoice.currency,
  due_date: invoice.due_date,
});

const FIELDS = ['customer_name', 'amount', 'currency', 'due_date'] as const;

export const changedFields = (a: Snapshot, b: Snapshot) => FIELDS.filter((field) => a[field] !== b[field]);
export const sameSnapshot = (a: Snapshot, b: Snapshot) => changedFields(a, b).length === 0;

export const remoteBalance = (qb: QboInvoice) => moneyFromNumber(qb.Balance ?? qb.TotalAmt);

// QuickBooks has no void status field: voiding zeroes the amounts and prefixes the PrivateNote with
// "Voided". Checked against the real sandbox; used when the notification doesn't say "void" (e.g. CDC).
export const isVoidedInQuickBooks = (qb: QboInvoice) =>
  (qb.TotalAmt ?? 0) === 0 && (qb.PrivateNote ?? '').startsWith('Voided');

// "Sent" in QuickBooks: emailed, or marked as sent (in the app or through the API). Marking it as sent
// in the QuickBooks app doesn't change the invoice's SyncToken (checked in the sandbox).
export const isSentInQuickBooks = (qb: QboInvoice) => qb.EmailStatus === 'EmailSent';

// The local status from QuickBooks' state: paid when nothing is left to pay (payments are recorded in
// QuickBooks), and a draft becomes sent once QuickBooks sent it. "Sent" only moves forward.
export const statusFromQuickBooks = (qb: QboInvoice, balance: string, amount: string, current: InvoiceStatus): InvoiceStatus => {
  if (current === INVOICE_STATUS.VOID) return INVOICE_STATUS.VOID;
  if (balance === '0.00' && amount !== '0.00') return INVOICE_STATUS.PAID;
  if (current === INVOICE_STATUS.PAID) return INVOICE_STATUS.SENT;
  if (current === INVOICE_STATUS.DRAFT && isSentInQuickBooks(qb)) return INVOICE_STATUS.SENT;
  return current;
};

// A local "paid" that QuickBooks doesn't have yet: marked paid here while the balance last read from
// QuickBooks is still above 0 (a paid invoice synced from QuickBooks always has balance 0). Only then is
// a payment recorded for it; a stale "paid" (e.g. its payment was deleted in QuickBooks) is not paid again.
export const paidLocallyUnpushed = (invoice: SyncInvoice) => invoice.status === INVOICE_STATUS.PAID && Number(invoice.balance) > 0;

// statusFromQuickBooks for a local invoice. A local "paid" not pushed yet stays paid: its outbound job
// records the payment, so QuickBooks' balance doesn't undo it meanwhile.
export const syncedStatus = (invoice: SyncInvoice, qb: QboInvoice, balance: string, amount: string): InvoiceStatus =>
  paidLocallyUnpushed(invoice) ? INVOICE_STATUS.PAID : statusFromQuickBooks(qb, balance, amount, invoice.status);

// A local status QuickBooks doesn't have yet, so it still has to be pushed: "paid" while QuickBooks
// shows a balance, "sent" not sent there, "void" not voided there
export const statusLeftToPush = (status: InvoiceStatus, qb: QboInvoice) =>
  (status === INVOICE_STATUS.PAID && remoteBalance(qb) !== '0.00') ||
  (status === INVOICE_STATUS.SENT && !isSentInQuickBooks(qb)) ||
  (status === INVOICE_STATUS.VOID && !isVoidedInQuickBooks(qb));

// Whether QuickBooks already has everything the local invoice says: the same content, no deletion and
// no status left to push. Only then can the local version be recorded as synced without pushing it.
export const matchesQuickBooks = (invoice: SyncInvoice, qb: QboInvoice) =>
  !invoice.deleted_at && sameSnapshot(localSnapshot(invoice), remoteSnapshot(qb)) && !statusLeftToPush(invoice.status, qb);

// QuickBooks applies sales tax to it (included in TotalAmt)
export const isTaxedInQuickBooks = (qb: QboInvoice) => (qb.TxnTaxDetail?.TotalTax ?? 0) > 0;

// SyncToken is QuickBooks' version number for an entity: a higher one is newer
export const isNewerToken = (candidate: string, current: string | null) =>
  current === null || Number(candidate) > Number(current);
