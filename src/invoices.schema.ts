import { z } from 'zod';
import { MoneyInput } from './money';
import {
  CREATE_INVOICE_STATUSES,
  INVOICE_STATUS,
  STATUSES,
  SYNC_STATUSES,
  type InvoiceStatus,
  type SyncStatus,
} from './statuses';

export { CREATE_INVOICE_STATUSES, INVOICE_STATUS, STATUSES, SYNC_STATUS, SYNC_STATUSES } from './statuses';

// The invoice content kept in sync with QuickBooks. last_synced_snapshot stores it as both sides
// agreed at the last sync, so we can tell which side changed what.
export const Snapshot = z.object({
  customer_name: z.string(),
  amount: z.string(),
  currency: z.string(),
  due_date: z.string(),
});

// Invoice as returned by the API
export const Invoice = z.object({
  id: z.number(),
  customer_name: z.string(),
  amount: z.string(), // decimal string, e.g. "1500.00"
  balance: z.string(), // still to be paid, from QuickBooks payments
  currency: z.string(),
  status: z.enum(STATUSES),
  due_date: z.string(),
  quickbooks_id: z.string().nullable(),
  // QuickBooks applies sales tax to it: its content (amount with tax, customer, due date) is then owned by
  // QuickBooks and can't be edited here (the local model is one line with one amount)
  taxed_in_quickbooks: z.boolean(),
  sync_status: z.enum(SYNC_STATUSES),
  sync_error: z.string().nullable(),
  sync_conflict: z.record(z.unknown()).nullable(),
  created_at: z.string(),
  updated_at: z.string(),
});

// Invoice with the internal columns the sync needs
export const SyncInvoice = Invoice.extend({
  quickbooks_realm_id: z.string().nullable(),
  quickbooks_sync_token: z.string().nullable(),
  version: z.number(),
  synced_version: z.number().nullable(),
  last_synced_snapshot: Snapshot.nullable(),
  deleted_at: z.string().nullable(),
  voided_at: z.string().nullable(),
});

// "paid" records a payment in QuickBooks for the invoice's balance (and a payment received in
// QuickBooks makes the invoice paid here)
export const CreateInvoiceInput = z.object({
  customer_name: z.string().trim().min(1),
  amount: MoneyInput,
  currency: z.string().length(3).toUpperCase().default('USD'),
  status: z.enum(CREATE_INVOICE_STATUSES).default(INVOICE_STATUS.DRAFT),
  due_date: z.string().date(), // YYYY-MM-DD
});

// For updates every field is optional, but at least one must be sent. "void" voids it in QuickBooks too.
export const UpdateInvoiceInput = CreateInvoiceInput.extend({ status: z.enum(STATUSES) })
  .partial()
  .refine((data) => Object.values(data).some((v) => v !== undefined), {
    message: 'At least one field must be provided',
  });

export type Invoice = z.infer<typeof Invoice>;
export type SyncInvoice = z.infer<typeof SyncInvoice>;
export type Snapshot = z.infer<typeof Snapshot>;
export type CreateInvoiceInput = z.infer<typeof CreateInvoiceInput>;
export type UpdateInvoiceInput = z.infer<typeof UpdateInvoiceInput>;
export type { InvoiceStatus, SyncStatus };

// Whether a change touches the content kept in sync (not only the status)
export const touchesContent = (changes: object) =>
  (['customer_name', 'amount', 'currency', 'due_date'] as const).some((field) => field in changes && (changes as Record<string, unknown>)[field] !== undefined);
