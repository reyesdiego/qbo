export const INVOICE_STATUS = {
  DRAFT: 'draft',
  SENT: 'sent',
  PAID: 'paid',
  VOID: 'void',
} as const;
export const STATUSES = [INVOICE_STATUS.DRAFT, INVOICE_STATUS.SENT, INVOICE_STATUS.PAID, INVOICE_STATUS.VOID] as const;
export const CREATE_INVOICE_STATUSES = [INVOICE_STATUS.DRAFT, INVOICE_STATUS.SENT, INVOICE_STATUS.PAID] as const;
export type InvoiceStatus = (typeof STATUSES)[number];

export const SYNC_STATUS = {
  PENDING: 'pending',
  SYNCED: 'synced',
  CONFLICT: 'conflict',
  UNKNOWN: 'unknown',
  FAILED: 'failed',
} as const;
export const SYNC_STATUSES = [
  SYNC_STATUS.PENDING,
  SYNC_STATUS.SYNCED,
  SYNC_STATUS.CONFLICT,
  SYNC_STATUS.UNKNOWN,
  SYNC_STATUS.FAILED,
] as const;
export const PAYMENT_SYNC_STATUSES = [SYNC_STATUS.PENDING, SYNC_STATUS.SYNCED, SYNC_STATUS.UNKNOWN, SYNC_STATUS.FAILED] as const;
export type SyncStatus = (typeof SYNC_STATUSES)[number];
export type PaymentSyncStatus = (typeof PAYMENT_SYNC_STATUSES)[number];

export const JOB_STATUS = {
  PENDING: 'PENDING',
  PROCESSING: 'PROCESSING',
  COMPLETED: 'COMPLETED',
  FAILED: 'FAILED',
  UNKNOWN: 'UNKNOWN',
} as const;
export const JOB_STATUSES = [
  JOB_STATUS.PENDING,
  JOB_STATUS.PROCESSING,
  JOB_STATUS.COMPLETED,
  JOB_STATUS.FAILED,
  JOB_STATUS.UNKNOWN,
] as const;
export type JobStatus = (typeof JOB_STATUSES)[number];
