// Runs one claimed job and decides what happens when it fails:
// - retryable (network, 5xx, 429, stale version, ...): retried with exponential backoff and jitter,
//   until max_attempts, then FAILED
// - permanent (validation): FAILED right away
// - ambiguous create (timeout after sending it): UNKNOWN, resolved by reconciliation, never resent blindly
// - not connected / auth: waits for a connection without using up attempts
// - entity busy: retried in a few seconds without using up attempts
// FAILED and UNKNOWN jobs can be inspected and retried with GET/POST /sync/jobs.
import { AmbiguousWriteError, classifyError, DeferError, errorMessage, LeaseLostError } from './errors';
import { setSyncState } from './invoices.repository';
import { log } from './logger';
import { qbo as quickbooks, type QboApi } from './quickbooks.client';
import { processInboundJob } from './sync.inbound';
import { processOutboundInvoice } from './sync.outbound';
import { processOutboundPayment, setPaymentSyncState } from './sync.payments';
import { failJob, markUnknown, rescheduleJob, type SyncJob } from './sync.repository';

const RETRY_BASE_SECONDS = Number(process.env.SYNC_RETRY_BASE_SECONDS || 10);
const RETRY_MAX_SECONDS = 15 * 60;

// Exponential backoff with "equal jitter": between half and all of base * 2^(attempt - 1), capped.
// The randomness spreads out retries so many failed jobs don't hit QuickBooks at the same moment.
export const retryDelaySeconds = (attempt: number) => {
  const exponential = Math.min(RETRY_MAX_SECONDS, RETRY_BASE_SECONDS * 2 ** Math.max(0, attempt - 1));
  return exponential / 2 + Math.random() * (exponential / 2);
};

// The sync_status of an invoice or payment mirrors its outbound job (inbound jobs don't change it on failure)
const setInvoiceState = async (job: SyncJob, syncStatus: 'pending' | 'unknown' | 'failed', error: string | null) => {
  if (job.direction !== 'OUTBOUND') return;
  if (job.entity_type === 'invoice') await setSyncState(Number(job.entity_id), syncStatus, error);
  if (job.entity_type === 'payment') await setPaymentSyncState(Number(job.entity_id), syncStatus, error);
};

const handleFailure = async (job: SyncJob, err: unknown, context: Record<string, unknown>) => {
  const message = errorMessage(err);
  // Another worker took the job over after our lease expired (and may have synced it): leave the job,
  // the invoice and the event to it
  const leaseLost = () => log.warn('sync.job.lease_lost', { ...context, error: message });

  if (err instanceof LeaseLostError) return leaseLost();
  if (err instanceof DeferError) {
    await rescheduleJob(job, { errorClass: 'deferred', message, delaySeconds: 5, countAttempt: false });
    return log.info('sync.job.deferred', { ...context, reason: message });
  }
  if (err instanceof AmbiguousWriteError) {
    if (!(await markUnknown(job, message))) return leaseLost();
    await setInvoiceState(job, 'unknown', message);
    return log.warn('sync.job.unknown', { ...context, error_class: 'ambiguous', error: message });
  }

  const { errorClass, retryable } = classifyError(err);
  if (!retryable) {
    if (!(await failJob(job, errorClass, message))) return leaseLost();
    await setInvoiceState(job, 'failed', message);
    return log.error('sync.job.failed', { ...context, error_class: errorClass, error: message });
  }

  const waitingForConnection = errorClass === 'not_connected' || errorClass === 'auth';
  const delaySeconds = waitingForConnection
    ? 60
    : errorClass === 'rate_limited'
      ? Math.max(60, retryDelaySeconds(job.attempts))
      : retryDelaySeconds(job.attempts);

  const status = await rescheduleJob(job, { errorClass, message, delaySeconds, countAttempt: !waitingForConnection });
  if (status === null) return leaseLost();
  if (status === 'FAILED') {
    await setInvoiceState(job, 'failed', message);
    return log.error('sync.job.failed', { ...context, error_class: errorClass, error: message, reason: 'max attempts reached' });
  }
  await setInvoiceState(job, 'pending', message);
  log.warn('sync.job.retry_scheduled', { ...context, error_class: errorClass, error: message, delay_seconds: Math.round(delaySeconds) });
};

export const processJob = async (job: SyncJob, qbo: QboApi = quickbooks) => {
  const started = Date.now();
  const context = {
    job_id: job.id,
    event_id: job.event_id,
    direction: job.direction,
    entity_type: job.entity_type,
    entity_id: job.entity_id,
    realm_id: job.realm_id,
    operation: job.operation,
    attempt: job.attempts,
  };

  try {
    if (job.direction === 'INBOUND') await processInboundJob(job, qbo);
    else if (job.entity_type === 'payment') await processOutboundPayment(job, qbo);
    else await processOutboundInvoice(job, qbo);
    log.info('sync.job.completed', { ...context, duration_ms: Date.now() - started });
  } catch (err) {
    try {
      await handleFailure(job, err, { ...context, duration_ms: Date.now() - started });
    } catch (handlingError) {
      // e.g. the database is down: the lease expires and the job is recovered later
      log.error('sync.job.failure_not_recorded', { ...context, error: errorMessage(err), cause: errorMessage(handlingError) });
    }
  }
};
