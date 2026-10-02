// The PostgreSQL synchronization queue (sync_jobs) and the log of incoming notifications (sync_events).
//
// A job is claimed by one worker at a time: claiming sets a random claim_token and a lease. Every
// later update checks the claim_token, so a worker whose lease expired (and whose job another worker
// took over) can't finish or overwrite it. No transaction stays open while talking to QuickBooks.
import { sql, type CommonQueryMethods } from 'slonik';
import { z } from 'zod';
import { getPool } from './db';
import { LeaseLostError } from './errors';
import { shouldFail } from './faults';
import { JOB_STATUS, JOB_STATUSES, SYNC_STATUS, type JobStatus } from './statuses';

export const SyncJob = z.object({
  id: z.string(),
  direction: z.enum(['INBOUND', 'OUTBOUND']),
  entity_type: z.enum(['invoice', 'payment', 'account']),
  entity_id: z.string(),
  entity_key: z.string(),
  realm_id: z.string().nullable(),
  operation: z.string(),
  event_id: z.number().nullable(),
  payload: z.record(z.unknown()),
  status: z.enum(JOB_STATUSES),
  attempts: z.number(),
  max_attempts: z.number(),
  next_run_at: z.string(),
  locked_at: z.string().nullable(),
  locked_by: z.string().nullable(),
  claim_token: z.string().nullable(),
  lease_expires_at: z.string().nullable(),
  create_sent_at: z.string().nullable(),
  last_error: z.string().nullable(),
  last_error_class: z.string().nullable(),
  duration_ms: z.number().nullable(),
  completed_at: z.string().nullable(),
  created_at: z.string(),
  updated_at: z.string(),
});
export type SyncJob = z.infer<typeof SyncJob>;
export { JOB_STATUSES };

const Id = z.object({ id: z.string() });
const Status = z.object({ status: z.enum(JOB_STATUSES) });

export const invoiceEntityKey = (invoiceId: number | string) => `invoice:${invoiceId}`;

// ---------------------------------------------------------------------------
// Enqueueing (always inside the transaction that saves the change)
// ---------------------------------------------------------------------------

type NewJob = {
  direction: 'INBOUND' | 'OUTBOUND';
  entityType: 'invoice' | 'payment' | 'account';
  entityId: string;
  entityKey: string;
  operation: string;
  realmId?: string | null;
  eventId?: number | null;
  payload?: Record<string, unknown>;
};

// skipIfPending: don't add a job when one for the same entity is already waiting. It will load the
// entity's latest state when it runs, so it covers this change too. (Payment jobs share their
// invoice's entity key to run in order with it, so the entity type is compared too.)
export const enqueueJob = async (db: CommonQueryMethods, job: NewJob, { skipIfPending = false } = {}) => {
  const inserted = await db.maybeOne(sql.type(Id)`
    INSERT INTO sync_jobs
      (direction, entity_type, entity_id, entity_key, realm_id, operation, event_id, payload, max_attempts)
    SELECT ${job.direction}, ${job.entityType}, ${job.entityId}, ${job.entityKey}, ${job.realmId ?? null},
           ${job.operation}, ${job.eventId ?? null}, ${JSON.stringify(job.payload ?? {})}::jsonb,
           ${Number(process.env.SYNC_MAX_ATTEMPTS || 8)}
    WHERE ${!skipIfPending} OR NOT EXISTS (
      SELECT 1 FROM sync_jobs
      WHERE entity_key = ${job.entityKey} AND direction = ${job.direction} AND entity_type = ${job.entityType}
        AND status = ${JOB_STATUS.PENDING}
    )
    RETURNING id
  `);
  // Wakes the workers up (not if nothing was added). NOTIFY is transactional: it's only delivered if
  // this transaction commits.
  if (inserted) await db.query(sql.unsafe`SELECT pg_notify('sync_jobs', '')`);
  return inserted?.id ?? null;
};

// Queues a push of the invoice's latest state, unless one is already waiting
export const enqueueInvoicePush = (db: CommonQueryMethods, invoiceId: number) =>
  enqueueJob(
    db,
    { direction: 'OUTBOUND', entityType: 'invoice', entityId: String(invoiceId), entityKey: invoiceEntityKey(invoiceId), operation: 'upsert' },
    { skipIfPending: true },
  );

type NewEvent = {
  source: 'webhook' | 'reconciliation';
  realmId: string;
  entity: string;
  id: string;
  operation: string;
  eventKey: string;
  payload: unknown;
  ignoredReason?: string | null; // recorded for auditing, but no job
};

const SYNCED_ENTITIES = ['invoice', 'payment', 'account'] as const;

// Jobs for the same invoice share a key so they run one at a time. Before the QuickBooks invoice is
// linked to a local one, its key is based on the QuickBooks id.
const inboundEntityKey = async (db: CommonQueryMethods, realmId: string, entity: string, id: string) => {
  if (entity === 'invoice') {
    const local = await db.maybeOne(sql.type(z.object({ id: z.number() }))`
      SELECT id FROM invoices WHERE quickbooks_realm_id = ${realmId} AND quickbooks_id = ${id}
    `);
    if (local) return invoiceEntityKey(local.id);
  }
  return `qbo-${entity}:${realmId}:${id}`;
};

// Records a notification and queues its job, in the caller's transaction. A notification already
// recorded (same event_key, e.g. a webhook redelivery) is skipped.
export const recordEventAndEnqueue = async (db: CommonQueryMethods, event: NewEvent) => {
  const recorded = await db.maybeOne(sql.type(z.object({ id: z.number() }))`
    INSERT INTO sync_events
      (source, realm_id, entity_type, external_entity_id, operation, event_key, payload, processing_status, error)
    VALUES
      (${event.source}, ${event.realmId}, ${event.entity}, ${event.id}, ${event.operation}, ${event.eventKey},
       ${JSON.stringify(event.payload)}::jsonb, ${event.ignoredReason ? 'ignored' : 'pending'},
       ${event.ignoredReason ?? null})
    ON CONFLICT (event_key) DO NOTHING
    RETURNING id
  `);
  if (!recorded) return 'duplicate' as const;
  if (event.ignoredReason) return 'ignored' as const;
  if (shouldFail('webhook.enqueue')) throw new Error('Injected fault: webhook.enqueue');

  await enqueueJob(db, {
    direction: 'INBOUND',
    entityType: event.entity as (typeof SYNCED_ENTITIES)[number],
    entityId: event.id,
    entityKey: await inboundEntityKey(db, event.realmId, event.entity, event.id),
    realmId: event.realmId,
    operation: event.operation,
    eventId: recorded.id,
  });
  return 'enqueued' as const;
};

export const isSyncedEntity = (entity: string) => (SYNCED_ENTITIES as readonly string[]).includes(entity);

// ---------------------------------------------------------------------------
// Claiming and finishing jobs
// ---------------------------------------------------------------------------

// Limits a claim to some entities (used by tests, which share one database)
const onlyEntityKeys = (entityKeys: string[] | undefined, column = sql.identifier(['entity_key'])) =>
  entityKeys ? sql.fragment`AND ${column} = ANY(${sql.array(entityKeys, 'text')})` : sql.fragment``;

type ClaimOptions = { workerId: string; leaseSeconds: number; entityKeys?: string[] };

// Claims the next runnable job. FOR UPDATE SKIP LOCKED lets several workers claim at the same time
// without getting the same job. A job only runs when no other job for the same entity is running
// or unresolved (UNKNOWN), and after the older ones: jobs for one invoice run one at a time, in order.
// entityKeys limits the claim to some entities (used by tests).
export const claimNextJob = async ({ workerId, leaseSeconds, entityKeys }: ClaimOptions) => {
  const pool = await getPool();

  return pool.maybeOne(sql.type(SyncJob)`
    UPDATE sync_jobs
    SET status = ${JOB_STATUS.PROCESSING}, attempts = attempts + 1, claim_token = gen_random_uuid(),
        locked_by = ${workerId}, locked_at = now(),
        lease_expires_at = now() + make_interval(secs => ${leaseSeconds}), updated_at = now()
    WHERE id = (
      SELECT j.id FROM sync_jobs j
      WHERE j.status = ${JOB_STATUS.PENDING} AND j.next_run_at <= now() ${onlyEntityKeys(entityKeys, sql.identifier(['j', 'entity_key']))}
        AND NOT EXISTS (
          SELECT 1 FROM sync_jobs other
          WHERE other.entity_key = j.entity_key AND other.id <> j.id
            AND (other.status IN (${JOB_STATUS.PROCESSING}, ${JOB_STATUS.UNKNOWN})
                 OR (other.status = ${JOB_STATUS.PENDING} AND (other.created_at, other.id) < (j.created_at, j.id)))
        )
      ORDER BY j.next_run_at, j.created_at
      LIMIT 1
      FOR UPDATE SKIP LOCKED
    )
    RETURNING *
  `);
};

// Only the worker holding the job's claim token can change it
const owned = (job: SyncJob) =>
  sql.fragment`id = ${job.id} AND claim_token = ${job.claim_token} AND status IN (${JOB_STATUS.PROCESSING}, ${JOB_STATUS.UNKNOWN})`;

// Recorded right before sending a create to QuickBooks: if the worker dies after this, the job
// becomes UNKNOWN instead of being retried, because the invoice may already exist in QuickBooks
export const markCreateSent = async (job: SyncJob, details: Record<string, unknown>) => {
  const pool = await getPool();
  const updated = await pool.maybeOne(sql.type(Id)`
    UPDATE sync_jobs
    SET create_sent_at = now(), payload = payload || ${JSON.stringify(details)}::jsonb, updated_at = now()
    WHERE ${owned(job)}
    RETURNING id
  `);
  if (!updated) throw new LeaseLostError(`Job ${job.id} was reclaimed by another worker`);
};

// Marks the job completed, in the same transaction as the changes it made. Throws (rolling the
// transaction back) if another worker has taken the job over.
export const completeJob = async (db: CommonQueryMethods, job: SyncJob, result: string) => {
  const done = await db.maybeOne(sql.type(Id)`
    UPDATE sync_jobs
    SET status = ${JOB_STATUS.COMPLETED}, completed_at = now(), claim_token = NULL, lease_expires_at = NULL,
        duration_ms = (extract(epoch FROM now() - locked_at) * 1000)::int,
        payload = payload || ${JSON.stringify({ result })}::jsonb, updated_at = now()
    WHERE ${owned(job)}
    RETURNING id
  `);
  if (!done) throw new LeaseLostError(`Job ${job.id} was reclaimed by another worker`);
  if (job.event_id) {
    await db.query(sql.unsafe`
      UPDATE sync_events SET processing_status = 'processed', processed_at = now() WHERE id = ${job.event_id}
    `);
  }
};

type Reschedule = { errorClass: string; message: string; delaySeconds: number; countAttempt: boolean };

// Retry later. A counted attempt beyond max_attempts fails the job instead. Not counting is for
// waits that aren't the job's fault (not connected, entity busy). Returns the new status, or null
// if the job was reclaimed meanwhile.
export const rescheduleJob = async (job: SyncJob, { errorClass, message, delaySeconds, countAttempt }: Reschedule) => {
  const pool = await getPool();
  const updated = await pool.maybeOne(sql.type(Status)`
    UPDATE sync_jobs
    SET status = CASE WHEN ${countAttempt} AND attempts >= max_attempts THEN ${JOB_STATUS.FAILED} ELSE ${JOB_STATUS.PENDING} END,
        attempts = CASE WHEN ${countAttempt} THEN attempts ELSE attempts - 1 END,
        next_run_at = now() + make_interval(secs => ${delaySeconds}),
        claim_token = NULL, lease_expires_at = NULL, create_sent_at = NULL,
        last_error = ${message}, last_error_class = ${errorClass}, updated_at = now()
    WHERE ${owned(job)}
    RETURNING status
  `);
  if (updated?.status === JOB_STATUS.FAILED) await markEventFailed(job, message);
  return updated?.status ?? null;
};

// Returns false if the job was reclaimed meanwhile (then nothing is changed)
export const failJob = async (job: SyncJob, errorClass: string, message: string) => {
  const pool = await getPool();
  const updated = await pool.maybeOne(sql.type(Id)`
    UPDATE sync_jobs
    SET status = ${JOB_STATUS.FAILED}, claim_token = NULL, lease_expires_at = NULL,
        last_error = ${message}, last_error_class = ${errorClass}, updated_at = now()
    WHERE ${owned(job)}
    RETURNING id
  `);
  if (updated) await markEventFailed(job, message);
  return updated !== null;
};

// The create may or may not have happened: park the job until reconciliation finds out. While
// UNKNOWN, it also blocks other jobs for the same invoice (a later update can't create a duplicate).
// Returns false if the job was reclaimed meanwhile (then nothing is changed)
export const markUnknown = async (job: SyncJob, message: string) => {
  const pool = await getPool();
  const updated = await pool.maybeOne(sql.type(Id)`
    UPDATE sync_jobs
    SET status = ${JOB_STATUS.UNKNOWN}, claim_token = NULL, lease_expires_at = NULL, next_run_at = now(),
        last_error = ${message}, last_error_class = 'ambiguous', updated_at = now()
    WHERE ${owned(job)}
    RETURNING id
  `);
  return updated !== null;
};

const markEventFailed = async (job: Pick<SyncJob, 'event_id'>, message: string) => {
  if (!job.event_id) return;
  const pool = await getPool();
  await pool.query(sql.unsafe`
    UPDATE sync_events SET processing_status = ${SYNC_STATUS.FAILED}, processed_at = now(), error = ${message}
    WHERE id = ${job.event_id}
  `);
};

// Jobs whose worker died or took longer than its lease: retried, unless a create may have been sent
// (then UNKNOWN, to be reconciled) or attempts are exhausted
export const recoverExpiredLeases = async () => {
  const pool = await getPool();
  const recovered = await pool.any(sql.type(SyncJob.pick({ id: true, status: true, direction: true, entity_type: true, entity_id: true, event_id: true }))`
    UPDATE sync_jobs
    SET status = CASE WHEN status = ${JOB_STATUS.UNKNOWN} OR create_sent_at IS NOT NULL THEN ${JOB_STATUS.UNKNOWN}
                      WHEN attempts >= max_attempts THEN ${JOB_STATUS.FAILED}
                      ELSE ${JOB_STATUS.PENDING} END,
        claim_token = NULL, lease_expires_at = NULL, locked_by = NULL,
        last_error = CASE WHEN status = ${JOB_STATUS.UNKNOWN} THEN last_error
                          ELSE 'Lease expired: the worker stopped or took too long' END,
        last_error_class = CASE WHEN status = ${JOB_STATUS.UNKNOWN} THEN last_error_class ELSE 'lease_expired' END,
        updated_at = now()
    WHERE status IN (${JOB_STATUS.PROCESSING}, ${JOB_STATUS.UNKNOWN}) AND lease_expires_at < now()
    RETURNING id, status, direction, entity_type, entity_id, event_id
  `);
  for (const job of recovered) {
    if (job.status === JOB_STATUS.FAILED) await markEventFailed(job, 'Lease expired on the last attempt');
  }
  return recovered;
};

// Whether QuickBooks sent a delete notification for this entity after the given job was queued
export const hasLaterDeleteNotification = async (job: SyncJob) => {
  const pool = await getPool();
  return pool.exists(sql.type(Id)`
    SELECT id FROM sync_jobs
    WHERE direction = 'INBOUND' AND entity_type = ${job.entity_type} AND realm_id = ${job.realm_id}
      AND entity_id = ${job.entity_id} AND operation = 'delete' AND created_at > ${job.created_at}
  `);
};

// ---------------------------------------------------------------------------
// UNKNOWN jobs (reconciliation)
// ---------------------------------------------------------------------------

// Claims an UNKNOWN create that's older than the grace period (QuickBooks' search can lag behind a
// create). It stays UNKNOWN; the claim token just keeps two workers from reconciling it at once.
// entityKeys limits it to some entities, like claimNextJob (used by tests)
export const claimUnknownJob = async ({ workerId, leaseSeconds, graceSeconds, entityKeys }: ClaimOptions & { graceSeconds: number }) => {
  const pool = await getPool();
  return pool.maybeOne(sql.type(SyncJob)`
    UPDATE sync_jobs
    SET claim_token = gen_random_uuid(), locked_by = ${workerId}, locked_at = now(),
        lease_expires_at = now() + make_interval(secs => ${leaseSeconds}), updated_at = now()
    WHERE id = (
      SELECT id FROM sync_jobs
      WHERE status = ${JOB_STATUS.UNKNOWN} AND direction = 'OUTBOUND' AND next_run_at <= now()
        AND create_sent_at <= now() - make_interval(secs => ${graceSeconds})
        AND (lease_expires_at IS NULL OR lease_expires_at < now()) ${onlyEntityKeys(entityKeys)}
      ORDER BY create_sent_at
      LIMIT 1
      FOR UPDATE SKIP LOCKED
    )
    RETURNING *
  `);
};

// Reconciliation confirmed QuickBooks doesn't have the invoice: send the create again
export const requeueUnknown = async (job: SyncJob, message: string) => {
  const pool = await getPool();
  await pool.query(sql.unsafe`
    UPDATE sync_jobs
    SET status = ${JOB_STATUS.PENDING}, create_sent_at = NULL, claim_token = NULL, lease_expires_at = NULL,
        next_run_at = now(), last_error = ${message}, last_error_class = 'reconciled', updated_at = now()
    WHERE ${owned(job)}
  `);
};

// Reconciliation couldn't reach QuickBooks: stay UNKNOWN and try again later
export const releaseUnknown = async (job: SyncJob, message: string, delaySeconds: number) => {
  const pool = await getPool();
  await pool.query(sql.unsafe`
    UPDATE sync_jobs
    SET claim_token = NULL, lease_expires_at = NULL, next_run_at = now() + make_interval(secs => ${delaySeconds}),
        last_error = ${message}, updated_at = now()
    WHERE ${owned(job)}
  `);
};

// ---------------------------------------------------------------------------
// Operations: inspection, manual retry, metrics
// ---------------------------------------------------------------------------

// A job with the invoice it's about (local id) and the QuickBooks id of its entity. entity_id is the
// local id for OUTBOUND jobs but the QuickBooks id for INBOUND ones, which is easy to misread.
// Payment jobs show the invoice the payment belongs to.
const SyncJobView = SyncJob.extend({
  local_invoice_id: z.number().nullable(),
  quickbooks_id: z.string().nullable(),
});

export const listJobs = async ({ status, invoiceId, limit }: { status?: JobStatus; invoiceId?: number; limit: number }) => {
  const pool = await getPool();
  const conditions = [
    ...(status ? [sql.fragment`j.status = ${status}`] : []),
    ...(invoiceId ? [sql.fragment`coalesce(i.id, p.invoice_id) = ${invoiceId}`] : []),
  ];
  const filter = conditions.length ? sql.fragment`WHERE ${sql.join(conditions, sql.fragment` AND `)}` : sql.fragment``;
  return pool.any(sql.type(SyncJobView)`
    SELECT j.*,
           coalesce(i.id, p.invoice_id) AS local_invoice_id,
           CASE WHEN j.direction = 'INBOUND' THEN j.entity_id ELSE coalesce(i.quickbooks_id, p.quickbooks_id) END AS quickbooks_id
    FROM sync_jobs j
    LEFT JOIN invoices i ON j.entity_type = 'invoice' AND (
      (j.direction = 'OUTBOUND' AND i.id::text = j.entity_id)
      OR (j.direction = 'INBOUND' AND i.quickbooks_realm_id = j.realm_id AND i.quickbooks_id = j.entity_id)
    )
    -- A QuickBooks payment of several invoices has one row per invoice: one is shown (the filtered
    -- invoice's, if any), so each job is listed once
    LEFT JOIN LATERAL (
      SELECT p.invoice_id, p.quickbooks_id
      FROM invoice_payments p JOIN invoices pi ON pi.id = p.invoice_id
      WHERE j.entity_type = 'payment' AND (
        (j.direction = 'OUTBOUND' AND p.id::text = j.entity_id)
        OR (j.direction = 'INBOUND' AND p.quickbooks_id = j.entity_id AND pi.quickbooks_realm_id = j.realm_id)
      ) ${invoiceId ? sql.fragment`AND p.invoice_id = ${invoiceId}` : sql.fragment``}
      ORDER BY p.invoice_id
      LIMIT 1
    ) p ON true
    ${filter}
    ORDER BY j.created_at DESC
    LIMIT ${limit}
  `);
};

export const listEvents = async ({ limit }: { limit: number }) => {
  const pool = await getPool();
  return pool.any(sql.type(z.record(z.unknown()))`SELECT * FROM sync_events ORDER BY id DESC LIMIT ${limit}`);
};

// Manually retry a FAILED or UNKNOWN job (e.g. after fixing the data or checking QuickBooks).
// A create is sent again with the same requestid, so QuickBooks won't duplicate it.
export const retryJobManually = async (id: string) => {
  const pool = await getPool();
  return pool.transaction(async (tx) => {
    const job = await tx.maybeOne(sql.type(SyncJob)`
      UPDATE sync_jobs
      SET status = ${JOB_STATUS.PENDING}, attempts = 0, next_run_at = now(), create_sent_at = NULL,
          claim_token = NULL, lease_expires_at = NULL, last_error = NULL, last_error_class = NULL, updated_at = now()
      WHERE id = ${id} AND status IN (${JOB_STATUS.FAILED}, ${JOB_STATUS.UNKNOWN})
      RETURNING *
    `);
    if (job?.direction === 'OUTBOUND' && job.entity_type === 'invoice') {
      await tx.query(sql.unsafe`
        UPDATE invoices SET sync_status = ${SYNC_STATUS.PENDING}, sync_error = NULL
        WHERE id = ${Number(job.entity_id)} AND sync_status IN (${SYNC_STATUS.FAILED}, ${SYNC_STATUS.UNKNOWN})
      `);
    }
    if (job?.direction === 'OUTBOUND' && job.entity_type === 'payment') {
      await tx.query(sql.unsafe`
        UPDATE invoice_payments SET sync_status = ${SYNC_STATUS.PENDING}, sync_error = NULL WHERE id = ${Number(job.entity_id)}
      `);
    }
    if (job) await tx.query(sql.unsafe`SELECT pg_notify('sync_jobs', '')`);
    return job;
  });
};

// After connecting to QuickBooks: jobs that were waiting for a connection run now
export const wakeJobsWaitingForConnection = async () => {
  const pool = await getPool();
  await pool.query(sql.unsafe`
    UPDATE sync_jobs SET next_run_at = now()
    WHERE status = ${JOB_STATUS.PENDING} AND last_error_class IN ('not_connected', 'auth')
  `);
  await pool.query(sql.unsafe`SELECT pg_notify('sync_jobs', '')`);
};

export const getStats = async () => {
  const pool = await getPool();
  return pool.one(sql.type(
    z.object({
      pending: z.number(),
      processing: z.number(),
      completed: z.number(),
      failed: z.number(),
      unknown: z.number(),
      retried_jobs: z.number(),
      total_retries: z.number(),
      oldest_pending_seconds: z.number().nullable(),
      avg_duration_ms_last_hour: z.number().nullable(),
    }),
  )`
    SELECT
      count(*) FILTER (WHERE status = ${JOB_STATUS.PENDING}) AS pending,
      count(*) FILTER (WHERE status = ${JOB_STATUS.PROCESSING}) AS processing,
      count(*) FILTER (WHERE status = ${JOB_STATUS.COMPLETED}) AS completed,
      count(*) FILTER (WHERE status = ${JOB_STATUS.FAILED}) AS failed,
      count(*) FILTER (WHERE status = ${JOB_STATUS.UNKNOWN}) AS unknown,
      count(*) FILTER (WHERE attempts > 1) AS retried_jobs,
      coalesce(sum(greatest(attempts - 1, 0)), 0)::int AS total_retries,
      extract(epoch FROM now() - min(created_at) FILTER (WHERE status = ${JOB_STATUS.PENDING}))::int AS oldest_pending_seconds,
      round(avg(duration_ms) FILTER (WHERE status = ${JOB_STATUS.COMPLETED} AND completed_at > now() - interval '1 hour'))::int
        AS avg_duration_ms_last_hour
    FROM sync_jobs
  `);
};
