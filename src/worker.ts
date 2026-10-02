// Sync worker: runs as its own process (`npm run worker`); several can run at once.
//
// - Processes queued sync jobs as soon as they're created: the API sends NOTIFY in the transaction
//   that queues a job, and the worker LISTENs. Polling every SYNC_POLL_INTERVAL_SECONDS is the safety
//   net for notifications missed while the worker was down or disconnected.
// - Recovers jobs whose worker died (expired leases).
// - Every SYNC_RECONCILE_INTERVAL_SECONDS: queues QuickBooks changes that webhooks missed, and
//   resolves UNKNOWN creates.
// - Every SYNC_CONSISTENCY_INTERVAL_HOURS (and at start): full consistency check with QuickBooks.
// - On SIGINT/SIGTERM it stops claiming, finishes the job in progress, and exits.
import { hostname } from 'node:os';
import { setTimeout as sleep } from 'node:timers/promises';
import pg from 'pg';
import { DATABASE_URL, getPool } from './db';
import { errorMessage, NotConnectedError } from './errors';
import { setSyncState } from './invoices.repository';
import { log } from './logger';
import { qbo } from './quickbooks.client';
import { JOB_STATUS, SYNC_STATUS } from './statuses';
import { setPaymentSyncState } from './sync.payments';
import { processJob } from './sync.processor';
import { checkConsistency } from './sync.consistency';
import { enqueueRemoteChanges, reconcileUnknownJobs } from './sync.reconcile';
import { claimNextJob, recoverExpiredLeases } from './sync.repository';

const POLL_MS = Number(process.env.SYNC_POLL_INTERVAL_SECONDS || 5) * 1000;
const RECONCILE_MS = Number(process.env.SYNC_RECONCILE_INTERVAL_SECONDS || 300) * 1000;
const LEASE_SECONDS = Number(process.env.SYNC_JOB_LEASE_SECONDS || 300);
const UNKNOWN_GRACE_SECONDS = Number(process.env.SYNC_UNKNOWN_GRACE_SECONDS || 120);
const CONSISTENCY_MS = Number(process.env.SYNC_CONSISTENCY_INTERVAL_HOURS || 24) * 60 * 60 * 1000;
const workerId = `${hostname()}:${process.pid}`;

const shutdown = new AbortController();
const stop = () => {
  if (shutdown.signal.aborted) process.exit(1); // second Ctrl+C: don't wait
  log.info('sync.worker.stopping', { worker_id: workerId });
  shutdown.abort();
};
process.on('SIGINT', stop);
process.on('SIGTERM', stop);

// Sleeps until the next poll, a NOTIFY, or shutdown. A notification that arrives while we're busy
// isn't lost: the next wait returns right away.
let wake: (() => void) | null = null;
let notifiedWhileBusy = false;

const onNotification = () => {
  if (wake) wake();
  else notifiedWhileBusy = true;
};

const waitForWork = (ms: number) =>
  new Promise<void>((resolve) => {
    if (notifiedWhileBusy || shutdown.signal.aborted) {
      notifiedWhileBusy = false;
      return resolve();
    }
    const done = () => {
      clearTimeout(timer);
      wake = null;
      shutdown.signal.removeEventListener('abort', done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    wake = done;
    shutdown.signal.addEventListener('abort', done);
  });

// LISTEN needs its own long-lived connection (not one from the pool). If it drops, reconnect and
// check for jobs right away, since notifications sent meanwhile were lost.
const listenForJobs = async () => {
  while (!shutdown.signal.aborted) {
    const client = new pg.Client({ connectionString: DATABASE_URL });
    try {
      await client.connect();
      client.on('notification', onNotification);
      await client.query('LISTEN sync_jobs');
      log.info('sync.worker.listening', { worker_id: workerId });
      await new Promise<void>((resolve, reject) => {
        client.on('error', reject);
        client.on('end', () => resolve());
        shutdown.signal.addEventListener('abort', () => resolve(), { once: true });
      });
    } catch (err) {
      log.warn('sync.worker.listen_failed', { worker_id: workerId, error: errorMessage(err) });
    } finally {
      await client.end().catch(() => {});
    }
    onNotification();
    await sleep(5000, undefined, { signal: shutdown.signal }).catch(() => {});
  }
};

const recoverLeases = async () => {
  for (const job of await recoverExpiredLeases()) {
    log.warn('sync.job.lease_expired', { job_id: job.id, direction: job.direction, entity_id: job.entity_id, new_status: job.status });
    if (job.direction === 'OUTBOUND' && job.status !== JOB_STATUS.PENDING) {
      const state = job.status === JOB_STATUS.UNKNOWN ? SYNC_STATUS.UNKNOWN : SYNC_STATUS.FAILED;
      if (job.entity_type === 'payment') await setPaymentSyncState(Number(job.entity_id), state, 'Worker stopped while syncing');
      else await setSyncState(Number(job.entity_id), state, 'Worker stopped while syncing');
    }
  }
};

let waitingForConnection = false;

const reconcile = async () => {
  try {
    const enqueued = await enqueueRemoteChanges(qbo);
    const unknownChecked = await reconcileUnknownJobs(qbo, {
      workerId,
      leaseSeconds: LEASE_SECONDS,
      graceSeconds: UNKNOWN_GRACE_SECONDS,
    });
    log.info('sync.reconcile.done', { worker_id: workerId, enqueued, unknown_checked: unknownChecked });
    waitingForConnection = false;
  } catch (err) {
    if (!(err instanceof NotConnectedError)) {
      log.error('sync.reconcile.failed', { worker_id: workerId, error: errorMessage(err) });
    } else if (!waitingForConnection) {
      log.info('sync.worker.waiting_for_connection', { hint: 'visit /quickbooks/connect' });
      waitingForConnection = true;
    }
  }
};

const consistencyCheck = async () => {
  try {
    const report = await checkConsistency(qbo);
    log.info('sync.consistency.done', {
      worker_id: workerId,
      quickbooks_invoices: report.quickbooks_invoices,
      local_linked_invoices: report.local_linked_invoices,
      missing_locally: report.missing_locally.length,
      missing_in_quickbooks: report.missing_in_quickbooks.length,
      jobs_enqueued: report.jobs_enqueued,
    });
  } catch (err) {
    if (!(err instanceof NotConnectedError)) log.error('sync.consistency.failed', { worker_id: workerId, error: errorMessage(err) });
  }
};

const main = async () => {
  log.info('sync.worker.started', {
    worker_id: workerId,
    poll_seconds: POLL_MS / 1000,
    reconcile_seconds: RECONCILE_MS / 1000,
    lease_seconds: LEASE_SECONDS,
  });
  void listenForJobs();
  let lastReconcileAt = 0;
  let lastConsistencyAt = 0;

  while (!shutdown.signal.aborted) {
    try {
      await recoverLeases();
      if (Date.now() - lastReconcileAt >= RECONCILE_MS) {
        lastReconcileAt = Date.now();
        await reconcile();
      }
      if (Date.now() - lastConsistencyAt >= CONSISTENCY_MS) {
        lastConsistencyAt = Date.now();
        await consistencyCheck();
      }
      // Process everything that's ready, one job at a time
      while (!shutdown.signal.aborted) {
        const job = await claimNextJob({ workerId, leaseSeconds: LEASE_SECONDS });
        if (!job) break;
        await processJob(job);
      }
    } catch (err) {
      log.error('sync.worker.loop_failed', { worker_id: workerId, error: errorMessage(err) });
    }
    await waitForWork(POLL_MS);
  }

  await (await getPool()).end();
  log.info('sync.worker.stopped', { worker_id: workerId });
};

main();
