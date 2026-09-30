// The PostgreSQL job queue: claiming, per-entity ordering, retries, leases
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { sql } from 'slonik';
import { getPool } from '../src/db';
import { LeaseLostError } from '../src/errors';
import { retryDelaySeconds } from '../src/sync.processor';
import {
  claimNextJob,
  completeJob,
  enqueueJob,
  getStats,
  markCreateSent,
  recoverExpiredLeases,
  rescheduleJob,
} from '../src/sync.repository';
import { jobsFor, uniqueName } from './helpers';

after(async () => {
  await (await getPool()).end();
});

const enqueue = async (entityKey: string) => {
  const pool = await getPool();
  return enqueueJob(pool, { direction: 'OUTBOUND', entityType: 'invoice', entityId: '0', entityKey, operation: 'upsert' });
};

const claim = (entityKeys: string[], workerId = 'worker-a') => claimNextJob({ workerId, leaseSeconds: 60, entityKeys });

const expireLease = async (jobId: string) => {
  const pool = await getPool();
  await pool.query(sql.unsafe`UPDATE sync_jobs SET lease_expires_at = now() - interval '1 second' WHERE id = ${jobId}`);
};

test('concurrent workers never claim the same job', async () => {
  const keys = Array.from({ length: 5 }, () => uniqueName('entity'));
  for (const key of keys) await enqueue(key);

  // 8 workers race for 5 jobs
  const claimed = await Promise.all(Array.from({ length: 8 }, (_, i) => claim(keys, `worker-${i}`)));
  const ids = claimed.filter((job) => job !== null).map((job) => job!.id);
  assert.equal(ids.length, 5);
  assert.equal(new Set(ids).size, 5);
});

test('jobs of the same entity run one at a time, oldest first', async () => {
  const key = uniqueName('invoice');
  const firstId = await enqueue(key);
  const secondId = await enqueue(key);

  // Two workers at once: only one gets a job, and it's the oldest
  const [a, b] = await Promise.all([claim([key], 'worker-a'), claim([key], 'worker-b')]);
  const running = a ?? b;
  assert.equal(running?.id, firstId);
  assert.equal(a && b, null);

  // Nothing else for this entity while the first runs
  assert.equal(await claim([key]), null);

  const pool = await getPool();
  await completeJob(pool, running!, 'done');
  assert.equal((await claim([key]))?.id, secondId);
});

test('retry scheduling', async (t) => {
  await t.test('backoff grows exponentially, with jitter between half and all of it', () => {
    for (let attempt = 1; attempt <= 5; attempt++) {
      const exponential = 10 * 2 ** (attempt - 1);
      const delay = retryDelaySeconds(attempt);
      assert.ok(delay >= exponential / 2 && delay <= exponential, `attempt ${attempt}: ${delay}s`);
    }
    assert.ok(retryDelaySeconds(20) <= 15 * 60); // capped
  });

  await t.test('a retryable failure goes back to PENDING, later', async () => {
    const key = uniqueName('invoice');
    await enqueue(key);
    const job = (await claim([key]))!;
    const status = await rescheduleJob(job, { errorClass: 'network', message: 'reset', delaySeconds: 30, countAttempt: true });
    assert.equal(status, 'PENDING');

    const [stored] = await jobsFor(key);
    assert.equal(stored.attempts, 1);
    assert.equal(stored.last_error_class, 'network');
    assert.ok(Date.parse(stored.next_run_at) > Date.now() + 25_000);
    assert.equal(await claim([key]), null); // not runnable before next_run_at
  });

  await t.test('after max_attempts the job is FAILED', async () => {
    const key = uniqueName('invoice');
    await enqueue(key);
    const pool = await getPool();
    await pool.query(sql.unsafe`UPDATE sync_jobs SET max_attempts = 1 WHERE entity_key = ${key}`);
    const job = (await claim([key]))!;
    assert.equal(await rescheduleJob(job, { errorClass: 'server', message: '503', delaySeconds: 1, countAttempt: true }), 'FAILED');
  });

  await t.test('waiting (e.g. for a QuickBooks connection) does not use up attempts', async () => {
    const key = uniqueName('invoice');
    await enqueue(key);
    const job = (await claim([key]))!;
    await rescheduleJob(job, { errorClass: 'not_connected', message: 'not connected', delaySeconds: 60, countAttempt: false });
    assert.equal((await jobsFor(key))[0].attempts, 0);
  });
});

test('recovering jobs of workers that died', async (t) => {
  await t.test('an expired lease puts the job back in the queue', async () => {
    const key = uniqueName('invoice');
    await enqueue(key);
    const job = (await claim([key]))!;
    await expireLease(job.id);

    const recovered = await recoverExpiredLeases();
    assert.equal(recovered.find((r) => r.id === job.id)?.status, 'PENDING');
    assert.equal((await claim([key]))?.id, job.id);
  });

  await t.test('if a create may have been sent, it becomes UNKNOWN instead of being retried', async () => {
    const key = uniqueName('invoice');
    await enqueue(key);
    const job = (await claim([key]))!;
    await markCreateSent(job, { reference: 'local-invoice-1-1' });
    await expireLease(job.id);

    const recovered = await recoverExpiredLeases();
    assert.equal(recovered.find((r) => r.id === job.id)?.status, 'UNKNOWN');
    assert.equal(await claim([key]), null); // UNKNOWN blocks the entity until reconciled
  });

  await t.test('the old worker can no longer finish a job another worker took over', async () => {
    const key = uniqueName('invoice');
    await enqueue(key);
    const stale = (await claim([key], 'worker-a'))!;
    await expireLease(stale.id);
    await recoverExpiredLeases();
    const current = (await claim([key], 'worker-b'))!;
    assert.equal(current.id, stale.id);

    const pool = await getPool();
    await assert.rejects(completeJob(pool, stale, 'late'), LeaseLostError);
    const [stored] = await jobsFor(key);
    assert.equal(stored.status, 'PROCESSING');
    assert.equal(stored.locked_by, 'worker-b');
  });
});

test('stats', async () => {
  const stats = await getStats();
  for (const field of ['pending', 'processing', 'completed', 'failed', 'unknown', 'retried_jobs', 'total_retries'] as const) {
    assert.equal(typeof stats[field], 'number', field);
  }
});
