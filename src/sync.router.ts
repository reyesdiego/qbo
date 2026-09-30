// Operations endpoints: inspect the sync queue, retry failed jobs, metrics, resolve conflicts
import { Router } from 'express';
import { z } from 'zod';
import { NotConnectedError } from './errors';
import { qbo } from './quickbooks.client';
import { resolveConflict } from './sync.conflicts';
import { checkConsistency } from './sync.consistency';
import { getStats, JOB_STATUSES, listEvents, listJobs, retryJobManually } from './sync.repository';

const router = Router();

const Limit = z.coerce.number().int().min(1).max(200).default(50);

// e.g. GET /sync/jobs?status=FAILED or /sync/jobs?invoice_id=75 (a local invoice id, in both directions)
router.get('/sync/jobs', async (req, res) => {
  const { status, invoice_id, limit } = z
    .object({
      status: z.enum(JOB_STATUSES).optional(),
      invoice_id: z.coerce.number().int().positive().max(2147483647).optional(),
      limit: Limit,
    })
    .parse(req.query);
  res.json(await listJobs({ status, invoiceId: invoice_id, limit }));
});

// Retry a FAILED or UNKNOWN job (e.g. after fixing the data or checking QuickBooks)
router.post('/sync/jobs/:id/retry', async (req, res) => {
  const job = await retryJobManually(z.string().uuid().parse(req.params.id));
  if (!job) {
    res.status(409).json({ error: 'Job not found, or not FAILED/UNKNOWN' });
    return;
  }
  res.json(job);
});

// Pending, failed and unknown jobs, retries, oldest pending job, average processing time
router.get('/sync/stats', async (req, res) => {
  res.json(await getStats());
});

// Notifications received from QuickBooks (webhooks and reconciliation), newest first
router.get('/sync/events', async (req, res) => {
  res.json(await listEvents({ limit: Limit.parse(req.query.limit) }));
});

// Compares every QuickBooks invoice with the local ones and queues the repairs (?repair=false: report only)
router.post('/sync/consistency-check', async (req, res) => {
  const repair = req.query.repair !== 'false';
  try {
    res.json(await checkConsistency(qbo, { repair }));
  } catch (err) {
    if (!(err instanceof NotConnectedError)) throw err;
    res.status(401).json({ error: err.message });
  }
});

router.post('/sync/conflicts/:invoiceId/resolve', async (req, res) => {
  const invoiceId = z.coerce.number().int().positive().max(2147483647).parse(req.params.invoiceId);
  const { keep } = z.object({ keep: z.enum(['local', 'remote']) }).parse(req.body);

  const outcome = await resolveConflict(invoiceId, keep);
  if (outcome === 'not_found') {
    res.status(404).json({ error: 'Invoice not found' });
  } else if (outcome === 'no_conflict') {
    res.status(409).json({ error: 'The invoice has no conflict to resolve' });
  } else {
    res.json({ status: 'resolved', keep });
  }
});

export default router;
