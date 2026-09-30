import { Router, type Request, type Response } from 'express';
import { z } from 'zod';
import * as invoices from './invoices.repository';
import * as payments from './payments.repository';
import { STATUSES, CreateInvoiceInput, UpdateInvoiceInput, touchesContent } from './invoices.schema';

const router = Router();

const IdParam = z.coerce.number().int().positive().max(2147483647); // fits in SERIAL

const ListQuery = z.object({
  status: z.enum(STATUSES).optional(),
  limit: z.coerce.number().int().min(1).max(100).default(20),
  offset: z.coerce.number().int().min(0).default(0),
});

// Express 5 forwards rejected promises (including ZodErrors) to the error handler.
router.get('/', async (req, res) => {
  const query = ListQuery.parse(req.query);
  res.json(await invoices.list(query));
});

router.get('/:id', async (req, res) => {
  const invoice = await invoices.findById(IdParam.parse(req.params.id));
  if (!invoice) {
    res.status(404).json({ error: 'Invoice not found' });
    return;
  }
  res.json(invoice);
});

const IdempotencyKey = z.string().max(255);

// Required Idempotency-Key header on every POST that creates something: sending the same key again
// returns what was created the first time (200) instead of a duplicate, so clients can safely retry.
// Answers 400 and returns undefined when it's missing.
const requireIdempotencyKey = (req: Request, res: Response) => {
  const key = req.get('Idempotency-Key');
  if (!key) {
    res.status(400).json({ error: 'The Idempotency-Key header is required (e.g. a new UUID per request; reuse it when retrying)' });
    return undefined;
  }
  return IdempotencyKey.parse(key);
};

router.post('/', async (req, res) => {
  const idempotencyKey = requireIdempotencyKey(req, res);
  if (!idempotencyKey) return;
  const input = CreateInvoiceInput.parse(req.body);
  const { invoice, created } = await invoices.create(input, idempotencyKey);
  res.status(created ? 201 : 200).json(invoice);
});

router.patch('/:id', async (req, res) => {
  const id = IdParam.parse(req.params.id);
  const changes = UpdateInvoiceInput.parse(req.body);

  const invoice = await invoices.update(id, changes);
  if (invoice) {
    res.json(invoice);
    return;
  }
  // Not updated: find out why
  const current = await invoices.findById(id);
  if (!current) res.status(404).json({ error: 'Invoice not found' });
  // QuickBooks doesn't allow editing a voided invoice, so its local copy can't change either
  else if (current.status === 'void') res.status(409).json({ error: 'The invoice is voided: it cannot be changed (it can only be deleted)' });
  else if (current.taxed_in_quickbooks && touchesContent(changes)) {
    res.status(409).json({ error: 'The invoice has sales tax in QuickBooks: change its amount, customer or due date there' });
  }
  // A paid invoice has a payment in QuickBooks: un-paying it means deleting that payment there
  else res.status(409).json({ error: 'The invoice is paid: delete its payments (DELETE /invoices/:id/payments/:paymentId)' });
});

router.delete('/:id', async (req, res) => {
  const deleted = await invoices.remove(IdParam.parse(req.params.id));
  if (!deleted) {
    res.status(404).json({ error: 'Invoice not found' });
    return;
  }
  res.status(204).end();
});

// Payments recorded here (full or partial) are pushed to QuickBooks; the invoice's balance and paid
// status then come back from it. Idempotency-Key header required, like POST /invoices.
router.get('/:id/payments', async (req, res) => {
  res.json(await payments.listForInvoice(IdParam.parse(req.params.id)));
});

router.post('/:id/payments', async (req, res) => {
  const invoiceId = IdParam.parse(req.params.id);
  const idempotencyKey = requireIdempotencyKey(req, res);
  if (!idempotencyKey) return;
  const input = payments.CreatePaymentInput.parse(req.body);

  const result = await payments.create(invoiceId, input, idempotencyKey);
  if ('payment' in result) {
    res.status(result.outcome === 'created' ? 201 : 200).json(result.payment);
  } else if (result.outcome === 'invoice_not_found') {
    res.status(404).json({ error: 'Invoice not found' });
  } else if (result.outcome === 'invoice_void') {
    res.status(409).json({ error: 'The invoice is void' });
  } else if (result.outcome === 'key_reused') {
    res.status(409).json({ error: 'This Idempotency-Key was already used for a payment of another invoice' });
  } else {
    res.status(422).json({ error: `The payment exceeds what is left to pay (${result.available})` });
  }
});

// Deletes the payment here and in QuickBooks (the invoice's balance then comes back from QuickBooks)
router.delete('/:id/payments/:paymentId', async (req, res) => {
  const outcome = await payments.remove(IdParam.parse(req.params.id), IdParam.parse(req.params.paymentId));
  if (outcome === 'not_found') {
    res.status(404).json({ error: 'Payment not found' });
  } else if (outcome === 'shared') {
    res.status(409).json({ error: 'This QuickBooks payment also pays other invoices: delete it in QuickBooks' });
  } else {
    res.status(204).end();
  }
});

export default router;
