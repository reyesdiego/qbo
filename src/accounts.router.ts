import { Router } from 'express';
import * as accounts from './accounts.repository';

const router = Router();

// Chart of accounts from the local copy (kept in sync with QuickBooks by webhooks and the worker).
// GET /quickbooks/accounts reads the same data live from QuickBooks.
router.get('/', async (req, res) => {
  res.json(await accounts.list());
});

export default router;
