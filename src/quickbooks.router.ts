import { randomUUID } from 'node:crypto';
import { Router, type ErrorRequestHandler, type Response } from 'express';
import { z } from 'zod';
import OAuthClient from 'intuit-oauth';
import {
  getConnectedRealmId,
  createOAuthClient,
  NotConnectedError,
  qboQuery,
  qboRequest,
  saveConnection,
} from './quickbooks.client';
import { findDuplicates } from './quickbooks.duplicates';
import { wakeJobsWaitingForConnection } from './sync.repository';

// OAuth flow in progress: `state` is a random value sent to Intuit and checked on the callback
// (CSRF protection); `returnTo` is where to send the user once connected.
let pendingAuth: { state: string; returnTo?: string } | undefined;

const router = Router();

// Step 1: send the user to Intuit to log in and authorize the app
const redirectToIntuit = (res: Response, returnTo?: string) => {
  const state = randomUUID();
  pendingAuth = { state, returnTo };
  res.redirect(createOAuthClient().authorizeUri({ scope: [OAuthClient.scopes.Accounting], state }));
};

router.get('/quickbooks/connect', (req, res) => {
  redirectToIntuit(res);
});

// Step 2: Intuit redirects back here with an authorization code, which we exchange for tokens
router.get('/callback', async (req, res) => {
  if (req.query.error) {
    res.status(400).json({ error: `QuickBooks authorization failed: ${req.query.error}` });
    return;
  }
  if (!pendingAuth || req.query.state !== pendingAuth.state) {
    res.status(400).json({ error: 'Invalid OAuth state. Start again from /quickbooks/connect.' });
    return;
  }
  const { returnTo } = pendingAuth;
  pendingAuth = undefined;

  const oauthClient = createOAuthClient();
  await oauthClient.createToken(req.originalUrl);
  const token = oauthClient.getToken();
  await saveConnection(token.realmId!, token);
  // Jobs that were waiting for a connection run now
  await wakeJobsWaitingForConnection();

  if (returnTo) {
    res.redirect(returnTo);
    return;
  }
  res.json({ status: 'connected', realmId: token.realmId });
});

// Whether a QuickBooks company is connected, and which one
router.get('/quickbooks/status', async (req, res) => {
  const realmId = await getConnectedRealmId();
  res.json({ connected: realmId !== null, realm_id: realmId });
});

const Pagination = z.object({
  limit: z.coerce.number().int().min(1).max(100).default(20),
  offset: z.coerce.number().int().min(0).default(0),
});

// Step 3: read data from the connected company
router.get('/quickbooks/accounts', async (req, res) => {
  const data = await qboQuery('SELECT * FROM Account');
  res.json(data.QueryResponse.Account ?? []);
});

router.get('/quickbooks/invoices', async (req, res) => {
  const { limit, offset } = Pagination.parse(req.query);
  // Safe to interpolate: both are validated integers. STARTPOSITION is 1-based.
  const data = await qboQuery(
    `SELECT * FROM Invoice ORDERBY MetaData.CreateTime DESC STARTPOSITION ${offset + 1} MAXRESULTS ${limit}`,
  );
  res.json(data.QueryResponse.Invoice ?? []);
});

router.get('/quickbooks/company-info', async (req, res) => {
  const data = await qboRequest((realmId) => `companyinfo/${realmId}`);
  res.json(data.CompanyInfo);
});

// Reports duplicates: several QuickBooks invoices created from the same local invoice, and local
// invoices that look alike (same customer, amount and due date)
router.get('/quickbooks/duplicates', async (req, res) => {
  res.json(await findDuplicates());
});

// Browsers are sent through the OAuth flow and back to the page they asked for; API clients get a 401.
// QuickBooks validation errors are returned as 400 with its message.
const quickbooksErrorHandler: ErrorRequestHandler = (err, req, res, next) => {
  if (err instanceof NotConnectedError) {
    if (req.method === 'GET' && req.accepts(['json', 'html']) === 'html') {
      redirectToIntuit(res, req.originalUrl);
      return;
    }
    res.status(401).json({ error: err.message });
    return;
  }
  if (err.fault) {
    res.status(400).json({ error: 'QuickBooks rejected the request', details: err.fault.errors });
    return;
  }
  next(err);
};
router.use(quickbooksErrorHandler);

export default router;
