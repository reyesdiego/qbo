import express, { type ErrorRequestHandler } from 'express';
import { ZodError } from 'zod';
import accountsRouter from './accounts.router';
import invoicesRouter from './invoices.router';
import quickbooksRouter from './quickbooks.router';
import quickbooksWebhooksRouter from './quickbooks.webhooks';
import syncRouter from './sync.router';

const app = express();

// Before express.json(): verifying the webhook signature needs the raw request body
app.use(quickbooksWebhooksRouter);
app.use(express.json());

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.use('/invoices', invoicesRouter);
app.use('/accounts', accountsRouter);
app.use(quickbooksRouter);
app.use(syncRouter);

app.use((req, res) => {
  res.status(404).json({ error: 'Not found' });
});

const errorHandler: ErrorRequestHandler = (err, req, res, _next) => {
  if (err instanceof ZodError) {
    res.status(400).json({ error: 'Validation failed', details: err.issues });
    return;
  }
  if (err.type === 'entity.parse.failed') {
    res.status(400).json({ error: 'Invalid JSON body' });
    return;
  }

  console.error(err);
  res.status(500).json({ error: 'Internal server error' });
};

app.use(errorHandler);

export default app;
