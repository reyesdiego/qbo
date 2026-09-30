// QuickBooks webhooks: Intuit calls POST /quickbooks/webhooks when invoices, payments or accounts
// change. The handler only verifies, records and queues: each notification is stored in sync_events
// together with its sync job, in one transaction, and we answer 200 only after it commits. If saving
// fails we answer 500 and Intuit redelivers. The worker then fetches the current entity and applies it.
import { createHmac, randomUUID, timingSafeEqual } from 'node:crypto';
import express, { Router } from 'express';
import { z } from 'zod';
import { getPool } from './db';
import { log } from './logger';
import { getConnectedRealmId } from './quickbooks.client';
import { isSyncedEntity, recordEventAndEnqueue } from './sync.repository';

// Intuit signs each notification: base64(HMAC-SHA256(raw body, verifier token)) in the
// intuit-signature header. The verifier token is on the app's Webhooks page in the Intuit portal.
export const isValidSignature = (rawBody: Buffer, signature: string | undefined, verifierToken: string) => {
  if (!signature) return false;
  const expected = createHmac('sha256', verifierToken).update(rawBody).digest();
  const received = Buffer.from(signature, 'base64');
  return received.length === expected.length && timingSafeEqual(received, expected);
};

const LegacyPayload = z.object({
  eventNotifications: z.array(
    z.object({
      realmId: z.string(),
      dataChangeEvent: z.object({
        entities: z.array(
          z.object({ name: z.string(), id: z.string(), operation: z.string(), lastUpdated: z.string().optional() }),
        ),
      }),
    }),
  ),
});

const CloudEvents = z.array(
  z
    .object({
      id: z.string(),
      type: z.string().regex(/^qbo\.\w+\.\w+/), // e.g. "qbo.invoice.updated.v1"
      intuitentityid: z.string(),
      intuitaccountid: z.string(), // the company (realmId)
    })
    .passthrough(),
);

const CLOUD_EVENT_OPERATIONS: Record<string, string> = {
  created: 'create',
  updated: 'update',
  deleted: 'delete',
  voided: 'void',
  merged: 'merge',
  emailed: 'emailed',
};

export type WebhookEvent = {
  realmId: string;
  entity: string; // lowercase: 'invoice', 'payment', 'account', ...
  id: string;
  operation: string; // 'create', 'update', 'delete', 'void', ...
  eventKey: string; // identifies this notification, for deduplication
  payload: unknown;
};

// Intuit sends either its original format ({ eventNotifications: [...] }) or CloudEvents (an array).
// One notification can carry several entities.
export const parseWebhookPayload = (payload: unknown): WebhookEvent[] => {
  if (Array.isArray(payload)) {
    return CloudEvents.parse(payload).map((event) => {
      const [, entity = '', operation = ''] = event.type.split('.');
      return {
        realmId: event.intuitaccountid,
        entity: entity.toLowerCase(),
        id: event.intuitentityid,
        operation: CLOUD_EVENT_OPERATIONS[operation] ?? operation,
        eventKey: `cloudevent:${event.id}`,
        payload: event,
      };
    });
  }

  return LegacyPayload.parse(payload).eventNotifications.flatMap((notification) =>
    notification.dataChangeEvent.entities.map((entity) => ({
      realmId: notification.realmId,
      entity: entity.name.toLowerCase(),
      id: entity.id,
      operation: entity.operation.toLowerCase(),
      // The original format has no event id. Two notifications for the same entity are only
      // duplicates if they report the same change (same operation and lastUpdated).
      eventKey: `webhook:${notification.realmId}:${entity.name}:${entity.id}:${entity.operation}:${entity.lastUpdated ?? randomUUID()}`,
      payload: entity,
    })),
  );
};

const router = Router();

// Raw body parser: the signature is computed over the exact bytes Intuit sent
router.post('/quickbooks/webhooks', express.raw({ type: '*/*' }), async (req, res) => {
  log.info('sync.webhook.received', { has_signature: Boolean(req.get('intuit-signature')), bytes: req.body?.length ?? 0 });

  const verifierToken = process.env.QBO_WEBHOOK_VERIFIER_TOKEN;
  if (!verifierToken) {
    res.status(503).json({ error: 'Webhooks are not configured: set QBO_WEBHOOK_VERIFIER_TOKEN' });
    return;
  }

  const rawBody = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0);
  if (!isValidSignature(rawBody, req.get('intuit-signature'), verifierToken)) {
    // Usually QBO_WEBHOOK_VERIFIER_TOKEN doesn't match the token on the Intuit webhooks page
    log.warn('sync.webhook.rejected', { reason: 'invalid signature (check QBO_WEBHOOK_VERIFIER_TOKEN)' });
    res.status(401).json({ error: 'Invalid signature' });
    return;
  }

  let events: WebhookEvent[];
  try {
    events = parseWebhookPayload(JSON.parse(rawBody.toString('utf8')));
  } catch {
    res.status(400).json({ error: 'Invalid webhook payload' });
    return;
  }

  const connectedRealmId = await getConnectedRealmId();
  const pool = await getPool();
  // All events of the notification and their jobs in one transaction; an error answers 500 (Intuit retries)
  const outcomes = await pool.transaction(async (tx) => {
    const results = [];
    for (const event of events) {
      const ignoredReason = !isSyncedEntity(event.entity)
        ? 'entity not synced'
        : event.realmId !== connectedRealmId
          ? 'not the connected company'
          : null;
      const outcome = await recordEventAndEnqueue(tx, { ...event, source: 'webhook', ignoredReason });
      results.push({ event, outcome, ignoredReason });
    }
    return results;
  });

  for (const { event, outcome, ignoredReason } of outcomes) {
    log.info('sync.webhook.event', {
      event_key: event.eventKey,
      realm_id: event.realmId,
      entity_type: event.entity,
      entity_id: event.id,
      operation: event.operation,
      outcome, // enqueued | duplicate | ignored
      reason: ignoredReason ?? undefined,
    });
  }
  res.status(200).end();
});

export default router;
