// The QuickBooks webhook endpoint: signature check, parsing, and recording events + jobs atomically
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { createHmac, randomUUID } from 'node:crypto';
import type { Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { sql } from 'slonik';
import { z } from 'zod';
import app from '../src/app';
import { getPool } from '../src/db';
import { armFault } from '../src/faults';
import { isValidSignature, parseWebhookPayload } from '../src/quickbooks.webhooks';
import { ensureTestConnection, TEST_REALM } from './helpers';

const VERIFIER_TOKEN = 'test-verifier-token';
const sign = (body: string, token = VERIFIER_TOKEN) => createHmac('sha256', token).update(body).digest('base64');

// A notification with unique entity ids, so tests don't see each other's events
const legacyPayload = (realmId = TEST_REALM) => {
  const suffix = randomUUID();
  return {
    eventNotifications: [
      {
        realmId,
        dataChangeEvent: {
          entities: [
            { name: 'Invoice', id: `inv-${suffix}`, operation: 'Update', lastUpdated: '2026-09-27T10:00:00-0700' },
            { name: 'Payment', id: `pay-${suffix}`, operation: 'Create', lastUpdated: '2026-09-27T10:00:05-0700' },
          ],
        },
      },
    ],
  };
};

let server: Server;
let baseUrl: string;

before(async () => {
  await ensureTestConnection();
  process.env.QBO_WEBHOOK_VERIFIER_TOKEN = VERIFIER_TOKEN;
  server = app.listen(0);
  baseUrl = `http://localhost:${(server.address() as AddressInfo).port}`;
});

after(async () => {
  server.close();
  await (await getPool()).end();
});

const postWebhook = (body: string, signature?: string) =>
  fetch(`${baseUrl}/quickbooks/webhooks`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...(signature ? { 'intuit-signature': signature } : {}) },
    body,
  });

const eventsFor = async (entityId: string) => {
  const pool = await getPool();
  return pool.any(sql.type(z.object({ id: z.number(), processing_status: z.string(), error: z.string().nullable() }))`
    SELECT id, processing_status, error FROM sync_events WHERE external_entity_id = ${entityId}
  `);
};

const jobsForEntity = async (entityId: string) => {
  const pool = await getPool();
  return pool.any(sql.type(z.object({ id: z.string(), event_id: z.number().nullable() }))`
    SELECT id, event_id FROM sync_jobs WHERE entity_id = ${entityId}
  `);
};

test('parses the original Intuit format (several entities per notification)', () => {
  const events = parseWebhookPayload({
    eventNotifications: [
      {
        realmId: '123',
        dataChangeEvent: {
          entities: [
            { name: 'Invoice', id: '42', operation: 'Update', lastUpdated: '2026-09-27T10:00:00-0700' },
            { name: 'Payment', id: '7', operation: 'Delete', lastUpdated: '2026-09-27T10:00:00-0700' },
          ],
        },
      },
    ],
  });
  assert.deepEqual(events.map(({ realmId, entity, id, operation }) => ({ realmId, entity, id, operation })), [
    { realmId: '123', entity: 'invoice', id: '42', operation: 'update' },
    { realmId: '123', entity: 'payment', id: '7', operation: 'delete' },
  ]);
  assert.notEqual(events[0].eventKey, events[1].eventKey);
});

test('parses the CloudEvents format', () => {
  const [event] = parseWebhookPayload([
    { specversion: '1.0', id: 'evt-1', type: 'qbo.account.created.v1', intuitentityid: '91', intuitaccountid: '123', data: {} },
  ]);
  assert.deepEqual(
    { realmId: event.realmId, entity: event.entity, id: event.id, operation: event.operation, eventKey: event.eventKey },
    { realmId: '123', entity: 'account', id: '91', operation: 'create', eventKey: 'cloudevent:evt-1' },
  );
});

test('checks the signature', () => {
  const body = Buffer.from('{"a":1}');
  assert.equal(isValidSignature(body, sign('{"a":1}'), VERIFIER_TOKEN), true);
  assert.equal(isValidSignature(body, sign('{"a":2}'), VERIFIER_TOKEN), false); // body changed
  assert.equal(isValidSignature(body, sign('{"a":1}', 'other-token'), VERIFIER_TOKEN), false);
  assert.equal(isValidSignature(body, undefined, VERIFIER_TOKEN), false);
});

test('webhook endpoint', async (t) => {
  await t.test('records each event with its job, then answers 200', async () => {
    const payload = legacyPayload();
    const body = JSON.stringify(payload);
    assert.equal((await postWebhook(body, sign(body))).status, 200);

    for (const entity of payload.eventNotifications[0].dataChangeEvent.entities) {
      const [event] = await eventsFor(entity.id);
      assert.equal(event.processing_status, 'pending');
      const jobs = await jobsForEntity(entity.id);
      assert.equal(jobs.length, 1);
      assert.equal(jobs[0].event_id, event.id);
    }
  });

  await t.test('a duplicate delivery is recorded and queued once', async () => {
    const payload = legacyPayload();
    const body = JSON.stringify(payload);
    await postWebhook(body, sign(body));
    assert.equal((await postWebhook(body, sign(body))).status, 200);

    const invoiceId = payload.eventNotifications[0].dataChangeEvent.entities[0].id;
    assert.equal((await eventsFor(invoiceId)).length, 1);
    assert.equal((await jobsForEntity(invoiceId)).length, 1);
  });

  await t.test('if queuing the job fails, nothing is recorded and Intuit gets a 500 (and retries)', async () => {
    const payload = legacyPayload();
    const body = JSON.stringify(payload);
    armFault('webhook.enqueue');
    assert.equal((await postWebhook(body, sign(body))).status, 500);

    for (const entity of payload.eventNotifications[0].dataChangeEvent.entities) {
      assert.equal((await eventsFor(entity.id)).length, 0);
      assert.equal((await jobsForEntity(entity.id)).length, 0);
    }
  });

  await t.test('events of another company are recorded as ignored, without a job', async () => {
    const payload = legacyPayload('another-company');
    const body = JSON.stringify(payload);
    assert.equal((await postWebhook(body, sign(body))).status, 200);

    const invoiceId = payload.eventNotifications[0].dataChangeEvent.entities[0].id;
    const [event] = await eventsFor(invoiceId);
    assert.equal(event.processing_status, 'ignored');
    assert.equal(event.error, 'not the connected company');
    assert.equal((await jobsForEntity(invoiceId)).length, 0);
  });

  await t.test('rejects a missing or wrong signature', async () => {
    const body = JSON.stringify(legacyPayload());
    assert.equal((await postWebhook(body)).status, 401);
    assert.equal((await postWebhook(body, sign(body, 'other-token'))).status, 401);
  });

  await t.test('rejects a signed body that is not a valid payload', async () => {
    assert.equal((await postWebhook('not json', sign('not json'))).status, 400);
    const wrongShape = JSON.stringify({ eventNotifications: [{ nope: true }] });
    assert.equal((await postWebhook(wrongShape, sign(wrongShape))).status, 400);
  });

  await t.test('is disabled until the verifier token is configured', async () => {
    const body = JSON.stringify(legacyPayload());
    delete process.env.QBO_WEBHOOK_VERIFIER_TOKEN;
    try {
      assert.equal((await postWebhook(body, sign(body))).status, 503);
    } finally {
      process.env.QBO_WEBHOOK_VERIFIER_TOKEN = VERIFIER_TOKEN;
    }
  });
});
