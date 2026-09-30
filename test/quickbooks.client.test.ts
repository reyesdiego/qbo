// The QuickBooks client's token handling: a refresh token that expired or was revoked
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import OAuthClient from 'intuit-oauth';
import { sql } from 'slonik';
import { getPool } from '../src/db';
import { classifyError } from '../src/errors';
import { qbo } from '../src/quickbooks.client';
import { ensureTestConnection, TEST_REALM } from './helpers';

const HOUR = 60 * 60 * 1000;

before(async () => {
  // The OAuth client needs credentials to be created; these tests never reach Intuit
  process.env.QBO_CLIENT_ID ??= 'test-client';
  process.env.QBO_CLIENT_SECRET ??= 'test-secret';
  await ensureTestConnection();
});

after(async () => {
  const pool = await getPool();
  await pool.query(sql.unsafe`UPDATE quickbooks_connection SET token = '{}'::jsonb WHERE realm_id = ${TEST_REALM}`);
  await pool.end();
});

const saveToken = async (token: object) => {
  const pool = await getPool();
  await pool.query(sql.unsafe`UPDATE quickbooks_connection SET token = ${JSON.stringify(token)}::jsonb WHERE realm_id = ${TEST_REALM}`);
};

// A job failing with this waits for a new connection (no attempt used up) and is woken on reconnect
const waitsForConnection = (err: unknown) => {
  const { errorClass, retryable } = classifyError(err);
  return retryable && (errorClass === 'not_connected' || errorClass === 'auth');
};

test('an expired refresh token: the request fails as "not connected", so jobs wait for a new connection', async () => {
  // Both tokens expired (the refresh token lasts 100 days)
  await saveToken({ access_token: 'a', refresh_token: 'r', expires_in: 3600, x_refresh_token_expires_in: 3600, createdAt: Date.now() - 2 * HOUR });
  await assert.rejects(qbo.request(() => 'companyinfo/1'), (err) => waitsForConnection(err));
});

test('a refresh token rejected by Intuit (invalid_grant, e.g. revoked): same', async () => {
  await saveToken({ access_token: 'a', refresh_token: 'r', expires_in: 3600, x_refresh_token_expires_in: 8640000, createdAt: Date.now() - 2 * HOUR });
  // What intuit-oauth throws when Intuit answers the refresh with 400 invalid_grant
  const proto = OAuthClient.prototype as unknown as { refresh: () => Promise<unknown> };
  const refresh = proto.refresh;
  proto.refresh = async () => {
    throw Object.assign(new Error('invalid_grant'), { error: 'invalid_grant', error_description: 'Incorrect or invalid refresh token' });
  };
  try {
    await assert.rejects(qbo.request(() => 'companyinfo/1'), (err) => waitsForConnection(err));
  } finally {
    proto.refresh = refresh;
  }
});

test('concurrent requests never use each other\'s token', async () => {
  const valid = (access_token: string) => ({ access_token, refresh_token: 'r', expires_in: 3600, x_refresh_token_expires_in: 8640000, createdAt: Date.now() });
  const proto = OAuthClient.prototype as unknown as { makeApiCall: (this: OAuthClient) => Promise<unknown> };
  const makeApiCall = proto.makeApiCall;
  const used: string[] = [];
  proto.makeApiCall = async function () {
    used.push(this.getToken().access_token);
    return { json: {} };
  };
  try {
    await saveToken(valid('first'));
    await qbo.request(() => 'companyinfo/1'); // the next request now waits for the throttle
    await saveToken(valid('A'));
    const a = qbo.request(() => 'companyinfo/1'); // loads token A, then waits its turn
    await sleep(50);
    await saveToken(valid('B')); // e.g. another process refreshed it
    const b = qbo.request(() => 'companyinfo/1');
    await Promise.all([a, b]);
    assert.deepEqual(used, ['first', 'A', 'B']);
  } finally {
    proto.makeApiCall = makeApiCall;
  }
});

