import { setTimeout as sleep } from 'node:timers/promises';
import OAuthClient from 'intuit-oauth';
import { sql } from 'slonik';
import { z } from 'zod';
import { getPool } from './db';
import { NotConnectedError } from './errors';

export { NotConnectedError };

const requireEnv = (name: string): string => {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var ${name} (see .env.example)`);
  return value;
};

const environment = process.env.QBO_ENVIRONMENT === 'production' ? 'production' : 'sandbox';
const apiBaseUrl = OAuthClient.environment[environment];

// A new client each time: it holds the token it's using, so concurrent requests can't overwrite each
// other's (the token itself lives in Postgres). Created on use, so the rest of the app (and its tests)
// runs without QuickBooks credentials.
export const createOAuthClient = (): OAuthClient =>
  new OAuthClient({
    clientId: requireEnv('QBO_CLIENT_ID'),
    clientSecret: requireEnv('QBO_CLIENT_SECRET'),
    environment,
    redirectUri: process.env.QBO_REDIRECT_URI || 'http://localhost:3000/callback/',
  });

const Connection = z.object({
  realm_id: z.string(),
  token: z.record(z.unknown()),
  cdc_cursor: z.string().nullable(),
});

// Tokens live in Postgres so the connection survives restarts and is shared with the worker.
// (In production they should be encrypted at rest.)
export const saveConnection = async (realmId: string, token: object) => {
  const pool = await getPool();
  await pool.transaction(async (tx) => {
    // sql.unsafe = no result schema to validate; values are still sent as query parameters
    await tx.query(sql.unsafe`DELETE FROM quickbooks_connection`);
    await tx.query(sql.unsafe`
      INSERT INTO quickbooks_connection (realm_id, token)
      VALUES (${realmId}, ${JSON.stringify(token)}::jsonb)
    `);
  });
};

// The connected company's id (realmId), or null if not connected
export const getConnectedRealmId = async (): Promise<string | null> => {
  const pool = await getPool();
  const connection = await pool.maybeOne(sql.type(Connection)`SELECT * FROM quickbooks_connection`);
  return connection?.realm_id ?? null;
};

export const getCdcCursor = async (): Promise<string | null> => (await loadConnection()).cdc_cursor;

// GREATEST: with several workers reconciling, the cursor never moves backwards
export const saveCdcCursor = async (cursor: string) => {
  const pool = await getPool();
  await pool.query(sql.unsafe`
    UPDATE quickbooks_connection
    SET cdc_cursor = GREATEST(cdc_cursor, ${cursor}::timestamptz), updated_at = now()
  `);
};

const loadConnection = async () => {
  const pool = await getPool();
  const connection = await pool.maybeOne(sql.type(Connection)`SELECT * FROM quickbooks_connection`);
  if (!connection) throw new NotConnectedError();
  return connection;
};


// Loads the token from the database, refreshing it if expired (access tokens last 1 hour). The
// refresh is an HTTP call, so it happens outside any transaction; the new token is then saved only
// if nobody else refreshed meanwhile (compare-and-set on the old refresh token). If another process
// (API or worker) won, we use the token it saved.
const getConnectedClient = async () => {
  const connection = await loadConnection();
  const oauthClient = createOAuthClient();
  oauthClient.setToken(connection.token);
  if (oauthClient.isAccessTokenValid()) return { oauthClient, realmId: connection.realm_id };

  // A refresh token that expired (after 100 days) or was revoked (Intuit answers invalid_grant): only
  // connecting again helps. Reported as "not connected", so jobs wait for the new connection (woken by
  // it) instead of using up their attempts. Other errors, e.g. network ones, are retried.
  const reconnect = () => new NotConnectedError('The QuickBooks authorization expired or was revoked. Visit /quickbooks/connect again.');
  const token = oauthClient.getToken();
  if (!token.refreshToken() || !token.isRefreshTokenValid()) throw reconnect();
  const oldRefreshToken = token.refreshToken();
  try {
    await oauthClient.refresh();
  } catch (err) {
    throw (err as { error?: string }).error === 'invalid_grant' ? reconnect() : err;
  }

  const pool = await getPool();
  const saved = await pool.maybeOne(sql.type(z.object({ realm_id: z.string() }))`
    UPDATE quickbooks_connection
    SET token = ${JSON.stringify(oauthClient.getToken())}::jsonb, updated_at = now()
    WHERE realm_id = ${connection.realm_id} AND token->>'refresh_token' = ${oldRefreshToken}
    RETURNING realm_id
  `);
  if (!saved) oauthClient.setToken((await loadConnection()).token);
  return { oauthClient, realmId: connection.realm_id };
};

// QuickBooks allows 500 requests per minute per company (and 10 at a time). Spacing each process's
// requests keeps it well below that; a 429 is still retried with backoff by the worker.
const MIN_REQUEST_INTERVAL_MS = Number(process.env.QBO_MIN_REQUEST_INTERVAL_MS || 150);
let nextRequestAt = 0;

const throttle = async () => {
  const now = Date.now();
  const wait = Math.max(0, nextRequestAt - now);
  nextRequestAt = Math.max(now, nextRequestAt) + MIN_REQUEST_INTERVAL_MS;
  if (wait) await sleep(wait);
};

type RequestOptions = {
  method?: 'GET' | 'POST';
  params?: Record<string, string>;
  body?: unknown;
};

// Calls the QuickBooks Accounting API for the connected company.
// Throws NotConnectedError when no company has been connected yet.
export const qboRequest = async (
  buildPath: (realmId: string) => string,
  { method = 'GET', params = {}, body }: RequestOptions = {},
) => {
  const { oauthClient, realmId } = await getConnectedClient();
  await throttle();
  // makeApiCall doesn't retry on its own, so a write is never resent behind our back
  const response = await oauthClient.makeApiCall({
    url: `${apiBaseUrl}v3/company/${realmId}/${buildPath(realmId)}`,
    method,
    params: { minorversion: '75', ...params },
    body,
  });
  return response.json;
};

export const qboQuery = (query: string) => qboRequest(() => 'query', { params: { query } });

// String literals in QuickBooks queries are single-quoted, escaped with a backslash
export const quote = (value: string) => `'${value.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}'`;

// What the sync needs from QuickBooks. The sync functions receive it as a parameter, so tests can
// pass an in-memory fake instead of the real API.
export type QboApi = {
  request: (buildPath: (realmId: string) => string, options?: RequestOptions) => Promise<any>;
  query: (query: string) => Promise<any>;
  realmId: () => Promise<string | null>;
};

export const qbo: QboApi = { request: qboRequest, query: qboQuery, realmId: getConnectedRealmId };
