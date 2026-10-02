export class NotConnectedError extends Error {
  constructor(message = 'Not connected to QuickBooks. Visit /quickbooks/connect first.') {
    super(message);
  }
}

// Retrying won't help (e.g. invalid data): the job fails right away
export class PermanentError extends Error {}

// The entity is busy with another job: try again shortly, without counting it as a failed attempt
export class DeferError extends Error {}

// Another worker reclaimed the job after our lease expired: stop without touching it
export class LeaseLostError extends Error {}

// QuickBooks may or may not have applied a write (e.g. a timeout after sending a create):
// never resend blindly, reconcile first
export class AmbiguousWriteError extends Error {}

// QuickBooks answered, but the body did not contain the shape this integration needs.
export class UnexpectedQuickBooksResponseError extends Error {}

export type ErrorClass =
  | 'not_connected'
  | 'auth'
  | 'rate_limited'
  | 'timeout'
  | 'network'
  | 'server'
  | 'version_conflict'
  | 'not_found'
  | 'validation'
  | 'internal';

type QboError = { code?: string; fault?: { errors: { code: string; message: string; detail?: string }[] } };

const result = (errorClass: ErrorClass, retryable: boolean, ambiguous = false) => ({ errorClass, retryable, ambiguous });

// Maps an error to how the sync should react. `ambiguous` means a write may have been applied even
// though we got an error (timeouts, dropped connections, 5xx after the request was sent).
export const classifyError = (err: unknown) => {
  if (err instanceof NotConnectedError) return result('not_connected', true);
  if (err instanceof PermanentError) return result('validation', false);

  const { code, fault } = (err ?? {}) as QboError;
  if (fault?.errors.some((e) => e.code === '5010')) return result('version_conflict', true); // stale SyncToken
  if (fault?.errors.some((e) => e.code === '610')) return result('not_found', true);
  if (fault) return result('validation', false);

  switch (code) {
    case 'RATE_LIMIT_EXCEEDED':
    case '429':
      return result('rate_limited', true);
    case 'TIMEOUT_ERROR':
      return result('timeout', true, true);
    case 'NETWORK_ERROR':
      return result('network', true, true);
    case '500':
    case 'INTERNAL_SERVER_ERROR':
    case '502':
    case '503':
    case '504':
      return result('server', true, true);
    case '401':
    case '403':
      return result('auth', true);
    case '404':
      return result('not_found', true);
    case '400':
      return result('validation', false);
    default:
      // Our own bugs or database errors: retry, and after max attempts leave it FAILED for inspection
      return result('internal', true);
  }
};

export const errorMessage = (err: unknown): string => {
  const { fault } = (err ?? {}) as QboError;
  if (fault) return fault.errors.map((e) => (e.detail ? `${e.message}: ${e.detail}` : e.message)).join('; ');
  return err instanceof Error ? err.message : String(err);
};
