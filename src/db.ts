import {
  createPool,
  SchemaValidationError,
  type DatabasePool,
  type Interceptor,
  type DriverTypeParser,
} from 'slonik';

export const DATABASE_URL =
  process.env.DATABASE_URL || 'postgres://postgres:postgres@localhost:5433/invoices';

// Validates every returned row against the zod schema passed to `sql.type(...)`.
const resultParserInterceptor: Interceptor = {
  name: 'result-parser',
  transformRow: (context, query, row) => {
    if (!context.resultParser) return row;

    const result = context.resultParser.safeParse(row);
    if (!result.success) {
      throw new SchemaValidationError(query, row, result.error.issues);
    }
    return result.data;
  },
};

// Keep DATE as 'YYYY-MM-DD' and TIMESTAMPTZ as ISO strings, which is what the API returns.
// BIGINT as a regular number (exact up to 2^53, plenty for amounts in cents) instead of a BigInt,
// which JSON can't serialize.
const typeParsers: DriverTypeParser[] = [
  { name: 'int8', parse: (value) => Number(value) },
  { name: 'date', parse: (value) => value },
  { name: 'timestamptz', parse: (value) => new Date(value).toISOString() },
];

let poolPromise: Promise<DatabasePool> | undefined;

export const getPool = (): Promise<DatabasePool> => {
  poolPromise ??= createPool(DATABASE_URL, {
    interceptors: [resultParserInterceptor],
    typeParsers,
  });
  return poolPromise;
};
