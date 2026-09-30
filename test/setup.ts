// Loaded before the tests (see the "test" script). Tests create and delete invoices, so refuse
// to run against anything but the test database.
if (!process.env.DATABASE_URL?.endsWith('/invoices_test')) {
  throw new Error('Tests must run against the invoices_test database: use `npm test`');
}
