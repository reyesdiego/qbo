// Loaded before the sandbox tests (see the "test:sandbox" script). They create real invoices in
// QuickBooks, so they refuse to run against anything but a sandbox company.
if (process.env.QBO_ENVIRONMENT !== 'sandbox') {
  throw new Error('Sandbox tests only run with QBO_ENVIRONMENT=sandbox');
}
