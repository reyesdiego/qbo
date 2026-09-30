// Controlled fault injection for tests: a test arms a named fault, and the code checking it fails
// once, at that exact point (e.g. "QuickBooks created the invoice but the response was lost").
// Nothing arms faults outside tests, so in production every check is a no-op.
export type FaultName =
  | 'invoice.enqueue' // after saving the invoice, before inserting its sync job
  | 'webhook.enqueue' // after recording a webhook event, before inserting its sync job
  | 'qbo.create.response-lost' // after QuickBooks created an invoice, before we read the response
  | 'qbo.payment.response-lost'; // after QuickBooks recorded a payment, before we read the response

const armed = new Set<FaultName>();

export const armFault = (name: FaultName) => armed.add(name);
export const disarmFaults = () => armed.clear();
export const shouldFail = (name: FaultName) => armed.delete(name);
