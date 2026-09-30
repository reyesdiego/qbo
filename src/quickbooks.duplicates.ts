// Duplicate check between Postgres and QuickBooks (GET /quickbooks/duplicates).
// The sync is designed not to create duplicates; this verifies it and catches any that slip through.
import * as invoices from './invoices.repository';
import { qboQuery } from './quickbooks.client';
import { localInvoiceKey, parseLocalInvoiceKey, type QboInvoice } from './quickbooks.mapping';

const PAGE_SIZE = 1000; // QuickBooks' maximum

const fetchAllQuickBooksInvoices = async (): Promise<QboInvoice[]> => {
  const all: QboInvoice[] = [];
  for (let start = 1; ; start += PAGE_SIZE) {
    const data = await qboQuery(`SELECT * FROM Invoice STARTPOSITION ${start} MAXRESULTS ${PAGE_SIZE}`);
    const page: QboInvoice[] = data.QueryResponse.Invoice ?? [];
    all.push(...page);
    if (page.length < PAGE_SIZE) return all;
  }
};

export const findDuplicates = async () => {
  // QuickBooks invoices created by the sync, grouped by the local invoice key in their PrivateNote
  const byKey = new Map<string, { localId: number; quickbooksIds: string[] }>();
  for (const qb of await fetchAllQuickBooksInvoices()) {
    const marker = parseLocalInvoiceKey(qb.PrivateNote);
    if (!marker) continue;
    const group = byKey.get(marker.key) ?? { localId: marker.id, quickbooksIds: [] };
    group.quickbooksIds.push(qb.Id);
    byKey.set(marker.key, group);
  }

  // More than one QuickBooks invoice for the same local invoice
  const groups = [...byKey].filter(([, group]) => group.quickbooksIds.length > 1);
  const locals = await invoices.findLinksByIds(groups.map(([, group]) => group.localId));

  const quickbooks = groups.map(([key, group]) => {
    const local = locals.find((l) => l.id === group.localId && localInvoiceKey(l) === key);
    return {
      local_invoice_id: local ? local.id : null, // null: the local invoice no longer exists
      linked_quickbooks_id: local?.quickbooks_id ?? null, // the one the sync keeps; the others are extra
      quickbooks_ids: group.quickbooksIds,
    };
  });

  return { quickbooks, local: await invoices.findLikelyDuplicates() };
};
