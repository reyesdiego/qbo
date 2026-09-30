import { z } from 'zod';

// Money is a decimal string with two decimals ("1500.00"), stored as NUMERIC(14,2), so amounts are
// never JavaScript floats in the API or the database.

const normalize = (value: string) => {
  const [units, cents = ''] = value.split('.');
  return `${BigInt(units)}.${cents.padEnd(2, '0')}`;
};

export const MoneyInput = z
  .string()
  .regex(/^\d{1,12}(\.\d{1,2})?$/, 'Must be a decimal string with up to 2 decimals, e.g. "1500.00"')
  .transform(normalize);

// QuickBooks sends amounts as JSON numbers; with 2 decimals this is exact for amounts below 10^13
export const moneyFromNumber = (value: number | undefined) => (value ?? 0).toFixed(2);

// ...and expects JSON numbers back
export const moneyToNumber = (value: string) => Number(value);
