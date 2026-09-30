#!/bin/sh
# (Re)creates the empty test database used by `npm test`; its tables come from the migrations,
# which `npm test` applies before running. Runs on the first start of the Postgres container,
# and `npm run db:test:reset` runs it again.
set -e
dropdb -U "$POSTGRES_USER" --if-exists invoices_test
createdb -U "$POSTGRES_USER" invoices_test
