# Claude Code Prompt: QuickBooks Online Bidirectional Synchronization

## Role

Act as a Senior Backend Engineer specializing in Node.js, TypeScript, PostgreSQL, distributed systems, and third-party API integrations.

Your task is to inspect the existing Invoice API repository and implement reliable bidirectional synchronization between our local PostgreSQL database and the real QuickBooks Online (QBO) Developer Sandbox.

The solution must be production-oriented but pragmatic. Avoid unnecessary abstractions, excessive architectural patterns, and additional infrastructure.

**Important: Do not implement a message broker. Use PostgreSQL as the synchronization queue.**

## 1. Technology and architectural constraints

Use the following technologies:

* Node.js + TypeScript.
* Express for the existing REST API.
* PostgreSQL as the primary database.
* Slonik for all PostgreSQL interactions.
* Zod for runtime validation where appropriate.
* The existing testing framework and migration tooling, if available.
* The existing QuickBooks OAuth 2.0 integration and QBO client, if already implemented.

Mandatory restrictions:

* Do not use Prisma, TypeORM or another ORM.
* Do not use PostgreSQL triggers or database functions for synchronization.
* Do not introduce RabbitMQ, Kafka, Redis or BullMQ.
* Do not introduce microservices.
* Do not implement event sourcing.
* Do not perform external QBO API calls inside database transactions.

Use explicit SQL and Slonik transactions.

Preserve the existing project structure and coding conventions whenever possible.

## 2. Primary objective

Implement reliable bidirectional invoice synchronization.

### Local to QuickBooks (OUTBOUND)

When an invoice is created, modified or deleted through our Express API:

1. Validate the request.
2. Persist the local change.
3. Insert the corresponding synchronization job.
4. Commit both operations within a single PostgreSQL transaction.
5. Return the local result without waiting for QBO.
6. Let a background worker synchronize the invoice asynchronously.

The invoice and its synchronization job must be committed atomically.

Use the Transactional Outbox pattern without database triggers.

### QuickBooks to Local (INBOUND)

When QBO sends an invoice or payment webhook:

1. Validate the webhook signature using the original raw HTTP request body.
2. Parse and validate the notification.
3. Persist the incoming event.
4. Enqueue the corresponding synchronization job.
5. Commit the event and job within a single transaction.
6. Return HTTP 200 only after successful persistence.

Do not update invoices directly inside the webhook handler.

The worker must fetch the current remote entity using the real QBO API rather than treating the webhook payload as a complete invoice.

Support notifications containing multiple entities.

## 3. Database design

Inspect the existing database schema before creating migrations.

Implement or adapt the following tables.

### sync_events

This table records incoming notifications and relevant synchronization events.

Suggested fields:

* id
* source
* realm_id
* entity_type
* external_entity_id
* operation
* event_key
* payload JSONB
* received_at
* processing_status
* processed_at
* error

Implement deduplication using a suitable unique event identifier or fingerprint.

Do not assume that two notifications for the same entity are necessarily duplicates.

Preserve useful audit information.

### sync_jobs

This table acts as the durable PostgreSQL job queue.

Suggested fields:

* id UUID
* direction: INBOUND or OUTBOUND
* entity_type
* entity_id
* realm_id
* operation
* payload JSONB, when necessary
* status: PENDING, PROCESSING, COMPLETED, FAILED or UNKNOWN
* attempts
* max_attempts
* next_run_at
* locked_at
* locked_by
* claim_token
* lease_expires_at
* last_error
* created_at
* updated_at

Create appropriate indexes for pending-job retrieval and entity lookup.

Use database constraints to enforce valid states.

Implement migrations using the repository's existing migration tooling.

Do not create triggers.

### Invoice synchronization metadata

Extend the existing invoice model only where necessary.

Consider:

* QBO invoice ID.
* QBO realm ID.
* QBO SyncToken.
* Local version.
* Synchronization status.
* Last synchronized snapshot.
* Last synchronized timestamp.
* Deletion or voiding metadata.

Use a unique constraint scoped to the appropriate QBO company.

Preserve financial precision using PostgreSQL NUMERIC and an appropriate TypeScript representation.

## 4. PostgreSQL queue implementation

Implement a lightweight background worker using Slonik.

Use PostgreSQL's FOR UPDATE SKIP LOCKED to atomically claim available jobs.

The worker must:

* Claim pending jobs safely.
* Support multiple worker instances.
* Avoid processing the same job concurrently.
* Increment attempt counters.
* Track job ownership.
* Implement configurable polling.
* Apply exponential backoff with jitter.
* Respect QBO rate limits.
* Recover abandoned jobs.
* Support graceful shutdown.

Use a claim token and lease expiration to prevent an old worker from completing a job after another worker has reclaimed it.

Do not keep a PostgreSQL transaction open while making an HTTP request to QBO.

Provide clear SQL queries and TypeScript repository functions.

### Entity-level concurrency

SKIP LOCKED prevents duplicate processing of the same job, but does not prevent two different jobs from modifying the same invoice simultaneously.

Implement a simple entity-level coordination mechanism.

Ensure that inbound and outbound jobs affecting the same invoice cannot overwrite each other's work.

Consider how to coordinate entities before a local-to-QBO mapping exists.

Avoid introducing a complex distributed locking framework.

## 5. OUTBOUND synchronization

Implement synchronization from the local database to QBO.

Support:

* Invoice creation.
* Invoice updates.
* Invoice deletion or voiding, according to QBO capabilities and the invoice's financial state.

The worker must load the latest local invoice state rather than blindly executing an outdated queued payload.

Use the existing QBO OAuth connection and refresh tokens when necessary.

Persist remote identifiers and SyncTokens.

Update the local synchronization metadata and job completion status atomically.

### Prevent duplicate invoice creation

Handle this critical scenario:

1. The worker sends a CREATE request to QBO.
2. QBO successfully creates the invoice.
3. The HTTP connection times out.
4. The worker does not receive the QBO invoice ID.

Do not automatically resend CREATE.

Mark the operation as UNKNOWN and implement reconciliation using a stable integration reference and the supported QBO API capabilities.

If the remote result cannot be determined safely, preserve the job for manual investigation.

Never assume exactly-once delivery.

## 6. INBOUND synchronization

Implement processing for incoming QBO webhook jobs.

For each job:

1. Identify the QBO company and entity.
2. Fetch the current entity from QBO.
3. Resolve its local mapping.
4. Compare the remote state against the last synchronized snapshot.
5. Check for pending local changes.
6. Apply safe changes or register a conflict.
7. Update synchronization metadata.
8. Complete the job.

Handle duplicate and out-of-order notifications.

Fetching the latest remote state should prevent stale webhook payloads from overwriting newer information.

Avoid synchronization loops when the remote state already matches the synchronized local state.

Do not interpret every unsuccessful remote GET as proof that an invoice was deleted.

### Payment notifications

Process relevant QBO payment events.

Retrieve the affected payment and invoice information and update the local invoice's payment status and balance.

Do not implement a separate payment processor unless the existing project explicitly requires one.

## 7. Conflict detection

Implement pragmatic conflict detection using:

* Last synchronized snapshot.
* Current local state.
* Current remote state.

Distinguish between:

* Only local changes.
* Only remote changes.
* No meaningful changes.
* Concurrent changes.

Do not automatically overwrite conflicting financial information.

Store enough information to investigate and resolve conflicts.

Avoid implementing a generic merge engine unless necessary.

## 8. Error handling and recovery

Implement explicit error classification.

Handle:

* Network errors.
* HTTP 429.
* HTTP 5xx.
* OAuth token expiration.
* Validation errors.
* QBO version conflicts.
* Ambiguous timeouts.
* Worker crashes.
* Duplicate webhook deliveries.
* Out-of-order notifications.

Use bounded retries with exponential backoff and jitter.

Differentiate permanent failures from retryable failures.

Implement recovery for expired worker leases.

Do not blindly retry ambiguous remote writes.

Provide a mechanism to inspect and manually retry failed jobs.

## 9. Reconciliation

Implement a lightweight reconciliation process to detect changes missed because of webhook delivery failures.

Use the supported QBO API capabilities.

Reuse the existing synchronization queue.

Make the reconciliation process configurable and safe to rerun.

Avoid unnecessary full synchronization on every execution.

## 10. Testing

Use the existing testing framework.

Implement unit and integration tests covering:

* Atomic invoice and job creation.
* Atomic webhook event and job creation.
* Duplicate webhook delivery.
* Concurrent worker execution.
* Entity-level concurrency.
* Retry scheduling.
* Expired lease recovery.
* Invalid webhook signatures.
* QBO version conflicts.
* Out-of-order events.
* Synchronization-loop prevention.
* Ambiguous CREATE timeout.
* Conflict detection.

Implement real integration tests against the QuickBooks Developer Sandbox for supported operations.

Use a dedicated sandbox customer and uniquely identifiable test invoices.

Do not delete unrelated sandbox sample data.

Use controlled fault injection to simulate network failures and duplicate events that cannot be reliably reproduced naturally.

Keep sandbox integration tests separate from ordinary unit tests.

## 11. Observability

Implement structured logging using the project's existing logger.

Include:

* Job ID.
* Event ID, when applicable.
* Entity ID.
* Realm ID.
* Direction.
* Attempt number.
* Processing duration.
* Error classification.

Never log OAuth tokens or sensitive credentials.

Provide simple queries or metrics for:

* Pending jobs.
* Failed jobs.
* Unknown jobs.
* Retry counts.
* Oldest pending job.
* Average processing time.

Avoid introducing an additional observability platform.

## 12. Implementation workflow

Follow this order:

1. Inspect the existing repository and explain which components can be reused.
2. Identify missing functionality and propose a minimal implementation plan.
3. Implement SQL migrations and Slonik repositories.
4. Implement the PostgreSQL queue and worker.
5. Implement outbound synchronization.
6. Implement inbound webhook processing.
7. Implement conflict detection and reconciliation.
8. Add recovery mechanisms and tests.
9. Document the architecture and operational procedures.

Do not rewrite unrelated parts of the application.

Do not create unnecessary interfaces, generic repositories or additional architectural layers.

Prioritize small, understandable modules.

Before implementing a feature, verify whether the repository already contains an equivalent implementation.

## 13. Expected deliverables

Provide:

* Working TypeScript implementation.
* SQL migrations.
* Slonik repository functions.
* PostgreSQL worker.
* Express webhook endpoint.
* QBO synchronization services.
* Automated tests.
* Environment configuration example.
* README explaining how to run the API and worker.
* Instructions for testing against the real QBO sandbox.

Document important architectural decisions and their tradeoffs.

Explain why PostgreSQL was selected instead of RabbitMQ or Kafka and identify the conditions under which a dedicated message broker would become appropriate.

## Acceptance criteria

The implementation is complete when:

1. Creating a local invoice atomically enqueues its synchronization job.
2. The worker creates the corresponding invoice in the real QBO sandbox.
3. Remote changes received through webhooks are synchronized locally.
4. Duplicate events do not produce duplicate financial operations.
5. Multiple workers can operate safely.
6. Concurrent changes do not silently overwrite financial information.
7. Temporary failures are retried safely.
8. Ambiguous remote writes are reconciled instead of blindly repeated.
9. Worker interruptions do not permanently lose jobs.
10. All synchronization logic works without PostgreSQL triggers or external message brokers.

**Keep the implementation simple enough to explain clearly in a Senior Backend System Design interview, while demonstrating sound distributed-systems engineering.**
