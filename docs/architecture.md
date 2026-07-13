# Architecture and Crate Boundaries

`graphql-orm-ai` owns reusable agent orchestration: sessions, protected
messages, runs, tools, approvals, proposals, usage, budgets, egress decisions,
provider adapters, and recovery. It owns the `graphql_orm_ai_*` schema module
but does not own application domain data.

`graphql-orm` owns backend SQL, generated repositories, transactions, CAS,
keyset pagination, schema modules, migration planning, backup metadata, and
portable durable-stream/lease primitives. `graphql-orm-ai` never issues SQL.

`agql-auth` owns safe principal references, current-principal rehydration,
session/token status, assurance, and reusable audience/resource-bound
delegation. The AI crate owns tool maturity, AI approvals, provider egress, and
budgets.

Host applications own domain resolvers and policies, scope mapping, request
context construction, proposal schemas/UI, deployment network and secret
isolation, and the final mutations that apply reviewed proposals.

## Execution topologies

The runtime supports both embedded and separately deployed execution. It does
not understand federation products. A tool targets a logical deployment ID;
the host resolves that ID to a finished local schema or private authenticated
GraphQL transport.

Remote targets bind audience, resource, schema fingerprint, operation document,
projection, and disclosure metadata. Delegated credentials are ephemeral. The
user's original bearer token, endpoint URL, and secret material are never
stored in sessions, tool calls, or model context.

For every registered call the bridge rehydrates the principal, invokes a
required host `AiToolAuthorizationPolicy` over the scope/descriptor/validated
arguments, builds the ordinary request context, and executes the resolver.
The runtime returns an `AiToolExecutionResult` only after byte/list bounds and
the closed static disclosure schema succeed. External disclosure still needs a
separate egress decision.

## Persistence and streaming

Local ORM state is canonical. Messages, blocks, runs, tool calls, and durable
events are independently windowed. Subscriptions replay to a watermark and use
commit-only wakeups; clients never need a complete session snapshot.

Schema migration, backup, restore, and runtime readiness use the dependency-
owned `AiSchemaModule`. A restored database is not runnable until leases,
approvals, provider continuations, uncertain side effects, and content
protection have been reconciled.

Provider capacity is reserved through an ORM state-machine transaction across
every applicable scope/tenant/principal policy. The concrete service validates
the fresh principal and current run fence in that transaction, uses a unique
principal/idempotency binding, and reconciles every counter exactly once.
Unknown external outcomes retain their full reservation; the ordinary worker
path cannot release uncertain capacity.

## Durable worker and provider turn

`OrmAiRunService` is the concrete SQLite/PostgreSQL queue boundary. A claim
atomically appends an immutable attempt fact and changes the run to `Leased`
with a new attempt ID, owner, expiry, row version, and monotonically increasing
generation. Every subsequent write re-reads and validates that complete fence.
Attempt completion, retry, and recovery are separate append-only outcome facts.

The implemented provider turn is deliberately security ordered:

1. require an open runtime and exact running lease;
2. rehydrate current authority and recheck session/scope write access;
3. reserve every applicable budget atomically against the persisted active
   session owner/tenant/scope and run fence;
4. authorize every transfer and append each exact allow/deny egress decision;
5. mark budget uncertain immediately before transport;
6. bound and normalize the provider stream;
7. settle the exact immutable pricing version and commit authoritative usage;
   and
8. reauthorize/protect and atomically append the assistant message blocks,
   completed-message event, and renewed run fence.

Terminal completion is a separate fenced write after output persistence. This
ordering prevents a completed run without durable history and prevents a late
worker from writing output after recovery/reclaim.

The executor supports bounded provider turns, provider built-ins with separate
manifests, and a deliberately narrow application-tool branch. That branch
accepts only exact enabled, idempotent read-only GraphQL queries, persists
protected arguments before the ordinary authenticated resolver path, applies
static disclosure, separately authorizes/audits result egress, persists the
protected outcome, advances the fence, and permits only exact bounded
continuation.

A crash-resumable top-level coordinator and all consequential paths remain
closed. Mutations, proposals, approval-required/non-idempotent tools, ambiguous
resume, and stateless reasoning continuation require their complete preview,
approval, fresh-authorization, persistence, and reconciliation contracts before
they can be exposed.

## Proposal and approval staging

The ORM proposal service accepts only catalog-validated structured output and
persists it through the current run fence. Authenticated GraphQL review can
accept, schema-validly edit, or reject only AI-owned staging rows. A final
application mutation remains consumer-owned; afterward, a freshly authorized
trusted recorder may link its resource and ordinary application audit
references.

The ORM approval service persists protected server-generated previews and full
action bindings, parks the exact run/tool call, records CAS-bound human
decisions, and atomically consumes an approved grant once after rehydrating the
original actor. Consumption returns a renewed run fence but no GraphQL
authority. The later consequential executor must invoke the exact registered
resolver through the ordinary bridge and let all current application policy
run again.
