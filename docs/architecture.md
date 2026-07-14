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

Attachment metadata and lifecycle state are AI-owned; bytes use the reviewed
provider-neutral `graphql-orm-storage::BlobStore` boundary. Random quarantine
and final keys, scanning, acceptance, and release remain inside the attachment
service. A host-scheduled maintenance boundary deletes only exact references
selected and fenced through durable attachment lifecycle state; it never lists
storage prefixes or reads content. Application resolvers receive only an
authorized AI attachment ID and decide independently whether it may be linked
to domain data.

## Execution topologies

The runtime supports both embedded and separately deployed execution. It does
not understand federation products. A tool targets a logical deployment ID;
the host resolves that ID to a finished local schema or private authenticated
GraphQL transport.

Remote targets bind audience, resource, schema fingerprint, operation document,
projection, and disclosure metadata. Delegated credentials are ephemeral. The
user's original bearer token, endpoint URL, and secret material are never
stored in sessions, tool calls, or model context.

`AiRemoteAuthenticatedGraphqlAdapter` implements the same context-factory and
executor contracts for private routed/direct targets. It creates a redacted
exact delegation request after the ordinary bridge rehydrates and authorizes
the principal. A deployment issuer mints the ephemeral credential and a
deployment transport maps the logical target to a fixed private destination.
The adapter is federation-neutral; direct routes must not gain authority beyond
their equivalent routed operation.

Local HTTP model servers remain ordinary provider adapters. Installed model or
agent programs use the separate `AiLocalHarnessProvider` and
`AiLocalHarnessDriver` boundary. The provider maps a server-authored logical
model to one immutable deployment registration after the same egress/budget
validation as remote providers. The bounded JSON-lines driver owns framing and
normalization; `AiLocalHarnessProcessLauncher` is the deployment seam that must
apply an operating-system/container sandbox and process-tree lifecycle. This
separation keeps executable, arguments, digest, filesystem, network, and
environment authority outside GraphQL/model input while allowing the normal
provider executor to retain fencing, audit, accounting, and persistence
semantics.

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

A separate exact-principal inbox provides a small cross-session stream for
chat drawers. Source operations and their protected notification commit in one
transaction. Delivery rechecks the referenced session/scope; GraphQL-managed
retention deletes only an expired prefix and advances an explicit retained
cursor without reusing sequence values. See the
[principal inbox guide](principal-inbox.md).

Optional visible provider deltas follow the same durable event path. They are
UTF-8/time/byte bounded, freshly authorized and protected, then committed only
after exact run-fence and uncertain-budget validation. They remain provisional
until the separately persisted completed assistant message is available.

Session retention is a trusted, host-scheduled ORM service rather than a user
resolver. It keyset-scans bounded session shells, reloads the exact current
GraphQL-managed scope policy inside each deletion transaction, and never opens
protected content. Eligible provisional deltas are removed without rewinding
the stream; eligible terminal unattached messages retain metadata but replace
preview/blocks with a structural tombstone. Event readers detect resulting
gaps and require a bounded client reset. Attachments and all immutable
audit/usage/fence evidence remain separate lifecycle obligations.

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

Budget-policy administration uses the existing configuration subgraph rather
than private generated CRUD. Exact-scope reads and recent-MFA/CAS/audited
mutations are host-authorized independently. A deterministic scope key keeps
queries bounded while preserving explicit tenant-wildcard policy semantics;
the key is checked against stored scope fields and never treated as authority.

Authoritative reconciliation appends one immutable usage fact in that same
transaction, uniquely bound to the reservation. Reporting is a separate
authenticated projection: host policy selects exact current-principal or
exact-scope visibility before stable keyset pagination, and the public view
never includes prompt, transcript, tool, pricing-policy, or counter content.
See [usage and budgets](usage-and-budgets.md).

Immutable provider/model pricing is a separate authenticated configuration
projection over an append-only ORM entity. A unique version reference is
carried from conservative quote to reservation to authoritative settlement;
there is no mutable “current rate” lookup that could reprice an in-flight or
restored call. Static deployment bounds, recent MFA, host scope authorization,
and same-transaction audit govern creation. The concrete catalog accounts for
tokens only; built-in tool/image billing stays closed until exact provider
billable-unit evidence can be represented.

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

Terminal completion is a separate fenced write after output persistence. The
output transaction also appends and links an exact final-output checkpoint, so
expired-lease recovery can verify and finish that one crash window. This
ordering prevents a completed run without durable history and prevents a late
worker from writing output after recovery/reclaim.

The executor supports bounded provider turns, provider built-ins with separate
manifests, and a deliberately narrow application-tool branch. That branch
accepts only exact enabled, idempotent read-only GraphQL queries, persists
protected arguments before the ordinary authenticated resolver path, applies
static disclosure, separately authorizes/audits result egress, persists the
protected outcome, advances the fence, and permits only exact bounded
continuation.

The top-level read-only coordinator owns fenced provider heartbeats, bounded
turn/tool sequencing, exact continuation, output persistence, and safe
terminal/recovery classification. Accepted provider results and complete
model-visible tool batches are protected through the current run fence before
the next phase consumes them. An exact complete read-only tool batch can be
adopted across one new generation only after current-authority protected-state
validation and is consumed before provider transport. Provider-turn and
partially completed batches remain closed. Mutations, approval-required/non-
idempotent tools, other ambiguous resume, and stateless reasoning continuation
require their complete preview, approval, fresh-authorization, persistence,
and reconciliation contracts before exposure.

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
