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
