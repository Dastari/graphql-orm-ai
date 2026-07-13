# Getting Started

`graphql-orm-ai` is currently a Git-only pre-release crate. Use the same
reviewed dependency universe for `graphql-orm-ai`, `graphql-orm`, and
`agql-auth`. The public manifest pins reviewed full Git revisions; local path
overrides are unsupported release artifacts.

The current public source snapshot consumes the final reviewed `graphql-orm`
0.7.0 merge commit and `agql-auth` 0.10.0 annotated-tag target. Standalone Git
builds therefore resolve the released dependency universe without depending on
moving sibling default branches.

## Features

Exactly one persistence backend is currently required:

- `sqlite` (default)
- `postgres`
- `mssql` (schema/compile support until ORM write parity lands)

Provider adapters are opt-in. `provider-openai` enables the native OpenAI
Responses adapter. `graphql-case-pascal` changes the complete GraphQL naming
contract from default camelCase to PascalCase.

## Host integration outline

1. Compose `AiSchemaModule` and apply its managed schema through
   `graphql-orm`.
2. Install `AuthPrincipal` in normal GraphQL request context and provide a
   `CurrentPrincipalResolver` for durable work.
3. Provide session/configuration access, fresh principal-aware
   `AiToolAuthorizationPolicy`, egress, secret-store, and content-protection
   implementations.
4. Register immutable logical GraphQL targets. Remote target URLs and
   credential issuance remain deployment-owned and never model-visible.
5. Register reviewed application tools with server-authored documents, exact
   operation contracts, and static disclosure schemas. Registration does not
   enable a tool.
6. Register proposal types and provider adapters.
7. Install `OrmAiProposalService`/`OrmAiApprovalService` when composing their
   authenticated GraphQL roots. Supply host policies, fresh principal
   rehydration, content protection, recent-MFA policy, and the same fenced run
   service; do not expose the private generated ORM entities.
8. Apply/validate migrations and restore reconciliation, then open the runtime
   start gate.

For private routed/direct targets, use one cloned
`AiRemoteAuthenticatedGraphqlAdapter` as both request-context factory and
executor. Implement its authority issuer at the short-lived credential boundary
and its transport at the fixed private logical-route boundary. Do not retain or
forward the user's bearer token. See the
[remote execution guide](remote-graphql-execution.md).

For SQLite/PostgreSQL hosts, construct `OrmAiBudgetService` with a trusted
`agql-auth::Clock` and validated deployment-owned `AiBudgetServiceLimits`.
Provider orchestration must call `reserve` before egress and `reconcile` after
the result classification. It must durably mark the reservation uncertain
immediately before handing the authorized proof to provider transport; after
that boundary the ordinary unused-release path is deliberately unavailable.
Budget policy configuration is not yet exposed as a public GraphQL lifecycle,
so do not seed policies with application SQL or expose the private generated
ORM entities; that authenticated configuration surface remains an
implementation gate.

Construct `OrmAiRunService` from the same ORM database and trusted clock. The
lower-level concrete provider path is deliberately explicit:

1. `claim_next` and `start` a run, replacing the returned lease after every
   successful fenced call.
2. Execute a server-authored `AiProviderCallPlan` through
   `AiProviderCallExecutor`, configured with the budget service and a durable
   `OrmAiEgressDecisionAudit`. Supply `AiProviderUsageAccounting` backed by an
   immutable deployment pricing catalog; it must settle the exact pricing
   version rather than substituting current rates or reserved estimates.
3. If the provider result is terminal and has no application-tool calls,
   persist it with `OrmAiProviderOutputService::persist`. This reauthorizes
   again, protects content, writes windowable blocks and a session event, and
   returns a renewed lease.
4. Finish the run with that renewed lease.

For the bounded registered read-only tool path, prefer
`AiReadOnlyAgentCoordinator` over manually sequencing these calls. Supply a
trusted `AiReadOnlyAgentTurnPlanner` that constructs initial turns with
`new_with_tools` and consumes exact later `AiAgentContinuation` values with
`new_continuation_with_tools`. Configure its heartbeat interval comfortably
shorter than the run-service lease TTL. Also supply an
`OrmAiCoordinatorCheckpointService` as the required
`AiAgentCheckpointWriter`, using the same principal/access/protection
boundaries as transcript persistence. A successful coordinator outcome means
the terminal/recovery state was durably committed; a lost fence returns an
error and must not be followed by another write from that worker. Protected
non-final checkpoints are not yet cross-generation resume authority; see the
[checkpoint guide](coordinator-checkpoints.md).

If transport or streaming becomes ambiguous, do not finish or release the
reservation. It remains uncertain and expired-run reconciliation moves the run
to `RecoveryRequired`.

`AiProviderCallPlan::new` intentionally remains tool-free. The separately
gated `new_with_tools` path exposes only exact registered, policy-enabled,
idempotent read-only application queries. Execute returned calls with
`OrmAiApplicationToolCallService`, carry its renewed lease forward, and use
`AiAgentLoopGuard` plus `new_continuation_with_tools` to prevent missing,
duplicated, or swapped results. Supervised plans use the dedicated constructors
and `OrmAiConsequentialToolCallService` to request a server-previewed one-shot
approval and execute it through fresh ordinary resolver authorization;
top-level approval-wait coordination remains host-owned. Arbitrary GraphQL and
ambiguous replay remain closed. See the
[read-only tool-loop guide](read-only-tool-loop.md).

See the [worker and provider-turn guide](worker-provider-turn.md) and
[implementation status](implementation-status.md). Attachment handling,
budget/usage GraphQL management, partial-batch restart adoption, durable live
delta persistence, and top-level approval-wait coordination remain under
implementation. The proposal/approval GraphQL lifecycles and consequential
executor are implemented; approval consumption is always followed by fresh
ordinary resolver authorization in that path. See the
[proposal and approval lifecycle guide](review-lifecycles.md) and
[supervised tool guide](supervised-tool-loop.md).
