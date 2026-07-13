# Getting Started

`graphql-orm-ai` is currently a Git-only pre-release crate. Use the same
reviewed dependency universe for `graphql-orm-ai`, `graphql-orm`, and
`agql-auth`. The public manifest pins reviewed full Git revisions; local path
overrides are unsupported release artifacts.

The current public source snapshot consumes candidate `graphql-orm` 0.7.0 and
`agql-auth` 0.9.0 revisions. They are available by exact commit while their
upstream PRs are reviewed, so standalone Git builds do not depend on whatever
currently happens to be at the sibling default branches.

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
first concrete provider path is deliberately explicit:

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

If transport or streaming becomes ambiguous, do not finish or release the
reservation. It remains uncertain and expired-run reconciliation moves the run
to `RecoveryRequired`.

`AiProviderCallPlan::new` intentionally remains tool-free. The separately
gated `new_with_tools` path exposes only exact registered, policy-enabled,
idempotent read-only application queries. Execute returned calls with
`OrmAiApplicationToolCallService`, carry its renewed lease forward, and use
`AiAgentLoopGuard` plus `new_continuation_with_tools` to prevent missing,
duplicated, or swapped results. Mutations, approval-required operations,
arbitrary GraphQL, and ambiguous resume remain closed. See the
[read-only tool-loop guide](read-only-tool-loop.md).

See the [worker and provider-turn guide](worker-provider-turn.md) and
[implementation status](implementation-status.md). Attachment handling,
budget/usage GraphQL management, the crash-resumable top-level loop worker,
and the consequential tool executor remain under implementation. The proposal
and approval persistence/GraphQL lifecycles are implemented; approval
consumption still must be followed by fresh ordinary resolver authorization.
See the [proposal and approval lifecycle guide](review-lifecycles.md).
