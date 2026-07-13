# Migration Guide

`graphql-orm-ai` is not yet published. This guide is still mandatory so early
Git consumers and disposable test deployments can track schema and API changes
without guessing.

## Unreleased: upstream dependency alignment

The public manifest now resolves one exact Git dependency universe:

- `graphql-orm` 0.7.0 at
  `f24db2f0e64dbc939ca875984d48326f47542aeb`; and
- `agql-auth` 0.9.0 at
  `2ab5dc1f963dad401a3393fd3af1392c2bb51e50`.

Remove host patches or path overrides to older sibling versions. Hosts that
also depend directly on either crate must use these exact revisions until a
later coordinated release updates the dependency universe. This is a Rust
dependency/source-identity migration only; it changes no GraphQL SDL, AI schema
module version, persisted data, or application authorization policy.

## Unreleased: schema module 0.7.0 to 0.8.0

Apply `AiSchemaModule` with provider starts and workers closed. This revision
adds the persistent semantics required by authenticated proposal and approval
lifecycles:

- `graphql_orm_ai_proposals.item_count` stores the schema-validated logical
  review-item count;
- proposal, proposal-item, and approval IDs are assigned by the owning service
  before content protection so envelope associated data binds the real row ID;
- proposal and approval tables expose stable dependency-owned keyset metadata;
  and
- approval `session_id` is an internal filterable field for bounded,
  scope-authorized repository windows (it is not exposed as generic CRUD).

The module version is `0.8.0`; do not apply these semantics under an earlier
module version. Existing pre-release proposal rows have no authoritative item
count, and existing protected proposal/approval envelopes may not have been
bound to a service-assigned row ID. Reconcile and remove unfinished rows through
their owning pre-release service, or recreate a disposable environment. Never
invent item counts, decrypt/rewrite envelopes with manual SQL, or treat a prior
pending/approved row as consumable. Completed chat history and provider usage
need no data rewrite.

Keep the runtime gate closed until managed migration validation and restore
reconciliation report module `0.8.0` ready.

### Rust, GraphQL, and behavior changes

- `OrmAiProposalService::persist_validated` stages a catalog-validated proposal
  through the current `AiRunLease` and returns a renewed lease. The previous
  lease is stale.
- `AiProposalService` provides bounded reads and CAS review. `AcceptEdited`
  requires a replacement payload and item count and revalidates both against
  the current exact registered schema version.
- `AiProposalOutcomeRecorder::record_applied_outcome` now takes an
  `&AuthPrincipal`. Call it only after the ordinary application mutation has
  committed; the service rehydrates/authorizes and links the authoritative
  application audit reference without performing the mutation.
- `OrmAiApprovalService::request_approval` requires a server-generated
  canonical preview and complete binding, then atomically parks the exact run
  and tool call in `WaitingApproval`. Its constructor now also requires the
  current `AiToolCatalog`; request and consumption reject catalog/descriptor/
  GraphQL-contract drift.
- `decide_approval` and `revoke_approval` are CAS-bound authenticated GraphQL
  operations. Recent MFA is enforced when the durable request requires it.
- `consume_approval` requires the current waiting lease plus a freshly rebuilt
  binding/preview, rehydrates the original actor, atomically consumes exactly
  once, and returns a renewed `Running` lease and `ConsumedAiApproval`. That
  proof does not replace the fresh resolver authorization/resource-version
  check that must immediately follow.
- Hosts may compose `AiProposalQueryRoot`, `AiProposalMutationRoot`,
  `AiApprovalQueryRoot`, and `AiApprovalMutationRoot`. This adds public GraphQL
  SDL when those roots are composed; regenerate affected client documents.
  Default names are camelCase and the `graphql-case-pascal` feature changes the
  entire new contract coherently without aliases.
- `AiProposalCatalog::descriptor` is new, and registration now rejects zero or
  excessive payload/source/item limits.

No application-domain data migration is required. No proposal review or
approval decision grants domain write authority by itself.

## Unreleased: schema module 0.6.0 to 0.7.0

Apply `AiSchemaModule` through the managed `graphql-orm` schema manager with
workers and provider starts closed. This revision extends
`graphql_orm_ai_tool_calls` with:

- a unique run/provider-call key plus the opaque provider call ID;
- provider-turn and within-turn ordering;
- current authorization policy and authorization-state bindings;
- static disclosure fingerprint and result classification;
- exact result-egress decision ID and manifest hash; and
- the ordinary application audit reference.

The module version is `0.7.0`; never apply these persistent semantics under a
previous module version. Existing pre-release active tool-call rows cannot
safely infer provider call identity, current authorization, disclosure, or
egress proofs. Stop/reconcile them through their owning pre-release service and
classify ambiguity as recovery-required. Recreate a disposable environment if
that service path is unavailable. Do not backfill invented values or use manual
SQL. Conversational messages and completed provider usage need no data rewrite.

Keep the runtime gate closed until managed schema validation and restore
reconciliation report module `0.7.0` ready. This change adds no public GraphQL
root or SDL field, so GraphQL clients need no document regeneration.

### Rust API and behavior changes

- `ModelRequest` requires a `continuation: Option<ModelContinuation>` field.
  Existing request literals should set it to `None`.
- `ModelInputBlock` adds `ToolResult`. A request containing tool results must
  carry an exact previous-response continuation, and a continuation without a
  tool result is rejected by this initial contract.
- `AiProviderCallLimits::with_maximum_tool_calls` sets a per-turn hard limit.
- `AiProviderCallPlan::new` still rejects custom tools. Use `new_with_tools`
  only with an exact current `AiToolPolicySet`; it accepts registered,
  fingerprint-matching, explicitly enabled, idempotent read-only application
  queries with no approval requirement. Use `new_continuation_with_tools` for
  subsequent turns so result blocks and exact manifests cannot be swapped.
- `AiProviderCallResult::tool_calls` exposes normalized unforgeable call
  requests, and `continuation` returns the exact prior-response identity.
- `OrmAiApplicationToolCallService` and `AiApplicationToolCallLimits` own
  protected/fenced read-only resolver execution and result egress. Replace the
  lease after every returned outcome.
- `AiAgentLoopGuard` enforces provider-turn/tool-call bounds and exact call/
  result/continuation ordering. Reconstructing a guard is not a recovery
  mechanism; uncertain work remains closed for restore/operator review.
- `OrmAiProviderOutputService::persist` now rejects results with pending custom
  tool calls.
- OpenAI stateful continuation requires `store_responses = true` and every
  exact transfer manifest must use
  `AI_EGRESS_RETENTION_PROVIDER_RESPONSE` (`provider_response`). The default
  remains false. No provider retention is silently enabled by migration.

The public API additions and `ModelRequest` field are deliberate pre-1.0
changes within the unreleased `0.2.0` line. This `0.7.0` slice did not change
approval semantics; the later `0.8.0` section defines canonical-preview and
one-shot approval persistence. Consequential execution remains unavailable
until the separately gated executor performs fresh resolver authorization.

## Unreleased: schema module 0.5.0 to 0.6.0

The crate version moves from `0.1.0` to `0.2.0` because this revision changes
public pre-1.0 Rust contracts as well as persistence.

Apply `AiSchemaModule` through the managed `graphql-orm` schema manager. This
revision changes budget persistence and must not reuse a previously applied
`0.5.0` module version.

Budget policy rows gain optional principal-kind matching and tool/image unit
ceilings. Budget counter rows gain:

- a stable `period_key` and unique `(budget_policy_id, period_key)` boundary;
- reserved and committed tool/image units; and
- an upsert identity for safe concurrent counter creation.

Budget reservation rows gain principal-kind binding, unique
`(principal_kind, principal_subject, idempotency_key)` enforcement, and an
`actual_runs` field so every reserved dimension reconciles completely.

Run-attempt completion/retry/recovery is now stored in the new append-only
`graphql_orm_ai_run_attempt_outcomes` table, uniquely keyed by `attempt_id`.
The existing append-only claim row remains immutable. Egress event IDs are now
caller-supplied exact `AiEgressDecisionId` values rather than unrelated
generated UUIDs.

The legacy optional completion columns on the pre-release attempt-claim table
remain physically present for non-destructive migration compatibility, but new
workers leave them null. The separate outcome table is the source of truth;
do not disable append-only enforcement to populate the legacy columns.

The `0.5.0` pre-release counter and reservation shapes cannot safely infer
principal kind or a stable interval key. Before applying this migration in an
early test deployment, stop workers and provider starts, classify every
in-flight call, preserve required audit/usage facts, and remove the old active
counter/reservation rows through the owning pre-release service. Do not invent
bindings or run manual SQL. Recreate disposable environments when that safe
service path is unavailable. Keep the runtime start gate closed until schema
validation and reconciliation report module `0.6.0` ready.

### Rust API and behavior changes

- `AiBudgetService::reconcile` returns `AiBudgetReconciliationResult` with
  committed, released, and still-held amounts.
- `AiError::BudgetDenied` reports missing/exhausted applicable capacity.
- `AiBudgetReservation` and `AiBudgetReconciliationResult` are no longer Serde
  deserializable. Obtain reservations from `AiBudgetService`; do not reconstruct
  proof-bearing values from persisted or client-controlled JSON.
- `OrmAiBudgetService` requires validated deployment-owned
  `AiBudgetServiceLimits` and a trusted `agql-auth::Clock`.
- A request now requires exactly one run unit, a fresh `ResolvedPrincipal`, a
  current running lease/attempt/fencing generation, an active persisted
  session with matching owner/tenant/exact scope, an expiry no later than the
  lease, and at least one applicable policy.
- Ordinary reconciliation may move an uncertain reservation to committed when
  authoritative actual usage arrives, but cannot optimistically release it.
- Orchestration must persist `MarkUncertain` immediately before handing the
  authorized reservation proof to provider transport. `ReleaseUnused` is only
  available while the durable reservation is still `Reserved`.
- `OrmAiRunService` and `AiRunServiceLimits` now own queue claims, heartbeats,
  retries, completion, and expired-lease recovery. Callers must replace the
  lease value after every successful fenced write; an older row-version proof
  is deliberately rejected.
- `AiProviderCallExecutor` requires a durable `AiEgressDecisionAudit`, budget
  service, `AiProviderUsageAccounting`, trusted clock, and bounded
  `AiProviderCallLimits`. Accounting implementations must resolve the exact
  `pricing_policy_version`, return provider-observed input/output tokens and
  one run exactly, and authoritatively compute cost/tool/image units. It
  supports one security-ordered provider turn. Tool-free construction remains
  the default; the later `0.7.0` section documents the separately gated
  read-only application-tool path.
- A successful provider result must be passed to
  `OrmAiProviderOutputService::persist` before terminal completion. Use the
  renewed lease returned by that service for `OrmAiRunService::finish`.
- `AiProviderCallResult` is bound to session/run/attempt/generation/provider/
  model and is not a transferable authorization proof.

This release adds no GraphQL budget configuration fields yet. Existing host
GraphQL SDL is unchanged, so no client document regeneration is needed for
this worker/provider slice. Hosts that previously implemented the budget trait
must update the reconciliation return type and preserve the same atomic,
fenced, idempotent semantics. Existing pre-release run claims have no outcome
fact to backfill unless an authoritative final state is known; never infer a
provider outcome. Keep ambiguous active work closed for recovery review.

## Unreleased: schema module 0.4.0 to 0.5.0

Apply the dependency-owned `AiSchemaModule` through the normal
`graphql-orm` schema manager using a new managed migration version. Do not copy
SQL or create AI tables manually.

The additive schema change creates:

- `graphql_orm_ai_budget_counters`
- `graphql_orm_ai_budget_reservations`

It also adds exact target/schema/document/projection/disclosure, principal/
delegation, resource precondition, policy/auth-state, canonical preview, and
one-shot consumption columns to `graphql_orm_ai_approvals`.

No existing conversational content needs rewriting. Existing pre-release
approval rows cannot safely manufacture the new bindings: expire/revoke them
during restore/startup reconciliation and require a fresh approval. Existing
unaccounted provider work must complete or be classified as uncertain before
enabling hard budgets.

Back up a disposable environment before rehearsing the migration. Runtime
workers, subscriptions, webhooks, and schedules remain closed until managed
schema validation and restore reconciliation report module `0.5.0` ready.

### Rust API changes

- `ProviderRequestContext::new` requires an `AuthorizedBudgetReservation`.
- `AiRuntimeBuilder` requires `graphql_targets(...)`.
- `GraphqlRequestContextFactory::build` receives the validated
  `GraphqlExecutionTarget`.
- `ToolGraphqlRequest` carries an exact `GraphqlOperationContract`.
- `GraphqlInvocationContext` carries explicit causation and optional safe
  delegation references plus the exact application scope.
- Application GraphQL tools use
  `AiToolCatalog::register_with_disclosure`; `register` is reserved for
  internal proposal-staging tools.
- `AiRuntimeBuilder` requires `tool_authorization_policy(...)` so current
  principal/scope/descriptor/arguments are authorized on every call.
- `AiRuntime::execute_tool` requires the registered `AiToolId` and returns an
  `AiToolExecutionResult` after argument, output-limit, and disclosure checks.
- Tool argument schemas must explicitly declare JSON Schema 2020-12.

These are deliberate pre-1.0 breaking changes. Update host construction and
mock fixtures together; do not create permissive placeholder targets,
disclosure schemas, or budget grants.

### Provider error classification

The OpenAI adapter now maps HTTP 401 to `ProviderError::CredentialUnavailable`
instead of `ProviderError::Rejected`. Hosts matching public error categories
should handle the credential category as a redacted configuration/rotation
failure. No data migration is required.

### GraphQL naming

The default SDL remains async-graphql camelCase. Hosts requiring PascalCase
enable:

```toml
graphql-orm-ai = {
  version = "0.1.0",
  features = ["sqlite", "graphql-case-pascal"]
}
```

This changes resolver, argument, input, output, subscription, and generated ORM
field names as one compile-time schema contract. There are no lowercase aliases.
Regenerate client documents and compare SDL before rollout. No database
migration is caused solely by the naming feature.

## Initial adoption

New deployments compose `AiSchemaModule`, apply its managed schema, configure
content protection and immutable deployment boundaries, and keep the runtime
start gate closed until readiness succeeds. PostgreSQL/MSSQL rehearsal must use
a disposable Docker-owned database; never point migration commands at a live
machine database.
