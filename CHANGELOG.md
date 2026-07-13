# Changelog

All notable user-visible changes are recorded here. The crate follows
Semantic Versioning and keeps migration instructions in [MIGRATION.md](MIGRATION.md).

## [Unreleased]

This development line advances the pre-1.0 crate version to `0.4.0` and the AI
schema module to `0.10.0`.

### Added

- Project-agnostic AI schema module with 37 private persistence entities for
  configuration, sessions, protected history, fenced runs, tools, approvals,
  proposals, budgets, usage, egress, audit, and restore readiness.
- Owner-isolated ORM-backed session/configuration services and resumable
  durable session-event subscriptions for SQLite/PostgreSQL.
- Provider-neutral streaming contracts, deterministic mock provider, and a
  feature-gated OpenAI Responses adapter.
- Explicit egress manifests/proofs, secret-store/content-protection contracts,
  default-deny tools, structured proposals, and restore/start gates.
- Logical local/remote GraphQL execution targets with schema/document/
  projection/disclosure bindings and no model-visible URL.
- Static recursive disclosure schemas that reject unknown, mismatched,
  oversized, secret, and structurally non-exportable result nodes.
- Atomic budget reservation domain contracts and provider-call proofs bound to
  run, attempt, fence, provider, model, output ceiling, pricing version, and
  expiry.
- ORM-backed SQLite/PostgreSQL budget service with multi-policy atomic
  reservation, principal/fence validation, bounded serialization retries,
  content-bound idempotency, exact-once reconciliation, conservative uncertain
  capacity, and in-memory concurrency tests.
- ORM-backed SQLite/PostgreSQL run service with bounded durable queue claims,
  immutable attempt/outcome history, monotonic fencing generations, renewable
  leases, bounded retries, terminal transitions, expired-lease recovery, and
  stale-worker/concurrent-claim tests.
- Append-only ORM egress-decision audit plus a security-ordered provider-turn
  executor that reauthorizes access, reserves budget, records every exact
  allow/deny decision before transport, marks calls uncertain at the transport
  boundary, bounds normalized events, and commits authoritative usage.
- Deployment-owned `AiProviderUsageAccounting` contract so an exact immutable
  pricing version settles cost/tool/image units after provider token usage;
  estimated cost is never mislabeled as authoritative actual usage.
- Fenced provider-output persistence that reauthorizes the current principal,
  resolves current content-protection policy, splits large assistant output
  into windowable protected blocks, and atomically appends the message,
  session event, and renewed run fence.
- Durable read-only application-tool execution for SQLite/PostgreSQL with exact
  registered/policy-bound model definitions, bounded normalized call IDs and
  arguments, protected pre-execution arguments and post-execution results,
  ordinary current-principal GraphQL resolver execution, static disclosure,
  separately audited tool-result egress, session events, run-step history, and
  renewed run fencing.
- `AiAgentLoopGuard` and exact `AiAgentContinuation` sequencing that bind a
  provider response, every requested `call_id`, every protected tool result,
  and its immutable egress manifest under hard provider-turn/tool-call limits.
- `AiReadOnlyAgentCoordinator` with host-owned exact turn planning, periodic
  fenced heartbeats during provider streams, bounded multi-turn/tool
  sequencing, protected final-output persistence, terminal classification, and
  conservative `RecoveryRequired` handling for ambiguous provider, resolver,
  and output handoffs.
- UTF-8-safe `AiLiveDeltaCoalescer` primitives enforcing deployment bounds no
  weaker than 50 ms or 4 KiB while excluding tool arguments and other
  structured provider events from visible live batches.
- Immutable fenced run checkpoints and `latest_checkpoint_id` recovery
  binding. Final protected assistant output and its exact redacted checkpoint
  now commit atomically; expired-lease reconciliation can safely finalize that
  proven crash window instead of misclassifying it as an uncertain replay.
- Explicit provider-response continuation and `ModelInputBlock::ToolResult`;
  the OpenAI adapter maps these to Responses `previous_response_id` and
  `function_call_output` only when provider response storage is deliberately
  enabled.
- ORM-backed protected proposal staging with current-principal/scope policy,
  schema/provenance validation, fenced creation, bounded keyset reads,
  schema-revalidated human edits, CAS review, durable session events, and
  trusted post-domain-mutation application/audit linkage.
- ORM-backed exact approval lifecycle with protected canonical previews and
  resource bindings, fenced `WaitingApproval` parking, authenticated bounded
  GraphQL reads/decisions/revocation, optional recent-MFA decisions, fresh
  original-actor rehydration, atomic one-shot consumption, session events, and
  renewed run fencing. Request and consumption also re-resolve the current
  registered supervised-mutation descriptor and exact GraphQL contract.
- Composable `AiProposalQueryRoot`/`AiProposalMutationRoot` and
  `AiApprovalQueryRoot`/`AiApprovalMutationRoot` with coherent optional
  PascalCase naming and fail-closed authentication.
- Full approval action-envelope types binding resources/versions, policies,
  actor/delegation identity, operation contracts, and server-generated
  canonical previews.
- Fresh principal/scope/descriptor/argument-aware tool authorization inside
  the authenticated bridge, JSON Schema 2020-12 argument validation, and
  disclosure-validated runtime result envelopes.
- Optional `graphql-case-pascal` feature for coherent PascalCase resolvers,
  arguments, inputs, outputs, subscriptions, and forwarded ORM fields.
- Repository governance, documentation index, README/changelog/migration
  release-policy enforcement, warnings- and missing-docs-denied CI Rustdoc
  checks, and SemVer enforcement scaffolding.
- Project-agnostic local execution design covering local HTTP model servers and
  allowlisted native/ACP subprocess harnesses without arbitrary shell,
  environment, filesystem, network, or tool authority.
- Explicit supervised provider-plan constructors accepting only enabled exact
  read-only tools and `SupervisedWrite` application mutations with one-shot
  approval and non-secret consequential risk classes.
- `AiCanonicalActionPreviewBuilder`, `AiToolPreauthorization`, and
  `OrmAiConsequentialToolCallService` for server-owned current-state previews,
  protected approval staging, exact consumption, freshly policy-bound ordinary
  resolver execution, protected results, separate egress, and fenced outcomes.
- Durable consequential tool-call bindings for provider/model/response,
  settled budget reservation, correlation/causation, and safe delegation
  references so approval execution can be rebuilt after an interactive wait.

### Changed

- `AiRuntime::execute_tool` now rejects every approval-required descriptor.
  One-shot supervised mutations use `execute_approved_tool`, which recomputes
  current host tool policy and compares its version and authorization-state
  digest before building the normal resolver request context.
- AI schema module version is now `0.10.0`. Existing tool-call history keeps
  nullable new provider/audit fields; a waiting pre-`0.10.0` consequential row
  cannot be resumed and fails closed for reconciliation.
- Approval principal freshness is sampled after asynchronous rehydration,
  avoiding false future-timestamp rejection with sub-second system clocks.

- `AiProviderCallPlan::new_with_tools` now accepts initial turns only and
  rejects pre-populated provider continuation/tool-result input. Exact later
  turns must consume `AiAgentContinuation` through
  `new_continuation_with_tools`.
- `AiRunRecoveryReport` now reports safely finalized output checkpoints in its
  `completed` counter. That checkpoint slice introduced schema module `0.9.0`;
  the current module is `0.10.0`.

- Multi-repository development now uses one owning agent per repository.
  `graphql-orm-ai` agents treat sibling worktrees as read-only, stage ignored
  handoff prompts for upstream owners, and repin only reviewed final upstream
  commits in dependency order.
- Public Git builds now pin the final `graphql-orm` 0.7.0 merge commit and
  `agql-auth` 0.10.0 annotated-tag target instead of requiring an adjacent
  local sibling checkout or an open-PR revision. CI checks out the same exact
  revisions for baseline compatibility verification.
- Crate version is now `0.2.0` because the public budget reconciliation and
  proof-serialization changes are pre-1.0 breaking API changes.
- AI schema module version is now `0.8.0`. In addition to the `0.7.0` tool-call
  changes, proposal rows now persist validated item counts and proposal/
  approval records have deterministic service-owned IDs and stable keyset
  windows required by their authenticated lifecycle services.
- Budget policies/counters now cover
  tool and image units, counters have stable period keys and a unique policy/
  period boundary, and reservations have principal-kind/idempotency uniqueness
  plus complete actual-usage fields.
- Run attempts now receive a separate append-only outcome fact instead of
  relying on mutation of append-only claim history. Egress event IDs are the
  exact policy decision IDs so audit/proof correlation is lossless.
- `AiBudgetService::reconcile` now returns an
  `AiBudgetReconciliationResult`, and `AiError::BudgetDenied` distinguishes
  exhausted capacity from authorization or persistence failures.
- `AiBudgetReservation` and reconciliation results no longer implement Serde
  deserialization; callers obtain validated reservations from a budget service.
- `ProviderRequestContext` now requires an exact `AuthorizedBudgetReservation`
  in addition to egress proofs.
- `AuthenticatedToolBridge` now requires an immutable logical target registry;
  request-context factories receive the validated target, and runtime builders
  require an `AiToolAuthorizationPolicy`.
- `AiRuntime::execute_tool` now requires a registered tool ID and returns an
  `AiToolExecutionResult` only after current policy, resolver, byte/list limit,
  and static disclosure checks succeed.
- `ModelRequest` now has an explicit `continuation` field and its input enum has
  a `ToolResult` variant. Tool results and continuations must occur together.
- `AiProviderCallPlan::new` remains tool-free. The new `new_with_tools` accepts
  only exact explicitly enabled read-only application queries, while
  `new_continuation_with_tools` installs matched result blocks and their exact
  manifests as one unforgeable continuation unit.
- `OrmAiProviderOutputService` rejects a provider turn that still has pending
  custom tool calls instead of prematurely finalizing it as assistant output.
- `AiProposalOutcomeRecorder::record_applied_outcome` now requires the current
  authenticated principal so the ORM service can freshly rehydrate and
  authorize post-mutation linkage. `AiProposalCatalog::descriptor` exposes
  read-only registered metadata and registration now rejects unbounded limits.
- Non-internal tool catalog registration now requires an exact GraphQL
  operation contract and static disclosure schema.
- The opt-in OpenAI smoke-test key file now rejects labels, wrapped values, and
  internal whitespace instead of sending an ambiguous bearer credential.

### Security

- Approval-required descriptors can no longer use the ordinary unapproved
  runtime execution entry point. A consumed proof must match the complete
  rebuilt binding, and fresh policy version/state must still match before the
  resolver is invoked.
- Supervised execution verifies the exact provider turn has a committed,
  reconciled budget reservation before consuming approval. Any resolver
  timeout or post-side-effect persistence/authorization ambiguity terminally
  closes the run as `RecoveryRequired` and is never automatically replayed.

- Tool registration rejects current AI control-plane and GraphQL introspection
  roots, including casing variants, before policy enablement.
- Provider model/output swaps invalidate budget proofs before transport.
- Budget reservation fails closed for stale principal resolutions, tenant
  mismatch, stale/expired run fences, absent policies, invalid counters, and
  partial multi-policy capacity. Uncertain external calls cannot be released by
  the ordinary worker reconciliation path.
- Budget reservation now verifies the active persisted session owner, tenant,
  and exact scope in the same transaction as the run fence and counters.
- A failed egress audit write prevents provider transport and releases only a
  reservation still proven unstarted. An incomplete/erroring provider stream
  retains uncertain budget capacity and is never silently retried.
- Every worker child/terminal write validates run, attempt, generation, owner,
  expiry, state, and row version; pre-provider lease expiry may requeue while
  post-start expiry becomes `RecoveryRequired`.
- Approval changes to resource, policy, schema, document, projection, actor,
  preview, or authorization-state bindings invalidate the grant.
- OpenAI HTTP 401 responses map to the redacted `CredentialUnavailable`
  category instead of a generic provider rejection.
- OpenAI retained-response mode now requires every exact transfer manifest to
  declare `provider_response` retention. The secure default remains
  `store_responses = false`; stateful tool continuation fails closed under that
  default until stateless encrypted continuation is implemented.
- Consequential, proposal, mutation, subscription, approval-required, and
  non-idempotent descriptors cannot enter the implemented read-only loop.
- Proposal acceptance changes only AI-owned staged state and never executes an
  application mutation. Approval consumption proves intent once but still
  requires fresh ordinary resolver authorization and current resource-version
  enforcement before any consequential side effect.

## 0.1.0

Initial release is not yet published. Everything above remains unreleased
until the production gates in `docs/implementation-status.md` are satisfied.
