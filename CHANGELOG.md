# Changelog

All notable user-visible changes are recorded here. The crate follows
Semantic Versioning and keeps migration instructions in [MIGRATION.md](MIGRATION.md).

## [Unreleased]

### Added

- Project-agnostic AI schema module with 36 private persistence entities for
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
- Repository governance, documentation index, migration/release policy,
  warnings- and missing-docs-denied CI Rustdoc checks, and SemVer enforcement
  scaffolding.
- Project-agnostic local execution design covering local HTTP model servers and
  allowlisted native/ACP subprocess harnesses without arbitrary shell,
  environment, filesystem, network, or tool authority.

### Changed

- Public Git builds now pin `graphql-orm` 0.7.0 and `agql-auth` 0.9.0 by exact
  reviewed commit instead of requiring an adjacent local sibling checkout.
  CI checks out the same revisions for baseline compatibility verification.
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
