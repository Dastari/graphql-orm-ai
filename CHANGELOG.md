# Changelog

All notable user-visible changes are recorded here. The crate follows
Semantic Versioning and keeps migration instructions in [MIGRATION.md](MIGRATION.md).

## [Unreleased]

This development line advances the pre-1.0 crate version to `0.23.0`. The AI
schema module remains `0.22.0` because this provider-only change adds no
persistent entity or semantic change.

### Added

- A feature-gated native Anthropic Messages/SSE adapter fixed to the official
  HTTPS endpoint and API version. It resolves API keys just before transport,
  requires exact egress and atomic budget proofs, and supports bounded
  streaming text/JSON, strict custom and parallel application tools,
  protected stateless continuation, and JSON-schema structured output.
  Provider-retained continuation, attachments, provider built-ins, extended
  thinking, and prompt-cache creation remain fail-closed. Anthropic cache-read
  tokens are included in total input and retained as the cached subset; an
  unexpected cache write is rejected because its distinct billing class is
  not yet represented by the authoritative pricing ledger.

- Provider-independent bounded stateless application-tool continuation through
  `ModelContinuationMode`, protected `StatelessConversation` history, exact
  assistant/tool identity, and domain-separated continuation-chain hashes.
  Every historical and current tool output requires its own unique freshly
  authorized `ToolResult` manifest; hidden thinking, attachments, provider
  built-ins, arbitrary roles, and model-authored instructions cannot enter the
  replay format.
- Native Ollama custom and parallel application-tool calls with exact
  provider-name/local-ID/fingerprint mapping, native message/tool-result
  replay, bounded normalized calls, and no provider-retained response state.
- Installed local-harness JSON-lines protocol v2, including exact stateless
  history/tool definitions and a start/delta/complete tool-event state machine
  restricted to server-offered IDs. Registrations may opt into custom tools
  only together with stateless continuation; filesystem, network, shell,
  credential, built-in, and provider-retained authority remain unavailable.
- Protected stateless tool-batch checkpoints and cross-generation adoption
  using the existing generated ORM entities. A replacement fence can continue
  only after reopening the protected conversation and proving every historical
  and current tool call against its original attempt/generation, committed
  budget, run step, protected arguments/result, disclosure classification,
  immutable egress decision, and unique replay manifest. Resolver calls are
  never rerun during adoption, and any missing, swapped, duplicated, tampered,
  or newly denied evidence remains closed for recovery.

- A host-only `AiSessionRetentionService` and generated-ORM
  `OrmAiSessionRetentionService` for bounded keyset scan cycles. Each session
  transaction reloads its exact current GraphQL-managed scope policy, deletes
  only expired provisional `provider_live_delta` events, and scrubs protected
  preview/block content only from finalized messages whose producing run is
  terminal and which have no linked attachment. Nonterminal, attached,
  corrupt, unconfigured, or concurrently changed state fails closed.
- Explicit retained-message tombstones through `AiMessageView::content_purged`.
  Authorized message windows retain metadata and a fixed server-authored
  preview, while block reads return an empty window without opening protected
  data. Session-event windows now signal `reset_required` when selective delta
  retention creates a sequence gap.
- Same-transaction redacted retention audit, bounded/idempotent SQLite tests,
  and fatal restore evidence for inconsistent message tombstones or retention
  gap classification.

- An append-only, GraphQL-managed immutable pricing catalog with exact
  scope/provider/model bindings, globally unique version references,
  integer-only fixed/input/cached-input/output token rates, deployment hard
  bounds, per-route version caps, recent MFA, separate host read/write
  decisions, and same-transaction redacted audit.
- `OrmAiPricingService` as both a conservative exact-version preflight quote
  service and authoritative token-only provider usage accountant. Cached input
  is priced as a subset, estimates assume non-cached input, arithmetic is
  checked and rounded conservatively, and provider built-ins fail closed until
  authoritative billable-unit catalogs are implemented.
- Authoritative provider usage observations now retain the exact application
  scope from their budget plan, so settlement rejects cross-scope pricing
  references as well as provider/model/version swaps.
- Fatal restore evidence for corrupt immutable pricing references, scope/route
  bindings, rates, or creator-audit linkage.

- Authenticated GraphQL budget-policy reads and recent-MFA-protected CAS
  create/update/enable/disable through the existing configuration service.
  Deployment management limits cap every configurable dimension and policies
  per exact scope; scope/principal/interval bindings are immutable, and every
  mutation appends a redacted audit fact in the same transaction.

- An append-only authoritative usage ledger written exactly once in the same
  state-machine transaction that commits a budget reservation. Each fact has a
  unique reservation binding, exact scope and principal kind/subject,
  provider/model, total and cached input tokens, output/tool/image units, and
  settled cost; idempotent reconciliation cannot duplicate it.
- Authenticated `aiUsage`/`AiUsage` reporting through the ordinary query root
  and a separately composable usage root. Host policy grants either exact
  current-principal or exact-scope visibility, and reads use bounded
  bidirectional keysets with provider/model and bounded time filters without
  exposing prompts, transcripts, tool content, pricing rules, or counter
  internals.
- Public `ModelRequest::conservative_egress_bytes` so host planners can bind
  manifests to the same complete metadata and attachment-encoding ceiling the
  provider boundary enforces.

- An optional installed local-harness foundation with immutable
  deployment-owned logical-model registrations, fixed absolute executable and
  arguments, mandatory executable digest/sandbox/resource contracts, a trusted
  process-launcher seam, and a bounded JSON-lines v2 provider driver. The safe
  protocol supports text/structured output plus opt-in stateless application
  tools, and its deterministic fake process suite covers fixed launch facts, environment/command
  non-injection, framing, stderr/output limits, cancellation cleanup, swapped
  budget/model proofs, secret non-persistence, and unoffered tool events.
- A native feature-gated Ollama `/api/chat` adapter for bounded NDJSON text
  streaming, exact ephemeral PNG/JPEG/WebP image input, JSON-schema structured
  output, and authoritative prompt/evaluation token usage. Deployment endpoint
  policy remains mandatory, redirects and URL credentials are forbidden, and
  local execution does not bypass exact egress or atomic budget proofs.
- Durable exact-principal cross-session inbox sequencing with protected
  lifecycle/message/assistant-output events committed atomically with their
  source state, bounded catch-up pages, receiver-before-replay subscriptions,
  lag recovery, explicit retention-gap reset, and periodic current-principal
  reauthorization.
- GraphQL-managed, recent-MFA/CAS/audit-protected scope retention settings,
  including explicit inbox age and recent-event-floor bounds. The host-only
  `OrmAiInboxPruningService` deletes only an expired contiguous prefix,
  serializes with appends through the stream CAS, never rewinds sequence heads,
  and reports missing policies or concurrent conflicts without unsafe deletion.

- Exact, provider-neutral attachment reopening through
  `AiProviderAttachmentResolver`, private-field request/resolved payload types,
  deployment raw-byte/cardinality limits, and the ORM attachment service. The
  resolver rechecks current owner/session/scope and released/clean/linked state,
  streams only the exact opaque object, verifies metadata/length/SHA-256, and
  detects durable row changes around storage I/O.
- Native OpenAI Responses image/file input using ephemeral inline data URLs.
  Supported image MIME types map to `input_image`; other accepted files map to
  `input_file` under the provider's per-request raw-file limit. No provider file
  ID, storage URL, or provider-side cleanup obligation is created.
- Host-only `AiAttachmentCleanupService` and ORM implementation for bounded
  expired-ticket, interrupted upload/removal, and orphan-reference cleanup.
  Claims use monotonic generations, expiring row-version fences, confirmed
  idempotent blob deletion, durable redacted audit, and bounded retry backoff.
- Configurable upload-processing and cleanup claim lifetimes, plus a redacted
  per-pass cleanup report suitable for deployment telemetry.
- Project-agnostic AI schema module with 39 private persistence entities for
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
- A provider-neutral `AiRemoteAuthenticatedGraphqlAdapter` for private routed
  or direct GraphQL targets, plus redacted exact delegation requests,
  deployment authority-issuer and transport seams, logical-route conformance
  fixtures, short-lived secret handling, and current-principal freshness
  limits without embedding a router or federation product.
- `OrmAiCoordinatorCheckpointService` and `AiAgentCheckpointWriter` for
  protected, size-bounded, freshly authorized, fenced provider-turn and exact
  completed-tool-batch checkpoints. The same transaction verifies committed
  provider usage and every protected/egress-audited tool row before advancing
  the run's latest checkpoint.
- `AiAgentCheckpointAdopter` and opaque `AiAdoptedReadOnlyToolBatch` proofs for
  current-authority cross-generation adoption of exact completed read-only
  tool batches. Adoption reopens protected arguments/results, validates the
  original budget, tool, step, disclosure and egress records, reconstructs the
  bounded loop guard, and atomically consumes the checkpoint before transport.
- `AiLiveDeltaSink`, its private-field exact persistence context, and
  `OrmAiLiveDeltaService` for optional protected durable provisional model
  output. The provider executor coalesces only visible text and reasoning
  summaries, applies sequential persistence backpressure, and appends a
  cursor-addressable session event only after fresh authority, scope,
  protection-policy, run-fence, and uncertain-budget validation.
- Owner-isolated attachment intake built on the exact reviewed
  `graphql-orm-storage` `BlobStore`: bounded one-time upload tickets,
  current-owner streaming upload, random scope-bound quarantine/final keys,
  exact byte/hash attestations, trusted full-object scanning, separate
  fail-closed host acceptance policy, conditional promotion, protected durable
  events, bounded metadata reads, finalization, and unlinked-object removal.
- Composable `AiAttachmentQueryRoot`/`AiAttachmentMutationRoot` with coherent
  optional PascalCase naming. Large bytes never pass through GraphQL JSON and
  raw upload tokens, token hashes, blob keys, checksums, and scanner internals
  are not returned in ordinary attachment views.

### Changed

- Budget policies now carry an indexed deterministic non-secret `scope_key`.
  Runtime reservation queries cover the exact tenant scope plus an explicitly
  tenant-wildcard scope, validate every stored binding, and remain default-deny
  for absent, corrupt, or excessive policy sets.
- `ai_scope_key` is now backend-neutral and available to schema-only/MSSQL
  builds as well as SQLite/PostgreSQL migration and configuration code.
- The crate root no longer accidentally reexports macro-generated private ORM
  record inputs, filters, mutations, subscriptions, or repositories. Supported
  public persistence API remains `AiSchemaModule` and its module constants;
  consumers continue through authenticated service/GraphQL contracts.

- Provider-neutral request validation now bounds instruction/text/JSON/output-
  schema/custom-tool-schema metadata, output-token ceilings, and built-in tool
  cardinality/configuration, with a 64-MiB aggregate metadata ceiling. Built-in
  kinds and their domain/store values must be unique and structurally valid.
  Exact egress byte estimates now include
  the complete serialized request plus attachment transfer encoding, so tool,
  schema, continuation, and built-in metadata cannot escape the authorized
  transfer ceiling.
- `ProviderKind` and the GraphQL `AiProviderKindInput` add `LocalHarness` with
  stable value `local_harness`; provider-profile GraphQL may enable and route a
  logical installed profile but cannot configure its process registration.
  The new `local-harness` feature exports the registry, provider, protocol
  driver, process boundary, limits, and non-sensitive transport errors.
- `provider-ollama` enables the optional HTTP/Base64 dependencies and exports
  `OllamaProvider`/`OllamaProviderConfig`. It reports only implemented
  capabilities: native stateless custom tools are supported, while provider
  built-ins, files, provider-retained continuation, and hidden thinking fail
  closed.
- AI schema module `0.16.0` adds principal inbox stream heads, exact
  principal-sequence uniqueness, captured event scope identity, and nullable
  migration-gated inbox fields plus a stable scope key on retention policies.
  Session/provider-output writes now append the applicable inbox event in the
  same transaction.
- `AiConfigurationService` and the configuration GraphQL roots now include
  retention query/mutation contracts. This is a pre-1.0 public Rust and
  GraphQL SDL change requiring host service implementations to add both
  methods and configuration policies to handle `ReadRetention` and
  `ManageRetention`.

- `AiProviderCallExecutor` optionally accepts the exact attachment resolver and
  validated reopening limits. Attachment turns fail before transport when it
  is absent, and current scope/session access is checked again after storage
  I/O before budget state becomes uncertain.
- Provider payload estimation now conservatively includes Base64 expansion for
  attachment bytes, and request validation rejects duplicate attachment IDs.
  Existing server-authored manifests may need larger `estimated_bytes` bounds.
- The OpenAI adapter now advertises image/file input capability when
  `provider-openai` is enabled; that feature also enables its private optional
  Base64 dependency.
- AI schema module version is now `0.15.0`. Attachment rows add nullable
  processing/cleanup deadlines, cleanup generation, retry count and next
  attempt metadata; lifecycle state fields are now privately filterable for
  bounded maintenance queries.
- `ModelInputBlock::Attachment` now requires exact verified `byte_count` and
  lowercase `sha256`. Provider request validation rejects malformed/oversized
  attachment blocks and accounts the full attachment bytes instead of only
  the opaque ID/MIME metadata.
- AI schema module `0.14.0` introduced attachment metadata supporting
  durable pending uploads with nullable object facts, hashed expiring one-time
  capabilities, expected size, scanner/policy versions, and redacted rejection
  state. Existing finalized attachment rows remain representable.
- The dependency universe now pins `graphql-orm-storage` 0.5.0 at its reviewed
  full Git revision with default backend features disabled.
- `AiProviderCallExecutor` can opt into durable live output with
  `with_live_delta_sink`. Visible batches are bounded to no weaker than 50 ms
  or 4 KiB and are committed before a subscription wakeup. The default remains
  no provisional event persistence.
- AI schema module `0.13.0` introduced the persistent meaning of the
  protected `provider_live_delta` session-event type. Entity shape and public
  GraphQL SDL are unchanged.
- `AiReadOnlyAgentCoordinator::new` now requires both an
  `AiAgentCheckpointWriter` and `AiAgentCheckpointAdopter`. Accepted provider
  results are checkpointed before tool/output consumption, and a completed
  tool batch is checkpointed before the next continuation plan. Exact adopted
  batches are consumed before the following provider call. Any checkpoint or
  adoption ambiguity closes the run for recovery.
- AI schema module `0.11.0` added nullable,
  private `protected_state`. Existing final-output checkpoints remain valid
  with no protected state, while older active runs gain no inferred resume
  authority.
- AI schema module `0.12.0` introduced the persistent semantics of
  checkpoint adoption eligibility, cross-generation checkpoint retention, and
  one-shot pre-provider consumption. The entity shape is unchanged.
- `GraphqlRequestContextFactory::build` now receives the complete validated
  `ToolGraphqlRequest` instead of only `GraphqlInvocationContext`, allowing a
  remote issuer to bind delegated authority to the exact server-authored
  operation, canonical variables, projection, disclosure, and audit context.
- `AiRuntime::execute_tool` now rejects every approval-required descriptor.
  One-shot supervised mutations use `execute_approved_tool`, which recomputes
  current host tool policy and compares its version and authorization-state
  digest before building the normal resolver request context.
- The supervised-tool slice introduced AI schema module `0.10.0`. Existing
  tool-call history keeps nullable provider/audit fields; a waiting
  pre-`0.10.0` consequential row cannot be resumed and fails closed for
  reconciliation. The current module is `0.19.0`.
- Approval principal freshness is sampled after asynchronous rehydration,
  avoiding false future-timestamp rejection with sub-second system clocks.

- `AiProviderCallPlan::new_with_tools` now accepts initial turns only and
  rejects pre-populated provider continuation/tool-result input. Exact later
  turns must consume `AiAgentContinuation` through
  `new_continuation_with_tools`.
- `AiRunRecoveryReport` now reports safely finalized output checkpoints in its
  `completed` counter. That checkpoint slice introduced schema module `0.9.0`;
  the current module is `0.19.0`.
- `AiRunRecoveryReport` adds `checkpoint_requeued`. Expired `Running` attempts
  requeue only an exact hash-bound, committed, complete tool-batch checkpoint;
  provider-turn, partial, malformed, consumed, or exhausted adoption attempts
  remain closed as `RecoveryRequired`.

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

- Attachment contents are never resolved from a model-visible storage
  reference. Exact budget and audited egress proofs precede reopening; resolved
  bytes remain redacted from `Debug`, are rebound to request metadata, and are
  accepted by provider context only with complete one-to-one request coverage.
  A missing resolver, changed row/object, owner mismatch, changed checksum, or
  lost authorization releases an unstarted reservation and prevents transport.
- Expired/interrupted attachment cleanup never lists arbitrary storage
  prefixes, reads content, or trusts a stale row. Every candidate is reloaded
  and CAS-claimed; ambiguous deletion retains opaque references and moves to
  bounded backoff, while expired leases are safely reclaimable.
- Every image/file attachment capability proof must contain the exact canonical
  versioned user-provided source reference returned by
  `ModelInputBlock::attachment_egress_reference`. It binds ID, byte count,
  detected MIME and SHA-256; swapping any fact or reusing a broader capability
  manifest is rejected before provider transport.
- Attachment filenames are display-only sanitized metadata and never storage
  paths. Ticket plaintext is returned once, redacted from `Debug`, never
  serialized by Rust APIs, stored only as SHA-256, compared in constant time,
  and insufficient without the current authenticated owner. Scanner or policy
  failure never releases bytes; linked transcript attachments cannot be
  removed through the unlinked-upload mutation.
- Durable live output never receives raw provider frames, tool arguments,
  structured tool events, or hidden reasoning. Every batch is reauthorized and
  protected before a serializable transaction validates the exact active run
  fence and uncertain budget reservation. A sink failure after transport keeps
  provider usage uncertain and must not trigger replay. Provisional events are
  historical progress, not proof of final assistant completion.
- Durable coordinator checkpoints rehydrate the principal and re-resolve an
  unchanged ready protection policy around asynchronous protection. Payloads
  bind the exact attempt/generation, provider result, loop counts, scope,
  result route, completed tool outputs/manifests, and continuation. They are
  persistence proofs only. Only a complete read-only tool batch can become an
  adoption proof, after fresh reopening and durable-record validation; provider
  turns and partial batches remain non-replayable.
- Remote GraphQL execution now rejects local/unregistered targets, stale or
  expired principals, expired delegated authority, changed documents or
  canonical variables, changed operation/projection/disclosure/audit bindings,
  contexts from another adapter, and recursive AI/introspection operations
  before private transport. Incoming bearer tokens and target URLs are not
  accepted by the adapter contract.
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
