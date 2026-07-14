# Implementation Status

This file is intentionally explicit about what is a compiled contract versus
production-ready behavior.

## Implemented foundation

- Crate scaffold and SQLite/PostgreSQL/MSSQL compile-time backend selection.
- Project-boundary test rejecting direct SQLx/Tiberius, generic database URLs,
  and known consumer/deployment references from crate source.
- `agql-auth` safe `PrincipalReference`, `ResolvedPrincipal`,
  `CurrentPrincipalResolver`, purpose-bound grant reference, and linked
  invocation audit metadata.
- `graphql-orm` dependency-owned schema-module catalog with module ID, version,
  namespace, fingerprint, backup metadata, and restore-hook declarations.
- `graphql-orm` portable fencing/CAS state contracts and stale-worker tests.
- `graphql-orm` validated Relay-style bidirectional keyset input, portable
  `before` predicates, and generated repository `first/after` plus
  `last/before` connections.
- AI schema-module identity (currently version `0.29.0`) and 39 private records
  spanning provider/model configuration, content/egress/tool/retention/budget
  policy and atomic reservations, sessions, attachments, runs, approvals,
  proposals/items, checkpoints, skills/versions, usage, webhook receipts,
  audit, secret cleanup, egress decisions, and restore readiness.
- Private repository generation for SQLite/PostgreSQL AI records without
  composing or exporting generic internal CRUD roots; MSSQL remains
  schema-only until write parity exists.
- Test-owned PostgreSQL 17 parity through a random, ownership-labeled
  disposable Docker container and unique database. The harness accepts no
  database URL, binds only a Docker-assigned IPv4 loopback port, applies the
  generated module, exercises atomic session/message/run, protected skills,
  hierarchical rules, and keyset behavior, proves stale-fence rejection, and
  verifies ownership again before cleanup.
- Provider-neutral capability/request/event/stream interfaces with validated
  function schemas and separately authorized built-in tools.
- Deterministic mock provider and native feature-gated OpenAI Responses/SSE
  adapter with `store: false` by default, redirects disabled, secret resolution
  immediately before each request, structured output, custom functions,
  built-in web/file/code/image request mapping, typed normalization, usage,
  citations, forward-compatible unknown events, and no hidden reasoning
  persistence. Exact released PNG/JPEG/WEBP/GIF and direct file inputs are
  encoded inline without creating provider-persistent file IDs.
- Native feature-gated Anthropic Messages/SSE adapter with a fixed official
  endpoint/version, just-in-time secret resolution, bounded text/JSON,
  JSON-schema output, registered custom and parallel application tools,
  protected stateless replay, and exact cumulative usage. Attachments,
  built-ins, retained continuation, extended thinking, arbitrary endpoints,
  and prompt-cache creation remain fail-closed.
- Native feature-gated xAI Responses/SSE adapter with a fixed official endpoint,
  just-in-time Bearer credential resolution, bounded text/JSON, JSON-schema
  output, strict custom/parallel application tools, exact provider-kind proofs,
  and zero-data-retention attestation required by default. Ordinary retention
  and retained response-ID continuation require explicit configuration and
  egress authorization; attachments, xAI server tools, encrypted reasoning
  replay, and arbitrary endpoints remain closed.
- Feature-gated OpenAI-compatible Responses/SSE adapter with one immutable,
  deployment-authorized endpoint and an exact GraphQL-managed, versioned
  capability/retention contract. It supports bounded text/JSON and only the
  explicitly declared strict tools, parallel calls, structured output, and
  retained response-ID continuation. Profile/destination/model/retention are
  re-bound to every egress proof; redirects, capability probing, attachments,
  built-ins, and runtime/model-selected URLs remain closed.
- Native feature-gated Ollama `/api/chat` adapter with deployment-authorized
  fixed root endpoint, redirects disabled, bounded NDJSON normalization,
  exact PNG/JPEG/WebP image reopening, JSON-schema output, registered custom
  and parallel application tools, bounded stateless replay, and authoritative
  prompt/evaluation token usage. It explicitly omits thinking and rejects
  files, built-ins, provider-retained continuation, and arbitrary tool IDs.
- Feature-gated installed local-harness foundation with immutable logical-model
  registry, fixed executable/arguments/digest/version/sandbox/resource
  registration, a trusted process-tree launcher seam, strict bounded
  JSON-lines request/event protocol, ordinary provider proof validation, and a
  deterministic fake-process conformance suite. No unsandboxed concrete child
  process launcher is supplied.
- Exact provider request binding: an egress proof cannot be paired with a
  changed provider/model/session/run/payload estimate, every built-in or
  attachment capability requires its own matching authorized transfer, and an
  opaque atomic budget proof must match run/attempt/fence/provider/model/output
  ceiling/expiry before transport.
- Complete provider-neutral metadata bounds and conservative transfer sizing
  cover tools/schemas/built-ins/continuation as well as prompt and exact
  attachment encoding; duplicate or malformed built-in configuration fails
  before egress.
- Concrete SQLite/PostgreSQL ORM budget service: fresh-principal and tenant
  binding, current run-fence validation, bounded policy resolution, atomic
  multi-counter reservation, stable window keys, unique content-bound
  idempotency, bounded serialization retries, exact-once usage reconciliation,
  truthful over-estimate accounting, and conservative uncertain capacity.
- Authenticated budget-policy configuration through the existing GraphQL
  configuration roots: exact-scope bounded reads, deployment-ceiling opt-in,
  recent MFA, immutable targeting/interval bindings, CAS updates, atomic
  redacted audit, and deterministic exact/tenant-wildcard scope keys.
- Authenticated append-only pricing-catalog management through the existing
  configuration roots and a separately installed service: exact
  scope/provider/model reads, globally unique immutable references, recent
  MFA, separate host read/write decisions, deployment rate/cardinality bounds,
  and atomic redacted creation audit. The same ORM service provides
  conservative exact-version quotes and authoritative cached/non-cached token
  settlement with checked integer arithmetic; built-in provider units remain
  deliberately unsupported by the concrete accountant.
- Authoritative usage facts append exactly once in the budget reconciliation
  transaction with a unique reservation binding, exact scope/principal and
  provider/model dimensions, total/cached token separation, units, and settled
  cost. Authenticated GraphQL reporting is exact-principal or exact-scope by
  host policy, redacted, filterable, and bounded by bidirectional keysets.
- In-memory SQLite budget tests prove concurrent calls cannot overspend one
  counter, a later applicable policy rolls the whole reservation back, stale
  principal/fence inputs fail closed, reconciliation is idempotent, and only
  proven unused capacity becomes available again.
- Concrete SQLite/PostgreSQL ORM run service with bounded oldest-first
  queued/retry claims, immutable attempt and unique outcome facts, monotonic
  generations, renewable leases, strict row-version fencing, retry scheduling,
  terminal transitions, and bounded expired-lease reconciliation.
- In-memory SQLite worker tests prove racing workers cannot double-claim,
  heartbeats invalidate old row-version proofs, reclaimed generations fence
  old workers, only pre-provider expiry requeues, post-start expiry requires
  recovery, and terminal/retry outcomes append exactly once.
- Concrete append-only ORM egress decision audit and one-turn provider
  executor: current access is reauthorized, budget is reserved, every exact
  allow/deny decision is persisted before transport, capacity becomes
  uncertain at the transport boundary, normalized streams are bounded, and
  deployment-owned immutable-version pricing settles authoritative usage
  exactly once.
- Fenced protected assistant-output persistence with current principal/access/
  protection revalidation, exact result-to-lease binding, UTF-8-safe block
  splitting, bounded previews, and atomic message/block/session-event/run-fence
  commit. The end-to-end mock path covers claim through terminal completion.
- Fenced read-only application-tool execution with exact catalog/policy/model
  definition binding, bounded normalized calls, protected pre-execution
  arguments, current ordinary GraphQL resolver authorization, static result
  disclosure, a separately authorized and immutably audited result transfer,
  protected result/event persistence, and lease renewal.
- Bounded provider continuation that binds the exact prior response, every
  opaque call ID, durable model-visible result, and immutable egress manifest.
  The OpenAI adapter requires explicit retained-response configuration and
  matching retention manifests for stateful continuation.
- Provider-independent bounded stateless continuation for native Ollama and
  reviewed installed harnesses. It retains only trusted instructions, visible
  text/JSON, exact assistant calls, and disclosure-validated tool output;
  requires one unique freshly authorized manifest per replayed result; and
  protects checkpoints for same-generation consumption or exact
  cross-generation current-authority adoption.
- Top-level read-only coordinator with host-owned exact initial/continuation
  planning, periodic fenced provider heartbeats, bounded loop/tool sequencing,
  protected output persistence, safe terminal classification, and conservative
  recovery-required closure for ambiguous provider/tool/output handoffs.
- Immutable run checkpoints linked from the current run. Protected final
  assistant output and its exact checkpoint commit together, allowing expired
  recovery to safely finalize only that proven post-output/pre-terminal crash
  window while malformed or other active phases remain closed.
- Protected provider-turn and exact completed-tool-batch checkpoints with
  current principal/policy revalidation, bounded protected state, committed
  budget verification, complete tool/result/egress transaction checks, and
  coordinator-required persistence before phase handoff.
- Cross-generation adoption for exact completed provider-retained and bounded
  stateless read-only tool batches. The ORM adopter reopens and validates every
  current and historical protected argument/result, committed budget, ordered
  tool/step row, disclosure classification, and immutable egress allow audit
  under current authority, reconstructs bounded continuation state without
  rerunning a resolver, and consumes the checkpoint before provider transport.
- Optional protected durable visible provider output. UTF-8-safe coalescing
  enforces a maximum 50 ms / 4 KiB batch and excludes structured/tool events;
  the ORM sink freshly validates authority and protection policy, then commits
  a provisional cursor event through the exact active fence and uncertain
  budget before wakeup.
- Owner-isolated attachment intake over the exact pinned
  `graphql-orm-storage` `BlobStore`: hashed expiring one-time tickets, current-
  owner streaming upload, random scope-bound quarantine/final keys, exact
  size/hash comparison, complete-object scanner attestation, separate
  fail-closed acceptance, promotion, protected session events, explicit clean
  release, bounded metadata, and safe unlinked removal.
- Host-only bounded attachment cleanup with expiry-aware candidate selection,
  fresh CAS reload, monotonic generations, reclaimable leases, confirmed
  idempotent exact-reference deletion, redacted audit, capped retry backoff,
  legacy interrupted-state handling, and concurrent-worker tests.
- Provider-neutral exact attachment reopening with private-field request and
  resolved payloads, deployment raw-byte/cardinality limits, current owner/
  session/scope checks, released/clean/message-linked enforcement, object
  length/hash validation, durable row recheck around storage I/O, complete
  provider-context coverage, and post-I/O principal reauthorization before
  transport becomes uncertain.
- Secret-store contract plus explicit, read-only, allowlist-mapped environment
  bootstrap store. Runtime construction now requires a secret store.
- Per-scope content-protection policy/envelope/protector contracts with a
  fail-closed database-managed implementation and authorized policy resolver.
  Runtime construction now requires both the resolver and protector.
- Redacted authenticated GraphQL configuration roots for provider profiles,
  credential set/rotate/remove, and content-protection policy. Credential and
  protection mutations explicitly require service-level CAS, redacted audit,
  administrative authorization, and recent MFA.
- Default-deny tool catalog, fingerprint-bound policies, rollout maturity
  caps, JSON Schema 2020-12 argument validation, and current principal/scope/
  descriptor/argument-aware authorization inside the authenticated bridge.
- Exact local/private-routed/private-direct logical GraphQL target registry
  with audience/resource and schema/document/projection/disclosure bindings;
  the model never receives a target URL or credential.
- Generic private remote GraphQL adapter with complete exact-operation
  delegation requests, freshness/expiry enforcement, redacted non-serializable
  authority, deployment-owned issuer/transport seams, logical target routing,
  routed/direct parity requirements, and request-swap/expiry conformance tests.
- Static recursive result-disclosure schemas with closed objects, bounded
  lists, `NeverExport` nodes, classification tightening, stable fingerprints,
  and runtime enforcement before tool results leave the execution boundary.
- Full action-envelope approval domain contract binding tool/argument,
  principal/delegation, logical target/schema/document/projection/disclosure,
  resources/versions, policy/auth-state, canonical preview, expiry, and
  one-shot consumption identity.
- Concrete protected proposal staging/review service and authenticated GraphQL
  roots: fresh policy, fenced creation, schema/provenance validation, keyset
  windows, CAS accept/edit/reject, protected session events, and freshly
  authorized post-domain-mutation outcome/audit linkage.
- Concrete canonical-preview approval service and authenticated GraphQL roots:
  protected resource/preview envelopes, exact fenced run/tool parking,
  optional recent-MFA decision, revocation, current original-actor
  rehydration, atomic one-shot consumption, protected events, and renewed
  running fences.
- Supervised provider-plan exposure and a concrete ORM consequential mutation
  service: exact current tool preauthorization, deployment-provided canonical
  preview generation, protected restart bindings, committed-budget validation,
  one-shot consumption, fresh policy version/state comparison before ordinary
  resolver execution, protected/static-disclosed results, separate result
  egress, renewed fences, and recovery-required closure for ambiguous effects.
- Explicit egress manifests, deployment boundary, policy decision, and
  allowed-manifest proof.
- JSON Schema 2020-12 structured proposal registry and provenance validation.
- Protected immutable skill catalog with exact-scope bounded reads,
  recent-MFA/CAS publication and enablement, same-transaction redacted audit,
  strict versioned policy JSON, protected instructions, canonical checksums,
  exact tool/UI-intent fingerprints, and authority-free runtime resolution.
- Default-deny logical UI-intent catalog with bounded JSON Schema 2020-12
  validation and exact descriptor fingerprints. Validated values are frontend
  suggestions only and contain no route or executable authority.
- Fenced `OrmAiUiIntentDeliveryService`: exact completed tool-free provider
  envelopes, ordered response/usage identity checks, exact schema binding,
  current authority before and after protection, matching committed budget,
  atomic protected session/principal-inbox events, redacted audit, renewed
  fence, idempotent retry, and fatal restore evidence. It performs no route or
  resource action.
- Canonical host request-context/executor contracts and current-principal tool
  bridge. Registered tool execution now returns a bounded
  `AiToolExecutionResult` only after fresh tool policy, ordinary resolver
  execution, and static disclosure validation.
- Fail-closed runtime builder and restore/start readiness gate.
- Pure fenced run-state and side-effect-safe restore reconciliation planning.
- Initial authenticated `AiQueryRoot`/`AiMutationRoot` session contract with
  bounded session/message/block/event reads and lifecycle/message operations
  over an owner/scope-aware service trait.
- Concrete SQLite/PostgreSQL ORM-backed session service using only generated
  repository/transaction APIs: principal-kind-aware owner isolation, separate
  message/event sequence heads, protected preview/content/event envelopes,
  content-bound client idempotency, atomic message+block+queued-run+event
  persistence, attachment ownership/quarantine checks, CAS archive/restore/
  delete, and bounded keyset/event/block reads.
- Concrete SQLite/PostgreSQL ORM-backed configuration service: host-owned admin
  policy, recent-MFA enforcement, endpoint SSRF policy seam, provider-profile
  and content-policy CAS, same-transaction redacted audit append, fresh-reference
  credential rotation with compensation, and durable obsolete-secret cleanup.
- In-memory SQLite service tests cover owner isolation, idempotent atomic send,
  windowed reads, lifecycle CAS, recent MFA, stale-version rejection, endpoint
  policy, credential rotation/removal, and content-protection readiness.
- `AiSubscriptionRoot` and a concrete SQLite/PostgreSQL durable session-event
  subscription service: receiver-before-replay race avoidance, bounded replay
  to a captured watermark, commit-only wakeup hints, database re-reads after
  wake/lag, explicit reset signaling, and periodic principal rehydration plus
  session/scope reauthorization.
- Exact-principal cross-session inbox streams with same-transaction
  session/message/assistant-output notifications, protected bounded payloads,
  catch-up pages, receiver-before-replay subscriptions, lag recovery, explicit
  reset signaling, and periodic current-principal plus referenced-session/scope
  reauthorization.
- GraphQL-managed, recent-MFA/CAS/audit-protected retention settings and a
  bounded ORM inbox-pruning worker. Pruning reads captured scope identities and
  current policies in the deletion transaction, preserves a recent-event
  floor, deletes only a contiguous expired prefix, never rewinds the stream
  head, and fails closed for absent/legacy policy.
- Host-only bounded session retention using generated ORM keysets and
  state-machine transactions. The exact current scope policy gates deletion of
  expired provisional live deltas and scrubbing of terminal unattached message
  preview/blocks; metadata tombstones remain windowable, event gaps request a
  client reset, and every changed session appends redacted audit atomically.
- Project-neutral hierarchical rule management and runtime resolution through
  generated ORM operations. Host-derived application/tenant-project/user
  lineages intersect immutable deployment ceilings and every explicit exact
  scope across tool fingerprints, disclosure/maturity, providers/capabilities,
  approvals, retention/BYOK, and budgets. Reads, management, and resolution are
  separately authorized; writes require recent MFA/CAS/audit; missing,
  cross-tenant, corrupt, stale, or widening layers fail closed. A resolved rule
  set is constraint evidence and grants no ordinary authority.
- Mandatory read-only coordinator rule binding through a double-rehydrating
  `OrmAiCurrentRuleResolver`. Plans are checked before provider egress and
  results/tools are checked afterward; protected checkpoint v2 carries the
  exact fingerprint and cumulative provider/step/time/token/cost/tool/image
  usage. Adoption rejects changed lineages, exceeded budgets, and legacy
  checkpoints without weakening ordinary budget/egress/resolver authority.
- Restart-safe approved-wait worker handoff through
  `OrmAiRunService::claim_next_approved`: approval/run state and the current
  owner/row-version fence rotate atomically without changing the staged
  attempt/generation. Concurrent claims are tested and the claim grants no
  approval consumption or resolver authority.
- Protected same-attempt resumption for one provider-retained supervised
  mutation. The resume service validates the exact pre-wait provider
  checkpoint, committed budget, current rules, `resume_claimed` approval,
  staged tool and route; executes through fresh ordinary resolver
  authorization; then writes a distinct consumed-approval-bound continuation
  checkpoint. Read-only checkpoint append/adoption structurally rejects write
  and approval-bearing tool rows.
- Optional coherent `graphql-case-pascal` contract covering roots, arguments,
  inputs, outputs, subscriptions, enums, and forwarded generated ORM fields
  without lowercase aliases.
- Root README, documentation index/guides, changelog, migration guide,
  repository rules, release-policy script, SemVer CI, and warnings-denied
  Rustdoc/SDL checks.

## Not yet production-ready

- Consumer-owned migration validation and production deployment acceptance.
  The crate's disposable PostgreSQL parity harness is implemented, but it does
  not substitute for a consumer's schema composition/restore rehearsal.
- Complete deleting-session, raw-provider/tool payload, audit, attachment/blob,
  and provider-persistent-file retention workflows. Principal-inbox pruning
  and bounded per-session provisional-delta/message-content pruning are
  implemented; their reports do not claim complete erasure.
- Application-encrypted field/keyring and production mutable secret-store
  implementations. Database-managed protection and the safe service seams are
  implemented.
- Attachment quotas, derivative artifacts, retention purge, and
  provider-persistent file upload/search/deletion. Core ticketed quarantine/
  scan/promotion/release, exact ephemeral provider reopening, OpenAI inline
  image/file input, and expired/interrupted exact-reference cleanup are
  implemented.
- Top-level supervised coordinator for decision classification, protected
  continuation consumption, and the remaining provider loop. One claimed
  provider-retained mutation now has exact pre-wait adoption and post-mutation
  checkpointing, but multi-call/stateless/cross-generation supervised
  adoption remains closed; the read-only coordinator cannot route mutations.
- Per-item proposal review and application-specific proposal rendering. Whole
  structured payload accept/edit/reject and trusted post-mutation outcome
  linkage are implemented.
- OpenAI background/webhooks, provider-persistent file upload/search/deletion,
  richer provider file-type preflight, and full built-in result normalization.
  Exact inline image/file input is implemented and remains independently gated
  by host MIME policy, budget, egress, current authority, and reopening limits.
- Provider webhooks/background processing.
- Privileged uncertain-call
  recovery, retention/purge, and telemetry sinks. Budget-policy management,
  ordinary transactional reservation/reconciliation, and authenticated usage
  reporting are implemented.
- Cross-generation adoption for validated provider-turn or partially completed
  application-tool checkpoints. Exact completed provider-retained and bounded
  stateless read-only tool-batch adoption, the bounded coordinator, protected
  checkpoints/continuation, protected live output, and final-output crash
  reconciliation are implemented; all ambiguous resume remains closed.
- Backup adapter execution and applied restore transactions.
- Resolver-operation disclosure metadata generation and complete schema-aware
  control-plane recursion validation. The current catalog uses explicit
  reviewed operation contracts, disclosure schemas, and a fail-closed
  identifier scanner.
- Deployment-specific delegated-credential issuers and private HTTP GraphQL
  transports. The generic exact-binding adapter is implemented; credential
  format, fixed destination mapping, network isolation, and application audit
  integration intentionally remain host-owned.
- A production OS/container implementation of the trusted local-harness
  launcher and optional ACP framing. Ollama custom tools and JSON-lines v2
  harness tools now use the
  bounded stateless contract; no model may choose command, arguments, working
  directory, environment, mount, or network authority.
- Any consumer integration testing or migration. That work is explicitly left
  to each consumer project/agent.

## Next implementation slice

1. Continue the bounded deleting-session/raw-payload/audit retention workers
   without exposing private ORM records or accepting generic database URLs.
2. Add cross-generation adoption for the protected supervised tool-batch,
   then build the top-level approval-wait coordinator that consumes it and
   resumes the full provider loop under fresh rule/fence/current-principal
   guarantees.

A production OS/container local-harness launcher remains deployment-owned. A
generic `Command` implementation in this crate could not prove immutable-image
digest verification, mount/network isolation, cgroups, descendant cleanup, or
the absence of inherited authority, so the public trusted launcher seam stays
intentional.

## Current verification

- `cargo test --features provider-openai,provider-anthropic,provider-xai,provider-ollama,provider-openai-compatible,local-harness`:
  full SQLite, OpenAI/Anthropic/xAI/compatible mocks, and native Ollama loopback-mock
  coverage passed; one explicit live-provider test remained ignored.
  Deterministic installed-harness process conformance and generated private-ORM
  doctests were included; the latter remained intentionally ignored.
- `cargo clippy --all-targets --features provider-openai,provider-anthropic,provider-xai,provider-ollama,provider-openai-compatible,local-harness -- -D warnings`:
  passed.
- Warnings-denied Rustdoc passed for all native and profiled provider adapters and
  `graphql-case-pascal`.
- PascalCase SDL contract test passed with no camelCase aliases.
- `cargo check --no-default-features --features postgres`: passed, compile-only.
- Test-owned PostgreSQL 17 migration/session/keyset/fencing parity passed; the
  ownership-labeled container and unique database were removed afterward.
- `cargo check --no-default-features --features mssql`: passed, schema-only.
- The mutually exclusive backend features intentionally cannot be checked with
  Cargo `--all-features` in one build.

## Provider test note

- Mocked OpenAI, Anthropic, and xAI HTTP/SSE and all other automated tests pass
  without credentials.
- The synthetic live OpenAI smoke test is explicit opt-in and is not part of
  automated verification. Its key-file loader requires exactly one unwrapped
  credential and never logs the value or provider response body.
