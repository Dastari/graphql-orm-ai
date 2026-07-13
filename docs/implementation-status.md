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
- AI schema-module identity (currently version `0.15.0`) and 37 private records
  spanning provider/model configuration, content/egress/tool/retention/budget
  policy and atomic reservations, sessions, attachments, runs, approvals,
  proposals/items, checkpoints, skills/versions, usage, webhook receipts,
  audit, secret cleanup, egress decisions, and restore readiness.
- Private repository generation for SQLite/PostgreSQL AI records without
  composing or exporting generic internal CRUD roots; MSSQL remains
  schema-only until write parity exists.
- Provider-neutral capability/request/event/stream interfaces with validated
  function schemas and separately authorized built-in tools.
- Deterministic mock provider and native feature-gated OpenAI Responses/SSE
  adapter with `store: false` by default, redirects disabled, secret resolution
  immediately before each request, structured output, custom functions,
  built-in web/file/code/image request mapping, typed normalization, usage,
  citations, forward-compatible unknown events, and no hidden reasoning
  persistence.
- Exact provider request binding: an egress proof cannot be paired with a
  changed provider/model/session/run/payload estimate, every built-in or
  attachment capability requires its own matching authorized transfer, and an
  opaque atomic budget proof must match run/attempt/fence/provider/model/output
  ceiling/expiry before transport.
- Concrete SQLite/PostgreSQL ORM budget service: fresh-principal and tenant
  binding, current run-fence validation, bounded policy resolution, atomic
  multi-counter reservation, stable window keys, unique content-bound
  idempotency, bounded serialization retries, exact-once usage reconciliation,
  truthful over-estimate accounting, and conservative uncertain capacity.
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
- Cross-generation adoption for exact completed read-only tool batches. The
  ORM adopter reopens and validates original protected arguments/results,
  budget, ordered tool/step rows, disclosure blocks and immutable egress allow
  audits under current authority, reconstructs bounded continuation state, and
  consumes the checkpoint before provider transport.
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
- Optional coherent `graphql-case-pascal` contract covering roots, arguments,
  inputs, outputs, subscriptions, enums, and forwarded generated ORM fields
  without lowercase aliases.
- Root README, documentation index/guides, changelog, migration guide,
  repository rules, release-policy script, SemVer CI, and warnings-denied
  Rustdoc/SDL checks.

## Not yet production-ready

- Applied host migrations and production PostgreSQL parity testing. PostgreSQL
  remains compile-checked only; no local or production PostgreSQL was touched.
- Durable per-principal inbox sequencing/subscriptions and retention purge
  execution. Session-event live wakeup/replay/reauthorization is implemented;
  reset signaling is present, while actual retention pruning remains.
- Usage, skill, and inbox subscription roots beyond the session,
  configuration, attachment, proposal, and approval surfaces.
- Application-encrypted field/keyring and production mutable secret-store
  implementations. Database-managed protection and the safe service seams are
  implemented.
- Attachment quotas, derivative artifacts, provider-file/image resolution and
  provider-side deletion. Core ticketed quarantine/scan/promotion/release and
  expired/interrupted exact-reference cleanup are implemented.
- Top-level supervised coordinator for heartbeating long human approval waits,
  restart adoption, and exact provider continuation. The generic consequential
  executor and preview-builder seam are implemented; the read-only coordinator
  deliberately cannot route mutation descriptors.
- Per-item proposal review and application-specific proposal rendering. Whole
  structured payload accept/edit/reject and trusted post-mutation outcome
  linkage are implemented.
- Provider HTTP adapters for Anthropic, xAI, Ollama, and explicitly profiled
  OpenAI-compatible endpoints.
- OpenAI attachment/file upload resolution, background/webhooks, provider file
  deletion, image/file input, and full built-in result normalization. The
  current adapter intentionally rejects local opaque attachment IDs until that
  pipeline exists.
- Provider webhooks/background processing.
- Authenticated GraphQL budget-policy management, usage reporting, pricing
  catalog validation, privileged uncertain-call recovery, retention/purge, and
  telemetry sinks. The ordinary transactional reservation/reconciliation path
  is implemented.
- Cross-generation adoption for validated provider-turn or partially completed
  application-tool checkpoints and provider-independent stateless
  continuation. Exact completed read-only tool-batch adoption, the bounded
  coordinator, protected stateful checkpoints/continuation, protected live
  output, and final-output crash reconciliation are implemented; all other
  ambiguous resume remains closed.
- Backup adapter execution and applied restore transactions.
- Resolver-operation disclosure metadata generation and complete schema-aware
  control-plane recursion validation. The current catalog uses explicit
  reviewed operation contracts, disclosure schemas, and a fail-closed
  identifier scanner.
- Deployment-specific delegated-credential issuers and private HTTP GraphQL
  transports. The generic exact-binding adapter is implemented; credential
  format, fixed destination mapping, network isolation, and application audit
  integration intentionally remain host-owned.
- Ollama/OpenAI-compatible local provider adapters and the allowlisted installed
  local-harness/ACP process driver. Local execution remains in scope; no model
  may choose a command, arguments, working directory, environment, mount, or
  network authority.
- Any consumer integration testing or migration. That work is explicitly left
  to each consumer project/agent.

## Next implementation slice

1. Add exact authorized image/file resolution to provider adapters, including
   provider-side file lifecycle/deletion where applicable.
2. Add the per-principal inbox stream and retention/pruning worker, then the
   remaining provider/configuration surfaces, including Ollama and the
   deterministic fake-process foundation for an allowlisted local harness.
3. Add Docker-owned PostgreSQL parity tests only after the harness can prove it
   created the exact disposable database handle.

## Current verification

- `cargo test --features provider-openai`: 44 integration tests and 47 active
  unit tests passed; one explicit live-provider test remained ignored. Thirty
  generated private-ORM search doctests remained intentionally ignored.
- `cargo clippy --all-targets --features provider-openai -- -D warnings`:
  passed.
- Warnings-denied Rustdoc passed for `provider-openai` and
  `graphql-case-pascal`.
- PascalCase SDL contract test passed with no camelCase aliases.
- `cargo check --no-default-features --features postgres`: passed, compile-only.
- `cargo check --no-default-features --features mssql`: passed, schema-only;
  existing dependency warnings remain in `graphql-orm`.
- The mutually exclusive backend features intentionally cannot be checked with
  Cargo `--all-features` in one build.

## Provider test note

- Mocked OpenAI HTTP/SSE and all other automated tests pass without credentials.
- The synthetic live OpenAI smoke test is explicit opt-in and is not part of
  automated verification. Its key-file loader requires exactly one unwrapped
  credential and never logs the value or provider response body.
