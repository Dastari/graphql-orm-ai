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
- AI schema-module identity (currently version `0.5.0`) and 35 private records
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
- Static recursive result-disclosure schemas with closed objects, bounded
  lists, `NeverExport` nodes, classification tightening, stable fingerprints,
  and runtime enforcement before tool results leave the execution boundary.
- Full action-envelope approval domain contract binding tool/argument,
  principal/delegation, logical target/schema/document/projection/disclosure,
  resources/versions, policy/auth-state, canonical preview, expiry, and
  one-shot consumption identity.
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
- Approval, proposal, attachment, usage, skill, and subscription roots beyond
  the initial session/configuration surfaces.
- Application-encrypted field/keyring and production mutable secret-store
  implementations. Database-managed protection and the safe service seams are
  implemented.
- Attachment/quarantine/storage pipeline.
- Durable database worker claim/heartbeat/recovery operations.
- Transactional approval persistence, canonical preview provider, atomic
  one-shot consumption, and recent-MFA flow. Exact domain bindings and schema
  columns exist, but the lifecycle service/root does not.
- Provider HTTP adapters for Anthropic, xAI, Ollama, and explicitly profiled
  OpenAI-compatible endpoints.
- OpenAI attachment/file upload resolution, background/webhooks, provider file
  deletion, image/file input, and full built-in result normalization. The
  current adapter intentionally rejects local opaque attachment IDs until that
  pipeline exists.
- Provider webhooks/background processing.
- Concrete transactional budget counter/reservation service, usage,
  retention/purge, and telemetry sinks. Atomic request/proof/reconciliation
  contracts and persistence entities exist.
- Backup adapter execution and applied restore transactions.
- Resolver-operation disclosure metadata generation and complete schema-aware
  control-plane recursion validation. The current catalog uses explicit
  reviewed operation contracts, disclosure schemas, and a fail-closed
  identifier scanner.
- Concrete delegated-credential issuer and remote HTTP GraphQL executor. The
  target/audience/resource/context contracts are present and transport remains
  host-owned.
- Ollama/OpenAI-compatible local provider adapters and the allowlisted installed
  local-harness/ACP process driver. Local execution remains in scope; no model
  may choose a command, arguments, working directory, environment, mount, or
  network authority.
- Any consumer integration testing or migration. That work is explicitly left
  to each consumer project/agent.

## Next implementation slice

1. Implement the ORM-backed transactional budget counter/reservation service,
   conservative reconciliation, and concurrency tests using only in-memory
   SQLite.
2. Implement mock-provider orchestration through the fenced durable worker,
   registered tool execution, result disclosure, and provider egress loop.
3. Implement proposal and exact approval services/GraphQL lifecycles, including
   canonical previews and atomic one-shot consumption.
4. Add the generic delegated-authority seam and remote authenticated GraphQL
   executor fixtures without embedding a federation/router product.
5. Add the attachment quarantine/scanning/storage pipeline and connect its
   authorized image/file resolution to provider adapters.
6. Add the per-principal inbox stream and retention/pruning worker, then the
   remaining provider/configuration surfaces, including Ollama and the
   deterministic fake-process foundation for an allowlisted local harness.
7. Add Docker-owned PostgreSQL parity tests only after the harness can prove it
   created the exact disposable database handle.

## Current verification

- `cargo test --features provider-openai`: 34 integration tests and four
  active unit tests passed; one explicit live-provider test remained ignored.
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
