# Implementation Status

This file is intentionally explicit about what is a compiled contract versus
production-ready behavior.

The [checkpoint-based completion plan](completion-plan.md) defines the active
work order and exit gates. This file remains authoritative for the implemented
and deliberately incomplete inventory.

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
- AI schema-module identity (currently version `0.50.0`) and 40 private records
  spanning provider/model configuration, content/egress/tool/retention/budget
  policy and atomic reservations, sessions, attachments, runs, approvals,
  proposals/items, checkpoints, skills/versions, usage, background submissions,
  webhook receipts, audit, secret cleanup, egress decisions, and restore
  readiness.
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
  settlement with checked integer arithmetic. Deployment-supplied immutable
  web/file-search per-call rates are independently administration-bounded;
  quotes reserve a shared provider-enforced maximum at the greatest enabled
  rate, while settlement counts only exact normalized completions. Code-
  interpreter and image-generation billing dimensions remain unsupported.
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
- Native OpenAI exact-reference artifact deletion bound to one logical provider
  profile, fixed official Files endpoint, just-in-time credentials, exact
  acknowledgement validation, and authoritative same-ID absence confirmation.
  It cannot list, upload, search, or retrieve file content.
- Exact-profile OpenAI webhook verification over bounded raw request bytes,
  just-in-time signing secrets, exact delivery headers, HMAC-SHA256, and a
  bounded replay window. SQLite/PostgreSQL intake atomically stores one
  content-free receipt plus redacted audit; concurrent/later redelivery is
  idempotent, immutable collisions fail closed, and unsupported signed events
  are ignored durably. Restore preflight makes an invalid deterministic
  identity/binding/state/audit count fatal. No provider output is retrieved and
  no run is mutated.
- Exact initial OpenAI background submission under an active fenced run. The
  ORM service binds one tool-free/attachment-free request to the exact
  run/attempt/generation/profile/model/request hash, uncertain budget, and
  model-inference egress allow event before performing one non-retried create.
  Known pre-transport failures release unused capacity and the worker renews
  the same fence while awaiting the bounded acknowledgement. The native adapter
  forces non-streaming background mode and exact opaque echoed metadata. The
  acknowledgement also binds exact model, output ceiling, and provider storage
  choice. Acceptance parks the run lease-free as `WaitingProvider`; transport/
  acknowledgement ambiguity closes it as `RecoveryRequired` with an immutable
  attempt outcome. Restore makes invalid submission bindings fatal and never
  replays restored waiting work.
- Bounded generated-ORM OpenAI background reconciliation claims over accepted
  submissions. Claim/reclaim revalidates the deterministic submission, active
  session, lease-free waiting run, original attempt and absent outcome,
  uncertain budget, and original allow before CAS-incrementing a distinct
  reconciliation generation. Heartbeats rotate the exact row-version fence;
  voluntary pre-retrieval release clears ownership, increments a bounded retry
  count, and schedules a later attempt without mutating the run. Concurrent
  workers, expired reclaim, stale proofs, deadline/retry bounds, missing legacy
  deadlines, and malformed support graphs are covered by in-memory SQLite
  tests. The opaque claim grants no provider or terminal authority.
- Complete exact OpenAI background terminal reconciliation. Claiming
  deterministically links at most one exact verified receipt through a bounded
  composite index but polling remains independently live. Retrieval rehydrates
  current principal/access/protection authority, audits and binds one exact
  fixed-destination response egress decision, and returns only bounded reviewed
  status/output/usage. Private classified failure proofs drive retryable
  exponential backoff or recovery closure; queued/in-progress observations
  release similarly, and bounded expiry closes immutable deadline failures.
  Terminal commit rehydrates and rechecks authority, applies immutable host
  pricing, protects completed output, then atomically commits exact-once
  budget/counters/usage, optional message/blocks/checkpoint/session/inbox
  events, receipt states, attempt outcome, audit, submission, and run. Failed,
  incomplete, and cancelled responses settle usage without output; conflicts
  preserve uncertain capacity for recovery. Exact replay validates the durable
  terminal graph instead of repeating settlement or output.
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
  expired provisional live deltas and selectively tombstones age-expired
  terminal tool/approval protected payloads while preserving newer/live
  authority. A separate database-enforced append-only transaction then purges
  bounded age-expired orphaned protected coordinator checkpoints only after
  terminal run, closed attempt-outcome, committed budget, absent current
  pointer, and final-output or tombstoned-tool dependency proof. Once an exact
  `deleting`/`deleted_at` session reaches
  `deleted_content_purge_seconds`, bounded passes delete every
  protected session event kind, then exhaust independently bounded protected
  context-summary checkpoint pages. Ordinary message retention first
  physically invalidates every covering context checkpoint under a complete
  lookahead proof. A whole-session lookahead proof then
  tombstones protected proposal/item content only for terminal outcomes and
  terminal owning runs; accepted proposals without a trusted applied outcome
  stay blocked. A later whole-session proof tombstones protected tool
  arguments/results and approval resources/previews only for bounded terminal
  runs, exact finished steps, and compatible terminal one-shot approvals;
  active or uncertain authority blocks the session. The complete bounded
  attachment-artifact set next enters separately claimed generation/lease-
  fenced cleanup before any parent attachment. That worker re-proves each
  parent, current cutoff, and exact local object absence; provider references
  require a host-supplied authoritative delete-and-confirm-absent boundary.
  Ambiguity retains references and protected derivatives under backoff. Only a
  later retention pass removes confirmed artifact metadata, then coordinates
  the parent exact-reference blob cleanup, before
  eligible terminal message scrubbing, even when ordinary message retention is
  disabled. After all ordinary protected sources are exhausted, retention
  clears validated terminal run pointers before a separate generated-ORM
  transaction independently re-proves tool/approval tombstones, purges bounded
  immutable coordinator-checkpoint pages, and atomically appends redacted
  audit. Protected session-bound inbox payloads are CAS-tombstoned before
  message content while their shared-stream rows and sequences remain. A later
  complete proof of the current cutoff, valid inbox payload tombstones, retained
  message tombstones, terminal runs, zero pointers/checkpoints, and absence of
  every ordinary protected/external dependency atomically redacts the title and
  finalizes the hidden shell as `deleted`. Message, proposal, tool, and approval
  metadata tombstones remain, event gaps request a client reset, ambiguous
  artifacts and unsafe dependencies remain blocked, and all other append-only
  security facts stay non-purgeable.
- Protected context compaction under a current running lease. Preparation
  rehydrates principal/owner/scope authority, renews the complete fence, opens
  only one exact contiguous message segment after the latest valid checkpoint,
  leaves a configured recent tail verbatim, and returns a sensitive provider
  request plus exact `Restricted` source manifest. Persistence accepts only the
  matching ordinary provider result with committed budget and exact
  `context_compaction` egress evidence, rejects non-visible/tool output, and
  transactionally re-proves every parent/message/block before inserting the
  protected summary, chained hash, provenance, and fence/budget evidence.
  Latest-valid loading reauthorizes and validates lineage; Debug is redacted,
  stale sources/parents/fences and over-bound sets remain closed. Restore adds
  a fatal context-checkpoint integrity count.
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
- Bounded live approval-wait reconciliation under rehydrated principal,
  current scope policy, exact provider-turn checkpoint hash, committed budget,
  and unique call/step/approval linkage. Valid waits remain parked;
  denied/revoked/expired/cutoff/deleted-session/policy-cancelled waits close
  atomically with protected/redacted/immutable facts; malformed linkage closes
  only the run for recovery. The worker grants no consumption, resolver,
  provider, or replay authority and restored waits remain recovery-only.
- Protected same-attempt resumption for one provider-retained supervised
  mutation. The resume service validates the exact pre-wait provider
  checkpoint, committed budget, current rules, `resume_claimed` approval,
  staged tool and route; executes through fresh ordinary resolver
  authorization; then writes a distinct consumed-approval-bound continuation
  checkpoint. Read-only checkpoint append/adoption structurally rejects write
  and approval-bearing tool rows.
- Cross-generation adoption of that exact completed provider-retained
  mutation result. Expired-lease and trusted snapshot restore planning can
  requeue only after complete approval/tool/result/egress/budget evidence;
  adoption reopens every protected envelope under current authority/rules and
  consumes the checkpoint once before later provider transport without
  replaying the resolver.
- Top-level bounded sequential supervised coordination. Host plans expose only
  provider-retained `SupervisedWrite`/`OneShot` definitions; accepted provider
  turns are checkpointed before approval staging; the worker stops through the
  human wait; approved claims execute through the ordinary resolver; and the
  protected result is adopted, rule-checked, consumed once, and continued.
  Parallel/mixed/stateless/autonomous paths remain closed, ambiguity never
  replays a mutation, and loop capacity is proven before approval or checkpoint
  consumption.
- Optional coherent `graphql-case-pascal` contract covering roots, arguments,
  inputs, outputs, subscriptions, enums, and forwarded generated ORM fields
  without lowercase aliases.
- Root README, documentation index/guides, changelog, migration guide,
  repository rules, release-policy script, SemVer CI, and warnings-denied
  Rustdoc/SDL checks.
- Exporter-neutral `AiOperationalTelemetrySink` and typed content-free
  provider, durable run/tool, expired-run recovery, retention, restore-plan,
  and restore-readiness observations. The vocabulary excludes content,
  arbitrary strings, durable/principal IDs, endpoints, credentials, provider
  response/model/profile identities, restore issue text/fingerprints, and
  retention cursors. A random telemetry-only operation ID supports trace/event
  correlation but is explicitly prohibited from metric labels.

## Not yet production-ready

- Consumer-owned migration validation and production deployment acceptance.
  The crate's disposable PostgreSQL parity harness is implemented, but it does
  not substitute for a consumer's schema composition/restore rehearsal.
- Complete deleting-session, audit, attachment/blob, and provider-persistent-
  file retention workflows beyond the implemented exact OpenAI deletion seam.
  Principal-inbox pruning
  plus bounded provisional-delta and post-deletion-cutoff session-event/
  context-summary/terminal-proposal/terminal-tool-and-approval/artifact/basic-attachment/
  message-content pruning are implemented; unresolved accepted proposals,
  active or uncertain tool authority, artifact/provider objects without exact
  absence proof, unsafe message dependencies, required redacted session shells,
  runs and attempt history, non-checkpoint append-only facts, and other external
  content remain, so reports do not claim physical record or audit erasure.
  Bounded deleting-session and age-expired orphaned protected coordinator-
  checkpoint purge are implemented; current, nonterminal, recovery-required,
  or dependency-ambiguous checkpoints remain deliberately closed.
- Application-encrypted field/keyring and production mutable secret-store
  implementations. Database-managed protection and the safe service seams are
  implemented.
- Attachment quotas, derivative-artifact production, and provider-persistent
  file upload/search. Core ticketed quarantine/
  scan/promotion/release, exact ephemeral provider reopening, OpenAI inline
  image/file input, expired/interrupted exact-reference cleanup, and verified
  deleting-session cleanup for artifacts and parent attachment
  objects/metadata are implemented. Provider artifact deletion is an exact
  host seam with a native profile-bound OpenAI implementation; no provider
  file is created or searched by this crate.
- Multi-call, mixed, parallel, or stateless supervised coordination. The
  sequential provider-retained top-level coordinator and bounded live
  human-wait reconciliation are implemented; the read-only coordinator still
  cannot route mutations.
- Per-item proposal review and application-specific proposal rendering. Whole
  structured payload accept/edit/reject and trusted post-mutation outcome
  linkage are implemented.
- Provider-persistent file upload/search, plus richer provider file-type
  preflight and full built-in result normalization. Exact inline image/file input remains
  independently gated by host MIME policy, budget, egress, current authority,
  and reopening limits.
- Privileged uncertain-call recovery and complete retention/purge application.
  Budget-policy management, ordinary transactional reservation/reconciliation,
  authenticated usage reporting, and the content-free operational telemetry
  sink contract are implemented.
- Cross-generation adoption for validated provider-turn or partially completed
  application-tool checkpoints. Exact completed provider-retained supervised
  and provider-retained/bounded-stateless read-only tool-batch adoption, the
  bounded read-only coordinator, protected checkpoints/continuation, protected
  live output, and final-output crash reconciliation are implemented; all
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
- A production OS/container implementation of the trusted local-harness
  launcher and optional ACP framing. Ollama custom tools and JSON-lines v2
  harness tools now use the
  bounded stateless contract; no model may choose command, arguments, working
  directory, environment, mount, or network authority.
- Any consumer integration testing or migration. That work is explicitly left
  to each consumer project/agent.

## Next implementation slice

The detailed sequence and acceptance gates are maintained in the
[completion plan](completion-plan.md). The leading runtime priorities are:

1. Finish the `0.54.0` PostgreSQL/restore/release-matrix checkpoint for the
   complete OpenAI background terminal lifecycle.
2. Add provider-persistent file upload/search behind independent authority,
   egress, budget, fencing, retention, and restore proofs, building on exact
   profile-bound deletion. Extend authoritative unit pricing only when each
   additional billing dimension is complete.
3. Design complete ordering/history proofs before considering multi-call or
   stateless supervised resumption; keep both paths closed until those proofs
   are reviewable.

A production OS/container local-harness launcher remains deployment-owned. A
generic `Command` implementation in this crate could not prove immutable-image
digest verification, mount/network isolation, cgroups, descendant cleanup, or
the absence of inherited authority, so the public trusted launcher seam stays
intentional.

## Current verification

- The complete `0.53.0` SQLite/provider matrix passes: 166 unit tests, all
  integration tests, one explicit live OpenAI test ignored, and 31 generated
  private-ORM doctests intentionally ignored.
- Full warnings-denied Clippy and warnings/missing-docs-denied Rustdoc passed
  with all native/profiled provider adapters and the installed harness.
  PascalCase SDL and missing-docs Rustdoc also passed with no lowercase aliases.
- Bare PostgreSQL and MSSQL plus the OpenAI/MSSQL feature combinations pass
  compile-only checks. The complete generated-ORM PostgreSQL parity test passes
  with native OpenAI enabled in an ownership-labeled disposable PostgreSQL 17
  container, which was removed after the test.
- Release-policy and package-file review pass against the `0.52.0` checkpoint.
  `cargo-semver-checks` completes successfully for `0.52.0` to `0.53.0`; the
  pre-1.0 minor move is a breaking-version boundary, so no compatibility lints
  are applicable. The package contains no ignored handoff, credential, local
  path, or consumer-specific artifact.
- The dependency universe now resolves exactly `graphql-orm` 0.15.0 at
  `6beef53633befd90a4d4810887a3e4640dc4ad91` and `agql-auth` 0.12.0 at
  `3f3b0c5365adfbe436514a681d977b600991b797`, with one runtime/macro/auth
  source and public type universe.
- Pushed `0.49.0`/schema `0.46.0` and PR CI run `29495696905` are fully green.
  The `0.51.0`/schema `0.47.0` dependency checkpoint is committed and pushed
  at `21f11e46f5e7b221959844cacba7f5ad81841e36`; draft PR #2 CI run
  `30253200905` passed unit/compile, owned PostgreSQL parity, release-policy,
  and SemVer jobs. The Slice 1 terminal-reconciliation design checkpoint is
  pushed at `a10b3d6f35798367229ce605872b666dbf925993`; its first durable claim-
  schema implementation checkpoint is
  `a715682d331db899742e3f5d21dde6c485964a42`, and its schema verification
  checkpoint is `e06eeb5cd85beac983e9a44ffeeffdbe56090952`. The bounded claim-runtime
  implementation and local verification are committed at
  `30b09bb5f0bc6fc337164c4568957278c3e402e9`; its follow-up plan/status
  checkpoint is `85ecc8f5c22371c04bf3b2cb8b9ee7b0ce364154`. The locally verified
  fixed-destination retrieval implementation is committed at
  `924a4fad840aab4c687f38a3702688403fd1faef`. No release, tag, publish, or
  upstream-repository mutation has occurred.
- The mutually exclusive backend features intentionally cannot be checked with
  Cargo `--all-features` in one build.

## Provider test note

- Mocked OpenAI, Anthropic, and xAI HTTP/SSE and all other automated tests pass
  without credentials.
- The synthetic live OpenAI smoke test is explicit opt-in and is not part of
  automated verification. Its key-file loader requires exactly one unwrapped
  credential and never logs the value or provider response body.
