# graphql-orm-ai

`graphql-orm-ai` is a project-agnostic, security-first AI agent runtime for
applications built with [`graphql-orm`](https://github.com/Dastari/graphql-orm)
and [`agql-auth`](https://github.com/Dastari/agql-auth). It turns explicitly
reviewed application GraphQL operations into authenticated agent tools while
keeping application authorization, disclosure policy, approvals, spend, and
durable history under server control.

This crate is an active, unpublished pre-release. The concrete session,
configuration, subscription, fenced worker, provider-turn, protected
read-only application-tool/result, bounded coordinator/continuation, protected
output/provider-turn/tool-batch checkpoints, supervised consequential-tool, and
private remote GraphQL adapter foundations compile and are tested. Protected
proposal review and exact one-shot approval lifecycles also compile and are
tested. Exact completed read-only tool batches with provider-retained response
IDs or bounded stateless history can also be adopted across a new fenced
generation under current authority. A bounded top-level coordinator now drives
sequential provider-retained supervised mutations through independent human
approvals and ordinary authenticated resolvers. Provider-turn/partial-batch
adoption, stateless or parallel supervised waits, and several operational
adapters listed below are still being implemented. A bounded live worker now
reconciles denied, revoked, expired, policy-cancelled, and malformed approval
waits without polling, resuming, or executing them.

## What it provides

- An ORM-owned `AiSchemaModule` with 39 private records for configuration,
  protected chat history, runs, attempts, tool calls, proposals, approvals,
  budgets, usage, egress, audit, skills, and restore readiness.
- Multiple owner-isolated, archivable chat sessions per principal with
  protected message blocks, stable pagination, idempotent send, and resumable
  session-event subscriptions designed for virtualized frontends.
- A durable per-principal cross-session inbox with atomic lifecycle/message/
  assistant-output notifications, bounded catch-up, resumable subscriptions,
  periodic current-principal reauthorization, explicit retention-gap reset,
  and a GraphQL-policy-driven bounded pruning worker.
- A host-only bounded session-retention worker that uses generated ORM
  transactions to remove expired provisional deltas, purge all bounded
  protected session events after the deleting-session cutoff, delete bounded
  protected context-summary checkpoints before they can outlive covered
  content, tombstone only terminal proposal/item payloads under whole-session
  bounds, tombstone only exactly linked terminal tool/approval payloads after a
  complete bounded authority proof, coordinate artifact-free attachment objects
  through independently verified exact-reference cleanup, delete only confirmed
  attachment tombstones, scrub eligible terminal message content, clear
  validated terminal run-checkpoint pointers, and finally purge bounded
  immutable checkpoint pages under the exact current GraphQL-managed scope
  policy. Proposal/tool/approval/message/run metadata remains, sequence gaps
  force bounded client reset, and unresolved accepted proposals, active or
  uncertain tool authority, attachment artifacts, nonterminal work, and other
  append-only security facts stay closed.
- Local or remote authenticated GraphQL execution through deployment-owned
  logical targets. A model never selects an endpoint, audience, credential,
  schema, operation document, projection, or disclosure contract.
- A product-neutral private remote GraphQL adapter that rechecks freshly
  resolved principal age, requests one short-lived exact delegated authority,
  verifies the operation/variables/audit binding at handoff, and invokes only a
  deployment-owned logical-route transport. No incoming bearer or URL crosses
  this contract.
- Default-deny tool registration and enablement, maturity gates, exact
  descriptor fingerprints, recursive AI-control-plane denial, and static
  result disclosure schemas.
- A protected, immutable, GraphQL-managed skill catalog with recent-MFA
  publication, exact scope/CAS/audit enforcement, tool and UI-intent
  descriptor fingerprints, capability/classification/maturity requests,
  schemas, and hard per-run ceilings. Skill resolution grants no authority.
- Project-neutral hierarchical rules resolved through host-defined
  application, tenant/project, and user scope lineages. Immutable deployment
  limits and every exact GraphQL-managed layer are intersected across tool
  fingerprints, classification/maturity, providers/capabilities, approvals,
  retention/BYOK, and budgets; the result can only reject or narrow and never
  grants ordinary authority.
- Mandatory hierarchical-rule binding for read-only coordinator turns and
  protected checkpoint v2. Current rules are re-resolved before/after provider
  egress and before every resolver tool; exact fingerprints and cumulative
  provider/step/time/token/cost/tool/image usage survive safe checkpoint
  adoption, while stale rules or exceeded actual usage fail closed.
- A default-deny logical UI-intent registry with JSON Schema 2020-12 payload
  validation and exact descriptor fingerprints. Validated intents are
  suggestions only; consumers own resource reauthorization and route mapping.
  Exact provider envelopes can be durably delivered as protected session and
  principal-inbox events only after current authority, committed usage, and
  the current worker fence are all re-proven.
- Fresh `agql-auth` principal rehydration before application tools, with the
  host's ordinary GraphQL context, resolver authorization, row policy,
  assurance, rate limits, and audit remaining authoritative.
- Provider-neutral streaming events, deterministic network-free mocks, native
  OpenAI/xAI Responses/SSE and Anthropic Messages/SSE adapters, and a native
  Ollama `/api/chat` adapter. Anthropic and Ollama support exact stateless
  application-tool loops; xAI supports strict custom tools through explicitly
  retained response IDs. OpenAI-compatible Responses endpoints use an exact
  GraphQL-managed capability/retention profile and deployment-authorized URL.
- An opt-in installed local-harness boundary with deployment-frozen logical
  model registrations, fixed executable/digest/sandbox/resource contracts, a
  bounded JSON-lines v2 provider driver, optional stateless application-tool
  calls, and deterministic fake-process conformance tests. The crate
  intentionally supplies no generic unsandboxed subprocess launcher.
- Separate, exact proofs for provider egress and atomic budget reservation.
  Provider built-ins such as web search, file search, code execution, image
  analysis, and image generation require their own authorized transfer.
  Request validation and transfer estimates cover bounded tool/schema/built-in
  metadata and attachment encoding, not only visible prompt text.
- Structured AI-owned proposals and exact one-shot approval envelopes bound to
  resource versions, policy/auth state, actor/delegation, target/schema/
  document/projection, and a server-generated canonical action preview.
- ORM-backed, authenticated proposal/approval GraphQL lifecycles: proposal
  acceptance changes only protected AI-owned staging data; approval decisions
  are CAS-bound and optional-recent-MFA-gated; exact consumption rehydrates the
  original actor, advances the run fence, and still grants no resolver
  authority.
- A one-owner approved-wait handoff for restart-safe workers. The original
  attempt/generation remains bound to the staged action while approval/run
  state, worker owner, expiry, row-version fence, and redacted audit rotate
  atomically; concurrent resumers cannot receive the same action.
- A bounded `OrmAiApprovalWaitReconciliationService` for live human waits. It
  rehydrates current authority and validates the exact checkpoint, budget,
  call, step, and approval linkage before retaining a pending/approved wait or
  atomically cancelling denied, revoked, expired, deleted-session, cutoff, or
  policy-cancelled work. Malformed linkage closes only the run for recovery;
  the worker never claims, consumes, resumes, or executes an approval.
- Protected execution of one claimed, approved, provider-retained mutation.
  The runtime reopens the exact pre-wait provider checkpoint, current rules,
  committed budget, approval, staged tool, and route; executes through fresh
  ordinary resolver authorization; and writes a distinct approval-bound
  continuation checkpoint without another provider call. An expired worker or
  validated restored snapshot can re-adopt that exact result under a new fence
  and consume it once before provider transport. Multi-call and stateless
  supervised continuation remain closed.
- A top-level `AiSupervisedAgentCoordinator` for sequential supervised work.
  It accepts only host-planned provider-retained turns exposing exact
  supervised one-shot mutations, checkpoints before approval, stops during
  the human wait, executes approved claims through fresh ordinary resolver
  authorization, consumes protected results once before provider transport,
  and may repeat with a new approval. Parallel/mixed/stateless/autonomous paths
  remain closed.
- A supervised application-mutation service that accepts only explicitly
  enabled exact `SupervisedWrite` descriptors, builds current server-owned
  previews, consumes approval once, recomputes host policy before ordinary
  resolver execution, and closes any side-effect ambiguity for recovery rather
  than retry.
- Fenced run/attempt contracts, fail-closed startup, and restore reconciliation
  that treats uncertain external effects as uncertain rather than replayable.
- A concrete ORM worker for bounded claims, lease renewal, retry scheduling,
  immutable attempt outcomes, and recovery; plus a mock-tested provider turn
  that durably audits egress before transport and persists protected assistant
  output through the exact current fence.
- A deliberately narrow read-only application-tool path that persists
  protected arguments before ordinary resolver execution, applies static
  disclosure, separately authorizes and audits result egress, persists the
  protected result, renews the exact run fence, and creates bounded exact
  provider continuations.
- A top-level `AiReadOnlyAgentCoordinator` that heartbeats the current fence
  during provider streams, consumes only host-planned exact turns, enforces the
  loop guard, executes each durable read query, persists final output, and
  closes ambiguous handoffs as `RecoveryRequired` instead of replaying them.
- Same-transaction final-output checkpoints that let expired-lease recovery
  safely finish the exact crash window after protected message persistence but
  before terminal run finalization.
- Protected fenced coordinator checkpoints written after every accepted
  provider turn and complete model-visible read-only tool batch. They bind
  settled usage, loop state, scope/route, exact outputs, and continuation;
  failed checkpoint handoff requires recovery and does not trigger replay.
- Provider-independent stateless continuation for Ollama and reviewed local
  harnesses. Only bounded visible text/JSON, exact assistant calls, and
  disclosure-validated tool outputs are retained; every replayed output has a
  unique freshly authorized egress proof.
- One-shot cross-generation adoption for exact completed provider-retained or
  stateless read-only tool batches and one approval-bound provider-retained
  mutation result. Recovery preserves only a hash- and budget-bound complete
  batch; the new worker freshly reauthorizes, reopens and validates every
  required result, approval, budget, step, and egress proof, reconstructs the
  bounded continuation without rerunning a resolver, and consumes the
  checkpoint before the next provider transport.
- Optional protected durable provisional output. Only visible text and
  reasoning summaries are UTF-8-coalesced within 50 ms / 4 KiB; each batch is
  freshly authorized, protected, exact-fence/budget validated, and committed
  as a cursor event before subscription wakeup.
- Owner-isolated attachment intake using `graphql-orm-storage`: one-time
  ticketed streaming upload, random quarantine keys, exact size/hash checks,
  complete-object scanning, separate acceptance policy, protected events, and
  explicit clean release before ordinary message linkage.
- Exact attachment model-input binding: ID, MIME, verified bytes and SHA-256
  must match a separately authorized image/file manifest source before any
  provider adapter can start transport.
- Fresh exact attachment reopening through the current principal and durable
  ORM state, with bounded object streaming, post-I/O reauthorization, and
  ephemeral inline OpenAI image/file input that creates no provider file ID.
- A host-only, bounded attachment maintenance worker that fences cleanup
  generations, expires abandoned tickets/processing, confirms idempotent blob
  deletion, audits outcomes, and backs off safely on ambiguous storage errors.
- Optional coherent PascalCase GraphQL naming for consumers whose schema
  conventions require it; lowercase aliases are not emitted.

The crate never accesses application tables directly and contains no consumer
domain entity, resolver, route, deployment product, or policy. Applications
register their own scopes, targets, tools, projections, disclosure schemas,
proposal types, provider policies, and authenticated executor.

## Security model

Tool discovery is not authorization. Reading data is not permission to send it
to a model. Approval is not resolver authorization. These boundaries are
enforced independently:

1. A server-authored tool descriptor is registered with an exact GraphQL
   operation, logical target, schema fingerprint, result projection, and
   recursive static disclosure schema.
2. Deployment and scope policy explicitly enable that exact fingerprint.
3. The current principal is rehydrated and the normal application GraphQL
   authorization path executes the operation.
4. The result must conform to its static disclosure schema. Unknown fields,
   wrong shapes, limits, and `NeverExport` nodes fail closed; runtime
   classification may only tighten it.
5. Each external transfer requires an exact egress decision and a concurrent,
   atomic budget reservation bound to the run attempt and fencing generation.
6. Consequential work additionally requires a current, one-shot approval for
   the exact canonical action, followed by fresh authorization.

Bearer tokens, provider keys, raw delegation credentials, arbitrary URLs,
hidden model reasoning, and secret-classified result nodes are never stored in
chat or exposed to a model. See the [security guide](docs/security.md) for the
complete trust model.

## Feature flags

Exactly one persistence backend should be selected:

| Feature | Default | Status |
| --- | --- | --- |
| `sqlite` | yes | ORM persistence and in-memory automated tests |
| `postgres` | no | ORM persistence plus test-owned disposable-Docker parity |
| `mssql` | no | Schema/compile support pending ORM write parity |
| `provider-openai` | no | Native OpenAI Responses/SSE adapter |
| `provider-anthropic` | no | Native Anthropic Messages/SSE: text/JSON, structured output, stateless application tools |
| `provider-xai` | no | Native xAI Responses/SSE: text/JSON, structured output, strict parallel application tools |
| `provider-ollama` | no | Native Ollama chat: text, exact images, structured output, stateless application tools |
| `provider-openai-compatible` | no | Profiled Responses/SSE: text/JSON and opt-in strict tools, structured output, retained continuation |
| `local-harness` | no | Installed JSONL v2 text/structured/stateless-tool protocol over a trusted sandbox launcher |
| `graphql-case-pascal` | no | PascalCase roots, arguments, inputs, outputs, and ORM fields |

Do not build with `--all-features`: the database backends are mutually
exclusive.

## Integration outline

Add the crate from a reviewed revision using the same dependency universe as
the matching `graphql-orm` and `agql-auth` releases:

```toml
[dependencies]
graphql-orm-ai = { git = "https://github.com/Dastari/graphql-orm-ai", rev = "<reviewed-commit>", features = ["sqlite"] }
```

> **Pre-release dependency note:** this source snapshot pins the reviewed final
> `graphql-orm` 0.9.0, `agql-auth` 0.10.0, and `graphql-orm-storage` 0.5.0
> commits exactly. Keep the full revisions from this manifest; do not replace
> the shared contracts with moving branches or application-specific
> substitutes.

A host then:

1. Composes `AiSchemaModule` and applies its dependency-owned migration
   through the `graphql-orm` schema manager.
2. Installs its ordinary `AuthPrincipal` request context and a
   `CurrentPrincipalResolver` for durable work.
3. Supplies protected-content, secret-store, session access, fresh
   principal-aware tool authorization, egress policy, provider, and
   restore-readiness implementations.
4. Registers immutable logical GraphQL targets and reviewed application tools
   with exact operation and disclosure contracts.
5. Composes `AiQueryRoot`, `AiMutationRoot`, and `AiSubscriptionRoot`, plus
   separately composable configuration, proposal, attachment, and skill roots
   as required, into the application or dedicated AI subgraph.
6. Opens the runtime start gate only after managed migration validation and
   restore reconciliation succeed.

Remote/federated consumers supply an `AiRemoteGraphqlAuthorityIssuer` and
`AiRemoteGraphqlTransport` to `AiRemoteAuthenticatedGraphqlAdapter`. The
transport privately maps logical targets to fixed destinations; the issuer
mints exact, short-lived authority. Both remain deployment-owned because the
crate deliberately has no dependency on a particular router, federation
implementation, credential format, HTTP stack, or service topology. See the
[remote execution guide](docs/remote-graphql-execution.md).

The [getting-started guide](docs/getting-started.md) tracks which runtime
services are concrete today and which host seams are still foundations.
Skill publication and logical frontend suggestions are described in the
[skills and UI-intents guide](docs/skills-and-ui-intents.md).

## Chat and streaming model

Messages, content blocks, events, runs, tool calls, and artifacts are separate
bounded resources. Reads use stable keyset windows; per-session and
cross-session principal-inbox subscriptions replay from a cursor to a captured
watermark and then switch to commit-only wakeups. A frontend can therefore
retain a small virtualized window even for extremely large histories instead
of receiving or rendering the entire session. See the
[principal inbox guide](docs/principal-inbox.md).

Attachments use opaque AI-owned references. Ticketed streaming upload,
ownership, byte/hash checks, quarantine, scanning, acceptance, promotion,
release, protected events, and message linkage are implemented. Provider
file/image reopening and OpenAI inline input are implemented; derivative
artifacts, quotas, retention purge, and provider-persistent file/search
lifecycle remain gated.

## Current maturity

Implemented and tested foundations include ORM-backed SQLite/PostgreSQL
session and configuration services, resumable session events, durable run
claim/heartbeat/retry/recovery, OpenAI and mock provider contracts, immutable
egress audit, protected windowed assistant-output persistence, content
protection, egress proofs, logical GraphQL target contracts, static disclosure
validation, authenticated recent-MFA/CAS/audited budget-policy management, an
atomic ORM-backed budget reservation/reconciliation service,
an exactly-once append-only usage ledger with authenticated bounded reporting,
an authenticated immutable pricing catalog with conservative quoting and
authoritative token settlement,
exact approval binding, proposal schemas, fenced state transitions, and restore
planning. Protected proposal creation/review/outcome linkage and canonical-
preview approval request/decision/revocation/one-shot consumption are also
implemented through authenticated, optionally PascalCase GraphQL roots. An
approved wait can be handed to exactly one new worker under a rotated
owner/row-version fence without changing its action-bound attempt/generation.
A claimed single provider-retained mutation can then reopen its exact protected
provider turn, execute through fresh approval/resolver authority, and protect a
distinct consumed-approval-bound continuation without another provider call.
Protected immutable skill publication/resolution and exact schema-fingerprinted
logical UI-intent validation and fenced durable delivery are implemented as
separately composable, project-neutral contracts.

Production blockers include partial/multi-call and stateless supervised
tool-batch adoption, authoritative built-in-tool
pricing/unit catalogs, privileged uncertain-call
recovery, completion of deleting-session/provider-raw/audit retention workflows,
per-item proposal review, provider-persistent file/search lifecycle,
attachment quotas/derivatives/retention purge, production mutable secret
stores/keyrings,
deployment-specific delegated credential issuers/private HTTP transports,
generated resolver disclosure metadata, and production OS/container local-
harness launchers/ACP framing. Details live in
[implementation status](docs/implementation-status.md).

See [session retention](docs/session-retention.md) for the bounded deletion and
tombstone contract. See [protected live streaming](docs/live-streaming.md) for
the opt-in sink, provisional-event contract, client reconciliation rules, and
failure model.
See [attachment intake](docs/attachments.md) for the streaming endpoint,
scanner, policy, promotion, and GraphQL contracts.

The native Anthropic Messages adapter supports fixed-destination streaming,
structured output, and strict stateless application tools. Its exact usage,
credential, caching, and deliberately unsupported feature contracts are in the
[Anthropic guide](docs/anthropic.md).

The native xAI Responses adapter uses the fixed official endpoint and requires
the provider's zero-data-retention attestation by default. Its opt-out,
retained-continuation, and deliberately narrow tool surface are documented in
the [xAI guide](docs/xai.md).

The OpenAI-compatible Responses adapter accepts only a GraphQL-managed,
versioned capability and retention profile plus an endpoint authorized by the
deployment SSRF/network policy. Its deliberately narrow compatibility surface
and exact egress bindings are documented in the
[OpenAI-compatible guide](docs/openai-compatible.md).

Local execution is a first-class path. The native Ollama adapter is
implemented; its exact supported and deliberately gated behaviors are in the
[Ollama guide](docs/ollama.md). OpenAI-compatible loopback servers use the
separately profiled Responses adapter. The installed-harness JSON-lines
foundation is also implemented with no shell, fixed command and arguments, no
inherited environment or network authority, sandbox/resource contracts,
stateless application-tool framing, and a trusted launcher seam. GraphQL may
select an approved logical profile but can never configure a command. A
concrete OS/container launcher, ACP, and general mediated coding-workspace
callbacks remain separately gated; see the
[local harness guide](docs/local-harness.md).

## Development safety and checks

Automated tests never connect to an external database. SQLite tests use only
in-memory databases. PostgreSQL/MSSQL checks are compile-only unless a test
harness proves that it created and owns a disposable Docker container, unique
credentials, database, and cleanup. Generic `DATABASE_URL` fallbacks are
forbidden. Consumer-application integration tests belong to those consumers.
The owned PostgreSQL harness exercises only generated ORM migrations,
transactions, queries, keysets, skill/rule persistence, and fencing; it
contains no application raw SQL.

```bash
cargo fmt --check
cargo test --features provider-openai,provider-anthropic,provider-xai,provider-ollama,provider-openai-compatible,local-harness
cargo clippy --all-targets --features provider-openai,provider-anthropic,provider-xai,provider-ollama,provider-openai-compatible,local-harness -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --features provider-openai,provider-anthropic,provider-xai,provider-ollama,provider-openai-compatible,local-harness --no-deps
cargo test --features graphql-case-pascal --test graphql_naming
cargo check --no-default-features --features postgres
cargo check --no-default-features --features mssql
# Creates and cleans only its own labeled container; never accepts a DB URL.
cargo test --no-default-features --features postgres --test postgres_parity -- --test-threads=1
```

The ignored live OpenAI smoke test sends only synthetic text and is never part
of CI. It must be explicitly selected and given a key-file path. The file must
contain exactly one raw `sk-…` credential with no labels, wrapping, or other
values:

```bash
GRAPHQL_ORM_AI_OPENAI_KEY_FILE=/path/to/key \
  cargo test --features provider-openai \
  live_openai_synthetic_text_smoke_test -- --ignored
```

Mocked HTTP/SSE tests are the default and require no provider credential.

## Documentation and releases

The [documentation index](docs/README.md) links architecture, security,
development, release, and implementation-status guides. Public APIs are
documented in generated Rustdoc.

The root README stays aligned with every public/runtime change. Every
user-visible change updates [CHANGELOG.md](CHANGELOG.md), and every public
Rust/GraphQL/configuration/security/persistence/restore contract change also
updates [MIGRATION.md](MIGRATION.md), even when no data migration is required.
CI enforces that documentation bundle, crate/schema version movement,
`cargo-semver-checks`, warnings- and missing-docs-denied Rustdoc, and the
PascalCase SDL contract. Repository rules are recorded in
[AGENTS.md](AGENTS.md).

Development uses a single-owner workflow per repository. This crate's agent
treats `agql-auth` and `graphql-orm` as read-only and sends reusable changes to
their owning agents through explicit handoffs. See the
[upstream contribution workflow](docs/upstream-contributions.md).

## License

MIT
