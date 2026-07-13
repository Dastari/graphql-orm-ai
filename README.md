# graphql-orm-ai

`graphql-orm-ai` is a project-agnostic, security-first AI agent runtime for
applications built with [`graphql-orm`](https://github.com/Dastari/graphql-orm)
and [`agql-auth`](https://github.com/Dastari/agql-auth). It turns explicitly
reviewed application GraphQL operations into authenticated agent tools while
keeping application authorization, disclosure policy, approvals, spend, and
durable history under server control.

This crate is an active, unpublished pre-release. The concrete session,
configuration, subscription, fenced worker, provider-turn, protected
read-only application-tool/result, bounded continuation, protected output, and
security foundations compile and are tested. Protected proposal review and
exact one-shot approval lifecycles also compile and are tested. A
crash-resumable top-level loop, consequential tool executor, and several
operational adapters listed below are still being implemented.

## What it provides

- An ORM-owned `AiSchemaModule` with 36 private records for configuration,
  protected chat history, runs, attempts, tool calls, proposals, approvals,
  budgets, usage, egress, audit, skills, and restore readiness.
- Multiple owner-isolated, archivable chat sessions per principal with
  protected message blocks, stable pagination, idempotent send, and resumable
  session-event subscriptions designed for virtualized frontends.
- Local or remote authenticated GraphQL execution through deployment-owned
  logical targets. A model never selects an endpoint, audience, credential,
  schema, operation document, projection, or disclosure contract.
- Default-deny tool registration and enablement, maturity gates, exact
  descriptor fingerprints, recursive AI-control-plane denial, and static
  result disclosure schemas.
- Fresh `agql-auth` principal rehydration before application tools, with the
  host's ordinary GraphQL context, resolver authorization, row policy,
  assurance, rate limits, and audit remaining authoritative.
- Provider-neutral streaming events, deterministic network-free mocks, and a
  feature-gated OpenAI Responses/SSE adapter. Anthropic, xAI, Ollama, and
  explicitly profiled OpenAI-compatible adapters have reserved feature gates.
- Separate, exact proofs for provider egress and atomic budget reservation.
  Provider built-ins such as web search, file search, code execution, image
  analysis, and image generation require their own authorized transfer.
- Structured AI-owned proposals and exact one-shot approval envelopes bound to
  resource versions, policy/auth state, actor/delegation, target/schema/
  document/projection, and a server-generated canonical action preview.
- ORM-backed, authenticated proposal/approval GraphQL lifecycles: proposal
  acceptance changes only protected AI-owned staging data; approval decisions
  are CAS-bound and optional-recent-MFA-gated; exact consumption rehydrates the
  original actor, advances the run fence, and still grants no resolver
  authority.
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
| `postgres` | no | ORM persistence, compile-checked without a database |
| `mssql` | no | Schema/compile support pending ORM write parity |
| `provider-openai` | no | Native OpenAI Responses/SSE adapter |
| `provider-anthropic` | no | Reserved; adapter not implemented yet |
| `provider-xai` | no | Reserved; adapter not implemented yet |
| `provider-ollama` | no | Reserved; adapter not implemented yet |
| `provider-openai-compatible` | no | Reserved; requires explicit endpoint profiles |
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

> **Pre-release dependency note:** this source snapshot consumes API additions
> in `graphql-orm` and `agql-auth` that have not yet landed in their public
> default branches. Until matching reviewed upstream revisions are published,
> a standalone public clone is suitable for review but will not compile against
> those default branches. Do not replace the missing contracts with local
> application-specific substitutes.

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
5. Composes `AiQueryRoot`, `AiMutationRoot`, and `AiSubscriptionRoot` into the
   application or dedicated AI subgraph.
6. Opens the runtime start gate only after managed migration validation and
   restore reconciliation succeed.

Remote/federated consumers implement the same `AuthenticatedGraphqlExecutor`
contract with private destination enforcement and short-lived bounded
delegation. The crate deliberately has no dependency on a particular router,
federation implementation, or service topology.

The [getting-started guide](docs/getting-started.md) tracks which runtime
services are concrete today and which host seams are still foundations.

## Chat and streaming model

Messages, content blocks, events, runs, tool calls, and artifacts are separate
bounded resources. Reads use stable keyset windows; subscriptions replay from
a cursor to a captured watermark and then switch to commit-only wakeups. A
frontend can therefore retain a small virtualized window even for extremely
large histories instead of receiving or rendering the entire session.

Attachments use opaque AI-owned references and will pass through ownership,
size/type, quarantine, scanning, disclosure, and provider-egress checks. The
full attachment storage/scanning pipeline is not production-ready yet.

## Current maturity

Implemented and tested foundations include ORM-backed SQLite/PostgreSQL
session and configuration services, resumable session events, durable run
claim/heartbeat/retry/recovery, OpenAI and mock provider contracts, immutable
egress audit, protected windowed assistant-output persistence, content
protection, egress proofs, logical GraphQL target contracts, static disclosure
validation, an atomic ORM-backed budget reservation/reconciliation service,
exact approval binding, proposal schemas, fenced state transitions, and restore
planning. Protected proposal creation/review/outcome linkage and canonical-
preview approval request/decision/revocation/one-shot consumption are also
implemented through authenticated, optionally PascalCase GraphQL roots.

Production blockers include the durable multi-turn registered-tool/approval
coordinator, the consequential tool executor that uses consumed approvals,
live delta coalescing, authenticated budget-policy/usage GraphQL lifecycles,
per-item proposal review, attachment pipeline, production mutable secret
stores/keyrings, other provider adapters,
remote delegated credential implementation, generated resolver disclosure
metadata, Ollama/OpenAI-compatible and allowlisted installed local-harness
drivers, and Docker-owned PostgreSQL parity testing. Details live in
[implementation status](docs/implementation-status.md).

Local execution remains in scope. Ollama and OpenAI-compatible loopback servers
will use ordinary provider adapters. Installed CLI/ACP agents will use a
separate deployment-registered subprocess driver with no shell, fixed command
and arguments, sanitized environment, sandbox/resource limits, and mediated
tool callbacks through this runtime; GraphQL may select an approved logical
profile but can never configure an arbitrary command.

## Development safety and checks

Automated tests never connect to an external database. SQLite tests use only
in-memory databases. PostgreSQL/MSSQL checks are compile-only unless a test
harness proves that it created and owns a disposable Docker container, unique
credentials, database, and cleanup. Generic `DATABASE_URL` fallbacks are
forbidden. Consumer-application integration tests belong to those consumers.

```bash
cargo fmt --check
cargo test --features provider-openai
cargo clippy --all-targets --features provider-openai -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --features provider-openai --no-deps
cargo test --features graphql-case-pascal --test graphql_naming
cargo check --no-default-features --features postgres
cargo check --no-default-features --features mssql
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

Every user-visible change updates [CHANGELOG.md](CHANGELOG.md). Every public
Rust/GraphQL/configuration/security/persistence/restore contract change also
updates [MIGRATION.md](MIGRATION.md), even when no data migration is required.
CI enforces those files, crate/schema version movement, `cargo-semver-checks`,
warnings-denied Rustdoc, and the PascalCase SDL contract. Repository rules are
recorded in [AGENTS.md](AGENTS.md).

## License

MIT
