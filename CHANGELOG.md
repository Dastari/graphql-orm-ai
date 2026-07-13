# Changelog

All notable user-visible changes are recorded here. The crate follows
Semantic Versioning and keeps migration instructions in [MIGRATION.md](MIGRATION.md).

## [Unreleased]

### Added

- Project-agnostic AI schema module with 35 private persistence entities for
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
- Full approval action-envelope types binding resources/versions, policies,
  actor/delegation identity, operation contracts, and server-generated
  canonical previews.
- Fresh principal/scope/descriptor/argument-aware tool authorization inside
  the authenticated bridge, JSON Schema 2020-12 argument validation, and
  disclosure-validated runtime result envelopes.
- Optional `graphql-case-pascal` feature for coherent PascalCase resolvers,
  arguments, inputs, outputs, subscriptions, and forwarded ORM fields.
- Repository governance, documentation index, migration/release policy, CI
  rustdoc checks, and SemVer enforcement scaffolding.
- Project-agnostic local execution design covering local HTTP model servers and
  allowlisted native/ACP subprocess harnesses without arbitrary shell,
  environment, filesystem, network, or tool authority.

### Changed

- AI schema module version is now `0.5.0` after adding budget counter and
  reservation tables plus stronger approval columns.
- `ProviderRequestContext` now requires an exact `AuthorizedBudgetReservation`
  in addition to egress proofs.
- `AuthenticatedToolBridge` now requires an immutable logical target registry;
  request-context factories receive the validated target, and runtime builders
  require an `AiToolAuthorizationPolicy`.
- `AiRuntime::execute_tool` now requires a registered tool ID and returns an
  `AiToolExecutionResult` only after current policy, resolver, byte/list limit,
  and static disclosure checks succeed.
- Non-internal tool catalog registration now requires an exact GraphQL
  operation contract and static disclosure schema.
- The opt-in OpenAI smoke-test key file now rejects labels, wrapped values, and
  internal whitespace instead of sending an ambiguous bearer credential.

### Security

- Tool registration rejects current AI control-plane and GraphQL introspection
  roots, including casing variants, before policy enablement.
- Provider model/output swaps invalidate budget proofs before transport.
- Approval changes to resource, policy, schema, document, projection, actor,
  preview, or authorization-state bindings invalidate the grant.
- OpenAI HTTP 401 responses map to the redacted `CredentialUnavailable`
  category instead of a generic provider rejection.

## 0.1.0

Initial release is not yet published. Everything above remains unreleased
until the production gates in `docs/plan.md` are satisfied.
