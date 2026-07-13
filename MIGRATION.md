# Migration Guide

`graphql-orm-ai` is not yet published. This guide is still mandatory so early
Git consumers and disposable test deployments can track schema and API changes
without guessing.

## Unreleased: schema module 0.4.0 to 0.5.0

Apply the dependency-owned `AiSchemaModule` through the normal
`graphql-orm` schema manager using a new managed migration version. Do not copy
SQL or create AI tables manually.

The additive schema change creates:

- `graphql_orm_ai_budget_counters`
- `graphql_orm_ai_budget_reservations`

It also adds exact target/schema/document/projection/disclosure, principal/
delegation, resource precondition, policy/auth-state, canonical preview, and
one-shot consumption columns to `graphql_orm_ai_approvals`.

No existing conversational content needs rewriting. Existing pre-release
approval rows cannot safely manufacture the new bindings: expire/revoke them
during restore/startup reconciliation and require a fresh approval. Existing
unaccounted provider work must complete or be classified as uncertain before
enabling hard budgets.

Back up a disposable environment before rehearsing the migration. Runtime
workers, subscriptions, webhooks, and schedules remain closed until managed
schema validation and restore reconciliation report module `0.5.0` ready.

### Rust API changes

- `ProviderRequestContext::new` requires an `AuthorizedBudgetReservation`.
- `AiRuntimeBuilder` requires `graphql_targets(...)`.
- `GraphqlRequestContextFactory::build` receives the validated
  `GraphqlExecutionTarget`.
- `ToolGraphqlRequest` carries an exact `GraphqlOperationContract`.
- `GraphqlInvocationContext` carries explicit causation and optional safe
  delegation references plus the exact application scope.
- Application GraphQL tools use
  `AiToolCatalog::register_with_disclosure`; `register` is reserved for
  internal proposal-staging tools.
- `AiRuntimeBuilder` requires `tool_authorization_policy(...)` so current
  principal/scope/descriptor/arguments are authorized on every call.
- `AiRuntime::execute_tool` requires the registered `AiToolId` and returns an
  `AiToolExecutionResult` after argument, output-limit, and disclosure checks.
- Tool argument schemas must explicitly declare JSON Schema 2020-12.

These are deliberate pre-1.0 breaking changes. Update host construction and
mock fixtures together; do not create permissive placeholder targets,
disclosure schemas, or budget grants.

### Provider error classification

The OpenAI adapter now maps HTTP 401 to `ProviderError::CredentialUnavailable`
instead of `ProviderError::Rejected`. Hosts matching public error categories
should handle the credential category as a redacted configuration/rotation
failure. No data migration is required.

### GraphQL naming

The default SDL remains async-graphql camelCase. Hosts requiring PascalCase
enable:

```toml
graphql-orm-ai = {
  version = "0.1.0",
  features = ["sqlite", "graphql-case-pascal"]
}
```

This changes resolver, argument, input, output, subscription, and generated ORM
field names as one compile-time schema contract. There are no lowercase aliases.
Regenerate client documents and compare SDL before rollout. No database
migration is caused solely by the naming feature.

## Initial adoption

New deployments compose `AiSchemaModule`, apply its managed schema, configure
content protection and immutable deployment boundaries, and keep the runtime
start gate closed until readiness succeeds. PostgreSQL/MSSQL rehearsal must use
a disposable Docker-owned database; never point migration commands at a live
machine database.
