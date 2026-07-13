# Getting Started

`graphql-orm-ai` is currently a Git-only pre-release crate. Use the same
reviewed dependency universe for `graphql-orm-ai`, `graphql-orm`, and
`agql-auth`; local sibling paths are for development only.

## Features

Exactly one persistence backend is currently required:

- `sqlite` (default)
- `postgres`
- `mssql` (schema/compile support until ORM write parity lands)

Provider adapters are opt-in. `provider-openai` enables the native OpenAI
Responses adapter. `graphql-case-pascal` changes the complete GraphQL naming
contract from default camelCase to PascalCase.

## Host integration outline

1. Compose `AiSchemaModule` and apply its managed schema through
   `graphql-orm`.
2. Install `AuthPrincipal` in normal GraphQL request context and provide a
   `CurrentPrincipalResolver` for durable work.
3. Provide session/configuration access, fresh principal-aware
   `AiToolAuthorizationPolicy`, egress, secret-store, and content-protection
   implementations.
4. Register immutable logical GraphQL targets. Remote target URLs and
   credential issuance remain deployment-owned and never model-visible.
5. Register reviewed application tools with server-authored documents, exact
   operation contracts, and static disclosure schemas. Registration does not
   enable a tool.
6. Register proposal types and provider adapters.
7. Apply/validate migrations and restore reconciliation, then open the runtime
   start gate.

The current crate exposes concrete session/configuration/subscription services
and foundation contracts; the durable orchestration worker, attachment
pipeline, budget persistence service, and full approval GraphQL lifecycle are
still under implementation. See [implementation status](implementation-status.md).
