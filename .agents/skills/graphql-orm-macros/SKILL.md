---
name: graphql-orm-macros
description: >
  Use when working on the graphql-orm runtime plus graphql-orm-macros derive
  layer for GraphQL entities, relations, CRUD operations, schema modules,
  resolver metadata, migrations, pagination, durable streams, encryption, and
  backend integration in graphql-orm-ai.
---

# graphql-orm Skill

## Use This Skill When

- deriving `GraphQLEntity`, `GraphQLRelations`, or `GraphQLOperations`
- composing schema roots with `schema_roots!`
- adding AI persistence entities without exposing unsafe generated CRUD
- changing resolver-operation metadata or schema-module integration
- implementing bidirectional keyset pagination or durable event streams
- reviewing relation loading or N+1 behavior
- changing runtime metadata, query rendering, schema diffing, migrations, or backup descriptors
- adding encrypted-field support
- implementing SQLite, PostgreSQL, or MSSQL backend behavior

## Crates

- Application-facing dependency: `graphql-orm`
- Runtime and macro repo: `../graphql-orm`
- Upstream runtime repo: `https://github.com/Dastari/graphql-orm`

## Preferred Usage

Import through the runtime crate:

- `use graphql_orm::prelude::*;`
- `use graphql_orm::mutation_result;`
- use derive macros by name on structs

`graphql-orm-ai` should normally depend only on `graphql-orm`. Do not add a
direct `graphql-orm-macros` dependency unless explicitly developing or
debugging the proc-macro crate.

## Integration Rules

1. Use the runtime-plus-macro split correctly.
Generated code comes from re-exported macros. Runtime behavior, metadata, query
rendering, relation loading, policy enforcement, migrations, and backend SQL
belong to `graphql-orm`.

2. Keep all database syntax in `graphql-orm`.
`graphql-orm-ai` must use generated repository, transaction, migration,
pagination, stream, and backup APIs. Do not issue raw SQL or depend directly on
SQLx or Tiberius database execution APIs.

3. Use macros for persistence boilerplate, not agent policy.
Model routing, tool policy, approvals, data classification, provider behavior,
and session orchestration belong in `graphql-orm-ai`.

4. Keep generated types aligned with async-graphql.
Ensure generated output/input types remain compatible with async-graphql and
that sensitive/private fields are not accidentally exposed.

5. Treat resolver metadata as discovery, not authorization.
Generated operation descriptors may describe every resolver, but AI tool
exposure remains default-deny and runtime resolver policies remain
authoritative.

6. Keep subscriptions fail-closed.
Do not expose generated subscriptions as AI tools until row/field filtering,
durable replay, lag recovery, and long-lived reauthorization are implemented.

7. Use stable keysets for large timelines.
Chat and event history must use bounded bidirectional keyset connections, never
unbounded lists or offset pagination for deep history.

8. Keep persistence backend-agnostic at the AI layer.
Backend-specific SQL rendering, MSSQL write support, migration planning, vector
queries, and schema introspection belong in `graphql-orm`.

9. Use schema modules for internal entities.
AI entities should contribute migration and backup metadata without forcing
ordinary generated CRUD fields into the host's public schema.

10. Preserve ordinary authorization paths.
Application tools execute through the composed GraphQL schema with current auth
context. Do not replace this with trusted repository or system access.

11. Keep database tests isolated.
SQLite may use temporary databases. PostgreSQL and MSSQL integration tests must
use disposable Docker containers and must never connect to live local
databases.

## When Not To Use

- provider HTTP/SSE protocol work with no ORM impact
- authentication, token lifecycle, or principal rehydration
- frontend-only GraphQL documents
- simple handwritten types where a derive would add unnecessary coupling

## Common Pattern

```rust
use graphql_orm::prelude::*;

#[derive(
    GraphQLEntity,
    GraphQLOperations,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
)]
#[graphql_entity(
    table = "ai_sessions",
    plural = "AiSessions",
    keyset = "updated_at desc, id desc"
)]
struct AiSession {
    #[primary_key]
    id: graphql_orm::uuid::Uuid,

    owner_subject: String,
    updated_at: i64,
}
```

## Project Guidance

- keep `graphql-orm-ai` project-agnostic
- use `graphql-orm` as the normal runtime and macro re-export surface
- contribute reusable persistence primitives back to `graphql-orm`
- do not work around missing ORM features with raw SQL in this crate
- preserve backward compatibility for existing generated resolver clients
- treat resolver metadata, encryption, streams, vector search, and MSSQL writes
  as shared ORM concerns when they benefit multiple consumers
