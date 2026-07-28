# Backend and capability acceptance matrix

This matrix records the Slice 7 acceptance boundary. A compile result is not a
production claim, and a crate-level test does not replace consumer schema,
authorization, migration, restore, or deployment acceptance.

## Backend status

| Backend/profile | Evidence in this repository | Current claim |
| --- | --- | --- |
| SQLite | Complete in-memory generated-ORM unit/integration matrix, prior-to-current schema migration, concurrency/fence/recovery/security tests, provider-profile tests, GraphQL naming, Clippy, and Rustdoc. | Supported for the implemented capability set in test-owned or deployment-reviewed SQLite stores. Applied full backup/restore is still closed. |
| PostgreSQL | Compile profiles plus an owned disposable Docker parity test. The harness generates its own container, credentials, port, and database; it never consumes `DATABASE_URL` or `TEST_DATABASE_URL`. It covers migration and representative generated-ORM concurrency/state-machine behavior. | Supported for the implemented capability set only after the consumer repeats schema composition, migration, RLS/authorization, restore, and deployment tests. Applied full backup/restore is still closed. |
| MSSQL | Schema derives and compile-only feature profiles. There is no owned disposable behavioral harness in this crate, and the required upstream transaction/write/migration/policy/queue/stream/encryption/backup/concurrency parity has not been accepted. | Experimental compile/schema profile only. Not production-supported. |

## Capability status

| Capability | Implemented here | Host/consumer proof still required | Closed or unsupported |
| --- | --- | --- | --- |
| Session/message runtime and protected storage | Generated-ORM persistence, authorization bridge, content protection, fencing, bounded streaming/checkpoints, retention workers. | Schema composition, encryption/key deployment, migration and restore rehearsal, access-policy parity. | Runtime readiness after full restore remains closed until applied reconciliation succeeds. |
| Native/provider adapters | OpenAI, Anthropic, xAI, Ollama, reviewed OpenAI-compatible profiles, and trusted local-harness interface under explicit features. | Real credentials, endpoint/network policy, provider account retention/residency settings, live opt-in tests. | Generic endpoint/model authority and arbitrary child-process launch are unsupported. |
| OpenAI background runs | Exact submission, retrieval, webhook receipt, retry/deadline handling, terminal usage/budget/output reconciliation. | Worker scheduling/operations and provider-account configuration. | Ambiguous provider creation or conflicting terminal evidence enters recovery; it is never replayed. |
| Application tools | Explicit catalog, disclosure contracts, read-only loop, sequential one-mutation supervised flow, fresh principal/tool policy/resolver authorization. | Application operation registration, policy, logical target transport, delegated credential issuer, disclosure classification. | Recursive control-plane/introspection, autonomous writes, mixed/partial/parallel/stateless supervised execution are closed; generic parallel consequential execution is unsupported. |
| Provider-persistent files | Exact inline attachment input and deletion of already-known provider artifacts are independent implemented seams. | MIME/egress/storage policy for inline input and cleanup adapter behavior. | Provider upload/index/search and raw vector-store IDs are closed. |
| Configuration | Authenticated provider/profile credential, content-protection, retention, budget, and pricing lifecycles. | Administrative policy, recent-MFA deployment, secret/keyring backend, endpoint policy. | Durable tool-policy management remains closed until its constraints and applied restore are complete. |
| Backup/restore | Schema-module identity, entity/ordering metadata, restore hooks, pure reconciliation plan, and readiness-closed design. | Reviewed compatible backup adapter, object storage, privileged operator workflow, empty-target rehearsal. | Applied backup/row/object restore and automatic runtime opening are closed. |
| Recovery | Expired-lease reclaim, exact completed-result adoption, background reconciliation, cleanup/retention workers, recovery-required preservation. | Privileged operator evidence and audit integration. | Uncertain external effects/budget are never guessed, cleared, or replayed. |

## Release acceptance

For a candidate commit, record:

- exact crate, schema-module, `graphql-orm`, `graphql-orm-macros`,
  `agql-auth`, and storage dependency revisions;
- one resolved package/type universe;
- formatting, complete tests, warnings-denied Clippy, warnings- and
  missing-docs-denied Rustdoc, PascalCase SDL, backend compile profiles,
  release policy, packaging, and SemVer results;
- the owned disposable PostgreSQL result, or an explicit environmental skip
  that prevents a PostgreSQL production claim;
- migration and schema-fingerprint deltas;
- open `.handoffs/` prompts and their reviewed final SHAs, if returned; and
- every capability intentionally kept closed.

Passing the crate matrix does not publish the crate, change `publish = false`,
claim MSSQL production support, run a consumer migration, or authorize a
provider/application action.

## Consumer acceptance

Each consumer owner must independently verify its composed GraphQL SDL,
PascalCase/default naming choice, exact registered tool documents and
disclosure projections, authorization and tenant isolation, egress/residency,
budget policy, secrets/delegation/private transport, migration from its actual
deployed version, empty-target backup/restore, and operational rollback. These
tests belong to the consumer repository and must use only consumer-owned
disposable infrastructure.
