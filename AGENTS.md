# Repository Rules

These rules apply to every human or automated change in this repository.

## Project boundary

- Keep `graphql-orm-ai` project-agnostic. No consumer crate, product entity,
  route, tenant policy, deployment topology, or domain mutation belongs in
  `src/`, public examples, fixtures, or generated GraphQL names.
- Consumers extend the crate through typed tools, proposal schemas, access and
  egress policy, logical GraphQL targets, providers, storage, and auth traits.
- Application work executes through authenticated GraphQL resolvers. Do not
  add raw SQL, direct application repository access, shell, or arbitrary
  model-authored GraphQL execution.
- Database syntax and backend-specific migrations belong in `graphql-orm`.
  Authentication, principal lifecycle, assurance, and reusable delegation
  primitives belong in `agql-auth`.

## Repository ownership and upstream handoffs

- The agent working in this repository owns only `graphql-orm-ai`. Treat
  sibling repositories, including `agql-auth` and `graphql-orm`, as read-only.
  Do not edit, format, commit, rebase, merge, stash, clean, switch branches, or
  otherwise mutate their worktrees or GitHub branches.
- Read-only inspection of sibling source, tags, PRs, and dependency metadata is
  allowed when needed to define an integration requirement.
- Never implement an upstream change from this repository, regardless of its
  size or urgency. Every required change to `graphql-orm`, `agql-auth`, or any
  other upstream crate must be expressed as a copy-ready prompt in `.handoffs/`
  and assigned to a separate owning agent. Until that owner returns a reviewed
  final merge or release SHA, this repository remains read-only and blocked on
  that upstream requirement.
- When a reusable upstream change is required, stage a copy-ready prompt in
  `.handoffs/` for the owning repository agent. That directory is deliberately
  ignored so temporary coordination state is not published with the crate.
- Use one owning agent and one isolated branch/worktree per repository. An
  owning agent may create its repository's implementation PR; downstream
  agents wait for the reviewed upstream merge and final commit SHA.
- After an upstream merge, update exact dependency revisions only in this
  repository, then regenerate `Cargo.lock`, verify one dependency universe,
  update release documentation, and rerun the full matrix.
- A squash or rebase merge invalidates downstream pins to PR-head commits. A
  merge commit may retain ancestry, but downstream crates should still repin to
  the reviewed final `main` or release-tag commit before merging.

## Database and integration safety

- Never connect tests, migrations, diagnostics, or development commands to a
  live local, development, staging, or production PostgreSQL/MSSQL database.
- SQLite tests use temporary or in-memory stores.
- PostgreSQL/MSSQL tests must create and own a disposable Docker container,
  generated credentials, a unique database, and cleanup. Never fall back to
  `DATABASE_URL` or `TEST_DATABASE_URL`.
- Do not run integration tests against consumer applications from this
  repository. Consumer agents own their integration and migration tests.

## Security invariants

- Tool discovery is not authorization; registration and enablement are
  default-deny.
- Rehydrate current principals before provider egress, every application tool,
  after approval, and at long-running checkpoints. Never persist bearer
  credentials or stale scope/role snapshots.
- Provider disclosure requires an exact egress proof and atomic budget proof.
- Application tool results require a fingerprint-bound static disclosure
  schema. Runtime classification can only tighten the static result.
- Approval never substitutes for resolver authorization. Consequential actions
  use a server-generated canonical preview and exact one-shot binding.
- Every worker/provider result is fenced. Restore keeps the runtime closed
  until reconciliation succeeds.

## Change documentation and SemVer

- Update `CHANGELOG.md` under `Unreleased` for every user-visible API,
  behavior, feature, security, provider, GraphQL, or persistence change.
- Update `MIGRATION.md` in the same change for every public Rust API, GraphQL
  SDL, feature/default, configuration, authorization, persistence,
  backup/restore, or behavioral contract change. State explicitly when no data
  migration is needed.
- Any entity, index, constraint, or persistent semantic change must bump
  `AI_SCHEMA_MODULE_VERSION`; never reuse an applied module version.
- Follow SemVer, including pre-1.0 breaking changes. Bump `Cargo.toml` before a
  release/compatibility branch and run `cargo-semver-checks` against the
  reviewed base or tag. Rust API checks do not replace GraphQL SDL and schema
  migration checks.
- Keep `Cargo.lock`, dependency source identity, sibling versions, README
  examples, changelog, and migration guide consistent.

## Documentation and verification

- Document every public Rust item. Fallible public APIs include `# Errors`;
  security-sensitive APIs describe what the type proves and does not prove.
- Keep the root README concise and route detailed guidance through
  `docs/README.md`.
- Before handoff run formatting, tests, warnings-denied Clippy, warnings-denied
  rustdoc, PascalCase GraphQL contract tests, and compile-only PostgreSQL/MSSQL
  checks. Never use `--all-features` while backend features are mutually
  exclusive.

See `docs/development.md` and `docs/release-process.md` for exact commands.
See `docs/upstream-contributions.md` for the multi-repository workflow.
