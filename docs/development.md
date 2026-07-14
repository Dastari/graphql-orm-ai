# Development and Verification

## Default checks

```bash
cargo fmt --check
cargo test --features provider-openai,provider-anthropic,provider-xai,provider-ollama,provider-openai-compatible,local-harness
cargo clippy --all-targets --features provider-openai,provider-anthropic,provider-xai,provider-ollama,provider-openai-compatible,local-harness -- -D warnings
RUSTDOCFLAGS="-D warnings -D missing_docs" \
  cargo doc --features provider-openai,provider-anthropic,provider-xai,provider-ollama,provider-openai-compatible,local-harness --no-deps
```

Check the optional naming contract independently:

```bash
cargo test --features graphql-case-pascal --test graphql_naming
RUSTDOCFLAGS="-D warnings -D missing_docs" \
  cargo doc --features graphql-case-pascal --no-deps
```

Compile other backends without connecting to them:

```bash
cargo check --no-default-features --features postgres
cargo check --no-default-features --features mssql
```

Run PostgreSQL behavioral parity only through the self-owning harness:

```bash
cargo test --no-default-features --features postgres \
  --test postgres_parity -- --test-threads=1
```

The test forces the local Docker socket, generates a container identity,
ownership label, credentials, database, and loopback port, and accepts no URL
from the environment or command line. It verifies the label again before
cleanup. CI treats an unavailable Docker socket as failure; a local developer
run reports a skip.

Do not run Cargo `--all-features`: the persistence backends are intentionally
mutually exclusive. Provider feature matrices must select exactly one backend.

## Database tests

Default tests use in-memory SQLite. PostgreSQL and MSSQL integration tests are
permitted only through a harness that creates and owns a disposable Docker
container, generated credentials, unique database, and cleanup. A generic
database URL is never accepted. The PostgreSQL parity test is the concrete
implementation of that rule; no MSSQL behavioral harness exists yet.

## Rustdoc

The crate denies rustdoc warnings and missing public documentation in CI.
Public APIs need useful documentation, not placeholder comments. Fallible
methods document `# Errors`; proof types explain the exact binding they
establish and any checks that still remain.

Private ORM derive output is kept in the private persistence module and is the
only scoped exception to generated missing-doc warnings.

## Release-policy check

Public Rust/runtime changes must update `README.md`, `CHANGELOG.md`, and
`MIGRATION.md` together. The policy also enforces crate-version movement and a
new schema-module version for persistence changes.

On a committed branch, compare against the reviewed base:

```bash
scripts/check-release-policy.sh <base-revision>
cargo semver-checks --baseline-rev <base-revision> --default-features
```

CI additionally runs `cargo-semver-checks` against a sibling baseline worktree
so local path dependencies resolve consistently. The explicit default-feature
selection is required: the backend features are mutually exclusive, while the
tool's ordinary heuristic attempts to enable every feature at once.
