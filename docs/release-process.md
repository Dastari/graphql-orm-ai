# Release Process

## Change classification

For every change, classify all affected contracts:

- Public Rust API and Cargo features/defaults.
- GraphQL SDL and naming.
- Persistent entities, indexes, constraints, data semantics, and schema-module
  version.
- Configuration, authorization, egress, approval, budget, provider, backup,
  restore, and operational behavior.

Keep `README.md` aligned with the current public surface, update `CHANGELOG.md`
for user-visible changes, and update `MIGRATION.md` for every contract category
above, including an explicit “no data migration required” statement where
applicable. The release-policy check requires all three files whenever public
Rust or runtime source changes.

## SemVer

Use Cargo SemVer rules, including the stronger compatibility implications of
pre-1.0 minor versions. `cargo-semver-checks` is mandatory but does not cover
all Rust type changes, GraphQL SDL, persistence schemas, generated macro output,
or runtime behavior; review those separately.

Public source changes require a crate version change relative to the reviewed
release/base branch. Persistent schema changes also require a new
`AI_SCHEMA_MODULE_VERSION`. Never rewrite an applied schema-module version.

## Release gate

1. Confirm one exact reviewed dependency universe and full Git revisions for
   unpublished sibling crates.
2. Run `scripts/check-release-policy.sh <release-base>`.
3. Run formatting, tests, warnings-denied Clippy, warnings- and
   missing-docs-denied Rustdoc, PascalCase SDL, and compile-only backend checks
   from `docs/development.md`.
4. Run `cargo semver-checks --baseline-rev <release-base>
   --default-features` against the reviewed baseline. Do not allow its ordinary
   all-compatible-features heuristic to combine mutually exclusive backends.
5. Review GraphQL SDL, schema-module metadata/fingerprint, migration and restore
   behavior, backup inclusion, and public error changes.
6. Confirm no test used a live database or real consumer integration.
7. Move `Unreleased` notes to the release version/date, update `Cargo.toml` and
   `Cargo.lock`, commit, and create an annotated tag.

Git consumers pin the reviewed full tag commit. Do not depend on a moving
default branch.
