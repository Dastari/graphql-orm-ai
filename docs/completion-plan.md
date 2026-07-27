# Completion Plan

This is the active execution plan for completing `graphql-orm-ai` from the
current `0.51.0` dependency-alignment checkpoint. The historical architecture
plan at `1573017:docs/plan.md` remains useful design context, but its original
delivery phases no longer describe the worktree: most foundation, provider,
persistence, authorization, approval, and coordination contracts are already
implemented.

[Implementation status](implementation-status.md) is authoritative for the
current inventory. This document is authoritative for work order, dependencies,
and exit gates. Update both whenever a slice changes what is implemented or
what remains deliberately closed.

## Baseline

- Crate version: `0.51.0` (unpublished pre-release).
- AI schema-module version: `0.47.0`.
- Exact reviewed dependencies:
  - `graphql-orm` and `graphql-orm-macros` `0.15.0` at
    `6beef53633befd90a4d4810887a3e4640dc4ad91`.
  - `agql-auth` `0.12.0` at
    `3f3b0c5365adfbe436514a681d977b600991b797`.
- SQLite/provider, warnings-denied Clippy, warnings/missing-docs-denied
  Rustdoc, PascalCase GraphQL, PostgreSQL/MSSQL compile-only, owned disposable
  PostgreSQL migration, release-policy, package, and SemVer checks pass
  locally.
- The dependency-alignment checkpoint is committed and pushed at
  `21f11e46f5e7b221959844cacba7f5ad81841e36`; draft PR #2 CI run
  `30253200905` passed all four jobs.
- No upstream handoff is currently open.

## Rules for every slice

1. Keep the crate project-agnostic. Consumer entities, policies, routes,
   deployments, and domain mutations are not implementation shortcuts.
2. Keep all sibling repositories read-only. If a missing reusable contract is
   discovered, stop that dependent slice, write a copy-ready prompt under
   `.handoffs/`, assign it to the owning repository agent, and wait for a
   reviewed final merge or release SHA.
3. Use generated `graphql-orm` repository, transaction, schema-module,
   migration, and restore APIs only. Do not introduce raw SQL or a direct
   database driver.
4. Rehydrate current principals before provider egress, every application tool,
   after approval, and at long-running checkpoints. Never persist bearer
   credentials or stale scope/role snapshots.
5. Keep tool registration, provider capability, webhook verification, and
   discovery separate from authorization.
6. Preserve exact egress, retention, budget, disclosure, fencing, and
   one-shot-approval proofs. Ambiguous external effects remain closed and are
   never replayed automatically.
7. Use only temporary/in-memory SQLite or a test-owned disposable Docker
   database. Never consume a host database URL.
8. For every public API, GraphQL, schema, persistence, authorization, feature,
   or behavioral change, update the changelog, migration guide, focused docs,
   Rustdoc, and schema-module version when required.
9. Finish each slice with the repository release matrix. Never use Cargo
   `--all-features` while database backends are mutually exclusive.

## Work order

```text
0. Durable 0.51.0 checkpoint
             |
             v
1. OpenAI background terminal reconciliation
             |
             v
2. Provider-persistent file lifecycle
             |
             v
3. Supervised ordering/history proof
             |
             v
4. Recovery, retention, and restore closure
             |
             v
5. Carefully gated coordination and review expansion
             |
             v
6. Control-plane and production integration closure
             |
             v
7. Backend production acceptance
```

Slices are ordered by dependency and risk. A later slice may be designed while
an earlier slice is under review, but its runtime boundary must remain closed
until every prerequisite exit gate passes.

## Slice 0: make the current checkpoint durable

Status: complete on the draft PR branch. Merge, tagging, and publishing remain
separate owner decisions.

### Work

- Review the complete `0.51.0` dependency-alignment diff.
- Verify one Cargo source/type universe for `graphql-orm`,
  `graphql-orm-macros`, and `agql-auth`.
- Commit and push the downstream-only change.
- Run branch CI and record the final downstream commit and CI result in
  [implementation status](implementation-status.md).
- Confirm the worktree is clean before beginning a schema or runtime change.

### Exit gate

- The exact reviewed dependency revisions resolve from a pushed commit.
- The complete local matrix and branch CI are green.
- No ignored handoff, credential, local path, or consumer-specific artifact is
  present in the commit.
- No sibling worktree or branch was mutated.

## Slice 1: OpenAI background terminal reconciliation

This is the first runtime implementation slice. Exact background submission and
verified webhook intake already exist, but an accepted run remains parked in
`WaitingProvider`, its budget remains uncertain, and a receipt grants no
authority to retrieve output or mutate the run.

Status: the complete design gate and negative/crash matrix are documented in
the two focused guides. Runtime and schema implementation have not started.

### Design gate

Before changing code, document the complete state machine and transaction
boundaries in [OpenAI background submission](openai-background.md) and
[OpenAI webhook intake](openai-webhooks.md). The design must cover:

- deterministic submission, verified receipt, provider response, run, attempt,
  generation, profile, model, output ceiling, storage, budget, egress, and
  retention matching;
- bounded worker claiming, leases, fencing, idempotency, redelivery, and
  concurrent reconciliation;
- just-in-time secret resolution and current-principal rehydration before
  provider retrieval and again before durable mutation;
- fixed-destination retrieval with bounded terminal status, output, usage, and
  unknown-field handling;
- exact-once budget/usage settlement and protected output persistence;
- cancellation, provider failure, malformed output, revoked authority, policy
  changes, expired retention, and ambiguous transport behavior;
- crash windows before retrieval, after retrieval, during protected
  persistence, and after terminal commit; and
- restore facts and readiness behavior for pending, complete, invalid, and
  recovery-required reconciliation.

### Implementation

- Add only the minimum durable state and indexes needed for bounded claims and
  exact-once terminal reconciliation.
- Add an OpenAI retrieval capability that cannot list responses, choose a
  destination, or retrieve an unbound response ID.
- Reuse provider normalization, disclosure, content protection, egress audit,
  pricing, usage, and run-fence contracts rather than creating a background
  bypass.
- Commit the protected assistant result, reconciled budget/usage facts,
  submission/receipt terminal states, immutable outcome, and run terminal
  transition with an explicitly reviewed atomicity boundary.
- Keep unsupported or unprovable provider states in a redacted,
  operator-reviewable recovery state.

### Required tests

- Exact successful receipt-to-submission-to-response completion.
- Polling or receipt selection cannot substitute for an exact durable match.
- Duplicate receipt, duplicate worker, and restart retry are idempotent.
- Mismatched provider/profile/model/response/storage/output metadata fails
  closed.
- Revoked/expired principal, changed session access, denied egress, expired
  retention, stale fence, or invalid budget prevents retrieval or mutation as
  appropriate.
- Usage cannot settle twice and uncertain capacity is not released without
  proof.
- Oversized, malformed, unsupported, incomplete, cancelled, and unknown
  provider responses cannot become assistant output.
- Every enumerated crash window has a deterministic retry, terminal, or
  recovery-required result.
- Restore readiness rejects invalid reconciliation facts and never replays an
  external call.
- SQLite and owned disposable PostgreSQL behavior agree.

### Exit gate

- A valid accepted background run can reach one protected terminal outcome
  without manual database intervention.
- A verified webhook alone still grants no execution authority.
- No ambiguous provider effect is automatically repeated.
- Budget and usage are reconciled exactly once.
- Focused docs, changelog, migration guide, schema contracts, and the full
  release matrix pass.

## Slice 2: provider-persistent file lifecycle

Build this on the existing attachment quarantine/release/reopening flow and the
exact profile-bound OpenAI deletion seam.

### Design gate

Define separate capabilities for upload, retrieval/use, search, and deletion.
The model must never list files, choose an arbitrary provider object, reuse a
file across an unauthorized scope, or turn a local attachment reference into
provider authority.

The design must bind:

- current principal, scope, session, attachment, released object hash and MIME;
- logical provider profile, model/capability, destination, provider file ID,
  purpose, expiry, and retention class;
- upload and search egress manifests, budgets, quotas, pricing dimensions, and
  immutable audit;
- local/provider object lifecycle, cleanup generations, exact absence proof,
  backup metadata, restore readiness, and deleting-session behavior; and
- derivatives to their exact source, producer, policy, and content-protection
  state.

### Implementation and tests

- Implement bounded upload and exact-reference use/search without list or
  arbitrary-ID APIs.
- Add attachment count/byte quotas and derivative records only where the full
  cleanup and restore lifecycle is defined.
- Extend pricing and settlement only for dimensions that can be normalized and
  reconciled authoritatively.
- Prove concurrent upload idempotency, cross-scope denial, content swap denial,
  interrupted upload cleanup, exact deletion, retention expiry, session
  deletion, and restore closure on SQLite and owned PostgreSQL.

### Exit gate

- Every provider-persistent object has exact creation authority, an owner and
  retention binding, bounded use, authoritative cost handling, and a
  deterministic deletion or operator-recovery path.
- The crate does not claim upload/search support for a provider until its
  entire lifecycle passes conformance tests.

## Slice 3: supervised ordering and history proof

This slice is a design deliverable, not permission to open additional runtime
paths.

### Work

- Specify canonical ordering for provider calls, parallel application calls,
  approvals, mutations, results, egress decisions, budgets, checkpoints, and
  continuation history.
- Define what can be adopted across generations without repeating a resolver or
  provider effect.
- Separate safe completed-batch adoption from partial read-only work,
  consequential work with consumed approval, and unknown external effects.
- Define capacity accounting before approval/checkpoint consumption so a later
  continuation cannot exceed loop, provider, tool, or budget bounds.
- Define stateless transcript reconstruction rules for each provider family,
  including reasoning or provider-owned state that cannot be safely replayed.
- Review whether partial or parallel consequential batches should remain
  permanently unsupported rather than forcing an unsafe generic abstraction.

### Exit gate

- The proof has reviewable invariants, state transitions, crash windows,
  negative tests, and explicit unsupported cases.
- No runtime path is opened merely because a provider advertises parallel or
  multi-call capability.
- Any missing reusable ORM/auth primitive has a copy-ready upstream handoff and
  the dependent implementation remains blocked pending a reviewed final SHA.

## Slice 4: recovery, retention, and restore closure

Complete lifecycle safety before broadening orchestration.

### Work

- Add bounded, privileged uncertain-call recovery based on exact evidence;
  never infer success or replay a consequential effect.
- Finish deleting-session and age-based purge handling for provider objects,
  attachments/blobs, protected payloads, checkpoints, and other erasable
  content while preserving required redacted security facts.
- Define explicit treatment for append-only usage/audit facts, active or
  recovery-required runs, accepted proposals, and dependency-ambiguous
  artifacts instead of claiming erasure.
- Implement backup adapter execution and applied restore transactions where
  current reviewed dependency contracts suffice.
- Keep runtime startup, workers, subscriptions, and callbacks closed until
  reconciliation and restore application both succeed.

### Upstream gate

Inspect sibling APIs read-only before implementation. If portable transaction,
backup, migration, encrypted-field, or restore primitives are missing, write a
prompt in `.handoffs/` for the owning agent. Do not add SQL, backend-specific
workarounds, substitute types, or downstream copies.

### Exit gate

- Every externally uncertain or content-bearing state has a bounded normal,
  terminal, retained-with-reason, or privileged-recovery outcome.
- Backup/restore preserves required bindings and cannot reopen stale authority.
- Retention reports distinguish logical tombstoning, physical content/blob
  deletion, retained audit facts, and blocked items truthfully.

## Slice 5: gated coordination and review expansion

Implement only the paths admitted by the Slice 3 proof, in this order:

1. validated provider-turn checkpoint adoption;
2. safe partial read-only batch recovery, if proven;
3. sequential multi-call provider-retained supervision;
4. mixed read/write supervision with a fresh checkpoint before every
   consequential action;
5. parallel read-only calls; and
6. stateless supervised continuation only for providers whose complete visible
   history can be reconstructed safely.

Parallel consequential execution, autonomous writes, or replay of ambiguous
effects may remain intentionally unsupported. Completion means a defensible
closed boundary, not enabling every provider feature.

Also add generic per-item proposal review while leaving application-specific
rendering and final domain mutation in the consumer.

### Exit gate

- Every consequential action has fresh current authority, exact policy and
  resource versions, recent assurance when required, a canonical preview,
  one-shot approval, a current resolver authorization check, and its own
  egress/budget checkpoint.
- Retries and cross-generation adoption cannot duplicate provider or
  application side effects.
- Unsupported batch shapes fail before approval consumption or external I/O.

## Slice 6: control-plane and production integration closure

### Work

- Complete the authenticated tool-enablement configuration lifecycle.
- Generate resolver-operation disclosure metadata where reviewed upstream
  contracts expose sufficient information.
- Complete schema-aware recursion/control-plane validation while retaining the
  explicit reviewed catalog and fail-closed scanner as defense in depth.
- Complete richer provider file preflight and built-in result normalization for
  every capability the crate advertises.
- Add conformance suites for production secret stores/keyrings, delegated
  credential issuers, and private GraphQL transports without embedding
  deployment-specific implementations.
- Confirm documentation clearly distinguishes a required host implementation
  from a missing crate implementation.

### Upstream gate

Resolver metadata generation, portable encrypted fields, backend migration
behavior, or other reusable dependency work belongs upstream. If an existing
reviewed API is insufficient, create an upstream handoff and wait; never patch
the sibling repository from this worktree.

### Exit gate

- Configuration and disclosure management are authenticated, bounded,
  versioned, auditable, and fail closed on schema drift.
- Every production host seam has a public conformance contract and no insecure
  built-in fallback.

## Slice 7: backend production acceptance

Run acceptance separately for each backend and capability profile.

### SQLite and PostgreSQL

- Run formatting, complete tests, warnings-denied Clippy, warnings and
  missing-docs-denied Rustdoc, GraphQL naming/SDL contracts, SemVer, packaging,
  release policy, and privacy checks.
- Rehearse prior-to-current migration and backup/restore in test-owned stores.
- Run concurrency, stale-fence, crash-window, retention, reconciliation, and
  provider conformance suites.
- Publish an exact capability matrix that distinguishes implemented,
  host-supplied, deliberately unsupported, and experimental behavior.
- Require consumer owners to perform their own schema composition, migration,
  restore, authorization-parity, and deployment tests.

### MSSQL

MSSQL production support remains a separate claim. Do not advertise it until
reviewed upstream write, transaction, migration, policy, queue, stream,
encryption, backup, and concurrency parity exists and this crate's disposable
MSSQL matrix passes. Any required upstream work is delegated through
`.handoffs/`; it is never implemented here.

### Exit gate

- The selected backend/capability profile meets every applicable production
  acceptance criterion with recorded evidence.
- The exact release commit, dependencies, schema module, SDL, migration guide,
  changelog, and capability documentation agree.
- Publishing remains an explicit owner decision; passing this gate does not
  silently change `publish = false`.

## Intentionally host- or consumer-owned

The following do not move into this crate:

- consumer schema composition, migration acceptance, integration tests, and
  restore rehearsal;
- application entities, tenant/domain policy, routes, final proposal rendering,
  and domain mutations;
- concrete principal/session persistence and current-principal resolver
  implementations;
- deployment-specific delegated credential issuance, private network
  transport, audit integration, and secret/key management;
- production OS/container implementation of the trusted local-harness launcher;
  and
- frontend routing or execution of typed UI intents.

The crate should provide safe traits, exact bindings, fail-closed defaults, and
conformance tests for these seams. It should not absorb deployment authority.

## Current queue

1. Implement Slice 1's minimum submission claim/lease fields, legal state
   validation, schema migration, and bounded concurrent claim tests.
2. Add the fixed-destination OpenAI retrieval binding and terminal normalizer.
3. Add the one-transaction terminal graph, restore validation, PostgreSQL
   parity, and full release-matrix proof.
