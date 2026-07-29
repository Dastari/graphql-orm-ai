# Recovery, Retention, Backup, and Restore

Status: Slice 4 audit complete; ordinary lifecycle closure is implemented,
while applied backup/restore and privileged uncertain-effect resolution remain
blocked.

This guide classifies every current externally uncertain or content-bearing
state against the [canonical ordering proof](ordering-history.md). It does not
grant an operator authority to infer success, repeat an effect, release
uncertain budget, or reopen a restored runtime.

## Current lifecycle classification

| State/fact | Current outcome |
|---|---|
| Pre-provider expired lease | Requeue under a new attempt/generation |
| Exact final-output checkpoint | Finalize the already durable output |
| Exact complete read-only batch | Requeue only for current-authority adoption |
| Exact complete single supervised retained-result batch | Requeue only for current-authority adoption |
| Accepted OpenAI background response | Bounded exact terminal reconciliation |
| Ambiguous provider/resolver/mutation transport | `RecoveryRequired`; never replay |
| Uncertain budget without authoritative usage | Full reservation remains held |
| Live approval wait | Bounded current-policy keep/cancel/recovery classification |
| Restored approval/tool/provider wait | `RecoveryRequired`; never live-resumed |
| Released attachment/local blob | Exact fenced cleanup; ambiguity retains reference |
| Provider-file artifact | Exact profile-bound deletion/absence proof or retained blocked state |
| Protected message/proposal/tool/approval/context/checkpoint content | Bounded age/deleting-session dependency proof or retained blocked state |
| Usage, pricing, audit, egress, run-attempt/outcome facts | Append-only retained security/accounting evidence |
| Deleted session shell | Empty-title hidden tombstone with redacted lifecycle evidence |

`RecoveryRequired` is itself a truthful retained-with-reason outcome. It is not
a queue state. Current code provides no generic operator mutation that changes
it, because provider truth and application mutation truth require different
authoritative evidence.

## Privileged uncertain-effect recovery contract

A future privileged recovery service may perform only one of these bounded
actions:

1. attach authoritative provider usage and settle the exact uncertain
   reservation while leaving the run closed;
2. record authoritative proof that transport never began and release the exact
   unused reservation while terminally closing the attempt; or
3. record a host-owned application-effect reference and retain the run,
   approval, tool, and budget facts for manual/domain reconciliation.

It must never synthesize assistant output, reconstruct a tool result, mark an
application mutation successful, requeue a consequential effect, reuse an
approval, or clear a provider/object reference. A host policy must authorize
the exact recovery action under a freshly rehydrated operator with recent MFA.
The request must bind the run, source attempt, generation, reservation,
provider/profile/model or tool/approval identity, expected row versions,
evidence kind, safe external evidence reference, and intended terminal
classification.

One generated-ORM state-machine transaction must revalidate the complete
durable graph, settle any authoritative budget/usage exactly once, append a
redacted immutable audit and recovery epoch, and leave no live lease. Exact
replay may return the already recorded decision; conflicting evidence remains
closed. A generic free-form “operator override” is permanently unsupported.

This service is not implemented yet. The existing provider-neutral budget
reconciler deliberately requires a current running fence, while the specialized
background terminal transaction owns only exact OpenAI response evidence.
Generalizing that arithmetic and recovery evidence is downstream work and does
not require an ORM/auth change.

## Retention truth

The current retention service distinguishes:

- logical session deletion;
- protected payload tombstoning;
- exact local/provider object deletion;
- append-only checkpoint maintenance;
- retained redacted security/accounting facts; and
- blocked dependencies.

It never reports complete erasure merely because a row became invisible.
Accepted proposals without a recorded applied outcome, nonterminal or
recovery-required runs, active/uncertain tool authority, unconsumed approvals,
ambiguous artifacts, current coordinator checkpoints, and over-bound histories
remain blockers. Append-only usage, pricing, audit, egress, attempts, and
attempt outcomes are retained. The complete details and bounded transaction
order are in [session retention](session-retention.md).

No further retention mutation is admitted while applied restore remains
unavailable: changing what a backup can no longer recover would make the
restore claim weaker, not more complete.

## Backup and applied restore boundary

`AiSchemaModule` exposes all private entity metadata, backup descriptors, the
module fingerprint, and ordered preflight/reconcile/validate/readiness hook
declarations. `AiRestoreReconciler` already produces a side-effect-free plan:

- terminal runs stay terminal;
- safe pre-effect or exact adopted-batch states may receive a new attempt;
- waits and uncertain effects become `RecoveryRequired`;
- provider continuations/files require re-verification;
- pending approval/consent counts remain explicit; and
- invalid encryption, attachment, usage, budget, pricing, skill, rule,
  checkpoint, webhook, background, UI-intent, retention, or stream facts keep
  readiness closed.

What is missing is the production adapter chain that:

1. creates/verifies a manifest backup through `graphql-orm-backup`;
2. restores database rows and exact referenced attachment objects into a test-
   owned empty target;
3. collects the restore facts from the restored rows rather than accepting
   unbound caller assertions;
4. applies each planned run/approval/consent repair through generated ORM
   transactions;
5. revalidates every post-apply invariant without provider/application I/O;
6. records the exact recovery epoch; and
7. opens readiness only for that applied, zero-fatal epoch.

`graphql-orm-backup` 0.6.0 is merged at
`6a9ccedd76fd140c351c8861de72c4cb7c99feea` and is pinned here against the
same reviewed ORM 0.16.0 and storage 0.5.0 revisions. Cargo resolves one
database/metadata/storage type universe. AI schema module 0.51.0 also includes
finalized local attachment and artifact object keys in the confidential
database export while continuing to redact quarantine, upload-token, provider,
credential, and secret references.

This satisfies the upstream compatibility gate, not the applied-restore exit
gate. The checkpoint intentionally exposes no production restore service.
Runtime startup, workers, subscriptions, webhook callbacks, and provider
access remain closed after raw database/object import until the AI-specific
collector, generated-ORM repair applier, post-apply validator, exact recovery
epoch, and readiness gate are implemented and exercised through SQLite and an
owned disposable PostgreSQL round trip. Consumer owners must still rehearse
their composed schema and object store separately.

The owning ORM agent is considering consolidation of the `graphql-orm-*`
packages. This branch is therefore stopped at the exact reviewed dependency and
backup-schema checkpoint. Any later repository-layout change must arrive as a
reviewed upstream commit, be repinned here without sibling mutation, and pass
the complete dependency and backend matrix.
