# Migration Guide

`graphql-orm-ai` is not yet published. This guide is still mandatory so early
Git consumers and disposable test deployments can track schema and API changes
without guessing.

## Unreleased: deleting-session proposal tombstones (crate 0.38.0 to 0.39.0; schema 0.36.0 to 0.37.0)

Apply AI schema module `0.37.0` while session, proposal, retention, attachment,
coordinator, and restore workers are closed. Do not run 0.39.0 code against a
module still registered as 0.36.0. The generated migration keeps the existing
39 private entities, adds nullable `payload_purged_at` to the proposal table,
and makes proposal `protected_payload`/`source_references` plus proposal-item
`protected_suggested_value`/`source_references` nullable. Use only the generated
`graphql-orm` migration; no application SQL, consumer-table change, blob-key
rewrite, or application-authored data copy is required. Existing non-null
payloads and sources remain unchanged until retention proves them eligible.

At and after the exact current deleting-session cutoff, repeated bounded scan
cycles now order protected content as context summaries, proposal payloads,
attachment cleanup, message bodies, and terminal coordinator checkpoints. A
proposal pass first proves the complete session proposal/item set is within its
lookahead bounds. It retains every accepted or accepted-edited proposal because
the ordinary application mutation or authoritative outcome recorder may still
be pending. It may tombstone only rejected, applied, expired, or expired
pending-review proposals whose owning run is terminal. The same transaction
clears all protected item values/rationales/sources/review values, clears the
parent protected payload/sources, writes `payload_purged_at`, changes an expired
pending review to `expired`, and appends redacted audit. Identity, schema,
logical item count, review decisions, creator/reviewer, applied resource and
application-audit references, timestamps, state, and CAS versions remain.

The existing `AiSessionRetentionLimits` constructors remain source-compatible
and derive proposal/item defaults from message/block limits. Call
`with_proposal_limits` for independent `1..=5_000` proposal and `1..=20_000`
item bounds; new getters return both. `AiSessionRetentionReport` adds
`deleting_session_proposal_payloads_purged` and
`proposal_payload_purges_blocked`. Downstream exhaustive struct literals must
initialize the new fields or use `..Default::default()`.

This is a pre-1.0 public Rust API, private persistent-shape,
backup/schema-fingerprint, and retention-behavior change. It changes no public
GraphQL SDL, Cargo feature/default, table/entity count, append-only policy, or
consumer schema. Deploy the generated schema migration, then run retention in
complete repeated cycles. One pass is not an erasure certificate; tool calls,
approvals, provider raw payloads, proposal metadata, attachment artifacts,
session shells, and immutable audit/usage/history facts remain.

## Unreleased: verified deleting-session attachment cleanup (crate 0.37.0 to 0.38.0; schema 0.35.0 to 0.36.0)

Apply AI schema module `0.36.0` while session writers, attachment upload and
cleanup workers, retention workers, provider attachment reopeners, and restore
callbacks are closed. Do not run 0.38.0 code against a module still registered
as 0.35.0. The generated migration adds no table, column, index, constraint,
or entity and rewrites no row data. It advances the module because existing
attachment `quarantine_state`/`processing_state` values now include a private
`deleting`/`retention_cleanup_required` transition whose meaning is bound to
the exact current session-deletion cutoff. No consumer table, protected-payload
rewrite, blob-key rewrite, or application SQL is required.

Hosts must schedule both maintenance services. After
`deleted_at + deleted_content_purge_seconds`,
`OrmAiSessionRetentionService` proves the current exact scope policy and a
whole-session attachment lookahead bound. Artifact-free rows that still own or
may own storage enter the retention cleanup state by CAS; no reference is
cleared at this step. `OrmAiAttachmentService::cleanup_once` then reloads the
session and policy in its claim transaction, re-proves the cutoff, claims one
generation, deletes only the stored opaque final/quarantine references, and
verifies absence. Storage errors or ambiguous absence checks preserve the
references in bounded backoff. A later retention pass may physically delete an
ordinary attachment row only when it has no artifacts, both blob references
and its upload capability hash are absent, its cleanup is complete, and its
deleted timestamp plus a positive cleanup generation are present. Linked
message scrubbing can proceed only after that metadata deletion.

Attachment artifacts—including provider-file references, derivative blobs,
or protected artifact content—remain blockers. This release does not infer
provider deletion, clear an artifact, or weaken any append-only fact. Runs,
attempt history, non-checkpoint immutable facts, tool/proposal payloads, and
session shells remain. One report is not an erasure certificate.

The existing `AiSessionRetentionLimits` constructors remain source-compatible
and use their message bound as the default attachment bound. Call
`with_attachment_limit` to set an independent `1..=5_000` whole-session proof
bound; `maximum_attachments_per_session` returns it.
`AiSessionRetentionReport` adds
`deleting_session_attachment_cleanups_requested`,
`deleting_session_attachments_deleted`, and `attachment_cleanups_blocked`.
Downstream exhaustive struct literals must initialize the new public fields or
use `..Default::default()`.

This is a pre-1.0 public Rust API and persistent lifecycle-behavior change. It
changes no GraphQL SDL, Cargo feature/default, entity shape, append-only policy,
or protected row representation. No row-data migration is needed, but hosts
must deploy the new module version and run cleanup plus retention repeatedly;
running retention without cleanup intentionally leaves attachments blocked.

## Unreleased: terminal run-checkpoint purge (crate 0.36.0 to 0.37.0; schema 0.34.0 to 0.35.0)

Update the exact `graphql-orm` pin from 0.7.0 to 0.9.0 at
`f996cdbe2ef1867dea029ec3ff16e051dbe7566e`, refresh one dependency lockfile,
and apply AI schema module `0.35.0` while session writers, coordinator workers,
retention workers, and restore callbacks are closed. Do not run 0.37.0 code
against a module still registered as 0.34.0. The generated migration adds no
table, column, index, constraint, or entity and rewrites no row data. It does
change managed append-only enforcement for
`graphql_orm_ai_run_checkpoints`: SQLite and PostgreSQL gain the reviewed
transaction-scoped retention-delete path while ordinary update/delete remains
prohibited. Checkpoint IDs also become privately sortable so bounded retention
pages use stable `created_at ASC, id ASC` ordering. PostgreSQL enforcement
objects and managed row-security integration are regenerated by `graphql-orm`;
hosts must not reproduce them with application SQL.

At and after the existing deleting-session cutoff, repeat bounded complete
scan cycles. The worker still removes protected events and context summaries
before eligible message content. Only after those sources are exhausted and
the configured run page proves every run terminal does an ordinary
state-machine transaction validate each current checkpoint and clear
`latest_checkpoint_id`. A separate retention transaction then reloads the
session and policy, repeats the empty-source and terminal-run proofs, requires
all pointers to be absent, purges one exact bounded checkpoint ID set, and
appends a redacted audit in the same commit. A crash between transactions
leaves an orphan checkpoint for a later pass rather than a dangling pointer.

CAS conflicts during ordinary pruning now roll back the entire per-session
transaction before incrementing `sessions_conflicted`. Hosts may safely retry a
later scan cycle; no earlier deletion or pointer change from the conflicted
transaction is committed without its audit.

Only run checkpoints opt in. Immutable run attempts/outcomes, pricing and
skill versions, usage, egress, and audit facts remain non-purgeable. Runs,
messages, session shells, attachments/external objects, tool/proposal payloads,
and unsafe dependencies also remain. MSSQL remains schema-only/read-only and
does not opt the entity into retention purge. One report is not an erasure
certificate.

The existing `AiSessionRetentionLimits` constructors remain source-compatible.
They derive run and checkpoint bounds from their existing message/context
bounds; call `with_run_checkpoint_limits` to set independent values. New public
getters expose those values. `AiSessionRetentionReport` adds
`deleting_session_run_checkpoint_references_cleared`,
`deleting_session_run_checkpoints_deleted`, and
`run_checkpoint_purges_blocked`; exhaustive downstream struct literals must
initialize them or use `..Default::default()`.

Constructing `OrmAiSessionRetentionService` installs the exact
`graphql_orm_ai.run_checkpoint.retention_purge` entity-policy grant on the
service's cloned database handle while delegating every other access surface
to the handle's existing policy. This construction is the host's explicit
enablement of trusted maintenance; it grants no GraphQL or arbitrary purge
surface. Existing row policy may still deny a selected checkpoint.

This is a pre-1.0 public Rust, dependency, database-enforcement, authorization,
backup/schema-fingerprint, and runtime-behavior change. It changes no GraphQL
SDL, Cargo feature/default, entity shape, or protected row representation. No
AI row-data migration, consumer-table migration, or protected-payload rewrite
is required.

## Unreleased: context-first deleting-session retention (crate 0.35.0 to 0.36.0; schema 0.33.0 to 0.34.0)

Apply AI schema module `0.34.0` while session writers, context workers,
retention workers, subscriptions, and restore callbacks are closed. Do not run
0.36.0 code against a module still registered as 0.33.0. The generated
migration adds no table, column, index, constraint, or entity and still owns 39
private records. It advances the module because existing session,
context-checkpoint, message/block, retention-policy, and audit records now have
a context-before-content deletion meaning. No data copy, protected-payload
rewrite, consumer table, or application SQL is required.

After the existing deleting-session cutoff, each
`OrmAiSessionRetentionService` transaction now loads at most the configured
context-checkpoint bound. A nonempty page is validated and deleted atomically,
and all message scrubbing is deferred for that session until a later pass.
Hosts must repeat complete bounded scan cycles until all protected context
summaries are gone; only then can eligible terminal unattached message content
be scrubbed. Protected event deletion may progress in the same earlier passes.
This order prevents a retained summary from outliving message content it may
cover.

The existing four-argument `AiSessionRetentionLimits::new` remains compatible
and uses `maximum_messages_per_session` as the context-checkpoint bound. Hosts
that need an independent limit should migrate to
`new_with_context_checkpoints`. `AiSessionRetentionReport` adds the public
`deleting_session_context_checkpoints_deleted` field; downstream exhaustive
struct literals must initialize it or use `..Default::default()`.

This slice deletes only deleting-session context-summary rows. Ordinary
message-retention invalidation, context-summary production/selection, run and
coordinator checkpoints, tool/proposal payloads, attachment/external content,
append-only facts, and final session-shell deletion remain closed. The context
producer must stay disabled until exact source coverage and ordinary-retention
invalidation are implemented. This is a pre-1.0 public Rust API and persistent
behavior change with no GraphQL SDL or data migration.

## Unreleased: deleting-session content cutoff (crate 0.34.0 to 0.35.0; schema 0.32.0 to 0.33.0)

Apply AI schema module `0.33.0` while session writers, retention workers,
subscriptions, and restore callbacks are closed. Do not run 0.35.0 code
against a module still registered as 0.32.0. The generated migration adds no
table, column, index, constraint, or entity and still owns 39 private records.
It advances the module because existing session, protected event,
message/block, run, attachment, retention-policy, and audit records now have a
deleting-session content-cutoff meaning. No data copy, protected-payload
rewrite, consumer table, or application SQL is required.

Hosts should continue scheduling `OrmAiSessionRetentionService` as bounded
keyset scan cycles. For an exact `deleting` session with a valid `deleted_at`,
the worker now compares that timestamp plus the current scope policy's
`deleted_content_purge_seconds` to its trusted clock. Before the cutoff, the
existing ordinary live-delta/message-retention rules apply. At and after the
cutoff, each bounded session transaction may delete every protected session
event kind and scrub eligible terminal unattached message previews and blocks
even when `message_retention_seconds` is absent. Repeat complete scan cycles
until operational telemetry shows no further eligible rows; one report is not
an erasure certificate.

The worker preserves session/message metadata, unsafe message content linked
to nonterminal runs or attachments, attachments/blobs, provider-persistent
files, raw provider/tool payloads outside these rows, checkpoints, proposals,
approvals, usage, egress decisions, audit facts, fencing, and restore evidence.
It appends a redacted `session_deletion_retention_expired` audit in the same
transaction as each changed session. Those retained dependencies require
separately ordered workers; this slice begins but does not complete the
`deleting` lifecycle. Append-only retention remains closed until the reusable
generated-ORM deletion primitive is reviewed upstream.

`AiSessionRetentionReport` adds the public
`deleting_session_events_deleted` field. Downstream exhaustive struct literals
must initialize it or use `..Default::default()`. This is a pre-1.0 public Rust
API and persistent-behavior change with no GraphQL SDL or data migration.

## Unreleased: live approval-wait reconciliation (crate 0.33.0 to 0.34.0; schema 0.31.0 to 0.32.0)

Apply AI schema module `0.32.0` while run claimers, approval workers, generic
expired-lease recovery, and restore callbacks are closed. Do not run 0.34.0
code against a module still registered as 0.31.0. The generated migration adds
no table, column, index, constraint, or entity and still owns 39 private
records. It advances the module because existing approval, tool-call, run-step,
provider-turn checkpoint, run, event, audit, and attempt-outcome records now
have a live approval-wait reconciliation meaning. No data copy, protected
payload rewrite, consumer table, or application SQL is required.

Hosts should construct `OrmAiApprovalWaitReconciliationService` with the same
generated ORM database, run service, current-principal resolver, content
protection boundaries, clock, and a current
`AiApprovalWaitReconciliationPolicy`. Configure positive principal/wait
durations and a bounded `1..=256` candidate scan through
`AiApprovalWaitReconciliationLimits`. The policy's decision may only leave an
exact pending/approved wait parked or cancel it; it is not approval, resolver,
provider, egress, or replay authority.

Run `reconcile_waits` before `OrmAiRunService::recover_expired_leases` in each
live worker cycle. The reconciler does not heartbeat or poll a human wait.
Generic expired-lease recovery no longer selects `WaitingApproval`; the
dedicated worker owns its decision, policy, expiry, and deployment-cutoff
transition. `WaitingTool` and other externally ambiguous expired states remain
conservative recovery cases.
Denied, revoked, expired, deployment-cutoff, deleted-session, and current-policy
cancellations atomically close the run/call/step fence and append protected,
redacted, and immutable outcome facts. Valid pending/approved waits remain
unchanged. Exact CAS races are reported and can be reconsidered by the next
bounded cycle. Malformed or unprovable checkpoint/budget/call/step/approval
linkage moves the run to `RecoveryRequired` without changing the linked
approval or call.

Approved work still uses only `claim_next_approved`, fresh preview/policy/rule
validation, one-shot consumption, and the ordinary authenticated GraphQL
resolver path. The reconciler never claims or resumes it. During snapshot
restore, keep all workers closed: restored `WaitingApproval` and `WaitingTool`
states continue to become `RecoveryRequired` through restore reconciliation
and are deliberately not eligible for this live worker. This is a behavioral
and public Rust API change with no GraphQL SDL or data migration.

## Unreleased: bounded sequential supervised coordinator (crate 0.32.0 to 0.33.0; schema 0.30.0 to 0.31.0)

Apply AI schema module `0.31.0` while workers, provider calls, approval waits,
and restore callbacks are closed. Do not run 0.33.0 code against a module still
registered as 0.30.0. The generated migration adds no table, column, index,
constraint, or entity and still owns 39 private records. It advances the module
because the existing provider-turn, approval-wait, supervised-result
checkpoint, and run states now have a top-level sequential orchestration
meaning. No data copy, consumer table, application SQL, or protected-payload
rewrite is required.

Hosts may now construct `AiSupervisedAgentCoordinator` from their existing
fenced run control, provider executor, protected output/checkpoint services,
consequential approval service, supervised resume service, current-rule
resolver, trusted clock, and a new `AiSupervisedAgentTurnPlanner`. Route normal
queue claims to `execute_claimed`; route one-owner
`OrmAiRunService::claim_next_approved` results to `execute_approved_claim`.
Never call both entry points concurrently for one run fence.

Every `AiSupervisedAgentTurnPlan` must contain only exact registered
`SupervisedWrite`/`OneShot` definitions, use provider-retained continuation,
match the resolved-rule scope/fingerprint, and carry a current server-selected
result-egress route. Initial plans have no continuation. Continuation plans
must use `AiProviderCallPlan::new_supervised_continuation_with_tools` with the
opaque result supplied by the coordinator; do not reconstruct call IDs,
provider response IDs, or model-visible result blocks.

The coordinator checkpoints each accepted provider result before staging one
canonical-preview approval and then returns `WaitingApproval` without
heartbeating through the human wait. After approval, the existing resume
service reopens the exact provider checkpoint, consumes the approval once,
executes the ordinary authenticated GraphQL resolver, and protects its result.
The coordinator re-adopts and consumes that result checkpoint immediately
before a freshly planned provider turn. A later turn may request another
single mutation, producing a new independent approval. Parallel/mixed tool
batches, stateless supervised continuation, autonomous writes, model-authored
GraphQL, and mutation replay remain rejected.

`AiSupervisedResumeOutcome::RecoveryRequired` now includes `provider_turns`
and `total_tool_calls`, exposes matching getters, and the enum is
`#[non_exhaustive]`. Downstream matches must use `..`; downstream code must not
construct this outcome as authority. This is a pre-1.0 source-breaking API
change. The new top-level outcome and planner/stager/checkpoint/resume traits
are re-exported from the crate prelude.

Read-only and supervised coordinators now check remaining provider-turn
capacity before consuming an exact continuation checkpoint. The supervised
coordinator also refuses to stage an approval on the final allowed provider
turn, because no permitted turn would remain to disclose the mutation result.
This is a fail-closed behavior change and needs no data migration.

Denied, revoked, never-approved, and expired human decisions still require the
host's bounded waiting-run reconciliation worker; `execute_approved_claim`
accepts only an exact approved claim. Do not poll or heartbeat a pending human
wait through this coordinator. Multi-call and stateless supervised resumption
(including Ollama/local-harness mutation waits) remain closed.

## Unreleased: cross-generation supervised checkpoint adoption (crate 0.31.0 to 0.32.0; schema 0.29.0 to 0.30.0)

Apply AI schema module `0.30.0` while workers, provider calls, restore
callbacks, and approval execution are closed. Do not run 0.32.0 code against a
module still registered as 0.29.0. The generated migration adds no table,
column, index, constraint, or entity and still owns 39 private records. It
advances the module because the existing protected supervised-checkpoint kind
gains a stricter approval-binding payload and becomes eligible for
cross-generation adoption. No data migration, consumer table, application
SQL, or payload rewrite is required.

After an exact `supervised_tool_batch_persisted` checkpoint loses its worker
lease, expired-lease recovery may now requeue it under a new attempt and lease
generation. The recovery transaction requires one completed write-risk tool,
its exact consumed one-use approval, complete step/result/egress state, a
committed reconciled provider budget, and the checkpoint hash. It never
executes or retries the consequential resolver.

`OrmAiCoordinatorCheckpointService::adopt_supervised_tool_batch` then reopens
the old generation's protected checkpoint and every protected argument,
result, approval-resource, and canonical-preview envelope. It verifies the
exact provider response/budget/tool/approval/egress rows, approval binding,
preview hash, policy/auth-state evidence, current principal/scope/protection
policy, and current hierarchical rules before returning the opaque
`AiAdoptedSupervisedToolBatch`. The provider-retained continuation remains
private. `consume_supervised_before_provider` accepts that proof and clears the
exact latest-checkpoint link through the current row-version fence; it must run
before the next provider transport and succeeds only once.

Trusted backup/restore fact producers must populate the new
`AiRestoredRun::coordinator_checkpoint` field using
`AiRestoredCoordinatorCheckpoint`. A confirmed external effect is eligible for
`RequeueWithNewAttempt` only when the snapshot state is `Running`, the linked
checkpoint was fully validated as `SupervisedToolBatch`, and a provider
continuation exists. `WaitingApproval`, `WaitingTool`, uncertain effects,
uncheckpointed confirmed mutations, invalid coordinator counts, and malformed
adoption evidence remain `RecoveryRequired` or fatal. This new required field
is a public Rust construction API change; update snapshot adapters before
upgrading. Legacy serialized facts without the field deserialize as `None` and
therefore fail closed rather than acquiring adoption eligibility.

The supported supervised checkpoint is still exactly one mutation with a
provider-retained response ID. Multi-call, partial-batch, and stateless
supervised adoption (including Ollama/local-harness approval waits) remain
closed. The top-level supervised provider coordinator is a later gate. Existing
read-only checkpoint adoption remains strictly read-only.

## Unreleased: protected supervised continuation handoff (crate 0.30.0 to 0.31.0; schema 0.28.0 to 0.29.0)

Apply AI schema module `0.29.0` while workers, provider calls, human approval
waits, and restore callbacks are closed. Do not run 0.31.0 code against a
module still registered as 0.28.0. The generated migration adds no table,
column, index, constraint, or entity and still owns 39 private records. It
advances the module because existing private checkpoint records gain a new
authorization-sensitive kind and stricter interpretation. No consumer table,
application SQL, or data copy is introduced.

`OrmAiSupervisedResumeService::execute_claimed` accepts the exact
`AiApprovedRunClaim`. It reopens the linked `provider_turn_persisted`
checkpoint, committed provider budget, single staged tool, and
`resume_claimed` approval under current principal, scope/session, protection,
and hierarchical-rule authority. It then uses the normal consequential tool
service to rebuild the canonical preview, consume approval once, and execute
the ordinary authenticated GraphQL mutation. It never calls the provider.

An unambiguous model-visible result is protected as
`supervised_tool_batch_persisted`, with the exact consumed approval, result
egress manifest, provider-retained response continuation, rule fingerprint,
and cumulative provider/tool usage. `AiSupervisedResumeOutcome` returns either
that opaque checkpoint or a durable recovery-required tool ID. If resolver or
post-mutation persistence is ambiguous, no approval or mutation is replayed.

This first resume contract accepts exactly one supervised mutation and a
provider-retained response ID. Multi-call batches and stateless continuation
(including Ollama and local-harness turns) remain closed at this handoff until
their complete ordering/history evidence is implemented. Existing provider
and local-harness support is unchanged outside approved-wait resumption.

Trusted supervised planners should call the new public
`AiProviderCallPlan::project_supervised_rule_usage` with the exact freshly
resolved hierarchy before provider execution. Plans now retain private
plan-time fingerprint/maturity/approval bindings: safe reads must remain
approval-free, and supervised mutations must remain one-shot. The method also
checks provider capabilities, classification, retention/BYOK, and estimated
usage, but does not replace atomic budget reservation, egress, tool policy, or
resolver authorization.

Read-only `tool_batch_persisted` append and adoption now reject every tool row
whose risk is not `read_only` or whose approval ID is present, including all
stateless history. The new supervised kind requires one allowed write-risk row
and its exact consumed, one-use approval. Finish or reconcile active 0.30.0
coordinator checkpoints before upgrading; legacy ambiguous/misclassified
records are not adopted. No data migration is required.

Live expired-lease and snapshot restore do not yet adopt a supervised
continuation across a new attempt/generation. A process loss before or after
the supervised checkpoint therefore remains `RecoveryRequired`; do not relink
or replay it manually. Cross-generation adoption and the top-level supervised
provider loop are later gates.

## Unreleased: fenced approved-wait handoff (crate 0.29.0 to 0.30.0; schema 0.27.0 to 0.28.0)

Apply AI schema module `0.28.0` while workers, approval decisions, backups,
and restore callbacks are closed. Do not run 0.30.0 code against a module
still registered as 0.27.0. The generated migration adds no table, column,
index, constraint, or entity and still owns 39 private records. It advances
the module because existing approval/run records gain strict durable handoff
semantics. No consumer table, application SQL, or data copy is introduced.

Workers that resume human-approved actions should call
`OrmAiRunService::claim_next_approved`. The returned `AiApprovedRunClaim`
contains private, non-forgeable approval/tool IDs and the sole current lease.
The transaction preserves the existing attempt and lease generation so the
staged approval, provider usage, and tool call retain their exact bindings,
but rotates owner, expiry, heartbeat, and row version. It also changes the
approval from `approved` to `resume_claimed`, moves the run from
`WaitingApproval` to `WaitingTool`, and appends a redacted audit fact. Exactly
one concurrent worker succeeds; the old waiting lease becomes stale.
Expired `approved` rows encountered in a bounded claim scan are changed to
`expired` with a redacted audit fact before the scan continues, preventing an
old block of approvals from permanently starving newer eligible work.

`AiApprovalState` adds the pre-1.0 `ResumeClaimed` variant. Approval views may
return `resume_claimed`. Consumption accepts either the original direct
`approved` path or the claimed path, always rehydrates and rebuilds the exact
binding, then atomically clears the internal run marker while moving to
`Running`. Revocation accepts both unconsumed states. A claim remains neither
approval consumption nor resolver/rule/egress authority.

This is the durable queue-handoff foundation, not yet the complete top-level
supervised coordinator. Consumers must not reconstruct provider continuations
or replay a mutation after a resumed worker crash. Full protected provider-turn
adoption will build on this proof. Existing 0.29.0 approvals require no data
migration; finish or reconcile active waits before upgrading so their state is
not interpreted across versions.

Restore snapshot producers must include pending, approved, and
`resume_claimed` unconsumed rows in `pending_approval_count`. The pure restore
planner now classifies both `WaitingApproval` and `WaitingTool` as
`RecoveryRequired` regardless of the coarse external-effect flag; a restored
snapshot cannot use the live same-attempt handoff or infer replay authority.

## Unreleased: rule-bound coordinator checkpoints (crate 0.28.0 to 0.29.0; schema 0.26.0 to 0.27.0)

Apply AI schema module `0.27.0` while workers, provider calls, backups, and
restore callbacks are closed. Do not run 0.29.0 code against a module still
registered as 0.26.0. The generated migration adds no table, column, index,
constraint, or entity and still owns 39 private records. It advances the
module because existing protected run-checkpoint fields now require strict v2
rule fingerprint and cumulative usage semantics. No consumer table or raw SQL
is introduced.

Every `AiReadOnlyAgentTurnPlan::new` call now supplies an exact
`AiResolvedRuleSet` and a trusted planner-derived `uses_byok` flag. Construct
`OrmAiCurrentRuleResolver` from the durable current-principal resolver, the
same `Arc<dyn AiRulePolicyService>` used for GraphQL rule management, a trusted
clock, and bounded principal freshness. Install it as the new
`AiAgentRuleResolver` argument on both `AiReadOnlyAgentCoordinator` and
`OrmAiCoordinatorCheckpointService`; normally one shared instance is used at
both boundaries.

Checkpoint-writer implementations receive the exact rules and
`AiRuleRunUsage`. Protected format v2 binds the target/fingerprint and
cumulative provider calls, provider/application-tool steps, trusted start
time, output tokens, cost, tool units, and image units. The coordinator checks
estimated capacity before provider egress and replaces it with authoritative
committed usage after return. It re-resolves the current hierarchy before
transport, after transport, before each resolver tool, around checkpoint
protection, and during adoption. A pre-egress mismatch fails safely; a
post-egress mismatch or actual-usage overrun becomes `RecoveryRequired` rather
than replaying or exposing the result.

The new checks only narrow. They do not replace atomic budget reservations,
authoritative pricing, egress manifests, provider-profile authorization,
current tool policy, ordinary GraphQL resolver authorization, or approval.
`uses_byok` is a server-owned planning assertion checked against the rule set,
not proof that a credential exists or is usable. A turn exposing any custom
application tool also requires both the `CustomTools` and
`ParallelToolCalls` rule capabilities: even one advertised tool definition can
be selected more than once in a provider turn.

Legacy protected coordinator checkpoint v1 does not contain enough evidence
for safe adoption and is deliberately rejected by 0.29.0. Before upgrade,
finish or reconcile active 0.28.0 runs. If an old checkpoint remains after a
crash/restore, keep the runtime closed and classify the run for privileged
manual recovery; do not rewrite protected checkpoint JSON or counters with
application SQL.

Restore snapshot producers must populate the new
`AiRestoreSnapshotFacts::invalid_coordinator_checkpoint_count`. Count legacy
format, malformed protected state, rule fingerprint/current-lineage mismatch,
invalid cumulative usage, or fence/scope mismatch. Any nonzero value emits
fatal `AI_RESTORE_COORDINATOR_CHECKPOINT_INVALID` evidence. This field and the
additional constructor/trait arguments are pre-1.0 source-breaking changes.
No consumer-data migration is required.

## Unreleased: hierarchical rule narrowing (crate 0.27.0 to 0.28.0; schema 0.25.0 to 0.26.0)

Apply AI schema module `0.26.0` while workers, rule/configuration mutations,
backups, and restore callbacks are closed. Do not run 0.28.0 code against a
module still registered as 0.25.0. The generated migration adds no table,
column, index, constraint, or entity and still owns 39 private records. It
advances the module because the existing private scope-policy record now has a
strict deterministic ID, deny-unknown-fields v1 hierarchical-rule payload,
scope-bound checksum, and restore meaning. No application raw SQL or
consumer-owned data access is introduced.

New public Rust APIs include `AiRuleConstraints`, budget/provider/approval
constraint types, `AiRuleDeploymentLimits`, `AiResolvedRuleSet`, access and
hierarchy traits, `AiRulePolicyService`, the redacted GraphQL input/view and
roots, and `OrmAiRulePolicyService`. Compose the rule roots separately and
install one `Arc<dyn AiRulePolicyService>`. Writes require a current principal,
exact `Manage` authorization, recent MFA, immutable deployment-limit
validation, and CAS. Reads and run resolution have independent authorization
actions.

Implement `AiRuleHierarchyResolver` from authoritative application state and
the current principal. It must return the complete broadest-to-target lineage.
Every participating layer must have an explicit policy; a missing, duplicate,
over-depth, wrong-target, cross-tenant, unauthorized, corrupt, or deployment-
widening layer fails closed. GraphQL scope kinds remain opaque strings and do
not add any product entity or tenant hierarchy to this crate.

Resolve the hierarchy before trusted run planning and carry its canonical
fingerprint and exact row versions into host orchestration. Apply the effective
tool approval floor and budget ceilings at their real execution boundaries.
`AiResolvedRuleSet` is only negative constraint evidence: a positive helper
result is not tool enablement, resolver authorization, disclosure approval,
provider routing, egress authorization, spend reservation, BYOK permission, or
one-shot approval consumption.

The GraphQL SDL adds `aiRulePolicy`/`AiRulePolicy` and
`setAiRulePolicy`/`SetAiRulePolicy`, plus their inputs and enums, following the
selected camelCase/PascalCase feature without aliases. Secret classification
and autonomous-write maturity are absent. An absent allowlist/budget value
inherits the effective broader constraint; an empty allowlist or zero budget
explicitly denies that dimension.

Restore snapshot producers must populate the new
`AiRestoreSnapshotFacts::invalid_rule_policy_count`. Any nonzero value emits
fatal `AI_RESTORE_RULE_POLICY_INVALID` evidence and keeps the runtime closed.
This added public struct field is a pre-1.0 source-breaking change for
struct-literal producers.

The public service did not exist in 0.27.0, so a normal deployment has no
service-created policy rows and needs no row rewrite or consumer-data
migration. If a private integration pre-seeded `AiScopePolicyRecord`, treat
those rows as unsupported legacy data: keep the runtime closed and replace
them through the authenticated `setAiRulePolicy` mutation as part of a
controlled migration. Do not expose generic CRUD roots or repair private JSON
with application SQL.

## Unreleased: durable validated UI-intent suggestions (crate 0.26.0 to 0.27.0; schema 0.24.0 to 0.25.0)

Apply AI schema module `0.25.0` while AI workers, subscriptions, backups, and
restore callbacks are closed. Do not run 0.27.0 code against a module still
registered as 0.24.0. The generated migration adds no table, column, index,
constraint, or entity and still owns 39 private records. It advances the
module because existing session/inbox event rows now have a strict protected
`ui_intent_suggested` semantic bound to an exact provider result, descriptor,
committed budget reservation, owner/scope, audit fact, and run fence. No raw
SQL or consumer-owned data access is introduced.

New public Rust APIs are `AiPersistedUiIntent`,
`AiUiIntentDeliveryService`, `AiUiIntentDeliveryLimits`, and
`OrmAiUiIntentDeliveryService`. Construct the ORM service with the same fenced
run service, current-principal resolver, session/scope access policy,
content-protection policy/protector, trusted clock, and immutable UI-intent
catalog used by the worker. Delivery consumes an exact
`AiUiIntentTypeBinding`; catalog registration alone is not enablement or
authorization.

For a provider turn that returns a UI-intent envelope, persist the ordinary
protected assistant output first, pass its renewed lease to UI-intent delivery,
then pass the delivery result's renewed lease to the next fenced write or run
completion. The provider-visible text must be exactly one camelCase object:
`{"formatVersion":1,"intentType":"…","payload":{…}}`. The normalized
event stream must contain one ordered start, usage, and completion; hidden
reasoning, tool calls, built-ins, citations, unknown events, extra envelope
fields, stale fingerprints, schema-invalid payloads, mismatched response/usage
evidence, or absent committed budget proof fail closed. Exact retries return
the existing event without advancing either stream or fence twice.

Restore snapshot producers must populate the new
`AiRestoreSnapshotFacts::invalid_ui_intent_event_count`. Validate protected
session/inbox event pairs, deterministic source and descriptor evidence,
owner/scope linkage, the matching committed budget fact, and redacted audit.
Any nonzero value emits fatal `AI_RESTORE_UI_INTENT_EVENT_INVALID` evidence and
keeps the runtime closed. This additional public struct field is a pre-1.0
source-breaking change for struct-literal producers.

Existing 0.26.0 deployments have no crate-created UI-intent events because the
delivery service did not exist. They need no row rewrite or consumer-data
migration. If a private integration previously reused the
`ui_intent_suggested` event name, treat those rows as unsupported legacy data
and keep the runtime closed until its controlled restore/migration process has
removed or replaced them; do not repair protected event payloads with
application raw SQL. GraphQL SDL is unchanged.

## Unreleased: protected skills and typed UI intents (crate 0.25.0 to 0.26.0; schema 0.23.0 to 0.24.0)

Apply AI schema module `0.24.0` while AI workers, backups, and restore callbacks
are closed. Do not run 0.26.0 code against a module still registered as
0.23.0. The generated migration adds no table, column, index, constraint, or
entity and still owns 39 private records. It advances the module because the
existing skill/version fields now have strict v1 protected-instruction,
policy, checksum, provenance, and restore semantics. Skill scope fields also
participate in generated exact-scope filters, and skill-version IDs are
assigned by the catalog before protection so their row identity can be bound
into the protected envelope. Neither change introduces application-written
SQL.

The new separately composable GraphQL SDL exports `AiSkillQueryRoot` and
`AiSkillMutationRoot` with bounded redacted list, safe metadata upsert,
immutable version publication, and enable/disable operations. Names follow the
selected camelCase or PascalCase feature with no aliases. Install exactly one
`Arc<dyn AiSkillCatalogService>` in schema data. The concrete ORM service also
requires an `AiSkillAccessPolicy`, current `AuthPrincipal`, ready exact-scope
content-protection resolver/protector, recent-MFA policy, and trusted clock.

The service was not publicly available before 0.26.0 and private generated
skill CRUD roots have never been exported, so a normal 0.25.0 deployment has no
catalog-created skill rows and needs no row rewrite. If an early deployment
privately pre-seeded these tables, treat those rows as unsupported legacy data:
keep the runtime closed, inventory them through the deployment's controlled
backup/migration process, and publish a replacement current version through
the authenticated skill GraphQL mutation using the known skill ID and CAS
version. Do not repair policy JSON with application raw SQL. Unknown fields,
legacy empty objects, malformed protected content, or a legacy current version
fail closed until replaced. Consumer-owned data is unaffected.

New public Rust APIs include the skill inputs/views/service/access policy and
ORM service, plus `AiUiIntentTypeId`, `AiUiIntentTypeDescriptor`, exact
bindings, draft/validated values, and `AiUiIntentCatalog`. UI-intent schemas
must explicitly declare JSON Schema 2020-12. A skill stores the descriptor
fingerprint, not only its logical name. Consumers must re-register the exact
descriptor on startup and validate model drafts with `validate_bound` before
delivery. A validated intent remains a suggestion: the consumer must recheck
current resource authorization and map the logical type to frontend behavior.
No route or navigation is performed by this crate.

Restore snapshot producers must populate the new
`AiRestoreSnapshotFacts::invalid_skill_catalog_count`. Count any malformed
skill/current-version relationship, protected envelope, strict policy object,
provenance, or checksum. Any nonzero value produces fatal
`AI_RESTORE_SKILL_CATALOG_INVALID` evidence and must keep the runtime closed.

This is a pre-1.0 additive Rust API and GraphQL SDL change plus a persistent
semantic, authorization, content-protection, audit, backup, and restore
contract change. No database DDL or consumer-data migration is required for
deployments that did not privately pre-seed skill rows.

## Unreleased: owned PostgreSQL parity harness (crate 0.25.0; schema 0.23.0)

CI now runs the PostgreSQL parity test through a container created by the test
itself on the local Docker socket. The harness generates its own user,
password, database, container identity, ownership label, and Docker-assigned
IPv4 loopback port. It never reads or accepts a database URL and verifies its
ownership label before removing the container. Local runs skip only when the
local Docker socket is unavailable; CI fails closed instead. The 0.26.0
harness additionally exercises protected skill publication/resolution through
generated ORM operations.

This changes test and release-gate behavior only. It adds no public Rust API,
GraphQL SDL, entity, index, constraint, persistent semantic, authorization,
backup, or restore change. `AI_SCHEMA_MODULE_VERSION` remains `0.23.0`; no
database or consumer-data migration is required.

## Unreleased: profiled OpenAI-compatible adapter (crate 0.24.0 to 0.25.0; schema 0.22.0 to 0.23.0)

Apply AI schema module `0.23.0` while configuration/provider workers, backups,
and restore callbacks are closed. Do not run 0.25.0 code against a module still
registered as 0.22.0. The generated migration adds no table, column, index,
constraint, or entity and still owns 39 private records. The module advances
because `AiProviderProfileRecord.data_policy` now stores a strict version-1
OpenAI-compatible capability and retention contract.

The GraphQL `UpsertAiProviderProfileInput` SDL adds nullable
`openaiCompatible`/`OpenaiCompatible` input according to the selected naming
feature, and `AiProviderProfileView` adds the corresponding redacted view.
Creating or updating an `OpenAiCompatible` profile requires this nested value;
all other provider kinds reject it. The retention label is bounded and the
parallel-tool flag requires custom tools. Updating a profile remains
recent-MFA-, host-policy-, endpoint-policy-, CAS-, and audit-gated.

Existing compatible profiles whose `data_policy` is the legacy empty object
remain readable with no compatible contract, but they cannot construct the new
adapter. Re-save each intended profile through the authenticated GraphQL
mutation with an explicitly reviewed endpoint, retention label, and minimal
capability set before enabling routing. Unexpected or malformed nonempty
policy data fails closed. Existing native-provider profiles need no rewrite.
No consumer-owned or chat data is migrated.

Enable `provider-openai-compatible` to export
`OpenAiCompatibleProviderConfig`, `OpenAiCompatibleCapabilities`, and
`OpenAiCompatibleProvider`. The adapter expects a Responses-compatible SSE
endpoint—not the older Chat Completions surface—and never discovers
capabilities. Build from the redacted profile plus its separately loaded
`SecretRef`, then pass the same deployment endpoint policy and secret store to
the provider constructor. Every call needs exact egress and atomic budget
proofs matching the profile ID, normalized destination, provider/model, and
retention declaration.

This is a pre-1.0 additive Rust API/Cargo-feature change and an additive
GraphQL SDL plus persistent semantic change. It changes provider routing,
configuration, egress, retention, and restore validation contracts. No
database-row or consumer-data rewrite is required beyond the administrator
re-save needed to activate a legacy compatible profile.

## Unreleased: native xAI adapter (crate 0.23.0 to 0.24.0)

Enable `provider-xai` to activate the native xAI Responses/SSE adapter and its
optional HTTP dependencies. The feature now exports `XAiProviderConfig` and
`XAiProvider`. Construct configuration from a secret-store `SecretRef`, then
supply an `Arc<dyn AiSecretStore>` to `XAiProvider::new`. The production URL is
fixed to xAI's official Responses endpoint; GraphQL, provider profiles, and
model input cannot select a URL, header, or plaintext credential.

`require_zero_data_retention` defaults to true and requires the exact xAI
response attestation before any streamed output is accepted. Hosts without
xAI enterprise ZDR must explicitly set it false and separately ensure the
egress policy describes and permits xAI's documented ordinary retention.
`store_responses` remains false by default. It cannot be combined with required
ZDR verification and still needs an exact provider-response retention proof on
every call when enabled. Existing OpenAI provider configuration is unchanged.

The initial adapter supports bounded text/JSON, JSON-schema structured output,
and strict custom/parallel application tools. Every request requires an output
token ceiling. Attachments, xAI server tools, stateless/encrypted-reasoning
continuation, and arbitrary endpoints fail closed. The shared Responses
normalizer now rejects a non-SSE response and any built-in event whose exact
kind was not in the server-authored request. It also requires exact model,
response ID, completed status, usage, bounded event/text/tool-call state, and
an unambiguous terminal event. This tightens malformed, truncated, swapped, or
unsolicited OpenAI responses as well.

This is a pre-1.0 additive Rust API, feature, dependency, provider transport,
retention, egress, and behavioral contract change. It adds no GraphQL SDL,
persistent entity, index, constraint, backup/restore behavior, or data semantic
change. `AI_SCHEMA_MODULE_VERSION` remains `0.22.0`; no database or consumer
data migration is required.

## Unreleased: native Anthropic adapter (crate 0.22.0 to 0.23.0)

Enable `provider-anthropic` to activate the native Anthropic Messages/SSE
adapter and its optional `reqwest` dependency. The feature now exports
`AnthropicProviderConfig` and `AnthropicProvider`; code that treated the
previous empty feature as a marker should update its feature expectations.
Construct configuration with a secret-store `SecretRef`, optionally narrow
the bounded timeout, then supply an `Arc<dyn AiSecretStore>` to
`AnthropicProvider::new`. The endpoint and `anthropic-version` header are
adapter-owned and cannot be selected through GraphQL or model input.

Requests require an explicit `maximum_output_tokens`, exact Anthropic egress
proof, and atomic provider-call budget proof. Supported inputs are bounded
text/JSON, strict application tools with protected stateless continuation,
and JSON-schema structured output. Attachments, provider built-ins,
provider-retained continuation, extended thinking, and prompt-cache creation
are rejected. Cache-read usage is reported as a subset of checked total input;
nonzero cache creation fails closed because the generic authoritative pricing
catalog does not yet represent Anthropic's separate cache-write price class.

This is a pre-1.0 additive Rust API, feature, dependency, provider transport,
egress, and accounting behavior change. It adds no GraphQL field or SDL
change, persistent entity, index, constraint, backup/restore behavior, or data
semantic change. `AI_SCHEMA_MODULE_VERSION` therefore remains `0.22.0`; no
database or consumer-data migration is required.

## Unreleased: stateless checkpoint adoption (crate/schema 0.21.0 to 0.22.0)

Apply AI schema module `0.22.0` while provider/coordinator workers, backups,
and restore callbacks are closed. Do not run 0.22.0 code against a module
registered as 0.21.0. The generated migration has no table, column, index, or
consumer-data rewrite and the module still owns 39 private entities. The
module version advances because the protected checkpoint and restore semantic
contract now permits a fully proven stateless tool history to cross a lease
generation.

Expired-run recovery may now requeue an exact completed stateless
`tool_batch_persisted` checkpoint instead of classifying lease loss as
`RecoveryRequired`. The replacement worker must use
`AiAgentCheckpointAdopter`; it cannot read or reconstruct checkpoint JSON
itself. Adoption rehydrates current authority, opens the protected payload,
and validates every historical and current tool against its original
attempt/generation, committed budget reservation, finished run step,
canonical arguments, protected result, disclosure classification, immutable
allow audit, and unique tool-result manifest. It then rechecks authority and
the protection policy. No application resolver or previous provider turn is
rerun. The linked checkpoint is still atomically consumed through the new
fence before the next provider transport.

Existing 0.21.0 stateless version-2 checkpoints need no rewrite or backfill.
They become eligible only when all durable evidence satisfies the stricter
adopter; missing provider-name metadata, stale policy, malformed history,
tampering, duplicate identities, incomplete work, or denied egress fails
closed. Existing provider-retained checkpoint behavior is unchanged. No
consumer-owned application/domain data is read or changed by migration.

There is no new public Rust item, feature/default change, or GraphQL SDL
change. This is a pre-1.0 persistence-semantic, restore, and behavioral
contract change. Hosts that intentionally treated every stateless lease loss
as permanently non-resumable should update operator runbooks: exact completed
batches can now be safely requeued, while provider-turn, partial-batch,
consequential, and otherwise ambiguous checkpoints remain
`RecoveryRequired`.

## Unreleased: stateless local tool continuation (crate/schema 0.20.0 to 0.21.0)

Apply AI schema module `0.21.0` while provider workers, coordinator workers,
backups, and restore callbacks are closed. Do not run 0.21.0 code against a
module registered as 0.20.0. The module still owns 39 private entities and the
generated migration has no table, column, index, or consumer-data rewrite.
The module version advances because the protected coordinator-checkpoint
semantic contract now accepts version-2 stateless conversation payloads.
Existing version-1 provider-retained checkpoints remain readable.

Public Rust API changes are pre-1.0 breaking for exhaustive matches and struct
literals:

- `ModelRequest` adds required `continuation_mode`; use
  `ProviderRetained` for the existing OpenAI response-ID path and
  `StatelessReplay` only for an adapter that advertises it.
- `ModelContinuation` adds `StatelessConversation`, and the new
  `ModelConversationMessage`/`ModelConversationToolCall` types retain exact
  bounded visible history. Update exhaustive matches.
- `ProviderCapabilities` adds `provider_retained_continuation` and
  `stateless_continuation`. Providers and registries must state these
  independently; neither is inferred from `custom_tools`.
- Provider plans now reject duplicate manifest hashes and permit at most 288
  transfers so a bounded stateless replay can carry one distinct proof for
  each of up to 256 tool results. Every `ToolResult` manifest must contain one
  `application_tool_result` source and may cover exactly one replayed result.

The native Ollama adapter now accepts only server-authored custom-tool plans in
`StatelessReplay` mode. It maps the full protected text/JSON, assistant-call,
and tool-result history into `/api/chat`, rejects hidden thinking and
provider-retained continuation, and normalizes only offered function names
back to local tool IDs. Existing text/image/structured requests in
`ProviderRetained` mode continue unchanged.

The installed-harness framing contract changes from
`graphql-orm-ai/local-harness-jsonl/v1` to `/v2`. Update reviewed harnesses to
accept `continuation_mode`, `continuation`, and `tools` in the single request
frame. A tool-capable registration must set `custom_tools = true` and
`stateless_continuation = true` together; `parallel_tool_calls` is optional.
The driver accepts only exact offered tool IDs in bounded
start/delta/complete order. Text-only harnesses may keep both capabilities
false but must still implement v2 framing.

Stateless tool batches are protected and checkpointed through generated
`graphql-orm` repositories and transactions; no raw SQL is introduced. The
same fenced generation consumes the checkpoint before its next provider call.
If that lease expires, restore validates the durable budget/tool/step/hash
evidence but moves the run to `RecoveryRequired`; it does not replay a local
model or application resolver. Cross-generation adoption remains limited to
provider-retained response-ID checkpoints. No existing data backfill is
needed, and no consumer-owned application/domain data is changed.

This is a pre-1.0 Rust API, provider capability, local-harness protocol,
persistence-semantic, restore, and behavioral contract change. It adds no
GraphQL field or root and therefore does not change the public GraphQL SDL.

## Unreleased: bounded session content retention (crate/schema 0.19.0 to 0.20.0)

Apply AI schema module `0.20.0` while session/provider workers, subscriptions,
backups, and restore callbacks are closed. Do not run 0.20.0 code against
module 0.19.0. The module still owns 39 private entities and changes only
AI-owned storage:

- make `graphql_orm_ai_messages.protected_preview` nullable;
- add nullable `content_purged_at` and required CAS `row_version` to messages;
  and
- add a non-unique lookup index on
  `graphql_orm_ai_attachments.message_id`.

For every existing message, preserve its protected preview, set
`content_purged_at` to null, and initialize `row_version` to zero using the
dependency-generated migration/default. Do not synthesize a tombstone or
delete a block during migration. Validate that each unpurged complete message
still has a protected preview and exactly `block_count` ordered block rows.
There is no consumer-owned application/domain data migration.

After migration, schedule `OrmAiSessionRetentionService` as a trusted host
worker. Begin each scan cycle with no cursor, pass each returned opaque
`next_session_cursor` into the next call, and begin a later cycle only after it
returns absent. Deployment limits bound every scan, event query, message query,
and block deletion. The worker uses generated ORM repositories and
state-machine transactions only; do not replace it with application SQL or
expose it as a user GraphQL operation.

The current exact scope retention policy is reloaded and validated inside each
session transaction. Missing/legacy policy, corrupt rows, CAS conflicts,
nonterminal runs, linked attachments, and block-count mismatch retain content
and fail closed or are reported. A successful pass may:

- delete expired provisional `provider_live_delta` session-event rows;
- clear the protected preview and delete blocks for an expired, finalized,
  terminal-run message with no attachment;
- retain the message shell with `content_purged_at`, `block_count = 0`, and a
  fixed server-authored tombstone in authenticated reads; and
- append one redacted audit fact in the same transaction.

This is not complete erasure. It never deletes a session, message metadata,
attachments or blob objects, runs, tool/proposal/approval payloads, raw
provider payloads, provider-persistent files, usage, egress, audit, fence, or
restore evidence. Continue to treat those retention fields and deleting-
session workflows as separate obligations.

`AiMessageView` gains required GraphQL/Rust field `content_purged` (or
`ContentPurged` under the PascalCase feature). A purged authorized message has
the fixed preview `Content removed by retention policy`, reports zero blocks,
and returns an empty authorized block window. Clients must not infer that the
message metadata or linked external artifacts were erased.

Selective live-delta deletion can leave durable sequence gaps without reusing
or rewinding sequence values. `AiSessionService::session_event_page` now
returns an empty page with `reset_required = true` when the requested replay
window crosses such a gap. Subscription and virtualized clients must discard
provisional state and reload bounded authoritative message/session windows.

Restore fact collectors must populate
`AiRestoreSnapshotFacts::invalid_session_retention_count`. Report nonzero for
inconsistent purged/unpurged message shapes, retained blocks behind a
tombstone, or an event gap that cannot be classified as expected retention.
Any nonzero value adds fatal `AI_RESTORE_SESSION_RETENTION_INVALID` and keeps
runtime readiness closed. Expected, validated retention gaps remain represented
through reset semantics; duplicate sequence values remain independently fatal.

This is a pre-1.0 public Rust API, GraphQL SDL, persistence, migration,
backup/restore, and behavioral contract change.

## Unreleased: immutable pricing catalog (crate/schema 0.18.0 to 0.19.0)

Apply AI schema module `0.19.0` while configuration writes, provider workers,
budget reservations, backups, and restore callbacks are closed. Do not run
0.19.0 code against module 0.18.0. The module adds its 39th private entity,
append-only `graphql_orm_ai_pricing_policies`, with a required globally unique
`version_reference`, exact deterministic scope key and scope fields, exact
provider/model, integer-only fixed/input/cached-input/output rates, creator
principal identity, and creation time. No consumer-owned application/domain
data migration is needed.

Pricing versions are immutable and immediately eligible for explicit
selection. There is no update, delete, activation, or implicit “latest” API.
Create a new version to change a rate, then bind its exact returned reference
into new budget reservations. Existing reservations and uncertain calls retain
their original reference and must never be repriced under a newer version.

Hosts composing `AiConfigurationQueryRoot`/`AiConfigurationMutationRoot` gain
`aiPricingPolicies` and `createAiPricingPolicy` (or coherent PascalCase names).
Install a separate `Arc<dyn AiPricingCatalogService>` in GraphQL context;
existing `AiConfigurationService` implementations do not gain methods. Add
the new exhaustive `AiConfigurationAction::ReadPricingCatalog` and
`ManagePricingCatalog` cases to every host policy. Reads are exact-route and
bounded to 100 versions. Creation requires a user principal with recent MFA,
the exact host write decision, deployment `AiPricingCatalogManagementLimits`,
per-route capacity, and an atomic redacted audit append.

`OrmAiPricingService` also implements `AiPricingQuoteService` and
`AiProviderUsageAccounting`. Quotes bind exact scope, provider, model, and
version and conservatively price all estimated input at the non-cached rate.
Settlement prices authoritative total/cached input and output under the same
version. `AiProviderUsageObservation::scope` exposes the exact application
scope copied from the bound budget plan so custom accountants can enforce the
same cross-scope rejection. Rates and totals use checked integer microunit arithmetic with
per-dimension ceiling division; cached rate cannot exceed ordinary input rate.
Version/provider/model swaps, corrupt rows, negative rates, and overflow fail
closed. The initial concrete accountant rejects provider built-ins because a
requested tool is not authoritative provider-billed usage; deployments using
built-ins must retain a custom complete accounting implementation until exact
billable-unit catalogs land.

Restore fact collectors must populate the new
`AiRestoreSnapshotFacts::invalid_pricing_policy_count`. Validate unique
references, deterministic scope-key equality, exact supported provider/model,
non-negative rates, cached rate no greater than input, creator identity, and
the corresponding creation audit before reporting zero. Any nonzero value adds
fatal `AI_RESTORE_PRICING_POLICY_INVALID` and keeps runtime readiness closed.

This is a pre-1.0 public Rust API, GraphQL SDL, authorization, configuration,
persistence, migration, backup/restore, and behavioral contract change.

## Unreleased: authenticated budget-policy management (crate/schema 0.17.0 to 0.18.0)

Apply AI schema module `0.18.0` while provider workers, configuration writes,
budget reservations, backups, and restore callbacks are closed. Do not run
0.18.0 code against module 0.17.0. The managed schema adds required indexed
`scope_key` to `graphql_orm_ai_budget_policies`; the module still owns 38
private entities.

Backfill every existing budget policy with `ai_scope_key` computed from its
stored `scope_kind`, `scope_id`, and optional `tenant_id`. This is a
deterministic non-secret lookup identity, not authorization. Reject or repair
rows with invalid scopes, unpaired principal kind/subject, unknown intervals,
negative/no ceilings, duplicate/corrupt IDs, or a key that does not exactly
match those stored scope fields. Do not infer a tenant or principal. No
consumer-owned application/domain data migration is needed.
The helper is now exported for every backend, including schema-only MSSQL
builds; its availability does not imply MSSQL write-service parity.

`AiConfigurationAction` gains `ReadBudgetPolicies` and
`ManageBudgetPolicies`. `AiConfigurationService` gains `budget_policies` and
`upsert_budget_policy`; every custom implementation must add both methods.
Composed configuration GraphQL schemas gain `aiBudgetPolicies` and
`upsertAiBudgetPolicy` (or coherent PascalCase names), the
`AiBudgetIntervalInput` enum, input, and redacted view.

The ORM configuration service leaves mutations closed until the host calls
`with_budget_policy_management(AiBudgetPolicyManagementLimits)`. These
deployment bounds cap every GraphQL-configurable token/tool/image/cost/run
ceiling and allow at most 100 policies per exact scope. They do not grant
configuration authority and do not replace the independent per-call
`AiBudgetServiceLimits`. Choose the per-scope management bound together with
the budget service's maximum-applicable-policy bound so exact plus
tenant-wildcard policy sets remain executable.

Reads require the host's exact-scope `ReadBudgetPolicies` decision and return
at most 100 records. Mutations require a user principal with recent MFA, the
host's `ManageBudgetPolicies` decision, validated deployment ceilings, a
create/update identity pairing, and exact CAS. Create accepts an optional exact
principal kind/subject pair. On update the scope, tenant, principal pair, and
interval are immutable; create a replacement and disable the old policy to
change those bindings. There is no delete operation. Each successful mutation
appends a redacted audit event in the same state-machine transaction.

The reservation service now selects policies by the exact deterministic scope
key plus the matching tenant-wildcard key, then verifies the stored key and
scope fields before applying principal filters. Missing, excessive, corrupt,
or ceiling-free effective policies remain fail-closed. Existing counters keep
their committed/reserved values across ceiling changes; new reservations use
the current policy row version and a disabled policy no longer participates in
new reservations.

Restore fact collectors must populate the new
`AiRestoreSnapshotFacts::invalid_budget_policy_count`. Any nonzero value adds
fatal `AI_RESTORE_BUDGET_POLICY_INVALID` and keeps readiness closed. Validate
scope-key integrity, principal pairing, interval, non-negative bounded
ceilings, and policy/counter version relationships before reporting zero.

This is a pre-1.0 public Rust API, GraphQL SDL, authorization, configuration,
persistence, migration, backup/restore, and behavioral contract change.

The crate root no longer glob-reexports macro-generated types from the private
persistence module. These types were an accidental compile-visible leak and
were never a supported application CRUD surface. Replace any use with
`AiSchemaModule` for migrations and the authenticated configuration, budget,
usage, session, run, proposal, approval, attachment, or worker service traits.
`AiSchemaModule`, `AI_SCHEMA_MODULE_ID`, `AI_SCHEMA_MODULE_VERSION`, and
`AI_TABLE_NAMESPACE` remain public.

## Unreleased: authoritative usage ledger and reporting (crate/schema 0.16.0 to 0.17.0)

Apply AI schema module `0.17.0` while provider workers, budget reconciliation,
usage readers, backups, and restore callbacks are closed. Do not run 0.17.0
code against module 0.16.0. The managed migration:

- adds nullable `actual_cached_input_tokens` to
  `graphql_orm_ai_budget_reservations`;
- adds required, unique `budget_reservation_id` and required
  `principal_kind` to append-only `graphql_orm_ai_usage_entries`;
- adds generated query indexes for exact scope kind/ID/tenant, principal
  kind/subject, provider kind/model, run, creation time, and reservation; and
- advances `AI_SCHEMA_MODULE_VERSION` from `0.16.0` to `0.17.0` without adding
  an entity (the module still owns 38 private records).

The 0.16.0 usage entity was reserved private storage with no supported writer
or reader and should be empty. If a deployment wrote private usage rows, do not
invent a reservation, principal kind, tenant, or authority. While the runtime
is closed, a dependency-owned migration must prove each row's exact committed
budget reservation and matching run/session/scope/principal/provider/model,
reject duplicates, validate cached input is no greater than total input, and
then backfill it; otherwise remove those unsupported rows. Never expose an
unproven legacy row through the new service.

Existing committed reservations are not silently converted into historical
usage facts. If historical reporting is required, backfill only from complete
authoritative provider and committed-reservation evidence with a unique
one-to-one reservation binding. An absence remains an explicit historical gap;
estimated values must never be relabeled as actual usage. No consumer-owned
application/domain data migration is needed.

`AiBudgetReconciliation` gains `cached_input_tokens`. A committed result must
supply `Some(value)` and prove it is no greater than total `actual.input_tokens`;
an unused release must supply `None`; an uncertain result may carry an
observation but creates no usage fact. `AiBudgetAmounts::input_tokens` and
`AiProviderUsageObservation::input_tokens()` now explicitly mean total input,
with cached input recorded as a subset rather than added to the total. Update
every struct initializer and pricing implementation accordingly.

On authoritative commit, the ORM budget service now appends exactly one usage
fact in the counter/reservation transaction. Its unique reservation ID is the
idempotency boundary. A replay must match the original actual and cached usage
and returns the prior reconciliation; it never appends another fact. Release
and uncertain outcomes append none.

Hosts composing `AiQueryRoot` gain `aiUsage` (or `AiUsage` under
`graphql-case-pascal`). Install `Arc<dyn AiUsageService>` in GraphQL context.
`OrmAiUsageService` additionally requires an `AiUsageAccessPolicy`; return
`OwnPrincipal` for personal reporting, `WholeScope` only for independently
authorized scope administrators, and `Denied` otherwise. The policy result is
read authority only and grants no provider, budget-management, transcript, or
tool authority. Default pages contain at most 50 rows and the hard maximum is
200. Time filtering requires both bounds, is limited to a 366-day interval,
and uses the current generated GraphQL integer range.

Backups and restores must preserve the usage table as immutable facts and
validate: one usage fact per reservation; referenced reservations are
committed; exact scope/principal/provider/model fields agree; numeric usage is
non-negative; cached input does not exceed total input; and no report opens
until restore reconciliation succeeds. Retention or correction of usage facts
is not introduced by this release. Restore fact collectors must populate the
new `AiRestoreSnapshotFacts::invalid_usage_fact_count`; any nonzero value adds
the fatal `AI_RESTORE_USAGE_FACT_INVALID` issue and keeps readiness closed.

This is a pre-1.0 public Rust API, GraphQL SDL, persistence, migration,
backup/restore, reporting, budget-reconciliation, and behavioral contract
change.

Host egress planners should call the new public
`ModelRequest::conservative_egress_bytes()` when constructing the inference
manifest. It exposes the exact conservative calculation enforced by the
provider context; callers must not reproduce the older input-only estimate.

## Unreleased: bounded complete provider request metadata (crate 0.15.0 to 0.16.0)

`ModelRequest::validate` now rejects oversized instructions, text/JSON blocks,
output schemas, custom-tool schemas/fingerprints, zero or excessively large
output-token ceilings, more than 16 provider built-ins, duplicate built-in
kinds, duplicate/invalid web domains or file-store IDs, and invalid built-in
result limits. Serialized non-attachment request metadata has a 64-MiB hard
aggregate ceiling. Web-domain filters accept normalized DNS names and an optional
leading `*.` only; schemes, paths, whitespace, empty labels, and invalid label
characters are rejected.

Provider egress validation now estimates the complete serialized
`ModelRequest`, including model, instructions, tool definitions, schemas,
built-in configuration, continuation and tool-result metadata, then adds the
exact Base64 expansion of attachment bytes. Existing egress planners must use
the request's current conservative estimate rather than reproducing the older
input-only calculation. A previously accepted manifest whose
`estimated_bytes` omitted tool/schema/built-in metadata now correctly fails
before transport and must be reauthorized with the complete ceiling.

This is a pre-1.0 provider validation, egress, and behavioral contract change.
It adds no Rust type, GraphQL SDL, Cargo feature/default, entity, field, index,
constraint, persistent semantic, or backup/restore change.
`AI_SCHEMA_MODULE_VERSION` remains `0.16.0`; no AI-owned or
application-domain data migration is needed.

## Unreleased: installed local-harness foundation (crate 0.14.0 to 0.15.0)

`ProviderKind` and GraphQL `AiProviderKindInput` now include `LocalHarness`
with stable persistence/configuration value `local_harness`. Exhaustive Rust
matches must add that variant. Composed configuration GraphQL schemas gain the
corresponding enum value (`LOCAL_HARNESS` by default or `LocalHarness` with
`graphql-case-pascal`). A local-harness provider profile accepts no `base_url`:
GraphQL may enable, disable, scope, and route a logical profile, but cannot
create or alter executable, arguments, digest, working directory, sandbox,
environment, network, or resource authority.
Credential set/rotation is rejected for these profiles, and a credentialed
profile cannot be changed to `LocalHarness` until its provider credential is
removed through the ordinary audited mutation.

The opt-in `local-harness` feature exports `AiLocalHarnessRegistration`, its
immutable registry and limits, `AiLocalHarnessProvider`, the bounded
`AiJsonLinesLocalHarnessDriver`, and trusted process launcher/session traits.
Registrations require a normalized absolute executable and working directory,
fixed arguments, lowercase executable SHA-256, reviewed version, sandbox
profile, identical narrow capabilities, and hard protocol/process ceilings.
The initial registration has no environment, credential, mount, network, file,
image, built-in, tool, continuation, reasoning, background, embedding, or code
authority.

The crate does not include a generic unsandboxed child-process launcher. A host
implementation of `AiLocalHarnessProcessLauncher` must atomically verify and
execute the registered image without a shell, clear the complete inherited
environment, enforce the reviewed OS/container profile and denied network,
contain descendants, apply memory/CPU/wall/output limits, and synchronously
initiate process-tree termination on drop. Construction of the registration is
syntactic validation, not proof that those deployment controls were applied.

Every installed harness turn still enters through `AiProviderCallExecutor` as
`ProviderKind::LocalHarness`, with current-principal reauthorization, exact
egress audit, atomic budget reservation, fencing, bounded normalized output,
usage reconciliation, and protected persistence. A logical local destination
does not bypass disclosure or spend policy. The JSON-lines v1 protocol accepts
only response-started, visible-text, bounded-usage, and response-completed
events without response IDs; unsupported process events terminate the session
and fail closed.

This is a pre-1.0 public Rust API, Cargo feature, GraphQL SDL, configuration,
provider, security, and operational contract change. Default features do not
change. It adds no entity, field, index, constraint, persistent semantic, or
backup/restore change. `AI_SCHEMA_MODULE_VERSION` remains `0.16.0`; no
AI-owned or application-domain data migration is needed.

## Unreleased: native Ollama adapter (crate 0.13.0 to 0.14.0)

Enabling `provider-ollama` now compiles the native HTTP adapter and its optional
Base64/Reqwest dependencies. Construct `OllamaProvider` with
`OllamaProviderConfig` and a deployment-owned `AiProviderEndpointPolicy`. The
configured value must be a root `http` or `https` origin without URL
credentials, query, fragment, or path. Redirects are disabled. Endpoint policy
is still responsible for exact host/port allowlisting, DNS rebinding defenses,
and network-zone isolation; the configuration value is not an SSRF proof.

The adapter supports bounded native `/api/chat` NDJSON text streaming,
ephemeral inline PNG/JPEG/WebP inputs, JSON-schema structured output, and
reported prompt/evaluation token usage. Every call still requires a matching
model-inference egress proof and atomic budget proof. Each image additionally
requires its exact image-analysis transfer and freshly reopened attachment
bytes. A local destination does not imply disclosure authorization.

Custom tools, provider built-ins, non-image files, provider-response
continuation, and model thinking output are not supported by this adapter.
They fail closed rather than silently losing conversation state or persisting
hidden reasoning. Native Ollama tool calling remains gated until the runtime
can durably checkpoint and reconstruct a provider-independent stateless
conversation. No API key is required by this adapter; if a deployment places
authentication in front of Ollama, it must use a separately reviewed fixed
transport boundary rather than URL credentials.

This is an additive pre-1.0 public Rust API, feature, dependency, and provider
behavior change. Default features and GraphQL SDL do not change. It adds no
entity, field, index, constraint, persistent semantic, or backup/restore
change. `AI_SCHEMA_MODULE_VERSION` remains `0.16.0`; no AI-owned or
application-domain data migration is needed.

## Unreleased: schema module 0.15.0 to 0.16.0 and principal inbox (crate 0.12.0 to 0.13.0)

Apply AI schema module `0.16.0` while session writes, provider-output commits,
subscriptions, pruning workers, and restore callbacks are closed. Do not start
0.13.0 code against the 0.15.0 module: session creation, message queueing,
archive/restore/delete, and final assistant-output persistence now append a
principal-inbox event in the same state-machine transaction.

The managed schema changes are:

- add private `graphql_orm_ai_inbox_streams`, with deterministic ID, exact
  principal kind/subject, never-rewound `stream_head`,
  `minimum_retained_sequence`, last-event time, row-version fence, and a unique
  principal kind/subject index;
- add a unique principal kind/subject/sequence constraint to
  `graphql_orm_ai_inbox_events`;
- add required captured `scope_key`, `scope_kind`, `scope_id`, and optional
  `tenant_id` to new inbox events; and
- add nullable `scope_key`, `inbox_event_retention_seconds`, and
  `inbox_minimum_events` to retention policies, plus a unique index on
  non-null scope keys.

The inbox-event entity existed as reserved private schema, but 0.12.0 exposed
no writer, query, or subscription for it. A normal deployment should therefore
find it empty. If an early consumer wrote private rows anyway, do not infer
authority or silently assign scopes. A dependency-owned migration must either
prove each row's exact owner/session/scope, backfill captured scope fields,
validate unique contiguous per-principal sequences, and construct the matching
stream head, or remove the unsupported rows while the runtime is closed. No
client cursor existed in the public 0.12.0 GraphQL contract.

Legacy retention rows remain stored but are not effective for inbox pruning
until all three nullable migration fields are populated and valid. Supported
write-backend migration diagnostics may use `ai_scope_key` to reproduce the
stable non-secret scope identity; that value is not authorization. Prefer the
new recent-MFA-protected `setAiRetentionPolicy` mutation to create the current
scope policy. Resolve duplicate logical legacy policies explicitly before
adding/currently relying on the unique keyed policy. Never invent a retention
period or treat absence as permission to delete.

Host `AiConfigurationService` implementations must add `retention_policy` and
`set_retention_policy`. Host `AiConfigurationAccessPolicy` implementations
must handle `ReadRetention` and `ManageRetention`. Compose the corresponding
configuration query/mutation fields if GraphQL management is enabled. The
mutation is CAS-bound, requires current recent MFA in the ORM service, and
audits in the same transaction.

Hosts composing `AiQueryRoot`/`AiSubscriptionRoot` gain
`aiInboxEventPage`/`aiInboxEvents` (or coherent PascalCase names). Install an
explicit `Arc<dyn AiInboxService>`; missing registration fails closed. Schedule
`OrmAiInboxPruningService` only as a trusted host worker after all required
scope policies are current. It deletes only a bounded expired prefix, keeps the
configured recent-event floor, and atomically advances the retained cursor.
Do not expose pruning as an ordinary user mutation or manually renumber rows.

This is a pre-1.0 Rust API, GraphQL SDL, persistence, index/constraint,
authorization, backup/restore, and behavioral contract change. Cargo features
and defaults do not change. Backups must include the new stream entity and
captured inbox scope fields. Restore reconciliation must validate stream
bounds and retained-prefix continuity before reopening. No application-domain
table or data migration is required.

## Unreleased: exact provider attachment reopening (crate 0.11.0 to 0.12.0)

Provider turns containing `ModelInputBlock::Attachment` now require exact
freshly reopened bytes in addition to the attachment egress proof introduced
in 0.10.0. Configure `AiProviderCallExecutor::with_attachment_resolver` with a
trusted `AiProviderAttachmentResolver` and validated
`AiProviderAttachmentResolutionLimits`. SQLite/PostgreSQL hosts can use the
same `OrmAiAttachmentService` that owns intake. A missing resolver fails before
provider transport; do not replace it with a signed URL, raw storage key, or
model-selected object lookup.

The new public `AiProviderAttachmentRequest` and
`AiResolvedProviderAttachment` values bind opaque ID, scanner-detected MIME,
raw byte count, lowercase SHA-256, sanitized filename, and content. The
resolved type validates length/hash but is not authorization proof. Resolver
implementations must use the supplied fresh `ResolvedPrincipal`, recheck the
current session/scope/owner and released/clean/message-linked state, read only
the exact durable object, and fail if either object facts or the row changes.
`ProviderRequestContext::with_resolved_attachments` requires one-to-one exact
coverage; provider adapters retrieve content with `resolved_attachment`.

`ModelRequest::validate` now rejects duplicate attachment IDs. Its estimated
payload includes conservative Base64 expansion, so existing inference and
image/file manifests may need larger `estimated_bytes` values. The exact
capability manifest is still separate and must carry the canonical attachment
source. Deployment limits may only narrow the model/request hard limits.

With `provider-openai`, supported PNG/JPEG/WEBP/GIF inputs are sent as
ephemeral Responses `input_image` data URLs. Host scanning/acceptance must
reject animated GIFs because OpenAI accepts only non-animated GIF input. Other
host-accepted files are sent as inline `input_file` data under the adapter's
less-than-50-MiB
per-file and 50-MiB combined raw-file bounds. This path creates no provider
file ID and therefore no provider-file deletion lifecycle. Hosts remain
responsible for MIME acceptance and must separately authorize `ImageAnalysis`
or `ProviderFile`; provider rejection remains a normal uncertain transport
failure. The feature now enables an optional Base64 dependency; default
features are unchanged.

This pre-1.0 release adds public Rust APIs and changes provider behavior and
payload estimates. It adds no GraphQL SDL, entity, field, index, constraint,
backup/restore, or persistent semantic change. `AI_SCHEMA_MODULE_VERSION`
remains `0.15.0`; no AI-owned or application-domain data migration is needed.

## Unreleased: schema module 0.14.0 to 0.15.0 and attachment cleanup (crate 0.10.0 to 0.11.0)

Apply AI schema module `0.15.0` before starting the new worker. The existing
`graphql_orm_ai_attachments` table gains nullable `processing_expires_at`,
`cleanup_generation`, `cleanup_lease_expires_at`, `cleanup_retry_count`, and
`cleanup_next_attempt_at` columns. Lifecycle state fields become private
maintenance filters. No entity is added or removed; blob references retain
their backup-redaction contract.

Hosts should schedule `AiAttachmentCleanupService::cleanup_once` through a
trusted singleton or distributed worker scheduler. The service itself permits
safe concurrency: every row gets a monotonic generation, expiring CAS claim,
and redacted audit outcome. Do not expose it through GraphQL, delete storage
prefixes, or clear blob references manually. Configure upload-processing time
longer than maximum upload plus full-object scanner latency. Storage ambiguity
enters capped retry backoff rather than being reported as deletion.

`AiAttachmentServiceLimits::new` remains source-compatible and defaults the new
processing lifetime to one hour; `with_upload_processing_ttl` can narrow or
widen it within the documented hard bound. `OrmAiAttachmentService` adds
`with_cleanup_limits`. Public `AiAttachmentCleanupLimits`,
`AiAttachmentCleanupReport`, and `AiAttachmentCleanupService` are new additive
Rust APIs. GraphQL SDL, Cargo features/defaults, and application authorization
contracts do not change.

Existing pending tickets need no rewrite. Legacy interrupted `uploading` rows
with no processing deadline fall back to their ticket expiry; legacy
`deleting` rows with no deadline are reclaimable. After restore, keep the
runtime start gate closed until the module migration and normal restore
reconciliation have completed, then run cleanup. This is an AI-owned metadata
migration only; there is no application-domain data migration.

## Unreleased: exact attachment egress binding (crate 0.9.0 to 0.10.0)

Every `ModelInputBlock::Attachment` constructor must now supply the exact
verified `byte_count` and lowercase `sha256` from the released attachment. Its
separate `ImageAnalysis` or `ProviderFile` egress manifest must include a
source with `kind: "attachment"`, `trust: UserProvided`, and canonical
`reference` returned by `ModelInputBlock::attachment_egress_reference`. The
versioned value binds ID, byte count, detected MIME, and SHA-256. The manifest
byte/count limits must cover the full request including attachment bytes.
Changed content or metadata requires a new manifest decision and audit; never
copy a proof between attachments.

This is a pre-1.0 breaking Rust API and provider behavior change. It adds no
GraphQL SDL, Cargo feature/default, entity, field, index, constraint,
backup/restore, or data semantic change. `AI_SCHEMA_MODULE_VERSION` remains
`0.14.0`; no AI or application-domain data migration is required.

## Unreleased: schema module 0.13.0 to 0.14.0 and attachment intake (crate 0.8.0 to 0.9.0)

This pre-1.0 release adds the owner-isolated attachment service and composable
attachment GraphQL roots. Add the exact pinned `graphql-orm-storage` 0.5.0
dependency universe from this manifest. Construct `OrmAiAttachmentService`
with the ordinary session/scope access policy, content-protection boundaries,
a provider-neutral `BlobStore`, a complete-object `AiAttachmentScanner`, a
separate fail-closed `AiAttachmentAcceptancePolicy`, and a trusted clock.

The GraphQL `createAiAttachmentUpload` mutation returns a one-time token. A
host-owned authenticated streaming endpoint passes that token as
`SecretString` plus `StorageByteStream` to `AiAttachmentUploadService::upload`;
do not put it in a URL, log, database, or GraphQL file body. The current owner
must still authenticate. After clean scanning and policy acceptance, call
`finalizeAiAttachmentUpload`; only released/clean attachment IDs can enter the
ordinary `sendAiMessage` mutation. Existing applications must compose the new
roots explicitly; no fields are silently added.

Apply `AiSchemaModule` `0.14.0` while provider starts, uploads, subscriptions,
and restore callbacks are closed. `graphql_orm_ai_attachments` changes as
follows:

- `blob_reference`, `detected_mime`, `byte_count`, and `sha256` become nullable
  so a durable pending ticket never invents final-object facts;
- nullable `quarantine_blob_reference` keeps cleanup work addressable without
  exposing or overloading the final object reference;
- nullable `expected_byte_count`, `upload_token_hash`, and
  `upload_expires_at` bind new uploads without making legacy finalized rows
  invalid; and
- nullable scanner version, acceptance-policy version, and redacted rejection
  code record lifecycle evidence.

For existing finalized rows, no content rewrite is required. Optionally
backfill `expected_byte_count` from `byte_count`; leave upload token/expiry null.
Do not fabricate a token or move a legacy final object into quarantine. Any
legacy row whose object, checksum, owner, session, clean scan, or release state
cannot be verified must remain unavailable and be reported by restore
reconciliation. The managed migration must change column nullability/add the
new columns and record module `0.14.0`; never relabel an applied `0.13.0`
module.

This changes public Rust APIs and GraphQL SDL, but no Cargo feature/default.
Provider adapter file/image resolution, provider-side file retention/deletion,
derivative artifacts, expired-ticket/orphan pruning, scope quota configuration,
and bulk session purge remain explicit gates. No application-domain data
migration is required.

## Unreleased: schema module 0.12.0 to 0.13.0 and protected live output (crate 0.7.0 to 0.8.0)

This pre-1.0 release adds an optional durable provisional-output boundary to
`AiProviderCallExecutor`. Existing construction is unchanged and emits no
provisional events. To enable it, construct `OrmAiLiveDeltaService` with the
same run service, runtime, trusted clock, and validated protection/freshness
limits, then pass it to `with_live_delta_sink` together with validated
coalescing limits.

The new public `AiLiveDeltaSink` receives only bounded visible text or visible
reasoning-summary batches plus an immutable private-field context. A conforming
sink must rehydrate current authority, protect content, and validate the exact
session/run/attempt/generation/provider/model/budget binding. The built-in sink
does this for every batch, rechecks policy after protection, and commits a
protected `provider_live_delta` session event before the ordinary commit-only
subscription wakeup. Sink failure occurs after provider transport and therefore
leaves usage uncertain; do not automatically replay the provider call.

Clients must treat these events as provisional progress. The authoritative
`assistant_message_completed` event and windowed message blocks remain the
final transcript. A provisional event from an attempt later classified
`RecoveryRequired` remains partial history and must not be presented as a
completed assistant answer. Event payload format version 1 binds the run,
attempt, generation, provider/model/optional response, budget reservation,
batch sequence, visible kind, text, and byte count.

Advance `AiSchemaModule` to `0.13.0` through the managed `graphql-orm` schema
manager while provider starts and subscriptions remain closed. No entity,
field, index, constraint, public GraphQL root, client SDL, Cargo feature, or
default changes. No existing AI row or application-domain data rewrite is
required. The managed migration may be structurally empty, but the module bump
is mandatory because `graphql_orm_ai_session_events` gains the persistent
semantic contract for `provider_live_delta`; never relabel an applied `0.12.0`
module. Restore reconciliation must validate module `0.13.0` before reopening.

This slice does not add delta retention/purge. Existing retention policy fields
remain configuration only until the bounded pruning worker lands; do not delete
event rows manually or break session cursor monotonicity.

## Unreleased: schema module 0.11.0 to 0.12.0 and exact tool-batch adoption (crate 0.6.0 to 0.7.0)

This pre-1.0 release makes the read-only coordinator constructor deliberately
breaking: `AiReadOnlyAgentCoordinator::new` now requires an
`AiAgentCheckpointAdopter` in addition to `AiAgentCheckpointWriter`. Use the
same `OrmAiCoordinatorCheckpointService` value for both boundaries unless a
conforming wrapper preserves its current-principal, protection, durable-record,
and one-shot consumption checks.

Expired `Running` attempts are now requeued only when their linked checkpoint
is an exact `tool_batch_persisted` record with a valid protected-envelope hash,
committed/reconciled provider budget, and complete fenced tool/step rows. The
replacement claim retains that one checkpoint ID. Before planning or transport,
the adopter:

- rehydrates the current principal and rechecks session/scope access;
- reopens the protected checkpoint, tool arguments, and tool results under an
  unchanged ready protection policy;
- validates the original attempt/generation, provider response, budget,
  ordered call IDs, descriptor fingerprints, canonical arguments, disclosure
  outputs, egress manifests and immutable allow audits;
- reconstructs the loop counters and exact opaque continuation under the new
  fence; and
- atomically clears the linked checkpoint before the next provider call.

If a worker dies before consumption, the exact checkpoint can be considered by
bounded recovery again. If it dies after consumption or while the next provider
call may have started, ordinary conservative recovery applies. Provider-turn
checkpoints, partial tool batches, supervised mutations, exhausted adoption
retries, and malformed or missing records are never adopted automatically.
`AiRunRecoveryReport` adds the public `checkpoint_requeued` counter; update
exhaustive struct construction and report handling.

Advance `AiSchemaModule` to `0.12.0` through the managed `graphql-orm` schema
manager while workers and provider starts remain closed. There is no physical
entity, field, index, or constraint change, but the module version must record
the new persistent meaning of `latest_checkpoint_id`, the
`checkpoint_adoption_ready` retry marker, and one-shot checkpoint consumption.
The managed migration may therefore be structurally empty; do not skip its
module-version/readiness record or relabel an applied `0.11.0` module.

This adds no GraphQL root/SDL or Cargo feature/default. Existing completed
history needs no rewrite, and existing active provider-turn/partial/malformed
checkpoints remain closed. No AI row rewrite or application-domain data
migration is required. Restore reconciliation must validate module `0.12.0`
before reopening workers.

## Unreleased: schema module 0.10.0 to 0.11.0

Apply `AiSchemaModule` through the managed `graphql-orm` schema manager with
workers, provider starts, subscriptions, and restore callbacks closed. This
revision adds nullable private `protected_state` to append-only
`graphql_orm_ai_run_checkpoints`. It stores exact protected normalized provider
turns and completed model-visible read-only tool batches. Final assistant-output
checkpoints continue to prove their content through message/block rows and keep
this field null.

The coordinator now requires an `AiAgentCheckpointWriter`. Install
`OrmAiCoordinatorCheckpointService` with the same run service, current-principal
resolver, access policy, content-protection resolver/protector, trusted clock,
and deployment byte/freshness limits. Provider turns are checkpointed only
after authoritative budget reconciliation; tool-batch checkpoints additionally
verify every protected result, egress decision/manifest hash, run step, provider
response, and fence in the same transaction. A checkpoint failure after
external execution becomes `RecoveryRequired`.

This schema revision alone does not permit cross-generation adoption. Adoption
is supplied by the later `0.7.0` runtime contract above and is restricted to
exact complete read-only tool batches. Existing active pre-`0.11.0` runs and
malformed/absent checkpoints must continue through privileged recovery; never
infer provider output or tool results, replay a provider call, or backfill
protected state manually. Historical completed runs and final-output
checkpoints need no rewrite. No application-domain data migration is required.

This adds no public GraphQL root or client-visible SDL and changes no Cargo
feature/default. The AI schema migration is nullable/additive, but the runtime
gate must remain closed until managed validation and restore reconciliation
report module `0.11.0` ready. The new public service/trait and constructor
change advance the pre-1.0 crate from `0.5.0` to `0.6.0`; update the reviewed
Git revision and package expectation together.

## Unreleased: remote authenticated GraphQL execution (0.4.0 to 0.5.0)

This pre-1.0 Rust API boundary adds the project-agnostic private remote
execution adapter and deliberately changes
`GraphqlRequestContextFactory::build` to receive `&ToolGraphqlRequest` instead
of `&GraphqlInvocationContext`. Update every factory implementation to accept
the complete request. Local factories may continue to construct the same
ordinary application context; remote factories should use the additional
operation and variable bindings rather than discard them.

For private routed or direct targets, construct
`AiRemoteAuthenticatedGraphqlAdapter` and use the same cloned adapter value as
both `GraphqlRequestContextFactory` and `AuthenticatedGraphqlExecutor`. Supply:

- an `AiRemoteGraphqlAuthorityIssuer` that mints one audience/resource/
  operation-bound, short-lived credential while preserving the human actor;
- an `AiRemoteGraphqlTransport` that maps only deployment-registered logical
  target IDs to fixed private allowlisted destinations and propagates the
  correlation/causation audit chain; and
- validated authority-lifetime and freshly resolved principal-age limits.

Do not pass or persist the user's bearer token. Do not serialize, log, retain,
or reuse `AiRemoteGraphqlAuthority`. A direct-service transport must never
grant more authority than the equivalent routed request. The issuer and
transport remain trusted deployment boundaries: the crate binds and verifies
the redacted request but cannot inspect proprietary delegated-token claims or
prove private network configuration.

This change adds no GraphQL root or client-visible SDL, changes no Cargo
feature/default, and changes no persistent entity, index, constraint, backup,
restore, or authorization-policy data. `AI_SCHEMA_MODULE_VERSION` remains
`0.10.0`; no AI or application data migration is required. Update the package
expectation and reviewed Git revision together.

## Unreleased: schema module 0.9.0 to 0.10.0

Apply `AiSchemaModule` through the managed `graphql-orm` schema manager while
provider starts, workers, subscriptions, and approval callbacks remain closed.
This revision adds nullable restart/audit bindings to
`graphql_orm_ai_tool_calls`:

- `provider_kind`, `provider_model`, and `provider_response_id`;
- `budget_reservation_id`;
- `correlation_id` and `causation_id`; and
- `delegation_reference`.

New tool calls always populate the applicable fields. Supervised execution
requires provider/model, budget, correlation, and causation bindings and proves
that the referenced budget reservation is committed, reconciled, and matches
the exact session/run/attempt/fencing generation/provider/model before
consuming approval. Historical completed rows need no rewrite. A pending or
approved consequential row created before module `0.10.0` lacks authoritative
restart bindings and must fail closed for privileged reconciliation; do not
invent values or update private tables manually.

No application-domain data migration is required. The AI schema migration is
nullable/additive, but the runtime start gate must remain closed until managed
schema validation and restore reconciliation report module `0.10.0` ready.

### Rust API and behavior changes

- Use `AiProviderCallPlan::new_with_supervised_tools` and
  `new_supervised_continuation_with_tools` only when the deployment/scope
  policy explicitly enables exact supervised descriptors. The read-only plan
  constructors remain restricted to read-only queries.
- Implement `AiCanonicalActionPreviewBuilder` with trusted current application
  state. Returned resource versions and preview content are approval authority;
  model-written prose must never be used.
- Construct `OrmAiConsequentialToolCallService`, call `request_approval`, let a
  human decide through the existing approval GraphQL lifecycle, then call
  `execute_approved` with the exact waiting lease and current result-egress
  route. Replace the lease only when the returned persisted outcome contains a
  renewed one.
- `AiRuntime::execute_tool` now rejects descriptors whose approval rule is not
  `None`. Direct callers that previously passed a one-shot descriptor must use
  the supervised lifecycle; there is no compatibility bypass.
- `AiToolPreauthorization` proves only a fresh host tool-policy decision.
  `execute_approved_tool` recomputes and compares that policy version/state
  before resolver invocation; ordinary resolver authorization remains final.
- A post-consumption resolver or handoff ambiguity returns
  `AiConsequentialToolCallOutcome::RecoveryRequired` and terminally closes the
  run. Never retry that mutation or reuse its consumed approval.

This adds no public GraphQL field or root and changes no client SDL. Approval
query/mutation roots are unchanged. The new public APIs and deliberately
stricter runtime behavior advance the pre-1.0 crate from `0.3.0` to `0.4.0`;
consumers must update the package expectation and reviewed Git revision
together.

## Unreleased: schema module 0.8.0 to 0.9.0

This public/security/persistence slice advances the pre-1.0 crate version from
`0.2.0` to `0.3.0`. Update the Git revision and package expectation together;
the intentional initial-turn constructor restriction and new recovery-report
field are a pre-1.0 breaking API/behavior boundary.

Apply `AiSchemaModule` through the managed `graphql-orm` schema manager while
provider starts, workers, subscriptions, and callbacks remain closed. This
revision adds:

- nullable `graphql_orm_ai_runs.latest_checkpoint_id`; and
- append-only `graphql_orm_ai_run_checkpoints`, bound to the exact run,
  attempt, fencing generation, provider response reference, settled budget
  reservation, final assistant message, and a stable redacted checkpoint hash.

The protected assistant message/blocks, session event, renewed run fence, and
`assistant_output_persisted` checkpoint now commit in one state-machine
transaction. If the worker dies after that transaction but before terminal run
finalization, expired-lease reconciliation verifies the complete checkpoint,
hash, attempt/generation, and finalized assistant message before committing
`Completed`. Missing, swapped, malformed, or any other active checkpoint still
fails closed or becomes `RecoveryRequired`; it is never replay authority.

The module version is `0.9.0`; never apply these semantics under an earlier
version. Existing completed/history rows require no rewrite and may keep a null
checkpoint reference. Reconcile any existing active pre-release run through
the prior owning service before reopening workers. Do not invent checkpoint
rows or update the private tables with manual SQL. No application-domain data
migration is required. Keep the runtime start gate closed until managed schema
validation and restore reconciliation report module `0.9.0` ready.

### Rust API and behavior changes

- `AiReadOnlyAgentCoordinator` now owns the bounded top-level read-only loop.
  Hosts implement `AiReadOnlyAgentTurnPlanner` and supply proof-preserving run,
  provider-turn, tool, and output services. Replace the lease after every
  fenced operation and configure a heartbeat interval comfortably shorter than
  the run-service lease TTL.
- Provider/tool/output ambiguity is durably classified as
  `AiReadOnlyAgentRunOutcome::RecoveryRequired` when the current fence can
  commit it. A lost heartbeat fence returns an error without attempting a
  terminal write.
- `AiProviderCallPlan::new_with_tools` is now for initial turns only. Code that
  previously supplied `ModelContinuation` or `ModelInputBlock::ToolResult`
  directly must retain the exact `AiAgentContinuation` and call
  `new_continuation_with_tools`.
- `AiRunRecoveryReport` has a new public `completed` counter.
- In that release, `AiLiveDeltaCoalescer` and related public types provided
  synchronous bounded batching only. A raw batch remains neither authorization
  nor a durability proof. The later crate `0.8.0` contract at the top of this
  guide supplies the optional protected ORM sink.

This adds no public GraphQL root or client-visible SDL. The new generated ORM
records remain private implementation entities.

## Unreleased: multi-repository ownership workflow

Development now assigns one owning agent and isolated branch/worktree to each
repository. The `graphql-orm-ai` agent may inspect `agql-auth` and
`graphql-orm` read-only but sends requested changes to their owners instead of
mutating sibling worktrees. Upstream crates merge first and report final commit
SHAs; this crate then repins and verifies the reviewed dependency universe.

This is a contributor workflow change only. It changes no consumer Rust API,
GraphQL SDL, feature/default, configuration, authorization behavior, schema
module, backup/restore contract, or persisted data. No consumer or data
migration is required.

## Unreleased: documentation and release enforcement

CI and the documented release gate now deny missing public Rust documentation
in addition to ordinary Rustdoc warnings. The release-policy check also
requires `README.md`, `CHANGELOG.md`, and `MIGRATION.md` to move together with
public Rust/runtime changes. Contributors must add useful Rustdoc for every new
public item, including `# Errors` sections for fallible APIs and explicit proof
boundaries for security-sensitive types. This changes no consumer Rust API,
GraphQL SDL, feature/default, runtime behavior, schema module, or persisted
data; no consumer or data migration is required.

## Unreleased: upstream dependency alignment

The public manifest now resolves one exact Git dependency universe:

- `graphql-orm` 0.9.0 at
  `f996cdbe2ef1867dea029ec3ff16e051dbe7566e`; and
- `agql-auth` 0.10.0 at the peeled `v0.10.0` target
  `c92dcb441237bbe308499b26525945f60ffa394a`.

Remove host patches or path overrides to older sibling versions. Hosts that
also depend directly on either crate must use these exact revisions so Cargo
resolves one source/type universe. This changes no `graphql-orm-ai` GraphQL
SDL, AI schema-module version, persisted AI data, or application authorization
policy; no AI data migration is required.

`agql-auth` 0.10.0 separately adds a nullable `authorization_policy` field to
OAuth state storage. Hosts using its OIDC authorization-state persistence must
apply the auth crate's 0.10.0 migration: legacy absence remains an ordinary
login, while a flow requiring a bound policy fails closed when that binding is
absent. Hosts that do not use that OIDC storage need no auth data migration.
`graphql-orm` 0.9.0 adds retention metadata to its public schema, backup,
runtime, and migration models. Hosts constructing those ORM metadata types
manually must follow the upstream 0.9.0 migration notes. This AI crate uses the
derive-generated metadata and opts only run checkpoints into purge.

## Unreleased: schema module 0.7.0 to 0.8.0

Apply `AiSchemaModule` with provider starts and workers closed. This revision
adds the persistent semantics required by authenticated proposal and approval
lifecycles:

- `graphql_orm_ai_proposals.item_count` stores the schema-validated logical
  review-item count;
- proposal, proposal-item, and approval IDs are assigned by the owning service
  before content protection so envelope associated data binds the real row ID;
- proposal and approval tables expose stable dependency-owned keyset metadata;
  and
- approval `session_id` is an internal filterable field for bounded,
  scope-authorized repository windows (it is not exposed as generic CRUD).

The module version is `0.8.0`; do not apply these semantics under an earlier
module version. Existing pre-release proposal rows have no authoritative item
count, and existing protected proposal/approval envelopes may not have been
bound to a service-assigned row ID. Reconcile and remove unfinished rows through
their owning pre-release service, or recreate a disposable environment. Never
invent item counts, decrypt/rewrite envelopes with manual SQL, or treat a prior
pending/approved row as consumable. Completed chat history and provider usage
need no data rewrite.

Keep the runtime gate closed until managed migration validation and restore
reconciliation report module `0.8.0` ready.

### Rust, GraphQL, and behavior changes

- `OrmAiProposalService::persist_validated` stages a catalog-validated proposal
  through the current `AiRunLease` and returns a renewed lease. The previous
  lease is stale.
- `AiProposalService` provides bounded reads and CAS review. `AcceptEdited`
  requires a replacement payload and item count and revalidates both against
  the current exact registered schema version.
- `AiProposalOutcomeRecorder::record_applied_outcome` now takes an
  `&AuthPrincipal`. Call it only after the ordinary application mutation has
  committed; the service rehydrates/authorizes and links the authoritative
  application audit reference without performing the mutation.
- `OrmAiApprovalService::request_approval` requires a server-generated
  canonical preview and complete binding, then atomically parks the exact run
  and tool call in `WaitingApproval`. Its constructor now also requires the
  current `AiToolCatalog`; request and consumption reject catalog/descriptor/
  GraphQL-contract drift.
- `decide_approval` and `revoke_approval` are CAS-bound authenticated GraphQL
  operations. Recent MFA is enforced when the durable request requires it.
- `consume_approval` requires the current waiting lease plus a freshly rebuilt
  binding/preview, rehydrates the original actor, atomically consumes exactly
  once, and returns a renewed `Running` lease and `ConsumedAiApproval`. That
  proof does not replace the fresh resolver authorization/resource-version
  check that must immediately follow.
- Hosts may compose `AiProposalQueryRoot`, `AiProposalMutationRoot`,
  `AiApprovalQueryRoot`, and `AiApprovalMutationRoot`. This adds public GraphQL
  SDL when those roots are composed; regenerate affected client documents.
  Default names are camelCase and the `graphql-case-pascal` feature changes the
  entire new contract coherently without aliases.
- `AiProposalCatalog::descriptor` is new, and registration now rejects zero or
  excessive payload/source/item limits.

No application-domain data migration is required. No proposal review or
approval decision grants domain write authority by itself.

## Unreleased: schema module 0.6.0 to 0.7.0

Apply `AiSchemaModule` through the managed `graphql-orm` schema manager with
workers and provider starts closed. This revision extends
`graphql_orm_ai_tool_calls` with:

- a unique run/provider-call key plus the opaque provider call ID;
- provider-turn and within-turn ordering;
- current authorization policy and authorization-state bindings;
- static disclosure fingerprint and result classification;
- exact result-egress decision ID and manifest hash; and
- the ordinary application audit reference.

The module version is `0.7.0`; never apply these persistent semantics under a
previous module version. Existing pre-release active tool-call rows cannot
safely infer provider call identity, current authorization, disclosure, or
egress proofs. Stop/reconcile them through their owning pre-release service and
classify ambiguity as recovery-required. Recreate a disposable environment if
that service path is unavailable. Do not backfill invented values or use manual
SQL. Conversational messages and completed provider usage need no data rewrite.

Keep the runtime gate closed until managed schema validation and restore
reconciliation report module `0.7.0` ready. This change adds no public GraphQL
root or SDL field, so GraphQL clients need no document regeneration.

### Rust API and behavior changes

- `ModelRequest` requires a `continuation: Option<ModelContinuation>` field.
  Existing request literals should set it to `None`.
- `ModelInputBlock` adds `ToolResult`. A request containing tool results must
  carry an exact previous-response continuation, and a continuation without a
  tool result is rejected by this initial contract.
- `AiProviderCallLimits::with_maximum_tool_calls` sets a per-turn hard limit.
- `AiProviderCallPlan::new` still rejects custom tools. Use `new_with_tools`
  only with an exact current `AiToolPolicySet`; it accepts registered,
  fingerprint-matching, explicitly enabled, idempotent read-only application
  queries with no approval requirement. Use `new_continuation_with_tools` for
  subsequent turns so result blocks and exact manifests cannot be swapped.
- `AiProviderCallResult::tool_calls` exposes normalized unforgeable call
  requests, and `continuation` returns the exact prior-response identity.
- `OrmAiApplicationToolCallService` and `AiApplicationToolCallLimits` own
  protected/fenced read-only resolver execution and result egress. Replace the
  lease after every returned outcome.
- `AiAgentLoopGuard` enforces provider-turn/tool-call bounds and exact call/
  result/continuation ordering. Reconstructing a guard is not a recovery
  mechanism; uncertain work remains closed for restore/operator review.
- `OrmAiProviderOutputService::persist` now rejects results with pending custom
  tool calls.
- OpenAI stateful continuation requires `store_responses = true` and every
  exact transfer manifest must use
  `AI_EGRESS_RETENTION_PROVIDER_RESPONSE` (`provider_response`). The default
  remains false. No provider retention is silently enabled by migration.

The public API additions and `ModelRequest` field are deliberate pre-1.0
changes within the unreleased `0.2.0` line. This `0.7.0` slice did not change
approval semantics; the later `0.8.0` section defines canonical-preview and
one-shot approval persistence. Consequential execution remains unavailable
until the separately gated executor performs fresh resolver authorization.

## Unreleased: schema module 0.5.0 to 0.6.0

The crate version moves from `0.1.0` to `0.2.0` because this revision changes
public pre-1.0 Rust contracts as well as persistence.

Apply `AiSchemaModule` through the managed `graphql-orm` schema manager. This
revision changes budget persistence and must not reuse a previously applied
`0.5.0` module version.

Budget policy rows gain optional principal-kind matching and tool/image unit
ceilings. Budget counter rows gain:

- a stable `period_key` and unique `(budget_policy_id, period_key)` boundary;
- reserved and committed tool/image units; and
- an upsert identity for safe concurrent counter creation.

Budget reservation rows gain principal-kind binding, unique
`(principal_kind, principal_subject, idempotency_key)` enforcement, and an
`actual_runs` field so every reserved dimension reconciles completely.

Run-attempt completion/retry/recovery is now stored in the new append-only
`graphql_orm_ai_run_attempt_outcomes` table, uniquely keyed by `attempt_id`.
The existing append-only claim row remains immutable. Egress event IDs are now
caller-supplied exact `AiEgressDecisionId` values rather than unrelated
generated UUIDs.

The legacy optional completion columns on the pre-release attempt-claim table
remain physically present for non-destructive migration compatibility, but new
workers leave them null. The separate outcome table is the source of truth;
do not disable append-only enforcement to populate the legacy columns.

The `0.5.0` pre-release counter and reservation shapes cannot safely infer
principal kind or a stable interval key. Before applying this migration in an
early test deployment, stop workers and provider starts, classify every
in-flight call, preserve required audit/usage facts, and remove the old active
counter/reservation rows through the owning pre-release service. Do not invent
bindings or run manual SQL. Recreate disposable environments when that safe
service path is unavailable. Keep the runtime start gate closed until schema
validation and reconciliation report module `0.6.0` ready.

### Rust API and behavior changes

- `AiBudgetService::reconcile` returns `AiBudgetReconciliationResult` with
  committed, released, and still-held amounts.
- `AiError::BudgetDenied` reports missing/exhausted applicable capacity.
- `AiBudgetReservation` and `AiBudgetReconciliationResult` are no longer Serde
  deserializable. Obtain reservations from `AiBudgetService`; do not reconstruct
  proof-bearing values from persisted or client-controlled JSON.
- `OrmAiBudgetService` requires validated deployment-owned
  `AiBudgetServiceLimits` and a trusted `agql-auth::Clock`.
- A request now requires exactly one run unit, a fresh `ResolvedPrincipal`, a
  current running lease/attempt/fencing generation, an active persisted
  session with matching owner/tenant/exact scope, an expiry no later than the
  lease, and at least one applicable policy.
- Ordinary reconciliation may move an uncertain reservation to committed when
  authoritative actual usage arrives, but cannot optimistically release it.
- Orchestration must persist `MarkUncertain` immediately before handing the
  authorized reservation proof to provider transport. `ReleaseUnused` is only
  available while the durable reservation is still `Reserved`.
- `OrmAiRunService` and `AiRunServiceLimits` now own queue claims, heartbeats,
  retries, completion, and expired-lease recovery. Callers must replace the
  lease value after every successful fenced write; an older row-version proof
  is deliberately rejected.
- `AiProviderCallExecutor` requires a durable `AiEgressDecisionAudit`, budget
  service, `AiProviderUsageAccounting`, trusted clock, and bounded
  `AiProviderCallLimits`. Accounting implementations must resolve the exact
  `pricing_policy_version`, return provider-observed input/output tokens and
  one run exactly, and authoritatively compute cost/tool/image units. It
  supports one security-ordered provider turn. Tool-free construction remains
  the default; the later `0.7.0` section documents the separately gated
  read-only application-tool path.
- A successful provider result must be passed to
  `OrmAiProviderOutputService::persist` before terminal completion. Use the
  renewed lease returned by that service for `OrmAiRunService::finish`.
- `AiProviderCallResult` is bound to session/run/attempt/generation/provider/
  model and is not a transferable authorization proof.

This release adds no GraphQL budget configuration fields yet. Existing host
GraphQL SDL is unchanged, so no client document regeneration is needed for
this worker/provider slice. Hosts that previously implemented the budget trait
must update the reconciliation return type and preserve the same atomic,
fenced, idempotent semantics. Existing pre-release run claims have no outcome
fact to backfill unless an authoritative final state is known; never infer a
provider outcome. Keep ambiguous active work closed for recovery review.

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
