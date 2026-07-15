# Bounded Session Retention

`OrmAiSessionRetentionService` is a trusted, host-scheduled maintenance service
for narrowly defined classes of protected chat data:

- expired provisional `provider_live_delta` session events;
- age-expired protected tool arguments/results and approval resource
  bindings/action previews for exact terminal calls under the current
  `raw_payload_retention_seconds` policy;
- age-expired orphaned protected coordinator checkpoints for terminal runs
  after exact attempt/outcome, budget, current-pointer, and final-output or
  tombstoned-tool dependency proof;
- expired preview/block content from finalized messages whose producing run is
  terminal and which have no linked attachment;
- after a deleting-session cutoff, every bounded protected session event;
- after that cutoff, protected context-summary checkpoints;
- after context summaries are exhausted, terminal proposal and optional
  proposal-item protected content under whole-session lookahead bounds;
- after proposal payloads are exhausted, protected arguments/results and
  approval resource bindings/action previews for a completely bounded,
  terminal, exactly linked run/tool/approval graph;
- after tool and approval payloads are exhausted, attachment artifacts and
  then their parent attachments coordinated through separately verified exact-
  reference local/provider cleanup, followed by their ordinary metadata rows;
- after attachments are exhausted, the same safely detachable terminal message
  content even when ordinary message retention is disabled; and
- after all those protected sources are exhausted, immutable coordinator
  checkpoints belonging to a bounded, entirely terminal run set.

It does not expose a GraphQL mutation and does not issue SQL. All reads, CAS
updates, deletes, audit appends, keyset cursors, and transactions use generated
`graphql-orm` entity APIs. Database-specific syntax and migrations remain the
responsibility of `graphql-orm`.

## Scheduling

Construct the service with the AI ORM database, a trusted `agql-auth::Clock`,
and validated `AiSessionRetentionLimits`. Use
`new_with_context_checkpoints` when context summaries need a limit distinct
from the message-row limit, and chain `with_run_checkpoint_limits` when run
proofs and immutable checkpoint pages need independent bounds. Chain
`with_proposal_limits` when whole-session proposal and proposal-item proofs need
independent bounds. Chain `with_tool_payload_limits` when whole-session tool
call and approval proofs need independent bounds. Chain
`with_attachment_limit` when a whole-session attachment proof needs its own
bound, and `with_attachment_artifact_limit` for the independent complete
artifact-set proof. Schedule `OrmAiAttachmentService::cleanup_once`
independently; session retention never performs storage or provider I/O. Start
a complete scan cycle by calling
`prune_session_content(None)`. If the report contains
`next_session_cursor`, pass that opaque value unchanged to the next call.
Continue until the cursor is absent, then begin a later scheduled cycle from
`None`.

Every call and every session transaction is bounded independently. A report
states only what that completed page changed; it is not a global erasure
certificate. Persist worker scheduling/telemetry outside model-visible state,
alert on repeated `sessions_not_ready`, `sessions_conflicted`,
`messages_blocked`, `attachment_cleanups_blocked`, or
`proposal_payload_purges_blocked`, `tool_payload_purges_blocked`,
`raw_payload_purges_blocked`, `raw_checkpoint_purges_blocked`, or
`run_checkpoint_purges_blocked`. Also alert on parent or artifact cleanup
failures/deferred claims and never treat a partial scan cycle as complete.

## Policy and deletion rules

For each candidate, the worker reloads the session and its exact deterministic
scope policy inside one state-machine transaction. It validates stored scope
fields against the scope key and all retention bounds. Missing, duplicated,
legacy, or corrupt policy keeps content in place.

Expired provisional deltas are selected only by the fixed
`provider_live_delta` event kind. Their protected payloads are never opened.
Deleting one may create a sequence gap; sequence heads never move backward and
sequence values are never reused.

For active, archived, or pre-cutoff deleting sessions, the worker also computes
`now - raw_payload_retention_seconds` with checked arithmetic. It loads the
complete session run and tool-call sets under lookahead bounds, but selects only
calls whose owning run and call state are terminal and whose trusted completion
timestamp is at or before that cutoff. Any referenced approval must be the
exact state-compatible terminal one-shot record. The complete eligible subset
and its pre-purge payload shape validate before approval resources/previews are
cleared ahead of tool arguments/results. Each changed row receives
`payload_purged_at` and the transaction appends redacted audit.

Newer calls, nonterminal runs, and pending/approved/resume-claimed approvals are
not eligible and remain intact; they do not prevent an independently expired
terminal subset from being scrubbed. Lookahead overflow or malformed eligible
linkage blocks the age-based phase without a partial update. Provider adapters
normalize bounded responses and do not persist raw HTTP envelopes. Protected
provider/tool continuation state in immutable coordinator checkpoints is a
separate dependency and is not removed by this age-based phase.

A separate database-enforced append-only maintenance transaction applies the
same checked raw cutoff to protected `provider_turn_persisted`,
`tool_batch_persisted`, and `supervised_tool_batch_persisted` checkpoints. It
selects a deterministic created-time/ID page only from terminal runs and skips
every checkpoint named by a current run pointer. Without loading
`protected_state`, it re-proves the exact current scope policy, checkpoint
metadata, closed immutable attempt outcome, and committed/reconciled budget.
A provider-turn checkpoint must additionally have either a later current
final-output checkpoint and durable assistant message or its complete
correlated terminal tombstoned tool set. Tool-batch checkpoints require at
least one exact terminal tombstoned call; supervised batches require exactly
one. Every optional one-shot approval must be exact, terminal, and tombstoned.

All selected rows validate before one exact-cardinality purge and redacted
audit. A current checkpoint, nonterminal or recovery-required run, missing
attempt outcome, untombstoned call/approval, incomplete final-output proof,
lookahead overflow, or malformed correlation blocks this phase without a
partial delete. Sessions past the deleting cutoff use the stronger dependency-
ordered whole-session workflow instead. The age-based checkpoint phase retains
run, attempt/outcome, budget/usage, egress, call/approval, and audit metadata.

A session in `deleting` state must carry the exact `deleted_at` timestamp
written by the authenticated session lifecycle. Once that timestamp plus the
current policy's `deleted_content_purge_seconds` is at or before the trusted
clock, each bounded transaction may delete any protected session event kind.
The same event-page limit applies, so repeated scheduled cycles may be needed.
Before the cutoff, only ordinary expired live deltas are eligible. A malformed
state/timestamp pair or arithmetic overflow fails closed without deleting
content.

At the deleting-session cutoff, the worker also selects a bounded page of
protected context-summary checkpoints. If that page is nonempty, it deletes
the exact rows but skips every message body in the same pass. Repeated passes
must exhaust context summaries before message scrubbing can begin. Event and
context deletion may share one transaction and one redacted audit, but a
summary can never remain after this worker has scrubbed content it could
cover.

Only after the context page is empty does retention load the complete proposal
set and every optional proposal item under independent lookahead bounds. It
validates exact session/scope/run bindings, stable item ordering, and a terminal
owning run without opening protected values. Rejected, applied, expired, and
expired pending-review proposals are eligible. An expired pending review is
changed to `expired`. Accepted and accepted-edited proposals remain blocked
because a trusted application mutation or authoritative outcome record may
still be pending.

For an eligible whole-session set, one transaction clears item suggested
values, rationales, sources, and protected review values before clearing each
parent payload/source pair and writing `payload_purged_at`. Proposal and item
identity, type/schema, logical count, state, review decisions, creator/reviewer,
application resource/audit links, timestamps, and row versions remain as
non-content metadata. Any over-bound set, nonterminal run, unresolved accepted
state, malformed binding, or CAS race leaves all proposal content in place.
Attachment coordination and message scrubbing wait until a later pass.

Only after proposal payloads are exhausted does retention load the complete
bounded run, tool-call, and approval sets without opening protected values.
Every run must belong to the deleting session and be terminal. Every eligible
call must use a known terminal outcome and have an exact finished
`application_tool` step with matching run, state, and lease generation. Any
approval must bind exactly once in both directions, belong to the same session,
be terminal, and match its call outcome: consumed approvals bind completed or
closed execution outcomes, while denied/revoked/expired approvals bind only the
matching closed call state.

Before changing anything, the transaction validates the complete graph and
pre-purge payload shape. Active calls, pending/approved/resume-claimed
approvals, nonterminal or recovery-required runs, missing steps, cross-session
or duplicate linkage, incompatible states, malformed tombstones, and any
lookahead overflow block the whole set. For an eligible set, approval resource
bindings and canonical previews are cleared before tool arguments/results; each
row receives `payload_purged_at`. IDs, provider and tool references, hashes,
risk, state, authorization and egress evidence, application audit references,
approval decision/use metadata, timestamps, and row versions remain. Later
attachment and message phases wait for a separate pass. Ordinary approval,
checkpoint, and consequential-tool paths treat a tombstoned protected value as
unusable and fail closed.

Only after tool and approval payloads are exhausted does retention load the
session's entire attachment set and its complete artifact set under independent
lookahead bounds. Any over-bound set blocks the whole phase without a partial
claim. Each artifact is validated against its exact parent without opening
protected content. Retention either deletes metadata already carrying a fully
cleaned generation-fenced tombstone, leaves an existing claim/backoff intact,
or CAS-moves the artifact into private `cleanup_required` state without
clearing its blob reference, provider reference, expiry, or protected
derivative. While any artifact remains, its parent attachment is not moved into
cleanup.

The independently scheduled attachment worker processes artifact candidates
before parent candidates. Its claim transaction reloads the exact artifact and
parent, deleting session, current scope policy, and cutoff, then rotates a
monotonic generation and expiring lease. It deletes only the stored exact local
blob reference and confirms absence. A provider reference is eligible only
when the current policy has `provider_file_delete_required = true` and the host
installed `AiProviderFileDeletionService`. That boundary receives a redacted-
debug exact reference request; `Ok(())` must mean authoritative absence. An
expiry timestamp, missing service, rate limit, successful request without
absence semantics, or any other ambiguity is not proof and retains all
metadata/protected content under capped retry backoff.

After every external object is proven absent, one fenced CAS clears both
references, provider expiry, and protected derivative, writes `deleted_at`,
and appends redacted audit. A later retention pass physically deletes that
artifact metadata. Only once no artifact row remains does the parent attachment
follow the existing lifecycle:

- deletes ordinary metadata already proven fully cleaned and tombstoned by a
  positive cleanup generation;
- leaves an existing cleanup claim/backoff untouched; or
- CAS-moves the row to private `deleting` / `retention_cleanup_required` state
  without clearing either object reference.

The attachment worker claims that parent state only after reloading the same
deleting-session proof. It deletes only the row's opaque final and quarantine
references and confirms their absence. Failure or ambiguity retains the
references under bounded backoff. Successful cleanup clears the references and
records a deleted tombstone; only a later retention transaction deletes that
metadata. Both levels can therefore leave an object-free tombstone after a
crash, never a metadata deletion that merely assumes external success.

Message content is scrubbed only when all of these are true:

- either `message_retention_seconds` is configured and the finalized timestamp
  has expired, or the deleting-session cutoff has been reached;
- the message is complete and still has a protected preview plus an exact,
  bounded set of ordered blocks;
- the linked run belongs to the same session and is terminal;
- no attachment row references the message; and
- the message CAS version still matches.

The transaction clears the protected preview, deletes exact block rows, writes
`content_purged_at`, sets `block_count` to zero, and appends a redacted audit
fact. Any failure rolls back the whole session change.

Run-checkpoint purge starts only in a later pass that proves no protected
session event, context checkpoint, proposal/item payload, tool/approval
payload, attachment row, or unpurged message content remains. The run query
uses a lookahead bound and every returned run must belong to the session, have
valid fencing metadata, and be terminal. Each non-null current checkpoint
pointer must identify an exact, structurally valid checkpoint for that run. The
ordinary state-machine transaction clears those pointers with CAS before any
physical deletion can start.

The worker then opens a generated `graphql-orm` retention transaction. It
reloads and validates the deleting session, current scope policy, cutoff,
empty-source proofs, bounded terminal runs, complete tool/approval tombstones,
exact call/step/approval linkage, and absent pointers. It selects one
created-time/primary-key-ordered checkpoint page, validates only redacted
structure without opening protected state, deletes the exact typed ID set under
a nonzero `MutationLimit`, and appends a redacted purge audit atomically. A
crash between pointer clearing and purge leaves an unreferenced checkpoint for
a later pass; it cannot leave a run pointing at a deleted row.

Constructing the service grants `RetentionMaintenance` only for the exact run-
checkpoint entity and policy key on its private database-handle clone.
Ordinary append-only update/delete remains database-prohibited. Pricing,
skills, usage, audit, egress, and run-attempt/outcome facts are not opted in.

## Reader and frontend behavior

Authorized message pagination retains a small metadata shell. A purged message
has `content_purged = true`, zero blocks, and the fixed server-authored preview
`Content removed by retention policy`. An authorized block query returns an
empty window. The service checks session authorization before revealing even
that tombstone state.

When an event replay window crosses a removed sequence,
`session_event_page` returns no events and `reset_required = true`. A client
must discard provisional per-run rendering and reload bounded authoritative
session/message windows. It should continue using virtualized keyset windows;
retention never requires loading the complete transcript into the DOM.

## Deliberate limits

This workflow does not delete session or message metadata, active/ineligible
attachment artifacts or provider-persistent files lacking exact absence proof,
runs, run attempts/outcomes, tool-call or approval metadata, proposal metadata,
current or ineligible protected
normalized coordinator state, usage, egress decisions, audit facts,
pricing/skill history, or restore evidence. It therefore
advances but does not complete the `deleting` session lifecycle. Proposal
protected content is eligible only through its terminal whole-session proof.
Tool/approval protected payloads may use either the selective age-based proof
or, after the deletion cutoff, their terminal whole-session proof. Unresolved
accepted proposals and active or uncertain tool authority remain deliberately
closed. No raw provider HTTP envelope is persisted. Eligible orphaned protected
coordinator state follows the independent age-based proof above; current or
ambiguous checkpoint state remains.
Attachment artifacts and basic attachment objects/metadata are eligible only
through the dependency-ordered two-worker proof above. Unsafe or ambiguous
artifact, message, or run dependencies remain in place and are counted as
blocked. The remaining
resources require separate workers with their own dependency ordering,
external delete confirmation, fencing, restore reconciliation, and audit
contracts.

Ordinary message-retention expiry does not yet delete context checkpoints. The
context-compaction producer remains unimplemented and must stay disabled until
its ordinary-retention invalidation and exact source-coverage contract lands.

Restore collectors must distinguish validated retention gaps from corruption
and must validate message, proposal, tool, approval, attachment, and artifact
tombstone invariants, including exact terminal call/step/approval linkage and
every artifact parent/cleanup/reference state. A nonzero
`invalid_session_retention_count` is fatal and keeps runtime readiness closed.
