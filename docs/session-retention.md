# Bounded Session Retention

`OrmAiSessionRetentionService` is a trusted, host-scheduled maintenance service
for four narrowly defined classes of protected chat data:

- expired provisional `provider_live_delta` session events;
- expired preview/block content from finalized messages whose producing run is
  terminal and which have no linked attachment;
- after a deleting-session cutoff, every bounded protected session event;
- after that cutoff, protected context-summary checkpoints, followed only on a
  later pass by the same safely detachable terminal message content, even when
  ordinary message retention is disabled.

It does not expose a GraphQL mutation and does not issue SQL. All reads, CAS
updates, deletes, audit appends, keyset cursors, and transactions use generated
`graphql-orm` entity APIs. Database-specific syntax and migrations remain the
responsibility of `graphql-orm`.

## Scheduling

Construct the service with the AI ORM database, a trusted `agql-auth::Clock`,
and validated `AiSessionRetentionLimits`. Use
`new_with_context_checkpoints` when context summaries need a limit distinct
from the message-row limit. Start a complete scan cycle by
calling `prune_session_content(None)`. If the report contains
`next_session_cursor`, pass that opaque value unchanged to the next call.
Continue until the cursor is absent, then begin a later scheduled cycle from
`None`.

Every call and every session transaction is bounded independently. A report
states only what that completed page changed; it is not a global erasure
certificate. Persist worker scheduling/telemetry outside model-visible state,
alert on repeated `sessions_not_ready`, `sessions_conflicted`, or
`messages_blocked`, and never treat a partial scan cycle as complete.

## Policy and deletion rules

For each candidate, the worker reloads the session and its exact deterministic
scope policy inside one state-machine transaction. It validates stored scope
fields against the scope key and all retention bounds. Missing, duplicated,
legacy, or corrupt policy keeps content in place.

Expired provisional deltas are selected only by the fixed
`provider_live_delta` event kind. Their protected payloads are never opened.
Deleting one may create a sequence gap; sequence heads never move backward and
sequence values are never reused.

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

This worker does not delete session or message metadata, attachments or blob
objects, runs, run/coordinator checkpoints, tool calls/results, proposals,
approvals, provider raw payloads, provider-persistent files, usage, egress
decisions, audit facts, fencing, or restore evidence. It therefore starts but
does not complete the `deleting` session lifecycle. Unsafe message dependencies
remain in place and are counted as blocked. The remaining resources require
separate workers with their own dependency ordering, external delete
confirmation, fencing, restore reconciliation, and audit contracts.

Ordinary message-retention expiry does not yet delete context checkpoints. The
context-compaction producer remains unimplemented and must stay disabled until
its ordinary-retention invalidation and exact source-coverage contract lands.

Restore collectors must distinguish validated retention gaps from corruption
and must validate the tombstone invariant. A nonzero
`invalid_session_retention_count` is fatal and keeps runtime readiness closed.
