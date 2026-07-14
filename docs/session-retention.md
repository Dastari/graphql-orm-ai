# Bounded Session Retention

`OrmAiSessionRetentionService` is a trusted, host-scheduled maintenance service
for two narrowly defined classes of protected chat data:

- expired provisional `provider_live_delta` session events; and
- expired preview/block content from finalized messages whose producing run is
  terminal and which have no linked attachment.

It does not expose a GraphQL mutation and does not issue SQL. All reads, CAS
updates, deletes, audit appends, keyset cursors, and transactions use generated
`graphql-orm` entity APIs. Database-specific syntax and migrations remain the
responsibility of `graphql-orm`.

## Scheduling

Construct the service with the AI ORM database, a trusted `agql-auth::Clock`,
and validated `AiSessionRetentionLimits`. Start a complete scan cycle by
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

Message content is scrubbed only when all of these are true:

- `message_retention_seconds` is configured and the finalized timestamp has
  expired;
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

This worker does not delete sessions, message metadata, attachments or blob
objects, runs, tool calls/results, proposals, approvals, provider raw payloads,
provider-persistent files, usage, egress decisions, audit facts, fencing, or
restore evidence. It also does not complete the `deleting` session lifecycle.
Those require separate workers with their own dependency ordering, external
delete confirmation, fencing, restore reconciliation, and audit contracts.

Restore collectors must distinguish validated retention gaps from corruption
and must validate the tombstone invariant. A nonzero
`invalid_session_retention_count` is fatal and keeps runtime readiness closed.
