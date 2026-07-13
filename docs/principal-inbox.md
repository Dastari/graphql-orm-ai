# Durable Principal Inbox

The principal inbox is a small, durable cross-session activity stream for chat
drawers, notification badges, and background-run completion. It complements
the per-session event stream; it does not replace message, block, or session
pagination.

Each exact principal kind and subject owns one monotonic sequence. Creating a
session, queueing a user message, changing archive state, beginning deletion,
or committing an authoritative assistant message appends a protected inbox
event in the same ORM transaction as the underlying state change. A rollback
therefore creates neither the state change nor its notification.

## GraphQL contract

Compose the ordinary `AiQueryRoot` and `AiSubscriptionRoot`, and install both
service objects in schema data:

- `Arc<dyn AiSessionService>` for session windows and mutations;
- `Arc<dyn AiInboxService>` for `aiInboxEventPage` and `aiInboxEvents`.

`OrmAiInboxService` is the SQLite/PostgreSQL implementation. A missing inbox
service fails closed; registration is never inferred from the session service.
The optional `graphql-case-pascal` feature exposes the coherent
`AiInboxEventPage` and `AiInboxEvents` spellings with PascalCase arguments and
fields, without lowercase aliases.

The page takes an exclusive `afterSequence` and a `first` bound in `1..=500`.
It returns:

- authorized, opened events in ascending principal sequence;
- the captured stream `watermark`;
- `hasMore` for bounded catch-up; and
- `resetRequired` when retention removed history required by the cursor.

The subscription takes the same exclusive cursor. It attaches its commit-only
wakeup receiver before replay, pages durable rows to a captured watermark, and
then follows wakeups. A wakeup is only a hint: delivery always re-reads ORM
state and repeats principal/session/scope authorization plus protected-content
opening. Broadcast lag triggers durable replay rather than event loss.

Current event types are server-authored and bounded:

| Event | Meaning |
| --- | --- |
| `session_created` | A new owner-only session committed. |
| `message_queued` | A user message and fenced run committed atomically. |
| `assistant_message_completed` | Authoritative protected assistant output committed. |
| `session_archived` | The session entered archived state. |
| `session_restored` | The session returned to active state. |
| `session_deleting` | Durable deletion/purge state began. |

Payloads contain only bounded identifiers and state needed to refresh a
session shell. They use the exact scope content-protection policy; the inbox is
not a plaintext mirror of chat content.

## Client replay algorithm

Keep only a small virtualized list of session shells and a durable inbox
cursor:

1. query a bounded session-shell window;
2. page inbox events after the saved cursor until `hasMore` is false;
3. apply events only through their referenced session shell/message query;
4. subscribe after the last delivered sequence;
5. on each event, advance the cursor after applying/refetching it; and
6. on `resetRequired`, discard the stale cursor, reload bounded session shells,
   and reconnect from the returned watermark.

Do not put message history into the drawer DOM. Session lists use keyset
pagination, message lists use bounded bidirectional windows, and content blocks
load separately. The inbox lets a client know *what to refresh* without
receiving hundreds of thousands of transcript rows.

## Reauthorization and isolation

The initial principal comes from the authenticated GraphQL context. Every
durable event must match that exact principal kind and subject. Each referenced
session must still have the same owner, and current session and scope read
policy must allow access before the protected payload is opened.

Long-lived subscriptions periodically call `CurrentPrincipalResolver`. A
revoked, missing, changed-kind, or changed-subject principal terminates the
stream. Configure a non-zero reauthorization interval appropriate to the
host's revocation objective; the default is 30 seconds. This interval does not
replace transport expiry or ordinary resolver authorization.

## GraphQL-managed retention

Compose `AiConfigurationQueryRoot` and `AiConfigurationMutationRoot` to expose
redacted `aiRetentionPolicy` and CAS-bound `setAiRetentionPolicy`. The host
must authorize `ReadRetention` and `ManageRetention`; mutation additionally
requires current recent MFA and appends a redacted audit fact in the same
transaction.

`inboxEventRetentionSeconds` is bounded from 60 seconds through ten years.
`inboxMinimumEvents` is bounded from 1 through 100,000 and preserves that many
recent events regardless of age. Other retention fields remain explicit so a
single scope policy describes message, provisional-delta, raw-payload, audit,
deleted-content, provider-file, and inbox obligations. No implicit default is
used for a legacy or missing inbox policy.

Schedule `AiInboxPruningService::prune_inbox_events` through a trusted host
worker. The ORM implementation is bounded by `AiInboxPruningLimits` and:

- never opens protected payloads;
- reads the scope key captured atomically on each event;
- reads current GraphQL-managed policies in the pruning transaction;
- stops at the first non-expired, recent-floor, missing, or migration-pending
  policy boundary;
- CAS-serializes pruning with concurrent appends;
- deletes only exact contiguous event IDs;
- never rewinds or reuses `stream_head`;
- advances `minimum_retained_sequence` atomically; and
- appends a redacted policy-set-bound audit fact.

Missing policies are reported through `streams_not_ready` and delete nothing
at that boundary. CAS races are reported through `streams_conflicted` and are
safe to retry in a later bounded pass. Do not expose pruning as an ordinary
user GraphQL mutation or manually delete inbox rows.

## Restore behavior

Keep subscriptions, appends, and pruning closed while a restore is in
progress. Restore validation must verify the schema module, unique principal
sequence constraint, each stream's `1 <= minimum <= head + 1`, contiguous
retained prefixes, captured scope keys, and protection readiness before the
runtime start gate opens. A client cursor older than the restored retained
prefix receives an explicit reset; the server never fabricates missing events.
