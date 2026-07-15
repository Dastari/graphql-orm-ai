# Protected Context Compaction

Context checkpoints are trusted-backend storage for bounded, untrusted model
summaries. They reduce the verbatim history sent to later model turns without
turning a summary, provider response ID, or source hash into authorization.

`OrmAiContextCompactionService` is available for SQLite and PostgreSQL write
backends. MSSQL remains schema-only. The service uses generated ORM operations
only and never accepts a database URL or raw SQL.

## Security boundary

Preparation requires a current running `AiRunLease`. It renews the complete
run fence, rehydrates the durable principal reference, rechecks session and
scope write access, verifies owner/tenant binding, and resolves a ready content-
protection policy before opening any source.

One preparation covers a contiguous session prefix segment:

- the first checkpoint starts at message sequence 1;
- a later checkpoint starts immediately after the latest valid checkpoint;
- every message must be complete, finalized, unpurged, and have its exact
  ordered block count;
- the requested boundary must leave the configured recent-message tail
  verbatim; and
- message, block, checkpoint, source-byte, summary-byte, token, and principal-
  age limits apply independently.

The prepared value contains plaintext and is intentionally not serializable.
Its `Debug` output contains only redacted identities and counts. Do not place it
in logs, queues, frontend state, generic caches, or model-authored tool data.

## Provider call

Use `AiPreparedContextCompaction::model_request()` as the exact request for an
ordinary `AiProviderCallPlan`. The plan must contain exactly one model-
inference manifest for the returned session, run, scope, provider, and model.
Set its purpose to `context_compaction`, copy `egress_sources()` exactly, and
use byte/token estimates at least as large as the prepared estimates.

The source list is intentionally conservative. Message blocks and parent
summaries are `Restricted`; user blocks are `UserProvided`, while assistant
blocks and parent summaries are `ExternalUntrusted`. A deployment may deny
that manifest or require a local provider. The compaction API cannot lower the
classification.

Execute the plan only through `AiProviderCallExecutor`. That existing boundary
provides fresh principal reauthorization, exact egress policy and durable
audit, atomic budget reservation, pre-transport uncertainty, bounded provider
normalization, immutable pricing settlement, and committed usage. The
compaction service does not provide a second transport or budget path.

The request exposes no application tools or provider built-ins. Its trusted
instruction asks for plain visible summary text and tells the model to treat
all source content as untrusted data. Summary output remains untrusted and can
never grant resolver, tool, approval, egress, continuation, or replay
authority.

## Exact persistence

Pass the prepared value and the executor's private `AiProviderCallResult` to
`persist`. The service requires:

- exact session, run, attempt, generation, provider, model, and request;
- the exact sorted source set in a `context_compaction` model-inference
  manifest;
- committed positive provider usage;
- visible text only, with no tool, built-in, reasoning, citation, or unknown
  event;
- a nonempty summary inside the configured byte bound; and
- fresh principal, access, and protection policy.

Before insertion, one state-machine transaction revalidates the running lease,
active session, latest parent, checkpoint lookahead bound, every message row,
and every ordered block against the prepared snapshot. A retention pass,
message change, competing checkpoint, lease change, or parent deletion causes
a conflict and no summary is stored.

The protected payload contains the summary, a domain-separated SHA-256 hash of
the exact parent-plus-message prompt, direct message/block provenance, optional
parent checkpoint/hash, and run/attempt/generation/budget evidence. The record
also stores provider/model and authoritative output-token metadata. Carry the
returned renewed lease into the next operation.

`load_latest` renews and reauthorizes again, validates the protected payload
against its row and exact prefix lineage, and opens only the latest valid
summary. Keep later recent messages verbatim after that checkpoint when
assembling model context.

## Retention and restore

Ordinary message retention cannot leave a summary containing removed content.
In the same transaction, it loads the complete bounded checkpoint set with
one-row lookahead and physically deletes every checkpoint whose prefix could
cover the eligible message. Only then does it clear the preview and delete
message blocks. An over-bound set blocks the message with no partial
invalidation. The report field `context_checkpoints_invalidated` is the exact
row count removed this way.

Deleting-session retention keeps the stronger dependency order: it deletes
bounded context-checkpoint pages before any later pass may scrub messages.

Restore collectors must populate `invalid_context_checkpoint_count`. They must
validate prefix/parent ordering, source hashes, associated protection identity,
direct provenance, provider/model and positive token metadata, durable
run/attempt/generation and committed-budget evidence, and retention state. A
nonzero count is fatal. Legacy or application-authored checkpoint payloads are
not accepted by the reader and must be reviewed and removed or rebuilt while
the runtime remains closed.
