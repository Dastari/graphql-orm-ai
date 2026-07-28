# Provider-Persistent Files

Status: design gate complete; upload, indexing, persistent-file use, and file
search remain closed.

This contract defines the evidence required before `graphql-orm-ai` may create
or use a provider-persistent file. It does not turn an attachment ID, provider
file ID, vector-store ID, egress decision, or model request into authority.
Inline OpenAI attachment input is a separate ephemeral path described in
[attachment intake](attachments.md).

## Why the runtime remains closed

OpenAI file search is a Responses hosted tool over previously uploaded Files
attached to a vector store. The documented lifecycle therefore contains at
least three externally observable effects: create a File, create a vector
store, and attach/index the File. File and vector-store creation return
provider-assigned identifiers. The reviewed API contract does not provide a
crate-verifiable deterministic idempotency binding for either create
operation. If transport fails after a create takes effect but before its
bounded acknowledgement arrives, this crate cannot recover the exact object
without listing or guessing. Listing is not permitted because it would widen
scope and still would not prove identity.

The same lifecycle has storage-time billing in addition to per-search-call
billing. The existing immutable pricing contract can reserve and reconcile a
completed file-search call, but it does not yet represent provider-file or
vector-store byte-time. Upload/indexing cannot be declared cost-complete by
ignoring those dimensions.

For those reasons:

- `ModelBuiltinTool::FileSearch` is a reserved legacy wire shape and
  `ModelRequest::validate` rejects it, even when a raw store ID is syntactically
  valid;
- the native OpenAI adapter does not upload, create a vector store, attach a
  file, retrieve content, list objects, or search a store;
- the existing native OpenAI deletion service remains available only for an
  exact `file-...` reference already selected by fenced artifact cleanup; and
- a host-created provider object cannot be adopted into search authority merely
  by writing its ID into a request or egress manifest.

This is intentional capability closure, not an instruction for a consumer to
implement the missing lifecycle around the crate.

## Capability separation

A complete implementation must expose four independently installed,
default-deny capabilities:

1. `upload`: create one provider File from an exact reopened released
   attachment;
2. `index`: create one logical search store and attach the exact uploaded File;
3. `use/search`: resolve a crate-authored logical store reference for one
   authorized provider turn; and
4. `delete`: remove store membership/store before the File and confirm exact
   absence.

Upload does not authorize index, index does not authorize a model turn, an
egress allow does not authorize reuse, and approval does not replace current
scope/session authorization. No capability may list files or stores, accept an
arbitrary provider ID, select another profile/destination, or disclose opaque
provider references to a model or GraphQL client.

## Durable identity graph

The eventual persistent graph must bind these immutable facts:

```text
current principal reference
        |
scope --+-- session -- released attachment
                         | owner
                         | detected MIME
                         | exact bytes + SHA-256
                         | acceptance-policy version
                         v
                   provider file intent
                         | provider family/profile/destination
                         | purpose + expiry + retention class
                         | upload egress/budget/audit
                         | exact provider File ID
                         v
                   provider store intent
                         | exact File membership
                         | chunking/index policy
                         | index egress/budget/audit
                         | exact vector-store ID
                         v
                crate-authored logical store reference
                         |
                 one authorized model turn
```

Provider IDs are private fields of that graph. The public/model request carries
only a crate-authored logical reference. Immediately before use, a resolver
must reload the whole graph, rehydrate the principal, recheck scope/session
access and content-protection readiness, verify current provider profile/model
capability and retention, and return an in-memory exact provider binding. The
provider request context must cover that binding with a distinct
`ProviderFile` egress allow and an atomic budget proof.

A derivative must additionally bind its exact source attachment/hash, producer
identity and version, transformation policy, protected content, MIME, bytes,
hash, and cleanup dependency. A derivative cannot silently replace its source
or inherit broader scope.

## Proposed state machines

Each external effect receives its own monotonic generation, lease, retry
counter, immutable deadline, current exact egress decision, and redacted audit.
The states are:

- file: `prepared -> uploading -> uploaded -> deleting -> deleted`;
- store: `prepared -> creating -> created -> indexing -> ready -> deleting ->
  deleted`; and
- membership: `prepared -> attaching -> indexing -> ready -> removing ->
  removed`.

Any known pre-transport failure may release its unused reservation. A timeout,
disconnect, malformed acknowledgement, or crash after transport begins closes
that effect as `recovery_required`; it is never automatically replayed.
Provider `expired` is not exact absence. Failed indexing cannot expose a store
to search and must drive dependency-ordered cleanup.

Concurrent workers claim one exact row under compare-and-swap. A stale
generation cannot publish an ID, make a store ready, use it, settle cost, or
finalize cleanup. A successful exact replay may return the already-durable
outcome only after validating the whole graph.

## Quotas, cost, and egress

Before reopening bytes, creation must atomically prove deployment and current
scope/user quotas for:

- active file and store counts;
- total source and provider-retained bytes;
- per-file bytes and supported MIME;
- indexing work and provider calls; and
- retention duration.

Upload and indexing require separate exact egress manifests whose sole source
is the released attachment or exact uploaded File intent. The manifest binds
provider family, logical profile, fixed destination, purpose, retention,
classification, byte ceiling, and current policy/consent references.

Pricing versions must remain deployment-authored. Support may open only after
the immutable quote and settlement types cover every billable lifecycle
dimension authoritatively, including byte-time where applicable. The crate
must never embed a vendor's current prices or treat an expiry promise as a
settled storage charge.

## Cleanup, retention, and restore

Deleting-session and age-based cleanup process dependencies in this order:

1. prevent new store resolution;
2. remove the exact membership or delete the exact store and confirm absence;
3. delete the exact provider File and confirm absence;
4. delete any exact local derivative blob and protected derivative;
5. tombstone and then remove the durable lifecycle rows; and
6. allow parent attachment/message cleanup to continue.

Every ambiguous provider response retains all identifiers, protected content,
and ownership bindings under bounded retry or `recovery_required`. A missing
deletion adapter keeps the path closed. Cleanup never lists a provider
collection or storage prefix.

Backup metadata must retain the logical identity, owner/scope/session,
source/hash, provider/profile, purpose/retention, state/generation, redacted
audit linkage, and whether each external object needs exact re-verification.
Provider IDs and protected content use the established redaction/protection
rules. Restore performs no provider I/O and cannot make a store ready. Startup,
workers, subscriptions, and callbacks remain closed until migrations and
restore reconciliation classify every nonterminal object as safe,
reverify-required, or recovery-required.

## Required conformance evidence

Runtime support remains unavailable until SQLite and an owned disposable
PostgreSQL test prove:

- concurrent creation does not duplicate an external effect;
- cross-owner, cross-session, cross-scope, profile, model, and destination
  swaps fail before provider I/O;
- attachment byte/MIME/hash or policy changes fail before upload and again
  before use;
- interrupted create/index effects never retry without exact idempotency
  evidence;
- only a ready exact membership can be resolved and raw provider IDs are
  rejected;
- quotas, egress, budget, byte-time, and completed-search cost cannot be
  bypassed or settled twice;
- retention expiry and deleting-session cleanup remove dependencies in order
  and require exact absence;
- stale cleanup workers cannot clear references;
- every crash window has a deterministic retry, terminal, or
  recovery-required outcome; and
- backup/restore cannot reopen use authority or replay an external call.

The current reviewed `graphql-orm` and `agql-auth` revisions provide the
generated transaction/fencing and current-principal rehydration primitives
needed to express this downstream design. No upstream change is requested at
this checkpoint. The remaining blockers are the provider create/recovery and
complete downstream cost/lifecycle contracts, so no `.handoffs/` prompt is
needed.
