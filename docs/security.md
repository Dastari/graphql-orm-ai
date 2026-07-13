# Security Model

The runtime treats model output, resolver output, files, web results, provider
built-ins, and remote MCP data as untrusted input.

## Authority

- Resolver discovery is descriptive; tool registration and policy enablement
  are separate default-deny steps.
- After principal rehydration, a required host tool policy evaluates the exact
  scope, descriptor fingerprint, and schema-validated arguments on every call.
  A catalog entry or stale decision object is not execution authority.
- Every application operation uses a freshly rehydrated user or bounded
  delegation through the host's ordinary GraphQL authorization path.
- AI execution preserves the human actor and records the run/tool as mechanism.
- Approval is intent confirmation, not authorization. Resolver, row, field,
  rate-limit, assurance, and resource-version checks run again after approval.

## Disclosure

Read permission does not imply permission to disclose data externally. Every
provider, built-in, attachment, web, image, code, MCP, and remote model transfer
requires an exact egress manifest and decision.

Application tool output must conform to a server-owned static disclosure
schema. Unknown fields and `NeverExport` nodes fail closed. Runtime
classification may only raise classification or remove/redact fields. Secret
material is never model-facing, even when a deployment classification ceiling
is configured broadly. Serialized byte and registered list/record limits are
checked before a resolver result is returned to orchestration.

## External execution and spend

Provider calls require both an exact egress proof and an atomic budget
reservation proof. Reservations bind the run, attempt, fencing generation,
provider, model, output ceiling, pricing version, and expiry. Uncertain external
calls retain capacity until reconciliation.

Provider token observations are not silently treated as complete cost facts.
A deployment-owned `AiProviderUsageAccounting` implementation must resolve the
exact pricing-policy version and authoritatively settle cost/tool/image units.
Unknown pricing or arithmetic failure occurs after transport and therefore
leaves the reservation uncertain.

The concrete ORM budget service checks every applicable policy before writing
any counter, serializes competing starts, and accepts a reservation only while
the resolved principal and run lease remain current. Actual usage may exceed an
estimate and is still committed truthfully; this can exhaust a policy but must
not be hidden. Only proven unused capacity is released. Ordinary workers cannot
release a reservation already classified as uncertain.

Both allowed and denied provider transfers are appended to the immutable ORM
egress audit using the exact decision ID and manifest hash. The transport proof
is not used when that write fails. If no transport occurred, capacity may be
released while the reservation remains `Reserved`; after it is durably marked
`Uncertain`, stream/provider failures retain capacity for authoritative
reconciliation.

Logical remote GraphQL targets are deployment-registered. Models cannot choose
URLs, audiences, resources, direct-service routes, or credentials. Recursive
AI control-plane and introspection tools are rejected.

The remote adapter accepts only private routed/direct registrations and the
complete server-authored request. It rejects stale or expired principals before
issuance, bounds delegated expiry by both configured lifetime and principal
freshness/expiry, and binds the target, audience/resource, actor, scope, operation,
canonical variables, projection/disclosure fingerprints, run/tool IDs, audit
chain, delegation reference, and hashed idempotency key. The opaque credential
is non-serializable, non-cloneable, redacted from `Debug`, and rejected after
expiry or if the request/context changes. The deployment issuer must make its
real claims no broader than the redacted request, and the private transport
must enforce destination allowlisting and routed/direct authorization parity;
these external properties cannot be proven by the crate's Rust type alone.

## Operational safety

Runs use monotonically increasing fencing generations. Stale workers and late
provider callbacks cannot persist results. Restore closes the runtime until
uncertain work and security state are reconciled. All content and credentials
use the configured protection/secret boundaries; logs and auth audits remain
redacted.

The concrete worker also binds attempt ID, owner, expiry, state, and row version
on every child or terminal write. Lease expiry before `Running` is safe to
requeue. After start, an exact linked final-output checkpoint may be finalized,
and an exact completed read-only tool batch may be requeued for one current-
authority adoption. Recovery verifies hashes, original attempt/generation,
settled budget, and the corresponding complete durable rows. Successful model
output, bounded blocks, event, checkpoint, and renewed run fence commit
atomically.

The read-only coordinator heartbeats the current fence while provider
transport is pending and stops immediately when that proof is lost. Provider,
resolver, or output ambiguity is classified for recovery rather than replayed.
After a provider result is accepted, its settled budget, normalized content,
tool calls, scope/route, and loop state are protected and checkpointed before
tool/output consumption. A complete tool batch is checkpointed only after one
transaction verifies every protected result and exact egress audit. These
checkpoints remain bound to the original attempt/generation. A replacement
worker can adopt only a complete tool batch after reopening every protected
value and validating the original durable evidence under current access. It
must atomically consume that link before provider transport. Provider-turn and
partial-batch work still becomes `RecoveryRequired`.
Live-delta batches contain sensitive plaintext inside the trusted process;
coalescing supplies only UTF-8/time/byte bounds and never authorizes delivery.
The optional ORM sink rehydrates authority, resolves and rechecks protection
policy, and commits a protected cursor event only after validating the exact
active fence and uncertain budget. Tool arguments, raw frames, structured
events, and hidden reasoning never enter this path. Sink failure after
transport remains uncertain and cannot trigger replay. A committed delta is
provisional history, not proof of a completed assistant message; see the
[live-streaming guide](live-streaming.md).

Attachment upload requires both the current authenticated owner and an
expiring one-use high-entropy token stored only as a hash. Filenames never form
storage paths. Exact size/hash, complete-object scanner attestation, a separate
host acceptance policy, quarantine promotion, and explicit release all precede
message linkage. Scanner/policy/storage ambiguity remains closed; a released
attachment still has no provider-egress authority. Provider input additionally
requires a canonical user-provided source proof bound to the exact attachment
ID, MIME type, byte count, SHA-256 checksum, provider, model, and capability;
swapping any of those values invalidates the egress decision. Exact bytes are
reopened only after budget and audited egress authorization under a freshly
resolved principal. The ORM resolver checks current owner/session/scope and
released/clean/linked state, validates the object, and detects row changes
around storage I/O. The executor rehydrates and reauthorizes once more before
transport. Resolved bytes are redacted from `Debug`, are not persisted or
cached by the runtime, and require complete exact coverage in the provider
context. See the [attachment guide](attachments.md).

Attachment maintenance is a host-only delete capability over exact opaque
references already selected by durable lifecycle state. Each row is freshly
CAS-claimed with a generation and expiring lease. Storage absence must be
confirmed before references are cleared; ambiguity is audited and backed off,
and a reclaimed generation fences every stale finalizer. Cleanup never lists
storage prefixes, reads content, or bypasses owner-facing resolver policy.

The read-only coordinator accepts only exact enabled idempotent queries with no
approval requirement. The separate supervised service accepts only exact
enabled application mutations at `SupervisedWrite` maturity with one-shot
approval and an allowed non-secret consequential risk. Arguments are protected
before approval; provider/model/budget/audit bindings are durable; canonical
resource versions are rebuilt before consumption; and current tool policy is
recomputed and compared again before ordinary resolver execution. Statically
disclosed results still require exact egress audit before continuation.
Unoffered, malformed, autonomous, secret, AI-control-plane, introspection, and
wrong-maturity calls remain fail-closed.

Proposals never carry application write authority. Review mutates only
protected AI-owned staging state, and applied-outcome linkage happens only
after an ordinary domain mutation commits. The linkage requires a freshly
resolved principal and authoritative application audit reference.

Approval previews are server-generated and protected; model prose is never an
approval description. Human decisions are CAS-bound, optionally recent-MFA-
gated, and separately authorized. Consumption rehydrates the original actor,
revalidates the entire operation/policy/resource/preview envelope, changes the
grant to `Consumed` atomically, and advances the exact run fence. The resulting
proof grants no resolver authority, cannot be reused, and does not hide a
failed application mutation. Fresh ordinary resolver and resource-version
authorization must immediately follow.

`AiRuntime::execute_tool` structurally rejects approval-required descriptors.
Only `execute_approved_tool` accepts a consumed exact proof, and it checks the
new current policy version/state before constructing resolver context. After
consumption, timeouts and any uncertain resolver or post-side-effect handoff
terminally classify the run `RecoveryRequired`; workers must never replay the
mutation or reuse the approval.
