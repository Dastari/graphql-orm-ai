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

## Operational safety

Runs use monotonically increasing fencing generations. Stale workers and late
provider callbacks cannot persist results. Restore closes the runtime until
uncertain work and security state are reconciled. All content and credentials
use the configured protection/secret boundaries; logs and auth audits remain
redacted.

The concrete worker also binds attempt ID, owner, expiry, state, and row version
on every child or terminal write. Lease expiry before `Running` is safe to
requeue; expiry after start is classified `RecoveryRequired`. Successful model
output is bound to the exact session/run/attempt/generation, reauthorized, and
protected before the message, bounded blocks, event, and renewed run fence
commit atomically.

Application-tool events are accepted only when offered from an exact current
catalog/policy snapshot and only for idempotent read-only queries with no
approval requirement. Arguments are protected before ordinary resolver
authorization; statically disclosed results require their own exact egress
decision/audit before protected persistence and continuation. Unoffered,
malformed, consequential, mutation, proposal, approval-required, and
non-idempotent calls remain fail-closed. This prevents partial orchestration
from bypassing protected history, disclosure, egress, approval, or resolver
authorization.

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
