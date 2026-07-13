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

Logical remote GraphQL targets are deployment-registered. Models cannot choose
URLs, audiences, resources, direct-service routes, or credentials. Recursive
AI control-plane and introspection tools are rejected.

## Operational safety

Runs use monotonically increasing fencing generations. Stale workers and late
provider callbacks cannot persist results. Restore closes the runtime until
uncertain work and security state are reconciled. All content and credentials
use the configured protection/secret boundaries; logs and auth audits remain
redacted.
