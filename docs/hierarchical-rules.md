# Hierarchical AI rules

Hierarchical rules let a host narrow AI behavior at application-defined
application, tenant/project, and user layers without placing product concepts
inside `graphql-orm-ai`. A rule is constraint evidence only. It never grants a
tool, resolver, provider, disclosure, budget, credential, approval, or resource
permission.

## Resolution model

The host implements `AiRuleHierarchyResolver` and returns the complete
broadest-to-target lineage for the freshly resolved principal and exact target
`AiScope`. Scope kinds and relationships are opaque application data; the
crate does not assume a tenant model, project entity, route, or domain type.

Every returned layer must have an exact stored policy. Resolution fails closed
when a layer is missing, duplicated, malformed, unauthorized, over the bounded
depth, outside the target tenant boundary, or not terminated by the requested
target. This deliberately requires administrators to make every participating
layer explicit instead of silently treating missing policy as permission.

Resolution begins with immutable deployment limits and intersects every layer
in order. A layer can only:

- disable AI;
- lower the maximum disclosure classification or tool maturity;
- strengthen application-tool approval requirements;
- intersect exact tool-descriptor, provider-family, and provider-capability
  allowlists;
- disable provider retention or user-owned provider credentials; and
- lower step, time, token, cost, provider-call, tool-unit, or image-unit
  ceilings.

An absent allowlist or budget value means “add no narrower constraint at this
layer,” while an empty allowlist or zero budget explicitly denies that
dimension. Secret disclosure and autonomous application writes are
structurally unavailable from the GraphQL inputs.

`AiResolvedRuleSet` binds the target scope, effective intersection, and exact
row versions into a canonical fingerprint. The helper evaluations answer only
whether the rule set rejects a candidate. A positive answer must still be
combined with all ordinary current-principal, tool enablement, GraphQL resolver,
static disclosure, egress, atomic budget, provider-profile, attachment, and
one-shot approval checks. Approval rules never replace resolver authorization.

## Durable coordinator binding

`OrmAiCurrentRuleResolver` adapts a fenced lease to current rule evidence. It
rehydrates the exact principal and resolves the full hierarchy twice around
the decision. Read-only coordinator turn plans carry that unforgeable resolved
set plus a trusted server-derived BYOK classification.

Before provider egress, the coordinator checks the exact current fingerprint,
provider family, inferred request capabilities, highest manifest
classification, retention/BYOK state, every tool fingerprint at `ReadOnly`
maturity with no added approval, and remaining cumulative ceilings. It checks
again after provider return using authoritative committed usage and before
each resolver tool. A rule requiring one-shot approval therefore removes that
tool from the read-only coordinator; it does not silently bypass approval.
Any custom-tool turn requires both `CustomTools` and `ParallelToolCalls` in
the effective capability allowlist because a provider can select one
advertised definition multiple times in the same turn.

Protected coordinator checkpoint v2 stores the exact fingerprint and
`AiRuleRunUsage`: trusted start time, provider calls, provider/application-tool
steps, output tokens, cost microunits, provider/tool units, and image units.
Completed tool-batch adoption reopens that state, proves all existing durable
tool/budget/egress evidence, and re-resolves the current fingerprint. A changed
lineage, expired duration, exceeded counter, or legacy v1 checkpoint remains
closed. Atomic budget reservations and all ordinary authorization are still
separate mandatory proofs.

## GraphQL management

For SQLite and PostgreSQL, `OrmAiRulePolicyService` uses only generated
`graphql-orm` repositories and state-machine transactions. Compose
`AiRuleQueryRoot` and `AiRuleMutationRoot`, then install the same service as
`Arc<dyn AiRulePolicyService>`.

The roots are:

- `aiRulePolicy` / `AiRulePolicy`;
- `setAiRulePolicy` / `SetAiRulePolicy`.

The exact spelling follows the selected GraphQL naming feature; aliases are
not exported. Reads, management, and runtime resolution have separate
`AiRuleAction` decisions. Writes require current recent MFA, exact host access,
immutable deployment ceilings, compare-and-swap versioning, and a redacted
audit event in the same transaction.

Deployment limits are constructor-owned process configuration. GraphQL cannot
introduce a provider, capability, secret class, autonomous write, hierarchy
depth, or budget above those hard limits.

## Stored format and restore

Schema module `0.26.0` assigns strict meaning to the existing private scope
policy record. Its ID is deterministically derived from the exact scope, and a
deny-unknown-fields v1 payload plus checksum binds every constraint and the
scope identity. The generated row fields and protected JSON must agree.

Do not expose generic CRUD roots or repair the private policy JSON with
application SQL. Unknown formats, identity/checksum mismatch, duplicate stored
values, unsupported classifications/maturity, or deployment-limit widening
fail closed.

Restore snapshot producers count invalid scope-rule policies in
`invalid_rule_policy_count`. Any nonzero count emits
`AI_RESTORE_RULE_POLICY_INVALID` and keeps the runtime start gate closed until
the deployment's controlled migration or restore process reconciles the rows.
