# Supervised Mutation and Approval Execution

The supervised path is deliberately separate from the read-only coordinator.
It enables a host to expose a reviewed application mutation to a model without
turning discovery, model intent, or human approval into application authority.
Only the host's ordinary GraphQL resolver can perform the domain write.

## Eligible descriptors

`AiProviderCallPlan::new_with_supervised_tools` and
`new_supervised_continuation_with_tools` accept an exact policy-enabled mix of:

- idempotent application queries at `ReadOnly` maturity/risk with approval
  `None`; and
- application mutations at `SupervisedWrite` maturity with approval `OneShot`
  and `LowRiskWrite`, `NonIdempotentWrite`, or `HighImpact` risk.

Proposal staging remains an AI-owned lifecycle, not an application mutation.
`Secret`, `AutonomousWrite`, introspection, AI control-plane, unregistered,
wrong-fingerprint, and policy-disabled definitions fail closed. These plan
constructors govern model-visible exposure only; they grant no execution
authority.

## Host preview contract

Implement `AiCanonicalActionPreviewBuilder` using trusted current application
state or a server-owned dry-run/projection service. The builder receives a
freshly resolved principal, exact registered descriptor, and server-authored
`ToolGraphqlRequest`. It returns:

- a stable server-owned action kind and concise title;
- every opaque target resource and current row version/ETag/precondition; and
- a bounded structured impact/diff value.

Do not pass model prose through as preview content. Do not fetch arbitrary
model-selected documents or targets. Preview generation is not approval and
must not perform the mutation.

## Staging and decision

`OrmAiConsequentialToolCallService::request_approval` performs this order:

1. Bind the normalized provider call to the exact session, run, attempt,
   generation, turn, and position.
2. Resolve the registered descriptor and validate its supervised contract and
   JSON Schema arguments.
3. Rehydrate current session/scope access and content-protection policy.
4. Build the exact server-authored GraphQL request.
5. Preauthorize current host tool policy and obtain its version plus safe
   authorization-state digest.
6. Build the canonical resource/version preview.
7. Protect and durably stage arguments plus provider/model/response, settled
   budget reservation, correlation/causation, and safe delegation bindings.
8. Bind and protect the complete one-shot approval envelope, park the run in
   `WaitingApproval`, append its event, and return the renewed lease.

The human reads and decides the request through the authenticated approval
GraphQL lifecycle. Recent MFA remains a server-owned per-request choice. The UI
must render the decrypted canonical preview, not model-written explanation.

## Consumption and resolver execution

After approval, call `execute_approved` with the exact waiting lease, approval
ID, tool-call ID, and a current server-selected result-egress route. The
service:

1. Loads the protected durable call and rejects missing pre-`0.10.0` restart
   bindings.
2. Verifies the exact provider-turn budget reservation is committed,
   reconciled, and bound to this session/run/attempt/generation/provider/model.
3. Rehydrates current access, opens and re-hashes the protected arguments,
   preauthorizes current host tool policy, and rebuilds the canonical preview.
4. Atomically consumes the exact unexpired one-shot approval and returns the
   run/tool call to `Running`/`executing` under a renewed fence.
5. Rehydrates and authorizes yet again inside the authenticated bridge. The
   newly computed policy version and authorization-state digest must still
   equal the consumed binding before resolver context is built.
6. Executes the server-owned operation through the normal host request-context
   factory. Resolver, row, field, resource-version, tenant, assurance,
   rate-limit, and application audit policy remain authoritative.
7. Bounds and statically validates the result, rechecks current access,
   authorizes and immutably audits the exact provider disclosure, then protects
   and fences the result/event/step.

For a different worker or process, use
`OrmAiRunService::claim_next_approved`. One state-machine transaction changes
`approved` to `resume_claimed`, moves `WaitingApproval` to `WaitingTool`,
replaces the owner/expiry/heartbeat/row-version proof, and appends a redacted
audit fact. The original attempt and generation remain unchanged because the
staged approval, provider budget, and tool rows bind them; replacing owner and
row version immediately fences the staging worker. Two concurrent workers
cannot both receive an `AiApprovedRunClaim`. The returned claim is still only
queue ownership: all steps above remain mandatory. Snapshot restore never
uses this live handoff; restored `WaitingApproval`/`WaitingTool` runs require
reconciliation even if no external effect was recorded.
Expired approved rows in a bounded handoff window are atomically expired and
audited before scanning continues, so old waits cannot permanently starve a
newer eligible handoff.

`AiRuntime::execute_tool` rejects all approval-required descriptors, so callers
cannot bypass this lifecycle through the ordinary tool entry point.

## Ambiguity and retry rules

One-shot consumption is irreversible. If resolver execution times out/fails
ambiguously, or a post-side-effect protection/authorization/persistence handoff
cannot be proven, the service terminally closes the run as
`RecoveryRequired`. The tool row and consumed approval remain audit evidence.
Never retry the mutation automatically and never reuse the approval.

An unambiguous result can still have `EgressDenied` or `EgressAuditFailed`
state. In that case the protected local result is retained but no model input
is produced. Always replace a running lease with the renewed lease returned by
the persisted outcome. A recovery-required outcome has no continuing lease.

## Remaining orchestration gate

The generic service and restart-safe approved-wait claim are suitable for a
host-owned supervised workflow, but the crate does not yet provide the
top-level coordinator that reopens the protected provider turn and resumes its
exact continuation after the mutation result. The `AiReadOnlyAgentCoordinator`
remains read-only. Deployments must not route supervised descriptors through
it, reconstruct provider state from an approval/tool row, or infer mutation
replay authority after a resumed-worker crash.
