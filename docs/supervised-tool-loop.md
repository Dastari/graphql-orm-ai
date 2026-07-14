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

Before provider execution, the trusted planner calls
`project_supervised_rule_usage` with the exact current hierarchy and cumulative
usage. The plan retains private plan-time fingerprint/maturity/approval
bindings, so a safe read cannot acquire approval semantics and a supervised
write cannot lose one-shot approval. This remains constraint evidence only;
provider egress and atomic budget proofs still run independently.

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

## Sequential top-level coordination

`OrmAiSupervisedResumeService` now owns the first protected resumption step for
one provider-retained mutation. It reopens the exact pre-wait provider
checkpoint from `AiApprovedRunClaim`, executes through the complete fresh
approval/resolver path above, and protects the result plus continuation as
`supervised_tool_batch_persisted` without a second provider call. Read-only
checkpoint adoption rejects this distinct approval-bearing kind.

`AiSupervisedAgentCoordinator` closes the bounded provider loop for sequential
provider-retained mutations. Hosts supply `AiSupervisedAgentTurnPlanner`, which
must return an `AiSupervisedAgentTurnPlan` containing only exact registered
`SupervisedWrite`/`OneShot` definitions, current hierarchical-rule evidence, a
current result-egress route, and fresh provider/egress/atomic-budget planning.
The wrapper rejects read-only, proposal, mixed, stateless, or otherwise
inexact plans.

For a normal queue claim, `execute_claimed` starts the fence, plans the first
turn, re-resolves current rules, calls the provider with periodic lease
heartbeats, accepts authoritative usage, and persists the provider checkpoint.
A tool-free turn persists protected output and completes. A tool turn must
contain exactly one mutation and a retained provider response ID. The
coordinator rechecks the mutation fingerprint/rule, verifies another provider
turn remains available, stages the server-owned preview, and returns
`WaitingApproval`. It does not heartbeat or poll during human review.

A worker passes the exact one-owner `AiApprovedRunClaim` to
`execute_approved_claim`. The resume service executes the mutation and protects
its result before the coordinator performs any provider I/O. The coordinator
then re-adopts that checkpoint under current authority, obtains a continuation
plan, validates rules, consumes the exact checkpoint once, validates rules
again, and only then crosses the provider boundary. A later provider turn may
request one new mutation, but it receives a separate preview and approval.

Provider uncertainty, checkpoint ambiguity, approval-staging ambiguity,
resolver/post-side-effect ambiguity, output ambiguity, changed rules after
provider execution, or a lost continuation fence closes as
`RecoveryRequired`. Safe pre-egress plan/rule/limit denial closes as `Failed`.
An ambiguous approved mutation outcome is returned without calling the
provider and is never replayed.

An exact complete provider-retained result can be requeued after lease loss,
reopened under current principal/rule/protection authority, and consumed once
before later transport without executing the resolver again. The coordinator
checks provider-turn capacity before consuming that evidence and refuses to
stage an approval on the final allowed turn. Multi-call, mixed read/write, and
stateless (including Ollama/local-harness) supervised adoption remain closed;
incomplete or ambiguous process loss is `RecoveryRequired`. Denied, revoked,
never-approved, and expired waits still require bounded host reconciliation.

`AiReadOnlyAgentCoordinator` remains read-only. Deployments must not route
supervised descriptors through it, reconstruct provider state from an
approval/tool row, or infer mutation replay authority after a resumed-worker
crash.
