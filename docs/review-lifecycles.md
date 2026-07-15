# Proposal and Approval Lifecycles

Proposals and approvals solve different problems and are never interchangeable:

- a proposal is protected AI-owned staging data that a human may accept, edit,
  or reject; acceptance does not mutate application data; and
- an approval records human intent for one exact consequential action;
  consumption does not grant resolver authority or prove that the action ran.

Both lifecycles are project-agnostic. Applications register proposal schemas,
build canonical previews, decide access policy, and perform final domain work
through their ordinary authenticated GraphQL resolvers.

## Proposal sequence

`AiProposalCatalog` validates the exact registered JSON Schema version,
serialized size, logical item count, and required redacted provenance before a
proposal can become `ValidatedAiProposal`. `OrmAiProposalService` then:

1. rehydrates the run's safe principal reference and checks freshness;
2. authorizes `Create` for the exact session/scope through the host proposal
   policy;
3. resolves current content protection and protects the structured payload;
4. revalidates the current run/session/owner/tenant/fence in one state-machine
   transaction;
5. inserts the pending proposal, appends a protected session event, advances
   the session stream, and renews the run fence atomically.

Use the returned lease; the previous row-version proof is stale.

`AiProposalQueryRoot` returns bounded keyset windows and one authorized,
decrypted proposal. `AiProposalMutationRoot.reviewAiProposal` is CAS-bound:

- `Accept` preserves the exact validated payload;
- `AcceptEdited` requires a replacement payload and logical item count and
  revalidates them against the current exact registered schema version; and
- `Reject` records no replacement.

All outcomes update only AI-owned proposal/session-event rows. They cannot call
an application mutation.

After a human uses the accepted suggestion in the application's normal
workflow, the trusted integration calls
`AiProposalOutcomeRecorder::record_applied_outcome` with the current principal
and authoritative application audit/resource references. The service freshly
rehydrates and authorizes that linkage. It never performs or retries the domain
mutation. An exact repeated link is idempotent; a conflicting link is rejected.

After a session enters `deleting` and reaches its exact current content-purge
cutoff, proposal reads/reviews/outcome links remain inaccessible. The bounded
session-retention worker may later clear protected parent/item content only for
rejected, applied, expired, or expired pending-review proposals whose owning
run is terminal. It preserves non-content identity, schema, review, outcome,
application-audit, timestamp, and CAS metadata. Accepted or accepted-edited
proposals remain blocked because a domain mutation/outcome boundary may be
unresolved; operators must reconcile that state rather than treating deletion
as proof that an application effect did or did not occur.

After proposal content is exhausted, deleting-session retention separately
proves the complete bounded terminal run/tool/approval graph. Only exact
finished tool steps and state-compatible terminal one-shot approvals are
eligible. It clears the approval's protected resource bindings and canonical
preview plus the tool's protected arguments/result, while preserving hashes,
states, decision/use timestamps, authorization/egress evidence, and audit
references. Pending, approved, resume-claimed, recovery-required, malformed,
or over-bound authority stays intact and blocks later attachment/message and
checkpoint cleanup. Tombstoned terminal approvals cannot be read, consumed, or
reconstructed as fresh authority.

The same terminal graph can become eligible earlier under the current scope's
`raw_payload_retention_seconds` cutoff. This age-based path selects only
expired completed calls on terminal runs; it leaves newer calls and every live
wait or run intact, even when it safely tombstones an older terminal subset in
the same session. Protected coordinator state remains a separate dependency.
Retained hashes and decision/use facts prove what was authorized but cannot
reconstruct the removed preview, resources, arguments, or result.

After those exact tool tombstones exist, an independently expired orphaned
protected coordinator checkpoint may be physically deleted only through the
database-enforced append-only maintenance transaction. The terminal run,
closed attempt outcome, committed budget, absent current pointer, and complete
correlated tool/approval set are re-proved without reading checkpoint content.
A current or ambiguous approval/recovery checkpoint remains intact.

## Approval request

The host builds `AiCanonicalActionPreview` from current server-owned policy and
resource state. Model-authored prose is not a preview. The matching
`AiApprovalBinding` includes:

- exact tool call, canonical argument hash, descriptor fingerprint, and
  session/scope;
- logical GraphQL target plus schema, operation document, projection, and
  disclosure fingerprints;
- safe principal/delegation identity;
- current policy and authorization-state digests;
- every target resource and expected version; and
- the canonical preview hash.

`OrmAiApprovalService::request_approval` validates and protects that envelope,
then atomically binds the approval to the existing consequential tool call,
parks the current run in `WaitingApproval`, appends a protected event, and
returns a renewed waiting lease. The staging worker must stop after parking; it
does not heartbeat through a human wait. A different process can use
`OrmAiRunService::claim_next_approved` after approval. That atomic handoff
changes the approval to `resume_claimed`, moves the run to `WaitingTool`, and
rotates owner/row-version fencing without changing the action-bound
attempt/generation. It grants no consumption, resolver, rule, or egress
authority. An unconsumed resumed lease that expires is conservatively
`RecoveryRequired`; the runtime never reconstructs mutation replay authority.

## Human decision

Compose `AiApprovalQueryRoot` and `AiApprovalMutationRoot` only with an
installed `AiApprovalService`. Reads, decisions, and revocation all rehydrate
the request principal and reapply host scope/session policy. Decisions require
the exact displayed row version and unexpired `Pending` state. When the durable
request says recent MFA is required, the configured `agql-auth`
`RecentMfaPolicy` must accept the freshly resolved user.

The UI renders only the decrypted server-generated canonical preview. It must
not use model prose as the authoritative action description. Approval and
revocation append protected durable session events.

## Live wait reconciliation

Run `OrmAiApprovalWaitReconciliationService::reconcile_waits` in a bounded
live-runtime worker before generic expired-lease recovery. For each
`WaitingApproval` candidate it rehydrates the durable principal, resolves the
current content-protection policy, and requires an exact current provider-turn
checkpoint, committed one-run budget reservation, unique staged tool call,
running step, principal fingerprint, and one-shot approval binding.

Valid pending or approved rows are also evaluated by the deployment's current
`AiApprovalWaitReconciliationPolicy`. A continue decision leaves every row
unchanged. Denial, revocation, approval expiry, deployment wait cutoff,
deleted-session state, or policy cancellation atomically expires authority
when needed, terminally closes the call/step and run fence, and appends one
protected event, redacted audit, and immutable attempt outcome. A malformed or
unprovable linkage moves only the run to `RecoveryRequired`; potentially
unrelated approval and call rows remain untouched for operator evidence.

This worker never heartbeats or polls a human wait, infers approval, calls
`claim_next_approved`, consumes authority, executes a resolver, or contacts a
provider. Snapshot restore remains a separate closed-runtime path: restored
`WaitingApproval` and `WaitingTool` runs are recovery-only and cannot enter the
live reconciler.

## Exact one-shot consumption

Immediately before a consequential resolver call, server-owned code rebuilds
the complete binding and preview from current policy/resource state and calls
`consume_approval` with the exact waiting lease. The service:

1. validates the rebuilt preview/resources and full binding;
2. freshly rehydrates the original actor from the run reference;
3. reauthorizes the host `Consume` policy;
4. compares every durable operation, actor, policy, resource, preview, and
   protected envelope binding and re-resolves the current registered
   supervised-mutation descriptor/GraphQL contract;
5. validates approved/unexpired/unused state; and
6. atomically changes the approval to `Consumed`, changes the tool call back to
   executing, returns the run to `Running`, appends a protected event, and
   renews the fence.

`ConsumedAiApproval` proves only that this exact intent was consumed once. The
consequential executor must then immediately rehydrate again through the
ordinary tool bridge and execute the exact registered GraphQL resolver. Row,
field, tenant, rate-limit, assurance, resource-version, and domain policy remain
authoritative. If resolver execution fails after consumption, the approval is
not reusable.

## Deliberate remaining gates

- The generic consequential tool executor and host canonical-preview builder
  contract are implemented. One claimed provider-retained mutation can now
  reopen its exact pre-wait checkpoint and persist an approval-bound
  post-mutation continuation. `AiSupervisedAgentCoordinator` consumes that
  checkpoint and resumes a bounded sequential provider loop under fresh rules,
  fencing, egress, budget, and current-principal checks. Mutation/proposal/
  approval-required descriptors remain excluded from the read-only
  coordinator, while mixed, parallel, and stateless supervised batches remain
  closed.
- Per-item proposal review is not yet exposed; whole structured payload review
  is bounded and schema validated.
- The staging worker no longer heartbeats through a human wait; approved work
  has a one-owner same-attempt handoff and other decisions are handled by the
  bounded live reconciler. Exact multi-call/stateless provider continuation is
  not yet supported. Exact completed provider-retained mutation results can be
  adopted across generations;
  incomplete or ambiguous effects remain recovery-only.
- Consumer-specific UI, domain mutations, proposal rendering, and integration
  tests remain in each consuming application.
