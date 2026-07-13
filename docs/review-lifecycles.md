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
returns a renewed waiting lease. The caller must keep that lease current. If it
expires, ordinary recovery classifies the waiting attempt as
`RecoveryRequired`; the runtime does not reconstruct authority from an old
approval row.

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
  contract are not implemented yet, so mutation/proposal/approval-required
  descriptors remain excluded from the active agent loop.
- Per-item proposal review is not yet exposed; whole structured payload review
  is bounded and schema validated.
- Long-lived approval waits currently retain a fenced waiting lease and need
  heartbeat/recovery supervision. The current read-only coordinator must not
  weaken restore reconciliation or silently requeue uncertain work.
- Consumer-specific UI, domain mutations, proposal rendering, and integration
  tests remain in each consuming application.
