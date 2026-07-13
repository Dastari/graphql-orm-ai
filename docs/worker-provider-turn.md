# Durable Worker and Provider Turn

This guide describes the implemented fenced provider-turn backend path, its
read-only application-tool branch, and the bounded top-level coordinator. None
of these contracts permits arbitrary provider calls or model-authored GraphQL.

## Required services

- `OrmAiRunService` owns bounded queue scans, claims, heartbeats, fenced state
  changes, retries, immutable attempt outcomes, and expired-lease recovery.
- `OrmAiBudgetService` atomically reserves all applicable scope/principal
  counters and retains uncertain capacity.
- `OrmAiEgressDecisionAudit` appends the exact redacted allow/deny decision ID
  and manifest hash. A failed audit write closes transport.
- `AiProviderCallExecutor` performs one security-ordered provider turn.
- `AiProviderUsageAccounting` settles the exact immutable pricing-policy
  version into authoritative cost/tool/image units after provider token usage.
- `OrmAiProviderOutputService` reauthorizes and persists a successful result as
  protected, windowable assistant-message blocks and a durable session event.
- `OrmAiApplicationToolCallService` protects and persists exact read-only tool
  calls and results around the ordinary authenticated GraphQL resolver path.
- `AiAgentLoopGuard` enforces hard provider-turn/tool-call bounds and exact
  result-to-`call_id` continuation ordering.
- `AiReadOnlyAgentCoordinator` owns turn planning, provider heartbeats, exact
  tool sequencing, protected output, and safe terminal/recovery classification
  for one claimed read-only attempt.

All ORM services use generated repository/transaction operations. None accept a
database URL or expose a driver connection.

## State and proof sequence

```text
Queued/RetryScheduled
        │ claim_next (attempt + generation + expiry)
        ▼
      Leased
        │ start
        ▼
      Running
        │ fresh access → atomic budget → egress decisions + immutable audit
        │ mark budget Uncertain immediately before provider transport
        ▼
  provider event stream
        │ authoritative completion + usage
        ▼
  budget Committed
        ├─ no application calls
        │    fresh access + current protection policy
        │    message blocks + event + run fence in one transaction
        │
        └─ exact read-only calls
             protected arguments + running step + current fence
             fresh policy + ordinary authenticated resolver
             static disclosure + exact result egress decision/audit
             protected result + event + renewed fence
             exact bounded continuation → next provider turn
        ▼
  renewed Running lease
        │ output transaction also appends exact final-output checkpoint
        │ finish
        ▼
     Completed
```

Every successful heartbeat, start, protected-output append, and other fenced
write advances the run row version. Discard the previous `AiRunLease` and use
the newly returned value. A cloned older value is expected to fail with
`AiError::Conflict`.

## Failure behavior

- Before transport, a denied or unauditable egress decision releases capacity
  only while the reservation is still provably unstarted.
- Immediately before transport, the reservation becomes `Uncertain`.
- Provider rejection, stream error, missing authoritative usage/completion,
  unknown pricing version, settlement failure, oversized output, unoffered or
  malformed application-tool events, or worker loss does not optimistically
  release that capacity.
- An expired `Leased` claim is safe to requeue because provider orchestration
  has not started. An expired `Running` attempt is finalized only when its
  linked same-transaction checkpoint, hash, attempt/generation, settled budget
  reference, and complete protected assistant message all match. Every other
  running/waiting claim becomes `RecoveryRequired` and is never replayed.
- Recovery writes append an immutable attempt-outcome fact and invalidate the
  old worker fence.

## Current bounded-output behavior

The provider executor retains only a deployment-bounded normalized event list.
The output service extracts visible text, visible reasoning summaries,
citations, and redacted built-in results. It splits text on UTF-8 boundaries
into separately fetched blocks, keeps the message preview bounded, applies the
current scope content-protection policy to every stored value, and emits one
completed-message session event.

`AiLiveDeltaCoalescer` now provides UTF-8-safe time/byte batching no weaker than
the 50 ms / 4 KiB contract and accepts only visible text or visible reasoning
summary events. It is synchronous proof-oriented state, not a durable sink or
disclosure decision. Clients continue to consume durable cursor windows while
protected fenced delta-event persistence remains a later slice.

## Implemented read-only tool boundary

`AiProviderCallPlan::new` remains the ordinary tool-free constructor.
`new_with_tools` accepts only exact catalog/policy-matched, idempotent
application queries at read-only maturity/risk with no approval requirement.
The provider executor bounds and schema-validates every offered call. The ORM
tool service then persists protected arguments before execution, rehydrates
current authority, invokes the exact registered GraphQL request, applies the
static disclosure contract, creates and immutably audits a separate exact
tool-result egress decision, persists the protected outcome, and renews the
fence. `AiAgentLoopGuard` binds each result to its opaque provider `call_id`
before `new_continuation_with_tools` can construct the next request.

The coordinator is not yet restart-adoptable across a partial batch. A lost
guard, partial batch, unknown provider response, expired fence without the
exact final-output checkpoint, or restore state remains closed for
reconciliation instead of being reconstructed. Consequential, mutation,
proposal, approval-required, and non-idempotent descriptors remain rejected
until canonical preview, one-shot approval persistence, post-approval fresh
authorization, and recovery are owned by the coordinator. Callers must never
simulate either path with a model-authored document, operation name, URL,
shell command, or direct repository request. See
[Read-Only Application-Tool Loop](read-only-tool-loop.md).
