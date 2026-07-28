# Coordination and Review Expansion Gates

Status: Slice 5 design/classification complete; no new runtime path is opened
while Slice 4 applied restore remains blocked.

This matrix applies the [canonical ordering and history proof](ordering-history.md)
to the requested coordination sequence. “Provider parallel-tool support” means
only that an adapter can normalize an ordered batch. The application
coordinator remains sequential unless a separate durable execution proof says
otherwise.

## Runtime matrix

| Shape | Status | Required behavior |
|---|---|---|
| Completed read-only batch, provider-retained | Implemented | Revalidate full batch/current authority, consume once, then transport |
| Completed bounded stateless read-only history | Implemented for supported provider families | Revalidate every historical budget/step/result/egress fact |
| Single supervised mutation per retained provider turn | Implemented | Fresh preview/approval/resolver authority for every turn |
| Multiple sequential supervised turns | Implemented within loop bounds | One distinct provider checkpoint, preview, approval, mutation result, and continuation checkpoint per turn |
| Provider-turn-only adoption | Closed | Cannot prove whether a later resolver began |
| Partial read-only batch recovery | Closed | Missing call position has no unambiguous completion certificate |
| Mixed read/write batch | Closed | Read observations, one-shot approvals, and writes cannot share an incomplete batch proof |
| Parallel read-only resolver execution | Closed | Current durable fence and result rows establish sequential, not concurrent, completion |
| Parallel consequential execution | Permanently unsupported generically | No portable order for approvals, versions, effects, or ambiguity |
| Stateless supervised continuation | Closed | Provider-family reasoning/history plus consumed approval evidence is incomplete |
| Replay of unknown provider/application effect | Permanently unsupported | Requires authoritative external reconciliation, never retry |

The implemented supervised coordinator already supports repeated sequential
provider-retained mutations across top-level turns. Each later mutation gets a
new provider result, provider-turn checkpoint, canonical preview, human
approval, fresh GraphQL authorization, protected result, egress decision,
supervised batch checkpoint, and provider budget. Nothing is batch-approved.

## Mixed and partial work

A future mixed turn would have to split the provider's order into durable
single-position sub-batches:

1. complete and checkpoint each read;
2. rebuild the next write preview from current application state;
3. prove continuation/provider capacity before requesting approval;
4. consume one approval and execute one mutation;
5. checkpoint its exact result before considering the next position; and
6. make every remaining position invalid if an earlier result changes the
   resource/policy fingerprint on which it depended.

That protocol is not represented by the current provider-turn checkpoint,
which describes one complete normalized call list. Treating a prefix as a
complete batch would change provider history. Consequently, mixed turns remain
closed instead of being silently serialized.

Partial read-only adoption also remains closed. “Read-only” and “idempotent”
are descriptor constraints, not proof that repeating a query has no externally
observable audit, rate-limit, cache, or time-varying result. The current
sequential executor can safely finish a live batch under one fence, but process
loss before the complete checkpoint requires recovery review.

## Proposal review

Whole-payload structured proposal review and post-domain-mutation outcome
linkage are implemented. Generic per-item review remains gated. A safe item
contract must:

- bind each item to the immutable proposal schema/version and protected source;
- preserve stable server-authored item order and item identity;
- allow accept/edit/reject without mutating another item's protected value;
- revalidate edits against the proposal schema and item-specific bounds;
- prevent a partially reviewed set from appearing accepted;
- atomically derive one final reviewed payload and its checksum only after
  every required item is terminal;
- keep final application rendering/mutation consumer-owned; and
- retain restore/retention semantics for pending, rejected, superseded, and
  applied item graphs.

No per-item runtime is added before Slice 4 can back up, restore, and reconcile
that expanded persistent state.

## Preconditions for any future opening

Every newly admitted shape must pass:

- the complete crash-window and negative-test matrix in the ordering proof;
- SQLite and owned disposable PostgreSQL concurrency parity;
- applied backup/object restore and post-restore reconciliation;
- current-principal, current-rule, resolver, egress, budget, protection, and
  retention revalidation at every stated phase;
- exact checkpoint/approval one-shot consumption; and
- documentation that names unsupported provider families and histories.

No upstream ORM/auth primitive is required for this classification. The only
open prerequisite handoff is the backup-crate dependency alignment recorded in
[recovery and restore](recovery-and-restore.md).
