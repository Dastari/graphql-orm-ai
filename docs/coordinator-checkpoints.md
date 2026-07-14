# Protected Coordinator Checkpoints

The read-only coordinator durably checkpoints the two in-memory handoffs that
previously existed between an accepted provider result and the next protected
phase. This narrows recovery ambiguity without treating persistence as replay
authority.

Checkpoint format v2 also binds the exact hierarchical-rule fingerprint and
the authoritative cumulative provider-call, provider/tool-step, elapsed-time,
output-token, cost, tool-unit, and image-unit usage. The coordinator and ORM
checkpoint service must use the same current-rule resolver. Format v1 lacks
this evidence and is never eligible for adoption.

## Provider-turn checkpoint

After `AiAgentLoopGuard` accepts a normalized provider result,
`AiAgentCheckpointWriter::persist_provider_turn` runs before a tool call or
final assistant-output writer consumes it. The ORM implementation:

1. rehydrates the current principal and rechecks scope/session write access;
2. resolves the complete current hierarchical rules and a ready
   content-protection policy;
3. validates the current rule fingerprint and cumulative actual usage;
4. protects the exact normalized events, provider/model/response, authoritative
   usage, budget reservation, prior response, tool calls and arguments, loop
   counts, scope, rule evidence, correlation ID, and server-selected
   result-egress route;
5. rehydrates and resolves the rules and protection policy again after
   protection, rejecting drift;
6. transactionally verifies the current run fence and committed/reconciled
   provider budget reservation; and
7. appends the immutable checkpoint while renewing and rotating the current run
   row-version proof.

If any step fails after provider execution, the coordinator commits
`RecoveryRequired`; it does not call the provider again.

## Tool-batch checkpoint

After every exact call in a provider turn has a durable model-visible result,
the loop guard constructs its opaque continuation. Before asking the host to
plan the next provider turn, the checkpoint writer protects the provider result,
ordered tool results, their exact egress manifests, and the continuation as one
bounded payload.

The same generated-ORM state-machine transaction verifies that every referenced
tool row and run step belongs to the current run/generation and preceding
provider turn, has a protected result, has completed unambiguously, and has an
exact persisted egress decision and manifest hash. For stateless continuation,
the payload also binds the complete bounded visible conversation and one unique
manifest per historical result. Missing, reordered, duplicated, denied, or
partially executing calls fail closed. No raw SQL participates in this path.

A stateless checkpoint is consumed before the next provider transport. If the
lease is lost first, expired-run recovery may requeue only an exact completed
batch. The replacement worker must reopen the protected conversation and
validate every historical and current tool against its original committed
budget, finished step, protected arguments/result, disclosure classification,
immutable allow audit, and unique replay manifest. It never reruns an
application resolver or a preceding local-model turn.

## Exact completed-batch adoption

`protected_state` is private ORM data and is never exposed through generated
GraphQL reads. The checkpoint hash binds the run, attempt, generation, kind,
provider/model/response, budget reservation, checkpoint ID, and protected
envelope hash.

Expired recovery may requeue one provider-retained or stateless
`tool_batch_persisted` checkpoint only after its redacted hash,
committed/reconciled current-turn budget, complete protected tool rows, and
finished run steps validate under the old fence. The replacement lease retains
the immutable checkpoint ID but receives a new attempt and generation.

`AiAgentCheckpointAdopter` then rehydrates the current principal, rechecks
session/scope access, resolves the current ready protection policy, and opens
the checkpoint plus every protected argument/result. The ORM implementation
compares the original provider result, ordered calls, canonical arguments,
descriptor fingerprints, disclosure-validated model blocks, exact manifests,
immutable allow-audit records, counters, scope, rule fingerprint, cumulative
rule usage, and continuation chain. It re-resolves the complete current rule
hierarchy and rejects any fingerprint or usage mismatch. For a stateless
checkpoint it repeats those checks for every historical result and its
original budget/step rows, not just the current batch. It resolves the
principal/rules/policy again after opening before returning the opaque
`AiAdoptedReadOnlyToolBatch`.

The host planner must build a fresh continuation plan, including new budget and
egress proofs. Immediately before transport the coordinator atomically clears
the linked checkpoint through the current row-version fence. A crash before
that consume can be reconsidered within the retry ceiling; a crash after it is
conservative external-boundary recovery. The append-only checkpoint remains in
history, but no longer grants adoption eligibility.

Ordinary read-only checkpoint append/adoption additionally requires every
current and historical tool row to have `risk = read_only` and no approval ID.
This structurally prevents an approval-bearing mutation result from being
smuggled into the read-only adopter.

## Approved supervised handoff

`OrmAiSupervisedResumeService` handles the narrow same-attempt boundary after
`claim_next_approved`. It reopens the exact `provider_turn_persisted` payload,
committed provider budget, single staged mutation, `resume_claimed` approval,
result route, rule fingerprint, and cumulative usage under freshly rehydrated
authority. It then calls the consequential service, which still rebuilds the
canonical preview, consumes approval exactly once, and uses ordinary GraphQL
resolver authorization.

An exact model-visible result is immediately written as
`supervised_tool_batch_persisted`. Its state-machine append requires the one
write-risk tool row and exact consumed one-use approval and protects the
provider-retained continuation with the current rule/usage evidence. The
service never calls the provider. This first contract intentionally rejects
multi-call and stateless turns.

After lease loss, expired-run recovery may requeue this exact completed batch
under a new attempt/generation. `adopt_supervised_tool_batch` then reopens the
checkpoint and its tool/approval payloads, verifies the consumed one-shot
binding, preview/policy/auth-state evidence, committed budget, egress row,
current authority and current rules, and returns an opaque retained-response
proof. `consume_supervised_before_provider` clears the exact current link once
through the new fence before transport. Neither operation executes or retries
the mutation. Uncheckpointed, partial, multi-call, stateless, or mismatched
evidence remains `RecoveryRequired`.

Provider-turn checkpoints, incomplete batches, uncheckpointed consequential
mutations, missing/denied egress, malformed or unprovable stateless history,
retry exhaustion, and any changed current access/policy remain
`RecoveryRequired`. Operators must never manually relink a checkpoint or
reconstruct a continuation.

Final assistant output retains its stronger same-transaction message/block
checkpoint and may be finalized by ordinary expired-lease recovery as already
documented.
