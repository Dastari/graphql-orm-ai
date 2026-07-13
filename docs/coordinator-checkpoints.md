# Protected Coordinator Checkpoints

The read-only coordinator durably checkpoints the two in-memory handoffs that
previously existed between an accepted provider result and the next protected
phase. This narrows recovery ambiguity without treating persistence as replay
authority.

## Provider-turn checkpoint

After `AiAgentLoopGuard` accepts a normalized provider result,
`AiAgentCheckpointWriter::persist_provider_turn` runs before a tool call or
final assistant-output writer consumes it. The ORM implementation:

1. rehydrates the current principal and rechecks scope/session write access;
2. resolves a ready content-protection policy;
3. protects the exact normalized events, provider/model/response, authoritative
   usage, budget reservation, prior response, tool calls and arguments, loop
   counts, scope, correlation ID, and server-selected result-egress route;
4. rehydrates and resolves the policy again after protection, rejecting drift;
5. transactionally verifies the current run fence and committed/reconciled
   provider budget reservation; and
6. appends the immutable checkpoint while renewing and rotating the current run
   row-version proof.

If any step fails after provider execution, the coordinator commits
`RecoveryRequired`; it does not call the provider again.

## Tool-batch checkpoint

After every exact call in a provider turn has a durable model-visible result,
the loop guard constructs its opaque continuation. Before asking the host to
plan the next provider turn, the checkpoint writer protects the provider result,
ordered tool results, their exact egress manifests, and the continuation as one
bounded payload.

The same state-machine transaction verifies that every referenced tool row and
run step belongs to the current run/generation and preceding provider response,
has a protected result, has completed unambiguously, and has an exact persisted
egress decision and manifest hash. Missing, reordered, duplicated, denied, or
partially executing calls fail closed.

## Exact completed-batch adoption

`protected_state` is private ORM data and is never exposed through generated
GraphQL reads. The checkpoint hash binds the run, attempt, generation, kind,
provider/model/response, budget reservation, checkpoint ID, and protected
envelope hash.

Expired recovery may requeue one `tool_batch_persisted` checkpoint only after
its redacted hash, committed/reconciled budget, complete protected tool rows,
and finished run steps validate under the old fence. The replacement lease
retains the immutable checkpoint ID but receives a new attempt and generation.

`AiAgentCheckpointAdopter` then rehydrates the current principal, rechecks
session/scope access, resolves the current ready protection policy, and opens
the checkpoint plus every protected argument/result. The ORM implementation
compares the original provider result, ordered calls, canonical arguments,
descriptor fingerprints, disclosure-validated model blocks, exact manifests,
immutable allow-audit records, counters, scope, and response chain. It resolves
the principal/policy again after opening before returning the opaque
`AiAdoptedReadOnlyToolBatch`.

The host planner must build a fresh continuation plan, including new budget and
egress proofs. Immediately before transport the coordinator atomically clears
the linked checkpoint through the current row-version fence. A crash before
that consume can be reconsidered within the retry ceiling; a crash after it is
conservative external-boundary recovery. The append-only checkpoint remains in
history, but no longer grants adoption eligibility.

Provider-turn checkpoints, incomplete batches, consequential mutations,
missing/denied egress, malformed protected state, retry exhaustion, and any
changed current access/policy remain `RecoveryRequired`. Operators must never
manually relink a checkpoint or reconstruct a continuation.

Final assistant output retains its stronger same-transaction message/block
checkpoint and may be finalized by ordinary expired-lease recovery as already
documented.
