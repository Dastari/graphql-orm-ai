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

## Recovery boundary

`protected_state` is private ORM data and is never exposed through generated
GraphQL reads. The checkpoint hash binds the run, attempt, generation, kind,
provider/model/response, budget reservation, checkpoint ID, and protected
envelope hash.

The current implementation still moves an expired non-final attempt to
`RecoveryRequired`. A checkpoint proves durable state under its original
fence; it does not permit a new attempt/generation to adopt that state. Safe
adoption requires a separate reader to validate the protected payload, original
budget and tool rows, current principal/policy, new fence, loop counts, and
provider continuation semantics before issuing a new in-memory proof. Until
that reader exists, operators must not decrypt/reconstruct a continuation or
manually requeue an active checkpoint.

Final assistant output retains its stronger same-transaction message/block
checkpoint and may be finalized by ordinary expired-lease recovery as already
documented.
