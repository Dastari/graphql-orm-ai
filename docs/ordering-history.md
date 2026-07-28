# Canonical Ordering and History Proof

Status: Slice 3 design complete. This proof classifies existing runtime paths;
it does not enable a new execution shape.

The purpose of canonical ordering is to make every provider, resolver,
approval, budget, egress, checkpoint, and continuation effect attributable to
one position in one fenced run history. Provider support for parallel calls is
only an input-shape capability. It is not permission to execute application
effects concurrently.

## Canonical coordinates

Every effect is ordered by this tuple:

```text
(session_id,
 run_id,
 attempt lineage,
 lease_generation,
 provider_turn_index,
 tool_call_index,
 phase_index)
```

`attempt lineage` consists of the original attempt plus any explicitly adopted
replacement attempt. A replacement generation does not renumber an adopted
provider turn or tool call. `phase_index` has this fixed order:

1. current principal/rules/access and plan validation;
2. atomic provider budget reservation;
3. exact provider/built-in/attachment egress decisions;
4. mark provider capacity uncertain;
5. provider transport and normalized terminal observation;
6. authoritative usage/budget settlement;
7. protected provider-turn checkpoint;
8. for each call position, current rules/access and durable argument/step;
9. for that position, application resolver or approval lifecycle;
10. static result disclosure, result egress decision, and protected result;
11. complete ordered tool-batch checkpoint;
12. fresh continuation plan and capacity proof;
13. one-shot checkpoint consumption; and
14. the next provider transport or final protected output transaction.

Within one provider turn, tool-call positions are the exact normalized provider
order. Call IDs must be unique, but lexical call-ID order never changes
position. Results are reconstructed in call position order even if their
durable rows are queried in another order.

The existing read-only coordinator executes positions strictly sequentially
and carries the renewed row-version fence from one position to the next.
Adapters may normalize a provider response containing multiple calls, but no
application resolver is run concurrently. This is the canonical behavior.

## State transitions

```text
Running
  |
  | provider budget + egress + transport
  v
provider result observed
  |
  | protected provider_turn_persisted
  +-------------------------------+
  | no calls                      | exact ordered calls
  v                               v
protected final output       call[0] -> result[0]
  |                               |
  v                               v
Completed                    call[1] -> result[1] -> ...
                                  |
                                  v
                         complete batch checkpoint
                                  |
                         fresh plan + capacity proof
                                  |
                         consume checkpoint once
                                  |
                                  v
                           next provider turn
```

A supervised single mutation inserts `WaitingApproval` after the provider-turn
checkpoint. Approval decision, one-shot consumption, fresh resolver
authorization, mutation, protected result, and
`supervised_tool_batch_persisted` remain one sequential position. The next
provider call occurs only after that completed checkpoint is re-adopted,
capacity-checked, and consumed once.

An expected denial before any external effect is `Failed` or `Cancelled`
according to the owning lifecycle. An effect that may have crossed an external
boundary without a complete durable result is `RecoveryRequired`.

## Capacity before irreversible steps

The following proofs must exist before a checkpoint is consumed or a one-shot
approval is consumed:

- another provider turn fits the deployment loop ceiling;
- cumulative provider/tool-step/time/token/cost/tool/image usage fits current
  hierarchical rules;
- the fresh continuation plan has exact provider/model/capability bindings;
- a new atomic budget reservation and every exact egress allow can be obtained;
- the current principal, scope/session access, rule fingerprint, protection
  policy, retention, and provider profile remain valid; and
- the exact complete checkpoint is still linked by the current fence.

For supervised work, preview construction and current tool-policy
preauthorization precede the human decision. Immediately before mutation, the
service reopens arguments, rebuilds the canonical preview, rechecks current
authority, and atomically consumes the exact approval. A later provider-turn
capacity check must happen before staging an approval on a turn that could not
continue.

Failure of a safe precondition leaves an unconsumed checkpoint available for
durable terminal classification. After checkpoint consumption, a crash is
provider-boundary ambiguity and cannot reinstate the link. After approval
consumption, a crash cannot recreate or reuse the approval.

## Cross-generation adoption classes

| Durable state | Adoption result | Reason |
|---|---|---|
| Exact complete read-only batch, provider-retained continuation | Allowed after full validation | No resolver or provider effect is repeated |
| Exact complete bounded stateless read-only history | Allowed after every historical row, budget, result, disclosure, and egress proof validates | Visible history is reconstructible |
| Exact complete single supervised provider-retained mutation result with consumed approval | Allowed after full approval/result validation | Mutation is not repeated |
| Provider-turn checkpoint before any call result | Closed | It cannot prove whether a resolver later ran |
| Partial read-only batch | Closed | The missing position could be unstarted, running, completed but unpersisted, or ambiguous |
| Any partial consequential batch | Closed | One-shot consumption or mutation effect cannot be inferred |
| Uncheckpointed provider result or tool result | Recovery required | Durable handoff is missing |
| Consumed checkpoint without a later provider result | Recovery required | Provider transport may have begun |
| Changed call order, ID, tool/fingerprint, arguments, result, rule, budget, manifest, or continuation chain | Recovery required | Exact history identity failed |
| Restored active/waiting snapshot | Restore reconciliation only | Snapshot time is not live lease or effect authority |

Completed-batch adoption never changes the historical coordinates. It creates
a new attempt/generation only for future work and preserves the immutable
checkpoint ID, source attempt, original provider budget, steps, calls,
approval, egress facts, counters, and chain reference.

Partial read-only recovery remains deliberately unsupported. Although queries
are declared idempotent, repeating a resolver could observe new state, consume
rate limits, generate application audit, or trigger an unmodeled backend
effect. A future partial-read proof would need a durable per-position
unambiguous completion certificate and a complete ordered batch builder; the
current tool row alone is insufficient.

Parallel consequential execution is permanently unsupported by this generic
coordinator. There is no safe portable ordering for multiple simultaneous
one-shot approvals, mutations, resource-version changes, and ambiguous
outcomes. A consumer-specific workflow may expose one server-authored
transaction as one reviewed GraphQL mutation and one approval, but it does not
become a generic parallel batch.

## Stateless reconstruction by provider family

Stateless replay contains only the original trusted instructions, bounded
visible text/JSON user blocks, exact assistant tool-call records, and
disclosure-validated ordered tool results. It excludes hidden reasoning,
arbitrary roles, attachments, built-ins, output schemas, model-authored system
instructions, and unknown provider state. Every historical tool result gets a
fresh unique `ToolResult` transfer for the new provider turn while retaining
its original immutable result/audit proof.

- Anthropic: supported only with ordinary visible Messages history. Extended
  thinking and prompt-cache creation remain closed.
- Ollama: supported with `think: false`; no hidden thinking or provider-owned
  continuation exists.
- installed JSON-lines harness: supported only by a registration opting into
  the exact v2 stateless tool contract; the harness receives no filesystem,
  network, credential, or callback authority.
- OpenAI: tool continuation uses an explicitly retained exact response ID.
  Stateless reasoning/output-item replay remains closed because the complete
  provider history cannot be reconstructed by dropping encrypted or
  provider-owned items.
- xAI: tool continuation uses an explicitly retained exact response ID and is
  available only when the deployment disables the default ZDR requirement and
  authorizes retention. Stateless encrypted-reasoning replay remains closed.
- profiled OpenAI-compatible: only the reviewed profile's explicit
  provider-retained continuation is eligible. The crate does not infer
  stateless replay from an OpenAI-shaped endpoint.

Changing continuation family or retention mode across a chain is a mismatch,
not a fallback.

## Crash-window classification

- Before budget reservation: no external effect; fail safely.
- After reservation but before provider transport: release only with a proven
  pre-transport failure.
- During/after provider transport before authoritative result settlement:
  recovery required; never resend.
- After settled provider result but before provider-turn checkpoint: recovery
  required; never resend.
- After provider-turn checkpoint but before a resolver begins: the current
  generation may continue; a replacement generation cannot adopt it.
- During a read-only resolver or before its protected result transaction:
  recovery required; never repeat automatically.
- Between sequential completed read positions: partial batch remains closed.
- After all results but before batch checkpoint: recovery required.
- After batch checkpoint but before consumption: exact adoption may retry under
  current authority.
- After consumption but before/while provider transport: recovery required.
- Before approval consumption: an exact live wait may remain pending, be
  denied/cancelled, or be claimed by one worker.
- After approval consumption and before a complete mutation result checkpoint:
  recovery required; mutation and approval are never replayed.
- After complete supervised checkpoint but before consumption: exact
  provider-retained adoption may continue without rerunning the mutation.
- During final protected output persistence: only the existing exact
  same-transaction completion/recovery proof may finalize.

## Negative-test obligations

The existing suites prove exact ordering, unique call/result matching,
sequential fence rotation, complete read-only batch adoption, bounded stateless
history validation, single supervised retained-result adoption, pre-provider
checkpoint consumption, turn-limit preservation, and recovery on missing
checkpoints. Any future admitted shape must additionally test:

- reordered/duplicated/missing calls and results;
- stale attempts, generations, row versions, checkpoints, and approvals;
- changed rules, descriptors, arguments, previews, resources, budgets, egress
  manifests, provider profiles, retention, and continuation families;
- capacity denial before approval/checkpoint consumption;
- every crash window above;
- restored snapshots and deleting sessions; and
- concurrency proving at most one consumer of every one-shot link.

No missing reusable ORM or auth primitive was found while deriving this proof
from the current generated transaction/fencing and current-principal APIs. No
upstream handoff is open.
