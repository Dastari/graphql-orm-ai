# Read-Only Application-Tool Loop

The implemented custom-tool path is intentionally limited to explicit,
idempotent application GraphQL queries. It is not an approval shortcut and it
does not enable mutations, subscriptions, arbitrary GraphQL, shell commands,
raw repositories, MCP tools, or provider-selected endpoints.

## Eligibility

A model definition is accepted only when all of these are true:

- the stable tool ID is registered in `AiToolCatalog` with an exact logical
  GraphQL target, server-authored document, operation name, schema/document/
  projection fingerprint, and static disclosure schema;
- derive-generated operations additionally carry the exact current
  `graphql-orm` catalog and operation fingerprints, select only one unaliased
  generated root, and pass an explicit host application-domain policy through
  `register_generated_with_disclosure`; custom roots continue through the
  explicit reviewed scanner path;
- `AiToolPolicySet` contains an enabled binding for the current exact
  descriptor fingerprint;
- the descriptor is an application `Query` at `ReadOnly` maturity and risk;
- approval is `None` and the descriptor is marked idempotent; and
- provider name, description, JSON Schema, local ID, and fingerprint match the
  registered descriptor exactly.

`AiProviderCallPlan::new` remains the ordinary tool-free constructor.
`new_with_tools` makes read-only exposure deliberate. Registration and this
policy snapshot are still not execution authority: each actual call performs
fresh host tool policy and resolver authorization.

## Durable execution order

For each normalized provider call, `OrmAiApplicationToolCallService`:

1. verifies the provider result belongs to the exact session, run, attempt,
   generation, provider turn, and call position;
2. rechecks the current active session owner/tenant/scope and freshly resolved
   principal access;
3. protects and persists canonical arguments, their hash, descriptor
   fingerprint, provider call identity, and a running step behind the current
   lease fence;
4. constructs the exact registered `ToolGraphqlRequest` and invokes
   `AiRuntime::execute_tool` with a bounded timeout;
5. rehydrates again inside the authenticated bridge, applies current host tool
   policy, builds the ordinary GraphQL request context, and lets ordinary
   resolver/row/field/rate-limit authorization decide;
6. bounds safe error codes and output bytes, applies the closed static
   disclosure schema, and excludes the application audit reference from model
   output;
7. rehydrates/rechecks access again before building the exact result source,
   classification, byte count, destination, retention, purpose, session, run,
   provider, and model egress manifest;
8. authorizes and immutably audits that exact `ToolResult` disclosure; and
9. atomically protects the result, completes the tool-call and run-step rows,
   appends a protected session event, and renews the run fence.

Resolver/policy failures can produce only a generic, separately authorized
model-visible error. An egress denial or failed egress audit produces no
continuation block. Protection or persistence ambiguity fails closed and the
run is left for normal fenced recovery rather than replayed.

Always replace the old `AiRunLease` with the lease returned by
`AiPersistedApplicationToolCall`. The prior row-version proof is invalid.

## Bounded coordinator and continuation

`AiReadOnlyAgentCoordinator` is the supported top-level owner for one freshly
claimed read-only attempt. It starts the lease, heartbeats it while provider
streams are pending, obtains each exact plan from a trusted host
`AiReadOnlyAgentTurnPlanner`, applies the guard sequence below, persists final
protected output, and commits `Completed`, `Failed`, or `RecoveryRequired`.
Provider, resolver, and output ambiguity is never silently retried. If a
heartbeat loses the fence, the coordinator stops without attempting any final
write.

Each planner result must carry the exact current `AiResolvedRuleSet` and the
server-derived BYOK decision. Install one shared `OrmAiCurrentRuleResolver` on
the coordinator and ORM checkpoint service. It freshly rehydrates and resolves
the complete hierarchy before transport, after provider return, before every
resolver tool, around checkpoint protection, and during adoption. Estimates
are checked before transport; authoritative usage replaces them afterward.
The resulting v2 checkpoints bind the rule fingerprint and cumulative limits.
This negative rule proof never replaces tool enablement, resolver
authorization, egress authorization, atomic budget settlement, or approval.

Create one `AiAgentLoopGuard` from the original running lease. It binds the
session, run, attempt, generation, maximum provider turns, and maximum total
tool calls. For every turn:

1. pass the `AiProviderCallResult` to `observe_provider_turn`;
2. execute every returned call position once, carrying the renewed lease from
   one call to the next;
3. pass every durable outcome to `observe_tool_result`; and
4. obtain `AiAgentContinuation` only after all expected call IDs have exactly
   one model-visible result.

Use `AiProviderCallPlan::new_continuation_with_tools` for the next provider
turn. In `ProviderRetained` mode it installs the previous response ID. In
`StatelessReplay` mode it installs the protected original instructions,
visible text/JSON user input, exact assistant calls, preceding tool messages,
and matched current result blocks. Both modes carry the immutable manifests
for every result as one unit. The normal provider executor still reserves a
fresh atomic budget and freshly authorizes and audits model inference plus one
unique `ToolResult` transfer for every historical and current output before
transport.

Stateless history is bounded to 256 messages/tool results and excludes hidden
thinking, arbitrary roles, attachments, provider built-ins, output schemas,
and model-authored system instructions. Tool IDs, provider names,
fingerprints, argument objects, call order, and result order must still match
the currently reviewed definitions exactly.

The top-level coordinator additionally requires an `AiAgentCheckpointWriter`
and `AiAgentCheckpointAdopter`. The ORM checkpoint service implements both. It
protects and fences the accepted provider result before executing tools, then
protects the exact complete tool batch and opaque continuation before the next
plan. Checkpoint persistence failure after either external boundary closes the
run for recovery and never replays the provider or resolver.

Do not reconstruct a guard yourself. Cross-generation recovery can retain only
an exact complete provider-retained or stateless tool-batch checkpoint; the
adopter must freshly authorize, open and validate all durable evidence,
construct the opaque proof, and consume the checkpoint before the following
transport. Stateless adoption validates each historical protected result,
arguments, committed budget, finished step, disclosure classification, and
immutable allow audit and never reruns a resolver. A provider-turn checkpoint,
partially completed batch, consumed link, unknown response, malformed history,
or unprovable restore state stays closed for reconciliation/operator review.

## OpenAI continuation and retention

The native OpenAI adapter maps a continuation to `previous_response_id` and
each result to `function_call_output` with the exact `call_id`. Stateful
continuation requires the provider to retain the prior response, so it is
available only when deployment configuration explicitly sets
`OpenAiProviderConfig::store_responses = true` and every transfer manifest uses
`AI_EGRESS_RETENTION_PROVIDER_RESPONSE` (`provider_response`).

This mapping follows the official
[OpenAI function-calling guide](https://developers.openai.com/api/docs/guides/function-calling/).

The secure default remains `store_responses = false`. Under that default the
adapter rejects stateful continuation. Stateless continuation for reasoning
models requires protected encrypted reasoning/output-item handling and is not
yet implemented; the runtime does not silently drop required reasoning context
or turn retention on.

## Deliberate remaining gates

- No mutation, proposal, consequential, approval-required, or non-idempotent
  descriptor can enter this loop. Use the separate
  [supervised service](supervised-tool-loop.md) for exact one-shot application
  mutations; it is not yet owned by this coordinator.
- Cross-generation adoption is intentionally limited to exact completed
  provider-retained or bounded stateless read-only tool batches. Provider-turn,
  partial-batch, and consequential continuation adoption remain
  unimplemented.
- Optional protected live persistence is implemented for visible text and
  reasoning summaries. It excludes structured/tool events and validates fresh
  authority, protection policy, the exact run fence, and uncertain budget for
  every durable batch. It does not grant egress authority or change the
  coordinator's deliberately closed replay rules.
- Tool enablement management is not yet exposed through its final
  authenticated GraphQL configuration lifecycle.
- Anthropic, Ollama, and the installed JSON-lines v2 harness implement the
  bounded stateless contract. OpenAI and xAI use explicit provider-retained
  response IDs; xAI retained continuation is incompatible with its default
  zero-data-retention verification and must be separately configured and
  authorized.
