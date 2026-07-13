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

## Bounded continuation

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
turn. It installs the previous response ID, matched tool result blocks, and the
immutable manifests produced for those exact results as one unit. The normal
provider executor still reserves a fresh atomic budget and freshly authorizes
and audits the model-inference and result transfers before transport.

Do not reconstruct a guard to resume an ambiguous loop. A lost worker,
partially completed batch, expired lease, unknown provider response, or restore
snapshot stays closed for reconciliation/operator review.

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
  descriptor can enter this loop.
- No top-level crash-resumable loop worker is implemented yet; the guard and
  durable primitives make the proof sequence explicit for that worker.
- Live provider/tool delta coalescing is not implemented; clients continue to
  consume durable bounded cursor windows.
- Tool enablement management is not yet exposed through its final
  authenticated GraphQL configuration lifecycle.
- Provider-independent stateless continuation and Anthropic/xAI/Ollama tool
  result mappings remain future adapter work.
