# Installed Local Harness Boundary

The optional `local-harness` feature supports installed model or agent programs
without turning them into a shell tool. It is separate from Ollama and other
local HTTP providers: those use fixed endpoint adapters, while an installed
program uses an immutable deployment registration and a trusted process-tree
launcher.

The implemented foundation is suitable for a narrow text or structured-output
harness. It deliberately does not grant coding-workspace, filesystem, terminal,
MCP, provider built-in, attachment, network, credential, custom-tool, or
provider-continuation authority.

## Authority split

`AiLocalHarnessRegistration` fixes:

- the server-authored logical model name;
- a normalized absolute executable and fixed argument vector;
- a required lowercase executable SHA-256 and reviewed version;
- an isolated absolute working directory and named sandbox profile;
- common narrow provider capabilities; and
- request/frame/stdout/stderr/count, startup/turn/shutdown, memory, and CPU
  ceilings.

The type has no environment, secret, mount, URL, network-mode, user argument,
or shell field. It is not serializable and is not accepted through GraphQL.
`AiLocalHarnessRegistry` rejects duplicate models, more than 128 entries, and
capability differences between registrations. GraphQL provider configuration
may enable or scope a `LocalHarness` logical profile but must supply no base
URL and cannot alter deployment process facts. Credential set/rotation is
rejected, and an already credentialed provider profile cannot be converted to
`LocalHarness` until its credential is removed through the audited lifecycle.

Registration validation is not sandbox proof. A trusted implementation of
`AiLocalHarnessProcessLauncher` must:

- atomically verify and execute the same registered image by digest;
- directly execute it without a shell or path lookup;
- use only the registered arguments and clear the complete inherited
  environment;
- supply no user bearer token, provider key, SSH/cloud agent, home directory,
  socket, TTY, or ambient stdin;
- deny network and prevent access outside the reviewed OS/container sandbox;
- enforce memory, CPU, wall-time, output, concurrency, and process-count
  ceilings;
- own the complete descendant tree; and
- synchronously initiate forced tree termination when the process handle is
  dropped before a proven exit.

The crate does not include a generic `std::process::Command` or
`tokio::process::Command` implementation because those properties require a
deployment-specific OS/container boundary. A plain child process with inherited
environment and best-effort child-only kill does not satisfy the trait
contract.

## Protocol and provider path

`AiJsonLinesLocalHarnessDriver` writes exactly one bounded JSON line:

```json
{
  "protocol": "graphql-orm-ai/local-harness-jsonl/v1",
  "type": "request",
  "model": "deployment-logical-name",
  "instructions": ["trusted runtime instruction"],
  "input": [{"type": "text", "text": "authorized content"}],
  "output_schema": null,
  "maximum_output_tokens": 256
}
```

It then closes stdin. Stdout is arbitrary transport chunks containing newline-
terminated serialized `ProviderEvent` values. The initial driver accepts one
ordered sequence of:

1. `response_started` without a response ID;
2. zero or more visible `text_delta` values;
3. one usage event within registered context/output token ceilings; and
4. `response_completed` without a response ID, followed by successful process
   exit.

Every line, total stdout, discarded stderr, frame count, startup, turn, and
shutdown is bounded. A partial line, malformed JSON, excessive counter,
duplicate/out-of-order terminal event, response ID, reasoning event, citation,
tool/built-in request, unknown event, output overrun, timeout, or unsuccessful
exit fails closed. Raw stderr and process/request content are never placed in a
`ProviderError`.

`AiLocalHarnessProvider` first validates the ordinary `ProviderRequestContext`
as `ProviderKind::LocalHarness`, including the exact current call's atomic
budget and model-inference egress proofs. The normal `AiProviderCallExecutor`
still performs principal rehydration, session/scope access, immutable egress
audit, uncertain-boundary accounting, output limits, usage settlement, fenced
checkpoints, and protected transcript persistence. “Local” does not mean free,
trusted, or exportable.

## Construction outline

```rust,no_run
use std::sync::Arc;

use graphql_orm_ai::{
    AiJsonLinesLocalHarnessDriver, AiLocalHarnessLimits,
    AiLocalHarnessProcessLauncher, AiLocalHarnessProvider,
    AiLocalHarnessRegistration, AiLocalHarnessRegistry, ProviderCapabilities,
    ProviderError,
};

# fn build(launcher: Arc<dyn AiLocalHarnessProcessLauncher>) -> Result<(), ProviderError> {
let capabilities = ProviderCapabilities {
    streaming: true,
    structured_output: true,
    local: true,
    maximum_context_tokens: Some(8_192),
    maximum_output_tokens: Some(1_024),
    ..ProviderCapabilities::default()
};
let registration = AiLocalHarnessRegistration::new(
    "local-reviewed-model",
    "/opt/reviewed/bin/model-harness",
    vec!["--json-lines".to_owned(), "--single-turn".to_owned()],
    "/var/empty/model-harness",
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "reviewed-1.0.0",
    "isolated-no-network-v1",
    AiLocalHarnessLimits::default(),
    capabilities,
)?;
let registry = AiLocalHarnessRegistry::new([registration])?;
let driver = Arc::new(AiJsonLinesLocalHarnessDriver::new(launcher));
let provider = AiLocalHarnessProvider::new(registry, driver);
# let _ = provider;
# Ok(())
# }
```

Register the provider under `ProviderKind::LocalHarness`. The request model is
the logical registration name, never an executable path. Do not expose the
registration getters to GraphQL or model-authored configuration.

## Deterministic conformance

The repository tests use an in-memory fake process/launcher. They prove that
the process request does not contain executable, arguments, sandbox identity,
or stderr; fixed launch facts cannot be swapped by model input; model/budget
proof swaps fail before launch; arbitrary output chunk boundaries normalize;
unsafe capabilities and process-requested tools fail; stderr and partial-frame
limits terminate; and dropping a partial stream exercises the required
kill-on-drop path. The suite starts no subprocess, contacts no model/provider,
and opens no database.

ACP framing, mediated tool callbacks, resumable sessions, and any separately
sandboxed coding workspace remain future adapters. They must not widen this
safe registration implicitly.
