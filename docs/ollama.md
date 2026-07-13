# Native Ollama Provider

The optional `provider-ollama` feature provides a native Ollama `/api/chat`
adapter. It is intended for a deployment-managed Ollama server, including a
loopback or private-network server. “Local” describes the configured model
boundary; it is not permission to disclose data.

## Supported contract

The initial adapter supports:

- streamed text normalized from bounded NDJSON;
- exact released PNG, JPEG, and WebP attachments encoded ephemerally in the
  request;
- JSON-schema structured output;
- an explicit maximum output-token request; and
- authoritative `prompt_eval_count` and `eval_count` usage.

It sends `think: false` and never emits or persists Ollama thinking content.
It rejects custom tools, provider built-ins, non-image files, and continuation.
Ollama has native tool calling, but this runtime cannot safely advertise it
until a provider-independent stateless conversation checkpoint can reconstruct
the complete message/tool-result history after a durable handoff or restart.

The protocol behavior follows Ollama's official
[`/api/chat` reference](https://docs.ollama.com/api/chat),
[streaming contract](https://docs.ollama.com/api/streaming),
[vision guidance](https://docs.ollama.com/capabilities/vision), and
[structured-output guidance](https://docs.ollama.com/capabilities/structured-outputs).
Native [tool calling](https://docs.ollama.com/capabilities/tool-calling) remains
deliberately gated by the runtime guarantee above.

## Construction

```rust,no_run
use std::sync::Arc;

use graphql_orm_ai::{
    AiProviderEndpointPolicy, OllamaProvider, OllamaProviderConfig,
};

# fn build(policy: Arc<dyn AiProviderEndpointPolicy>) -> Result<(), graphql_orm_ai::ProviderError> {
let provider = OllamaProvider::new(
    OllamaProviderConfig::new("http://127.0.0.1:11434"),
    policy,
)?;
# let _ = provider;
# Ok(())
# }
```

Only a root `http` or `https` origin is accepted. URL credentials, path, query,
and fragment are rejected, and HTTP redirects are disabled. The endpoint policy
must match the normalized origin and enforce the deployment's exact host/port,
DNS rebinding, private-address, container-boundary, and network-zone rules. The
adapter does not discover servers or models and never accepts a model-authored
URL.

Ollama normally requires no API key. If a deployment puts an authenticating
gateway in front of it, that gateway needs a separately reviewed fixed
transport/credential boundary; credentials must not be placed in the URL.

## Egress, attachments, and budgets

Every call uses the ordinary `ProviderRequestContext` validation immediately
before transport. It must contain an exact `ModelInference` egress proof and an
atomic budget proof bound to Ollama, the selected model, run, attempt, fence,
output ceiling, pricing version, and expiry. Each image additionally needs its
exact `ImageAnalysis` transfer and `AiProviderAttachmentResolver` result.

The adapter accepts only the freshly reopened bytes whose opaque attachment ID,
detected MIME, byte count, and SHA-256 match the request. It never sends a blob
key or storage URL and creates no provider file object. Host quarantine,
scanning, acceptance, owner/scope authorization, egress classification, and
retention remain authoritative.

Ollama may keep a model loaded according to `keep_alive`; this is model-memory
lifetime, not a proof about prompt logging or response retention. Deployments
must separately configure and audit the server's storage, logs, network access,
model provenance, and process isolation.

## Verification

Automated tests use a deterministic loopback HTTP server that captures one
native request and fragments synthetic NDJSON. They do not contact a real
Ollama installation, fetch a model, use a credential, or connect to a database.
Wrong-model responses, truncated streams, unsafe endpoints, missing exact image
proofs, and unsupported tool requests fail closed.
