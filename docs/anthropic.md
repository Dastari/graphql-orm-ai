# Native Anthropic Provider

Enable `provider-anthropic` for a native adapter to Anthropic's
[Messages API](https://platform.claude.com/docs/en/api/messages/create) and
[streaming event contract](https://platform.claude.com/docs/en/build-with-claude/streaming).
It is not a configurable OpenAI-compatible proxy: the production constructor
uses the official HTTPS Messages endpoint, a fixed supported API-version
header, disabled redirects, and a bounded timeout.

## Construction and credentials

Create `AnthropicProviderConfig` from a `SecretRef`, then pass it and an
`Arc<dyn AiSecretStore>` to `AnthropicProvider::new`. The public configuration
retains only the secret reference. Plaintext is resolved immediately before
the HTTP request, validated as a header value, and neither retained nor placed
in errors. Provider profiles, GraphQL values, models, and tool arguments cannot
set a URL, header, or plaintext credential.

Only the deployment containing this adapter should receive Anthropic network
egress and credential-store access. DNS, proxy, firewall, residency, and
deployment secret isolation remain host responsibilities.

## Supported request contract

The adapter advertises:

- bounded SSE streaming;
- text and JSON input;
- strict server-authored custom tools and parallel tool calls;
- protected `StatelessConversation` continuation; and
- JSON-schema structured output through `output_config.format`.

Every request must include `maximum_output_tokens`. Application tools retain
the exact server-offered provider name, local tool ID, descriptor fingerprint,
arguments, and result identity. Contiguous tool results are mapped to one
Anthropic user turn, but every result still requires its own fresh disclosure
manifest in the provider-neutral request context. The adapter never accepts a
model-authored tool definition or arbitrary GraphQL document.

The following remain deliberately unsupported and fail closed:

- attachments and provider-persistent files;
- Anthropic server tools;
- provider-retained continuation;
- hidden or extended-thinking blocks;
- prompt-cache creation; and
- custom endpoints or arbitrary beta headers.

Unknown top-level SSE event types are preserved as bounded
`ProviderEvent::Unknown` values only during a valid active response. Unknown
content blocks/deltas, unoffered tool names, malformed partial JSON, excessive
events/arguments/tool calls, incomplete streams, and unsupported stop reasons
are rejected.

## Usage and pricing

Anthropic defines total input as uncached input plus cache-created and
cache-read input. The crate's authoritative ledger currently represents total
input and a cached-read subset, but not Anthropic's separately priced cache
creation class. The adapter therefore emits no cache-control directive and
rejects any response reporting a nonzero cache write. A cache read is included
in total input and reported as the cached subset so an exact route-specific
cached-input price can settle it.

This restriction is an accounting security boundary, not merely a missing
optimization. Prompt caching can be added only with a distinct immutable
billable-unit/rate contract, reservation ceiling, usage observation, migration,
and restore validation.

## Testing

Automated tests use a bounded IPv4-loopback mock that verifies request headers,
body mapping, SSE state normalization, usage, and safe HTTP/stream failure
classification. They use only a synthetic key and make no Anthropic request.
No database is required beyond the repository's ordinary in-memory SQLite
suite.
