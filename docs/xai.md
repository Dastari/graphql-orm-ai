# Native xAI Provider

Enable `provider-xai` for the native adapter to xAI's
[Responses API](https://docs.x.ai/developers/model-capabilities/text/comparison)
at `https://api.x.ai/v1/responses`. The production destination is fixed,
redirects are disabled, and the timeout is bounded. This is not an arbitrary
OpenAI-compatible endpoint profile.

## Construction and credentials

Create `XAiProviderConfig` from a `SecretRef`, then pass it and an
`Arc<dyn AiSecretStore>` to `XAiProvider::new`. Plaintext is resolved immediately
before transport and used only as an HTTP Bearer credential. It is never stored
in configuration, events, errors, or debug output.

Only the AI deployment should receive provider network egress and credential
access. DNS, proxy, firewall, residency, and secret-store isolation remain host
responsibilities.

## Retention contract

xAI documents ordinary temporary audit retention independently of the
Responses `store` flag. Enterprise zero-data-retention is attested by the
`x-zero-data-retention` response header. `require_zero_data_retention` therefore
defaults to true, and the adapter rejects the response before exposing streamed
output if the exact affirmative attestation is absent.

Disabling that check is an explicit deployment decision. It does not grant
egress authority: the route's disclosure and egress policy must still describe
and permit the provider's current retention contract.

`store_responses` defaults to false. Enabling it:

- is incompatible with required ZDR verification for HTTP continuation;
- advertises provider-retained continuation to route selection;
- requires the exact `provider_response` retention manifest and proof on every
  call; and
- never substitutes for current principal, tool, egress, or budget checks.

See xAI's current
[security documentation](https://docs.x.ai/developers/faq/security) before
changing either setting.

## Supported request contract

The adapter supports:

- bounded Responses SSE streaming;
- text and JSON input;
- JSON-schema structured output;
- strict server-authored custom tools; and
- parallel application tool calls.

xAI function schemas are strictly compiled by the provider. The adapter accepts
only local definitions whose `strict` contract is true, maps only exact offered
provider names back to local tool IDs, and relies on the ordinary application
resolver bridge for fresh authorization and static disclosure validation.
Every call still requires exact xAI/model/session/run egress and atomic budget
proofs plus an explicit maximum output-token ceiling.

The following are deliberately unsupported:

- image/file attachments and provider-persistent files;
- web/X search, code execution, file search, MCP, or other xAI server tools;
- stateless encrypted-reasoning continuation;
- arbitrary beta headers; and
- custom endpoints.

Provider server tools have distinct output, billable-unit, disclosure, and
retention contracts. They can be added only with exact per-tool egress proofs,
normalization, authoritative pricing, and result bounds.

## Testing

Automated tests use an IPv4-loopback SSE mock and a synthetic credential. They
verify the request/auth shape, provider-kind binding, strict parallel tools,
usage normalization, narrow capability declaration, ZDR attestation, and the
incompatible ZDR/retained-response configuration. No xAI request or external
database is used.
