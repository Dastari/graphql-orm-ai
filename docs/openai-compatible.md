# Profiled OpenAI-Compatible Provider

The `provider-openai-compatible` feature supplies a deliberately narrow
adapter for endpoints that implement the OpenAI Responses streaming protocol.
It is not a promise that an arbitrary Chat Completions proxy, model server, or
OpenAI-shaped API will work.

## Supported contract

Every endpoint has a server-reviewed profile. Streaming text/JSON is required.
The profile may independently enable:

- strict custom application tools;
- parallel custom tool calls, only when custom tools are enabled;
- JSON-schema structured output;
- provider-retained response-ID continuation.

Attachments, provider built-ins, background work, capability discovery,
model-selected endpoints, and runtime endpoint changes are unavailable. A
request using an undeclared capability fails before transport. The shared
Responses normalizer requires SSE, exact response/model/status identity,
bounded output and tool-call state, exact usage, and one unambiguous terminal
completion.

## GraphQL-managed profile

Create or update a provider profile through the authenticated configuration
mutation with kind `OpenAiCompatible`, an exact Responses endpoint, and the
nested capability contract. For example, using the default GraphQL naming:

```graphql
mutation ConfigureCompatibleProvider {
  upsertAiProviderProfile(
    input: {
      scope: { kind: "workspace", id: "example" }
      providerKind: OPEN_AI_COMPATIBLE
      displayName: "Reviewed compatible endpoint"
      baseUrl: "https://models.example/v1/responses"
      openaiCompatible: {
        retention: "processor-zdr-v1"
        customTools: true
        parallelToolCalls: false
        structuredOutput: true
        providerRetainedContinuation: false
      }
      enabled: true
    }
  ) {
    id
    rowVersion
    openaiCompatible {
      retention
      customTools
      parallelToolCalls
      structuredOutput
      providerRetainedContinuation
    }
  }
}
```

The mutation still requires host configuration authorization and recent MFA.
The endpoint must pass `AiProviderEndpointPolicy`; that policy owns the actual
host/port/TLS/network-zone and local-container rules. Library URL validation,
normalization, and redirect denial do not prevent DNS rebinding on their own.
Deployments should enforce DNS and outbound-network policy at the transport or
network boundary as well.

Credentials are set through the separate credential mutation and remain in an
`AiSecretStore`. They never appear in the profile view or adapter debug output.
The public view includes only whether a credential is configured.

## Provider construction

A trusted registry loader obtains the redacted profile and its separately
protected `SecretRef`. It can then create immutable transport configuration:

```rust,no_run
# #[cfg(feature = "provider-openai-compatible")]
# fn build(
#     profile: &graphql_orm_ai::AiProviderProfileView,
#     credential: graphql_orm_ai::SecretRef,
#     endpoint_policy: std::sync::Arc<dyn graphql_orm_ai::AiProviderEndpointPolicy>,
#     secrets: std::sync::Arc<dyn graphql_orm_ai::AiSecretStore>,
# ) -> Result<graphql_orm_ai::OpenAiCompatibleProvider, graphql_orm_ai::ProviderError> {
let config = graphql_orm_ai::OpenAiCompatibleProviderConfig::from_profile(
    profile,
    credential,
)?;
let provider = graphql_orm_ai::OpenAiCompatibleProvider::new(
    config,
    endpoint_policy,
    secrets,
)?;
# Ok(provider)
# }
```

Construction rejects a disabled, wrong-kind, legacy, incomplete, unsafe, or
endpoint-policy-denied profile. It fixes the normalized endpoint and disables
redirects. The secret plaintext is resolved only immediately before each HTTP
request.

## Egress and retention

The configured retention label is an exact policy identifier, not descriptive
model prose. Every provider/model transfer for the call must bind the same:

- provider kind `openai_compatible`;
- profile UUID;
- normalized endpoint URL;
- model name;
- retention label.

At least one exact `ModelInference` transfer is required, along with the
ordinary atomic budget proof. A changed endpoint, profile, model, or retention
label fails closed. Enabling provider-retained continuation changes data
retention and must be reflected both in GraphQL configuration and the egress
decision. The adapter does not infer or verify a provider's legal promises;
the deployment owns that review.

Legacy compatible profiles with an empty data-policy object remain readable
after the schema-module upgrade, but the adapter cannot be built from them.
An authorized administrator must re-save each one with an explicit minimal
contract before it can route requests.

## Local endpoints

Loopback or private endpoints are supported only if deployment policy permits
their exact normalized Responses URL and the surrounding network isolation is
appropriate. This adapter does not launch a model, grant filesystem access, or
provide a shell. Installed executable harnesses use the separate
`local-harness` boundary; native Ollama servers should normally use the
`provider-ollama` adapter.
