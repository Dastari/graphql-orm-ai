//! Explicitly profiled OpenAI-compatible Responses adapter.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use url::Url;

use super::openai::{CompatibleRouteBinding, OpenAiProvider, OpenAiProviderConfig};
use crate::{
    AiOpenAiCompatibleProfileView, AiProvider, AiProviderEndpointPolicy, AiProviderKindInput,
    AiProviderProfileView, AiSecretStore, ModelRequest, ProviderCapabilities, ProviderError,
    ProviderEventStream, ProviderKind, ProviderRequestContext, SecretRef,
};

/// Immutable capabilities declared for one OpenAI-compatible endpoint.
///
/// These flags are deployment-reviewed assertions, not capabilities discovered
/// from the endpoint or model. Streaming text is always required. Attachments,
/// provider built-ins, background work, and local authority are structurally
/// unavailable from this profile.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OpenAiCompatibleCapabilities {
    /// Permit strict custom application tools.
    pub custom_tools: bool,
    /// Permit more than one custom tool call in a response.
    pub parallel_tool_calls: bool,
    /// Permit JSON-schema structured output.
    pub structured_output: bool,
    /// Permit provider-retained response-ID continuation.
    pub provider_retained_continuation: bool,
}

impl OpenAiCompatibleCapabilities {
    fn validate(&self) -> Result<(), ProviderError> {
        if self.parallel_tool_calls && !self.custom_tools {
            return Err(ProviderError::InvalidConfiguration(
                "parallel compatible tools require custom-tool capability".to_owned(),
            ));
        }
        Ok(())
    }

    fn provider_capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            custom_tools: self.custom_tools,
            parallel_tool_calls: self.parallel_tool_calls,
            structured_output: self.structured_output,
            provider_retained_continuation: self.provider_retained_continuation,
            ..ProviderCapabilities::default()
        }
    }
}

impl From<&AiOpenAiCompatibleProfileView> for OpenAiCompatibleCapabilities {
    fn from(profile: &AiOpenAiCompatibleProfileView) -> Self {
        Self {
            custom_tools: profile.custom_tools,
            parallel_tool_calls: profile.parallel_tool_calls,
            structured_output: profile.structured_output,
            provider_retained_continuation: profile.provider_retained_continuation,
        }
    }
}

/// Deployment-owned immutable OpenAI-compatible profile.
///
/// The exact profile ID, normalized endpoint, and retention declaration are
/// re-bound to every inference/tool-result egress manifest. Credential
/// plaintext is resolved only immediately before transport.
#[derive(Clone)]
pub struct OpenAiCompatibleProviderConfig {
    /// Stable GraphQL-managed provider-profile ID.
    pub profile_id: String,
    /// Exact deployment-configured Responses endpoint.
    pub responses_endpoint: String,
    /// Secret-store reference for the endpoint's Bearer credential.
    pub credential: SecretRef,
    /// Exact provider retention label expected in every egress manifest.
    pub retention: String,
    /// Overall HTTP request and stream timeout.
    pub timeout: Duration,
    /// Reviewed endpoint capabilities.
    pub capabilities: OpenAiCompatibleCapabilities,
}

impl OpenAiCompatibleProviderConfig {
    /// Creates a text-only, non-retaining compatible profile.
    pub fn new(
        profile_id: impl Into<String>,
        responses_endpoint: impl Into<String>,
        credential: SecretRef,
        retention: impl Into<String>,
    ) -> Self {
        Self {
            profile_id: profile_id.into(),
            responses_endpoint: responses_endpoint.into(),
            credential,
            retention: retention.into(),
            timeout: Duration::from_secs(120),
            capabilities: OpenAiCompatibleCapabilities::default(),
        }
    }

    /// Creates a transport profile from one redacted GraphQL-managed view and
    /// its separately loaded secret reference.
    ///
    /// The view does not prove endpoint authorization, credential
    /// availability, or egress permission. [`OpenAiCompatibleProvider::new`]
    /// still validates the exact endpoint through the deployment policy, and
    /// each request must carry exact egress and budget proofs.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::InvalidConfiguration`] unless the view is an
    /// enabled, fully configured `openai_compatible` profile with an endpoint.
    pub fn from_profile(
        profile: &AiProviderProfileView,
        credential: SecretRef,
    ) -> Result<Self, ProviderError> {
        let compatible = profile.openai_compatible.as_ref().ok_or_else(|| {
            ProviderError::InvalidConfiguration(
                "OpenAI-compatible profile has no reviewed capability contract".to_owned(),
            )
        })?;
        let endpoint = profile.base_url.clone().ok_or_else(|| {
            ProviderError::InvalidConfiguration(
                "OpenAI-compatible profile has no endpoint".to_owned(),
            )
        })?;
        if profile.provider_kind != AiProviderKindInput::OpenAiCompatible.as_str()
            || !profile.enabled
            || !profile.credential_configured
        {
            return Err(ProviderError::InvalidConfiguration(
                "provider profile is not an enabled OpenAI-compatible route".to_owned(),
            ));
        }
        let mut config = Self::new(
            profile.id.to_string(),
            endpoint,
            credential,
            compatible.retention.clone(),
        );
        config.capabilities = compatible.into();
        Ok(config)
    }
}

impl std::fmt::Debug for OpenAiCompatibleProviderConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenAiCompatibleProviderConfig")
            .field("profile_id", &self.profile_id)
            .field("responses_endpoint", &"<deployment-authorized>")
            .field("credential", &"<secret-reference>")
            .field("retention", &self.retention)
            .field("timeout", &self.timeout)
            .field("capabilities", &self.capabilities)
            .finish()
    }
}

/// OpenAI-compatible Responses provider with immutable endpoint/profile bounds.
///
/// Compatibility is deliberately narrow: exact Responses SSE, bounded
/// text/JSON, optional strict application tools, optional structured output,
/// and optional explicitly retained response IDs. The adapter never probes
/// capabilities or accepts a runtime URL.
pub struct OpenAiCompatibleProvider {
    config: OpenAiCompatibleProviderConfig,
    inner: OpenAiProvider,
}

impl std::fmt::Debug for OpenAiCompatibleProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenAiCompatibleProvider")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl OpenAiCompatibleProvider {
    /// Builds an adapter only after deployment authorization of the exact
    /// normalized endpoint.
    ///
    /// Redirects are disabled. The endpoint policy must enforce permitted
    /// hosts, ports, DNS/network zones, TLS requirements, and local/container
    /// rules. String authorization cannot by itself prevent DNS rebinding.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::InvalidConfiguration`] for an unsafe or denied
    /// endpoint, invalid profile/retention/capability/timeout data, or HTTP
    /// client construction failure.
    pub fn new(
        config: OpenAiCompatibleProviderConfig,
        endpoint_policy: Arc<dyn AiProviderEndpointPolicy>,
        secrets: Arc<dyn AiSecretStore>,
    ) -> Result<Self, ProviderError> {
        validate_config(&config)?;
        let endpoint = normalized_responses_endpoint(&config.responses_endpoint)?;
        if !endpoint_policy
            .authorize_endpoint(AiProviderKindInput::OpenAiCompatible, endpoint.as_str())
        {
            return Err(ProviderError::InvalidConfiguration(
                "OpenAI-compatible endpoint was not authorized".to_owned(),
            ));
        }
        let capabilities = config.capabilities.provider_capabilities();
        let core = OpenAiProviderConfig {
            credential: config.credential.clone(),
            organization: None,
            project: None,
            timeout: config.timeout,
            store_responses: config.capabilities.provider_retained_continuation,
        };
        let binding = CompatibleRouteBinding {
            profile_id: config.profile_id.clone(),
            destination: endpoint.as_str().to_owned(),
            retention: config.retention.clone(),
        };
        let inner = OpenAiProvider::new_compatible(
            core,
            secrets,
            endpoint.as_str().to_owned(),
            capabilities,
            binding,
        )?;
        Ok(Self { config, inner })
    }
}

#[async_trait]
impl AiProvider for OpenAiCompatibleProvider {
    fn provider_kind(&self) -> ProviderKind {
        ProviderKind::OpenAiCompatible
    }

    fn capabilities(&self) -> ProviderCapabilities {
        self.inner.capabilities()
    }

    async fn stream(
        &self,
        request: ModelRequest,
        context: ProviderRequestContext,
    ) -> Result<ProviderEventStream, ProviderError> {
        self.inner.stream(request, context).await
    }
}

fn validate_config(config: &OpenAiCompatibleProviderConfig) -> Result<(), ProviderError> {
    if config.profile_id.is_empty()
        || config.profile_id.len() > 200
        || config.retention.is_empty()
        || config.retention.len() > 200
        || config.timeout.is_zero()
        || config.timeout > Duration::from_secs(600)
        || !config
            .profile_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
        || !config
            .retention
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(ProviderError::InvalidConfiguration(
            "invalid OpenAI-compatible profile binding".to_owned(),
        ));
    }
    config.capabilities.validate()
}

fn normalized_responses_endpoint(value: &str) -> Result<Url, ProviderError> {
    let mut url = Url::parse(value).map_err(|_| {
        ProviderError::InvalidConfiguration(
            "invalid OpenAI-compatible Responses endpoint".to_owned(),
        )
    })?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path().is_empty()
        || url.path() == "/"
        || url.path().len() > 2_048
    {
        return Err(ProviderError::InvalidConfiguration(
            "unsafe OpenAI-compatible Responses endpoint".to_owned(),
        ));
    }
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::TryStreamExt;
    use secrecy::SecretString;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    use super::*;
    use crate::{
        AiBudgetAmounts, AiBudgetReservation, AiBudgetReservationId, AiDataSourceRef,
        AiDestinationTrust, AiEgressCapability, AiEgressDecision, AiEgressManifest, AiRunId,
        AiScope, AiSessionId, AiSourceTrust, DataClassification, ModelContinuationMode,
        ModelInputBlock, ProviderEvent, SecretError,
    };

    struct TestEndpointPolicy(String, bool);

    impl AiProviderEndpointPolicy for TestEndpointPolicy {
        fn authorize_endpoint(
            &self,
            provider_kind: AiProviderKindInput,
            normalized_url: &str,
        ) -> bool {
            provider_kind == AiProviderKindInput::OpenAiCompatible
                && self.1
                && normalized_url == self.0
        }
    }

    struct TestSecrets(SecretRef);

    #[async_trait]
    impl AiSecretStore for TestSecrets {
        async fn resolve(&self, reference: &SecretRef) -> Result<SecretString, SecretError> {
            if reference == &self.0 {
                Ok(SecretString::from("synthetic-compatible-key".to_owned()))
            } else {
                Err(SecretError::Unavailable)
            }
        }

        async fn put(
            &self,
            _reference: Option<&SecretRef>,
            _value: SecretString,
        ) -> Result<SecretRef, SecretError> {
            Err(SecretError::ReadOnly)
        }

        async fn delete(&self, _reference: &SecretRef) -> Result<(), SecretError> {
            Err(SecretError::ReadOnly)
        }
    }

    fn request() -> ModelRequest {
        ModelRequest {
            model: "compatible-test".to_owned(),
            instructions: vec!["Return a synthetic greeting.".to_owned()],
            input: vec![ModelInputBlock::Text {
                text: "hello".to_owned(),
            }],
            continuation: None,
            continuation_mode: ModelContinuationMode::ProviderRetained,
            tools: Vec::new(),
            builtin_tools: Vec::new(),
            maximum_builtin_tool_calls: None,
            output_schema: None,
            maximum_output_tokens: Some(32),
        }
    }

    fn context(request: &ModelRequest, destination: &str) -> ProviderRequestContext {
        let session_id = AiSessionId::new();
        let run_id = AiRunId::new();
        let attempt_id = uuid::Uuid::new_v4();
        let manifest = AiEgressManifest {
            provider_profile_id: "compatible-profile".to_owned(),
            provider_kind: ProviderKind::OpenAiCompatible.as_str().to_owned(),
            model: request.model.clone(),
            destination: destination.to_owned(),
            destination_trust: AiDestinationTrust::ExternalProcessor,
            capability: AiEgressCapability::ModelInference,
            scope: AiScope::new("project", "compatible-test"),
            session_id: Some(session_id),
            run_id: Some(run_id),
            sources: vec![AiDataSourceRef {
                kind: "message".to_owned(),
                reference: "synthetic".to_owned(),
                classification: DataClassification::Public,
                trust: AiSourceTrust::UserProvided,
            }],
            estimated_bytes: request.conservative_egress_bytes(),
            estimated_tokens: 100,
            attachment_count: 0,
            purpose: "test".to_owned(),
            retention: "deployment-reviewed".to_owned(),
            residency: None,
            policy_version: "test".to_owned(),
            consent_reference: None,
        };
        let proof = AiEgressDecision::allow(&manifest, "test", "test-user")
            .authorize(&manifest)
            .expect("manifest should authorize");
        let budget = AiBudgetReservation::new_reserved(
            AiBudgetReservationId::new(),
            run_id,
            attempt_id,
            1,
            ProviderKind::OpenAiCompatible,
            &request.model,
            "test-pricing-v1",
            AiBudgetAmounts {
                input_tokens: 1_000,
                output_tokens: 1_000,
                runs: 1,
                ..AiBudgetAmounts::default()
            },
            time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        )
        .expect("budget should validate")
        .authorize_provider_call(
            run_id,
            attempt_id,
            1,
            &ProviderKind::OpenAiCompatible,
            &request.model,
            32,
            0,
            time::OffsetDateTime::now_utc(),
        )
        .expect("budget should authorize");
        ProviderRequestContext::new(session_id, run_id, "test", budget, manifest, proof)
            .expect("context should validate")
    }

    async fn mock_server() -> (
        String,
        oneshot::Receiver<Vec<u8>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener should bind");
        let address = listener.local_addr().expect("listener should have address");
        let endpoint = format!("http://{address}/v1/responses");
        let (request_tx, request_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("request should connect");
            let mut request = vec![0_u8; 64 * 1024];
            let count = socket
                .read(&mut request)
                .await
                .expect("request should read");
            request.truncate(count);
            let _ = request_tx.send(request);
            let body = concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-test\",\"model\":\"compatible-test\"}}\n\n",
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-test\",\"model\":\"compatible-test\",\"status\":\"completed\",\"usage\":{\"input_tokens\":2,\"output_tokens\":1}}}\n\n"
            );
            let headers = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            socket
                .write_all(headers.as_bytes())
                .await
                .expect("headers should write");
            socket
                .write_all(body.as_bytes())
                .await
                .expect("body should write");
        });
        (endpoint, request_rx, task)
    }

    #[tokio::test]
    async fn exact_profile_destination_and_retention_are_bound_before_transport() {
        let (endpoint, request_rx, server) = mock_server().await;
        let reference = SecretRef::parse("compatible/test").expect("reference should parse");
        let mut config = OpenAiCompatibleProviderConfig::new(
            "compatible-profile",
            endpoint.clone(),
            reference.clone(),
            "deployment-reviewed",
        );
        config.capabilities.provider_retained_continuation = true;
        let provider = OpenAiCompatibleProvider::new(
            config,
            Arc::new(TestEndpointPolicy(endpoint.clone(), true)),
            Arc::new(TestSecrets(reference)),
        )
        .expect("compatible provider should build");
        let model_request = request();
        let events = provider
            .stream(model_request.clone(), context(&model_request, &endpoint))
            .await
            .expect("compatible stream should start")
            .try_collect::<Vec<_>>()
            .await
            .expect("compatible stream should normalize");
        assert!(matches!(
            events.as_slice(),
            [
                ProviderEvent::ResponseStarted { .. },
                ProviderEvent::TextDelta { .. },
                ProviderEvent::Usage { .. },
                ProviderEvent::ResponseCompleted { .. }
            ]
        ));
        let raw = String::from_utf8(request_rx.await.expect("request should be captured"))
            .expect("request should be UTF-8");
        assert!(
            raw.to_ascii_lowercase()
                .contains("authorization: bearer synthetic-compatible-key")
        );
        assert!(raw.contains("\"store\":true"));
        server.await.expect("mock server should finish");

        let reference = SecretRef::parse("compatible/test").expect("reference should parse");
        let denied_endpoint = "http://127.0.0.1:1/v1/responses".to_owned();
        let provider = OpenAiCompatibleProvider::new(
            OpenAiCompatibleProviderConfig::new(
                "compatible-profile",
                denied_endpoint.clone(),
                reference.clone(),
                "deployment-reviewed",
            ),
            Arc::new(TestEndpointPolicy(denied_endpoint.clone(), true)),
            Arc::new(TestSecrets(reference)),
        )
        .expect("compatible provider should build");
        let model_request = request();
        assert!(matches!(
            provider
                .stream(
                    model_request.clone(),
                    context(&model_request, "http://swapped.invalid/v1/responses")
                )
                .await,
            Err(ProviderError::EgressDenied)
        ));
    }

    #[test]
    fn endpoint_and_capability_profiles_fail_closed() {
        let reference = SecretRef::parse("compatible/test").expect("reference should parse");
        let endpoint = "https://compatible.example/v1/responses".to_owned();
        let config = OpenAiCompatibleProviderConfig::new(
            "compatible-profile",
            endpoint.clone(),
            reference.clone(),
            "deployment-reviewed",
        );
        assert!(matches!(
            OpenAiCompatibleProvider::new(
                config,
                Arc::new(TestEndpointPolicy(endpoint.clone(), false)),
                Arc::new(TestSecrets(reference.clone()))
            ),
            Err(ProviderError::InvalidConfiguration(_))
        ));

        let mut invalid = OpenAiCompatibleProviderConfig::new(
            "compatible-profile",
            endpoint.clone(),
            reference.clone(),
            "deployment-reviewed",
        );
        invalid.capabilities.parallel_tool_calls = true;
        assert!(matches!(
            OpenAiCompatibleProvider::new(
                invalid,
                Arc::new(TestEndpointPolicy(endpoint, true)),
                Arc::new(TestSecrets(reference))
            ),
            Err(ProviderError::InvalidConfiguration(_))
        ));

        let profile = AiProviderProfileView {
            id: uuid::Uuid::new_v4(),
            scope_kind: "project".to_owned(),
            scope_id: "compatible-test".to_owned(),
            tenant_id: None,
            provider_kind: AiProviderKindInput::OpenAiCompatible.as_str().to_owned(),
            display_name: "Compatible test".to_owned(),
            base_url: Some("https://compatible.example/v1/responses".to_owned()),
            openai_compatible: Some(AiOpenAiCompatibleProfileView {
                retention: "deployment-reviewed".to_owned(),
                custom_tools: true,
                parallel_tool_calls: false,
                structured_output: true,
                provider_retained_continuation: false,
            }),
            credential_configured: true,
            enabled: true,
            row_version: 1,
            updated_at: 1,
        };
        let from_profile = OpenAiCompatibleProviderConfig::from_profile(
            &profile,
            SecretRef::parse("compatible/profile").expect("reference should parse"),
        )
        .expect("reviewed enabled profile should construct configuration");
        assert_eq!(from_profile.profile_id, profile.id.to_string());
        assert!(from_profile.capabilities.custom_tools);
        assert!(from_profile.capabilities.structured_output);

        let mut disabled = profile;
        disabled.enabled = false;
        assert!(matches!(
            OpenAiCompatibleProviderConfig::from_profile(
                &disabled,
                SecretRef::parse("compatible/profile").expect("reference should parse")
            ),
            Err(ProviderError::InvalidConfiguration(_))
        ));
    }
}
