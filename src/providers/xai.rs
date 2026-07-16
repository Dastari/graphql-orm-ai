//! Native xAI Responses API adapter.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use super::openai::{OpenAiProvider, OpenAiProviderConfig};
use crate::{
    AiProvider, AiSecretStore, ModelRequest, ProviderCapabilities, ProviderError,
    ProviderEventStream, ProviderKind, ProviderRequestContext, SecretRef,
};

/// Native xAI adapter configuration.
///
/// Credential plaintext is resolved only immediately before transport. The
/// official xAI Responses endpoint is fixed by the adapter and cannot be
/// supplied through GraphQL, provider-profile data, or model input.
#[derive(Clone, Debug)]
pub struct XAiProviderConfig {
    /// Secret-store reference for one xAI API key.
    pub credential: SecretRef,
    /// Overall HTTP request and stream timeout.
    pub timeout: Duration,
    /// Whether xAI may retain response state for response-ID continuation.
    ///
    /// This defaults to false. Enabling it does not itself authorize
    /// retention; each call still requires the exact provider-response
    /// retention proof in [`ProviderRequestContext`].
    pub store_responses: bool,
    /// Require xAI to attest zero-data-retention on every response.
    ///
    /// This defaults to true. Setting it to false is an explicit deployment
    /// decision and does not itself authorize xAI's documented ordinary audit
    /// retention; the egress policy must still disclose and permit it.
    pub require_zero_data_retention: bool,
}

impl XAiProviderConfig {
    /// Creates secure defaults for xAI's official Responses endpoint.
    pub fn new(credential: SecretRef) -> Self {
        Self {
            credential,
            timeout: Duration::from_secs(120),
            store_responses: false,
            require_zero_data_retention: true,
        }
    }
}

/// Native xAI/Grok Responses API provider.
///
/// The initial contract supports bounded text/JSON, JSON-schema structured
/// output, and strict custom/parallel application tools. Attachments, xAI
/// server tools, encrypted reasoning replay, and arbitrary endpoints remain
/// disabled. Stateful response-ID continuation is available only when both
/// deployment configuration and an exact egress-retention proof permit it.
pub struct XAiProvider {
    config: XAiProviderConfig,
    inner: OpenAiProvider,
}

impl std::fmt::Debug for XAiProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("XAiProvider")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl XAiProvider {
    /// Builds a provider fixed to xAI's official HTTPS Responses endpoint with
    /// redirects disabled.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::InvalidConfiguration`] for an invalid timeout
    /// or HTTP-client construction failure.
    pub fn new(
        config: XAiProviderConfig,
        secrets: Arc<dyn AiSecretStore>,
    ) -> Result<Self, ProviderError> {
        let inner = OpenAiProvider::new_xai(
            core_config(&config),
            secrets,
            config.require_zero_data_retention,
        )?;
        Ok(Self { config, inner })
    }

    #[cfg(test)]
    fn for_loopback_test(
        config: XAiProviderConfig,
        secrets: Arc<dyn AiSecretStore>,
        endpoint: String,
    ) -> Result<Self, ProviderError> {
        let inner = OpenAiProvider::for_xai_loopback_test(
            core_config(&config),
            secrets,
            endpoint,
            config.require_zero_data_retention,
        )?;
        Ok(Self { config, inner })
    }
}

fn core_config(config: &XAiProviderConfig) -> OpenAiProviderConfig {
    OpenAiProviderConfig {
        credential: config.credential.clone(),
        organization: None,
        project: None,
        timeout: config.timeout,
        store_responses: config.store_responses,
    }
}

#[async_trait]
impl AiProvider for XAiProvider {
    fn provider_kind(&self) -> ProviderKind {
        ProviderKind::Xai
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::TryStreamExt;
    use secrecy::SecretString;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    use super::*;
    use crate::{
        AiBudgetAmounts, AiBudgetReservation, AiBudgetReservationId, AiDataSourceRef,
        AiDestinationTrust, AiEgressCapability, AiEgressDecision, AiEgressManifest, AiRunId,
        AiScope, AiSessionId, AiSourceTrust, DataClassification, ModelContinuationMode,
        ModelInputBlock, ModelToolDefinition, ProviderEvent, SecretError,
    };

    struct TestSecrets(SecretRef, String);

    #[async_trait]
    impl AiSecretStore for TestSecrets {
        async fn resolve(&self, reference: &SecretRef) -> Result<SecretString, SecretError> {
            if reference == &self.0 {
                Ok(SecretString::from(self.1.clone()))
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
            model: "grok-test".to_owned(),
            instructions: vec!["Use only authorized application tools.".to_owned()],
            input: vec![ModelInputBlock::Text {
                text: "Read synthetic record 54".to_owned(),
            }],
            continuation: None,
            continuation_mode: ModelContinuationMode::ProviderRetained,
            tools: vec![ModelToolDefinition {
                tool_id: "records.read".to_owned(),
                provider_name: "records_read".to_owned(),
                fingerprint: "records-read-v1".to_owned(),
                description: "Read one authorized synthetic record".to_owned(),
                parameters: json!({
                    "type": "object",
                    "properties": {"recordId": {"type": "string"}},
                    "required": ["recordId"],
                    "additionalProperties": false
                }),
                strict: true,
            }],
            builtin_tools: Vec::new(),
            maximum_builtin_tool_calls: None,
            output_schema: None,
            maximum_output_tokens: Some(128),
        }
    }

    fn context(request: &ModelRequest) -> ProviderRequestContext {
        let session_id = AiSessionId::new();
        let run_id = AiRunId::new();
        let attempt_id = uuid::Uuid::new_v4();
        let manifest = AiEgressManifest {
            provider_profile_id: "xai-profile".to_owned(),
            provider_kind: ProviderKind::Xai.as_str().to_owned(),
            model: request.model.clone(),
            destination: "xai".to_owned(),
            destination_trust: AiDestinationTrust::ManagedProvider,
            capability: AiEgressCapability::ModelInference,
            scope: AiScope::new("project", "xai-test"),
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
            retention: "none".to_owned(),
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
            ProviderKind::Xai,
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
            &ProviderKind::Xai,
            &request.model,
            request
                .maximum_output_tokens
                .expect("request should have an output bound"),
            0,
            time::OffsetDateTime::now_utc(),
        )
        .expect("budget should authorize");
        ProviderRequestContext::new(session_id, run_id, "test", budget, manifest, proof)
            .expect("context should validate")
    }

    async fn mock_server(
        body: String,
        zero_data_retention: bool,
    ) -> (
        String,
        oneshot::Receiver<Vec<u8>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener should bind");
        let address = listener.local_addr().expect("listener should have address");
        let (request_tx, request_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("request should connect");
            let mut request = vec![0_u8; 128 * 1024];
            let count = socket
                .read(&mut request)
                .await
                .expect("request should read");
            request.truncate(count);
            let _ = request_tx.send(request);
            let zdr_header = if zero_data_retention {
                "x-zero-data-retention: true\r\n"
            } else {
                ""
            };
            let headers = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n{zdr_header}content-length: {}\r\nconnection: close\r\n\r\n",
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
        (format!("http://{address}/v1/responses"), request_rx, task)
    }

    async fn collect_zdr_stream(body: String) -> Result<Vec<ProviderEvent>, ProviderError> {
        let (endpoint, _request_rx, server) = mock_server(body, true).await;
        let reference = SecretRef::parse("xai/test").expect("secret reference should parse");
        let provider = XAiProvider::for_loopback_test(
            XAiProviderConfig::new(reference.clone()),
            Arc::new(TestSecrets(reference, "synthetic-xai-key".to_owned())),
            endpoint,
        )
        .expect("loopback xAI provider should build");
        let model_request = request();
        let result = provider
            .stream(model_request.clone(), context(&model_request))
            .await?
            .try_collect::<Vec<_>>()
            .await;
        server.await.expect("mock server should finish");
        result
    }

    #[tokio::test]
    async fn fixed_xai_responses_contract_maps_and_normalizes_tools() {
        let body = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-test\",\"model\":\"grok-test\"}}\n\n",
            "data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\",\"id\":\"item-test\",\"call_id\":\"call-test\",\"name\":\"records_read\"}}\n\n",
            "data: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"item-test\",\"arguments\":\"{\\\"recordId\\\":\\\"54\\\"}\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-test\",\"model\":\"grok-test\",\"status\":\"completed\",\"usage\":{\"input_tokens\":12,\"output_tokens\":3,\"input_tokens_details\":{\"cached_tokens\":2}}}}\n\n",
            "data: [DONE]\n\n"
        )
        .to_owned();
        let (endpoint, request_rx, server) = mock_server(body, true).await;
        let reference = SecretRef::parse("xai/test").expect("secret reference should parse");
        let provider = XAiProvider::for_loopback_test(
            XAiProviderConfig::new(reference.clone()),
            Arc::new(TestSecrets(reference, "synthetic-xai-key".to_owned())),
            endpoint,
        )
        .expect("loopback xAI provider should build");
        let model_request = request();
        let events = provider
            .stream(model_request.clone(), context(&model_request))
            .await
            .expect("xAI stream should start")
            .try_collect::<Vec<_>>()
            .await
            .expect("xAI stream should normalize");
        assert_eq!(
            events,
            vec![
                ProviderEvent::ResponseStarted {
                    response_id: Some("resp-test".to_owned()),
                },
                ProviderEvent::ToolCallStarted {
                    call_id: "call-test".to_owned(),
                    tool_id: "records.read".to_owned(),
                },
                ProviderEvent::ToolCallCompleted {
                    call_id: "call-test".to_owned(),
                    arguments: json!({"recordId": "54"}),
                },
                ProviderEvent::Usage {
                    input_tokens: 12,
                    output_tokens: 3,
                    cached_input_tokens: 2,
                },
                ProviderEvent::ResponseCompleted {
                    response_id: Some("resp-test".to_owned()),
                },
            ]
        );
        let request_bytes = request_rx.await.expect("request should be captured");
        let request_text = String::from_utf8(request_bytes).expect("request should be UTF-8");
        let request_lower = request_text.to_ascii_lowercase();
        assert!(request_lower.contains("authorization: bearer synthetic-xai-key"));
        assert!(request_text.contains("\"parallel_tool_calls\":true"));
        assert!(request_text.contains("\"store\":false"));
        assert!(!request_text.contains("openai-organization"));
        assert!(!request_text.contains("\"strict\""));
        server.await.expect("mock server should finish");

        let missing_zdr_body = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-test\",\"model\":\"grok-test\"}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-test\",\"model\":\"grok-test\",\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"
        )
        .to_owned();
        let (endpoint, _request_rx, server) = mock_server(missing_zdr_body, false).await;
        let reference = SecretRef::parse("xai/test").expect("secret reference should parse");
        let provider = XAiProvider::for_loopback_test(
            XAiProviderConfig::new(reference.clone()),
            Arc::new(TestSecrets(reference, "synthetic-xai-key".to_owned())),
            endpoint,
        )
        .expect("loopback xAI provider should build");
        let missing_zdr_request = request();
        assert!(matches!(
            provider
                .stream(missing_zdr_request.clone(), context(&missing_zdr_request))
                .await,
            Err(ProviderError::Rejected)
        ));
        server.await.expect("missing-ZDR server should finish");
    }

    #[test]
    fn capabilities_and_unsupported_surface_are_narrow() {
        let reference = SecretRef::parse("xai/test").expect("secret reference should parse");
        let provider = XAiProvider::for_loopback_test(
            XAiProviderConfig::new(reference.clone()),
            Arc::new(TestSecrets(reference, "synthetic-xai-key".to_owned())),
            "http://127.0.0.1:1/v1/responses".to_owned(),
        )
        .expect("loopback xAI provider should build");
        let capabilities = provider.capabilities();
        assert!(capabilities.streaming);
        assert!(capabilities.custom_tools);
        assert!(capabilities.parallel_tool_calls);
        assert!(capabilities.structured_output);
        assert!(!capabilities.image_input);
        assert!(!capabilities.file_input);
        assert!(!capabilities.web_search);
        assert!(!capabilities.code_execution);
        assert!(!capabilities.provider_retained_continuation);

        let reference = SecretRef::parse("xai/test").expect("secret reference should parse");
        let mut incompatible = XAiProviderConfig::new(reference.clone());
        incompatible.store_responses = true;
        assert!(matches!(
            XAiProvider::new(
                incompatible,
                Arc::new(TestSecrets(reference, "synthetic-xai-key".to_owned()))
            ),
            Err(ProviderError::InvalidConfiguration(_))
        ));
    }

    #[tokio::test]
    async fn wrong_model_incomplete_stream_and_unsolicited_builtin_fail_closed() {
        let wrong_model = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-test\",\"model\":\"swapped-model\"}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-test\",\"model\":\"swapped-model\",\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"
        )
        .to_owned();
        assert!(matches!(
            collect_zdr_stream(wrong_model).await,
            Err(ProviderError::Rejected)
        ));

        let incomplete =
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-test\",\"model\":\"grok-test\"}}\n\n"
                .to_owned();
        assert!(matches!(
            collect_zdr_stream(incomplete).await,
            Err(ProviderError::Unavailable)
        ));

        let unsolicited_builtin = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-test\",\"model\":\"grok-test\"}}\n\n",
            "data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"web_search_call\",\"id\":\"search-test\"}}\n\n"
        )
        .to_owned();
        assert!(matches!(
            collect_zdr_stream(unsolicited_builtin).await,
            Err(ProviderError::Rejected)
        ));
    }
}
