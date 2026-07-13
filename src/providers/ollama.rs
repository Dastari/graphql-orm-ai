//! Native deployment-authorized Ollama chat adapter.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use futures::StreamExt;
use serde_json::{Value, json};
use url::Url;

use crate::{
    AiProvider, AiProviderAttachmentRequest, AiProviderEndpointPolicy, AiProviderKindInput,
    ModelInputBlock, ModelRequest, ProviderCapabilities, ProviderError, ProviderEvent,
    ProviderEventStream, ProviderKind, ProviderRequestContext,
};

const MAXIMUM_NDJSON_LINE_BYTES: usize = 2 * 1024 * 1024;
const MAXIMUM_SCHEMA_BYTES: usize = 1024 * 1024;
const MAXIMUM_KEEP_ALIVE: Duration = Duration::from_secs(24 * 60 * 60);

/// Native Ollama adapter configuration.
///
/// The endpoint is deployment configuration, not model input. Construction
/// still requires an [`AiProviderEndpointPolicy`] decision; this value alone
/// proves neither SSRF safety nor network isolation.
#[derive(Clone)]
pub struct OllamaProviderConfig {
    /// Deployment-configured Ollama origin. Only a root `http`/`https` origin
    /// with no credentials, query, or fragment is accepted.
    pub base_url: String,
    /// Overall HTTP request/stream timeout.
    pub timeout: Duration,
    /// How long Ollama may retain the loaded model in memory after a request.
    /// This is not provider response retention.
    pub keep_alive: Duration,
}

impl OllamaProviderConfig {
    /// Creates local-provider defaults with a two-minute request timeout and
    /// five-minute model keep-alive.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            timeout: Duration::from_secs(120),
            keep_alive: Duration::from_secs(300),
        }
    }
}

impl std::fmt::Debug for OllamaProviderConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OllamaProviderConfig")
            .field("base_url", &"<deployment-configured>")
            .field("timeout", &self.timeout)
            .field("keep_alive", &self.keep_alive)
            .finish()
    }
}

/// Native Ollama `/api/chat` provider.
///
/// This initial adapter supports bounded streaming text, exact inline image
/// input, and JSON-schema output. It deliberately rejects custom tools,
/// provider built-ins, file inputs, and continuation until the runtime has a
/// provider-independent stateless conversation checkpoint; advertising a
/// native Ollama capability is not sufficient to make restart/replay safe.
pub struct OllamaProvider {
    config: OllamaProviderConfig,
    client: reqwest::Client,
    endpoint: Url,
}

impl std::fmt::Debug for OllamaProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OllamaProvider")
            .field("config", &self.config)
            .field("endpoint", &"<deployment-authorized>/api/chat")
            .finish_non_exhaustive()
    }
}

impl OllamaProvider {
    /// Builds an Ollama provider after deployment endpoint authorization.
    ///
    /// Redirects are disabled. The endpoint policy must additionally enforce
    /// permitted hosts, ports, DNS/network zones, and any loopback/container
    /// rules; this constructor cannot prove those deployment properties.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::InvalidConfiguration`] for an unsafe URL,
    /// invalid timeout/keep-alive, denied endpoint, or HTTP client failure.
    pub fn new(
        config: OllamaProviderConfig,
        endpoint_policy: Arc<dyn AiProviderEndpointPolicy>,
    ) -> Result<Self, ProviderError> {
        if config.timeout.is_zero()
            || config.timeout > Duration::from_secs(600)
            || config.keep_alive > MAXIMUM_KEEP_ALIVE
            || config.keep_alive.subsec_nanos() != 0
        {
            return Err(ProviderError::InvalidConfiguration(
                "invalid Ollama timeout or keep-alive".to_owned(),
            ));
        }
        let base_url = normalized_base_url(&config.base_url)?;
        if !endpoint_policy.authorize_endpoint(AiProviderKindInput::Ollama, base_url.as_str()) {
            return Err(ProviderError::InvalidConfiguration(
                "Ollama endpoint was not authorized".to_owned(),
            ));
        }
        let endpoint = base_url.join("api/chat").map_err(|_| {
            ProviderError::InvalidConfiguration("invalid Ollama endpoint".to_owned())
        })?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(config.timeout)
            .build()
            .map_err(|_| {
                ProviderError::InvalidConfiguration(
                    "Ollama HTTP client could not be constructed".to_owned(),
                )
            })?;
        Ok(Self {
            config,
            client,
            endpoint,
        })
    }

    fn request_body(
        &self,
        request: &ModelRequest,
        context: &ProviderRequestContext,
    ) -> Result<Value, ProviderError> {
        if request.input.is_empty() {
            return Err(ProviderError::InvalidRequest);
        }
        if request.continuation.is_some()
            || !request.tools.is_empty()
            || !request.builtin_tools.is_empty()
            || request
                .input
                .iter()
                .any(|block| matches!(block, ModelInputBlock::ToolResult { .. }))
        {
            return Err(ProviderError::Unsupported);
        }

        let mut messages = Vec::new();
        if !request.instructions.is_empty() {
            messages.push(json!({
                "role": "system",
                "content": request.instructions.join("\n\n")
            }));
        }
        let mut content = Vec::new();
        let mut images = Vec::new();
        for block in &request.input {
            match block {
                ModelInputBlock::Text { text } => content.push(text.clone()),
                ModelInputBlock::Json { value } => content.push(value.to_string()),
                ModelInputBlock::Attachment { mime, .. } => {
                    if !matches!(mime.as_str(), "image/png" | "image/jpeg" | "image/webp") {
                        return Err(ProviderError::Unsupported);
                    }
                    let attachment_request = AiProviderAttachmentRequest::try_from(block)
                        .map_err(|_| ProviderError::InvalidRequest)?;
                    let attachment = context.resolved_attachment(&attachment_request)?;
                    images
                        .push(base64::engine::general_purpose::STANDARD.encode(attachment.bytes()));
                }
                ModelInputBlock::ToolResult { .. } => return Err(ProviderError::Unsupported),
            }
        }
        let mut user_message = json!({
            "role": "user",
            "content": content.join("\n\n")
        });
        if !images.is_empty() {
            user_message["images"] = Value::Array(images.into_iter().map(Value::String).collect());
        }
        messages.push(user_message);

        let mut body = json!({
            "model": request.model,
            "messages": messages,
            "stream": true,
            "think": false,
            "keep_alive": format!("{}s", self.config.keep_alive.as_secs())
        });
        if let Some(schema) = &request.output_schema {
            if !schema.is_object()
                || serde_json::to_vec(schema)
                    .map_or(true, |encoded| encoded.len() > MAXIMUM_SCHEMA_BYTES)
            {
                return Err(ProviderError::InvalidRequest);
            }
            body["format"] = schema.clone();
        }
        if let Some(maximum_output_tokens) = request.maximum_output_tokens {
            let maximum_output_tokens = i64::try_from(maximum_output_tokens)
                .ok()
                .filter(|value| *value > 0 && *value <= i64::from(i32::MAX))
                .ok_or(ProviderError::InvalidRequest)?;
            body["options"] = json!({"num_predict": maximum_output_tokens});
        }
        Ok(body)
    }
}

#[async_trait]
impl AiProvider for OllamaProvider {
    fn provider_kind(&self) -> ProviderKind {
        ProviderKind::Ollama
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            image_input: true,
            structured_output: true,
            local: true,
            ..ProviderCapabilities::default()
        }
    }

    async fn stream(
        &self,
        request: ModelRequest,
        context: ProviderRequestContext,
    ) -> Result<ProviderEventStream, ProviderError> {
        context.validate_request(&ProviderKind::Ollama, &request)?;
        let body = self.request_body(&request, &context)?;
        let expected_model = request.model;
        let response = self
            .client
            .post(self.endpoint.clone())
            .json(&body)
            .send()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        if !response.status().is_success() {
            return Err(classify_status(response.status()));
        }
        Ok(normalized_stream(response, expected_model))
    }
}

fn normalized_stream(response: reqwest::Response, expected_model: String) -> ProviderEventStream {
    Box::pin(async_stream::try_stream! {
        let mut chunks = response.bytes_stream();
        let mut buffer = Vec::new();
        let mut started = false;
        let mut completed = false;
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.map_err(|_| ProviderError::Unavailable)?;
            buffer.extend_from_slice(&chunk);
            loop {
                let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') else {
                    if buffer.len() > MAXIMUM_NDJSON_LINE_BYTES {
                        Err(ProviderError::Rejected)?;
                    }
                    break;
                };
                let mut line = buffer.drain(..=newline).collect::<Vec<_>>();
                line.pop();
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                if line.is_empty() {
                    continue;
                }
                if line.len() > MAXIMUM_NDJSON_LINE_BYTES || completed {
                    Err(ProviderError::Rejected)?;
                }
                let value: Value = serde_json::from_slice(&line)
                    .map_err(|_| ProviderError::Rejected)?;
                for event in normalize_chunk(&value, &expected_model, &mut started, &mut completed)? {
                    yield event;
                }
            }
        }
        if !buffer.is_empty() {
            if buffer.last() == Some(&b'\r') {
                buffer.pop();
            }
            if buffer.len() > MAXIMUM_NDJSON_LINE_BYTES || completed {
                Err(ProviderError::Rejected)?;
            }
            let value: Value = serde_json::from_slice(&buffer)
                .map_err(|_| ProviderError::Rejected)?;
            for event in normalize_chunk(&value, &expected_model, &mut started, &mut completed)? {
                yield event;
            }
        }
        if !completed {
            Err(ProviderError::Unavailable)?;
        }
    })
}

fn normalize_chunk(
    value: &Value,
    expected_model: &str,
    started: &mut bool,
    completed: &mut bool,
) -> Result<Vec<ProviderEvent>, ProviderError> {
    if value.get("error").is_some() || *completed {
        return Err(ProviderError::Rejected);
    }
    let model = value
        .get("model")
        .and_then(Value::as_str)
        .ok_or(ProviderError::Rejected)?;
    if model != expected_model {
        return Err(ProviderError::Rejected);
    }
    let message = value
        .get("message")
        .and_then(Value::as_object)
        .ok_or(ProviderError::Rejected)?;
    if message.get("role").and_then(Value::as_str) != Some("assistant") {
        return Err(ProviderError::Rejected);
    }
    if message
        .get("tool_calls")
        .and_then(Value::as_array)
        .is_some_and(|calls| !calls.is_empty())
    {
        return Err(ProviderError::Unsupported);
    }
    if message
        .get("thinking")
        .is_some_and(|thinking| !thinking.is_string())
    {
        return Err(ProviderError::Rejected);
    }
    let content = message
        .get("content")
        .and_then(Value::as_str)
        .ok_or(ProviderError::Rejected)?;
    let done = value
        .get("done")
        .and_then(Value::as_bool)
        .ok_or(ProviderError::Rejected)?;
    let mut events = Vec::new();
    if !*started {
        *started = true;
        events.push(ProviderEvent::ResponseStarted { response_id: None });
    }
    if !content.is_empty() {
        events.push(ProviderEvent::TextDelta {
            text: content.to_owned(),
        });
    }
    if done {
        let input_tokens = value
            .get("prompt_eval_count")
            .and_then(Value::as_u64)
            .ok_or(ProviderError::Rejected)?;
        let output_tokens = value
            .get("eval_count")
            .and_then(Value::as_u64)
            .ok_or(ProviderError::Rejected)?;
        events.push(ProviderEvent::Usage {
            input_tokens,
            output_tokens,
            cached_input_tokens: 0,
        });
        events.push(ProviderEvent::ResponseCompleted { response_id: None });
        *completed = true;
    }
    Ok(events)
}

fn normalized_base_url(value: &str) -> Result<Url, ProviderError> {
    let mut url = Url::parse(value)
        .map_err(|_| ProviderError::InvalidConfiguration("invalid Ollama endpoint".to_owned()))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        return Err(ProviderError::InvalidConfiguration(
            "unsafe Ollama endpoint".to_owned(),
        ));
    }
    url.set_path("/");
    Ok(url)
}

fn classify_status(status: reqwest::StatusCode) -> ProviderError {
    match status.as_u16() {
        408 | 425 | 429 => ProviderError::RateLimited,
        500..=599 => ProviderError::Unavailable,
        _ => ProviderError::Rejected,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::TryStreamExt;
    use sha2::{Digest, Sha256};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    use super::*;
    use crate::{
        AiBudgetAmounts, AiBudgetReservation, AiBudgetReservationId, AiDataSourceRef,
        AiDestinationTrust, AiEgressCapability, AiEgressDecision, AiEgressManifest,
        AiResolvedProviderAttachment, AiRunId, AiScope, AiSessionId, AiSourceTrust,
        DataClassification, ModelToolDefinition,
    };

    struct ExactEndpointPolicy(String);

    impl AiProviderEndpointPolicy for ExactEndpointPolicy {
        fn authorize_endpoint(
            &self,
            provider_kind: AiProviderKindInput,
            normalized_url: &str,
        ) -> bool {
            provider_kind == AiProviderKindInput::Ollama && normalized_url == self.0
        }
    }

    fn provider(base_url: &str) -> OllamaProvider {
        OllamaProvider::new(
            OllamaProviderConfig::new(base_url),
            Arc::new(ExactEndpointPolicy(
                normalized_base_url(base_url)
                    .expect("test endpoint should normalize")
                    .to_string(),
            )),
        )
        .expect("test provider should build")
    }

    fn request(model: &str) -> ModelRequest {
        ModelRequest {
            model: model.to_owned(),
            instructions: vec!["Respond briefly.".to_owned()],
            input: vec![ModelInputBlock::Text {
                text: "synthetic hello".to_owned(),
            }],
            continuation: None,
            tools: vec![],
            builtin_tools: vec![],
            output_schema: None,
            maximum_output_tokens: Some(64),
        }
    }

    fn context(
        model: &str,
        request: &ModelRequest,
        attachment: Option<AiResolvedProviderAttachment>,
    ) -> ProviderRequestContext {
        let session_id = AiSessionId::new();
        let run_id = AiRunId::new();
        let attempt_id = uuid::Uuid::new_v4();
        let inference_manifest = manifest(
            model,
            session_id,
            run_id,
            AiEgressCapability::ModelInference,
            request,
        );
        let proof = AiEgressDecision::allow(&inference_manifest, "test", "test-user")
            .authorize(&inference_manifest)
            .expect("manifest should authorize");
        let budget = AiBudgetReservation::new_reserved(
            AiBudgetReservationId::new(),
            run_id,
            attempt_id,
            1,
            ProviderKind::Ollama,
            model,
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
            &ProviderKind::Ollama,
            model,
            64,
            time::OffsetDateTime::now_utc(),
        )
        .expect("budget should authorize");
        let mut context = ProviderRequestContext::new(
            session_id,
            run_id,
            "test",
            budget,
            inference_manifest,
            proof,
        )
        .expect("context should validate");
        if let Some(attachment) = attachment {
            let image_manifest = manifest(
                model,
                session_id,
                run_id,
                AiEgressCapability::ImageAnalysis,
                request,
            );
            let image_proof = AiEgressDecision::allow(&image_manifest, "test", "test-user")
                .authorize(&image_manifest)
                .expect("image manifest should authorize");
            context = context
                .with_authorized_transfer(image_manifest, image_proof)
                .expect("image transfer should bind")
                .with_resolved_attachments(request, vec![attachment])
                .expect("image bytes should bind");
        }
        context
    }

    fn manifest(
        model: &str,
        session_id: AiSessionId,
        run_id: AiRunId,
        capability: AiEgressCapability,
        request: &ModelRequest,
    ) -> AiEgressManifest {
        AiEgressManifest {
            provider_profile_id: "profile-local".to_owned(),
            provider_kind: "ollama".to_owned(),
            model: model.to_owned(),
            destination: "local-model-boundary".to_owned(),
            destination_trust: AiDestinationTrust::Local,
            capability,
            scope: AiScope::new("project", "test"),
            session_id: Some(session_id),
            run_id: Some(run_id),
            sources: request
                .input
                .iter()
                .filter_map(|block| {
                    block
                        .attachment_egress_reference()
                        .map(|reference| AiDataSourceRef {
                            kind: "attachment".to_owned(),
                            reference,
                            classification: DataClassification::Public,
                            trust: AiSourceTrust::UserProvided,
                        })
                })
                .chain(std::iter::once(AiDataSourceRef {
                    kind: "message".to_owned(),
                    reference: "synthetic".to_owned(),
                    classification: DataClassification::Public,
                    trust: AiSourceTrust::UserProvided,
                }))
                .collect(),
            estimated_bytes: 1_000_000,
            estimated_tokens: 1_000,
            attachment_count: request
                .input
                .iter()
                .filter(|block| matches!(block, ModelInputBlock::Attachment { .. }))
                .count() as u32,
            purpose: "test".to_owned(),
            retention: "none".to_owned(),
            residency: Some("local".to_owned()),
            policy_version: "test-v1".to_owned(),
            consent_reference: None,
        }
    }

    async fn mock_server(
        response_body: &'static str,
    ) -> (
        String,
        oneshot::Receiver<Vec<u8>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener should bind");
        let address = listener.local_addr().expect("listener should have address");
        let (sender, receiver) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("request should connect");
            let mut request = Vec::new();
            let mut temporary = [0_u8; 4_096];
            let expected_length = loop {
                let read = socket
                    .read(&mut temporary)
                    .await
                    .expect("request should read");
                assert!(read > 0, "request should contain headers");
                request.extend_from_slice(&temporary[..read]);
                if let Some(header_end) = request.windows(4).position(|item| item == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&request[..header_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .expect("request should have content length");
                    break (header_end + 4, content_length);
                }
            };
            while request.len() < expected_length.0 + expected_length.1 {
                let read = socket
                    .read(&mut temporary)
                    .await
                    .expect("request body should read");
                assert!(read > 0, "request body should complete");
                request.extend_from_slice(&temporary[..read]);
            }
            sender
                .send(request)
                .expect("request receiver should remain");
            let headers = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                response_body.len()
            );
            socket
                .write_all(headers.as_bytes())
                .await
                .expect("headers should write");
            for part in response_body.as_bytes().chunks(17) {
                if socket.write_all(part).await.is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        });
        (format!("http://{address}/"), receiver, task)
    }

    #[test]
    fn endpoint_and_capability_contract_is_explicit_and_redacted() {
        let provider = provider("http://127.0.0.1:11434");
        assert_eq!(provider.provider_kind(), ProviderKind::Ollama);
        let capabilities = provider.capabilities();
        assert!(capabilities.streaming);
        assert!(capabilities.image_input);
        assert!(capabilities.structured_output);
        assert!(capabilities.local);
        assert!(!capabilities.custom_tools);
        assert!(!format!("{provider:?}").contains("127.0.0.1"));

        assert!(matches!(
            OllamaProvider::new(
                OllamaProviderConfig::new("http://127.0.0.1:11434/private"),
                Arc::new(ExactEndpointPolicy("unused".to_owned())),
            ),
            Err(ProviderError::InvalidConfiguration(_))
        ));
        for unsafe_endpoint in [
            "ftp://127.0.0.1:11434",
            "http://user@127.0.0.1:11434",
            "http://127.0.0.1:11434?profile=other",
            "http://127.0.0.1:11434#other",
        ] {
            assert!(matches!(
                OllamaProvider::new(
                    OllamaProviderConfig::new(unsafe_endpoint),
                    Arc::new(ExactEndpointPolicy("unused".to_owned())),
                ),
                Err(ProviderError::InvalidConfiguration(_))
            ));
        }
        assert!(matches!(
            OllamaProvider::new(
                OllamaProviderConfig::new("http://127.0.0.1:11434"),
                Arc::new(ExactEndpointPolicy("http://127.0.0.1:9999/".to_owned())),
            ),
            Err(ProviderError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn request_mapping_supports_schema_and_exact_images_but_rejects_tools() {
        let provider = provider("http://127.0.0.1:11434");
        let image_bytes = b"synthetic-png".to_vec();
        let image_block = ModelInputBlock::Attachment {
            attachment_id: uuid::Uuid::new_v4().to_string(),
            mime: "image/png".to_owned(),
            byte_count: image_bytes.len() as u64,
            sha256: hex::encode(Sha256::digest(&image_bytes)),
        };
        let mut model_request = request("local-test");
        model_request.input.push(image_block.clone());
        model_request.output_schema = Some(json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {"answer": {"type": "string"}},
            "required": ["answer"]
        }));
        let resolved = AiResolvedProviderAttachment::new(
            AiProviderAttachmentRequest::try_from(&image_block)
                .expect("image attachment should parse"),
            "image.png",
            image_bytes.clone(),
        )
        .expect("image bytes should bind");
        let provider_context = context("local-test", &model_request, Some(resolved));
        let body = provider
            .request_body(&model_request, &provider_context)
            .expect("supported request should map");
        assert_eq!(body["think"], false);
        assert_eq!(body["stream"], true);
        assert_eq!(body["keep_alive"], "300s");
        assert_eq!(body["options"]["num_predict"], 64);
        assert_eq!(body["format"]["additionalProperties"], false);
        assert_eq!(
            body["messages"][1]["images"][0],
            base64::engine::general_purpose::STANDARD.encode(image_bytes)
        );

        let missing_image_proof = context("local-test", &model_request, None);
        assert!(matches!(
            provider.request_body(&model_request, &missing_image_proof),
            Err(ProviderError::EgressDenied)
        ));

        model_request.tools.push(ModelToolDefinition {
            tool_id: "records.read".to_owned(),
            provider_name: "records_read".to_owned(),
            fingerprint: "fingerprint".to_owned(),
            description: "Read records".to_owned(),
            parameters: json!({"type": "object"}),
            strict: true,
        });
        assert!(matches!(
            provider.request_body(&model_request, &provider_context),
            Err(ProviderError::Unsupported)
        ));
    }

    #[tokio::test]
    async fn ndjson_stream_rejects_wrong_model_and_truncation() {
        let wrong_model = concat!(
            "{\"model\":\"swapped-model\",\"message\":{\"role\":\"assistant\",\"content\":\"unsafe\"},\"done\":false}\n",
            "{\"model\":\"swapped-model\",\"message\":{\"role\":\"assistant\",\"content\":\"\"},\"done\":true,\"prompt_eval_count\":1,\"eval_count\":1}\n"
        );
        let (base_url, request_receiver, server) = mock_server(wrong_model).await;
        let wrong_model_provider = provider(&base_url);
        let model_request = request("local-test");
        let error = wrong_model_provider
            .stream(
                model_request.clone(),
                context("local-test", &model_request, None),
            )
            .await
            .expect("HTTP stream should start")
            .try_collect::<Vec<_>>()
            .await
            .expect_err("a model-swapped stream must fail");
        assert!(matches!(error, ProviderError::Rejected));
        request_receiver.await.expect("request should be captured");
        server.await.expect("server should finish");

        let truncated = "{\"model\":\"local-test\",\"message\":{\"role\":\"assistant\",\"content\":\"partial\"},\"done\":false}\n";
        let (base_url, request_receiver, server) = mock_server(truncated).await;
        let provider = provider(&base_url);
        let error = provider
            .stream(
                model_request.clone(),
                context("local-test", &model_request, None),
            )
            .await
            .expect("HTTP stream should start")
            .try_collect::<Vec<_>>()
            .await
            .expect_err("a stream without a terminal event must fail");
        assert!(matches!(error, ProviderError::Unavailable));
        request_receiver.await.expect("request should be captured");
        server.await.expect("server should finish");
    }

    #[tokio::test]
    async fn ndjson_stream_is_bounded_normalized_and_request_is_native_chat() {
        let ndjson = concat!(
            "{\"model\":\"local-test\",\"message\":{\"role\":\"assistant\",\"content\":\"hel\"},\"done\":false}\n",
            "{\"model\":\"local-test\",\"message\":{\"role\":\"assistant\",\"content\":\"lo\"},\"done\":false}\r\n",
            "{\"model\":\"local-test\",\"message\":{\"role\":\"assistant\",\"content\":\"\"},\"done\":true,\"done_reason\":\"stop\",\"prompt_eval_count\":3,\"eval_count\":2}\n"
        );
        let (base_url, request_receiver, server) = mock_server(ndjson).await;
        let provider = provider(&base_url);
        let model_request = request("local-test");
        let events = provider
            .stream(
                model_request.clone(),
                context("local-test", &model_request, None),
            )
            .await
            .expect("stream should start")
            .try_collect::<Vec<_>>()
            .await
            .expect("stream should normalize");
        let raw_request = request_receiver.await.expect("request should be captured");
        server.await.expect("server should finish");
        let raw_request = String::from_utf8(raw_request).expect("request should be UTF-8");
        assert!(raw_request.starts_with("POST /api/chat HTTP/1.1\r\n"));
        assert!(raw_request.contains("\"think\":false"));
        assert!(!raw_request.contains("synthetic-test-secret"));
        assert_eq!(
            events,
            vec![
                ProviderEvent::ResponseStarted { response_id: None },
                ProviderEvent::TextDelta {
                    text: "hel".to_owned()
                },
                ProviderEvent::TextDelta {
                    text: "lo".to_owned()
                },
                ProviderEvent::Usage {
                    input_tokens: 3,
                    output_tokens: 2,
                    cached_input_tokens: 0
                },
                ProviderEvent::ResponseCompleted { response_id: None }
            ]
        );
    }
}
