//! Native OpenAI Responses API adapter.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use futures::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use secrecy::ExposeSecret;
use serde_json::{Value, json};

use crate::{
    AiProvider, AiProviderAttachmentRequest, AiSecretStore, ModelBuiltinTool, ModelContinuation,
    ModelInputBlock, ModelRequest, ProviderCapabilities, ProviderError, ProviderEvent,
    ProviderEventStream, ProviderKind, ProviderRequestContext, SecretRef,
};

const OPENAI_RESPONSES_ENDPOINT: &str = "https://api.openai.com/v1/responses";
const MAXIMUM_SSE_EVENT_BYTES: usize = 2 * 1024 * 1024;

/// Native OpenAI adapter configuration. Credential plaintext is never stored
/// in this structure.
#[derive(Clone, Debug)]
pub struct OpenAiProviderConfig {
    /// Secret-store reference resolved immediately before each request.
    pub credential: SecretRef,
    /// Optional OpenAI organization header.
    pub organization: Option<String>,
    /// Optional OpenAI project header.
    pub project: Option<String>,
    /// Overall HTTP request/stream timeout.
    pub timeout: Duration,
    /// Whether OpenAI may retain the response object. Defaults to false so the
    /// local session remains canonical.
    pub store_responses: bool,
}

impl OpenAiProviderConfig {
    /// Creates secure defaults for the native Responses endpoint.
    pub fn new(credential: SecretRef) -> Self {
        Self {
            credential,
            organization: None,
            project: None,
            timeout: Duration::from_secs(120),
            store_responses: false,
        }
    }
}

/// Native OpenAI Responses API provider.
pub struct OpenAiProvider {
    config: OpenAiProviderConfig,
    secrets: Arc<dyn AiSecretStore>,
    client: reqwest::Client,
    endpoint: String,
}

impl std::fmt::Debug for OpenAiProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenAiProvider")
            .field("config", &self.config)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl OpenAiProvider {
    /// Builds a provider fixed to OpenAI's official HTTPS endpoint with
    /// redirects disabled.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::InvalidConfiguration`] for invalid safe header
    /// metadata or an HTTP client construction failure.
    pub fn new(
        config: OpenAiProviderConfig,
        secrets: Arc<dyn AiSecretStore>,
    ) -> Result<Self, ProviderError> {
        Self::build(config, secrets, OPENAI_RESPONSES_ENDPOINT.to_owned())
    }

    fn build(
        config: OpenAiProviderConfig,
        secrets: Arc<dyn AiSecretStore>,
        endpoint: String,
    ) -> Result<Self, ProviderError> {
        validate_optional_header(config.organization.as_deref())?;
        validate_optional_header(config.project.as_deref())?;
        if config.timeout.is_zero() || config.timeout > Duration::from_secs(600) {
            return Err(ProviderError::InvalidConfiguration(
                "OpenAI timeout must be between one millisecond and ten minutes".to_owned(),
            ));
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(config.timeout)
            .build()
            .map_err(|_| {
                ProviderError::InvalidConfiguration(
                    "OpenAI HTTP client could not be constructed".to_owned(),
                )
            })?;
        Ok(Self {
            config,
            secrets,
            client,
            endpoint,
        })
    }

    #[cfg(test)]
    fn for_loopback_test(
        config: OpenAiProviderConfig,
        secrets: Arc<dyn AiSecretStore>,
        endpoint: String,
    ) -> Result<Self, ProviderError> {
        if !endpoint.starts_with("http://127.0.0.1:") {
            return Err(ProviderError::InvalidConfiguration(
                "test endpoint must use IPv4 loopback".to_owned(),
            ));
        }
        Self::build(config, secrets, endpoint)
    }

    fn request_headers(&self) -> Result<HeaderMap, ProviderError> {
        let mut headers = HeaderMap::new();
        insert_optional_header(
            &mut headers,
            HeaderName::from_static("openai-organization"),
            self.config.organization.as_deref(),
        )?;
        insert_optional_header(
            &mut headers,
            HeaderName::from_static("openai-project"),
            self.config.project.as_deref(),
        )?;
        Ok(headers)
    }

    fn request_body(
        &self,
        request: &ModelRequest,
        context: &ProviderRequestContext,
    ) -> Result<Value, ProviderError> {
        if request.input.is_empty() {
            return Err(ProviderError::InvalidRequest);
        }
        let mut content = Vec::with_capacity(request.input.len());
        let mut tool_outputs = Vec::new();
        let mut direct_input_bytes = 0_u64;
        for block in &request.input {
            match block {
                ModelInputBlock::Text { text } => {
                    content.push(json!({"type": "input_text", "text": text}));
                }
                ModelInputBlock::Json { value } => {
                    content.push(json!({
                        "type": "input_text",
                        "text": value.to_string()
                    }));
                }
                ModelInputBlock::Attachment { mime, .. } => {
                    let attachment_request = AiProviderAttachmentRequest::try_from(block)
                        .map_err(|_| ProviderError::InvalidRequest)?;
                    let attachment = context.resolved_attachment(&attachment_request)?;
                    let is_supported_image = matches!(
                        mime.as_str(),
                        "image/png" | "image/jpeg" | "image/webp" | "image/gif"
                    );
                    if mime.starts_with("image/") && !is_supported_image {
                        return Err(ProviderError::Unsupported);
                    }
                    if !mime.starts_with("image/")
                        && attachment_request.byte_count() >= 50 * 1024 * 1024
                    {
                        return Err(ProviderError::Unsupported);
                    }
                    direct_input_bytes = direct_input_bytes
                        .checked_add(attachment_request.byte_count())
                        .ok_or(ProviderError::InvalidRequest)?;
                    if direct_input_bytes > 50 * 1024 * 1024 {
                        return Err(ProviderError::Unsupported);
                    }
                    let encoded =
                        base64::engine::general_purpose::STANDARD.encode(attachment.bytes());
                    let data_url = format!("data:{mime};base64,{encoded}");
                    if is_supported_image {
                        content.push(json!({
                            "type": "input_image",
                            "image_url": data_url,
                            "detail": "auto"
                        }));
                    } else {
                        content.push(json!({
                            "type": "input_file",
                            "filename": attachment.safe_filename(),
                            "file_data": data_url
                        }));
                    }
                }
                ModelInputBlock::ToolResult {
                    call_id, output, ..
                } => {
                    tool_outputs.push(json!({
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": output.to_string()
                    }));
                }
            }
        }

        let mut tools = Vec::with_capacity(request.tools.len() + request.builtin_tools.len());
        for tool in &request.tools {
            tools.push(json!({
                "type": "function",
                "name": tool.provider_name,
                "description": tool.description,
                "parameters": tool.parameters,
                "strict": tool.strict
            }));
        }
        for builtin in &request.builtin_tools {
            tools.push(openai_builtin(builtin)?);
        }

        let mut input = Vec::with_capacity(usize::from(!content.is_empty()) + tool_outputs.len());
        if !content.is_empty() {
            input.push(json!({"role": "user", "content": content}));
        }
        input.extend(tool_outputs);
        let mut body = json!({
            "model": request.model,
            "input": input,
            "stream": true,
            "store": self.config.store_responses,
            "tools": tools,
            "parallel_tool_calls": false
        });
        if let Some(ModelContinuation::ProviderResponse { response_id }) = &request.continuation {
            if !self.config.store_responses {
                return Err(ProviderError::Unsupported);
            }
            body["previous_response_id"] = Value::String(response_id.clone());
        }
        if !request.instructions.is_empty() {
            body["instructions"] = Value::String(request.instructions.join("\n\n"));
        }
        if let Some(maximum_output_tokens) = request.maximum_output_tokens {
            body["max_output_tokens"] = Value::from(maximum_output_tokens);
        }
        if let Some(schema) = &request.output_schema {
            body["text"] = json!({
                "format": {
                    "type": "json_schema",
                    "name": "graphql_orm_ai_response",
                    "strict": true,
                    "schema": schema
                }
            });
        }
        Ok(body)
    }
}

#[async_trait]
impl AiProvider for OpenAiProvider {
    fn provider_kind(&self) -> ProviderKind {
        ProviderKind::OpenAi
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            image_input: true,
            file_input: true,
            custom_tools: true,
            parallel_tool_calls: false,
            structured_output: true,
            web_search: true,
            file_search: true,
            code_execution: true,
            image_generation: true,
            embeddings: false,
            background: false,
            local: false,
            maximum_context_tokens: None,
            maximum_output_tokens: None,
        }
    }

    async fn stream(
        &self,
        request: ModelRequest,
        context: ProviderRequestContext,
    ) -> Result<ProviderEventStream, ProviderError> {
        context.validate_request(&ProviderKind::OpenAi, &request)?;
        if self.config.store_responses
            && !context.permits_retained_response(&ProviderKind::OpenAi, &request)
        {
            return Err(ProviderError::EgressDenied);
        }
        let body = self.request_body(&request, &context)?;
        let secret = self
            .secrets
            .resolve(&self.config.credential)
            .await
            .map_err(|_| ProviderError::CredentialUnavailable)?;
        let response = self
            .client
            .post(&self.endpoint)
            .headers(self.request_headers()?)
            .bearer_auth(secret.expose_secret())
            .json(&body)
            .send()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        let status = response.status();
        if let Some(error) = openai_http_error(status) {
            return Err(error);
        }

        let mut bytes = response.bytes_stream();
        let tool_ids = request
            .tools
            .iter()
            .map(|tool| (tool.provider_name.clone(), tool.tool_id.clone()))
            .collect::<BTreeMap<_, _>>();
        let output = async_stream::try_stream! {
            let mut decoder = SseDecoder::default();
            let mut normalizer = OpenAiEventNormalizer::new(tool_ids);
            while let Some(chunk) = bytes.next().await {
                let chunk = chunk.map_err(|_| ProviderError::Unavailable)?;
                for payload in decoder.push(&chunk)? {
                    let value: Value = serde_json::from_str(&payload)
                        .map_err(|_| ProviderError::Rejected)?;
                    for event in normalizer.normalize(&value)? {
                        yield event;
                    }
                }
            }
            decoder.finish()?;
        };
        Ok(Box::pin(output))
    }
}

fn openai_http_error(status: reqwest::StatusCode) -> Option<ProviderError> {
    if status.is_success() {
        None
    } else if status == reqwest::StatusCode::UNAUTHORIZED {
        Some(ProviderError::CredentialUnavailable)
    } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        Some(ProviderError::RateLimited)
    } else if status.is_server_error() {
        Some(ProviderError::Unavailable)
    } else {
        Some(ProviderError::Rejected)
    }
}

fn validate_optional_header(value: Option<&str>) -> Result<(), ProviderError> {
    if let Some(value) = value
        && (value.is_empty() || value.len() > 200 || HeaderValue::from_str(value).is_err())
    {
        return Err(ProviderError::InvalidConfiguration(
            "OpenAI organization/project header is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn insert_optional_header(
    headers: &mut HeaderMap,
    name: HeaderName,
    value: Option<&str>,
) -> Result<(), ProviderError> {
    if let Some(value) = value {
        let value = HeaderValue::from_str(value).map_err(|_| {
            ProviderError::InvalidConfiguration(
                "OpenAI organization/project header is invalid".to_owned(),
            )
        })?;
        headers.insert(name, value);
    }
    Ok(())
}

fn openai_builtin(tool: &ModelBuiltinTool) -> Result<Value, ProviderError> {
    match tool {
        ModelBuiltinTool::WebSearch { allowed_domains } => {
            if allowed_domains.len() > 100
                || allowed_domains
                    .iter()
                    .any(|domain| domain.is_empty() || domain.len() > 253)
            {
                return Err(ProviderError::InvalidRequest);
            }
            if allowed_domains.is_empty() {
                Ok(json!({"type": "web_search"}))
            } else {
                Ok(json!({
                    "type": "web_search",
                    "filters": {"allowed_domains": allowed_domains}
                }))
            }
        }
        ModelBuiltinTool::FileSearch {
            store_ids,
            maximum_results,
        } => {
            if store_ids.is_empty()
                || store_ids.len() > 20
                || store_ids.iter().any(|id| id.is_empty() || id.len() > 200)
                || maximum_results.is_some_and(|value| value == 0 || value > 50)
            {
                return Err(ProviderError::InvalidRequest);
            }
            let mut value = json!({
                "type": "file_search",
                "vector_store_ids": store_ids
            });
            if let Some(maximum_results) = maximum_results {
                value["max_num_results"] = Value::from(*maximum_results);
            }
            Ok(value)
        }
        ModelBuiltinTool::CodeInterpreter => Ok(json!({
            "type": "code_interpreter",
            "container": {"type": "auto"}
        })),
        ModelBuiltinTool::ImageGeneration => Ok(json!({"type": "image_generation"})),
    }
}

#[derive(Default)]
struct SseDecoder {
    buffer: Vec<u8>,
}

impl SseDecoder {
    fn push(&mut self, bytes: &[u8]) -> Result<Vec<String>, ProviderError> {
        self.buffer.extend_from_slice(bytes);
        if self.buffer.len() > MAXIMUM_SSE_EVENT_BYTES {
            return Err(ProviderError::Rejected);
        }
        let mut payloads = Vec::new();
        while let Some((position, delimiter_length)) = find_sse_delimiter(&self.buffer) {
            let frame = self.buffer.drain(..position).collect::<Vec<_>>();
            self.buffer.drain(..delimiter_length);
            if let Some(payload) = decode_sse_frame(&frame)? {
                payloads.push(payload);
            }
        }
        Ok(payloads)
    }

    fn finish(&self) -> Result<(), ProviderError> {
        if self.buffer.iter().all(u8::is_ascii_whitespace) {
            Ok(())
        } else {
            Err(ProviderError::Unavailable)
        }
    }
}

fn find_sse_delimiter(bytes: &[u8]) -> Option<(usize, usize)> {
    bytes
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|position| (position, 2))
        .or_else(|| {
            bytes
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|position| (position, 4))
        })
}

fn decode_sse_frame(frame: &[u8]) -> Result<Option<String>, ProviderError> {
    let frame = std::str::from_utf8(frame).map_err(|_| ProviderError::Rejected)?;
    let mut data = String::new();
    for line in frame.lines() {
        if let Some(value) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(value.strip_prefix(' ').unwrap_or(value));
        }
    }
    if data.is_empty() || data == "[DONE]" {
        Ok(None)
    } else {
        Ok(Some(data))
    }
}

#[derive(Clone, Debug)]
struct FunctionCallState {
    call_id: String,
}

struct OpenAiEventNormalizer {
    tool_ids: BTreeMap<String, String>,
    function_calls: BTreeMap<String, FunctionCallState>,
    builtin_calls: BTreeMap<String, (String, String)>,
    completed_calls: BTreeSet<String>,
}

impl OpenAiEventNormalizer {
    fn new(tool_ids: BTreeMap<String, String>) -> Self {
        Self {
            tool_ids,
            function_calls: BTreeMap::new(),
            builtin_calls: BTreeMap::new(),
            completed_calls: BTreeSet::new(),
        }
    }

    fn normalize(&mut self, event: &Value) -> Result<Vec<ProviderEvent>, ProviderError> {
        let event_type = event
            .get("type")
            .and_then(Value::as_str)
            .ok_or(ProviderError::Rejected)?;
        match event_type {
            "response.created" => Ok(vec![ProviderEvent::ResponseStarted {
                response_id: event
                    .pointer("/response/id")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            }]),
            "response.output_text.delta" => Ok(vec![ProviderEvent::TextDelta {
                text: required_string(event, "delta")?,
            }]),
            "response.reasoning_summary_text.delta" => {
                Ok(vec![ProviderEvent::ReasoningSummaryDelta {
                    text: required_string(event, "delta")?,
                }])
            }
            "response.output_item.added" => self.output_item_added(event),
            "response.function_call_arguments.delta" => {
                let item_id = required_string(event, "item_id")?;
                let state = self
                    .function_calls
                    .get(&item_id)
                    .ok_or(ProviderError::Rejected)?;
                Ok(vec![ProviderEvent::ToolArgumentsDelta {
                    call_id: state.call_id.clone(),
                    delta: required_string(event, "delta")?,
                }])
            }
            "response.function_call_arguments.done" => {
                let item_id = required_string(event, "item_id")?;
                let arguments = required_string(event, "arguments")?;
                self.complete_function(&item_id, &arguments)
            }
            "response.output_item.done" => self.output_item_done(event),
            "response.output_text.annotation.added" => {
                let annotation = event.get("annotation").ok_or(ProviderError::Rejected)?;
                if annotation.get("type").and_then(Value::as_str) == Some("url_citation") {
                    Ok(vec![ProviderEvent::Citation {
                        source: required_string(annotation, "url")?,
                        title: annotation
                            .get("title")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                    }])
                } else {
                    Ok(vec![ProviderEvent::Unknown {
                        event_type: event_type.to_owned(),
                    }])
                }
            }
            "response.web_search_call.completed"
            | "response.file_search_call.completed"
            | "response.code_interpreter_call.completed"
            | "response.image_generation_call.completed" => self.complete_builtin(event),
            "response.completed" => {
                let response = event.get("response").ok_or(ProviderError::Rejected)?;
                let mut events = Vec::with_capacity(2);
                if let Some(usage) = response.get("usage") {
                    events.push(ProviderEvent::Usage {
                        input_tokens: optional_u64(usage, "input_tokens"),
                        output_tokens: optional_u64(usage, "output_tokens"),
                        cached_input_tokens: usage
                            .pointer("/input_tokens_details/cached_tokens")
                            .and_then(Value::as_u64)
                            .unwrap_or(0),
                    });
                }
                events.push(ProviderEvent::ResponseCompleted {
                    response_id: response
                        .get("id")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                });
                Ok(events)
            }
            "response.failed" | "error" => Err(openai_stream_error(event)),
            _ => Ok(vec![ProviderEvent::Unknown {
                event_type: event_type.to_owned(),
            }]),
        }
    }

    fn output_item_added(&mut self, event: &Value) -> Result<Vec<ProviderEvent>, ProviderError> {
        let item = event.get("item").ok_or(ProviderError::Rejected)?;
        let item_type = required_string(item, "type")?;
        let item_id = required_string(item, "id")?;
        if item_type == "function_call" {
            let provider_name = required_string(item, "name")?;
            let tool_id = self
                .tool_ids
                .get(&provider_name)
                .ok_or(ProviderError::Rejected)?
                .clone();
            let call_id = required_string(item, "call_id")?;
            self.function_calls.insert(
                item_id,
                FunctionCallState {
                    call_id: call_id.clone(),
                },
            );
            return Ok(vec![ProviderEvent::ToolCallStarted { call_id, tool_id }]);
        }
        if let Some(kind) = builtin_kind(&item_type) {
            let call_id = item
                .get("call_id")
                .or_else(|| item.get("id"))
                .and_then(Value::as_str)
                .ok_or(ProviderError::Rejected)?
                .to_owned();
            self.builtin_calls
                .insert(item_id, (call_id.clone(), kind.to_owned()));
            return Ok(vec![ProviderEvent::BuiltinToolStarted {
                call_id,
                kind: kind.to_owned(),
            }]);
        }
        Ok(Vec::new())
    }

    fn output_item_done(&mut self, event: &Value) -> Result<Vec<ProviderEvent>, ProviderError> {
        let item = event.get("item").ok_or(ProviderError::Rejected)?;
        match item.get("type").and_then(Value::as_str) {
            Some("function_call") => {
                let item_id = required_string(item, "id")?;
                let arguments = required_string(item, "arguments")?;
                self.complete_function(&item_id, &arguments)
            }
            Some(item_type) if builtin_kind(item_type).is_some() => {
                self.complete_builtin_item(item)
            }
            _ => Ok(Vec::new()),
        }
    }

    fn complete_function(
        &mut self,
        item_id: &str,
        arguments: &str,
    ) -> Result<Vec<ProviderEvent>, ProviderError> {
        let state = self
            .function_calls
            .get(item_id)
            .ok_or(ProviderError::Rejected)?;
        if !self.completed_calls.insert(state.call_id.clone()) {
            return Ok(Vec::new());
        }
        let arguments = serde_json::from_str(arguments).map_err(|_| ProviderError::Rejected)?;
        Ok(vec![ProviderEvent::ToolCallCompleted {
            call_id: state.call_id.clone(),
            arguments,
        }])
    }

    fn complete_builtin(&mut self, event: &Value) -> Result<Vec<ProviderEvent>, ProviderError> {
        let item_id = event
            .get("item_id")
            .or_else(|| event.get("id"))
            .and_then(Value::as_str)
            .ok_or(ProviderError::Rejected)?;
        self.emit_builtin_completion(item_id)
    }

    fn complete_builtin_item(&mut self, item: &Value) -> Result<Vec<ProviderEvent>, ProviderError> {
        let item_id = required_string(item, "id")?;
        self.emit_builtin_completion(&item_id)
    }

    fn emit_builtin_completion(
        &mut self,
        item_id: &str,
    ) -> Result<Vec<ProviderEvent>, ProviderError> {
        let (call_id, kind) = self
            .builtin_calls
            .get(item_id)
            .ok_or(ProviderError::Rejected)?;
        if !self.completed_calls.insert(call_id.clone()) {
            return Ok(Vec::new());
        }
        Ok(vec![ProviderEvent::BuiltinToolCompleted {
            call_id: call_id.clone(),
            result: json!({"kind": kind, "status": "completed"}),
        }])
    }
}

fn required_string(value: &Value, field: &str) -> Result<String, ProviderError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(ProviderError::Rejected)
}

fn optional_u64(value: &Value, field: &str) -> u64 {
    value.get(field).and_then(Value::as_u64).unwrap_or(0)
}

fn openai_stream_error(event: &Value) -> ProviderError {
    match event
        .pointer("/response/error/code")
        .or_else(|| event.pointer("/error/code"))
        .or_else(|| event.get("code"))
        .and_then(Value::as_str)
    {
        Some("rate_limit_exceeded" | "insufficient_quota") => ProviderError::RateLimited,
        _ => ProviderError::Rejected,
    }
}

fn builtin_kind(item_type: &str) -> Option<&'static str> {
    match item_type {
        "web_search_call" => Some("web_search"),
        "file_search_call" => Some("file_search"),
        "code_interpreter_call" => Some("code_interpreter"),
        "image_generation_call" => Some("image_generation"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::TryStreamExt;
    use secrecy::SecretString;
    use sha2::Digest;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::{
        AiBudgetAmounts, AiBudgetReservation, AiBudgetReservationId, AiDataSourceRef,
        AiDestinationTrust, AiEgressCapability, AiEgressDecision, AiEgressManifest, AiRunId,
        AiScope, AiSessionId, AiSourceTrust, DataClassification, SecretError,
    };

    struct TestSecrets(SecretRef, String);

    struct LiveFileSecrets(SecretRef, PathBuf);

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

    #[async_trait]
    impl AiSecretStore for LiveFileSecrets {
        async fn resolve(&self, reference: &SecretRef) -> Result<SecretString, SecretError> {
            if reference != &self.0 {
                return Err(SecretError::Unavailable);
            }
            let value = tokio::fs::read_to_string(&self.1)
                .await
                .map_err(|_| SecretError::Unavailable)?;
            let value = value.trim();
            if !value.starts_with("sk-")
                || value.len() > 512
                || value.bytes().any(|byte| byte.is_ascii_whitespace())
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            {
                return Err(SecretError::Unavailable);
            }
            Ok(SecretString::from(value.to_owned()))
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

    async fn mock_server(body: &'static str) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener should bind");
        let address = listener.local_addr().expect("listener should have address");
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("request should connect");
            let mut request = vec![0_u8; 32 * 1024];
            let _ = socket
                .read(&mut request)
                .await
                .expect("request should read");
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
        (format!("http://{address}/v1/responses"), task)
    }

    fn context(model: &str, estimated_bytes: u64) -> ProviderRequestContext {
        let session_id = AiSessionId::new();
        let run_id = AiRunId::new();
        let attempt_id = uuid::Uuid::new_v4();
        let manifest = AiEgressManifest {
            provider_profile_id: "profile-1".to_owned(),
            provider_kind: "openai".to_owned(),
            model: model.to_owned(),
            destination: "openai".to_owned(),
            destination_trust: AiDestinationTrust::ManagedProvider,
            capability: AiEgressCapability::ModelInference,
            scope: AiScope::new("project", "test"),
            session_id: Some(session_id),
            run_id: Some(run_id),
            sources: vec![AiDataSourceRef {
                kind: "message".to_owned(),
                reference: "synthetic".to_owned(),
                classification: DataClassification::Public,
                trust: AiSourceTrust::UserProvided,
            }],
            estimated_bytes,
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
            ProviderKind::OpenAi,
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
            &ProviderKind::OpenAi,
            model,
            1_000,
            time::OffsetDateTime::now_utc(),
        )
        .expect("budget should authorize");
        ProviderRequestContext::new(session_id, run_id, "test", budget, manifest, proof)
            .expect("context should validate")
    }

    #[test]
    fn stateful_tool_continuation_requires_explicit_response_storage() {
        let reference = SecretRef::parse("openai/continuation-test")
            .expect("test secret reference should parse");
        let secrets: Arc<dyn AiSecretStore> =
            Arc::new(TestSecrets(reference.clone(), "not-a-real-key".to_owned()));
        let request = ModelRequest {
            model: "test-model".to_owned(),
            instructions: vec!["Continue after the exact tool output".to_owned()],
            input: vec![ModelInputBlock::ToolResult {
                call_id: "call-1".to_owned(),
                tool_id: "records.read".to_owned(),
                output: json!({"data": {"recordId": "54"}, "errorCodes": []}),
            }],
            continuation: Some(ModelContinuation::ProviderResponse {
                response_id: "resp-1".to_owned(),
            }),
            tools: Vec::new(),
            builtin_tools: Vec::new(),
            output_schema: None,
            maximum_output_tokens: Some(64),
        };

        let provider = OpenAiProvider::new(
            OpenAiProviderConfig::new(reference.clone()),
            secrets.clone(),
        )
        .expect("default provider should build");
        let provider_context = context("test-model", 1_000);
        assert!(matches!(
            provider.request_body(&request, &provider_context),
            Err(ProviderError::Unsupported)
        ));

        let mut config = OpenAiProviderConfig::new(reference);
        config.store_responses = true;
        let provider =
            OpenAiProvider::new(config, secrets).expect("retained-response provider should build");
        let body = provider
            .request_body(&request, &provider_context)
            .expect("explicitly retained continuation should map");
        assert_eq!(body["previous_response_id"], "resp-1");
        assert_eq!(body["input"][0]["type"], "function_call_output");
        assert_eq!(body["input"][0]["call_id"], "call-1");
        assert_eq!(body["store"], true);
    }

    #[test]
    fn exact_resolved_attachments_map_to_inline_image_and_file_inputs() {
        let reference =
            SecretRef::parse("openai/attachment-test").expect("test secret reference should parse");
        let provider = OpenAiProvider::new(
            OpenAiProviderConfig::new(reference.clone()),
            Arc::new(TestSecrets(reference, "not-a-real-key".to_owned())),
        )
        .expect("provider should build");
        let image_bytes = b"synthetic-png".to_vec();
        let file_bytes = b"synthetic text file".to_vec();
        let image_block = ModelInputBlock::Attachment {
            attachment_id: uuid::Uuid::new_v4().to_string(),
            mime: "image/png".to_owned(),
            byte_count: image_bytes.len() as u64,
            sha256: hex::encode(sha2::Sha256::digest(&image_bytes)),
        };
        let file_block = ModelInputBlock::Attachment {
            attachment_id: uuid::Uuid::new_v4().to_string(),
            mime: "text/plain".to_owned(),
            byte_count: file_bytes.len() as u64,
            sha256: hex::encode(sha2::Sha256::digest(&file_bytes)),
        };
        let request = ModelRequest {
            model: "test-model".to_owned(),
            instructions: vec![],
            input: vec![image_block.clone(), file_block.clone()],
            continuation: None,
            tools: vec![],
            builtin_tools: vec![],
            output_schema: None,
            maximum_output_tokens: Some(32),
        };
        let resolved = vec![
            crate::AiResolvedProviderAttachment::new(
                AiProviderAttachmentRequest::try_from(&image_block)
                    .expect("image attachment should parse"),
                "image.png",
                image_bytes.clone(),
            )
            .expect("image bytes should bind"),
            crate::AiResolvedProviderAttachment::new(
                AiProviderAttachmentRequest::try_from(&file_block)
                    .expect("file attachment should parse"),
                "notes.txt",
                file_bytes.clone(),
            )
            .expect("file bytes should bind"),
        ];
        let provider_context = context("test-model", 100_000)
            .with_resolved_attachments(&request, resolved)
            .expect("resolved attachments should bind");
        let body = provider
            .request_body(&request, &provider_context)
            .expect("inline attachments should map");

        let content = body["input"][0]["content"]
            .as_array()
            .expect("user content should be an array");
        assert_eq!(content[0]["type"], "input_image");
        assert_eq!(content[0]["detail"], "auto");
        assert_eq!(
            content[0]["image_url"],
            format!(
                "data:image/png;base64,{}",
                base64::engine::general_purpose::STANDARD.encode(image_bytes)
            )
        );
        assert_eq!(content[1]["type"], "input_file");
        assert_eq!(content[1]["filename"], "notes.txt");
        assert_eq!(
            content[1]["file_data"],
            format!(
                "data:text/plain;base64,{}",
                base64::engine::general_purpose::STANDARD.encode(file_bytes)
            )
        );
    }

    #[tokio::test]
    async fn retained_response_transport_requires_matching_egress_retention() {
        let reference =
            SecretRef::parse("openai/retention-test").expect("test secret reference should parse");
        let secrets: Arc<dyn AiSecretStore> =
            Arc::new(TestSecrets(reference.clone(), "not-a-real-key".to_owned()));
        let mut config = OpenAiProviderConfig::new(reference);
        config.store_responses = true;
        let provider =
            OpenAiProvider::new(config, secrets).expect("retained-response provider should build");
        let request = ModelRequest {
            model: "test-model".to_owned(),
            instructions: vec!["Respond briefly.".to_owned()],
            input: vec![ModelInputBlock::Text {
                text: "synthetic hello".to_owned(),
            }],
            continuation: None,
            tools: vec![],
            builtin_tools: vec![],
            output_schema: None,
            maximum_output_tokens: Some(32),
        };

        assert!(matches!(
            provider.stream(request, context("test-model", 1_000)).await,
            Err(ProviderError::EgressDenied)
        ));
    }

    #[tokio::test]
    async fn responses_sse_is_normalized_without_retaining_secret_or_raw_body() {
        let sse = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_test\"}}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_test\",\"usage\":{\"input_tokens\":3,\"output_tokens\":1,\"input_tokens_details\":{\"cached_tokens\":1}}}}\n\n"
        );
        let (endpoint, server) = mock_server(sse).await;
        let reference = SecretRef::parse("openai/test").expect("reference should parse");
        let provider = OpenAiProvider::for_loopback_test(
            OpenAiProviderConfig::new(reference.clone()),
            Arc::new(TestSecrets(reference, "not-a-real-key".to_owned())),
            endpoint,
        )
        .expect("provider should build");
        let request = ModelRequest {
            model: "test-model".to_owned(),
            instructions: vec!["Respond briefly.".to_owned()],
            input: vec![ModelInputBlock::Text {
                text: "synthetic hello".to_owned(),
            }],
            continuation: None,
            tools: vec![],
            builtin_tools: vec![],
            output_schema: None,
            maximum_output_tokens: Some(32),
        };
        let events = provider
            .stream(request, context("test-model", 1_000))
            .await
            .expect("stream should start")
            .try_collect::<Vec<_>>()
            .await
            .expect("stream should normalize");
        server.await.expect("server task should finish");

        assert_eq!(
            events,
            vec![
                ProviderEvent::ResponseStarted {
                    response_id: Some("resp_test".to_owned())
                },
                ProviderEvent::TextDelta {
                    text: "hello".to_owned()
                },
                ProviderEvent::Usage {
                    input_tokens: 3,
                    output_tokens: 1,
                    cached_input_tokens: 1
                },
                ProviderEvent::ResponseCompleted {
                    response_id: Some("resp_test".to_owned())
                }
            ]
        );
    }

    #[tokio::test]
    #[ignore = "explicit opt-in live OpenAI smoke test; synthetic text only"]
    async fn live_openai_synthetic_text_smoke_test() {
        let key_file = std::env::var_os("GRAPHQL_ORM_AI_OPENAI_KEY_FILE")
            .map(PathBuf::from)
            .expect("set GRAPHQL_ORM_AI_OPENAI_KEY_FILE for the ignored live test");
        let model =
            std::env::var("GRAPHQL_ORM_AI_OPENAI_MODEL").unwrap_or_else(|_| "gpt-5.4".to_owned());
        let reference = SecretRef::parse("openai/live-smoke").expect("reference should parse");
        let provider = OpenAiProvider::new(
            OpenAiProviderConfig::new(reference.clone()),
            Arc::new(LiveFileSecrets(reference, key_file)),
        )
        .expect("provider should build");
        let request = ModelRequest {
            model: model.clone(),
            instructions: vec!["Reply with exactly the uppercase word OK.".to_owned()],
            input: vec![ModelInputBlock::Text {
                text: "This is a synthetic provider smoke test.".to_owned(),
            }],
            continuation: None,
            tools: vec![],
            builtin_tools: vec![],
            output_schema: None,
            maximum_output_tokens: Some(64),
        };
        let events = provider
            .stream(request, context(&model, 1_000))
            .await
            .expect("live stream should start")
            .try_collect::<Vec<_>>()
            .await
            .expect("live stream should complete");

        assert!(events.iter().any(|event| matches!(
            event,
            ProviderEvent::ResponseCompleted {
                response_id: Some(_)
            }
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            ProviderEvent::TextDelta { text } if !text.is_empty()
        )));
    }

    #[test]
    fn sse_decoder_handles_chunk_boundaries_and_crlf() {
        let mut decoder = SseDecoder::default();
        assert!(
            decoder
                .push(b"event: x\r\ndata: {\"type\":\"x\"")
                .expect("partial frame should buffer")
                .is_empty()
        );
        assert_eq!(
            decoder
                .push(b"}\r\n\r\n")
                .expect("completed frame should decode"),
            vec!["{\"type\":\"x\"}"]
        );
        assert!(decoder.finish().is_ok());
    }

    #[test]
    fn stream_quota_errors_are_safely_classified_as_rate_limited() {
        let event = json!({
            "type": "error",
            "error": {"code": "insufficient_quota", "message": "not retained"}
        });
        assert!(matches!(
            openai_stream_error(&event),
            ProviderError::RateLimited
        ));
    }

    #[test]
    fn unauthorized_http_status_is_safely_classified_as_credential_unavailable() {
        assert!(matches!(
            openai_http_error(reqwest::StatusCode::UNAUTHORIZED),
            Some(ProviderError::CredentialUnavailable)
        ));
    }
}
