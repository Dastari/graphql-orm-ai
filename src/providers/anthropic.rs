//! Native Anthropic Messages API adapter.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use reqwest::header::{CONTENT_TYPE, HeaderValue};
use secrecy::ExposeSecret;
use serde_json::{Value, json};

use crate::{
    AiProvider, AiSecretStore, ModelContinuation, ModelContinuationMode, ModelConversationMessage,
    ModelInputBlock, ModelRequest, ProviderCapabilities, ProviderError, ProviderEvent,
    ProviderEventStream, ProviderKind, ProviderRequestContext, SecretRef,
};

const ANTHROPIC_MESSAGES_ENDPOINT: &str = "https://api.anthropic.com/v1/messages";
const ANTHROPIC_API_VERSION: &str = "2023-06-01";
const MAXIMUM_REQUEST_BYTES: usize = 32 * 1024 * 1024;
const MAXIMUM_SSE_EVENT_BYTES: usize = 2 * 1024 * 1024;
const MAXIMUM_STREAM_EVENTS: usize = 65_536;
const MAXIMUM_TOOL_ARGUMENT_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_TOOL_CALLS: usize = 64;
const MAXIMUM_VISIBLE_TEXT_BYTES: usize = 64 * 1024 * 1024;

/// Native Anthropic adapter configuration.
///
/// Credential plaintext is resolved only immediately before transport and is
/// never retained in this value. The API endpoint and version header are fixed
/// by the adapter rather than accepted from GraphQL or model input.
#[derive(Clone, Debug)]
pub struct AnthropicProviderConfig {
    /// Secret-store reference for one Anthropic API key.
    pub credential: SecretRef,
    /// Overall HTTP request and stream timeout.
    pub timeout: Duration,
}

impl AnthropicProviderConfig {
    /// Creates secure defaults for Anthropic's official Messages endpoint.
    pub fn new(credential: SecretRef) -> Self {
        Self {
            credential,
            timeout: Duration::from_secs(120),
        }
    }
}

/// Native Anthropic Messages API provider.
///
/// The adapter supports text/JSON input, strict custom application tools,
/// parallel tool calls, JSON-schema output, and provider-independent stateless
/// tool continuation. It deliberately rejects attachments, provider built-ins,
/// extended thinking, and provider-retained continuation until their complete
/// retention, billing, and normalization contracts are implemented.
pub struct AnthropicProvider {
    config: AnthropicProviderConfig,
    secrets: Arc<dyn AiSecretStore>,
    client: reqwest::Client,
    endpoint: String,
}

impl std::fmt::Debug for AnthropicProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AnthropicProvider")
            .field("config", &self.config)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl AnthropicProvider {
    /// Builds a provider fixed to Anthropic's official HTTPS endpoint with
    /// redirects disabled.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::InvalidConfiguration`] for an invalid timeout
    /// or HTTP-client construction failure.
    pub fn new(
        config: AnthropicProviderConfig,
        secrets: Arc<dyn AiSecretStore>,
    ) -> Result<Self, ProviderError> {
        Self::build(config, secrets, ANTHROPIC_MESSAGES_ENDPOINT.to_owned())
    }

    fn build(
        config: AnthropicProviderConfig,
        secrets: Arc<dyn AiSecretStore>,
        endpoint: String,
    ) -> Result<Self, ProviderError> {
        if config.timeout.is_zero() || config.timeout > Duration::from_secs(600) {
            return Err(ProviderError::InvalidConfiguration(
                "Anthropic timeout must be between one millisecond and ten minutes".to_owned(),
            ));
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(config.timeout)
            .build()
            .map_err(|_| {
                ProviderError::InvalidConfiguration(
                    "Anthropic HTTP client could not be constructed".to_owned(),
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
        config: AnthropicProviderConfig,
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

    fn request_body(&self, request: &ModelRequest) -> Result<Value, ProviderError> {
        request.validate()?;
        if request.input.is_empty()
            || !request.builtin_tools.is_empty()
            || request.tools.iter().any(|tool| !tool.strict)
            || request
                .input
                .iter()
                .any(|block| matches!(block, ModelInputBlock::Attachment { .. }))
            || request.maximum_output_tokens.is_none()
        {
            return Err(ProviderError::Unsupported);
        }
        if request.continuation_mode == ModelContinuationMode::ProviderRetained
            && (request.continuation.is_some()
                || !request.tools.is_empty()
                || request
                    .input
                    .iter()
                    .any(|block| matches!(block, ModelInputBlock::ToolResult { .. })))
        {
            return Err(ProviderError::Unsupported);
        }

        let definitions = request
            .tools
            .iter()
            .map(|tool| (tool.tool_id.as_str(), tool))
            .collect::<BTreeMap<_, _>>();
        let (instructions, messages) = match &request.continuation {
            None => (
                request.instructions.as_slice(),
                vec![anthropic_user_message(&request.input)?],
            ),
            Some(ModelContinuation::StatelessConversation {
                instructions,
                messages,
            }) => {
                let mut mapped = anthropic_history(messages, &definitions)?;
                mapped.push(anthropic_current_tool_results(
                    messages,
                    &request.input,
                    &definitions,
                )?);
                (instructions.as_slice(), mapped)
            }
            Some(ModelContinuation::ProviderResponse { .. }) => {
                return Err(ProviderError::Unsupported);
            }
        };
        if messages.is_empty() {
            return Err(ProviderError::InvalidRequest);
        }

        let mut body = json!({
            "model": request.model,
            "max_tokens": request.maximum_output_tokens,
            "messages": messages,
            "stream": true
        });
        if !instructions.is_empty() {
            body["system"] = Value::String(instructions.join("\n\n"));
        }
        if !request.tools.is_empty() {
            body["tools"] = Value::Array(
                request
                    .tools
                    .iter()
                    .map(|tool| {
                        json!({
                            "name": tool.provider_name,
                            "description": tool.description,
                            "input_schema": tool.parameters,
                            "strict": tool.strict
                        })
                    })
                    .collect(),
            );
            body["tool_choice"] = json!({"type": "auto"});
        }
        if let Some(schema) = &request.output_schema {
            body["output_config"] = json!({
                "format": {
                    "type": "json_schema",
                    "schema": schema
                }
            });
        }
        if serde_json::to_vec(&body).map_or(true, |encoded| encoded.len() > MAXIMUM_REQUEST_BYTES) {
            return Err(ProviderError::InvalidRequest);
        }
        Ok(body)
    }
}

#[async_trait]
impl AiProvider for AnthropicProvider {
    fn provider_kind(&self) -> ProviderKind {
        ProviderKind::Anthropic
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            custom_tools: true,
            parallel_tool_calls: true,
            structured_output: true,
            stateless_continuation: true,
            ..ProviderCapabilities::default()
        }
    }

    async fn stream(
        &self,
        request: ModelRequest,
        context: ProviderRequestContext,
    ) -> Result<ProviderEventStream, ProviderError> {
        context.validate_request(&ProviderKind::Anthropic, &request)?;
        let body = self.request_body(&request)?;
        let secret = self
            .secrets
            .resolve(&self.config.credential)
            .await
            .map_err(|_| ProviderError::CredentialUnavailable)?;
        let api_key = HeaderValue::from_str(secret.expose_secret())
            .map_err(|_| ProviderError::CredentialUnavailable)?;
        let response = self
            .client
            .post(&self.endpoint)
            .header("x-api-key", api_key)
            .header("anthropic-version", ANTHROPIC_API_VERSION)
            .json(&body)
            .send()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        if !response.status().is_success() {
            return Err(classify_status(response.status()));
        }
        if response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_none_or(|value| !value.to_ascii_lowercase().starts_with("text/event-stream"))
        {
            return Err(ProviderError::Rejected);
        }
        let offered_tools = request
            .tools
            .into_iter()
            .map(|tool| (tool.provider_name, tool.tool_id))
            .collect();
        Ok(normalized_stream(
            response,
            request.model,
            offered_tools,
            request
                .maximum_output_tokens
                .ok_or(ProviderError::InvalidRequest)?,
        ))
    }
}

fn anthropic_user_message(input: &[ModelInputBlock]) -> Result<Value, ProviderError> {
    let content = input
        .iter()
        .map(|block| match block {
            ModelInputBlock::Text { text } => Ok(json!({"type": "text", "text": text})),
            ModelInputBlock::Json { value } => {
                Ok(json!({"type": "text", "text": value.to_string()}))
            }
            ModelInputBlock::Attachment { .. } | ModelInputBlock::ToolResult { .. } => {
                Err(ProviderError::InvalidRequest)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    if content.is_empty() {
        return Err(ProviderError::InvalidRequest);
    }
    Ok(json!({"role": "user", "content": content}))
}

fn anthropic_history(
    history: &[ModelConversationMessage],
    definitions: &BTreeMap<&str, &crate::ModelToolDefinition>,
) -> Result<Vec<Value>, ProviderError> {
    let mut messages = Vec::new();
    let mut index = 0;
    while index < history.len() {
        match &history[index] {
            ModelConversationMessage::User { content } => {
                messages.push(anthropic_user_message(content)?);
                index += 1;
            }
            ModelConversationMessage::Assistant {
                content,
                tool_calls,
            } => {
                let mut blocks =
                    Vec::with_capacity(tool_calls.len() + usize::from(!content.is_empty()));
                if !content.is_empty() {
                    blocks.push(json!({"type": "text", "text": content}));
                }
                for call in tool_calls {
                    let definition = exact_definition(call, definitions)?;
                    blocks.push(json!({
                        "type": "tool_use",
                        "id": call.call_id,
                        "name": definition.provider_name,
                        "input": call.arguments
                    }));
                }
                messages.push(json!({"role": "assistant", "content": blocks}));
                index += 1;
            }
            ModelConversationMessage::Tool { .. } => {
                let mut blocks = Vec::new();
                while let Some(ModelConversationMessage::Tool {
                    call_id,
                    tool_id,
                    provider_name,
                    output,
                }) = history.get(index)
                {
                    let definition = definitions
                        .get(tool_id.as_str())
                        .ok_or(ProviderError::InvalidRequest)?;
                    if definition.provider_name != *provider_name {
                        return Err(ProviderError::InvalidRequest);
                    }
                    blocks.push(json!({
                        "type": "tool_result",
                        "tool_use_id": call_id,
                        "content": output.to_string()
                    }));
                    index += 1;
                }
                if blocks.is_empty() {
                    return Err(ProviderError::InvalidRequest);
                }
                messages.push(json!({"role": "user", "content": blocks}));
            }
        }
    }
    Ok(messages)
}

fn anthropic_current_tool_results(
    history: &[ModelConversationMessage],
    input: &[ModelInputBlock],
    definitions: &BTreeMap<&str, &crate::ModelToolDefinition>,
) -> Result<Value, ProviderError> {
    let calls = match history.last() {
        Some(ModelConversationMessage::Assistant { tool_calls, .. }) => tool_calls,
        _ => return Err(ProviderError::InvalidRequest),
    };
    if calls.len() != input.len() {
        return Err(ProviderError::InvalidRequest);
    }
    let content = calls
        .iter()
        .zip(input)
        .map(|(call, block)| {
            exact_definition(call, definitions)?;
            let ModelInputBlock::ToolResult {
                call_id,
                tool_id,
                output,
            } = block
            else {
                return Err(ProviderError::InvalidRequest);
            };
            if call_id != &call.call_id || tool_id != &call.tool_id {
                return Err(ProviderError::InvalidRequest);
            }
            Ok(json!({
                "type": "tool_result",
                "tool_use_id": call_id,
                "content": output.to_string()
            }))
        })
        .collect::<Result<Vec<_>, ProviderError>>()?;
    Ok(json!({"role": "user", "content": content}))
}

fn exact_definition<'a>(
    call: &crate::ModelConversationToolCall,
    definitions: &'a BTreeMap<&str, &crate::ModelToolDefinition>,
) -> Result<&'a crate::ModelToolDefinition, ProviderError> {
    let definition = definitions
        .get(call.tool_id.as_str())
        .copied()
        .ok_or(ProviderError::InvalidRequest)?;
    if definition.provider_name != call.provider_name
        || definition.fingerprint != call.tool_fingerprint
    {
        return Err(ProviderError::InvalidRequest);
    }
    Ok(definition)
}

fn classify_status(status: reqwest::StatusCode) -> ProviderError {
    match status.as_u16() {
        401 | 403 => ProviderError::CredentialUnavailable,
        408 | 425 | 429 => ProviderError::RateLimited,
        500..=599 => ProviderError::Unavailable,
        _ => ProviderError::Rejected,
    }
}

#[derive(Default)]
struct SseDecoder {
    buffer: Vec<u8>,
}

impl SseDecoder {
    fn push(&mut self, bytes: &[u8]) -> Result<Vec<SseFrame>, ProviderError> {
        self.buffer.extend_from_slice(bytes);
        if self.buffer.len() > MAXIMUM_SSE_EVENT_BYTES {
            return Err(ProviderError::Rejected);
        }
        let mut frames = Vec::new();
        while let Some((position, delimiter_length)) = find_sse_delimiter(&self.buffer) {
            let frame = self.buffer.drain(..position).collect::<Vec<_>>();
            self.buffer.drain(..delimiter_length);
            if let Some(frame) = decode_sse_frame(&frame)? {
                frames.push(frame);
            }
        }
        Ok(frames)
    }

    fn finish(&self) -> Result<(), ProviderError> {
        if self.buffer.iter().all(u8::is_ascii_whitespace) {
            Ok(())
        } else {
            Err(ProviderError::Unavailable)
        }
    }
}

struct SseFrame {
    event: String,
    data: String,
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

fn decode_sse_frame(frame: &[u8]) -> Result<Option<SseFrame>, ProviderError> {
    let frame = std::str::from_utf8(frame).map_err(|_| ProviderError::Rejected)?;
    let mut event = None;
    let mut data = String::new();
    for line in frame.lines() {
        if let Some(value) = line.strip_prefix("event:") {
            if event.is_some() {
                return Err(ProviderError::Rejected);
            }
            event = Some(value.strip_prefix(' ').unwrap_or(value).to_owned());
        } else if let Some(value) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(value.strip_prefix(' ').unwrap_or(value));
        }
    }
    if event.is_none() && data.is_empty() {
        Ok(None)
    } else {
        let event = event
            .filter(|value| !value.is_empty())
            .ok_or(ProviderError::Rejected)?;
        if data.is_empty() {
            return Err(ProviderError::Rejected);
        }
        Ok(Some(SseFrame { event, data }))
    }
}

enum ContentBlockState {
    Text,
    Tool { call_id: String, arguments: String },
}

struct AnthropicEventNormalizer {
    expected_model: String,
    offered_tools: BTreeMap<String, String>,
    blocks: BTreeMap<u64, ContentBlockState>,
    completed_indices: BTreeSet<u64>,
    call_ids: BTreeSet<String>,
    started: bool,
    stop_reason: Option<String>,
    input_tokens: Option<u64>,
    cached_input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    maximum_output_tokens: u64,
    maximum_visible_text_bytes: usize,
    visible_text_bytes: usize,
    wire_events: usize,
}

impl AnthropicEventNormalizer {
    fn new(
        expected_model: String,
        offered_tools: BTreeMap<String, String>,
        maximum_output_tokens: u64,
    ) -> Self {
        let maximum_visible_text_bytes = usize::try_from(
            maximum_output_tokens
                .saturating_mul(64)
                .min(MAXIMUM_VISIBLE_TEXT_BYTES as u64),
        )
        .unwrap_or(MAXIMUM_VISIBLE_TEXT_BYTES);
        Self {
            expected_model,
            offered_tools,
            blocks: BTreeMap::new(),
            completed_indices: BTreeSet::new(),
            call_ids: BTreeSet::new(),
            started: false,
            stop_reason: None,
            input_tokens: None,
            cached_input_tokens: None,
            output_tokens: None,
            maximum_output_tokens,
            maximum_visible_text_bytes,
            visible_text_bytes: 0,
            wire_events: 0,
        }
    }

    fn normalize(&mut self, frame: SseFrame) -> Result<Vec<ProviderEvent>, ProviderError> {
        self.wire_events = self
            .wire_events
            .checked_add(1)
            .filter(|count| *count <= MAXIMUM_STREAM_EVENTS)
            .ok_or(ProviderError::Rejected)?;
        let value: Value =
            serde_json::from_str(&frame.data).map_err(|_| ProviderError::Rejected)?;
        let event_type = value
            .get("type")
            .and_then(Value::as_str)
            .ok_or(ProviderError::Rejected)?;
        if frame.event != event_type {
            return Err(ProviderError::Rejected);
        }
        match event_type {
            "message_start" => self.message_start(&value),
            "content_block_start" => self.content_start(&value),
            "content_block_delta" => self.content_delta(&value),
            "content_block_stop" => self.content_stop(&value),
            "message_delta" => self.message_delta(&value),
            "message_stop" => self.message_stop(),
            "ping" => Ok(Vec::new()),
            "error" => Err(classify_stream_error(&value)),
            other if self.started && self.stop_reason.is_none() && valid_event_type(other) => {
                Ok(vec![ProviderEvent::Unknown {
                    event_type: other.to_owned(),
                }])
            }
            _ => Err(ProviderError::Rejected),
        }
    }

    fn message_start(&mut self, value: &Value) -> Result<Vec<ProviderEvent>, ProviderError> {
        let message = value.get("message").ok_or(ProviderError::Rejected)?;
        if self.started
            || message.get("role").and_then(Value::as_str) != Some("assistant")
            || message.get("model").and_then(Value::as_str) != Some(self.expected_model.as_str())
            || message
                .get("content")
                .and_then(Value::as_array)
                .is_none_or(|content| !content.is_empty())
        {
            return Err(ProviderError::Rejected);
        }
        let usage = message.get("usage").ok_or(ProviderError::Rejected)?;
        let uncached_input_tokens = required_u64(usage, "input_tokens")?;
        let cache_creation_input_tokens =
            optional_nullable_u64(usage, "cache_creation_input_tokens")?;
        let cache_read_input_tokens = optional_nullable_u64(usage, "cache_read_input_tokens")?;
        // Anthropic prices cache creation as a distinct billable class. The
        // provider-neutral ledger currently has total input plus a cached-read
        // subset, so accepting a cache write would make authoritative pricing
        // inexact. This adapter never emits cache-control directives and fails
        // closed if the provider nevertheless reports a write.
        if cache_creation_input_tokens != 0 {
            return Err(ProviderError::Rejected);
        }
        self.input_tokens = Some(
            uncached_input_tokens
                .checked_add(cache_read_input_tokens)
                .ok_or(ProviderError::Rejected)?,
        );
        self.cached_input_tokens = Some(cache_read_input_tokens);
        self.started = true;
        Ok(vec![ProviderEvent::ResponseStarted { response_id: None }])
    }

    fn content_start(&mut self, value: &Value) -> Result<Vec<ProviderEvent>, ProviderError> {
        if !self.started || self.stop_reason.is_some() {
            return Err(ProviderError::Rejected);
        }
        let index = required_u64(value, "index")?;
        if self.blocks.contains_key(&index) || self.completed_indices.contains(&index) {
            return Err(ProviderError::Rejected);
        }
        let block = value.get("content_block").ok_or(ProviderError::Rejected)?;
        match block.get("type").and_then(Value::as_str) {
            Some("text") => {
                let text = block
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or(ProviderError::Rejected)?;
                self.visible_text_bytes = self
                    .visible_text_bytes
                    .checked_add(text.len())
                    .filter(|bytes| *bytes <= self.maximum_visible_text_bytes)
                    .ok_or(ProviderError::Rejected)?;
                self.blocks.insert(index, ContentBlockState::Text);
                if text.is_empty() {
                    Ok(Vec::new())
                } else {
                    Ok(vec![ProviderEvent::TextDelta {
                        text: text.to_owned(),
                    }])
                }
            }
            Some("tool_use") => {
                if self.call_ids.len() >= MAXIMUM_TOOL_CALLS
                    || block
                        .get("input")
                        .is_none_or(|input| input.as_object().is_none_or(|input| !input.is_empty()))
                {
                    return Err(ProviderError::Rejected);
                }
                let call_id = required_string(block, "id")?;
                let provider_name = required_string(block, "name")?;
                let tool_id = self
                    .offered_tools
                    .get(&provider_name)
                    .cloned()
                    .ok_or(ProviderError::Rejected)?;
                if !valid_call_id(&call_id) || !self.call_ids.insert(call_id.clone()) {
                    return Err(ProviderError::Rejected);
                }
                self.blocks.insert(
                    index,
                    ContentBlockState::Tool {
                        call_id: call_id.clone(),
                        arguments: String::new(),
                    },
                );
                Ok(vec![ProviderEvent::ToolCallStarted { call_id, tool_id }])
            }
            _ => Err(ProviderError::Rejected),
        }
    }

    fn content_delta(&mut self, value: &Value) -> Result<Vec<ProviderEvent>, ProviderError> {
        if !self.started || self.stop_reason.is_some() {
            return Err(ProviderError::Rejected);
        }
        let index = required_u64(value, "index")?;
        let delta = value.get("delta").ok_or(ProviderError::Rejected)?;
        match (
            self.blocks.get_mut(&index),
            delta.get("type").and_then(Value::as_str),
        ) {
            (Some(ContentBlockState::Text), Some("text_delta")) => {
                let text = required_string(delta, "text")?;
                self.visible_text_bytes = self
                    .visible_text_bytes
                    .checked_add(text.len())
                    .filter(|bytes| *bytes <= self.maximum_visible_text_bytes)
                    .ok_or(ProviderError::Rejected)?;
                Ok(vec![ProviderEvent::TextDelta { text }])
            }
            (Some(ContentBlockState::Tool { call_id, arguments }), Some("input_json_delta")) => {
                let fragment = required_string(delta, "partial_json")?;
                if arguments.len().saturating_add(fragment.len()) > MAXIMUM_TOOL_ARGUMENT_BYTES {
                    return Err(ProviderError::Rejected);
                }
                arguments.push_str(&fragment);
                Ok(vec![ProviderEvent::ToolArgumentsDelta {
                    call_id: call_id.clone(),
                    delta: fragment,
                }])
            }
            _ => Err(ProviderError::Rejected),
        }
    }

    fn content_stop(&mut self, value: &Value) -> Result<Vec<ProviderEvent>, ProviderError> {
        if !self.started || self.stop_reason.is_some() {
            return Err(ProviderError::Rejected);
        }
        let index = required_u64(value, "index")?;
        let block = self.blocks.remove(&index).ok_or(ProviderError::Rejected)?;
        if !self.completed_indices.insert(index) {
            return Err(ProviderError::Rejected);
        }
        match block {
            ContentBlockState::Text => Ok(Vec::new()),
            ContentBlockState::Tool { call_id, arguments } => {
                let arguments: Value =
                    serde_json::from_str(&arguments).map_err(|_| ProviderError::Rejected)?;
                if !arguments.is_object() {
                    return Err(ProviderError::Rejected);
                }
                Ok(vec![ProviderEvent::ToolCallCompleted {
                    call_id,
                    arguments,
                }])
            }
        }
    }

    fn message_delta(&mut self, value: &Value) -> Result<Vec<ProviderEvent>, ProviderError> {
        if !self.started || !self.blocks.is_empty() || self.stop_reason.is_some() {
            return Err(ProviderError::Rejected);
        }
        let stop_reason = value
            .pointer("/delta/stop_reason")
            .and_then(Value::as_str)
            .ok_or(ProviderError::Rejected)?;
        if !matches!(stop_reason, "end_turn" | "tool_use" | "refusal") {
            return Err(ProviderError::Rejected);
        }
        if stop_reason == "tool_use" {
            if self.call_ids.is_empty() {
                return Err(ProviderError::Rejected);
            }
        } else if !self.call_ids.is_empty() {
            return Err(ProviderError::Rejected);
        }
        self.output_tokens = value
            .pointer("/usage/output_tokens")
            .and_then(Value::as_u64);
        if self
            .output_tokens
            .is_none_or(|tokens| tokens > self.maximum_output_tokens)
        {
            return Err(ProviderError::Rejected);
        }
        self.stop_reason = Some(stop_reason.to_owned());
        Ok(Vec::new())
    }

    fn message_stop(&mut self) -> Result<Vec<ProviderEvent>, ProviderError> {
        if !self.started
            || !self.blocks.is_empty()
            || self.stop_reason.is_none()
            || self.input_tokens.is_none()
            || self.cached_input_tokens.is_none()
            || self.output_tokens.is_none()
        {
            return Err(ProviderError::Rejected);
        }
        self.started = false;
        Ok(vec![
            ProviderEvent::Usage {
                input_tokens: self.input_tokens.take().ok_or(ProviderError::Rejected)?,
                output_tokens: self.output_tokens.take().ok_or(ProviderError::Rejected)?,
                cached_input_tokens: self
                    .cached_input_tokens
                    .take()
                    .ok_or(ProviderError::Rejected)?,
            },
            ProviderEvent::ResponseCompleted { response_id: None },
        ])
    }

    fn finish(&self) -> Result<(), ProviderError> {
        if !self.started && self.stop_reason.is_some() && self.blocks.is_empty() {
            Ok(())
        } else {
            Err(ProviderError::Unavailable)
        }
    }
}

fn normalized_stream(
    response: reqwest::Response,
    expected_model: String,
    offered_tools: BTreeMap<String, String>,
    maximum_output_tokens: u64,
) -> ProviderEventStream {
    Box::pin(async_stream::try_stream! {
        let mut bytes = response.bytes_stream();
        let mut decoder = SseDecoder::default();
        let mut normalizer = AnthropicEventNormalizer::new(
            expected_model,
            offered_tools,
            maximum_output_tokens,
        );
        while let Some(chunk) = bytes.next().await {
            let chunk = chunk.map_err(|_| ProviderError::Unavailable)?;
            for frame in decoder.push(&chunk)? {
                for event in normalizer.normalize(frame)? {
                    yield event;
                }
            }
        }
        decoder.finish()?;
        normalizer.finish()?;
    })
}

fn required_string(value: &Value, field: &str) -> Result<String, ProviderError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(ProviderError::Rejected)
}

fn required_u64(value: &Value, field: &str) -> Result<u64, ProviderError> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or(ProviderError::Rejected)
}

fn optional_nullable_u64(value: &Value, field: &str) -> Result<u64, ProviderError> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(0),
        Some(value) => value.as_u64().ok_or(ProviderError::Rejected),
    }
}

fn valid_call_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 1_024
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'"' | b'\\'))
}

fn valid_event_type(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 200
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn classify_stream_error(value: &Value) -> ProviderError {
    match value.pointer("/error/type").and_then(Value::as_str) {
        Some("authentication_error" | "permission_error") => ProviderError::CredentialUnavailable,
        Some("rate_limit_error") => ProviderError::RateLimited,
        Some("api_error" | "overloaded_error" | "timeout_error") => ProviderError::Unavailable,
        _ => ProviderError::Rejected,
    }
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
        AiScope, AiSessionId, AiSourceTrust, DataClassification, ModelConversationToolCall,
        ModelToolDefinition, SecretError,
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

    fn test_provider(endpoint: String) -> AnthropicProvider {
        let reference =
            SecretRef::parse("anthropic/test").expect("test secret reference should parse");
        AnthropicProvider::for_loopback_test(
            AnthropicProviderConfig::new(reference.clone()),
            Arc::new(TestSecrets(reference, "synthetic-anthropic-key".to_owned())),
            endpoint,
        )
        .expect("loopback Anthropic provider should build")
    }

    fn definition() -> ModelToolDefinition {
        ModelToolDefinition {
            tool_id: "records.read".to_owned(),
            provider_name: "records_read".to_owned(),
            fingerprint: "records-read-v1".to_owned(),
            description: "Read one authorized record".to_owned(),
            parameters: json!({
                "type": "object",
                "properties": {"recordId": {"type": "string"}},
                "required": ["recordId"],
                "additionalProperties": false
            }),
            strict: true,
        }
    }

    fn initial_request() -> ModelRequest {
        ModelRequest {
            model: "test-model".to_owned(),
            instructions: vec!["Use only authorized records.".to_owned()],
            input: vec![ModelInputBlock::Text {
                text: "Read record 54".to_owned(),
            }],
            continuation: None,
            continuation_mode: ModelContinuationMode::StatelessReplay,
            tools: vec![definition()],
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
            provider_profile_id: "anthropic-profile".to_owned(),
            provider_kind: ProviderKind::Anthropic.as_str().to_owned(),
            model: request.model.clone(),
            destination: "anthropic".to_owned(),
            destination_trust: AiDestinationTrust::ManagedProvider,
            capability: AiEgressCapability::ModelInference,
            scope: AiScope::new("project", "anthropic-test"),
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
            ProviderKind::Anthropic,
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
            &ProviderKind::Anthropic,
            &request.model,
            request
                .maximum_output_tokens
                .expect("test request should have output bound"),
            0,
            time::OffsetDateTime::now_utc(),
        )
        .expect("budget should authorize");
        ProviderRequestContext::new(session_id, run_id, "test", budget, manifest, proof)
            .expect("context should validate")
    }

    async fn mock_server(
        status: &'static str,
        content_type: &'static str,
        body: String,
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
            let headers = format!(
                "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
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
        (format!("http://{address}/v1/messages"), request_rx, task)
    }

    #[test]
    fn stateless_history_tools_and_structured_output_map_exactly() {
        let provider = test_provider("http://127.0.0.1:1/v1/messages".to_owned());
        let tool = definition();
        let first = ModelConversationToolCall {
            call_id: "toolu-first".to_owned(),
            tool_id: tool.tool_id.clone(),
            provider_name: tool.provider_name.clone(),
            tool_fingerprint: tool.fingerprint.clone(),
            arguments: json!({"recordId": "54"}),
        };
        let second = ModelConversationToolCall {
            call_id: "toolu-second".to_owned(),
            tool_id: tool.tool_id.clone(),
            provider_name: tool.provider_name.clone(),
            tool_fingerprint: tool.fingerprint.clone(),
            arguments: json!({"recordId": "55"}),
        };
        let current = ModelConversationToolCall {
            call_id: "toolu-current".to_owned(),
            tool_id: tool.tool_id.clone(),
            provider_name: tool.provider_name.clone(),
            tool_fingerprint: tool.fingerprint.clone(),
            arguments: json!({"recordId": "56"}),
        };
        let request = ModelRequest {
            model: "test-model".to_owned(),
            instructions: Vec::new(),
            input: vec![ModelInputBlock::ToolResult {
                call_id: current.call_id.clone(),
                tool_id: current.tool_id.clone(),
                output: json!({"recordId": "56", "subject": "current"}),
            }],
            continuation: Some(ModelContinuation::StatelessConversation {
                instructions: vec!["Use only authorized records.".to_owned()],
                messages: vec![
                    ModelConversationMessage::User {
                        content: vec![ModelInputBlock::Text {
                            text: "Read three records".to_owned(),
                        }],
                    },
                    ModelConversationMessage::Assistant {
                        content: "I will read the first two.".to_owned(),
                        tool_calls: vec![first.clone(), second.clone()],
                    },
                    ModelConversationMessage::Tool {
                        call_id: first.call_id.clone(),
                        tool_id: first.tool_id.clone(),
                        provider_name: first.provider_name.clone(),
                        output: json!({"recordId": "54"}),
                    },
                    ModelConversationMessage::Tool {
                        call_id: second.call_id.clone(),
                        tool_id: second.tool_id.clone(),
                        provider_name: second.provider_name.clone(),
                        output: json!({"recordId": "55"}),
                    },
                    ModelConversationMessage::Assistant {
                        content: String::new(),
                        tool_calls: vec![current.clone()],
                    },
                ],
            }),
            continuation_mode: ModelContinuationMode::StatelessReplay,
            tools: vec![tool],
            builtin_tools: Vec::new(),
            maximum_builtin_tool_calls: None,
            output_schema: None,
            maximum_output_tokens: Some(256),
        };
        let body = provider
            .request_body(&request)
            .expect("exact stateless history should map");
        assert_eq!(body["system"], "Use only authorized records.");
        assert_eq!(body["tools"][0]["name"], "records_read");
        assert_eq!(body["tools"][0]["strict"], true);
        assert_eq!(body["messages"].as_array().map(Vec::len), Some(5));
        assert_eq!(body["messages"][1]["content"][1]["id"], "toolu-first");
        assert_eq!(body["messages"][2]["role"], "user");
        assert_eq!(
            body["messages"][2]["content"].as_array().map(Vec::len),
            Some(2)
        );
        assert_eq!(
            body["messages"][4]["content"][0]["tool_use_id"],
            "toolu-current"
        );

        let structured = ModelRequest {
            model: "test-model".to_owned(),
            instructions: vec!["Return the requested structure.".to_owned()],
            input: vec![ModelInputBlock::Text {
                text: "Summarize this synthetic record.".to_owned(),
            }],
            continuation: None,
            continuation_mode: ModelContinuationMode::ProviderRetained,
            tools: Vec::new(),
            builtin_tools: Vec::new(),
            maximum_builtin_tool_calls: None,
            output_schema: Some(json!({
                "type": "object",
                "properties": {"summary": {"type": "string"}},
                "required": ["summary"],
                "additionalProperties": false
            })),
            maximum_output_tokens: Some(128),
        };
        let structured_body = provider
            .request_body(&structured)
            .expect("structured text request should map");
        assert_eq!(
            structured_body["output_config"]["format"]["type"],
            "json_schema"
        );

        let mut non_strict = initial_request();
        non_strict.tools[0].strict = false;
        assert!(matches!(
            provider.request_body(&non_strict),
            Err(ProviderError::Unsupported)
        ));
    }

    #[tokio::test]
    async fn messages_sse_normalizes_text_tools_and_cumulative_usage() {
        let body = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg-test\",\"role\":\"assistant\",\"model\":\"test-model\",\"content\":[],\"usage\":{\"input_tokens\":12,\"output_tokens\":1,\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":2}}}\n\n",
            "event: content_block_start\n",
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Checking.\"}}\n\n",
            "event: content_block_stop\n",
            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: content_block_start\n",
            "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu-test\",\"name\":\"records_read\",\"input\":{}}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"recordId\\\":\\\"54\\\"}\"}}\n\n",
            "event: content_block_stop\n",
            "data: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":7}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n"
        )
        .to_owned();
        let (endpoint, request_rx, server) = mock_server("200 OK", "text/event-stream", body).await;
        let provider = test_provider(endpoint);
        let request = initial_request();
        let events = provider
            .stream(request.clone(), context(&request))
            .await
            .expect("Anthropic stream should start")
            .try_collect::<Vec<_>>()
            .await
            .expect("Anthropic stream should normalize");
        assert_eq!(
            events,
            vec![
                ProviderEvent::ResponseStarted { response_id: None },
                ProviderEvent::TextDelta {
                    text: "Checking.".to_owned(),
                },
                ProviderEvent::ToolCallStarted {
                    call_id: "toolu-test".to_owned(),
                    tool_id: "records.read".to_owned(),
                },
                ProviderEvent::ToolArgumentsDelta {
                    call_id: "toolu-test".to_owned(),
                    delta: "{\"recordId\":\"54\"}".to_owned(),
                },
                ProviderEvent::ToolCallCompleted {
                    call_id: "toolu-test".to_owned(),
                    arguments: json!({"recordId": "54"}),
                },
                ProviderEvent::Usage {
                    input_tokens: 14,
                    output_tokens: 7,
                    cached_input_tokens: 2,
                },
                ProviderEvent::ResponseCompleted { response_id: None },
            ]
        );
        let request_bytes = request_rx.await.expect("request should be captured");
        let request_text = String::from_utf8(request_bytes).expect("request should be UTF-8");
        let request_lower = request_text.to_ascii_lowercase();
        assert!(request_lower.contains("x-api-key: synthetic-anthropic-key"));
        assert!(request_lower.contains("anthropic-version: 2023-06-01"));
        assert!(request_text.contains("\"name\":\"records_read\""));
        server.await.expect("mock server should finish");
    }

    #[tokio::test]
    async fn http_and_stream_failures_are_safely_classified() {
        let request = initial_request();
        let (endpoint, _request_rx, server) =
            mock_server("401 Unauthorized", "application/json", "{}".to_owned()).await;
        let provider = test_provider(endpoint);
        assert!(matches!(
            provider.stream(request.clone(), context(&request)).await,
            Err(ProviderError::CredentialUnavailable)
        ));
        server.await.expect("401 server should finish");

        let (endpoint, _request_rx, server) =
            mock_server("529 Overloaded", "application/json", "{}".to_owned()).await;
        let provider = test_provider(endpoint);
        assert!(matches!(
            provider.stream(request.clone(), context(&request)).await,
            Err(ProviderError::Unavailable)
        ));
        server.await.expect("529 server should finish");

        let mut normalizer = AnthropicEventNormalizer::new(
            "test-model".to_owned(),
            [("records_read".to_owned(), "records.read".to_owned())]
                .into_iter()
                .collect(),
            128,
        );
        normalizer
            .normalize(SseFrame {
                event: "message_start".to_owned(),
                data: json!({
                    "type": "message_start",
                    "message": {
                        "role": "assistant",
                        "model": "test-model",
                        "content": [],
                        "usage": {"input_tokens": 1}
                    }
                })
                .to_string(),
            })
            .expect("message should start");
        assert!(matches!(
            normalizer.normalize(SseFrame {
                event: "content_block_start".to_owned(),
                data: json!({
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {"type": "thinking", "thinking": "hidden"}
                })
                .to_string(),
            }),
            Err(ProviderError::Rejected)
        ));

        let mut cache_write =
            AnthropicEventNormalizer::new("test-model".to_owned(), BTreeMap::new(), 128);
        assert!(matches!(
            cache_write.normalize(SseFrame {
                event: "message_start".to_owned(),
                data: json!({
                    "type": "message_start",
                    "message": {
                        "role": "assistant",
                        "model": "test-model",
                        "content": [],
                        "usage": {
                            "input_tokens": 1,
                            "cache_creation_input_tokens": 2,
                            "cache_read_input_tokens": 0
                        }
                    }
                })
                .to_string(),
            }),
            Err(ProviderError::Rejected)
        ));
    }

    #[test]
    fn stream_output_and_forward_compatibility_are_bounded() {
        let start = |normalizer: &mut AnthropicEventNormalizer| {
            normalizer.normalize(SseFrame {
                event: "message_start".to_owned(),
                data: json!({
                    "type": "message_start",
                    "message": {
                        "role": "assistant",
                        "model": "test-model",
                        "content": [],
                        "usage": {"input_tokens": 1}
                    }
                })
                .to_string(),
            })
        };

        let mut oversized_text =
            AnthropicEventNormalizer::new("test-model".to_owned(), BTreeMap::new(), 1);
        start(&mut oversized_text).expect("bounded response should start");
        assert!(matches!(
            oversized_text.normalize(SseFrame {
                event: "content_block_start".to_owned(),
                data: json!({
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {"type": "text", "text": "x".repeat(65)}
                })
                .to_string(),
            }),
            Err(ProviderError::Rejected)
        ));

        let mut oversized_usage =
            AnthropicEventNormalizer::new("test-model".to_owned(), BTreeMap::new(), 1);
        start(&mut oversized_usage).expect("bounded response should start");
        assert!(matches!(
            oversized_usage.normalize(SseFrame {
                event: "message_delta".to_owned(),
                data: json!({
                    "type": "message_delta",
                    "delta": {"stop_reason": "end_turn", "stop_sequence": null},
                    "usage": {"output_tokens": 2}
                })
                .to_string(),
            }),
            Err(ProviderError::Rejected)
        ));

        let mut unknown =
            AnthropicEventNormalizer::new("test-model".to_owned(), BTreeMap::new(), 1);
        start(&mut unknown).expect("bounded response should start");
        let event_type = "x".repeat(201);
        assert!(matches!(
            unknown.normalize(SseFrame {
                event: event_type.clone(),
                data: json!({"type": event_type}).to_string(),
            }),
            Err(ProviderError::Rejected)
        ));
    }
}
