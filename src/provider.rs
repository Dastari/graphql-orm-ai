//! Provider-neutral model adapter contract.

use std::collections::BTreeSet;
use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

const MAXIMUM_PROVIDER_REQUEST_METADATA_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_STATELESS_TOOL_RESULTS: usize = 256;

use crate::{
    AiBudgetReservationId, AiEgressCapability, AiEgressManifest, AiError,
    AiProviderAttachmentRequest, AiResolvedProviderAttachment, AiRunId, AiSessionId,
    AuthorizedBudgetReservation, AuthorizedEgress,
};

/// Manifest retention value required before a provider adapter may retain a
/// response for stateful continuation.
pub const AI_EGRESS_RETENTION_PROVIDER_RESPONSE: &str = "provider_response";

/// Supported provider family.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    /// OpenAI native API.
    OpenAi,
    /// Anthropic native API.
    Anthropic,
    /// xAI/Grok native API.
    Xai,
    /// Ollama local/native API.
    Ollama,
    /// Explicitly configured OpenAI-compatible endpoint.
    OpenAiCompatible,
    /// Deployment-registered installed local harness.
    LocalHarness,
}

impl ProviderKind {
    /// Stable configuration/manifest value.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Anthropic => "anthropic",
            Self::Xai => "xai",
            Self::Ollama => "ollama",
            Self::OpenAiCompatible => "openai_compatible",
            Self::LocalHarness => "local_harness",
        }
    }
}

/// Capability declaration used for safe route selection.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCapabilities {
    /// Streaming text output.
    pub streaming: bool,
    /// Image input.
    pub image_input: bool,
    /// File input.
    pub file_input: bool,
    /// Custom application tools.
    pub custom_tools: bool,
    /// Parallel custom tool calls.
    pub parallel_tool_calls: bool,
    /// JSON-schema structured output.
    pub structured_output: bool,
    /// Provider web search.
    pub web_search: bool,
    /// Provider file search/retention.
    pub file_search: bool,
    /// Provider code execution.
    pub code_execution: bool,
    /// Image generation.
    pub image_generation: bool,
    /// Embeddings.
    pub embeddings: bool,
    /// Background processing/webhooks.
    pub background: bool,
    /// Provider-retained response continuation.
    pub provider_retained_continuation: bool,
    /// Full provider-independent stateless conversation replay.
    pub stateless_continuation: bool,
    /// Executes locally within the configured deployment boundary.
    pub local: bool,
    /// Maximum context tokens when known.
    pub maximum_context_tokens: Option<u64>,
    /// Maximum output tokens when known.
    pub maximum_output_tokens: Option<u64>,
}

/// Canonical model input block.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ModelInputBlock {
    /// Text content.
    Text {
        /// Text value after policy/egress authorization.
        text: String,
    },
    /// Opaque attachment reference resolved by the adapter pipeline.
    Attachment {
        /// AI-owned attachment ID.
        attachment_id: String,
        /// Safe detected MIME type.
        mime: String,
        /// Exact verified attachment bytes.
        byte_count: u64,
        /// Exact lowercase SHA-256 of the released object.
        sha256: String,
    },
    /// Structured JSON content.
    Json {
        /// JSON value.
        value: serde_json::Value,
    },
    /// Exact result of a provider-requested application tool call.
    ToolResult {
        /// Provider call identifier emitted by the immediately preceding turn.
        call_id: String,
        /// Stable local tool identifier used for audit binding only.
        tool_id: String,
        /// Disclosure-validated, separately egress-authorized tool output.
        output: serde_json::Value,
    },
}

impl ModelInputBlock {
    /// Returns the canonical source reference for an attachment block.
    ///
    /// The versioned reference binds the attachment ID, verified byte count,
    /// detected MIME type, and content checksum without containing attachment
    /// plaintext. Hosts should place this exact value in the
    /// [`crate::AiDataSourceRef::reference`] authorized for the attachment's
    /// image/file capability.
    ///
    /// This helper derives a reference; it does not prove that the block was
    /// released, that its metadata is valid, or that egress was authorized.
    /// [`ModelRequest::validate`] and the provider boundary enforce those
    /// remaining checks.
    pub fn attachment_egress_reference(&self) -> Option<String> {
        match self {
            Self::Attachment {
                attachment_id,
                mime,
                byte_count,
                sha256,
            } => Some(format!("v1:{attachment_id}:{byte_count}:{mime}:{sha256}")),
            Self::Text { .. } | Self::Json { .. } | Self::ToolResult { .. } => None,
        }
    }
}

/// Explicit provider-retained or protected stateless conversation continuation.
///
/// A continuation is not an authorization proof. The next request still needs
/// fresh principal access, budget, and exact egress proofs. Provider adapters
/// may reject retained-response continuation unless deployment configuration
/// deliberately permits provider response storage. Stateless replay contains
/// protected content but still proves neither current access nor egress.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ModelContinuation {
    /// Continue from one exact provider response retained by the provider.
    ProviderResponse {
        /// Opaque provider response identifier.
        response_id: String,
    },
    /// Replay one exact bounded conversation without provider-retained state.
    StatelessConversation {
        /// Original trusted runtime instructions, replayed unchanged.
        instructions: Vec<String>,
        /// Exact user/assistant/tool history ending in an assistant tool-call
        /// message whose results are supplied in the request input.
        messages: Vec<ModelConversationMessage>,
    },
}

impl ModelContinuation {
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) fn chain_reference(&self, input: &[ModelInputBlock]) -> Option<String> {
        match self {
            Self::ProviderResponse { response_id } => Some(response_id.clone()),
            Self::StatelessConversation { .. } => {
                let encoded = serde_json::to_vec(&(self, input)).ok()?;
                let mut hasher = Sha256::new();
                hasher.update(b"graphql-orm-ai/stateless-continuation/v1\0");
                hasher.update(encoded);
                Some(format!("stateless:{}", hex::encode(hasher.finalize())))
            }
        }
    }
}

/// Continuation storage strategy selected by the trusted server-authored plan.
///
/// This is not a provider capability proof. Adapters still reject a mode they
/// do not implement, and every replay needs fresh budget and egress proofs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelContinuationMode {
    /// The provider retains the preceding response under an opaque ID.
    #[default]
    ProviderRetained,
    /// The runtime replays a complete protected, bounded conversation.
    StatelessReplay,
}

/// One assistant-requested tool call retained in stateless conversation state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelConversationToolCall {
    /// Runtime-normalized opaque call ID.
    pub call_id: String,
    /// Stable local tool ID.
    pub tool_id: String,
    /// Provider-facing function name from the exact reviewed definition.
    pub provider_name: String,
    /// Exact registered descriptor fingerprint.
    pub tool_fingerprint: String,
    /// Complete schema-validated arguments.
    pub arguments: serde_json::Value,
}

/// One message in a provider-independent stateless tool conversation.
///
/// The representation deliberately excludes hidden thinking, attachments,
/// provider built-ins, arbitrary roles, and model-authored system messages.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum ModelConversationMessage {
    /// Initial user content. Stateless tool loops currently accept text/JSON
    /// only so an attachment cannot be replayed without exact reopening.
    User {
        /// Ordered bounded user input blocks.
        content: Vec<ModelInputBlock>,
    },
    /// Accumulated visible assistant content and exact application-tool calls.
    Assistant {
        /// Visible assistant text only; hidden thinking is never retained.
        content: String,
        /// Ordered exact tool requests from this turn.
        tool_calls: Vec<ModelConversationToolCall>,
    },
    /// One disclosure-validated tool result paired to the preceding assistant
    /// call by exact order and identity.
    Tool {
        /// Runtime-normalized opaque call ID.
        call_id: String,
        /// Stable local tool ID.
        tool_id: String,
        /// Provider-facing function name from the exact reviewed definition.
        provider_name: String,
        /// Disclosure-validated model-visible result.
        output: serde_json::Value,
    },
}

/// Provider-neutral request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelRequest {
    /// Provider model identifier.
    pub model: String,
    /// Trusted runtime instructions.
    pub instructions: Vec<String>,
    /// Bounded canonical context/input.
    pub input: Vec<ModelInputBlock>,
    /// Optional explicit continuation of the immediately preceding response.
    pub continuation: Option<ModelContinuation>,
    /// Server-selected continuation storage strategy.
    #[serde(default)]
    pub continuation_mode: ModelContinuationMode,
    /// Enabled custom tools already filtered by local policy.
    pub tools: Vec<ModelToolDefinition>,
    /// Enabled provider built-ins, each separately approved for egress.
    pub builtin_tools: Vec<ModelBuiltinTool>,
    /// Optional structured-output schema.
    pub output_schema: Option<serde_json::Value>,
    /// Maximum requested output tokens.
    pub maximum_output_tokens: Option<u64>,
}

impl ModelRequest {
    /// Validates bounded, provider-neutral request invariants.
    ///
    /// Provider adapters still apply their own capability and protocol limits.
    pub fn validate(&self) -> Result<(), ProviderError> {
        if self.model.trim().is_empty() || self.model.len() > 200 {
            return Err(ProviderError::InvalidRequest);
        }
        if self.instructions.len() > 32
            || self
                .instructions
                .iter()
                .any(|instruction| instruction.len() > 1024 * 1024)
            || self.input.len() > 256
            || self.tools.len() > 128
            || self.builtin_tools.len() > 16
            || self
                .maximum_output_tokens
                .is_some_and(|tokens| tokens == 0 || tokens > u64::from(u32::MAX))
            || self.output_schema.as_ref().is_some_and(|schema| {
                !schema.is_object()
                    || serde_json::to_vec(schema)
                        .map_or(true, |encoded| encoded.len() > 1024 * 1024)
            })
        {
            return Err(ProviderError::InvalidRequest);
        }
        match (&self.continuation_mode, &self.continuation) {
            (
                ModelContinuationMode::ProviderRetained,
                Some(ModelContinuation::ProviderResponse { response_id }),
            ) if valid_provider_reference(response_id) => {}
            (ModelContinuationMode::ProviderRetained, None)
            | (ModelContinuationMode::StatelessReplay, None) => {}
            (
                ModelContinuationMode::StatelessReplay,
                Some(ModelContinuation::StatelessConversation {
                    instructions,
                    messages,
                }),
            ) if self.instructions.is_empty()
                && valid_instructions(instructions)
                && validate_stateless_messages(messages, &self.input, &self.tools) => {}
            _ => return Err(ProviderError::InvalidRequest),
        }
        let has_tool_results = self
            .input
            .iter()
            .any(|block| matches!(block, ModelInputBlock::ToolResult { .. }));
        if has_tool_results != self.continuation.is_some() {
            return Err(ProviderError::InvalidRequest);
        }
        let mut tool_result_call_ids = BTreeSet::new();
        let mut attachment_ids = BTreeSet::new();
        for block in &self.input {
            if matches!(block, ModelInputBlock::Text { text } if text.len() > 16 * 1024 * 1024)
                || matches!(block, ModelInputBlock::Json { value }
                    if serde_json::to_vec(value)
                        .map_or(true, |encoded| encoded.len() > 16 * 1024 * 1024))
            {
                return Err(ProviderError::InvalidRequest);
            }
            if let ModelInputBlock::ToolResult {
                call_id,
                tool_id,
                output,
            } = block
                && (!valid_provider_reference(call_id)
                    || tool_id.is_empty()
                    || tool_id.len() > 200
                    || !tool_result_call_ids.insert(call_id.as_str())
                    || serde_json::to_vec(output)
                        .map_or(true, |encoded| encoded.len() > 16 * 1024 * 1024))
            {
                return Err(ProviderError::InvalidRequest);
            }
            if let ModelInputBlock::Attachment {
                attachment_id,
                mime,
                byte_count,
                sha256,
            } = block
                && (Uuid::parse_str(attachment_id).is_err()
                    || !crate::valid_mime(mime)
                    || *byte_count == 0
                    || *byte_count > 100 * 1024 * 1024
                    || !crate::valid_sha256(sha256)
                    || !attachment_ids.insert(attachment_id.as_str()))
            {
                return Err(ProviderError::InvalidRequest);
            }
        }
        let mut provider_names = BTreeSet::new();
        let mut tool_ids = BTreeSet::new();
        for tool in &self.tools {
            tool.validate()?;
            if !provider_names.insert(tool.provider_name.as_str())
                || !tool_ids.insert(tool.tool_id.as_str())
            {
                return Err(ProviderError::InvalidRequest);
            }
        }
        if self.continuation_mode == ModelContinuationMode::StatelessReplay
            && !self.tools.is_empty()
            && (self
                .input
                .iter()
                .any(|block| matches!(block, ModelInputBlock::Attachment { .. }))
                || !self.builtin_tools.is_empty()
                || self.output_schema.is_some())
        {
            return Err(ProviderError::InvalidRequest);
        }
        let mut builtin_kinds = BTreeSet::new();
        for builtin in &self.builtin_tools {
            let valid = match builtin {
                ModelBuiltinTool::WebSearch { allowed_domains } => {
                    builtin_kinds.insert("web_search")
                        && allowed_domains.len() <= 100
                        && unique_valid_values(allowed_domains, valid_web_domain)
                }
                ModelBuiltinTool::FileSearch {
                    store_ids,
                    maximum_results,
                } => {
                    builtin_kinds.insert("file_search")
                        && !store_ids.is_empty()
                        && store_ids.len() <= 20
                        && unique_valid_values(store_ids, valid_provider_reference)
                        && maximum_results.is_none_or(|value| (1..=50).contains(&value))
                }
                ModelBuiltinTool::CodeInterpreter => builtin_kinds.insert("code_interpreter"),
                ModelBuiltinTool::ImageGeneration => builtin_kinds.insert("image_generation"),
            };
            if !valid {
                return Err(ProviderError::InvalidRequest);
            }
        }
        if self.serialized_metadata_bytes().is_none() {
            return Err(ProviderError::InvalidRequest);
        }
        Ok(())
    }

    /// Returns the conservative byte ceiling an inference egress manifest must
    /// cover for this complete provider-neutral request.
    ///
    /// The value includes fixed serialization overhead, every instruction and
    /// input block, tool and built-in definitions, continuation/output schema,
    /// and Base64 expansion of declared attachment bytes. It is an egress
    /// capacity bound, not an exact provider wire-size or authorization proof.
    pub fn conservative_egress_bytes(&self) -> u64 {
        let serialized_bytes = self.serialized_metadata_bytes().unwrap_or(u64::MAX);
        let encoded_attachment_bytes = self
            .input
            .iter()
            .filter_map(|block| match block {
                ModelInputBlock::Attachment { byte_count, .. } => Some(
                    byte_count
                        .saturating_add(2)
                        .checked_div(3)
                        .unwrap_or(u64::MAX)
                        .saturating_mul(4),
                ),
                ModelInputBlock::Text { .. }
                | ModelInputBlock::Json { .. }
                | ModelInputBlock::ToolResult { .. } => None,
            })
            .fold(0_u64, u64::saturating_add);
        serialized_bytes.saturating_add(encoded_attachment_bytes)
    }

    fn serialized_metadata_bytes(&self) -> Option<u64> {
        let mut total = 4_096_u64
            .checked_add(serialized_bytes(&self.model)?)?
            .checked_add(64_u64.saturating_mul(self.instructions.len() as u64))?
            .checked_add(64_u64.saturating_mul(self.input.len() as u64))?
            .checked_add(64_u64.saturating_mul(self.tools.len() as u64))?
            .checked_add(64_u64.saturating_mul(self.builtin_tools.len() as u64))?;
        for instruction in &self.instructions {
            total = total.checked_add(serialized_bytes(instruction)?)?;
        }
        for block in &self.input {
            total = total.checked_add(serialized_bytes(block)?)?;
        }
        for tool in &self.tools {
            total = total.checked_add(serialized_bytes(tool)?)?;
        }
        for builtin in &self.builtin_tools {
            total = total.checked_add(serialized_bytes(builtin)?)?;
        }
        if let Some(continuation) = &self.continuation {
            total = total.checked_add(serialized_bytes(continuation)?)?;
        }
        if let Some(schema) = &self.output_schema {
            total = total.checked_add(serialized_bytes(schema)?)?;
        }
        (total <= MAXIMUM_PROVIDER_REQUEST_METADATA_BYTES).then_some(total)
    }
}

fn valid_instructions(instructions: &[String]) -> bool {
    instructions.len() <= 32
        && instructions
            .iter()
            .all(|instruction| instruction.len() <= 1024 * 1024)
}

fn validate_stateless_messages(
    messages: &[ModelConversationMessage],
    input: &[ModelInputBlock],
    tools: &[ModelToolDefinition],
) -> bool {
    if messages.len() < 2 || messages.len() > 256 || tools.is_empty() {
        return false;
    }
    let definitions = tools
        .iter()
        .map(|tool| {
            (
                tool.tool_id.as_str(),
                (tool.provider_name.as_str(), tool.fingerprint.as_str()),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut call_ids = BTreeSet::new();
    let mut pending: Vec<&ModelConversationToolCall> = Vec::new();
    let mut tool_result_count = 0_usize;
    let mut expecting_assistant = false;
    for (index, message) in messages.iter().enumerate() {
        match message {
            ModelConversationMessage::User { content }
                if index == 0
                    && !content.is_empty()
                    && content.len() <= 256
                    && content.iter().all(valid_stateless_user_block) =>
            {
                expecting_assistant = true;
            }
            ModelConversationMessage::Assistant {
                content,
                tool_calls,
            } if expecting_assistant
                && content.len() <= 16 * 1024 * 1024
                && !tool_calls.is_empty()
                && tool_calls.len() <= 64
                && tool_calls.iter().all(|call| {
                    definitions.get(call.tool_id.as_str()).is_some_and(
                        |(provider_name, fingerprint)| {
                            *provider_name == call.provider_name
                                && *fingerprint == call.tool_fingerprint
                                && valid_provider_reference(&call.call_id)
                                && call_ids.insert(call.call_id.as_str())
                                && call.arguments.is_object()
                                && serde_json::to_vec(&call.arguments)
                                    .is_ok_and(|encoded| encoded.len() <= 16 * 1024 * 1024)
                        },
                    )
                }) =>
            {
                pending = tool_calls.iter().collect();
                expecting_assistant = false;
            }
            ModelConversationMessage::Tool {
                call_id,
                tool_id,
                provider_name,
                output,
            } if !expecting_assistant && !pending.is_empty() => {
                let expected = pending.remove(0);
                if call_id != &expected.call_id
                    || tool_id != &expected.tool_id
                    || provider_name != &expected.provider_name
                    || serde_json::to_vec(output)
                        .map_or(true, |encoded| encoded.len() > 16 * 1024 * 1024)
                {
                    return false;
                }
                tool_result_count = match tool_result_count.checked_add(1) {
                    Some(count) if count <= MAXIMUM_STATELESS_TOOL_RESULTS => count,
                    _ => return false,
                };
                if pending.is_empty() {
                    expecting_assistant = true;
                }
            }
            _ => return false,
        }
    }
    if expecting_assistant || pending.is_empty() || input.len() != pending.len() {
        return false;
    }
    input.iter().zip(pending).all(|(block, expected)| {
        matches!(block, ModelInputBlock::ToolResult { call_id, tool_id, .. }
            if call_id == &expected.call_id && tool_id == &expected.tool_id)
    }) && tool_result_count.saturating_add(input.len()) <= MAXIMUM_STATELESS_TOOL_RESULTS
}

fn valid_stateless_user_block(block: &ModelInputBlock) -> bool {
    match block {
        ModelInputBlock::Text { text } => text.len() <= 16 * 1024 * 1024,
        ModelInputBlock::Json { value } => {
            serde_json::to_vec(value).is_ok_and(|encoded| encoded.len() <= 16 * 1024 * 1024)
        }
        ModelInputBlock::Attachment { .. } | ModelInputBlock::ToolResult { .. } => false,
    }
}

fn tool_result_egress_bytes(request: &ModelRequest) -> Vec<u64> {
    let historical = request
        .continuation
        .as_ref()
        .into_iter()
        .flat_map(|continuation| match continuation {
            ModelContinuation::StatelessConversation { messages, .. } => messages.as_slice(),
            ModelContinuation::ProviderResponse { .. } => &[],
        })
        .filter_map(|message| match message {
            ModelConversationMessage::Tool {
                call_id,
                tool_id,
                output,
                ..
            } => Some((call_id, tool_id, output)),
            ModelConversationMessage::User { .. } | ModelConversationMessage::Assistant { .. } => {
                None
            }
        });
    historical
        .chain(request.input.iter().filter_map(|block| match block {
            ModelInputBlock::ToolResult {
                call_id,
                tool_id,
                output,
            } => Some((call_id, tool_id, output)),
            ModelInputBlock::Text { .. }
            | ModelInputBlock::Attachment { .. }
            | ModelInputBlock::Json { .. } => None,
        }))
        .map(|(call_id, tool_id, output)| {
            u64::try_from(
                call_id
                    .len()
                    .saturating_add(tool_id.len())
                    .saturating_add(output.to_string().len()),
            )
            .unwrap_or(u64::MAX)
        })
        .collect()
}

fn serialized_bytes(value: &impl Serialize) -> Option<u64> {
    serde_json::to_vec(value)
        .ok()
        .and_then(|encoded| u64::try_from(encoded.len()).ok())
}

fn unique_valid_values(values: &[String], valid: impl Fn(&str) -> bool) -> bool {
    let mut unique = BTreeSet::new();
    values
        .iter()
        .all(|value| valid(value) && unique.insert(value.as_str()))
}

fn valid_web_domain(value: &str) -> bool {
    let value = value.strip_prefix("*.").unwrap_or(value);
    !value.is_empty()
        && value.len() <= 253
        && !value.starts_with(['.', '-'])
        && !value.ends_with(['.', '-'])
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

fn valid_provider_reference(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 1_024
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'"' | b'\\'))
}

/// Custom function definition sent to a provider after local authorization.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelToolDefinition {
    /// Stable local catalog ID.
    pub tool_id: String,
    /// Provider-safe function name used only for this model request.
    pub provider_name: String,
    /// Exact local descriptor fingerprint.
    pub fingerprint: String,
    /// Bounded model-facing description.
    pub description: String,
    /// JSON Schema for arguments.
    pub parameters: serde_json::Value,
    /// Request provider-side strict schema enforcement when supported.
    pub strict: bool,
}

impl ModelToolDefinition {
    fn validate(&self) -> Result<(), ProviderError> {
        let provider_name_valid = !self.provider_name.is_empty()
            && self.provider_name.len() <= 64
            && self
                .provider_name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));
        let schema_is_object = self
            .parameters
            .as_object()
            .and_then(|schema| schema.get("type"))
            .and_then(serde_json::Value::as_str)
            == Some("object");
        let schema_is_bounded =
            serde_json::to_vec(&self.parameters).is_ok_and(|encoded| encoded.len() <= 1024 * 1024);
        if self.tool_id.is_empty()
            || self.tool_id.len() > 200
            || !provider_name_valid
            || self.fingerprint.is_empty()
            || self.fingerprint.len() > 512
            || self.description.len() > 2_000
            || !schema_is_object
            || !schema_is_bounded
        {
            return Err(ProviderError::InvalidRequest);
        }
        Ok(())
    }
}

/// Provider-hosted tool requested for one model call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ModelBuiltinTool {
    /// Provider-hosted web search.
    WebSearch {
        /// Optional administrator-approved domain restriction.
        allowed_domains: Vec<String>,
    },
    /// Provider-hosted search over already-authorized provider stores.
    FileSearch {
        /// Non-secret provider store references.
        store_ids: Vec<String>,
        /// Bounded result count.
        maximum_results: Option<u32>,
    },
    /// Provider-hosted code interpreter.
    CodeInterpreter,
    /// Provider-hosted image generation.
    ImageGeneration,
}

/// One exact egress manifest paired with its unforgeable allow proof.
#[derive(Clone, Debug)]
struct AuthorizedProviderTransfer {
    manifest: AiEgressManifest,
    proof: AuthorizedEgress,
}

/// Safe context accompanying a provider call.
///
/// Fields are private so an authorized manifest cannot be swapped after this
/// context is created. Provider adapters must call [`Self::validate_request`]
/// immediately before transport egress.
#[derive(Clone, Debug)]
pub struct ProviderRequestContext {
    session_id: AiSessionId,
    run_id: AiRunId,
    correlation_id: String,
    budget: AuthorizedBudgetReservation,
    transfers: Vec<AuthorizedProviderTransfer>,
    resolved_attachments: Vec<AiResolvedProviderAttachment>,
}

impl ProviderRequestContext {
    /// Creates a context with its required model-inference transfer.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::EgressDenied`] when proof, session, or run does not
    /// match the manifest exactly.
    pub fn new(
        session_id: AiSessionId,
        run_id: AiRunId,
        correlation_id: impl Into<String>,
        budget: AuthorizedBudgetReservation,
        manifest: AiEgressManifest,
        proof: AuthorizedEgress,
    ) -> Result<Self, AiError> {
        Self {
            session_id,
            run_id,
            correlation_id: correlation_id.into(),
            budget,
            transfers: Vec::new(),
            resolved_attachments: Vec::new(),
        }
        .with_authorized_transfer(manifest, proof)
    }

    /// Adds a separately authorized built-in or attachment transfer.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::EgressDenied`] for a mismatched proof/session/run.
    pub fn with_authorized_transfer(
        mut self,
        manifest: AiEgressManifest,
        proof: AuthorizedEgress,
    ) -> Result<Self, AiError> {
        if manifest.session_id != Some(self.session_id)
            || manifest.run_id != Some(self.run_id)
            || manifest.stable_hash() != proof.manifest_hash()
        {
            return Err(AiError::EgressDenied);
        }
        self.transfers
            .push(AuthorizedProviderTransfer { manifest, proof });
        Ok(self)
    }

    /// Installs exact reopened attachment payloads for this provider request.
    ///
    /// Callers must obtain these values from a trusted
    /// [`crate::AiProviderAttachmentResolver`] under a freshly rehydrated
    /// principal. This method validates exact request coverage but does not
    /// itself perform authorization or storage access.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::InvalidRequest`] unless every attachment block
    /// has exactly one matching resolved payload and there are no extras.
    pub fn with_resolved_attachments(
        mut self,
        request: &ModelRequest,
        resolved: Vec<AiResolvedProviderAttachment>,
    ) -> Result<Self, ProviderError> {
        let expected = request
            .input
            .iter()
            .filter(|block| matches!(block, ModelInputBlock::Attachment { .. }))
            .map(AiProviderAttachmentRequest::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ProviderError::InvalidRequest)?;
        if expected.len() != resolved.len()
            || expected
                .iter()
                .any(|request| !resolved.iter().any(|item| item.request() == request))
        {
            return Err(ProviderError::InvalidRequest);
        }
        self.resolved_attachments = resolved;
        Ok(self)
    }

    /// Returns exact reopened bytes for one attachment request.
    ///
    /// The returned payload was installed by the provider executor after
    /// current-principal resolution. Adapters must use it only for the current
    /// request transport and must not log, persist, or cache its bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::EgressDenied`] when the exact attachment was
    /// not freshly resolved into this request context.
    pub fn resolved_attachment(
        &self,
        request: &AiProviderAttachmentRequest,
    ) -> Result<&AiResolvedProviderAttachment, ProviderError> {
        self.resolved_attachments
            .iter()
            .find(|attachment| attachment.request() == request)
            .ok_or(ProviderError::EgressDenied)
    }

    /// Session ID.
    pub fn session_id(&self) -> AiSessionId {
        self.session_id
    }

    /// Run ID.
    pub fn run_id(&self) -> AiRunId {
        self.run_id
    }

    /// Correlation ID.
    pub fn correlation_id(&self) -> &str {
        &self.correlation_id
    }

    /// Decision IDs suitable for redacted audit linkage.
    pub fn egress_decision_ids(&self) -> impl Iterator<Item = crate::AiEgressDecisionId> + '_ {
        self.transfers
            .iter()
            .map(|transfer| transfer.proof.decision_id())
    }

    /// Budget reservation identifier suitable for usage/audit linkage.
    pub fn budget_reservation_id(&self) -> AiBudgetReservationId {
        self.budget.reservation_id()
    }

    /// Validates that each request capability has a matching exact transfer.
    pub fn validate_request(
        &self,
        provider_kind: &ProviderKind,
        request: &ModelRequest,
    ) -> Result<(), ProviderError> {
        request.validate()?;
        let requested_maximum_output_tokens = request.maximum_output_tokens.unwrap_or(0);
        if !self.budget.matches(
            self.run_id,
            provider_kind,
            &request.model,
            requested_maximum_output_tokens,
            OffsetDateTime::now_utc(),
        ) {
            return Err(ProviderError::BudgetDenied);
        }
        let attachment_count = request
            .input
            .iter()
            .filter(|block| matches!(block, ModelInputBlock::Attachment { .. }))
            .count() as u32;
        let estimated_bytes = request.conservative_egress_bytes();

        self.require_capability(
            provider_kind,
            request,
            AiEgressCapability::ModelInference,
            attachment_count,
            estimated_bytes,
        )?;
        let mut tool_result_bytes = tool_result_egress_bytes(request);
        let tool_result_transfers = self
            .transfers
            .iter()
            .filter(|transfer| transfer.manifest.capability == AiEgressCapability::ToolResult)
            .collect::<Vec<_>>();
        let mut tool_result_hashes = BTreeSet::new();
        let mut tool_result_sources = BTreeSet::new();
        if tool_result_transfers.len() != tool_result_bytes.len()
            || tool_result_transfers.iter().any(|transfer| {
                transfer.manifest.provider_kind != provider_kind.as_str()
                    || transfer.manifest.model != request.model
                    || transfer.manifest.stable_hash() != transfer.proof.manifest_hash()
                    || transfer.manifest.sources.len() != 1
                    || transfer.manifest.sources[0].kind != "application_tool_result"
                    || !tool_result_hashes.insert(transfer.manifest.stable_hash())
                    || !tool_result_sources.insert(transfer.manifest.sources[0].reference.as_str())
            })
        {
            return Err(ProviderError::EgressDenied);
        }
        tool_result_bytes.sort_unstable();
        let mut transfer_capacities = tool_result_transfers
            .iter()
            .map(|transfer| transfer.manifest.estimated_bytes)
            .collect::<Vec<_>>();
        transfer_capacities.sort_unstable();
        if tool_result_bytes
            .iter()
            .zip(transfer_capacities)
            .any(|(required, capacity)| capacity < *required)
        {
            return Err(ProviderError::EgressDenied);
        }
        for block in &request.input {
            match block {
                ModelInputBlock::Attachment { mime, .. } => {
                    let attachment_request = AiProviderAttachmentRequest::try_from(block)
                        .map_err(|_| ProviderError::InvalidRequest)?;
                    let capability = if mime.starts_with("image/") {
                        AiEgressCapability::ImageAnalysis
                    } else {
                        AiEgressCapability::ProviderFile
                    };
                    self.require_capability(
                        provider_kind,
                        request,
                        capability,
                        1,
                        estimated_bytes,
                    )?;
                    let exact_reference = block
                        .attachment_egress_reference()
                        .expect("matched attachment block");
                    if !self.transfers.iter().any(|transfer| {
                        transfer.manifest.provider_kind == provider_kind.as_str()
                            && transfer.manifest.model == request.model
                            && transfer.manifest.capability == capability
                            && transfer.manifest.sources.iter().any(|source| {
                                source.kind == "attachment"
                                    && source.reference == exact_reference
                                    && source.trust == crate::AiSourceTrust::UserProvided
                            })
                            && transfer.manifest.stable_hash() == transfer.proof.manifest_hash()
                    }) {
                        return Err(ProviderError::EgressDenied);
                    }
                    self.resolved_attachment(&attachment_request)?;
                }
                ModelInputBlock::ToolResult { .. } => {}
                ModelInputBlock::Text { .. } | ModelInputBlock::Json { .. } => {}
            }
        }
        for builtin in &request.builtin_tools {
            let capability = match builtin {
                ModelBuiltinTool::WebSearch { .. } => AiEgressCapability::WebSearch,
                ModelBuiltinTool::FileSearch { .. } => AiEgressCapability::ProviderFile,
                ModelBuiltinTool::CodeInterpreter => AiEgressCapability::CodeExecution,
                ModelBuiltinTool::ImageGeneration => AiEgressCapability::ImageGeneration,
            };
            self.require_capability(
                provider_kind,
                request,
                capability,
                attachment_count,
                estimated_bytes,
            )?;
        }
        Ok(())
    }

    #[cfg(any(
        feature = "provider-openai",
        feature = "provider-xai",
        feature = "provider-openai-compatible"
    ))]
    pub(crate) fn permits_retained_response(
        &self,
        provider_kind: &ProviderKind,
        request: &ModelRequest,
    ) -> bool {
        let mut matched = false;
        for transfer in &self.transfers {
            if transfer.manifest.provider_kind == provider_kind.as_str()
                && transfer.manifest.model == request.model
            {
                matched = true;
                if transfer.manifest.retention != AI_EGRESS_RETENTION_PROVIDER_RESPONSE {
                    return false;
                }
            }
        }
        matched
    }

    #[cfg(feature = "provider-openai-compatible")]
    pub(crate) fn permits_profile_destination_retention(
        &self,
        provider_kind: &ProviderKind,
        request: &ModelRequest,
        profile_id: &str,
        destination: &str,
        retention: &str,
    ) -> bool {
        let mut inference_matched = false;
        for transfer in &self.transfers {
            if transfer.manifest.provider_kind == provider_kind.as_str()
                && transfer.manifest.model == request.model
            {
                if transfer.manifest.provider_profile_id != profile_id
                    || transfer.manifest.destination != destination
                    || transfer.manifest.retention != retention
                {
                    return false;
                }
                inference_matched |=
                    transfer.manifest.capability == AiEgressCapability::ModelInference;
            }
        }
        inference_matched
    }

    fn require_capability(
        &self,
        provider_kind: &ProviderKind,
        request: &ModelRequest,
        capability: AiEgressCapability,
        attachment_count: u32,
        estimated_bytes: u64,
    ) -> Result<(), ProviderError> {
        let allowed = self.transfers.iter().any(|transfer| {
            transfer.manifest.provider_kind == provider_kind.as_str()
                && transfer.manifest.model == request.model
                && transfer.manifest.capability == capability
                && transfer.manifest.attachment_count >= attachment_count
                && transfer.manifest.estimated_bytes >= estimated_bytes
                && transfer.manifest.stable_hash() == transfer.proof.manifest_hash()
        });
        if allowed {
            Ok(())
        } else {
            Err(ProviderError::EgressDenied)
        }
    }
}

/// Normalized provider event. Unknown provider events remain non-fatal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderEvent {
    /// Provider accepted/started a response.
    ResponseStarted {
        /// Provider response reference.
        response_id: Option<String>,
    },
    /// Visible text delta.
    TextDelta {
        /// Delta text.
        text: String,
    },
    /// Provider-supported visible reasoning summary; never hidden chain of thought.
    ReasoningSummaryDelta {
        /// Summary delta.
        text: String,
    },
    /// Custom tool call began.
    ToolCallStarted {
        /// Provider call ID.
        call_id: String,
        /// Stable local tool ID.
        tool_id: String,
    },
    /// Partial custom-tool arguments.
    ToolArgumentsDelta {
        /// Provider call ID.
        call_id: String,
        /// Partial serialized arguments.
        delta: String,
    },
    /// Provider completed a custom tool call request.
    ToolCallCompleted {
        /// Provider call ID.
        call_id: String,
        /// Complete parsed arguments.
        arguments: serde_json::Value,
    },
    /// Provider built-in started.
    BuiltinToolStarted {
        /// Provider call ID.
        call_id: String,
        /// Built-in kind.
        kind: String,
    },
    /// Provider built-in completed.
    BuiltinToolCompleted {
        /// Provider call ID.
        call_id: String,
        /// Redacted normalized result metadata.
        result: serde_json::Value,
    },
    /// Citation emitted by the provider.
    Citation {
        /// Safe source URL/reference.
        source: String,
        /// Optional display title.
        title: Option<String>,
    },
    /// Usage counters.
    Usage {
        /// Input tokens.
        input_tokens: u64,
        /// Output tokens.
        output_tokens: u64,
        /// Cached input tokens.
        cached_input_tokens: u64,
    },
    /// Successful completion.
    ResponseCompleted {
        /// Provider response reference.
        response_id: Option<String>,
    },
    /// Unknown forward-compatible event metadata.
    Unknown {
        /// Provider event type.
        event_type: String,
    },
}

/// Provider adapter error. Diagnostic text must never contain credentials or
/// raw sensitive payloads.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProviderError {
    /// Provider configuration is invalid.
    #[error("invalid provider configuration: {0}")]
    InvalidConfiguration(String),
    /// Request failed bounded provider-neutral validation.
    #[error("invalid provider request")]
    InvalidRequest,
    /// Credential resolution failed closed.
    #[error("provider credential unavailable")]
    CredentialUnavailable,
    /// Exact provider/built-in/attachment egress proof was absent or stale.
    #[error("provider egress denied")]
    EgressDenied,
    /// Exact atomic budget-reservation proof was absent, stale, or mismatched.
    #[error("provider budget denied")]
    BudgetDenied,
    /// Capability is not supported by this adapter/model.
    #[error("provider capability unsupported")]
    Unsupported,
    /// Request was rate limited.
    #[error("provider rate limited")]
    RateLimited,
    /// Retryable remote/transport failure.
    #[error("provider temporarily unavailable")]
    Unavailable,
    /// Provider rejected safe request metadata.
    #[error("provider rejected request")]
    Rejected,
    /// Stream was cancelled.
    #[error("provider stream cancelled")]
    Cancelled,
}

/// Provider event stream.
pub type ProviderEventStream =
    Pin<Box<dyn Stream<Item = Result<ProviderEvent, ProviderError>> + Send + 'static>>;

/// Provider-neutral adapter.
#[async_trait]
pub trait AiProvider: Send + Sync {
    /// Provider family.
    fn provider_kind(&self) -> ProviderKind;

    /// Adapter/model capabilities used before routing.
    fn capabilities(&self) -> ProviderCapabilities;

    /// Starts a streaming request. The context can only be constructed from an
    /// allowed exact egress decision.
    async fn stream(
        &self,
        request: ModelRequest,
        context: ProviderRequestContext,
    ) -> Result<ProviderEventStream, ProviderError>;
}
