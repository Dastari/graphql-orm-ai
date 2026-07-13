//! Provider-neutral model adapter contract.

use std::collections::BTreeSet;
use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

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

/// Explicit provider-side conversation continuation.
///
/// A continuation is not an authorization proof. The next request still needs
/// fresh principal access, budget, and exact egress proofs. Provider adapters
/// may reject retained-response continuation unless deployment configuration
/// deliberately permits provider response storage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ModelContinuation {
    /// Continue from one exact provider response retained by the provider.
    ProviderResponse {
        /// Opaque provider response identifier.
        response_id: String,
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
        if self.model.is_empty() || self.model.len() > 200 {
            return Err(ProviderError::InvalidRequest);
        }
        if self.instructions.len() > 32 || self.input.len() > 256 || self.tools.len() > 128 {
            return Err(ProviderError::InvalidRequest);
        }
        if let Some(ModelContinuation::ProviderResponse { response_id }) = &self.continuation
            && !valid_provider_reference(response_id)
        {
            return Err(ProviderError::InvalidRequest);
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
        Ok(())
    }

    fn estimated_payload_bytes(&self) -> u64 {
        let instruction_bytes: usize = self.instructions.iter().map(String::len).sum();
        let input_bytes: usize = self
            .input
            .iter()
            .map(|block| match block {
                ModelInputBlock::Text { text } => text.len(),
                ModelInputBlock::Attachment {
                    attachment_id,
                    mime,
                    byte_count,
                    sha256,
                } => attachment_id
                    .len()
                    .saturating_add(mime.len())
                    .saturating_add(sha256.len())
                    .saturating_add(
                        (usize::try_from(*byte_count)
                            .unwrap_or(usize::MAX)
                            .saturating_add(2)
                            / 3)
                        .saturating_mul(4),
                    ),
                ModelInputBlock::Json { value } => value.to_string().len(),
                ModelInputBlock::ToolResult {
                    call_id,
                    tool_id,
                    output,
                } => call_id
                    .len()
                    .saturating_add(tool_id.len())
                    .saturating_add(output.to_string().len()),
            })
            .sum();
        instruction_bytes.saturating_add(input_bytes) as u64
    }
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
        if self.tool_id.is_empty()
            || self.tool_id.len() > 200
            || !provider_name_valid
            || self.fingerprint.is_empty()
            || self.description.len() > 2_000
            || !schema_is_object
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
        let estimated_bytes = request.estimated_payload_bytes();

        self.require_capability(
            provider_kind,
            request,
            AiEgressCapability::ModelInference,
            attachment_count,
            estimated_bytes,
        )?;
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
                ModelInputBlock::ToolResult {
                    call_id,
                    tool_id,
                    output,
                } => {
                    let bytes = call_id
                        .len()
                        .saturating_add(tool_id.len())
                        .saturating_add(output.to_string().len())
                        as u64;
                    self.require_capability(
                        provider_kind,
                        request,
                        AiEgressCapability::ToolResult,
                        0,
                        bytes,
                    )?;
                }
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

    #[cfg(feature = "provider-openai")]
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
