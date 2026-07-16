//! Native OpenAI Responses API adapter.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use futures::StreamExt;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use secrecy::ExposeSecret;
use serde_json::{Value, json};

#[cfg(feature = "provider-openai")]
use crate::{AiError, AiProviderFileDeletionRequest, AiProviderFileDeletionService};
use crate::{
    AiProvider, AiProviderAttachmentRequest, AiSecretStore, ModelBuiltinTool, ModelContinuation,
    ModelInputBlock, ModelRequest, ProviderBackgroundBinding, ProviderBackgroundSubmission,
    ProviderCapabilities, ProviderError, ProviderEvent, ProviderEventStream, ProviderKind,
    ProviderRequestContext, SecretRef,
};

#[cfg(feature = "provider-openai")]
const OPENAI_RESPONSES_ENDPOINT: &str = "https://api.openai.com/v1/responses";
#[cfg(feature = "provider-openai")]
const OPENAI_FILES_ENDPOINT: &str = "https://api.openai.com/v1/files";
#[cfg(feature = "provider-xai")]
const XAI_RESPONSES_ENDPOINT: &str = "https://api.x.ai/v1/responses";
const MAXIMUM_SSE_EVENT_BYTES: usize = 2 * 1024 * 1024;
const MAXIMUM_RESPONSES_STREAM_EVENTS: usize = 65_536;
const MAXIMUM_RESPONSES_TOOL_CALLS: usize = 64;
const MAXIMUM_RESPONSES_VISIBLE_BYTES: usize = 64 * 1024 * 1024;
const MAXIMUM_RESPONSES_TOOL_ARGUMENT_BYTES: usize = 16 * 1024 * 1024;
#[cfg(feature = "provider-openai")]
const MAXIMUM_BACKGROUND_ACKNOWLEDGEMENT_BYTES: usize = 1024 * 1024;
#[cfg(feature = "provider-openai")]
const MAXIMUM_FILE_DELETE_RESPONSE_BYTES: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResponsesFlavor {
    OpenAi,
    XAi,
    Compatible,
}

#[cfg(feature = "provider-openai-compatible")]
#[derive(Clone, Debug)]
pub(super) struct CompatibleRouteBinding {
    pub(super) profile_id: String,
    pub(super) destination: String,
    pub(super) retention: String,
}

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
    #[cfg(feature = "provider-openai")]
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
    flavor: ResponsesFlavor,
    provider_kind: ProviderKind,
    require_xai_zero_data_retention: bool,
    capabilities: ProviderCapabilities,
    #[cfg(feature = "provider-openai-compatible")]
    compatible_binding: Option<CompatibleRouteBinding>,
}

/// Native OpenAI exact-reference file deletion boundary.
///
/// This adapter implements only the host maintenance capability selected by
/// [`crate::AiAttachmentCleanupService`]. It cannot list, upload, search, or
/// retrieve file content. Each call resolves credentials just in time, sends
/// an exact fixed-endpoint delete, validates the exact deletion response, and
/// then requires authoritative retrieval to report the same file as absent.
#[cfg(feature = "provider-openai")]
pub struct OpenAiFileDeletionService {
    provider_profile_id: String,
    config: OpenAiProviderConfig,
    secrets: Arc<dyn AiSecretStore>,
    client: reqwest::Client,
    files_endpoint: String,
}

#[cfg(feature = "provider-openai")]
impl std::fmt::Debug for OpenAiFileDeletionService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenAiFileDeletionService")
            .field("provider_profile_id", &"[REDACTED]")
            .field("credential", &"[REDACTED]")
            .field(
                "organization_configured",
                &self.config.organization.is_some(),
            )
            .field("project_configured", &self.config.project.is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "provider-openai")]
impl OpenAiFileDeletionService {
    /// Builds a deletion service fixed to OpenAI's official Files endpoint
    /// with redirects disabled.
    ///
    /// The `store_responses` setting is irrelevant to file maintenance; the
    /// remaining credential, organization/project, and timeout settings are
    /// shared with [`OpenAiProvider`].
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::InvalidConfiguration`] for an invalid logical
    /// profile, safe header metadata, timeout, or HTTP client construction.
    pub fn new(
        provider_profile_id: impl Into<String>,
        config: OpenAiProviderConfig,
        secrets: Arc<dyn AiSecretStore>,
    ) -> Result<Self, ProviderError> {
        Self::build(
            provider_profile_id.into(),
            config,
            secrets,
            OPENAI_FILES_ENDPOINT.to_owned(),
        )
    }

    fn build(
        provider_profile_id: String,
        config: OpenAiProviderConfig,
        secrets: Arc<dyn AiSecretStore>,
        files_endpoint: String,
    ) -> Result<Self, ProviderError> {
        validate_optional_header(config.organization.as_deref())?;
        validate_optional_header(config.project.as_deref())?;
        if provider_profile_id.trim().is_empty()
            || provider_profile_id.len() > 200
            || provider_profile_id.chars().any(char::is_control)
            || config.timeout.is_zero()
            || config.timeout > Duration::from_secs(600)
        {
            return Err(ProviderError::InvalidConfiguration(
                "OpenAI file-deletion profile or timeout is invalid".to_owned(),
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
            provider_profile_id,
            config,
            secrets,
            client,
            files_endpoint,
        })
    }

    #[cfg(all(test, feature = "provider-openai"))]
    fn for_loopback_test(
        provider_profile_id: impl Into<String>,
        config: OpenAiProviderConfig,
        secrets: Arc<dyn AiSecretStore>,
        files_endpoint: String,
    ) -> Result<Self, ProviderError> {
        if !files_endpoint.starts_with("http://127.0.0.1:")
            || !files_endpoint.ends_with("/v1/files")
        {
            return Err(ProviderError::InvalidConfiguration(
                "test Files endpoint must use IPv4 loopback".to_owned(),
            ));
        }
        Self::build(provider_profile_id.into(), config, secrets, files_endpoint)
    }

    fn request_headers(&self) -> Result<HeaderMap, AiError> {
        let mut headers = HeaderMap::new();
        insert_optional_header(
            &mut headers,
            HeaderName::from_static("openai-organization"),
            self.config.organization.as_deref(),
        )
        .map_err(|_| AiError::ProviderFailed)?;
        insert_optional_header(
            &mut headers,
            HeaderName::from_static("openai-project"),
            self.config.project.as_deref(),
        )
        .map_err(|_| AiError::ProviderFailed)?;
        Ok(headers)
    }

    async fn send(
        &self,
        method: reqwest::Method,
        endpoint: &str,
    ) -> Result<reqwest::Response, AiError> {
        let secret = self
            .secrets
            .resolve(&self.config.credential)
            .await
            .map_err(|_| AiError::ProviderFailed)?;
        self.client
            .request(method, endpoint)
            .headers(self.request_headers()?)
            .bearer_auth(secret.expose_secret())
            .send()
            .await
            .map_err(|_| AiError::ProviderFailed)
    }
}

#[cfg(feature = "provider-openai")]
#[async_trait]
impl AiProviderFileDeletionService for OpenAiFileDeletionService {
    async fn delete_and_confirm_absent(
        &self,
        request: &AiProviderFileDeletionRequest,
    ) -> Result<(), AiError> {
        if request.provider_kind() != &ProviderKind::OpenAi
            || request.provider_profile_id() != self.provider_profile_id
            || request.artifact_kind() != "provider_file"
            || !valid_openai_file_id(request.provider_reference())
        {
            return Err(AiError::ProviderFailed);
        }
        let endpoint = format!("{}/{}", self.files_endpoint, request.provider_reference());
        let deletion = self.send(reqwest::Method::DELETE, &endpoint).await?;
        if deletion.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        if !deletion.status().is_success() {
            return Err(AiError::ProviderFailed);
        }
        let acknowledgement = bounded_json(deletion).await?;
        if acknowledgement.get("id").and_then(Value::as_str) != Some(request.provider_reference())
            || acknowledgement.get("object").and_then(Value::as_str) != Some("file")
            || acknowledgement.get("deleted").and_then(Value::as_bool) != Some(true)
        {
            return Err(AiError::ProviderFailed);
        }

        let retrieval = self.send(reqwest::Method::GET, &endpoint).await?;
        if retrieval.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(AiError::ProviderFailed)
        }
    }
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
    #[cfg(feature = "provider-openai")]
    pub fn new(
        config: OpenAiProviderConfig,
        secrets: Arc<dyn AiSecretStore>,
    ) -> Result<Self, ProviderError> {
        Self::build(
            config,
            secrets,
            OPENAI_RESPONSES_ENDPOINT.to_owned(),
            ResponsesFlavor::OpenAi,
            false,
            None,
            #[cfg(feature = "provider-openai-compatible")]
            None,
        )
    }

    fn build(
        config: OpenAiProviderConfig,
        secrets: Arc<dyn AiSecretStore>,
        endpoint: String,
        flavor: ResponsesFlavor,
        require_xai_zero_data_retention: bool,
        compatible_capabilities: Option<ProviderCapabilities>,
        #[cfg(feature = "provider-openai-compatible")] compatible_binding: Option<
            CompatibleRouteBinding,
        >,
    ) -> Result<Self, ProviderError> {
        validate_optional_header(config.organization.as_deref())?;
        validate_optional_header(config.project.as_deref())?;
        if flavor == ResponsesFlavor::XAi
            && (config.organization.is_some() || config.project.is_some())
        {
            return Err(ProviderError::InvalidConfiguration(
                "xAI does not accept OpenAI organization/project headers".to_owned(),
            ));
        }
        if flavor == ResponsesFlavor::Compatible
            && (config.organization.is_some()
                || config.project.is_some()
                || compatible_capabilities.is_none())
        {
            return Err(ProviderError::InvalidConfiguration(
                "OpenAI-compatible profile is incomplete".to_owned(),
            ));
        }
        #[cfg(feature = "provider-openai-compatible")]
        if flavor == ResponsesFlavor::Compatible && compatible_binding.is_none() {
            return Err(ProviderError::InvalidConfiguration(
                "OpenAI-compatible route binding is incomplete".to_owned(),
            ));
        }
        if flavor == ResponsesFlavor::XAi
            && require_xai_zero_data_retention
            && config.store_responses
        {
            return Err(ProviderError::InvalidConfiguration(
                "xAI retained response continuation is unavailable when zero-data-retention verification is required"
                    .to_owned(),
            ));
        }
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
        let provider_kind = match flavor {
            ResponsesFlavor::OpenAi => ProviderKind::OpenAi,
            ResponsesFlavor::XAi => ProviderKind::Xai,
            ResponsesFlavor::Compatible => ProviderKind::OpenAiCompatible,
        };
        let capabilities = compatible_capabilities.unwrap_or_else(|| {
            let mut capabilities = ProviderCapabilities {
                streaming: true,
                custom_tools: true,
                parallel_tool_calls: flavor == ResponsesFlavor::XAi,
                structured_output: true,
                provider_retained_continuation: config.store_responses,
                ..ProviderCapabilities::default()
            };
            if flavor == ResponsesFlavor::OpenAi {
                capabilities.image_input = true;
                capabilities.file_input = true;
                capabilities.web_search = true;
                capabilities.file_search = true;
                capabilities.code_execution = true;
                capabilities.image_generation = true;
                capabilities.background = true;
            }
            capabilities
        });
        if !capabilities.streaming
            || capabilities.provider_retained_continuation != config.store_responses
        {
            return Err(ProviderError::InvalidConfiguration(
                "Responses capabilities do not match transport configuration".to_owned(),
            ));
        }
        Ok(Self {
            config,
            secrets,
            client,
            endpoint,
            flavor,
            provider_kind,
            require_xai_zero_data_retention,
            capabilities,
            #[cfg(feature = "provider-openai-compatible")]
            compatible_binding,
        })
    }

    #[cfg(feature = "provider-xai")]
    pub(super) fn new_xai(
        config: OpenAiProviderConfig,
        secrets: Arc<dyn AiSecretStore>,
        require_zero_data_retention: bool,
    ) -> Result<Self, ProviderError> {
        Self::build(
            config,
            secrets,
            XAI_RESPONSES_ENDPOINT.to_owned(),
            ResponsesFlavor::XAi,
            require_zero_data_retention,
            None,
            #[cfg(feature = "provider-openai-compatible")]
            None,
        )
    }

    #[cfg(feature = "provider-openai-compatible")]
    pub(super) fn new_compatible(
        config: OpenAiProviderConfig,
        secrets: Arc<dyn AiSecretStore>,
        endpoint: String,
        capabilities: ProviderCapabilities,
        binding: CompatibleRouteBinding,
    ) -> Result<Self, ProviderError> {
        Self::build(
            config,
            secrets,
            endpoint,
            ResponsesFlavor::Compatible,
            false,
            Some(capabilities),
            Some(binding),
        )
    }

    #[cfg(all(test, feature = "provider-openai"))]
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
        Self::build(
            config,
            secrets,
            endpoint,
            ResponsesFlavor::OpenAi,
            false,
            None,
            #[cfg(feature = "provider-openai-compatible")]
            None,
        )
    }

    #[cfg(all(test, feature = "provider-xai"))]
    pub(super) fn for_xai_loopback_test(
        config: OpenAiProviderConfig,
        secrets: Arc<dyn AiSecretStore>,
        endpoint: String,
        require_zero_data_retention: bool,
    ) -> Result<Self, ProviderError> {
        if !endpoint.starts_with("http://127.0.0.1:") {
            return Err(ProviderError::InvalidConfiguration(
                "test endpoint must use IPv4 loopback".to_owned(),
            ));
        }
        Self::build(
            config,
            secrets,
            endpoint,
            ResponsesFlavor::XAi,
            require_zero_data_retention,
            None,
            #[cfg(feature = "provider-openai-compatible")]
            None,
        )
    }

    fn request_headers(&self) -> Result<HeaderMap, ProviderError> {
        let mut headers = HeaderMap::new();
        if self.flavor != ResponsesFlavor::OpenAi {
            return Ok(headers);
        }
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
        if request.continuation_mode != crate::ModelContinuationMode::ProviderRetained {
            return Err(ProviderError::Unsupported);
        }
        if request.input.is_empty()
            || (self.flavor != ResponsesFlavor::OpenAi
                && (request.maximum_output_tokens.is_none()
                    || !request.builtin_tools.is_empty()
                    || request.tools.iter().any(|tool| !tool.strict)
                    || request
                        .input
                        .iter()
                        .any(|block| matches!(block, ModelInputBlock::Attachment { .. }))))
        {
            return Err(ProviderError::InvalidRequest);
        }
        if self.flavor == ResponsesFlavor::Compatible
            && ((!request.tools.is_empty() && !self.capabilities.custom_tools)
                || (request.output_schema.is_some() && !self.capabilities.structured_output))
        {
            return Err(ProviderError::Unsupported);
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
            let mut definition = json!({
                "type": "function",
                "name": tool.provider_name,
                "description": tool.description,
                "parameters": tool.parameters
            });
            if self.flavor != ResponsesFlavor::XAi {
                definition["strict"] = Value::Bool(tool.strict);
            }
            tools.push(definition);
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
            "parallel_tool_calls": self.capabilities.parallel_tool_calls
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
        if let Some(maximum_builtin_tool_calls) = request.maximum_builtin_tool_calls {
            body["max_tool_calls"] = Value::from(maximum_builtin_tool_calls);
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
        self.provider_kind.clone()
    }

    fn capabilities(&self) -> ProviderCapabilities {
        self.capabilities.clone()
    }

    async fn submit_background(
        &self,
        request: ModelRequest,
        context: ProviderRequestContext,
        binding: ProviderBackgroundBinding,
    ) -> Result<ProviderBackgroundSubmission, ProviderError> {
        if self.flavor != ResponsesFlavor::OpenAi || !self.capabilities.background {
            return Err(ProviderError::Unsupported);
        }
        context.validate_request(&self.provider_kind, &request)?;
        if !context.permits_background_response(
            &self.provider_kind,
            &request,
            binding.provider_profile_id(),
        ) || !request.tools.is_empty()
            || !request.builtin_tools.is_empty()
            || request.maximum_output_tokens.is_none()
            || request.continuation.is_some()
            || request
                .input
                .iter()
                .any(|block| matches!(block, ModelInputBlock::Attachment { .. }))
        {
            return Err(ProviderError::EgressDenied);
        }
        let mut body = self.request_body(&request, &context)?;
        body["stream"] = Value::Bool(false);
        body["background"] = Value::Bool(true);
        body["metadata"] = json!({
            "graphql_orm_ai_submission": binding.submission_id().to_string(),
            "graphql_orm_ai_binding": binding.submission_key(),
        });
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
        if let Some(error) = openai_http_error(response.status()) {
            return Err(error);
        }
        if response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_none_or(|value| !value.to_ascii_lowercase().starts_with("application/json"))
        {
            return Err(ProviderError::Rejected);
        }
        let acknowledgement =
            bounded_provider_json(response, MAXIMUM_BACKGROUND_ACKNOWLEDGEMENT_BYTES).await?;
        let response_id = acknowledgement
            .get("id")
            .and_then(Value::as_str)
            .filter(|value| valid_provider_identifier(value, "resp_"))
            .ok_or(ProviderError::Rejected)?;
        let status = acknowledgement
            .get("status")
            .and_then(Value::as_str)
            .filter(|value| {
                matches!(
                    *value,
                    "queued" | "in_progress" | "completed" | "failed" | "incomplete" | "cancelled"
                )
            })
            .ok_or(ProviderError::Rejected)?;
        let created_at = acknowledgement
            .get("created_at")
            .and_then(Value::as_i64)
            .filter(|value| *value > 0)
            .ok_or(ProviderError::Rejected)?;
        let maximum_output_tokens = request
            .maximum_output_tokens
            .ok_or(ProviderError::Rejected)?;
        let metadata = acknowledgement
            .get("metadata")
            .and_then(Value::as_object)
            .ok_or(ProviderError::Rejected)?;
        if acknowledgement.get("object").and_then(Value::as_str) != Some("response")
            || acknowledgement.get("background").and_then(Value::as_bool) != Some(true)
            || acknowledgement.get("model").and_then(Value::as_str) != Some(request.model.as_str())
            || acknowledgement
                .get("max_output_tokens")
                .and_then(Value::as_u64)
                != Some(maximum_output_tokens)
            || acknowledgement.get("store").and_then(Value::as_bool)
                != Some(self.config.store_responses)
            || metadata
                .get("graphql_orm_ai_submission")
                .and_then(Value::as_str)
                != Some(binding.submission_id().to_string().as_str())
            || metadata
                .get("graphql_orm_ai_binding")
                .and_then(Value::as_str)
                != Some(binding.submission_key())
        {
            return Err(ProviderError::Rejected);
        }
        Ok(ProviderBackgroundSubmission::new(
            response_id.to_owned(),
            status.to_owned(),
            created_at,
            request.model,
            maximum_output_tokens,
            self.config.store_responses,
        ))
    }

    async fn stream(
        &self,
        request: ModelRequest,
        context: ProviderRequestContext,
    ) -> Result<ProviderEventStream, ProviderError> {
        context.validate_request(&self.provider_kind, &request)?;
        #[cfg(feature = "provider-openai-compatible")]
        if let Some(binding) = &self.compatible_binding
            && !context.permits_profile_destination_retention(
                &self.provider_kind,
                &request,
                &binding.profile_id,
                &binding.destination,
                &binding.retention,
            )
        {
            return Err(ProviderError::EgressDenied);
        }
        if self.flavor != ResponsesFlavor::Compatible
            && self.config.store_responses
            && !context.permits_retained_response(&self.provider_kind, &request)
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
        if self.flavor == ResponsesFlavor::XAi
            && self.require_xai_zero_data_retention
            && response
                .headers()
                .get("x-zero-data-retention")
                .is_none_or(|value| !value.as_bytes().eq_ignore_ascii_case(b"true"))
        {
            return Err(ProviderError::Rejected);
        }
        if response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_none_or(|value| !value.to_ascii_lowercase().starts_with("text/event-stream"))
        {
            return Err(ProviderError::Rejected);
        }

        let mut bytes = response.bytes_stream();
        let tool_ids = request
            .tools
            .iter()
            .map(|tool| (tool.provider_name.clone(), tool.tool_id.clone()))
            .collect::<BTreeMap<_, _>>();
        let allowed_builtin_kinds = request
            .builtin_tools
            .iter()
            .map(model_builtin_kind)
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        let output = async_stream::try_stream! {
            let mut decoder = SseDecoder::default();
            let mut normalizer = OpenAiEventNormalizer::new(
                request.model,
                request.maximum_output_tokens,
                tool_ids,
                allowed_builtin_kinds,
            );
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
            normalizer.finish()?;
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

#[cfg(feature = "provider-openai")]
async fn bounded_provider_json(
    response: reqwest::Response,
    maximum_bytes: usize,
) -> Result<Value, ProviderError> {
    let mut body = Vec::new();
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|_| ProviderError::Unavailable)?;
        if body.len().saturating_add(chunk.len()) > maximum_bytes {
            return Err(ProviderError::Rejected);
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| ProviderError::Rejected)
}

#[cfg(feature = "provider-openai")]
fn valid_provider_identifier(value: &str, prefix: &str) -> bool {
    value.starts_with(prefix)
        && (prefix.len() + 1..=200).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

#[cfg(feature = "provider-openai")]
fn valid_openai_file_id(value: &str) -> bool {
    value.starts_with("file-")
        && (6..=200).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

#[cfg(feature = "provider-openai")]
async fn bounded_json(response: reqwest::Response) -> Result<Value, AiError> {
    let mut body = Vec::new();
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|_| AiError::ProviderFailed)?;
        if body.len().saturating_add(chunk.len()) > MAXIMUM_FILE_DELETE_RESPONSE_BYTES {
            return Err(AiError::ProviderFailed);
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| AiError::ProviderFailed)
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
    arguments: String,
}

struct OpenAiEventNormalizer {
    expected_model: String,
    maximum_output_tokens: Option<u64>,
    tool_ids: BTreeMap<String, String>,
    allowed_builtin_kinds: BTreeSet<String>,
    function_calls: BTreeMap<String, FunctionCallState>,
    builtin_calls: BTreeMap<String, (String, String)>,
    seen_call_ids: BTreeSet<String>,
    completed_calls: BTreeSet<String>,
    response_id: Option<String>,
    started: bool,
    completed: bool,
    visible_bytes: usize,
    wire_events: usize,
}

impl OpenAiEventNormalizer {
    fn new(
        expected_model: String,
        maximum_output_tokens: Option<u64>,
        tool_ids: BTreeMap<String, String>,
        allowed_builtin_kinds: BTreeSet<String>,
    ) -> Self {
        Self {
            expected_model,
            maximum_output_tokens,
            tool_ids,
            allowed_builtin_kinds,
            function_calls: BTreeMap::new(),
            builtin_calls: BTreeMap::new(),
            seen_call_ids: BTreeSet::new(),
            completed_calls: BTreeSet::new(),
            response_id: None,
            started: false,
            completed: false,
            visible_bytes: 0,
            wire_events: 0,
        }
    }

    fn normalize(&mut self, event: &Value) -> Result<Vec<ProviderEvent>, ProviderError> {
        self.wire_events = self
            .wire_events
            .checked_add(1)
            .filter(|count| *count <= MAXIMUM_RESPONSES_STREAM_EVENTS)
            .ok_or(ProviderError::Rejected)?;
        let event_type = event
            .get("type")
            .and_then(Value::as_str)
            .ok_or(ProviderError::Rejected)?;
        if !valid_responses_event_type(event_type) {
            return Err(ProviderError::Rejected);
        }
        match event_type {
            "response.created" => self.response_created(event),
            "response.failed" | "error" => Err(openai_stream_error(event)),
            _ if !self.started || self.completed => Err(ProviderError::Rejected),
            "response.output_text.delta" => {
                let text = required_string(event, "delta")?;
                self.record_visible_bytes(text.len())?;
                Ok(vec![ProviderEvent::TextDelta { text }])
            }
            "response.reasoning_summary_text.delta" => {
                let text = required_string(event, "delta")?;
                self.record_visible_bytes(text.len())?;
                Ok(vec![ProviderEvent::ReasoningSummaryDelta { text }])
            }
            "response.output_item.added" => self.output_item_added(event),
            "response.function_call_arguments.delta" => {
                let item_id = required_string(event, "item_id")?;
                let state = self
                    .function_calls
                    .get_mut(&item_id)
                    .ok_or(ProviderError::Rejected)?;
                let delta = required_string(event, "delta")?;
                if state.arguments.len().saturating_add(delta.len())
                    > MAXIMUM_RESPONSES_TOOL_ARGUMENT_BYTES
                {
                    return Err(ProviderError::Rejected);
                }
                state.arguments.push_str(&delta);
                Ok(vec![ProviderEvent::ToolArgumentsDelta {
                    call_id: state.call_id.clone(),
                    delta,
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
            "response.completed" => self.response_completed(event),
            _ => Ok(vec![ProviderEvent::Unknown {
                event_type: event_type.to_owned(),
            }]),
        }
    }

    fn response_created(&mut self, event: &Value) -> Result<Vec<ProviderEvent>, ProviderError> {
        let response = event.get("response").ok_or(ProviderError::Rejected)?;
        let response_id = required_string(response, "id")?;
        if self.started
            || self.completed
            || !valid_responses_reference(&response_id)
            || response.get("model").and_then(Value::as_str) != Some(self.expected_model.as_str())
        {
            return Err(ProviderError::Rejected);
        }
        self.started = true;
        self.response_id = Some(response_id.clone());
        Ok(vec![ProviderEvent::ResponseStarted {
            response_id: Some(response_id),
        }])
    }

    fn response_completed(&mut self, event: &Value) -> Result<Vec<ProviderEvent>, ProviderError> {
        let response = event.get("response").ok_or(ProviderError::Rejected)?;
        let response_id = required_string(response, "id")?;
        if response_id != self.response_id.as_deref().ok_or(ProviderError::Rejected)?
            || response.get("model").and_then(Value::as_str) != Some(self.expected_model.as_str())
            || response.get("status").and_then(Value::as_str) != Some("completed")
            || self
                .function_calls
                .values()
                .any(|state| !self.completed_calls.contains(&state.call_id))
            || self
                .builtin_calls
                .values()
                .any(|(call_id, _)| !self.completed_calls.contains(call_id))
        {
            return Err(ProviderError::Rejected);
        }
        let usage = response.get("usage").ok_or(ProviderError::Rejected)?;
        let input_tokens = required_u64(usage, "input_tokens")?;
        let output_tokens = required_u64(usage, "output_tokens")?;
        let cached_input_tokens =
            optional_nested_u64(usage, &["input_tokens_details", "cached_tokens"])?;
        if cached_input_tokens > input_tokens
            || self
                .maximum_output_tokens
                .is_some_and(|maximum| output_tokens > maximum)
        {
            return Err(ProviderError::Rejected);
        }
        self.completed = true;
        Ok(vec![
            ProviderEvent::Usage {
                input_tokens,
                output_tokens,
                cached_input_tokens,
            },
            ProviderEvent::ResponseCompleted {
                response_id: Some(response_id),
            },
        ])
    }

    fn record_visible_bytes(&mut self, additional: usize) -> Result<(), ProviderError> {
        self.visible_bytes = self
            .visible_bytes
            .checked_add(additional)
            .filter(|bytes| *bytes <= MAXIMUM_RESPONSES_VISIBLE_BYTES)
            .ok_or(ProviderError::Rejected)?;
        Ok(())
    }

    fn finish(&self) -> Result<(), ProviderError> {
        if self.started && self.completed {
            Ok(())
        } else {
            Err(ProviderError::Unavailable)
        }
    }

    fn output_item_added(&mut self, event: &Value) -> Result<Vec<ProviderEvent>, ProviderError> {
        let item = event.get("item").ok_or(ProviderError::Rejected)?;
        let item_type = required_string(item, "type")?;
        let item_id = required_string(item, "id")?;
        if !valid_responses_reference(&item_id)
            || self.function_calls.contains_key(&item_id)
            || self.builtin_calls.contains_key(&item_id)
        {
            return Err(ProviderError::Rejected);
        }
        if item_type == "function_call" {
            let provider_name = required_string(item, "name")?;
            let tool_id = self
                .tool_ids
                .get(&provider_name)
                .ok_or(ProviderError::Rejected)?
                .clone();
            let call_id = required_string(item, "call_id")?;
            if self.seen_call_ids.len() >= MAXIMUM_RESPONSES_TOOL_CALLS
                || !valid_responses_reference(&call_id)
                || !self.seen_call_ids.insert(call_id.clone())
            {
                return Err(ProviderError::Rejected);
            }
            self.function_calls.insert(
                item_id,
                FunctionCallState {
                    call_id: call_id.clone(),
                    arguments: match item.get("arguments") {
                        None => String::new(),
                        Some(value) => value
                            .as_str()
                            .filter(|value| value.len() <= MAXIMUM_RESPONSES_TOOL_ARGUMENT_BYTES)
                            .ok_or(ProviderError::Rejected)?
                            .to_owned(),
                    },
                },
            );
            return Ok(vec![ProviderEvent::ToolCallStarted { call_id, tool_id }]);
        }
        if let Some(kind) = builtin_kind(&item_type) {
            if !self.allowed_builtin_kinds.contains(kind) {
                return Err(ProviderError::Rejected);
            }
            let call_id = item
                .get("call_id")
                .or_else(|| item.get("id"))
                .and_then(Value::as_str)
                .ok_or(ProviderError::Rejected)?
                .to_owned();
            if self.seen_call_ids.len() >= MAXIMUM_RESPONSES_TOOL_CALLS
                || !valid_responses_reference(&call_id)
                || !self.seen_call_ids.insert(call_id.clone())
            {
                return Err(ProviderError::Rejected);
            }
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
        if arguments.len() > MAXIMUM_RESPONSES_TOOL_ARGUMENT_BYTES
            || (!state.arguments.is_empty() && state.arguments != arguments)
        {
            return Err(ProviderError::Rejected);
        }
        let arguments: Value =
            serde_json::from_str(arguments).map_err(|_| ProviderError::Rejected)?;
        if !arguments.is_object() {
            return Err(ProviderError::Rejected);
        }
        if !self.completed_calls.insert(state.call_id.clone()) {
            return Ok(Vec::new());
        }
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

fn required_u64(value: &Value, field: &str) -> Result<u64, ProviderError> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or(ProviderError::Rejected)
}

fn optional_nested_u64(value: &Value, path: &[&str]) -> Result<u64, ProviderError> {
    let mut current = value;
    for segment in path {
        let object = current.as_object().ok_or(ProviderError::Rejected)?;
        let Some(next) = object.get(*segment) else {
            return Ok(0);
        };
        if next.is_null() {
            return Ok(0);
        }
        current = next;
    }
    current.as_u64().ok_or(ProviderError::Rejected)
}

fn valid_responses_reference(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 1_024
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'"' | b'\\'))
}

fn valid_responses_event_type(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 200
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
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

fn model_builtin_kind(tool: &ModelBuiltinTool) -> &'static str {
    match tool {
        ModelBuiltinTool::WebSearch { .. } => "web_search",
        ModelBuiltinTool::FileSearch { .. } => "file_search",
        ModelBuiltinTool::CodeInterpreter => "code_interpreter",
        ModelBuiltinTool::ImageGeneration => "image_generation",
    }
}

#[cfg(all(test, feature = "provider-openai"))]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    struct CountingSecrets {
        reference: SecretRef,
        resolves: AtomicUsize,
    }

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
    impl AiSecretStore for CountingSecrets {
        async fn resolve(&self, reference: &SecretRef) -> Result<SecretString, SecretError> {
            self.resolves.fetch_add(1, Ordering::SeqCst);
            if reference == &self.reference {
                Ok(SecretString::from("not-a-real-key".to_owned()))
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

    async fn file_mock_server(
        responses: Vec<(String, String)>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener should bind");
        let address = listener.local_addr().expect("listener should have address");
        let task = tokio::spawn(async move {
            let mut requests = Vec::with_capacity(responses.len());
            for (status, body) in responses {
                let (mut socket, _) = listener.accept().await.expect("request should connect");
                let mut request = vec![0_u8; 32 * 1024];
                let count = socket
                    .read(&mut request)
                    .await
                    .expect("request should read");
                let request = String::from_utf8_lossy(&request[..count]);
                requests.push(request.lines().next().unwrap_or_default().to_owned());
                let headers = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
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
            }
            requests
        });
        (format!("http://{address}/v1/files"), task)
    }

    async fn background_mock_server(
        mismatched_store: bool,
    ) -> (String, tokio::task::JoinHandle<Value>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener should bind");
        let address = listener.local_addr().expect("listener should have address");
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("request should connect");
            let mut request = vec![0_u8; 64 * 1024];
            let count = socket
                .read(&mut request)
                .await
                .expect("request should read");
            let request = String::from_utf8(request[..count].to_vec())
                .expect("synthetic request should be UTF-8");
            let body = request
                .split_once("\r\n\r\n")
                .map(|(_, body)| body)
                .expect("request should contain a body");
            let body: Value =
                serde_json::from_str(body).expect("request body should contain exact JSON");
            let acknowledgement = json!({
                "id": "resp_background_exact_1",
                "object": "response",
                "created_at": 2_000_000_000_i64,
                "status": "queued",
                "background": true,
                "model": body["model"],
                "max_output_tokens": body["max_output_tokens"],
                "store": if mismatched_store {
                    Value::Bool(!body["store"].as_bool().unwrap_or(false))
                } else {
                    body["store"].clone()
                },
                "metadata": body["metadata"],
            });
            let encoded = acknowledgement.to_string();
            let headers = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                encoded.len()
            );
            socket
                .write_all(headers.as_bytes())
                .await
                .expect("headers should write");
            socket
                .write_all(encoded.as_bytes())
                .await
                .expect("body should write");
            body
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
                tool_units: 64,
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
            64,
            time::OffsetDateTime::now_utc(),
        )
        .expect("budget should authorize");
        ProviderRequestContext::new(session_id, run_id, "test", budget, manifest, proof)
            .expect("context should validate")
    }

    fn background_context(model: &str, estimated_bytes: u64) -> ProviderRequestContext {
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
            purpose: "test_background".to_owned(),
            retention: crate::AI_EGRESS_RETENTION_PROVIDER_RESPONSE.to_owned(),
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
            0,
            time::OffsetDateTime::now_utc(),
        )
        .expect("budget should authorize");
        ProviderRequestContext::new(session_id, run_id, "test", budget, manifest, proof)
            .expect("context should validate")
    }

    #[tokio::test]
    async fn exact_background_submission_is_bounded_and_metadata_bound() {
        let reference =
            SecretRef::parse("openai/background-test").expect("test secret reference should parse");
        let (endpoint, server) = background_mock_server(false).await;
        let provider = OpenAiProvider::for_loopback_test(
            OpenAiProviderConfig::new(reference.clone()),
            Arc::new(TestSecrets(reference, "not-a-real-key".to_owned())),
            endpoint,
        )
        .expect("background provider should build");
        let request = ModelRequest {
            model: "test-model".to_owned(),
            instructions: vec!["Respond eventually.".to_owned()],
            input: vec![ModelInputBlock::Text {
                text: "synthetic background input".to_owned(),
            }],
            continuation: None,
            continuation_mode: crate::ModelContinuationMode::ProviderRetained,
            tools: vec![],
            builtin_tools: vec![],
            maximum_builtin_tool_calls: None,
            output_schema: None,
            maximum_output_tokens: Some(32),
        };
        let submission_id = uuid::Uuid::new_v4();
        let binding =
            ProviderBackgroundBinding::new(submission_id, "a".repeat(64), "profile-1".to_owned());
        let debug = format!("{binding:?}");
        assert!(!debug.contains(&submission_id.to_string()));
        assert!(!debug.contains(&"a".repeat(64)));
        let estimated_bytes = request.conservative_egress_bytes();
        let acknowledgement = provider
            .submit_background(
                request,
                background_context("test-model", estimated_bytes),
                binding,
            )
            .await
            .expect("exact background request should be accepted");
        assert_eq!(acknowledgement.response_id(), "resp_background_exact_1");
        assert_eq!(acknowledgement.status(), "queued");
        assert_eq!(acknowledgement.provider_model(), "test-model");
        assert_eq!(acknowledgement.maximum_output_tokens(), 32);
        assert!(!acknowledgement.provider_store());
        assert!(!format!("{acknowledgement:?}").contains("resp_background_exact_1"));
        let body = server.await.expect("background server should finish");
        assert_eq!(body["background"], true);
        assert_eq!(body["stream"], false);
        assert_eq!(body["store"], false);
        assert_eq!(
            body["metadata"]["graphql_orm_ai_submission"],
            submission_id.to_string()
        );
        assert_eq!(body["metadata"]["graphql_orm_ai_binding"], "a".repeat(64));
    }

    #[tokio::test]
    async fn background_submission_rejects_swapped_storage_acknowledgement() {
        let reference = SecretRef::parse("openai/background-storage-swap-test")
            .expect("test secret reference should parse");
        let (endpoint, server) = background_mock_server(true).await;
        let provider = OpenAiProvider::for_loopback_test(
            OpenAiProviderConfig::new(reference.clone()),
            Arc::new(TestSecrets(reference, "not-a-real-key".to_owned())),
            endpoint,
        )
        .expect("background provider should build");
        let request = ModelRequest {
            model: "test-model".to_owned(),
            instructions: vec!["Respond eventually.".to_owned()],
            input: vec![ModelInputBlock::Text {
                text: "synthetic background input".to_owned(),
            }],
            continuation: None,
            continuation_mode: crate::ModelContinuationMode::ProviderRetained,
            tools: vec![],
            builtin_tools: vec![],
            maximum_builtin_tool_calls: None,
            output_schema: None,
            maximum_output_tokens: Some(32),
        };
        let binding = ProviderBackgroundBinding::new(
            uuid::Uuid::new_v4(),
            "b".repeat(64),
            "profile-1".to_owned(),
        );
        let estimated_bytes = request.conservative_egress_bytes();

        assert!(matches!(
            provider
                .submit_background(
                    request,
                    background_context("test-model", estimated_bytes),
                    binding,
                )
                .await,
            Err(ProviderError::Rejected)
        ));
        server.await.expect("background server should finish");
    }

    fn with_capability(
        context: ProviderRequestContext,
        model: &str,
        capability: AiEgressCapability,
        estimated_bytes: u64,
    ) -> ProviderRequestContext {
        let manifest = AiEgressManifest {
            provider_profile_id: "profile-1".to_owned(),
            provider_kind: "openai".to_owned(),
            model: model.to_owned(),
            destination: "openai".to_owned(),
            destination_trust: AiDestinationTrust::ManagedProvider,
            capability,
            scope: AiScope::new("project", "test"),
            session_id: Some(context.session_id()),
            run_id: Some(context.run_id()),
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
            .expect("capability manifest should authorize");
        context
            .with_authorized_transfer(manifest, proof)
            .expect("capability transfer should bind")
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
            continuation_mode: crate::ModelContinuationMode::ProviderRetained,
            tools: Vec::new(),
            builtin_tools: Vec::new(),
            maximum_builtin_tool_calls: None,
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
    fn builtin_call_ceiling_maps_to_the_openai_wire_contract() {
        let reference = SecretRef::parse("openai/builtin-limit-test")
            .expect("test secret reference should parse");
        let provider = OpenAiProvider::new(
            OpenAiProviderConfig::new(reference.clone()),
            Arc::new(TestSecrets(reference, "not-a-real-key".to_owned())),
        )
        .expect("provider should build");
        let request = ModelRequest {
            model: "test-model".to_owned(),
            instructions: vec![],
            input: vec![ModelInputBlock::Text {
                text: "synthetic search".to_owned(),
            }],
            continuation: None,
            continuation_mode: crate::ModelContinuationMode::ProviderRetained,
            tools: vec![],
            builtin_tools: vec![ModelBuiltinTool::WebSearch {
                allowed_domains: vec!["example.com".to_owned()],
            }],
            maximum_builtin_tool_calls: Some(3),
            output_schema: None,
            maximum_output_tokens: Some(32),
        };
        let estimated_bytes = request.conservative_egress_bytes();
        let provider_context = with_capability(
            context("test-model", estimated_bytes),
            "test-model",
            AiEgressCapability::WebSearch,
            estimated_bytes,
        );
        let body = provider
            .request_body(&request, &provider_context)
            .expect("authorized web search should map");
        assert_eq!(body["max_tool_calls"], 3);
        assert_eq!(body["tools"][0]["type"], "web_search");
    }

    #[tokio::test]
    async fn exact_file_deletion_requires_authoritative_absence() {
        let file_id = "file-maintenance_test";
        let deleted = format!(r#"{{"id":"{file_id}","object":"file","deleted":true}}"#);
        let (endpoint, server) = file_mock_server(vec![
            ("200 OK".to_owned(), deleted),
            (
                "404 Not Found".to_owned(),
                r#"{"error":{"code":"not_found"}}"#.to_owned(),
            ),
        ])
        .await;
        let reference = SecretRef::parse("openai/file-maintenance-test")
            .expect("test secret reference should parse");
        let service = OpenAiFileDeletionService::for_loopback_test(
            "profile-openai",
            OpenAiProviderConfig::new(reference.clone()),
            Arc::new(TestSecrets(reference, "not-a-real-key".to_owned())),
            endpoint,
        )
        .expect("file deletion service should build");
        let request = AiProviderFileDeletionRequest::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            "provider_file".to_owned(),
            ProviderKind::OpenAi,
            "profile-openai".to_owned(),
            file_id.to_owned(),
        );
        let request_debug = format!("{request:?}");
        assert!(!request_debug.contains("profile-openai"));
        assert!(!request_debug.contains(file_id));

        service
            .delete_and_confirm_absent(&request)
            .await
            .expect("exact deletion plus not-found retrieval should confirm absence");
        let requests = server.await.expect("file server should finish");
        assert_eq!(
            requests,
            vec![
                format!("DELETE /v1/files/{file_id} HTTP/1.1"),
                format!("GET /v1/files/{file_id} HTTP/1.1"),
            ]
        );
        let debug = format!("{service:?}");
        assert!(!debug.contains("profile-openai"));
        assert!(!debug.contains("file-maintenance-test"));
        assert!(!debug.contains("not-a-real-key"));
    }

    #[tokio::test]
    async fn absent_file_is_idempotent() {
        let (endpoint, server) = file_mock_server(vec![(
            "404 Not Found".to_owned(),
            r#"{"error":{"code":"not_found"}}"#.to_owned(),
        )])
        .await;
        let reference =
            SecretRef::parse("openai/file-absent-test").expect("test reference should parse");
        let service = OpenAiFileDeletionService::for_loopback_test(
            "profile-openai",
            OpenAiProviderConfig::new(reference.clone()),
            Arc::new(TestSecrets(reference, "not-a-real-key".to_owned())),
            endpoint,
        )
        .expect("file deletion service should build");
        let request = AiProviderFileDeletionRequest::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            "provider_file".to_owned(),
            ProviderKind::OpenAi,
            "profile-openai".to_owned(),
            "file-already_absent".to_owned(),
        );
        service
            .delete_and_confirm_absent(&request)
            .await
            .expect("not found should prove idempotent absence");
        assert_eq!(
            server.await.expect("file server should finish"),
            vec!["DELETE /v1/files/file-already_absent HTTP/1.1"]
        );
    }

    #[tokio::test]
    async fn wrong_file_owner_fails_before_credential_resolution() {
        let reference =
            SecretRef::parse("openai/file-owner-test").expect("test reference should parse");
        let secrets = Arc::new(CountingSecrets {
            reference: reference.clone(),
            resolves: AtomicUsize::new(0),
        });
        let service = OpenAiFileDeletionService::for_loopback_test(
            "profile-openai",
            OpenAiProviderConfig::new(reference),
            secrets.clone(),
            "http://127.0.0.1:9/v1/files".to_owned(),
        )
        .expect("file deletion service should build");

        let wrong_provider = AiProviderFileDeletionRequest::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            "provider_file".to_owned(),
            ProviderKind::Xai,
            "profile-openai".to_owned(),
            "file-wrong_provider".to_owned(),
        );
        assert!(matches!(
            service.delete_and_confirm_absent(&wrong_provider).await,
            Err(AiError::ProviderFailed)
        ));

        let wrong_profile = AiProviderFileDeletionRequest::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            "provider_file".to_owned(),
            ProviderKind::OpenAi,
            "profile-other".to_owned(),
            "file-wrong_profile".to_owned(),
        );
        assert!(matches!(
            service.delete_and_confirm_absent(&wrong_profile).await,
            Err(AiError::ProviderFailed)
        ));
        assert_eq!(secrets.resolves.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn ambiguous_file_deletion_responses_fail_closed() {
        let file_id = "file-ambiguous_test";
        let reference =
            SecretRef::parse("openai/file-ambiguous-test").expect("test reference should parse");
        let (endpoint, server) = file_mock_server(vec![(
            "200 OK".to_owned(),
            r#"{"id":"file-other","object":"file","deleted":true}"#.to_owned(),
        )])
        .await;
        let service = OpenAiFileDeletionService::for_loopback_test(
            "profile-openai",
            OpenAiProviderConfig::new(reference.clone()),
            Arc::new(TestSecrets(reference, "not-a-real-key".to_owned())),
            endpoint,
        )
        .expect("file deletion service should build");
        let request = AiProviderFileDeletionRequest::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            "provider_file".to_owned(),
            ProviderKind::OpenAi,
            "profile-openai".to_owned(),
            file_id.to_owned(),
        );
        assert!(matches!(
            service.delete_and_confirm_absent(&request).await,
            Err(AiError::ProviderFailed)
        ));
        assert_eq!(
            server.await.expect("file server should finish"),
            vec![format!("DELETE /v1/files/{file_id} HTTP/1.1")]
        );

        let deleted = format!(r#"{{"id":"{file_id}","object":"file","deleted":true}}"#);
        let (endpoint, server) = file_mock_server(vec![
            ("200 OK".to_owned(), deleted),
            (
                "200 OK".to_owned(),
                r#"{"id":"file-ambiguous_test"}"#.to_owned(),
            ),
        ])
        .await;
        let reference = SecretRef::parse("openai/file-still-present-test")
            .expect("test reference should parse");
        let service = OpenAiFileDeletionService::for_loopback_test(
            "profile-openai",
            OpenAiProviderConfig::new(reference.clone()),
            Arc::new(TestSecrets(reference, "not-a-real-key".to_owned())),
            endpoint,
        )
        .expect("file deletion service should build");
        assert!(matches!(
            service.delete_and_confirm_absent(&request).await,
            Err(AiError::ProviderFailed)
        ));
        assert_eq!(server.await.expect("file server should finish").len(), 2);
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
            continuation_mode: crate::ModelContinuationMode::ProviderRetained,
            tools: vec![],
            builtin_tools: vec![],
            maximum_builtin_tool_calls: None,
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
            continuation_mode: crate::ModelContinuationMode::ProviderRetained,
            tools: vec![],
            builtin_tools: vec![],
            maximum_builtin_tool_calls: None,
            output_schema: None,
            maximum_output_tokens: Some(32),
        };

        let estimated_bytes = request.conservative_egress_bytes();
        assert!(matches!(
            provider
                .stream(request, context("test-model", estimated_bytes))
                .await,
            Err(ProviderError::EgressDenied)
        ));
    }

    #[tokio::test]
    async fn responses_sse_is_normalized_without_retaining_secret_or_raw_body() {
        let sse = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_test\",\"model\":\"test-model\"}}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_test\",\"model\":\"test-model\",\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":1,\"input_tokens_details\":{\"cached_tokens\":1}}}}\n\n"
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
            continuation_mode: crate::ModelContinuationMode::ProviderRetained,
            tools: vec![],
            builtin_tools: vec![],
            maximum_builtin_tool_calls: None,
            output_schema: None,
            maximum_output_tokens: Some(32),
        };
        let estimated_bytes = request.conservative_egress_bytes();
        let events = provider
            .stream(request, context("test-model", estimated_bytes))
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
            continuation_mode: crate::ModelContinuationMode::ProviderRetained,
            tools: vec![],
            builtin_tools: vec![],
            maximum_builtin_tool_calls: None,
            output_schema: None,
            maximum_output_tokens: Some(64),
        };
        let estimated_bytes = request.conservative_egress_bytes();
        let events = provider
            .stream(request, context(&model, estimated_bytes))
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
