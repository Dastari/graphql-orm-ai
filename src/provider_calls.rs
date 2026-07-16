//! Security-ordered execution of one provider call for a fenced run.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use agql_auth::{Clock, ResolvedPrincipal};
use async_trait::async_trait;
use futures::StreamExt;

use crate::{
    AiAgentRuleResolution, AiApprovalRule, AiBudgetAmounts, AiBudgetReconciliation,
    AiBudgetReconciliationOutcome, AiBudgetReservation, AiBudgetReservationId,
    AiBudgetReservationRequest, AiBudgetService, AiEgressCapability, AiEgressDecisionAudit,
    AiEgressManifest, AiError, AiLiveDeltaBatch, AiLiveDeltaCoalescer, AiLiveDeltaCoalescerLimits,
    AiLiveDeltaPersistenceContext, AiLiveDeltaSink, AiProviderAttachmentRequest,
    AiProviderAttachmentResolver, AiResolvedProviderAttachment, AiRuleProviderCapability,
    AiRuleRunUsage, AiRunLease, AiRunState, AiRuntime, AiScope, AiSessionAction, AiToolPolicySet,
    ModelBuiltinTool, ModelContinuation, ModelContinuationMode, ModelConversationMessage,
    ModelConversationToolCall, ModelInputBlock, ModelRequest, ProviderEvent, ProviderKind,
    ProviderRequestContext, ToolMaturity,
};

const MAXIMUM_PROVIDER_TRANSFERS: usize = 288;

/// Deployment-owned bounds for a single normalized provider stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiProviderCallLimits {
    maximum_events: usize,
    maximum_event_bytes: usize,
    maximum_total_event_bytes: usize,
    maximum_tool_calls: usize,
    maximum_builtin_tool_calls: usize,
}

impl AiProviderCallLimits {
    /// Creates validated provider-stream bounds.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless event count is within
    /// `1..=65_536`, individual and total byte limits are positive, the total
    /// is at least the individual limit, and both are at most 64 MiB.
    pub fn new(
        maximum_events: usize,
        maximum_event_bytes: usize,
        maximum_total_event_bytes: usize,
    ) -> Result<Self, AiError> {
        const MAXIMUM_BYTES: usize = 64 * 1024 * 1024;
        if !(1..=65_536).contains(&maximum_events)
            || maximum_event_bytes == 0
            || maximum_event_bytes > MAXIMUM_BYTES
            || maximum_total_event_bytes < maximum_event_bytes
            || maximum_total_event_bytes > MAXIMUM_BYTES
        {
            return Err(AiError::InvalidConfiguration(
                "invalid provider-call stream limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_events,
            maximum_event_bytes,
            maximum_total_event_bytes,
            maximum_tool_calls: 8,
            maximum_builtin_tool_calls: 8,
        })
    }

    /// Sets the maximum number of custom application-tool calls accepted in
    /// one provider turn.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless the limit is within
    /// `1..=64`.
    pub fn with_maximum_tool_calls(mut self, maximum_tool_calls: usize) -> Result<Self, AiError> {
        if !(1..=64).contains(&maximum_tool_calls) {
            return Err(AiError::InvalidConfiguration(
                "invalid provider-call tool limit".to_owned(),
            ));
        }
        self.maximum_tool_calls = maximum_tool_calls;
        Ok(self)
    }

    /// Maximum custom application-tool calls accepted in one provider turn.
    pub const fn maximum_tool_calls(self) -> usize {
        self.maximum_tool_calls
    }

    /// Sets the deployment maximum for provider-hosted tool calls in one turn.
    ///
    /// A request must carry an equal or narrower provider-enforced ceiling.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless the limit is within
    /// `1..=64`.
    pub fn with_maximum_builtin_tool_calls(
        mut self,
        maximum_builtin_tool_calls: usize,
    ) -> Result<Self, AiError> {
        if !(1..=64).contains(&maximum_builtin_tool_calls) {
            return Err(AiError::InvalidConfiguration(
                "invalid provider-call built-in tool limit".to_owned(),
            ));
        }
        self.maximum_builtin_tool_calls = maximum_builtin_tool_calls;
        Ok(self)
    }

    /// Deployment maximum for provider-hosted tool calls in one turn.
    pub const fn maximum_builtin_tool_calls(self) -> usize {
        self.maximum_builtin_tool_calls
    }
}

/// Deployment raw-byte and cardinality limits for provider attachment reopening.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiProviderAttachmentResolutionLimits {
    maximum_attachments: usize,
    maximum_attachment_bytes: u64,
    maximum_total_bytes: u64,
}

impl AiProviderAttachmentResolutionLimits {
    /// Creates validated resolver limits.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless attachment count is in
    /// `1..=32`, per-object bytes are in `1..=100 MiB`, total bytes are at
    /// least the object limit, and total bytes do not exceed 100 MiB.
    pub fn new(
        maximum_attachments: usize,
        maximum_attachment_bytes: u64,
        maximum_total_bytes: u64,
    ) -> Result<Self, AiError> {
        if !(1..=32).contains(&maximum_attachments)
            || !(1..=100 * 1024 * 1024).contains(&maximum_attachment_bytes)
            || maximum_total_bytes < maximum_attachment_bytes
            || maximum_total_bytes > 100 * 1024 * 1024
        {
            return Err(AiError::InvalidConfiguration(
                "invalid provider attachment resolution limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_attachments,
            maximum_attachment_bytes,
            maximum_total_bytes,
        })
    }

    /// Maximum exact attachment inputs in one turn.
    pub const fn maximum_attachments(self) -> usize {
        self.maximum_attachments
    }

    /// Maximum raw bytes reopened for one attachment.
    pub const fn maximum_attachment_bytes(self) -> u64 {
        self.maximum_attachment_bytes
    }

    /// Maximum combined raw bytes reopened for one turn.
    pub const fn maximum_total_bytes(self) -> u64 {
        self.maximum_total_bytes
    }
}

impl Default for AiProviderAttachmentResolutionLimits {
    fn default() -> Self {
        Self {
            maximum_attachments: 8,
            maximum_attachment_bytes: 25 * 1024 * 1024,
            maximum_total_bytes: 50 * 1024 * 1024,
        }
    }
}

/// Server-authored, exact plan for one provider call.
///
/// Custom application tools are accepted only by [`Self::new_with_tools`],
/// which verifies every definition against an exact registered descriptor and
/// an explicit fingerprint-bound policy set. This plan is still only one
/// provider turn; durable tool execution and continuation use the dedicated
/// fenced tool-call service and bounded loop guard.
#[derive(Clone, Debug)]
pub struct AiProviderCallPlan {
    provider_kind: ProviderKind,
    request: ModelRequest,
    budget: AiBudgetReservationRequest,
    transfers: Vec<AiEgressManifest>,
    correlation_id: String,
    tool_rule_bindings: Vec<AiPlanToolRuleBinding>,
}

#[derive(Clone, Debug)]
struct AiPlanToolRuleBinding {
    fingerprint: String,
    maturity: ToolMaturity,
    approval: AiApprovalRule,
}

impl AiProviderCallPlan {
    /// Creates and statically binds a provider call to its budget and egress
    /// manifests.
    ///
    /// Exactly one model-inference manifest is required. Attachment and
    /// provider-built-in capabilities require their own additional manifests.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidInput`] for a malformed request/correlation,
    /// custom tools, mismatched provider/model/session/run/scope, duplicate or
    /// absent model-inference manifest, duplicate manifest, or more than 288
    /// transfers. The larger bound accommodates an exact proof for each item
    /// in one bounded stateless tool-result replay.
    pub fn new(
        provider_kind: ProviderKind,
        request: ModelRequest,
        budget: AiBudgetReservationRequest,
        transfers: Vec<AiEgressManifest>,
        correlation_id: impl Into<String>,
    ) -> Result<Self, AiError> {
        Self::new_internal(
            provider_kind,
            request,
            budget,
            transfers,
            correlation_id.into(),
            false,
        )
    }

    fn new_internal(
        provider_kind: ProviderKind,
        request: ModelRequest,
        budget: AiBudgetReservationRequest,
        mut transfers: Vec<AiEgressManifest>,
        correlation_id: String,
        allow_bound_tools: bool,
    ) -> Result<Self, AiError> {
        request
            .validate()
            .map_err(|_| AiError::InvalidInput("invalid provider call plan".to_owned()))?;
        if (!allow_bound_tools && !request.tools.is_empty())
            || correlation_id.trim().is_empty()
            || correlation_id.len() > 512
            || transfers.is_empty()
            || transfers.len() > MAXIMUM_PROVIDER_TRANSFERS
            || budget.provider_kind != provider_kind
            || budget.model != request.model
        {
            return Err(AiError::InvalidInput(
                "invalid provider call plan".to_owned(),
            ));
        }
        let mut manifest_hashes = BTreeSet::new();
        if transfers.iter().any(|manifest| {
            manifest.provider_kind != provider_kind.as_str()
                || manifest.model != request.model
                || manifest.session_id != Some(budget.session_id)
                || manifest.run_id != Some(budget.run_id)
                || manifest.scope != budget.scope
                || !manifest_hashes.insert(manifest.stable_hash())
        }) || transfers
            .iter()
            .filter(|manifest| manifest.capability == AiEgressCapability::ModelInference)
            .count()
            != 1
        {
            return Err(AiError::InvalidInput(
                "provider plan proofs are not exactly bound".to_owned(),
            ));
        }
        transfers.sort_by_key(|manifest| {
            usize::from(manifest.capability != AiEgressCapability::ModelInference)
        });
        Ok(Self {
            provider_kind,
            request,
            budget,
            transfers,
            correlation_id,
            tool_rule_bindings: Vec::new(),
        })
    }

    /// Creates an initial provider call exposing only explicitly enabled,
    /// registered, read-only application queries.
    ///
    /// The supplied policy set is a current configuration snapshot, not
    /// execution authorization. Each model-requested call is reauthorized with
    /// a fresh principal and then executed through the ordinary GraphQL
    /// resolver path.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidInput`] for ordinary plan-binding failures,
    /// pre-populated continuation/tool-result input, and
    /// [`AiError::Forbidden`] unless every tool definition exactly matches an
    /// enabled read-only descriptor with no per-call approval requirement.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_tools(
        provider_kind: ProviderKind,
        request: ModelRequest,
        budget: AiBudgetReservationRequest,
        transfers: Vec<AiEgressManifest>,
        correlation_id: impl Into<String>,
        catalog: &crate::AiToolCatalog,
        policy: &AiToolPolicySet,
    ) -> Result<Self, AiError> {
        if request.tools.is_empty()
            || request.continuation.is_some()
            || request
                .input
                .iter()
                .any(|block| matches!(block, crate::ModelInputBlock::ToolResult { .. }))
        {
            return Err(AiError::InvalidInput(
                "initial provider tool plan is invalid".to_owned(),
            ));
        }
        Self::new_with_bound_tools(
            provider_kind,
            request,
            budget,
            transfers,
            correlation_id,
            catalog,
            policy,
        )
    }

    /// Creates an initial provider call that may expose explicitly enabled
    /// read-only queries and supervised one-shot application mutations.
    ///
    /// This constructor only controls model-visible discovery. Every mutation
    /// still requires a server-generated canonical preview, exact durable
    /// approval, fresh policy comparison, and ordinary resolver authorization
    /// before execution.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidInput`] for ordinary plan-binding failures or
    /// pre-populated continuation/tool-result input, and
    /// [`AiError::Forbidden`] unless every definition exactly matches an
    /// enabled safe read or supervised one-shot mutation descriptor.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_supervised_tools(
        provider_kind: ProviderKind,
        request: ModelRequest,
        budget: AiBudgetReservationRequest,
        transfers: Vec<AiEgressManifest>,
        correlation_id: impl Into<String>,
        catalog: &crate::AiToolCatalog,
        policy: &AiToolPolicySet,
    ) -> Result<Self, AiError> {
        if request.tools.is_empty()
            || request.continuation.is_some()
            || request
                .input
                .iter()
                .any(|block| matches!(block, crate::ModelInputBlock::ToolResult { .. }))
        {
            return Err(AiError::InvalidInput(
                "initial supervised provider tool plan is invalid".to_owned(),
            ));
        }
        Self::new_with_bound_supervised_tools(
            provider_kind,
            request,
            budget,
            transfers,
            correlation_id,
            catalog,
            policy,
        )
    }

    fn new_with_bound_tools(
        provider_kind: ProviderKind,
        request: ModelRequest,
        budget: AiBudgetReservationRequest,
        transfers: Vec<AiEgressManifest>,
        correlation_id: impl Into<String>,
        catalog: &crate::AiToolCatalog,
        policy: &AiToolPolicySet,
    ) -> Result<Self, AiError> {
        if request.tools.is_empty() {
            return Err(AiError::InvalidInput(
                "provider tool plan has no tools".to_owned(),
            ));
        }
        for definition in &request.tools {
            catalog.validate_read_only_model_definition(definition, policy)?;
        }
        let bindings = request
            .tools
            .iter()
            .map(|definition| AiPlanToolRuleBinding {
                fingerprint: definition.fingerprint.clone(),
                maturity: ToolMaturity::ReadOnly,
                approval: AiApprovalRule::None,
            })
            .collect();
        let mut plan = Self::new_internal(
            provider_kind,
            request,
            budget,
            transfers,
            correlation_id.into(),
            true,
        )?;
        plan.tool_rule_bindings = bindings;
        Ok(plan)
    }

    fn new_with_bound_supervised_tools(
        provider_kind: ProviderKind,
        request: ModelRequest,
        budget: AiBudgetReservationRequest,
        transfers: Vec<AiEgressManifest>,
        correlation_id: impl Into<String>,
        catalog: &crate::AiToolCatalog,
        policy: &AiToolPolicySet,
    ) -> Result<Self, AiError> {
        if request.tools.is_empty() {
            return Err(AiError::InvalidInput(
                "supervised provider tool plan has no tools".to_owned(),
            ));
        }
        let mut bindings = Vec::with_capacity(request.tools.len());
        for definition in &request.tools {
            catalog.validate_supervised_model_definition(definition, policy)?;
            let descriptor = catalog
                .descriptor(&crate::AiToolId::parse(definition.tool_id.clone())?)
                .ok_or(AiError::Forbidden)?;
            bindings.push(AiPlanToolRuleBinding {
                fingerprint: descriptor.fingerprint.clone(),
                maturity: descriptor.maturity,
                approval: descriptor.approval,
            });
        }
        let mut plan = Self::new_internal(
            provider_kind,
            request,
            budget,
            transfers,
            correlation_id.into(),
            true,
        )?;
        plan.tool_rule_bindings = bindings;
        Ok(plan)
    }

    /// Creates a subsequent read-only tool turn from one exact bounded-loop
    /// continuation.
    ///
    /// The continuation installs either the previous response ID or exact
    /// bounded stateless history, plus matched tool-result blocks and their
    /// immutable exact egress manifests as one unit. The caller supplies a
    /// fresh model-inference manifest and budget request; every historical and
    /// current result transfer is freshly reauthorized and audited by the
    /// executor.
    ///
    /// # Errors
    ///
    /// Returns an error for a non-empty request input/continuation, ordinary
    /// plan-binding failures, too many transfers, or a stale/non-enabled tool
    /// definition.
    #[allow(clippy::too_many_arguments)]
    pub fn new_continuation_with_tools(
        provider_kind: ProviderKind,
        mut request: ModelRequest,
        budget: AiBudgetReservationRequest,
        mut transfers: Vec<AiEgressManifest>,
        correlation_id: impl Into<String>,
        continuation: crate::AiAgentContinuation,
        catalog: &crate::AiToolCatalog,
        policy: &AiToolPolicySet,
    ) -> Result<Self, AiError> {
        let tool_transfers = continuation.apply_with_transfers(&mut request)?;
        transfers.extend(tool_transfers);
        Self::new_with_bound_tools(
            provider_kind,
            request,
            budget,
            transfers,
            correlation_id,
            catalog,
            policy,
        )
    }

    /// Creates a continuation turn retaining the exact supervised tool
    /// exposure policy while installing one bounded, egress-authorized prior
    /// result set.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed continuation bindings or any stale,
    /// disabled, non-read-only/non-supervised, or otherwise unsafe tool
    /// definition.
    #[allow(clippy::too_many_arguments)]
    pub fn new_supervised_continuation_with_tools(
        provider_kind: ProviderKind,
        mut request: ModelRequest,
        budget: AiBudgetReservationRequest,
        mut transfers: Vec<AiEgressManifest>,
        correlation_id: impl Into<String>,
        continuation: crate::AiAgentContinuation,
        catalog: &crate::AiToolCatalog,
        policy: &AiToolPolicySet,
    ) -> Result<Self, AiError> {
        let tool_transfers = continuation.apply_with_transfers(&mut request)?;
        transfers.extend(tool_transfers);
        Self::new_with_bound_supervised_tools(
            provider_kind,
            request,
            budget,
            transfers,
            correlation_id,
            catalog,
            policy,
        )
    }

    /// Exact application scope bound into the atomic budget request and every
    /// egress manifest for this turn.
    pub fn scope(&self) -> &crate::AiScope {
        &self.budget.scope
    }

    /// Server-authored audit correlation reference for this turn.
    pub fn correlation_id(&self) -> &str {
        &self.correlation_id
    }

    #[cfg(all(
        any(feature = "sqlite", feature = "postgres"),
        feature = "provider-openai"
    ))]
    pub(crate) fn provider_kind_ref(&self) -> &ProviderKind {
        &self.provider_kind
    }

    #[cfg(all(
        any(feature = "sqlite", feature = "postgres"),
        feature = "provider-openai"
    ))]
    pub(crate) fn request_ref(&self) -> &ModelRequest {
        &self.request
    }

    #[cfg(all(
        any(feature = "sqlite", feature = "postgres"),
        feature = "provider-openai"
    ))]
    pub(crate) fn budget_request(&self) -> &AiBudgetReservationRequest {
        &self.budget
    }

    #[cfg(all(
        any(feature = "sqlite", feature = "postgres"),
        feature = "provider-openai"
    ))]
    pub(crate) fn transfers(&self) -> &[AiEgressManifest] {
        &self.transfers
    }

    /// Returns whether this turn exposes at least one validated application
    /// tool definition.
    pub fn has_application_tools(&self) -> bool {
        !self.request.tools.is_empty()
    }

    pub(crate) fn has_only_supervised_tools(&self) -> bool {
        !self.tool_rule_bindings.is_empty()
            && self.tool_rule_bindings.len() == self.request.tools.len()
            && self.tool_rule_bindings.iter().all(|binding| {
                binding.maturity == ToolMaturity::SupervisedWrite
                    && binding.approval == AiApprovalRule::OneShot
            })
    }

    pub(crate) fn uses_provider_retained_continuation(&self) -> bool {
        self.request.continuation_mode == ModelContinuationMode::ProviderRetained
    }

    pub(crate) fn project_rule_usage(
        &self,
        resolution: &AiAgentRuleResolution,
        usage: AiRuleRunUsage,
        uses_byok: bool,
    ) -> Result<AiRuleRunUsage, AiError> {
        self.project_bound_rule_usage(resolution, usage, uses_byok, false)
    }

    /// Projects hierarchical rule usage for one supervised-compatible tool
    /// plan.
    ///
    /// This verifies every immutable plan-time tool binding against the exact
    /// current resolved rule set: safe reads must remain approval-free and
    /// supervised mutations must retain one-shot approval. It also checks the
    /// provider family/capabilities, disclosure ceiling, retention, BYOK, and
    /// estimated provider budget. The returned usage is planning evidence
    /// only and grants no provider, egress, budget, approval, or resolver
    /// authority.
    ///
    /// # Errors
    ///
    /// Returns a safe denial when the plan has no bound application tools, any
    /// tool/rule binding changed, provider constraints reject the request, or
    /// cumulative estimated usage exceeds current rules.
    pub fn project_supervised_rule_usage(
        &self,
        resolution: &AiAgentRuleResolution,
        usage: AiRuleRunUsage,
        uses_byok: bool,
    ) -> Result<AiRuleRunUsage, AiError> {
        if self.request.tools.is_empty() {
            return Err(AiError::Forbidden);
        }
        self.project_bound_rule_usage(resolution, usage, uses_byok, true)
    }

    fn project_bound_rule_usage(
        &self,
        resolution: &AiAgentRuleResolution,
        usage: AiRuleRunUsage,
        uses_byok: bool,
        allow_supervised: bool,
    ) -> Result<AiRuleRunUsage, AiError> {
        let rules = resolution.rules();
        if rules.target_scope() != self.scope()
            || self.request.maximum_output_tokens.unwrap_or(0) > self.budget.estimate.output_tokens
            || self.request.tools.len() != self.tool_rule_bindings.len()
            || self
                .request
                .tools
                .iter()
                .zip(&self.tool_rule_bindings)
                .any(|(tool, binding)| {
                    tool.fingerprint != binding.fingerprint
                        || match (binding.maturity, binding.approval) {
                            (ToolMaturity::ReadOnly, AiApprovalRule::None) => {
                                rules.constrain_tool(
                                    &binding.fingerprint,
                                    binding.maturity,
                                    binding.approval,
                                ) != Some(AiApprovalRule::None)
                            }
                            (ToolMaturity::SupervisedWrite, AiApprovalRule::OneShot)
                                if allow_supervised =>
                            {
                                rules.constrain_tool(
                                    &binding.fingerprint,
                                    binding.maturity,
                                    binding.approval,
                                ) != Some(AiApprovalRule::OneShot)
                            }
                            _ => true,
                        }
                })
        {
            return Err(AiError::Forbidden);
        }
        let mut capabilities = BTreeSet::from([AiRuleProviderCapability::Streaming]);
        for block in &self.request.input {
            if let ModelInputBlock::Attachment { mime, .. } = block {
                capabilities.insert(if mime.starts_with("image/") {
                    AiRuleProviderCapability::ImageInput
                } else {
                    AiRuleProviderCapability::FileInput
                });
            }
        }
        if !self.request.tools.is_empty() {
            capabilities.insert(AiRuleProviderCapability::CustomTools);
            // One advertised definition can still be selected more than once
            // in one provider turn. Requiring the parallel-call capability
            // here keeps the rule proof conservative without trusting the
            // provider to infer a single-call ceiling from definition count.
            capabilities.insert(AiRuleProviderCapability::ParallelToolCalls);
        }
        if self.request.output_schema.is_some() {
            capabilities.insert(AiRuleProviderCapability::StructuredOutput);
        }
        for builtin in &self.request.builtin_tools {
            capabilities.insert(match builtin {
                ModelBuiltinTool::WebSearch { .. } => AiRuleProviderCapability::WebSearch,
                ModelBuiltinTool::FileSearch { .. } => AiRuleProviderCapability::FileSearch,
                ModelBuiltinTool::CodeInterpreter => AiRuleProviderCapability::CodeExecution,
                ModelBuiltinTool::ImageGeneration => AiRuleProviderCapability::ImageGeneration,
            });
        }
        if self.request.continuation_mode == ModelContinuationMode::StatelessReplay {
            capabilities.insert(AiRuleProviderCapability::StatelessContinuation);
        }
        let uses_provider_retention = self
            .transfers
            .iter()
            .any(|manifest| manifest.retention != "none")
            || matches!(
                self.request.continuation.as_ref(),
                Some(ModelContinuation::ProviderResponse { .. })
            );
        if uses_provider_retention {
            capabilities.insert(AiRuleProviderCapability::ProviderRetainedContinuation);
        }
        let classification = self
            .transfers
            .iter()
            .map(AiEgressManifest::maximum_classification)
            .max()
            .unwrap_or(crate::DataClassification::Public);
        if !rules.permits_provider_request(
            &self.provider_kind,
            &capabilities,
            classification,
            uses_provider_retention,
            uses_byok,
        ) {
            return Err(AiError::EgressDenied);
        }
        usage.projected_provider(self.budget.estimate, resolution)
    }

    pub(crate) fn is_continuation(&self) -> bool {
        self.request.continuation.is_some()
    }

    #[cfg(test)]
    pub(crate) fn test_plan(lease: &AiRunLease, scope: crate::AiScope, continuation: bool) -> Self {
        let model = "coordinator-test-model".to_owned();
        Self {
            provider_kind: ProviderKind::OpenAi,
            request: ModelRequest {
                model: model.clone(),
                instructions: Vec::new(),
                input: continuation
                    .then(|| crate::ModelInputBlock::ToolResult {
                        call_id: "test-call".to_owned(),
                        tool_id: "test.read".to_owned(),
                        output: serde_json::json!({"test": true}),
                    })
                    .into_iter()
                    .collect(),
                continuation: continuation.then(|| ModelContinuation::ProviderResponse {
                    response_id: "test-previous-response".to_owned(),
                }),
                continuation_mode: crate::ModelContinuationMode::ProviderRetained,
                tools: vec![crate::ModelToolDefinition {
                    tool_id: "test.read".to_owned(),
                    provider_name: "test_read".to_owned(),
                    fingerprint: "test-fingerprint".to_owned(),
                    description: "Test read".to_owned(),
                    parameters: serde_json::json!({
                        "type": "object",
                        "additionalProperties": false
                    }),
                    strict: true,
                }],
                builtin_tools: Vec::new(),
                maximum_builtin_tool_calls: None,
                output_schema: None,
                maximum_output_tokens: Some(64),
            },
            budget: AiBudgetReservationRequest {
                scope,
                session_id: lease.session_id(),
                run_id: lease.run_id(),
                attempt_id: lease.attempt_id(),
                lease_generation: lease.lease_generation(),
                provider_kind: ProviderKind::OpenAi,
                model,
                pricing_policy_version: "test-pricing-v1".to_owned(),
                estimate: AiBudgetAmounts {
                    output_tokens: 64,
                    runs: 1,
                    ..AiBudgetAmounts::default()
                },
                idempotency_key: uuid::Uuid::new_v4().to_string(),
                expires_at: time::OffsetDateTime::now_utc() + time::Duration::minutes(5),
            },
            transfers: Vec::new(),
            correlation_id: uuid::Uuid::new_v4().to_string(),
            tool_rule_bindings: vec![AiPlanToolRuleBinding {
                fingerprint: "test-fingerprint".to_owned(),
                maturity: ToolMaturity::ReadOnly,
                approval: AiApprovalRule::None,
            }],
        }
    }

    #[cfg(test)]
    pub(crate) fn test_supervised_plan(
        lease: &AiRunLease,
        scope: crate::AiScope,
        continuation: bool,
    ) -> Self {
        let mut plan = Self::test_plan(lease, scope, continuation);
        let definition = plan
            .request
            .tools
            .first_mut()
            .expect("coordinator test plan should contain one tool");
        definition.tool_id = "test.write".to_owned();
        definition.provider_name = "test_write".to_owned();
        definition.description = "Test supervised write".to_owned();
        if let Some(crate::ModelInputBlock::ToolResult { tool_id, .. }) =
            plan.request.input.first_mut()
        {
            *tool_id = "test.write".to_owned();
        }
        plan.tool_rule_bindings = vec![AiPlanToolRuleBinding {
            fingerprint: "test-fingerprint".to_owned(),
            maturity: ToolMaturity::SupervisedWrite,
            approval: AiApprovalRule::OneShot,
        }];
        plan
    }
}

/// Exact custom application-tool request normalized from one provider turn.
///
/// Fields are private so callers cannot manufacture a provider-originated
/// request or swap its descriptor fingerprint before durable execution.
#[derive(Clone, Debug)]
pub struct AiProviderToolCall {
    call_id: String,
    tool_id: crate::AiToolId,
    provider_name: String,
    tool_fingerprint: String,
    arguments: serde_json::Value,
}

impl AiProviderToolCall {
    /// Opaque provider call identifier.
    pub fn call_id(&self) -> &str {
        &self.call_id
    }

    /// Stable local registered tool identifier.
    pub fn tool_id(&self) -> &crate::AiToolId {
        &self.tool_id
    }

    /// Exact provider-facing name from the reviewed model definition.
    pub fn provider_name(&self) -> &str {
        &self.provider_name
    }

    /// Exact descriptor fingerprint exposed in the provider request.
    pub fn tool_fingerprint(&self) -> &str {
        &self.tool_fingerprint
    }

    /// Complete provider arguments. The durable tool service validates these
    /// again against the registered JSON Schema before resolver execution.
    pub fn arguments(&self) -> &serde_json::Value {
        &self.arguments
    }
}

/// Bounded normalized result of one successful provider turn.
///
/// The result can contain protected user/model content and is intended for the
/// trusted backend only. The caller must persist it through a fenced,
/// content-protected message/event writer before terminally completing the
/// run.
#[derive(Clone, Debug)]
pub struct AiProviderCallResult {
    session_id: crate::AiSessionId,
    run_id: crate::AiRunId,
    attempt_id: uuid::Uuid,
    lease_generation: i64,
    provider_kind: ProviderKind,
    provider_model: String,
    events: Vec<ProviderEvent>,
    usage: AiBudgetAmounts,
    cached_input_tokens: u64,
    provider_response_id: Option<String>,
    budget_reservation_id: AiBudgetReservationId,
    previous_response_id: Option<String>,
    previous_continuation_reference: Option<String>,
    tool_calls: Vec<AiProviderToolCall>,
    request_snapshot: ModelRequest,
    model_inference_manifest: AiEgressManifest,
    replay_tool_transfers: Vec<AiEgressManifest>,
}

/// Authoritative completed provider-hosted tool counts for one turn.
///
/// Counts come only from exact normalized start/completion pairs. They do not
/// count advertised, requested, started-only, or model-proposed tools.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AiProviderBuiltinUsage {
    web_search_calls: u64,
    file_search_calls: u64,
    code_interpreter_calls: u64,
    image_generation_calls: u64,
}

impl AiProviderBuiltinUsage {
    #[cfg(test)]
    pub(crate) const fn test_usage(
        web_search_calls: u64,
        file_search_calls: u64,
        code_interpreter_calls: u64,
        image_generation_calls: u64,
    ) -> Self {
        Self {
            web_search_calls,
            file_search_calls,
            code_interpreter_calls,
            image_generation_calls,
        }
    }

    /// Completed provider-hosted web-search calls.
    pub const fn web_search_calls(self) -> u64 {
        self.web_search_calls
    }

    /// Completed provider-hosted file-search calls.
    pub const fn file_search_calls(self) -> u64 {
        self.file_search_calls
    }

    /// Completed provider-hosted code-interpreter calls.
    pub const fn code_interpreter_calls(self) -> u64 {
        self.code_interpreter_calls
    }

    /// Completed provider-hosted image-generation calls.
    pub const fn image_generation_calls(self) -> u64 {
        self.image_generation_calls
    }

    fn record(&mut self, kind: &str) -> Result<(), AiError> {
        let counter = match kind {
            "web_search" => &mut self.web_search_calls,
            "file_search" => &mut self.file_search_calls,
            "code_interpreter" => &mut self.code_interpreter_calls,
            "image_generation" => &mut self.image_generation_calls,
            _ => return Err(AiError::ProviderFailed),
        };
        *counter = counter.checked_add(1).ok_or(AiError::ProviderFailed)?;
        Ok(())
    }
}

/// Authoritative provider usage observation requiring deployment pricing/unit
/// settlement before budget commit.
#[derive(Clone, Debug)]
pub struct AiProviderUsageObservation {
    scope: AiScope,
    provider_kind: ProviderKind,
    model: String,
    pricing_policy_version: String,
    input_tokens: u64,
    output_tokens: u64,
    cached_input_tokens: u64,
    builtin_usage: AiProviderBuiltinUsage,
}

impl AiProviderUsageObservation {
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn test_observation(
        scope: AiScope,
        provider_kind: ProviderKind,
        model: impl Into<String>,
        pricing_policy_version: impl Into<String>,
        input_tokens: u64,
        output_tokens: u64,
        cached_input_tokens: u64,
        builtin_usage: AiProviderBuiltinUsage,
    ) -> Self {
        Self {
            scope,
            provider_kind,
            model: model.into(),
            pricing_policy_version: pricing_policy_version.into(),
            input_tokens,
            output_tokens,
            cached_input_tokens,
            builtin_usage,
        }
    }

    /// Exact application scope bound to the provider budget reservation.
    pub fn scope(&self) -> &AiScope {
        &self.scope
    }

    /// Provider family.
    pub fn provider_kind(&self) -> &ProviderKind {
        &self.provider_kind
    }

    /// Exact provider model.
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Immutable pricing catalog/policy version selected by the plan.
    pub fn pricing_policy_version(&self) -> &str {
        &self.pricing_policy_version
    }

    /// Provider-reported total input tokens.
    pub const fn input_tokens(&self) -> u64 {
        self.input_tokens
    }

    /// Provider-reported output tokens.
    pub const fn output_tokens(&self) -> u64 {
        self.output_tokens
    }

    /// Provider-reported cached input tokens.
    pub const fn cached_input_tokens(&self) -> u64 {
        self.cached_input_tokens
    }

    /// Authoritative completed provider-hosted tool counts.
    pub const fn builtin_usage(&self) -> AiProviderBuiltinUsage {
        self.builtin_usage
    }
}

/// Deployment-owned immutable pricing and billable-unit settlement.
///
/// Implementations normally resolve the exact reviewed pricing version from a
/// local catalog. They must never fetch model-selected pricing or silently use
/// a newer rate. Errors occur after the transport boundary, so the reservation
/// remains uncertain until authoritative settlement succeeds.
#[async_trait]
pub trait AiProviderUsageAccounting: Send + Sync {
    /// Computes complete actual budget dimensions for an authoritative
    /// provider usage observation.
    ///
    /// Returned input/output tokens must exactly match the observation and
    /// `runs` must equal one. Cost, image, and tool units may exceed the
    /// estimate and are committed truthfully.
    ///
    /// # Errors
    ///
    /// Returns an error when the pricing version/model is unknown, units or
    /// cost cannot be determined authoritatively, or arithmetic overflows.
    async fn settle(
        &self,
        observation: &AiProviderUsageObservation,
    ) -> Result<AiBudgetAmounts, AiError>;
}

impl AiProviderCallResult {
    pub(crate) fn checkpoint_value(&self) -> serde_json::Value {
        serde_json::json!({
            "formatVersion": 1,
            "sessionId": self.session_id.0,
            "runId": self.run_id.0,
            "attemptId": self.attempt_id,
            "leaseGeneration": self.lease_generation,
            "providerKind": self.provider_kind,
            "providerModel": self.provider_model,
            "events": self.events,
            "usage": self.usage,
            "cachedInputTokens": self.cached_input_tokens,
            "providerResponseId": self.provider_response_id,
            "budgetReservationId": self.budget_reservation_id.0,
            "previousResponseId": self.previous_response_id,
            "toolCalls": self.tool_calls.iter().map(|call| serde_json::json!({
                "callId": call.call_id,
                "toolId": call.tool_id.as_str(),
                "providerName": call.provider_name,
                "toolFingerprint": call.tool_fingerprint,
                "arguments": call.arguments,
            })).collect::<Vec<_>>(),
        })
    }

    /// Session bound to the provider turn.
    pub const fn session_id(&self) -> crate::AiSessionId {
        self.session_id
    }

    /// Run bound to the provider turn.
    pub const fn run_id(&self) -> crate::AiRunId {
        self.run_id
    }

    /// Attempt bound to the provider turn.
    pub const fn attempt_id(&self) -> uuid::Uuid {
        self.attempt_id
    }

    /// Fencing generation bound to the provider turn.
    pub const fn lease_generation(&self) -> i64 {
        self.lease_generation
    }

    /// Provider family used by the turn.
    pub fn provider_kind(&self) -> &ProviderKind {
        &self.provider_kind
    }

    /// Exact provider model used by the turn.
    pub fn provider_model(&self) -> &str {
        &self.provider_model
    }

    /// Normalized provider events in arrival order.
    pub fn events(&self) -> &[ProviderEvent] {
        &self.events
    }

    /// Authoritative budget usage committed for the provider turn.
    pub const fn usage(&self) -> AiBudgetAmounts {
        self.usage
    }

    /// Provider-reported cached input tokens retained for usage accounting.
    pub const fn cached_input_tokens(&self) -> u64 {
        self.cached_input_tokens
    }

    /// Safe provider response reference, when emitted.
    pub fn provider_response_id(&self) -> Option<&str> {
        self.provider_response_id.as_deref()
    }

    /// Durable budget reservation/usage correlation identifier.
    pub const fn budget_reservation_id(&self) -> AiBudgetReservationId {
        self.budget_reservation_id
    }

    /// Prior provider response continued by this request, if any.
    pub fn previous_response_id(&self) -> Option<&str> {
        self.previous_response_id.as_deref()
    }

    pub(crate) fn previous_continuation_reference(&self) -> Option<&str> {
        self.previous_continuation_reference.as_deref()
    }

    pub(crate) fn replay_tool_transfers(&self) -> &[AiEgressManifest] {
        &self.replay_tool_transfers
    }

    pub(crate) fn request_snapshot(&self) -> &ModelRequest {
        &self.request_snapshot
    }

    pub(crate) fn model_inference_manifest(&self) -> &AiEgressManifest {
        &self.model_inference_manifest
    }

    pub(crate) fn uses_stateless_continuation(&self) -> bool {
        self.request_snapshot.continuation_mode == ModelContinuationMode::StatelessReplay
    }

    /// Exact normalized custom application-tool requests in arrival order.
    pub fn tool_calls(&self) -> &[AiProviderToolCall] {
        &self.tool_calls
    }

    /// Returns an explicit stateful continuation for a tool-requesting turn.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::ProviderFailed`] unless the turn requested at least
    /// one tool and emitted a bounded provider response identifier.
    pub fn continuation(&self) -> Result<ModelContinuation, AiError> {
        if self.tool_calls.is_empty() {
            return Err(AiError::ProviderFailed);
        }
        let response_id = self
            .provider_response_id
            .clone()
            .ok_or(AiError::ProviderFailed)?;
        Ok(ModelContinuation::ProviderResponse { response_id })
    }

    pub(crate) fn next_continuation(&self) -> Result<ModelContinuation, AiError> {
        if self.tool_calls.is_empty() {
            return Err(AiError::ProviderFailed);
        }
        match self.request_snapshot.continuation_mode {
            ModelContinuationMode::ProviderRetained => self.continuation(),
            ModelContinuationMode::StatelessReplay => {
                let (instructions, mut messages) = match &self.request_snapshot.continuation {
                    None => {
                        if self.request_snapshot.input.is_empty()
                            || self.request_snapshot.input.iter().any(|block| {
                                !matches!(
                                    block,
                                    ModelInputBlock::Text { .. } | ModelInputBlock::Json { .. }
                                )
                            })
                        {
                            return Err(AiError::Conflict);
                        }
                        (
                            self.request_snapshot.instructions.clone(),
                            vec![ModelConversationMessage::User {
                                content: self.request_snapshot.input.clone(),
                            }],
                        )
                    }
                    Some(ModelContinuation::StatelessConversation {
                        instructions,
                        messages,
                    }) => {
                        let mut messages = messages.clone();
                        let calls = match messages.last() {
                            Some(ModelConversationMessage::Assistant { tool_calls, .. }) => {
                                tool_calls.clone()
                            }
                            _ => return Err(AiError::Conflict),
                        };
                        if calls.len() != self.request_snapshot.input.len() {
                            return Err(AiError::Conflict);
                        }
                        for (call, input) in calls.iter().zip(&self.request_snapshot.input) {
                            let ModelInputBlock::ToolResult {
                                call_id,
                                tool_id,
                                output,
                            } = input
                            else {
                                return Err(AiError::Conflict);
                            };
                            if call_id != &call.call_id || tool_id != &call.tool_id {
                                return Err(AiError::Conflict);
                            }
                            messages.push(ModelConversationMessage::Tool {
                                call_id: call_id.clone(),
                                tool_id: tool_id.clone(),
                                provider_name: call.provider_name.clone(),
                                output: output.clone(),
                            });
                        }
                        (instructions.clone(), messages)
                    }
                    Some(ModelContinuation::ProviderResponse { .. }) => {
                        return Err(AiError::Conflict);
                    }
                };
                let mut content = String::new();
                for event in &self.events {
                    if let ProviderEvent::TextDelta { text } = event {
                        content
                            .try_reserve(text.len())
                            .map_err(|_| AiError::PersistenceFailed)?;
                        content.push_str(text);
                    }
                }
                let tool_calls = self
                    .tool_calls
                    .iter()
                    .map(|call| ModelConversationToolCall {
                        call_id: call.call_id.clone(),
                        tool_id: call.tool_id.as_str().to_owned(),
                        provider_name: call.provider_name.clone(),
                        tool_fingerprint: call.tool_fingerprint.clone(),
                        arguments: call.arguments.clone(),
                    })
                    .collect();
                messages.push(ModelConversationMessage::Assistant {
                    content,
                    tool_calls,
                });
                Ok(ModelContinuation::StatelessConversation {
                    instructions,
                    messages,
                })
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn test_result(
        lease: &AiRunLease,
        previous_response_id: Option<String>,
        provider_response_id: &str,
        tool_calls: Vec<(&str, &str, serde_json::Value)>,
    ) -> Self {
        let tool_calls = tool_calls
            .into_iter()
            .map(|(call_id, tool_id, arguments)| AiProviderToolCall {
                call_id: call_id.to_owned(),
                tool_id: crate::AiToolId::parse(tool_id)
                    .expect("coordinator test tool ID should be valid"),
                provider_name: tool_id.replace('.', "_"),
                tool_fingerprint: "test-fingerprint".to_owned(),
                arguments,
            })
            .collect();
        Self {
            session_id: lease.session_id(),
            run_id: lease.run_id(),
            attempt_id: lease.attempt_id(),
            lease_generation: lease.lease_generation(),
            provider_kind: ProviderKind::OpenAi,
            provider_model: "coordinator-test-model".to_owned(),
            events: vec![
                ProviderEvent::TextDelta {
                    text: "test response".to_owned(),
                },
                ProviderEvent::Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                    cached_input_tokens: 0,
                },
                ProviderEvent::ResponseCompleted {
                    response_id: Some(provider_response_id.to_owned()),
                },
            ],
            usage: AiBudgetAmounts {
                input_tokens: 1,
                output_tokens: 1,
                runs: 1,
                ..AiBudgetAmounts::default()
            },
            cached_input_tokens: 0,
            provider_response_id: Some(provider_response_id.to_owned()),
            budget_reservation_id: AiBudgetReservationId::new(),
            previous_response_id: previous_response_id.clone(),
            previous_continuation_reference: previous_response_id.clone(),
            tool_calls,
            request_snapshot: ModelRequest {
                model: "coordinator-test-model".to_owned(),
                instructions: Vec::new(),
                input: previous_response_id
                    .as_ref()
                    .map(|_| ModelInputBlock::ToolResult {
                        call_id: "previous-test-call".to_owned(),
                        tool_id: "test.read".to_owned(),
                        output: serde_json::json!({"test": true}),
                    })
                    .into_iter()
                    .collect(),
                continuation: previous_response_id
                    .clone()
                    .map(|response_id| ModelContinuation::ProviderResponse { response_id }),
                continuation_mode: ModelContinuationMode::ProviderRetained,
                tools: Vec::new(),
                builtin_tools: Vec::new(),
                maximum_builtin_tool_calls: None,
                output_schema: None,
                maximum_output_tokens: Some(64),
            },
            model_inference_manifest: test_model_inference_manifest(
                lease,
                "coordinator-test-model",
            ),
            replay_tool_transfers: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_ui_intent_result(
        lease: &AiRunLease,
        budget_reservation_id: AiBudgetReservationId,
        envelope: serde_json::Value,
    ) -> Self {
        let model = "ui-intent-test-model".to_owned();
        Self {
            session_id: lease.session_id(),
            run_id: lease.run_id(),
            attempt_id: lease.attempt_id(),
            lease_generation: lease.lease_generation(),
            provider_kind: ProviderKind::OpenAi,
            provider_model: model.clone(),
            events: vec![
                ProviderEvent::ResponseStarted {
                    response_id: Some("ui-intent-response".to_owned()),
                },
                ProviderEvent::TextDelta {
                    text: envelope.to_string(),
                },
                ProviderEvent::Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                    cached_input_tokens: 0,
                },
                ProviderEvent::ResponseCompleted {
                    response_id: Some("ui-intent-response".to_owned()),
                },
            ],
            usage: AiBudgetAmounts {
                input_tokens: 1,
                output_tokens: 1,
                runs: 1,
                ..AiBudgetAmounts::default()
            },
            cached_input_tokens: 0,
            provider_response_id: Some("ui-intent-response".to_owned()),
            budget_reservation_id,
            previous_response_id: None,
            previous_continuation_reference: None,
            tool_calls: Vec::new(),
            request_snapshot: ModelRequest {
                model,
                instructions: Vec::new(),
                input: Vec::new(),
                continuation: None,
                continuation_mode: ModelContinuationMode::StatelessReplay,
                tools: Vec::new(),
                builtin_tools: Vec::new(),
                maximum_builtin_tool_calls: None,
                output_schema: None,
                maximum_output_tokens: Some(256),
            },
            model_inference_manifest: test_model_inference_manifest(lease, "ui-intent-test-model"),
            replay_tool_transfers: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_context_compaction_result(
        lease: &AiRunLease,
        provider_kind: ProviderKind,
        request: ModelRequest,
        model_inference_manifest: AiEgressManifest,
        summary: impl Into<String>,
    ) -> Self {
        let provider_model = request.model.clone();
        Self {
            session_id: lease.session_id(),
            run_id: lease.run_id(),
            attempt_id: lease.attempt_id(),
            lease_generation: lease.lease_generation(),
            provider_kind,
            provider_model,
            events: vec![
                ProviderEvent::ResponseStarted {
                    response_id: Some("context-compaction-test-response".to_owned()),
                },
                ProviderEvent::TextDelta {
                    text: summary.into(),
                },
                ProviderEvent::Usage {
                    input_tokens: 100,
                    output_tokens: 20,
                    cached_input_tokens: 0,
                },
                ProviderEvent::ResponseCompleted {
                    response_id: Some("context-compaction-test-response".to_owned()),
                },
            ],
            usage: AiBudgetAmounts {
                input_tokens: 100,
                output_tokens: 20,
                runs: 1,
                ..AiBudgetAmounts::default()
            },
            cached_input_tokens: 0,
            provider_response_id: Some("context-compaction-test-response".to_owned()),
            budget_reservation_id: AiBudgetReservationId::new(),
            previous_response_id: None,
            previous_continuation_reference: None,
            tool_calls: Vec::new(),
            request_snapshot: request,
            model_inference_manifest,
            replay_tool_transfers: Vec::new(),
        }
    }
}

#[cfg(test)]
fn test_model_inference_manifest(lease: &AiRunLease, model: &str) -> AiEgressManifest {
    AiEgressManifest {
        provider_profile_id: "test-profile".to_owned(),
        provider_kind: ProviderKind::OpenAi.as_str().to_owned(),
        model: model.to_owned(),
        destination: "test-destination".to_owned(),
        destination_trust: crate::AiDestinationTrust::ManagedProvider,
        capability: AiEgressCapability::ModelInference,
        scope: AiScope {
            kind: "test".to_owned(),
            id: "test".to_owned(),
            tenant_id: None,
        },
        session_id: Some(lease.session_id()),
        run_id: Some(lease.run_id()),
        sources: Vec::new(),
        estimated_bytes: 0,
        estimated_tokens: 0,
        attachment_count: 0,
        purpose: "test".to_owned(),
        retention: "test".to_owned(),
        residency: None,
        policy_version: "test".to_owned(),
        consent_reference: None,
    }
}

/// Security-ordered executor for one provider turn.
///
/// The executor requires a running fenced lease, freshly reauthorizes session
/// and scope access, reserves budget atomically, durably records every egress
/// decision, marks the reservation uncertain immediately before transport,
/// enforces bounded normalized output, and commits authoritative usage. It
/// deliberately leaves run completion and transcript persistence to the next
/// fenced orchestration layer.
pub struct AiProviderCallExecutor {
    runtime: Arc<AiRuntime>,
    budget_service: Arc<dyn AiBudgetService>,
    egress_audit: Arc<dyn AiEgressDecisionAudit>,
    usage_accounting: Arc<dyn AiProviderUsageAccounting>,
    clock: Arc<dyn Clock>,
    limits: AiProviderCallLimits,
    live_delta_sink: Option<Arc<dyn AiLiveDeltaSink>>,
    live_delta_limits: AiLiveDeltaCoalescerLimits,
    attachment_resolver: Option<Arc<dyn AiProviderAttachmentResolver>>,
    attachment_limits: AiProviderAttachmentResolutionLimits,
}

impl AiProviderCallExecutor {
    /// Creates a provider-turn executor.
    pub fn new(
        runtime: Arc<AiRuntime>,
        budget_service: Arc<dyn AiBudgetService>,
        egress_audit: Arc<dyn AiEgressDecisionAudit>,
        usage_accounting: Arc<dyn AiProviderUsageAccounting>,
        clock: Arc<dyn Clock>,
        limits: AiProviderCallLimits,
    ) -> Self {
        Self {
            runtime,
            budget_service,
            egress_audit,
            usage_accounting,
            clock,
            limits,
            live_delta_sink: None,
            live_delta_limits: AiLiveDeltaCoalescerLimits::default(),
            attachment_resolver: None,
            attachment_limits: AiProviderAttachmentResolutionLimits::default(),
        }
    }

    /// Enables protected durable visible-delta persistence for this executor.
    ///
    /// Without a sink the provider result remains fully bounded and durable
    /// final output is unchanged, but no provisional session events are
    /// emitted. The sink never receives structured tool arguments or hidden
    /// reasoning.
    #[must_use]
    pub fn with_live_delta_sink(
        mut self,
        sink: Arc<dyn AiLiveDeltaSink>,
        limits: AiLiveDeltaCoalescerLimits,
    ) -> Self {
        self.live_delta_sink = Some(sink);
        self.live_delta_limits = limits;
        self
    }

    /// Enables exact released-attachment reopening for provider inputs.
    ///
    /// Without this resolver every provider plan containing an attachment
    /// fails before the uncertain/transport boundary.
    #[must_use]
    pub fn with_attachment_resolver(
        mut self,
        resolver: Arc<dyn AiProviderAttachmentResolver>,
        limits: AiProviderAttachmentResolutionLimits,
    ) -> Self {
        self.attachment_resolver = Some(resolver);
        self.attachment_limits = limits;
        self
    }

    /// Executes one exact provider turn for a current running lease.
    ///
    /// Provider/stream failures after the transport boundary intentionally
    /// leave the budget reservation `Uncertain`; expired-run reconciliation
    /// then moves the run to `RecoveryRequired` instead of replaying it.
    ///
    /// # Errors
    ///
    /// Fails closed for runtime readiness, stale/bad plan binding, current
    /// principal or access denial, budget denial, egress/audit denial, provider
    /// error, custom-tool output, missing completion/usage, output bounds, or
    /// reconciliation failure.
    pub async fn execute(
        &self,
        lease: &AiRunLease,
        plan: AiProviderCallPlan,
    ) -> Result<AiProviderCallResult, AiError> {
        if !self.runtime.start_gate().is_ready()
            || lease.state() != AiRunState::Running
            || plan.budget.session_id != lease.session_id()
            || plan.budget.run_id != lease.run_id()
            || plan.budget.attempt_id != lease.attempt_id()
            || plan.budget.lease_generation != lease.lease_generation()
            || plan
                .request
                .maximum_builtin_tool_calls
                .is_some_and(|calls| calls > self.limits.maximum_builtin_tool_calls as u64)
        {
            return Err(AiError::Conflict);
        }

        let principal = self
            .runtime
            .resolve_current_principal(lease.principal_reference())
            .await?;
        let scope_access = self
            .runtime
            .access_policy()
            .can_access_scope(
                principal.principal(),
                &plan.budget.scope,
                AiSessionAction::Write,
            )
            .await;
        let session_access = self
            .runtime
            .access_policy()
            .can_access_session(
                principal.principal(),
                lease.session_id(),
                AiSessionAction::Write,
            )
            .await;
        if !scope_access.is_allowed() || !session_access.is_allowed() {
            return Err(AiError::Forbidden);
        }

        let reservation = self
            .budget_service
            .reserve(&principal, plan.budget.clone())
            .await?;
        let authorized_budget = reservation
            .authorize_provider_call(
                lease.run_id(),
                lease.attempt_id(),
                lease.lease_generation(),
                &plan.provider_kind,
                &plan.request.model,
                plan.request.maximum_output_tokens.unwrap_or(0),
                plan.request.maximum_builtin_tool_calls(),
                self.clock.now(),
            )
            .map_err(|_| AiError::BudgetDenied)?;

        let mut context = match self
            .authorize_and_audit_transfers(lease, &plan, authorized_budget)
            .await
        {
            Ok(context) => context,
            Err(error) => {
                self.release_unstarted(lease, &reservation).await?;
                return Err(error);
            }
        };

        let mut current = match self
            .runtime
            .resolve_current_principal(lease.principal_reference())
            .await
        {
            Ok(current) => current,
            Err(error) => {
                self.release_unstarted(lease, &reservation).await?;
                return Err(error);
            }
        };
        if !self
            .runtime
            .access_policy()
            .can_access_scope(
                current.principal(),
                &plan.budget.scope,
                AiSessionAction::Write,
            )
            .await
            .is_allowed()
            || !self
                .runtime
                .access_policy()
                .can_access_session(
                    current.principal(),
                    lease.session_id(),
                    AiSessionAction::Write,
                )
                .await
                .is_allowed()
        {
            self.release_unstarted(lease, &reservation).await?;
            return Err(AiError::Forbidden);
        }
        if plan
            .request
            .input
            .iter()
            .any(|block| matches!(block, ModelInputBlock::Attachment { .. }))
        {
            let Some(resolver) = &self.attachment_resolver else {
                self.release_unstarted(lease, &reservation).await?;
                return Err(AiError::RuntimeNotReady);
            };
            let resolved = match self
                .resolve_provider_attachments(
                    resolver.as_ref(),
                    &current,
                    lease,
                    &plan.budget.scope,
                    &plan.request,
                )
                .await
            {
                Ok(resolved) => resolved,
                Err(error) => {
                    self.release_unstarted(lease, &reservation).await?;
                    return Err(error);
                }
            };
            context = match context.with_resolved_attachments(&plan.request, resolved) {
                Ok(context) => context,
                Err(_) => {
                    self.release_unstarted(lease, &reservation).await?;
                    return Err(AiError::ReauthorizationFailed);
                }
            };
            current = match self
                .runtime
                .resolve_current_principal(lease.principal_reference())
                .await
            {
                Ok(current) => current,
                Err(error) => {
                    self.release_unstarted(lease, &reservation).await?;
                    return Err(error);
                }
            };
            if !self
                .runtime
                .access_policy()
                .can_access_scope(
                    current.principal(),
                    &plan.budget.scope,
                    AiSessionAction::Write,
                )
                .await
                .is_allowed()
                || !self
                    .runtime
                    .access_policy()
                    .can_access_session(
                        current.principal(),
                        lease.session_id(),
                        AiSessionAction::Write,
                    )
                    .await
                    .is_allowed()
            {
                self.release_unstarted(lease, &reservation).await?;
                return Err(AiError::Forbidden);
            }
        }
        self.budget_service
            .reconcile(
                &current,
                AiBudgetReconciliation {
                    reservation_id: reservation.id(),
                    attempt_id: lease.attempt_id(),
                    lease_generation: lease.lease_generation(),
                    actual: None,
                    cached_input_tokens: None,
                    outcome: AiBudgetReconciliationOutcome::MarkUncertain,
                },
            )
            .await?;

        let provider_model = plan.request.model.clone();
        let live_scope = plan.budget.scope.clone();
        let live_correlation_id = plan.correlation_id.clone();
        let live_provider_kind = plan.provider_kind.clone();
        let offered_builtin_kinds = plan
            .request
            .builtin_tools
            .iter()
            .map(|builtin| match builtin {
                ModelBuiltinTool::WebSearch { .. } => "web_search",
                ModelBuiltinTool::FileSearch { .. } => "file_search",
                ModelBuiltinTool::CodeInterpreter => "code_interpreter",
                ModelBuiltinTool::ImageGeneration => "image_generation",
            })
            .collect::<BTreeSet<_>>();
        let maximum_normalized_builtin_tool_calls = plan
            .request
            .maximum_builtin_tool_calls
            .and_then(|calls| usize::try_from(calls).ok())
            .unwrap_or(self.limits.maximum_builtin_tool_calls);
        let request_snapshot = plan.request.clone();
        let model_inference_manifest = plan
            .transfers
            .iter()
            .find(|manifest| manifest.capability == AiEgressCapability::ModelInference)
            .cloned()
            .ok_or(AiError::EgressDenied)?;
        let previous_response_id =
            plan.request
                .continuation
                .as_ref()
                .and_then(|continuation| match continuation {
                    ModelContinuation::ProviderResponse { response_id } => {
                        Some(response_id.clone())
                    }
                    ModelContinuation::StatelessConversation { .. } => None,
                });
        let previous_continuation_reference = plan
            .request
            .continuation
            .as_ref()
            .and_then(|continuation| continuation.chain_reference(&plan.request.input));
        let replay_tool_transfers = plan
            .transfers
            .iter()
            .filter(|manifest| manifest.capability == AiEgressCapability::ToolResult)
            .cloned()
            .collect::<Vec<_>>();
        let offered_tools = plan
            .request
            .tools
            .iter()
            .map(|tool| {
                (
                    tool.tool_id.clone(),
                    (
                        tool.provider_name.clone(),
                        tool.fingerprint.clone(),
                        tool.parameters.clone(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut stream = self
            .runtime
            .stream_provider(&plan.provider_kind, plan.request, context)
            .await
            .map_err(|_| AiError::ProviderFailed)?;
        let mut events = Vec::new();
        let mut total_bytes = 0usize;
        let mut usage = None;
        let mut provider_response_id = None;
        let mut completed = false;
        let mut started_tool_calls = BTreeMap::new();
        let mut completed_tool_calls = BTreeMap::new();
        let mut tool_call_order = Vec::new();
        let mut tool_argument_bytes = BTreeMap::<String, usize>::new();
        let mut started_builtin_calls = BTreeMap::<String, String>::new();
        let mut completed_builtin_calls = BTreeSet::new();
        let mut live_coalescer = self
            .live_delta_sink
            .as_ref()
            .map(|_| AiLiveDeltaCoalescer::new(self.live_delta_limits));
        loop {
            let item = if live_coalescer.is_some() {
                tokio::select! {
                    item = stream.next() => item,
                    () = tokio::time::sleep(self.live_delta_limits.maximum_delay()) => {
                        let batches = live_coalescer
                            .as_mut()
                            .ok_or(AiError::ProviderFailed)?
                            .flush_due(Instant::now())?;
                        self.persist_live_batches(
                            lease,
                            &live_scope,
                            &live_correlation_id,
                            &live_provider_kind,
                            &provider_model,
                            provider_response_id.as_deref(),
                            reservation.id(),
                            &batches,
                        )
                        .await?;
                        continue;
                    }
                }
            } else {
                stream.next().await
            };
            let Some(item) = item else {
                break;
            };
            let event = item.map_err(|_| AiError::ProviderFailed)?;
            let event_bytes = serde_json::to_vec(&event)
                .map_err(|_| AiError::ProviderFailed)?
                .len();
            total_bytes = total_bytes
                .checked_add(event_bytes)
                .ok_or(AiError::ProviderFailed)?;
            if events.len() >= self.limits.maximum_events
                || event_bytes > self.limits.maximum_event_bytes
                || total_bytes > self.limits.maximum_total_event_bytes
            {
                return Err(AiError::ProviderFailed);
            }
            match &event {
                ProviderEvent::ResponseStarted { response_id }
                | ProviderEvent::ResponseCompleted { response_id } => {
                    if response_id
                        .as_ref()
                        .is_some_and(|value| value.len() > 1_024)
                    {
                        return Err(AiError::ProviderFailed);
                    }
                    if response_id.is_some() {
                        provider_response_id.clone_from(response_id);
                    }
                    if matches!(event, ProviderEvent::ResponseCompleted { .. }) {
                        completed = true;
                    }
                }
                ProviderEvent::Usage {
                    input_tokens,
                    output_tokens,
                    cached_input_tokens,
                } => usage = Some((*input_tokens, *output_tokens, *cached_input_tokens)),
                ProviderEvent::ToolCallStarted { call_id, tool_id } => {
                    if !valid_provider_call_id(call_id)
                        || !offered_tools.contains_key(tool_id)
                        || started_tool_calls.contains_key(call_id)
                        || started_builtin_calls.contains_key(call_id)
                        || tool_call_order.len() >= self.limits.maximum_tool_calls
                    {
                        return Err(AiError::ProviderFailed);
                    }
                    started_tool_calls.insert(call_id.clone(), tool_id.clone());
                    tool_argument_bytes.insert(call_id.clone(), 0);
                    tool_call_order.push(call_id.clone());
                }
                ProviderEvent::ToolArgumentsDelta { call_id, delta } => {
                    let Some(total) = tool_argument_bytes.get_mut(call_id) else {
                        return Err(AiError::ProviderFailed);
                    };
                    *total = total
                        .checked_add(delta.len())
                        .ok_or(AiError::ProviderFailed)?;
                    if *total > self.limits.maximum_event_bytes {
                        return Err(AiError::ProviderFailed);
                    }
                }
                ProviderEvent::ToolCallCompleted { call_id, arguments } => {
                    let Some(tool_id) = started_tool_calls.get(call_id) else {
                        return Err(AiError::ProviderFailed);
                    };
                    if completed_tool_calls.contains_key(call_id) {
                        return Err(AiError::ProviderFailed);
                    }
                    let Some((provider_name, fingerprint, argument_schema)) =
                        offered_tools.get(tool_id)
                    else {
                        return Err(AiError::ProviderFailed);
                    };
                    let validator = jsonschema::validator_for(argument_schema)
                        .map_err(|_| AiError::ProviderFailed)?;
                    if !validator.is_valid(arguments) {
                        return Err(AiError::ProviderFailed);
                    }
                    completed_tool_calls.insert(
                        call_id.clone(),
                        AiProviderToolCall {
                            call_id: call_id.clone(),
                            tool_id: crate::AiToolId::parse(tool_id.clone())
                                .map_err(|_| AiError::ProviderFailed)?,
                            provider_name: provider_name.clone(),
                            tool_fingerprint: fingerprint.clone(),
                            arguments: arguments.clone(),
                        },
                    );
                }
                ProviderEvent::BuiltinToolStarted { call_id, kind } => {
                    if !valid_provider_call_id(call_id)
                        || !offered_builtin_kinds.contains(kind.as_str())
                        || started_tool_calls.contains_key(call_id)
                        || started_builtin_calls.contains_key(call_id)
                        || started_builtin_calls.len() >= maximum_normalized_builtin_tool_calls
                    {
                        return Err(AiError::ProviderFailed);
                    }
                    started_builtin_calls.insert(call_id.clone(), kind.clone());
                }
                ProviderEvent::BuiltinToolCompleted { call_id, .. }
                    if !started_builtin_calls.contains_key(call_id)
                        || !completed_builtin_calls.insert(call_id.clone()) =>
                {
                    return Err(AiError::ProviderFailed);
                }
                _ => {}
            }
            if let Some(coalescer) = live_coalescer.as_mut() {
                let batches = coalescer.push_event(&event, Instant::now())?;
                self.persist_live_batches(
                    lease,
                    &live_scope,
                    &live_correlation_id,
                    &live_provider_kind,
                    &provider_model,
                    provider_response_id.as_deref(),
                    reservation.id(),
                    &batches,
                )
                .await?;
            }
            events.push(event);
        }
        if let Some(coalescer) = live_coalescer.as_mut() {
            let batches = coalescer.flush_all()?;
            self.persist_live_batches(
                lease,
                &live_scope,
                &live_correlation_id,
                &live_provider_kind,
                &provider_model,
                provider_response_id.as_deref(),
                reservation.id(),
                &batches,
            )
            .await?;
        }
        let Some((input_tokens, output_tokens, cached_input_tokens)) = usage else {
            return Err(AiError::ProviderFailed);
        };
        if !completed {
            return Err(AiError::ProviderFailed);
        }
        if started_tool_calls.len() != completed_tool_calls.len() {
            return Err(AiError::ProviderFailed);
        }
        if started_builtin_calls.len() != completed_builtin_calls.len() {
            return Err(AiError::ProviderFailed);
        }
        let mut builtin_usage = AiProviderBuiltinUsage::default();
        for (call_id, kind) in &started_builtin_calls {
            if !completed_builtin_calls.contains(call_id) {
                return Err(AiError::ProviderFailed);
            }
            builtin_usage.record(kind)?;
        }
        let mut tool_calls = Vec::with_capacity(tool_call_order.len());
        for call_id in tool_call_order {
            tool_calls.push(
                completed_tool_calls
                    .remove(&call_id)
                    .ok_or(AiError::ProviderFailed)?,
            );
        }
        if (request_snapshot.continuation_mode == ModelContinuationMode::StatelessReplay
            && provider_response_id.is_some())
            || (request_snapshot.continuation_mode == ModelContinuationMode::ProviderRetained
                && !tool_calls.is_empty()
                && provider_response_id.is_none())
        {
            return Err(AiError::ProviderFailed);
        }

        let observation = AiProviderUsageObservation {
            scope: plan.budget.scope.clone(),
            provider_kind: plan.provider_kind.clone(),
            model: provider_model.clone(),
            pricing_policy_version: plan.budget.pricing_policy_version,
            input_tokens,
            output_tokens,
            cached_input_tokens,
            builtin_usage,
        };
        let actual = self.usage_accounting.settle(&observation).await?;
        if actual.input_tokens != input_tokens
            || actual.output_tokens != output_tokens
            || actual.runs != 1
        {
            return Err(AiError::ProviderFailed);
        }
        let current = self
            .runtime
            .resolve_current_principal(lease.principal_reference())
            .await?;
        self.budget_service
            .reconcile(
                &current,
                AiBudgetReconciliation {
                    reservation_id: reservation.id(),
                    attempt_id: lease.attempt_id(),
                    lease_generation: lease.lease_generation(),
                    actual: Some(actual),
                    cached_input_tokens: Some(cached_input_tokens),
                    outcome: AiBudgetReconciliationOutcome::Commit,
                },
            )
            .await?;

        Ok(AiProviderCallResult {
            session_id: lease.session_id(),
            run_id: lease.run_id(),
            attempt_id: lease.attempt_id(),
            lease_generation: lease.lease_generation(),
            provider_kind: plan.provider_kind,
            provider_model,
            events,
            usage: actual,
            cached_input_tokens,
            provider_response_id,
            budget_reservation_id: reservation.id(),
            previous_response_id,
            previous_continuation_reference,
            tool_calls,
            request_snapshot,
            model_inference_manifest,
            replay_tool_transfers,
        })
    }

    async fn resolve_provider_attachments(
        &self,
        resolver: &dyn AiProviderAttachmentResolver,
        principal: &ResolvedPrincipal,
        lease: &AiRunLease,
        scope: &AiScope,
        request: &ModelRequest,
    ) -> Result<Vec<AiResolvedProviderAttachment>, AiError> {
        let attachment_requests = request
            .input
            .iter()
            .filter(|block| matches!(block, ModelInputBlock::Attachment { .. }))
            .map(AiProviderAttachmentRequest::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        if attachment_requests.len() > self.attachment_limits.maximum_attachments {
            return Err(AiError::InvalidInput(
                "provider attachment count exceeds deployment limit".to_owned(),
            ));
        }
        let mut total_bytes = 0_u64;
        let mut resolved = Vec::with_capacity(attachment_requests.len());
        for attachment in attachment_requests {
            if attachment.byte_count() > self.attachment_limits.maximum_attachment_bytes {
                return Err(AiError::InvalidInput(
                    "provider attachment exceeds deployment byte limit".to_owned(),
                ));
            }
            total_bytes = total_bytes
                .checked_add(attachment.byte_count())
                .ok_or_else(|| {
                    AiError::InvalidInput("provider attachment total overflow".to_owned())
                })?;
            if total_bytes > self.attachment_limits.maximum_total_bytes {
                return Err(AiError::InvalidInput(
                    "provider attachments exceed deployment total byte limit".to_owned(),
                ));
            }
            let item = resolver
                .resolve_for_provider(principal, lease.session_id(), scope, &attachment)
                .await?;
            if item.request() != &attachment {
                return Err(AiError::ReauthorizationFailed);
            }
            resolved.push(item);
        }
        Ok(resolved)
    }

    #[allow(clippy::too_many_arguments)]
    async fn persist_live_batches(
        &self,
        lease: &AiRunLease,
        scope: &AiScope,
        correlation_id: &str,
        provider_kind: &ProviderKind,
        provider_model: &str,
        provider_response_id: Option<&str>,
        budget_reservation_id: AiBudgetReservationId,
        batches: &[AiLiveDeltaBatch],
    ) -> Result<(), AiError> {
        let Some(sink) = &self.live_delta_sink else {
            return Ok(());
        };
        for batch in batches {
            let context = AiLiveDeltaPersistenceContext::new(
                lease,
                scope.clone(),
                correlation_id.to_owned(),
                provider_kind.clone(),
                provider_model.to_owned(),
                provider_response_id.map(str::to_owned),
                budget_reservation_id,
            );
            sink.persist_batch(lease, &context, batch).await?;
        }
        Ok(())
    }

    async fn authorize_and_audit_transfers(
        &self,
        lease: &AiRunLease,
        plan: &AiProviderCallPlan,
        budget: crate::AuthorizedBudgetReservation,
    ) -> Result<ProviderRequestContext, AiError> {
        let mut authorized = Vec::with_capacity(plan.transfers.len());
        for manifest in &plan.transfers {
            let decision = self
                .runtime
                .authorize_egress(lease.principal_reference(), manifest)
                .await?;
            self.egress_audit.record(manifest, &decision).await?;
            let proof = decision.authorize(manifest)?;
            authorized.push((manifest.clone(), proof));
        }
        let mut authorized = authorized.into_iter();
        let (inference_manifest, inference_proof) =
            authorized.next().ok_or(AiError::EgressDenied)?;
        let mut context = ProviderRequestContext::new(
            lease.session_id(),
            lease.run_id(),
            &plan.correlation_id,
            budget,
            inference_manifest,
            inference_proof,
        )?;
        for (manifest, proof) in authorized {
            context = context.with_authorized_transfer(manifest, proof)?;
        }
        Ok(context)
    }

    async fn release_unstarted(
        &self,
        lease: &AiRunLease,
        reservation: &AiBudgetReservation,
    ) -> Result<(), AiError> {
        let current = self
            .runtime
            .resolve_current_principal(lease.principal_reference())
            .await?;
        self.budget_service
            .reconcile(
                &current,
                AiBudgetReconciliation {
                    reservation_id: reservation.id(),
                    attempt_id: lease.attempt_id(),
                    lease_generation: lease.lease_generation(),
                    actual: None,
                    cached_input_tokens: None,
                    outcome: AiBudgetReconciliationOutcome::ReleaseUnused,
                },
            )
            .await?;
        Ok(())
    }
}

fn valid_provider_call_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 1_024
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'"' | b'\\'))
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;
    use agql_auth::{
        AccessTokenMetadata, AuthPrincipal, AuthUser, CurrentPrincipalResolver, ResolvedPrincipal,
        SessionContext, SystemClock,
    };
    use async_trait::async_trait;
    use graphql_orm::graphql::errors::{OrmErrorCode, OrmPublicError};
    use graphql_orm::graphql::orm::{
        ApplyOptions, ConditionalUpdateOutcome, OrmSchemaModule, TransactionMode,
    };
    use graphql_orm::prelude::{Database, SqliteBackend};
    use serde_json::json;
    use sha2::Digest;
    use time::{Duration, OffsetDateTime};
    use uuid::Uuid;

    use crate::orm_runs::{
        PreparedCoordinatorCheckpoint, PreparedCoordinatorCheckpointTool,
        coordinator_checkpoint_hash,
    };
    use crate::persistence::*;
    use crate::*;

    struct Resolver(AuthPrincipal);

    #[async_trait]
    impl CurrentPrincipalResolver for Resolver {
        async fn resolve(
            &self,
            reference: &agql_auth::PrincipalReference,
        ) -> agql_auth::AuthResult<ResolvedPrincipal> {
            ResolvedPrincipal::new(reference.clone(), self.0.clone(), OffsetDateTime::now_utc())
        }
    }

    struct AllowAccess;

    #[async_trait]
    impl AiAccessPolicy for AllowAccess {
        async fn can_access_scope(
            &self,
            _principal: &AuthPrincipal,
            _scope: &AiScope,
            _action: AiSessionAction,
        ) -> AiAccessDecision {
            AiAccessDecision::allow("provider_test", "access-v1")
        }

        async fn can_access_session(
            &self,
            _principal: &AuthPrincipal,
            _session_id: AiSessionId,
            _action: AiSessionAction,
        ) -> AiAccessDecision {
            AiAccessDecision::allow("provider_test", "access-v1")
        }
    }

    struct AllowEgress;

    #[async_trait]
    impl AiEgressPolicy for AllowEgress {
        async fn authorize(
            &self,
            principal: &ResolvedPrincipal,
            manifest: &AiEgressManifest,
        ) -> AiEgressDecision {
            AiEgressDecision::allow(manifest, "egress-v1", principal.principal().subject())
        }
    }

    struct ContextFactory;

    #[async_trait]
    impl GraphqlRequestContextFactory for ContextFactory {
        async fn build(
            &self,
            principal: &ResolvedPrincipal,
            _target: &GraphqlExecutionTarget,
            _request: &ToolGraphqlRequest,
        ) -> Result<GraphqlRequestContext, ToolExecutionError> {
            Ok(GraphqlRequestContext::new(
                principal.principal().subject().to_owned(),
            ))
        }
    }

    struct Executor(Arc<AtomicBool>);

    #[async_trait]
    impl AuthenticatedGraphqlExecutor for Executor {
        async fn execute(
            &self,
            context: GraphqlRequestContext,
            request: ToolGraphqlRequest,
        ) -> Result<ToolGraphqlResponse, ToolExecutionError> {
            if self.0.load(Ordering::SeqCst) {
                return Err(ToolExecutionError::Execution);
            }
            let subject = context
                .downcast_ref::<String>()
                .ok_or(ToolExecutionError::RequestContext)?;
            Ok(ToolGraphqlResponse {
                data: json!({
                    "subject": subject,
                    "recordId": request.variables.get("recordId"),
                }),
                error_codes: Vec::new(),
                application_audit_ref: Some("application-audit-1".to_owned()),
            })
        }
    }

    struct AllowTools(Arc<AtomicUsize>);

    #[async_trait]
    impl AiToolAuthorizationPolicy for AllowTools {
        async fn authorize(
            &self,
            principal: &ResolvedPrincipal,
            _scope: &AiScope,
            _descriptor: &AiToolDescriptor,
            _variables: &serde_json::Value,
        ) -> AiToolAuthorizationDecision {
            AiToolAuthorizationDecision::allow(
                "test_read_allowed",
                format!("tool-policy-v{}", self.0.load(Ordering::SeqCst)),
                format!(
                    "auth-state:{}:v{}",
                    principal.principal().subject(),
                    self.0.load(Ordering::SeqCst)
                ),
            )
        }
    }

    struct AllowApprovals;

    #[async_trait]
    impl AiApprovalAccessPolicy for AllowApprovals {
        async fn can_access_approval(
            &self,
            _principal: &AuthPrincipal,
            _scope: &AiScope,
            _session_id: AiSessionId,
            _action: AiApprovalAction,
        ) -> bool {
            true
        }
    }

    struct PreviewBuilder;

    #[async_trait]
    impl AiCanonicalActionPreviewBuilder for PreviewBuilder {
        async fn build_preview(
            &self,
            _principal: &ResolvedPrincipal,
            _descriptor: &AiToolDescriptor,
            request: &ToolGraphqlRequest,
        ) -> Result<AiCanonicalActionPreview, AiError> {
            let record_id = request
                .variables
                .get("recordId")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| AiError::InvalidInput("record ID is missing".to_owned()))?;
            Ok(AiCanonicalActionPreview {
                action_kind: "record_update".to_owned(),
                title: "Update one authorized record".to_owned(),
                targets: vec![AiApprovalResourceBinding {
                    resource_type: "record".to_owned(),
                    resource_id: record_id.to_owned(),
                    expected_version: "record-version-1".to_owned(),
                }],
                details: json!({"recordId": record_id, "change": "test_update"}),
            })
        }
    }

    struct ProtectionPolicy;

    #[async_trait]
    impl AiContentProtectionPolicyResolver for ProtectionPolicy {
        async fn resolve(
            &self,
            _principal: &AuthPrincipal,
            scope: &AiScope,
        ) -> Result<AiContentProtectionPolicy, AiError> {
            Ok(AiContentProtectionPolicy {
                scope: scope.clone(),
                mode: AiContentProtectionMode::DatabaseManaged,
                key_policy_reference: None,
                version: 1,
                ready: true,
            })
        }
    }

    struct FailAudit;

    #[async_trait]
    impl AiEgressDecisionAudit for FailAudit {
        async fn record(
            &self,
            _manifest: &AiEgressManifest,
            _decision: &AiEgressDecision,
        ) -> Result<(), AiError> {
            Err(AiError::PersistenceFailed)
        }
    }

    struct RejectLiveSink;

    #[async_trait]
    impl AiLiveDeltaSink for RejectLiveSink {
        async fn persist_batch(
            &self,
            _lease: &AiRunLease,
            _context: &AiLiveDeltaPersistenceContext,
            _batch: &AiLiveDeltaBatch,
        ) -> Result<(), AiError> {
            Err(AiError::PersistenceFailed)
        }
    }

    struct ExactAttachmentResolver {
        bytes: Arc<[u8]>,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl AiProviderAttachmentResolver for ExactAttachmentResolver {
        async fn resolve_for_provider(
            &self,
            _principal: &ResolvedPrincipal,
            _session_id: AiSessionId,
            _scope: &AiScope,
            request: &AiProviderAttachmentRequest,
        ) -> Result<AiResolvedProviderAttachment, AiError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            AiResolvedProviderAttachment::new(
                request.clone(),
                "provider-test.png",
                self.bytes.clone(),
            )
        }
    }

    struct TestUsageAccounting;

    #[async_trait]
    impl AiProviderUsageAccounting for TestUsageAccounting {
        async fn settle(
            &self,
            observation: &AiProviderUsageObservation,
        ) -> Result<AiBudgetAmounts, AiError> {
            if observation.pricing_policy_version() != "test-pricing-v1" {
                return Err(AiError::InvalidConfiguration(
                    "unknown test pricing version".to_owned(),
                ));
            }
            Ok(AiBudgetAmounts {
                input_tokens: observation.input_tokens(),
                output_tokens: observation.output_tokens(),
                tool_units: observation
                    .builtin_usage()
                    .web_search_calls()
                    .checked_add(observation.builtin_usage().file_search_calls())
                    .ok_or_else(|| {
                        AiError::InvalidConfiguration("test tool-unit overflow".to_owned())
                    })?,
                image_units: 0,
                cost_microunits: 42,
                runs: 1,
            })
        }
    }

    fn test_rules(scope: AiScope) -> AiResolvedRuleSet {
        test_rules_with_fingerprint(scope, '2')
    }

    fn test_rules_with_fingerprint(scope: AiScope, fingerprint: char) -> AiResolvedRuleSet {
        AiResolvedRuleSet::new(
            scope,
            AiRuleConstraints {
                enabled: true,
                maximum_classification: DataClassification::Restricted,
                maximum_tool_maturity: ToolMaturity::SupervisedWrite,
                approval_requirement: AiRuleApprovalRequirement::DescriptorPolicy,
                allowed_tool_fingerprints: None,
                allowed_provider_kinds: None,
                allowed_provider_capabilities: None,
                allow_provider_retention: true,
                allow_byok: true,
                budget: AiRuleBudgetCeilings {
                    maximum_steps: Some(32),
                    maximum_duration_seconds: Some(3_600),
                    maximum_output_tokens: Some(100_000),
                    maximum_cost_microunits: Some(100_000_000),
                    maximum_provider_calls: Some(16),
                    maximum_tool_units: Some(1_000),
                    maximum_image_units: Some(1_000),
                },
            },
            Vec::new(),
            fingerprint.to_string().repeat(64),
        )
    }

    fn test_rule_checkpoint(
        scope: &AiScope,
        results: &[&AiProviderCallResult],
        total_tool_calls: usize,
    ) -> (AiResolvedRuleSet, AiRuleRunUsage) {
        let resolution =
            AiAgentRuleResolution::new(test_rules(scope.clone()), OffsetDateTime::now_utc())
                .expect("test rules should resolve");
        let mut usage = AiRuleRunUsage::default();
        for result in results {
            usage = usage
                .accept_provider(result.usage(), &resolution)
                .expect("provider usage should fit test rules");
        }
        usage = usage
            .accept_tool_calls(total_tool_calls, &resolution)
            .expect("tool steps should fit test rules");
        (resolution.rules().clone(), usage)
    }

    #[derive(Default)]
    struct TestRuleResolver {
        fingerprint_version: AtomicUsize,
    }

    #[async_trait]
    impl AiAgentRuleResolver for TestRuleResolver {
        async fn resolve_rules(
            &self,
            _lease: &AiRunLease,
            scope: &AiScope,
        ) -> Result<AiAgentRuleResolution, AiError> {
            let rules = test_rules_with_fingerprint(
                scope.clone(),
                if self.fingerprint_version.load(Ordering::SeqCst) == 0 {
                    '2'
                } else {
                    '3'
                },
            );
            AiAgentRuleResolution::new(rules, OffsetDateTime::now_utc())
        }
    }

    struct Fixture {
        database: Database<SqliteBackend>,
        runtime: Arc<AiRuntime>,
        run_service: OrmAiRunService,
        budget_service: Arc<OrmAiBudgetService>,
        audit: Arc<OrmAiEgressDecisionAudit>,
        principal: AuthPrincipal,
        mock: MockProvider,
        lease: AiRunLease,
        scope: AiScope,
        tool_policy_version: Arc<AtomicUsize>,
        fail_execution: Arc<AtomicBool>,
    }

    async fn fixture(events: Vec<ProviderEvent>) -> Fixture {
        fixture_with_provider(MockProvider::new(events)).await
    }

    async fn fixture_with_event_batches(
        batches: impl IntoIterator<Item = Vec<ProviderEvent>>,
    ) -> Fixture {
        fixture_with_provider(MockProvider::new(Vec::new()).with_event_batches(batches)).await
    }

    async fn fixture_with_provider(mock: MockProvider) -> Fixture {
        let database = Database::<SqliteBackend>::connect_sqlite("sqlite::memory:")
            .await
            .expect("in-memory SQLite should open");
        let module = AiSchemaModule;
        let migration = database
            .schema()
            .plan_migration_to_entities(
                "provider-call-test-v1",
                "Provider call executor test",
                module.entities(),
            )
            .await
            .expect("AI schema should plan");
        database
            .schema()
            .apply_migration(&migration, ApplyOptions::default())
            .await
            .expect("AI schema should apply");

        let principal = AuthPrincipal::User(AuthUser {
            user_id: "provider-user".to_owned(),
            session_id: Uuid::new_v4(),
            roles: vec![],
            scopes: vec![],
            session: SessionContext::default(),
            token_claims: AccessTokenMetadata {
                tenant_id: Some("provider-tenant".to_owned()),
                ..AccessTokenMetadata::default()
            },
        });
        let scope = AiScope::new("tenant", "provider-tenant").with_tenant_id("provider-tenant");
        let session_id = AiSessionId::new();
        let run_id = AiRunId::new();
        let now = OffsetDateTime::now_utc();
        AiSessionRecord::insert(
            &database,
            CreateAiSessionRecordInput {
                id: session_id.0,
                owner_principal_kind: "user".to_owned(),
                owner_subject: principal.subject().to_owned(),
                tenant_id: scope.tenant_id.clone(),
                scope_kind: scope.kind.clone(),
                scope_id: scope.id.clone(),
                title: "Provider call test".to_owned(),
                state: "active".to_owned(),
                stream_head: 0,
                message_head: 0,
                last_activity_at: now.unix_timestamp(),
                archived_at: None,
                deleted_at: None,
            },
        )
        .await
        .expect("test session should insert");
        AiRunRecord::insert(
            &database,
            CreateAiRunRecordInput {
                id: run_id.0,
                session_id: session_id.0,
                input_message_id: Uuid::new_v4(),
                principal_reference: serde_json::to_value(principal.reference())
                    .expect("principal reference should serialize"),
                state: AiRunState::Queued.as_str().to_owned(),
                attempt_id: None,
                lease_owner: None,
                lease_generation: 0,
                lease_expires_at: None,
                lease_heartbeat_at: None,
                retry_count: 0,
                next_attempt_at: Some(now.unix_timestamp()),
                error_code: None,
                latest_checkpoint_id: None,
            },
        )
        .await
        .expect("test run should insert");
        AiBudgetPolicyRecord::insert(
            &database,
            CreateAiBudgetPolicyRecordInput {
                scope_key: crate::ai_scope_key(&scope),
                scope_kind: scope.kind.clone(),
                scope_id: scope.id.clone(),
                tenant_id: scope.tenant_id.clone(),
                principal_kind: None,
                principal_subject: None,
                interval_kind: "day".to_owned(),
                maximum_input_tokens: Some(10_000),
                maximum_output_tokens: Some(10_000),
                maximum_tool_units: Some(100),
                maximum_image_units: Some(100),
                maximum_cost_microunits: Some(1_000_000),
                maximum_runs: Some(100),
                enabled: true,
            },
        )
        .await
        .expect("test budget policy should insert");

        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let run_limits =
            AiRunServiceLimits::new(Duration::minutes(5), Duration::hours(1), 16, 2, 8)
                .expect("test run limits should validate");
        let run_service = OrmAiRunService::new(database.clone(), clock.clone(), run_limits);
        let lease = run_service
            .claim_next("provider-worker")
            .await
            .expect("test run should claim")
            .expect("test run should be eligible");
        let lease = run_service
            .start(&lease)
            .await
            .expect("test run should start");

        let budget_limits = AiBudgetServiceLimits::new(
            AiBudgetAmounts {
                input_tokens: 10_000,
                output_tokens: 10_000,
                tool_units: 100,
                image_units: 100,
                cost_microunits: 1_000_000,
                runs: 1,
            },
            Duration::minutes(5),
            Duration::seconds(30),
            16,
            8,
        )
        .expect("test budget limits should validate");
        let budget_service = Arc::new(OrmAiBudgetService::new(
            database.clone(),
            clock,
            budget_limits,
        ));
        let audit = Arc::new(OrmAiEgressDecisionAudit::new(database.clone()));
        let document =
            "query Record($recordId: ID!) { record(id: $recordId) { recordId subject } }";
        let disclosure = AiDisclosureSchema::new(
            "record-v1",
            AiDisclosureShape::object(
                AiDisclosureRule::exportable(DataClassification::Internal),
                [
                    (
                        "recordId".to_owned(),
                        AiDisclosureShape::scalar(AiDisclosureRule::exportable(
                            DataClassification::Internal,
                        )),
                    ),
                    (
                        "subject".to_owned(),
                        AiDisclosureShape::scalar(AiDisclosureRule::exportable(
                            DataClassification::Internal,
                        )),
                    ),
                ],
            ),
        )
        .expect("tool disclosure should validate");
        let contract = GraphqlOperationContract::new(
            GraphqlExecutionTargetId::parse("local-application").expect("target ID should parse"),
            "schema-v1",
            "Record",
            document,
            "record-projection-v1",
            disclosure.fingerprint.clone(),
        )
        .expect("tool contract should validate");
        let descriptor = AiToolDescriptor::new(
            "records.read",
            "Read one authorized record",
            AiToolOperationKind::Query,
            document,
            json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"recordId": {"type": "string"}},
                "required": ["recordId"],
                "additionalProperties": false
            }),
        )
        .expect("tool descriptor should validate")
        .with_result_projection("record-projection-v1")
        .with_graphql_contract(contract);
        let mut tool_catalog = AiToolCatalog::new();
        tool_catalog
            .register_with_disclosure(descriptor, disclosure)
            .expect("tool should register");
        let write_document = "mutation UpdateRecord($recordId: ID!) { updateRecord(id: $recordId) { recordId subject } }";
        let write_disclosure = AiDisclosureSchema::new(
            "record-update-v1",
            AiDisclosureShape::object(
                AiDisclosureRule::exportable(DataClassification::Internal),
                [
                    (
                        "recordId".to_owned(),
                        AiDisclosureShape::scalar(AiDisclosureRule::exportable(
                            DataClassification::Internal,
                        )),
                    ),
                    (
                        "subject".to_owned(),
                        AiDisclosureShape::scalar(AiDisclosureRule::exportable(
                            DataClassification::Internal,
                        )),
                    ),
                ],
            ),
        )
        .expect("write disclosure should validate");
        let write_contract = GraphqlOperationContract::new(
            GraphqlExecutionTargetId::parse("local-application").expect("target ID should parse"),
            "schema-v1",
            "UpdateRecord",
            write_document,
            "record-update-projection-v1",
            write_disclosure.fingerprint.clone(),
        )
        .expect("write contract should validate");
        let write_descriptor = AiToolDescriptor::new(
            "records.update",
            "Update one authorized record after exact approval",
            AiToolOperationKind::Mutation,
            write_document,
            json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"recordId": {"type": "string"}},
                "required": ["recordId"],
                "additionalProperties": false
            }),
        )
        .expect("write descriptor should validate")
        .with_result_projection("record-update-projection-v1")
        .with_graphql_contract(write_contract)
        .with_maturity(ToolMaturity::SupervisedWrite)
        .with_risk(AiToolRisk::HighImpact, AiApprovalRule::OneShot);
        tool_catalog
            .register_with_disclosure(write_descriptor, write_disclosure)
            .expect("supervised write should register");
        let mut targets = GraphqlExecutionTargetRegistry::new();
        targets
            .register(GraphqlExecutionTarget {
                id: GraphqlExecutionTargetId::parse("local-application")
                    .expect("target ID should parse"),
                class: GraphqlExecutionTargetClass::Local,
                audience: None,
                resource_type: None,
                resource_id: None,
                schema_fingerprint: "schema-v1".to_owned(),
            })
            .expect("target should register");
        let tool_policy_version = Arc::new(AtomicUsize::new(1));
        let fail_execution = Arc::new(AtomicBool::new(false));
        let runtime = AiRuntime::builder()
            .principal_resolver(Arc::new(Resolver(principal.clone())))
            .access_policy(Arc::new(AllowAccess))
            .tool_authorization_policy(Arc::new(AllowTools(tool_policy_version.clone())))
            .request_context_factory(Arc::new(ContextFactory))
            .graphql_executor(Arc::new(Executor(fail_execution.clone())))
            .graphql_targets(targets)
            .egress_policy(Arc::new(AllowEgress))
            .deployment_egress(AiDeploymentEgressBoundary {
                allowed_destination_trust: [AiDestinationTrust::Local].into_iter().collect(),
                allowed_capabilities: [
                    AiEgressCapability::ModelInference,
                    AiEgressCapability::ToolResult,
                    AiEgressCapability::ImageAnalysis,
                    AiEgressCapability::ProviderFile,
                    AiEgressCapability::WebSearch,
                ]
                .into_iter()
                .collect(),
                maximum_classification: DataClassification::Internal,
                maximum_bytes: 16_384,
                maximum_attachments: 8,
            })
            .maximum_tool_maturity(ToolMaturity::SupervisedWrite)
            .tool_catalog(tool_catalog)
            .secret_store(Arc::new(EnvironmentSecretStore::new()))
            .content_protection_policy_resolver(Arc::new(ProtectionPolicy))
            .content_protector(Arc::new(DatabaseManagedContentProtector))
            .provider(Arc::new(mock.clone()))
            .expect("mock provider should register")
            .build()
            .expect("test runtime should build");
        runtime
            .start_gate()
            .open(&AiRuntimeReadinessReport {
                module_fingerprint: runtime
                    .start_gate()
                    .expected_module_fingerprint()
                    .to_owned(),
                executor_bound: true,
                restore_reconciled: true,
                fatal_issue_count: 0,
            })
            .expect("test runtime should open");

        Fixture {
            database,
            runtime: Arc::new(runtime),
            run_service,
            budget_service,
            audit,
            principal,
            mock,
            lease,
            scope,
            tool_policy_version,
            fail_execution,
        }
    }

    #[tokio::test]
    async fn one_custom_tool_definition_still_requires_parallel_rule_capability() {
        let fixture = fixture(Vec::new()).await;
        let plan = tool_plan(&fixture);
        assert_eq!(plan.request.tools.len(), 1);

        let mut constraints = test_rules(fixture.scope.clone()).constraints().clone();
        constraints.allowed_provider_capabilities = Some(BTreeSet::from([
            AiRuleProviderCapability::Streaming,
            AiRuleProviderCapability::CustomTools,
        ]));
        let without_parallel = AiAgentRuleResolution::new(
            AiResolvedRuleSet::new(
                fixture.scope.clone(),
                constraints.clone(),
                Vec::new(),
                "5".repeat(64),
            ),
            OffsetDateTime::now_utc(),
        )
        .expect("test rule resolution should validate");
        assert!(matches!(
            plan.project_rule_usage(&without_parallel, AiRuleRunUsage::default(), false),
            Err(AiError::EgressDenied)
        ));

        constraints
            .allowed_provider_capabilities
            .as_mut()
            .expect("test allowlist should exist")
            .insert(AiRuleProviderCapability::ParallelToolCalls);
        let with_parallel = AiAgentRuleResolution::new(
            AiResolvedRuleSet::new(fixture.scope, constraints, Vec::new(), "6".repeat(64)),
            OffsetDateTime::now_utc(),
        )
        .expect("test rule resolution should validate");
        plan.project_rule_usage(&with_parallel, AiRuleRunUsage::default(), false)
            .expect("both custom-tool capabilities should permit the estimate");
    }

    fn plan(fixture: &Fixture) -> AiProviderCallPlan {
        let request = ModelRequest {
            model: "mock-model".to_owned(),
            instructions: vec!["Return a bounded test response".to_owned()],
            input: vec![ModelInputBlock::Text {
                text: "hello".to_owned(),
            }],
            continuation: None,
            continuation_mode: crate::ModelContinuationMode::ProviderRetained,
            tools: vec![],
            builtin_tools: vec![],
            maximum_builtin_tool_calls: None,
            output_schema: None,
            maximum_output_tokens: Some(100),
        };
        let budget = AiBudgetReservationRequest {
            scope: fixture.scope.clone(),
            session_id: fixture.lease.session_id(),
            run_id: fixture.lease.run_id(),
            attempt_id: fixture.lease.attempt_id(),
            lease_generation: fixture.lease.lease_generation(),
            provider_kind: ProviderKind::OpenAiCompatible,
            model: request.model.clone(),
            pricing_policy_version: "test-pricing-v1".to_owned(),
            estimate: AiBudgetAmounts {
                input_tokens: 100,
                output_tokens: 100,
                tool_units: 0,
                image_units: 0,
                cost_microunits: 100,
                runs: 1,
            },
            idempotency_key: format!("provider:{}:1", fixture.lease.attempt_id()),
            expires_at: OffsetDateTime::now_utc() + Duration::minutes(2),
        };
        let estimated_bytes = request.conservative_egress_bytes();
        let manifest = AiEgressManifest {
            provider_profile_id: "mock-profile".to_owned(),
            provider_kind: ProviderKind::OpenAiCompatible.as_str().to_owned(),
            model: request.model.clone(),
            destination: "local-mock".to_owned(),
            destination_trust: AiDestinationTrust::Local,
            capability: AiEgressCapability::ModelInference,
            scope: fixture.scope.clone(),
            session_id: Some(fixture.lease.session_id()),
            run_id: Some(fixture.lease.run_id()),
            sources: vec![AiDataSourceRef {
                kind: "message_block".to_owned(),
                reference: "test-source".to_owned(),
                classification: DataClassification::Internal,
                trust: AiSourceTrust::UserProvided,
            }],
            estimated_bytes,
            estimated_tokens: 100,
            attachment_count: 0,
            purpose: "test_inference".to_owned(),
            retention: "none".to_owned(),
            residency: None,
            policy_version: "egress-v1".to_owned(),
            consent_reference: None,
        };
        AiProviderCallPlan::new(
            ProviderKind::OpenAiCompatible,
            request,
            budget,
            vec![manifest],
            "provider-call-test",
        )
        .expect("test provider plan should validate")
    }

    #[cfg(feature = "provider-openai")]
    fn background_plan(fixture: &Fixture) -> AiProviderCallPlan {
        let request = ModelRequest {
            model: "mock-model".to_owned(),
            instructions: vec!["Return a bounded background test response".to_owned()],
            input: vec![ModelInputBlock::Text {
                text: "hello in the background".to_owned(),
            }],
            continuation: None,
            continuation_mode: ModelContinuationMode::ProviderRetained,
            tools: vec![],
            builtin_tools: vec![],
            maximum_builtin_tool_calls: None,
            output_schema: None,
            maximum_output_tokens: Some(100),
        };
        let budget = AiBudgetReservationRequest {
            scope: fixture.scope.clone(),
            session_id: fixture.lease.session_id(),
            run_id: fixture.lease.run_id(),
            attempt_id: fixture.lease.attempt_id(),
            lease_generation: fixture.lease.lease_generation(),
            provider_kind: ProviderKind::OpenAi,
            model: request.model.clone(),
            pricing_policy_version: "test-pricing-v1".to_owned(),
            estimate: AiBudgetAmounts {
                input_tokens: 100,
                output_tokens: 100,
                tool_units: 0,
                image_units: 0,
                cost_microunits: 100,
                runs: 1,
            },
            idempotency_key: format!("provider-background:{}:1", fixture.lease.attempt_id()),
            expires_at: OffsetDateTime::now_utc() + Duration::minutes(2),
        };
        let manifest = AiEgressManifest {
            provider_profile_id: "mock-profile".to_owned(),
            provider_kind: ProviderKind::OpenAi.as_str().to_owned(),
            model: request.model.clone(),
            destination: "local-mock".to_owned(),
            destination_trust: AiDestinationTrust::Local,
            capability: AiEgressCapability::ModelInference,
            scope: fixture.scope.clone(),
            session_id: Some(fixture.lease.session_id()),
            run_id: Some(fixture.lease.run_id()),
            sources: vec![AiDataSourceRef {
                kind: "message_block".to_owned(),
                reference: "background-test-source".to_owned(),
                classification: DataClassification::Internal,
                trust: AiSourceTrust::UserProvided,
            }],
            estimated_bytes: request.conservative_egress_bytes(),
            estimated_tokens: 100,
            attachment_count: 0,
            purpose: "test_background_inference".to_owned(),
            retention: AI_EGRESS_RETENTION_PROVIDER_RESPONSE.to_owned(),
            residency: None,
            policy_version: "egress-v1".to_owned(),
            consent_reference: None,
        };
        AiProviderCallPlan::new(
            ProviderKind::OpenAi,
            request,
            budget,
            vec![manifest],
            "provider-background-test",
        )
        .expect("background provider plan should validate")
    }

    #[cfg(feature = "provider-openai")]
    #[tokio::test]
    async fn background_submission_binds_one_call_and_parks_run_without_a_lease() {
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let mock = MockProvider::new(Vec::new())
            .with_kind(ProviderKind::OpenAi)
            .with_capabilities(ProviderCapabilities {
                streaming: true,
                structured_output: true,
                background: true,
                provider_retained_continuation: true,
                local: true,
                ..ProviderCapabilities::default()
            })
            .with_background_submission("resp_background_orm_1", "queued", now);
        let fixture = fixture_with_provider(mock).await;
        let service = OrmAiOpenAiBackgroundSubmissionService::new(
            fixture.run_service.clone(),
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            fixture.audit.clone(),
            Arc::new(SystemClock),
        );

        let accepted = service
            .submit(&fixture.lease, background_plan(&fixture))
            .await
            .expect("exact background submission should be accepted");
        assert_eq!(fixture.mock.request_count(), 1);
        assert_eq!(accepted.run_id(), fixture.lease.run_id());
        assert_eq!(accepted.attempt_id(), fixture.lease.attempt_id());
        assert_eq!(
            accepted.lease_generation(),
            fixture.lease.lease_generation()
        );
        assert_eq!(accepted.provider_profile_id(), "mock-profile");
        assert_eq!(accepted.maximum_output_tokens(), 100);
        assert!(!accepted.provider_store());
        assert_eq!(accepted.provider_response_id(), "resp_background_orm_1");
        assert_eq!(accepted.provider_status(), "queued");

        let submission = AiProviderBackgroundSubmissionRecord::find_by_id(
            &fixture.database,
            &accepted.submission_id(),
        )
        .await
        .expect("submission lookup should succeed")
        .expect("submission should exist");
        assert_eq!(submission.run_id, fixture.lease.run_id().0);
        assert_eq!(submission.attempt_id, fixture.lease.attempt_id());
        assert_eq!(
            submission.lease_generation,
            fixture.lease.lease_generation()
        );
        assert_eq!(submission.provider_kind, "openai");
        assert_eq!(submission.provider_profile_id, "mock-profile");
        assert_eq!(submission.provider_model, "mock-model");
        assert_eq!(submission.maximum_output_tokens, 100);
        assert_eq!(submission.provider_store, Some(false));
        assert_eq!(submission.state, "waiting_provider");
        assert_eq!(
            submission.provider_response_id.as_deref(),
            Some("resp_background_orm_1")
        );
        assert_eq!(submission.provider_status.as_deref(), Some("queued"));
        assert_eq!(submission.provider_created_at, Some(now));
        assert!(submission.submitted_at.is_some());
        assert_eq!(submission.row_version, 1);

        let run = AiRunRecord::find_by_id(&fixture.database, &fixture.lease.run_id().0)
            .await
            .expect("run lookup should succeed")
            .expect("run should exist");
        assert_eq!(run.state, AiRunState::WaitingProvider.as_str());
        assert_eq!(run.attempt_id, Some(fixture.lease.attempt_id()));
        assert_eq!(run.lease_generation, fixture.lease.lease_generation());
        assert!(run.lease_owner.is_none());
        assert!(run.lease_expires_at.is_none());
        assert!(run.lease_heartbeat_at.is_none());

        let reservation = AiBudgetReservationRecord::find_by_id(
            &fixture.database,
            &accepted.budget_reservation_id().0,
        )
        .await
        .expect("budget lookup should succeed")
        .expect("budget should exist");
        assert_eq!(reservation.state, "uncertain");
        assert!(reservation.reconciled_at.is_some());
        assert!(reservation.actual_input_tokens.is_none());
        assert!(reservation.actual_output_tokens.is_none());

        let audit_actions = fixture
            .database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    tx.query::<AiAuditEventRecord>()
                        .fetch_all()
                        .await
                        .map(|rows| {
                            rows.into_iter()
                                .filter(|row| {
                                    row.resource_kind == "ai_provider_background_submission"
                                })
                                .map(|row| row.action)
                                .collect::<Vec<_>>()
                        })
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("background audit events should query");
        assert_eq!(audit_actions.len(), 2);
        assert!(
            audit_actions
                .iter()
                .any(|action| action == "prepare_provider_background_submission")
        );
        assert!(
            audit_actions
                .iter()
                .any(|action| action == "accept_provider_background_submission")
        );

        let debug = format!("{accepted:?}");
        assert!(!debug.contains("resp_background_orm_1"));
        assert!(!debug.contains("mock-profile"));
        assert!(!debug.contains(&fixture.lease.run_id().0.to_string()));

        assert!(
            service
                .submit(&fixture.lease, background_plan(&fixture))
                .await
                .is_err()
        );
        assert_eq!(fixture.mock.request_count(), 1);
    }

    #[cfg(feature = "provider-openai")]
    #[tokio::test]
    async fn background_submission_releases_budget_when_egress_audit_fails() {
        let mock = MockProvider::new(Vec::new())
            .with_kind(ProviderKind::OpenAi)
            .with_capabilities(ProviderCapabilities {
                background: true,
                provider_retained_continuation: true,
                local: true,
                ..ProviderCapabilities::default()
            });
        let fixture = fixture_with_provider(mock).await;
        let service = OrmAiOpenAiBackgroundSubmissionService::new(
            fixture.run_service.clone(),
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            Arc::new(FailAudit),
            Arc::new(SystemClock),
        );

        assert!(matches!(
            service
                .submit(&fixture.lease, background_plan(&fixture))
                .await,
            Err(AiError::PersistenceFailed)
        ));
        assert_eq!(fixture.mock.request_count(), 0);
        assert_eq!(reservation_state(&fixture.database).await, "released");
        let submissions = fixture
            .database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    tx.query::<AiProviderBackgroundSubmissionRecord>()
                        .limit(1)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("background submissions should query");
        assert!(submissions.is_empty());
    }

    #[cfg(feature = "provider-openai")]
    #[tokio::test]
    async fn background_submission_releases_budget_when_prepare_fence_is_stale() {
        let mock = MockProvider::new(Vec::new())
            .with_kind(ProviderKind::OpenAi)
            .with_capabilities(ProviderCapabilities {
                background: true,
                provider_retained_continuation: true,
                local: true,
                ..ProviderCapabilities::default()
            });
        let fixture = fixture_with_provider(mock).await;
        fixture
            .run_service
            .heartbeat(&fixture.lease)
            .await
            .expect("test heartbeat should invalidate the original row version");
        let service = OrmAiOpenAiBackgroundSubmissionService::new(
            fixture.run_service.clone(),
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            fixture.audit.clone(),
            Arc::new(SystemClock),
        );

        assert!(matches!(
            service
                .submit(&fixture.lease, background_plan(&fixture))
                .await,
            Err(AiError::Conflict)
        ));
        assert_eq!(fixture.mock.request_count(), 0);
        assert_eq!(reservation_state(&fixture.database).await, "released");
        let submissions = fixture
            .database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    tx.query::<AiProviderBackgroundSubmissionRecord>()
                        .limit(1)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("background submissions should query");
        assert!(submissions.is_empty());
    }

    #[cfg(feature = "provider-openai")]
    #[tokio::test]
    async fn background_submission_requires_an_explicit_output_ceiling() {
        let mock = MockProvider::new(Vec::new())
            .with_kind(ProviderKind::OpenAi)
            .with_capabilities(ProviderCapabilities {
                background: true,
                provider_retained_continuation: true,
                local: true,
                ..ProviderCapabilities::default()
            });
        let fixture = fixture_with_provider(mock).await;
        let service = OrmAiOpenAiBackgroundSubmissionService::new(
            fixture.run_service.clone(),
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            fixture.audit.clone(),
            Arc::new(SystemClock),
        );
        let mut plan = background_plan(&fixture);
        plan.request.maximum_output_tokens = None;

        assert!(matches!(
            service.submit(&fixture.lease, plan).await,
            Err(AiError::Conflict)
        ));
        assert_eq!(fixture.mock.request_count(), 0);
        let reservations = fixture
            .database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    tx.query::<AiBudgetReservationRecord>()
                        .limit(1)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("budget reservations should query");
        assert!(reservations.is_empty());
    }

    #[cfg(feature = "provider-openai")]
    #[tokio::test]
    async fn background_submission_heartbeats_while_waiting_for_acknowledgement() {
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let mock = MockProvider::new(Vec::new())
            .with_kind(ProviderKind::OpenAi)
            .with_capabilities(ProviderCapabilities {
                background: true,
                provider_retained_continuation: true,
                local: true,
                ..ProviderCapabilities::default()
            })
            .with_background_submission("resp_background_heartbeat_1", "queued", now)
            .with_background_delay(std::time::Duration::from_millis(1_200));
        let fixture = fixture_with_provider(mock).await;
        let before = AiRunRecord::find_by_id(&fixture.database, &fixture.lease.run_id().0)
            .await
            .expect("run lookup should succeed")
            .expect("run should exist");
        let short_run_service = OrmAiRunService::new(
            fixture.database.clone(),
            Arc::new(SystemClock),
            AiRunServiceLimits::new(Duration::seconds(3), Duration::hours(1), 16, 2, 8)
                .expect("short test lease should validate"),
        );
        let service = OrmAiOpenAiBackgroundSubmissionService::new(
            short_run_service,
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            fixture.audit.clone(),
            Arc::new(SystemClock),
        );

        service
            .submit(&fixture.lease, background_plan(&fixture))
            .await
            .expect("slow acknowledgement should retain its exact fence");
        assert_eq!(fixture.mock.request_count(), 1);
        let after = AiRunRecord::find_by_id(&fixture.database, &fixture.lease.run_id().0)
            .await
            .expect("run lookup should succeed")
            .expect("run should exist");
        assert_eq!(after.state, AiRunState::WaitingProvider.as_str());
        assert!(after.row_version >= before.row_version + 3);
    }

    #[cfg(feature = "provider-openai")]
    #[tokio::test]
    async fn prior_ai_schema_migrates_to_background_submission_binding() {
        let database = Database::<SqliteBackend>::connect_sqlite("sqlite::memory:")
            .await
            .expect("in-memory SQLite should open");
        let module = AiSchemaModule;
        let prior_entities = module
            .entities()
            .iter()
            .copied()
            .filter(|entity| entity.table_name != "graphql_orm_ai_provider_background_submissions")
            .collect::<Vec<_>>();
        let prior_plan = database
            .schema()
            .plan_migration_to_entities(
                "provider-background-prior-v1",
                "prior AI schema",
                &prior_entities,
            )
            .await
            .expect("prior AI schema should plan");
        database
            .schema()
            .apply_migration(&prior_plan, ApplyOptions::default())
            .await
            .expect("prior AI schema should apply");

        let current_plan = database
            .schema()
            .plan_migration_to_entities(
                "provider-background-current-v1",
                "current AI schema",
                module.entities(),
            )
            .await
            .expect("current AI schema should plan");
        database
            .schema()
            .apply_migration(&current_plan, ApplyOptions::default())
            .await
            .expect("background submission table should migrate without row rewrites");
    }

    #[cfg(feature = "provider-openai")]
    #[tokio::test]
    async fn malformed_background_acknowledgement_closes_run_for_manual_recovery() {
        let mock = MockProvider::new(Vec::new())
            .with_kind(ProviderKind::OpenAi)
            .with_capabilities(ProviderCapabilities {
                background: true,
                provider_retained_continuation: true,
                local: true,
                ..ProviderCapabilities::default()
            })
            .with_background_submission(
                "invalid-response-id",
                "queued",
                OffsetDateTime::now_utc().unix_timestamp(),
            );
        let fixture = fixture_with_provider(mock).await;
        let service = OrmAiOpenAiBackgroundSubmissionService::new(
            fixture.run_service.clone(),
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            fixture.audit.clone(),
            Arc::new(SystemClock),
        );

        assert!(matches!(
            service
                .submit(&fixture.lease, background_plan(&fixture))
                .await,
            Err(AiError::ProviderFailed)
        ));
        assert_eq!(fixture.mock.request_count(), 1);
        let run = AiRunRecord::find_by_id(&fixture.database, &fixture.lease.run_id().0)
            .await
            .expect("run lookup should succeed")
            .expect("run should exist");
        assert_eq!(run.state, AiRunState::RecoveryRequired.as_str());
        assert_eq!(
            run.error_code.as_deref(),
            Some("provider_acknowledgement_not_persisted")
        );
        assert!(run.lease_owner.is_none());
        assert!(run.lease_expires_at.is_none());
        assert!(run.lease_heartbeat_at.is_none());

        let submissions = fixture
            .database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    tx.query::<AiProviderBackgroundSubmissionRecord>()
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("submission should query");
        assert_eq!(submissions.len(), 1);
        assert_eq!(submissions[0].state, "recovery_required");
        assert_eq!(
            submissions[0].safe_error_code.as_deref(),
            Some("provider_acknowledgement_not_persisted")
        );
        assert!(submissions[0].provider_response_id.is_none());
        assert!(submissions[0].provider_status.is_none());
        let reservation = AiBudgetReservationRecord::find_by_id(
            &fixture.database,
            &submissions[0].budget_reservation_id,
        )
        .await
        .expect("budget lookup should succeed")
        .expect("budget should exist");
        assert_eq!(reservation.state, "uncertain");

        let outcomes = fixture
            .database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    tx.query::<AiRunAttemptOutcomeRecord>()
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("attempt outcomes should query");
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].attempt_id, fixture.lease.attempt_id());
        assert_eq!(
            outcomes[0].final_state,
            AiRunState::RecoveryRequired.as_str()
        );
        assert_eq!(
            outcomes[0].outcome_code,
            "provider_acknowledgement_not_persisted"
        );
    }

    #[cfg(feature = "provider-openai")]
    #[tokio::test]
    async fn swapped_background_acknowledgement_binding_requires_manual_recovery() {
        let mock = MockProvider::new(Vec::new())
            .with_kind(ProviderKind::OpenAi)
            .with_capabilities(ProviderCapabilities {
                background: true,
                provider_retained_continuation: true,
                local: true,
                ..ProviderCapabilities::default()
            })
            .with_background_submission(
                "resp_background_swapped_1",
                "queued",
                OffsetDateTime::now_utc().unix_timestamp(),
            )
            .with_background_binding("swapped-model", 100, true);
        let fixture = fixture_with_provider(mock).await;
        let service = OrmAiOpenAiBackgroundSubmissionService::new(
            fixture.run_service.clone(),
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            fixture.audit.clone(),
            Arc::new(SystemClock),
        );

        assert!(matches!(
            service
                .submit(&fixture.lease, background_plan(&fixture))
                .await,
            Err(AiError::ProviderFailed)
        ));
        assert_eq!(fixture.mock.request_count(), 1);
        let submissions = fixture
            .database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    tx.query::<AiProviderBackgroundSubmissionRecord>()
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("submission should query");
        assert_eq!(submissions.len(), 1);
        assert_eq!(submissions[0].state, "recovery_required");
        assert!(submissions[0].provider_store.is_none());
        assert!(submissions[0].provider_response_id.is_none());
        assert_eq!(reservation_state(&fixture.database).await, "uncertain");
    }

    fn web_search_plan(fixture: &Fixture, maximum_builtin_tool_calls: u64) -> AiProviderCallPlan {
        let mut base = plan(fixture);
        base.request.builtin_tools = vec![ModelBuiltinTool::WebSearch {
            allowed_domains: vec!["example.com".to_owned()],
        }];
        base.request.maximum_builtin_tool_calls = Some(maximum_builtin_tool_calls);
        base.budget.estimate.tool_units = maximum_builtin_tool_calls;
        base.transfers[0].estimated_bytes = base.request.conservative_egress_bytes();
        let mut web_search_manifest = base.transfers[0].clone();
        web_search_manifest.capability = AiEgressCapability::WebSearch;
        base.transfers.push(web_search_manifest);
        AiProviderCallPlan::new(
            base.provider_kind,
            base.request,
            base.budget,
            base.transfers,
            base.correlation_id,
        )
        .expect("web-search provider plan should validate")
    }

    fn tool_plan(fixture: &Fixture) -> AiProviderCallPlan {
        let descriptor = fixture
            .runtime
            .tool_catalog()
            .descriptor(&AiToolId::parse("records.read").expect("tool ID should parse"))
            .expect("tool should be registered");
        let mut policy = AiToolPolicySet::new(ToolMaturity::ReadOnly);
        policy.bind(AiToolPolicyBinding {
            tool_id: descriptor.id.clone(),
            fingerprint: descriptor.fingerprint.clone(),
            enabled: true,
        });
        let mut base = plan(fixture);
        base.request.tools = vec![ModelToolDefinition {
            tool_id: descriptor.id.as_str().to_owned(),
            provider_name: "records_read".to_owned(),
            fingerprint: descriptor.fingerprint.clone(),
            description: descriptor.description.clone(),
            parameters: descriptor.argument_schema.clone(),
            strict: true,
        }];
        base.transfers[0].estimated_bytes = base.request.conservative_egress_bytes();
        AiProviderCallPlan::new_with_tools(
            base.provider_kind,
            base.request,
            base.budget,
            base.transfers,
            base.correlation_id,
            fixture.runtime.tool_catalog(),
            &policy,
        )
        .expect("registered enabled read-only tool plan should validate")
    }

    fn stateless_tool_plan(fixture: &Fixture) -> AiProviderCallPlan {
        let mut plan = tool_plan(fixture);
        plan.request.continuation_mode = ModelContinuationMode::StatelessReplay;
        plan.transfers[0].estimated_bytes = plan.request.conservative_egress_bytes();
        plan
    }

    fn attachment_plan(fixture: &Fixture, bytes: &[u8]) -> AiProviderCallPlan {
        let mut base = plan(fixture);
        let attachment = ModelInputBlock::Attachment {
            attachment_id: Uuid::new_v4().to_string(),
            mime: "image/png".to_owned(),
            byte_count: bytes.len() as u64,
            sha256: hex::encode(sha2::Sha256::digest(bytes)),
        };
        let exact_reference = attachment
            .attachment_egress_reference()
            .expect("attachment source should be canonical");
        base.request.input.push(attachment);
        base.budget.estimate.image_units = 1;
        let estimated_bytes = base.request.conservative_egress_bytes();
        base.transfers[0].attachment_count = 1;
        base.transfers[0].estimated_bytes = estimated_bytes;
        let image_manifest = AiEgressManifest {
            provider_profile_id: "mock-profile".to_owned(),
            provider_kind: ProviderKind::OpenAiCompatible.as_str().to_owned(),
            model: base.request.model.clone(),
            destination: "local-mock".to_owned(),
            destination_trust: AiDestinationTrust::Local,
            capability: AiEgressCapability::ImageAnalysis,
            scope: fixture.scope.clone(),
            session_id: Some(fixture.lease.session_id()),
            run_id: Some(fixture.lease.run_id()),
            sources: vec![AiDataSourceRef {
                kind: "attachment".to_owned(),
                reference: exact_reference,
                classification: DataClassification::Internal,
                trust: AiSourceTrust::UserProvided,
            }],
            estimated_bytes,
            estimated_tokens: 100,
            attachment_count: 1,
            purpose: "test_image_analysis".to_owned(),
            retention: "none".to_owned(),
            residency: None,
            policy_version: "egress-v1".to_owned(),
            consent_reference: None,
        };
        base.transfers.push(image_manifest);
        AiProviderCallPlan::new(
            base.provider_kind,
            base.request,
            base.budget,
            base.transfers,
            base.correlation_id,
        )
        .expect("exact attachment provider plan should validate")
    }

    fn supervised_tool_plan(fixture: &Fixture) -> AiProviderCallPlan {
        let descriptor = fixture
            .runtime
            .tool_catalog()
            .descriptor(&AiToolId::parse("records.update").expect("tool ID should parse"))
            .expect("supervised tool should be registered");
        let mut policy = AiToolPolicySet::new(ToolMaturity::SupervisedWrite);
        policy.bind(AiToolPolicyBinding {
            tool_id: descriptor.id.clone(),
            fingerprint: descriptor.fingerprint.clone(),
            enabled: true,
        });
        let mut base = plan(fixture);
        base.request.tools = vec![ModelToolDefinition {
            tool_id: descriptor.id.as_str().to_owned(),
            provider_name: "records_update".to_owned(),
            fingerprint: descriptor.fingerprint.clone(),
            description: descriptor.description.clone(),
            parameters: descriptor.argument_schema.clone(),
            strict: true,
        }];
        base.transfers[0].estimated_bytes = base.request.conservative_egress_bytes();
        AiProviderCallPlan::new_with_supervised_tools(
            base.provider_kind,
            base.request,
            base.budget,
            base.transfers,
            base.correlation_id,
            fixture.runtime.tool_catalog(),
            &policy,
        )
        .expect("registered enabled supervised tool plan should validate")
    }

    async fn stage_approved_supervised_call(
        fixture: &Fixture,
    ) -> (
        OrmAiConsequentialToolCallService,
        AiRequestedConsequentialToolCall,
    ) {
        let provider_executor = AiProviderCallExecutor::new(
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            fixture.audit.clone(),
            Arc::new(TestUsageAccounting),
            Arc::new(SystemClock),
            AiProviderCallLimits::new(64, 8_192, 64 * 1_024)
                .expect("test provider limits should validate"),
        );
        let provider_result = provider_executor
            .execute(&fixture.lease, supervised_tool_plan(fixture))
            .await
            .expect("supervised provider call should normalize");
        let approval_service = OrmAiApprovalService::new(
            fixture.database.clone(),
            fixture.run_service.clone(),
            Arc::new(Resolver(fixture.principal.clone())),
            Arc::new(AllowApprovals),
            Arc::new(fixture.runtime.tool_catalog().clone()),
            agql_auth::RecentMfaPolicy {
                maximum_age: Duration::minutes(5),
                clock_skew: Duration::seconds(30),
                allowed_amr: Vec::new(),
                allowed_acr: Vec::new(),
                match_mode: agql_auth::AssuranceMatchMode::All,
            },
            Arc::new(ProtectionPolicy),
            Arc::new(DatabaseManagedContentProtector),
            Arc::new(SystemClock),
        );
        let service = OrmAiConsequentialToolCallService::new(
            fixture.run_service.clone(),
            fixture.runtime.clone(),
            approval_service.clone(),
            Arc::new(PreviewBuilder),
            fixture.audit.clone(),
            Arc::new(SystemClock),
            AiApplicationToolCallLimits::new(
                8_192,
                16_384,
                4,
                4,
                Duration::seconds(30),
                Duration::seconds(10),
            )
            .expect("test tool limits should validate"),
        );
        let context = AiApplicationToolCallContext::new(
            0,
            0,
            fixture.scope.clone(),
            "supervised-recovery-test",
            provider_result.budget_reservation_id().0.to_string(),
        )
        .expect("supervised context should validate");
        let requested = service
            .request_approval(
                &fixture.lease,
                &provider_result,
                context,
                OffsetDateTime::now_utc() + Duration::minutes(5),
                false,
            )
            .await
            .expect("supervised call should park for approval");
        let pending = AiApprovalRecord::find_by_id(&fixture.database, &requested.approval_id().0)
            .await
            .expect("approval lookup should succeed")
            .expect("approval should exist");
        approval_service
            .decide_approval(
                &fixture.principal,
                DecideAiApprovalInput {
                    id: requested.approval_id().0,
                    decision: AiApprovalDecision::Approve,
                    expected_version: pending.row_version,
                },
            )
            .await
            .expect("human approval should persist");
        (service, requested)
    }

    async fn reservation_state(database: &Database<SqliteBackend>) -> String {
        database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    let rows = tx
                        .query::<AiBudgetReservationRecord>()
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if rows.len() != 1 {
                        return Err(OrmPublicError::internal(
                            "expected exactly one budget reservation",
                        ));
                    }
                    Ok(rows.into_iter().next().expect("one row was checked").state)
                })
            })
            .await
            .expect("reservation state should query")
    }

    #[tokio::test]
    async fn successful_mock_turn_audits_egress_and_commits_authoritative_usage() {
        let fixture = fixture(vec![
            ProviderEvent::ResponseStarted {
                response_id: Some("mock-response".to_owned()),
            },
            ProviderEvent::TextDelta {
                text: "hello back".to_owned(),
            },
            ProviderEvent::Usage {
                input_tokens: 12,
                output_tokens: 3,
                cached_input_tokens: 2,
            },
            ProviderEvent::ResponseCompleted {
                response_id: Some("mock-response".to_owned()),
            },
        ])
        .await;
        let live_sink = Arc::new(OrmAiLiveDeltaService::new(
            fixture.run_service.clone(),
            fixture.runtime.clone(),
            Arc::new(SystemClock),
            AiLiveDeltaPersistenceLimits::new(4_096, Duration::seconds(30))
                .expect("test live persistence limits should validate"),
        ));
        let executor = AiProviderCallExecutor::new(
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            fixture.audit.clone(),
            Arc::new(TestUsageAccounting),
            Arc::new(SystemClock),
            AiProviderCallLimits::new(64, 8_192, 64 * 1_024)
                .expect("test provider limits should validate"),
        )
        .with_live_delta_sink(
            live_sink,
            AiLiveDeltaCoalescerLimits::new(std::time::Duration::from_millis(50), 4_096)
                .expect("test live coalescer limits should validate"),
        );
        let result = executor
            .execute(&fixture.lease, plan(&fixture))
            .await
            .expect("mock provider turn should succeed");
        assert_eq!(fixture.mock.request_count(), 1);
        assert_eq!(result.usage().input_tokens, 12);
        assert_eq!(result.usage().output_tokens, 3);
        assert_eq!(result.usage().cost_microunits, 42);
        assert_eq!(result.cached_input_tokens(), 2);
        assert_eq!(result.provider_response_id(), Some("mock-response"));
        assert_eq!(reservation_state(&fixture.database).await, "committed");

        let live_events = fixture
            .database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    tx.query::<AiSessionEventRecord>()
                        .filter(AiSessionEventRecordWhereInput {
                            event_type: Some(graphql_orm::graphql::filters::StringFilter {
                                eq: Some("provider_live_delta".to_owned()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(4)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("live events should query");
        assert_eq!(live_events.len(), 1);
        assert_eq!(live_events[0].run_id, Some(fixture.lease.run_id().0));
        assert_eq!(
            live_events[0]
                .protected_payload
                .get("value")
                .and_then(|value| value.get("text"))
                .and_then(serde_json::Value::as_str),
            Some("hello back")
        );
        assert_eq!(
            live_events[0]
                .protected_payload
                .get("value")
                .and_then(|value| value.get("provisional"))
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        let session_service = OrmAiSessionService::new(
            fixture.database.clone(),
            Arc::new(AllowAccess),
            Arc::new(ProtectionPolicy),
            Arc::new(DatabaseManagedContentProtector),
        );
        let live_page = session_service
            .session_event_page(&fixture.principal, fixture.lease.session_id(), 0, 10)
            .await
            .expect("authorized session window should open live content");
        assert_eq!(live_page.events.len(), 1);
        assert_eq!(live_page.events[0].event_type, "provider_live_delta");
        assert_eq!(
            live_page.events[0]
                .payload
                .0
                .get("text")
                .and_then(serde_json::Value::as_str),
            Some("hello back")
        );

        let audit_count = fixture
            .database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    tx.query::<AiEgressEventRecord>()
                        .count()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("egress audit count should query");
        assert_eq!(audit_count, 1);
        let output_service = OrmAiProviderOutputService::new(
            fixture.run_service.clone(),
            Arc::new(Resolver(fixture.principal.clone())),
            Arc::new(AllowAccess),
            Arc::new(ProtectionPolicy),
            Arc::new(DatabaseManagedContentProtector),
            Arc::new(SystemClock),
            AiProviderOutputLimits::new(8, 16, 8, 64 * 1_024, Duration::seconds(30))
                .expect("test output limits should validate"),
        );
        let persisted = output_service
            .persist(&fixture.lease, &result)
            .await
            .expect("provider output should persist through the current fence");
        assert_eq!(
            persisted.block_count(),
            2,
            "text is split into bounded blocks"
        );
        let message = AiMessageRecord::find_by_id(&fixture.database, &persisted.message_id())
            .await
            .expect("assistant message lookup should succeed")
            .expect("assistant message should exist");
        assert_eq!(message.message_role, "assistant");
        assert_eq!(message.block_count, 2);
        assert!(matches!(
            fixture
                .run_service
                .finish(
                    &fixture.lease,
                    AiRunCompletion::new(AiRunState::Completed, "stale_completion", None, None,)
                        .expect("stale completion should validate"),
                )
                .await,
            Err(AiError::Conflict)
        ));
        fixture
            .run_service
            .finish(
                persisted.lease(),
                AiRunCompletion::new(
                    AiRunState::Completed,
                    "provider_completed",
                    None,
                    result.provider_response_id().map(str::to_owned),
                )
                .expect("test completion should validate"),
            )
            .await
            .expect("caller can terminally finish after handling the result");
    }

    #[tokio::test]
    async fn completed_builtins_are_counted_but_requested_unused_tools_are_not() {
        let completed_fixture = fixture(vec![
            ProviderEvent::BuiltinToolStarted {
                call_id: "web-call-1".to_owned(),
                kind: "web_search".to_owned(),
            },
            ProviderEvent::BuiltinToolCompleted {
                call_id: "web-call-1".to_owned(),
                result: serde_json::json!({"status": "completed"}),
            },
            ProviderEvent::Usage {
                input_tokens: 3,
                output_tokens: 2,
                cached_input_tokens: 0,
            },
            ProviderEvent::ResponseCompleted {
                response_id: Some("builtin-response".to_owned()),
            },
        ])
        .await;
        let completed_executor = AiProviderCallExecutor::new(
            completed_fixture.runtime.clone(),
            completed_fixture.budget_service.clone(),
            completed_fixture.audit.clone(),
            Arc::new(TestUsageAccounting),
            Arc::new(SystemClock),
            AiProviderCallLimits::new(64, 8_192, 64 * 1_024)
                .expect("test provider limits should validate")
                .with_maximum_builtin_tool_calls(2)
                .expect("test tool limit should validate"),
        );
        let completed = completed_executor
            .execute(
                &completed_fixture.lease,
                web_search_plan(&completed_fixture, 2),
            )
            .await
            .expect("exact completed built-in pair should settle");
        assert_eq!(completed.usage().tool_units, 1);
        assert_eq!(
            reservation_state(&completed_fixture.database).await,
            "committed"
        );

        let unused_fixture = fixture(vec![
            ProviderEvent::Usage {
                input_tokens: 3,
                output_tokens: 2,
                cached_input_tokens: 0,
            },
            ProviderEvent::ResponseCompleted {
                response_id: Some("unused-builtin-response".to_owned()),
            },
        ])
        .await;
        let unused_executor = AiProviderCallExecutor::new(
            unused_fixture.runtime.clone(),
            unused_fixture.budget_service.clone(),
            unused_fixture.audit.clone(),
            Arc::new(TestUsageAccounting),
            Arc::new(SystemClock),
            AiProviderCallLimits::new(64, 8_192, 64 * 1_024)
                .expect("test provider limits should validate")
                .with_maximum_builtin_tool_calls(2)
                .expect("test tool limit should validate"),
        );
        let unused = unused_executor
            .execute(&unused_fixture.lease, web_search_plan(&unused_fixture, 2))
            .await
            .expect("unused advertised built-in should not create usage");
        assert_eq!(unused.usage().tool_units, 0);
        assert_eq!(
            reservation_state(&unused_fixture.database).await,
            "committed"
        );
    }

    #[tokio::test]
    async fn malformed_builtin_completion_keeps_the_reservation_uncertain() {
        let fixture = fixture(vec![
            ProviderEvent::BuiltinToolCompleted {
                call_id: "never-started".to_owned(),
                result: serde_json::json!({"status": "completed"}),
            },
            ProviderEvent::Usage {
                input_tokens: 1,
                output_tokens: 1,
                cached_input_tokens: 0,
            },
            ProviderEvent::ResponseCompleted {
                response_id: Some("malformed-builtin-response".to_owned()),
            },
        ])
        .await;
        let executor = AiProviderCallExecutor::new(
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            fixture.audit.clone(),
            Arc::new(TestUsageAccounting),
            Arc::new(SystemClock),
            AiProviderCallLimits::new(64, 8_192, 64 * 1_024)
                .expect("test provider limits should validate")
                .with_maximum_builtin_tool_calls(2)
                .expect("test tool limit should validate"),
        );
        assert!(matches!(
            executor
                .execute(&fixture.lease, web_search_plan(&fixture, 2))
                .await,
            Err(AiError::ProviderFailed)
        ));
        assert_eq!(reservation_state(&fixture.database).await, "uncertain");
    }

    #[tokio::test]
    async fn live_delta_persistence_failure_keeps_provider_usage_uncertain() {
        let fixture = fixture(vec![
            ProviderEvent::ResponseStarted {
                response_id: Some("live-failure-response".to_owned()),
            },
            ProviderEvent::TextDelta {
                text: "must persist before delivery".to_owned(),
            },
            ProviderEvent::Usage {
                input_tokens: 3,
                output_tokens: 2,
                cached_input_tokens: 0,
            },
            ProviderEvent::ResponseCompleted {
                response_id: Some("live-failure-response".to_owned()),
            },
        ])
        .await;
        let executor = AiProviderCallExecutor::new(
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            fixture.audit.clone(),
            Arc::new(TestUsageAccounting),
            Arc::new(SystemClock),
            AiProviderCallLimits::new(64, 8_192, 64 * 1_024)
                .expect("test provider limits should validate"),
        )
        .with_live_delta_sink(
            Arc::new(RejectLiveSink),
            AiLiveDeltaCoalescerLimits::default(),
        );

        assert!(matches!(
            executor.execute(&fixture.lease, plan(&fixture)).await,
            Err(AiError::PersistenceFailed)
        ));
        assert_eq!(reservation_state(&fixture.database).await, "uncertain");
    }

    #[tokio::test]
    async fn incomplete_provider_stream_leaves_budget_uncertain_for_recovery() {
        let fixture = fixture(vec![
            ProviderEvent::ResponseStarted {
                response_id: Some("ambiguous-response".to_owned()),
            },
            ProviderEvent::TextDelta {
                text: "partial".to_owned(),
            },
        ])
        .await;
        let executor = AiProviderCallExecutor::new(
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            fixture.audit.clone(),
            Arc::new(TestUsageAccounting),
            Arc::new(SystemClock),
            AiProviderCallLimits::new(64, 8_192, 64 * 1_024)
                .expect("test provider limits should validate"),
        );
        assert!(matches!(
            executor.execute(&fixture.lease, plan(&fixture)).await,
            Err(AiError::ProviderFailed)
        ));
        assert_eq!(fixture.mock.request_count(), 1);
        assert_eq!(reservation_state(&fixture.database).await, "uncertain");
    }

    #[tokio::test]
    async fn failed_egress_audit_prevents_transport_and_releases_capacity() {
        let fixture = fixture(vec![ProviderEvent::Unknown {
            event_type: "must_not_run".to_owned(),
        }])
        .await;
        let executor = AiProviderCallExecutor::new(
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            Arc::new(FailAudit),
            Arc::new(TestUsageAccounting),
            Arc::new(SystemClock),
            AiProviderCallLimits::new(64, 8_192, 64 * 1_024)
                .expect("test provider limits should validate"),
        );
        assert!(matches!(
            executor.execute(&fixture.lease, plan(&fixture)).await,
            Err(AiError::PersistenceFailed)
        ));
        assert_eq!(fixture.mock.request_count(), 0);
        assert_eq!(reservation_state(&fixture.database).await, "released");
    }

    #[tokio::test]
    async fn attachment_plan_without_reopener_fails_before_transport_and_releases_capacity() {
        let fixture = fixture(vec![ProviderEvent::Unknown {
            event_type: "must_not_run".to_owned(),
        }])
        .await;
        let bytes = b"exact-provider-image".to_vec();
        let executor = AiProviderCallExecutor::new(
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            fixture.audit.clone(),
            Arc::new(TestUsageAccounting),
            Arc::new(SystemClock),
            AiProviderCallLimits::new(64, 8_192, 64 * 1_024)
                .expect("test provider limits should validate"),
        );

        assert!(matches!(
            executor
                .execute(&fixture.lease, attachment_plan(&fixture, &bytes))
                .await,
            Err(AiError::RuntimeNotReady)
        ));
        assert_eq!(fixture.mock.request_count(), 0);
        assert_eq!(reservation_state(&fixture.database).await, "released");
    }

    #[tokio::test]
    async fn attachment_plan_reopens_exact_bytes_before_uncertain_transport_boundary() {
        let fixture = fixture(vec![
            ProviderEvent::Usage {
                input_tokens: 5,
                output_tokens: 1,
                cached_input_tokens: 0,
            },
            ProviderEvent::ResponseCompleted {
                response_id: Some("attachment-response".to_owned()),
            },
        ])
        .await;
        let bytes: Arc<[u8]> = Arc::from(b"exact-provider-image".as_slice());
        let resolver_calls = Arc::new(AtomicUsize::new(0));
        let executor = AiProviderCallExecutor::new(
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            fixture.audit.clone(),
            Arc::new(TestUsageAccounting),
            Arc::new(SystemClock),
            AiProviderCallLimits::new(64, 8_192, 64 * 1_024)
                .expect("test provider limits should validate"),
        )
        .with_attachment_resolver(
            Arc::new(ExactAttachmentResolver {
                bytes: bytes.clone(),
                calls: resolver_calls.clone(),
            }),
            AiProviderAttachmentResolutionLimits::default(),
        );

        let result = executor
            .execute(&fixture.lease, attachment_plan(&fixture, &bytes))
            .await
            .expect("exact attachment provider turn should complete");
        assert_eq!(result.provider_response_id(), Some("attachment-response"));
        assert_eq!(resolver_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.mock.request_count(), 1);
        assert_eq!(reservation_state(&fixture.database).await, "committed");
    }

    #[tokio::test]
    async fn registered_tool_result_is_protected_audited_fenced_and_chainable() {
        let fixture = fixture(vec![
            ProviderEvent::ResponseStarted {
                response_id: Some("tool-response-1".to_owned()),
            },
            ProviderEvent::ToolCallStarted {
                call_id: "call-1".to_owned(),
                tool_id: "records.read".to_owned(),
            },
            ProviderEvent::ToolArgumentsDelta {
                call_id: "call-1".to_owned(),
                delta: "{\"recordId\":\"54\"}".to_owned(),
            },
            ProviderEvent::ToolCallCompleted {
                call_id: "call-1".to_owned(),
                arguments: json!({"recordId": "54"}),
            },
            ProviderEvent::Usage {
                input_tokens: 18,
                output_tokens: 7,
                cached_input_tokens: 0,
            },
            ProviderEvent::ResponseCompleted {
                response_id: Some("tool-response-1".to_owned()),
            },
        ])
        .await;
        let executor = AiProviderCallExecutor::new(
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            fixture.audit.clone(),
            Arc::new(TestUsageAccounting),
            Arc::new(SystemClock),
            AiProviderCallLimits::new(64, 8_192, 64 * 1_024)
                .expect("test provider limits should validate"),
        );
        let provider_result = executor
            .execute(&fixture.lease, tool_plan(&fixture))
            .await
            .expect("registered tool request should normalize");
        assert_eq!(provider_result.tool_calls().len(), 1);

        let mut guard = AiAgentLoopGuard::new(
            &fixture.lease,
            AiAgentLoopLimits::new(4, 4).expect("test loop limits should validate"),
        );
        assert_eq!(
            guard
                .observe_provider_turn(&provider_result)
                .expect("first tool turn should bind"),
            AiAgentLoopTurn::ToolCalls {
                provider_turn_index: 0,
                call_count: 1,
            }
        );
        let service = OrmAiApplicationToolCallService::new(
            fixture.run_service.clone(),
            fixture.runtime.clone(),
            fixture.audit.clone(),
            Arc::new(SystemClock),
            AiApplicationToolCallLimits::new(
                8_192,
                16_384,
                4,
                4,
                Duration::seconds(30),
                Duration::seconds(10),
            )
            .expect("test tool limits should validate"),
        );
        let call_context = AiApplicationToolCallContext::new(
            0,
            0,
            fixture.scope.clone(),
            "tool-loop-test",
            "provider-turn-1",
        )
        .expect("test call context should validate");
        let route = AiToolResultEgressRoute::new(
            "mock-profile",
            "local-mock",
            AiDestinationTrust::Local,
            "continue_authorized_tool_result",
            "none",
            "egress-v1",
        )
        .expect("test tool-result route should validate");
        let checkpoint_service = OrmAiCoordinatorCheckpointService::new(
            fixture.run_service.clone(),
            Arc::new(Resolver(fixture.principal.clone())),
            Arc::new(AllowAccess),
            Arc::new(ProtectionPolicy),
            Arc::new(DatabaseManagedContentProtector),
            Arc::new(TestRuleResolver::default()),
            Arc::new(SystemClock),
            AiCoordinatorCheckpointLimits::new(256 * 1_024, Duration::seconds(30))
                .expect("checkpoint limits should validate"),
        );
        let (rules, provider_rule_usage) =
            test_rule_checkpoint(&fixture.scope, &[&provider_result], 0);
        let checkpointed_lease = checkpoint_service
            .persist_provider_turn(
                &fixture.lease,
                &provider_result,
                &fixture.scope,
                "tool-loop-test",
                &route,
                &rules,
                provider_rule_usage,
                guard.provider_turns(),
                guard.total_tool_calls(),
            )
            .await
            .expect("provider result should be durably checkpointed");
        let persisted = service
            .execute_read_only(
                &checkpointed_lease,
                &provider_result,
                call_context,
                route.clone(),
            )
            .await
            .expect("tool should execute through the resolver and durable fence");
        assert_eq!(persisted.state(), AiApplicationToolCallState::Completed);
        guard
            .observe_tool_result(&persisted)
            .expect("durable result should match the pending provider call");

        let record = AiToolCallRecord::find_by_id(&fixture.database, &persisted.id().0)
            .await
            .expect("tool call lookup should succeed")
            .expect("tool call should exist");
        assert_eq!(record.state, "completed");
        assert_eq!(record.provider_call_id, "call-1");
        assert_eq!(
            record.authorization_policy_version.as_deref(),
            Some("tool-policy-v1")
        );
        assert!(record.result_egress_decision_id.is_some());
        assert_eq!(
            record
                .protected_arguments
                .as_ref()
                .and_then(|value| value.get("protection"))
                .and_then(serde_json::Value::as_str),
            Some("database_managed")
        );
        assert_eq!(
            record
                .protected_result
                .as_ref()
                .and_then(|value| value.get("protection"))
                .and_then(serde_json::Value::as_str),
            Some("database_managed")
        );

        let continuation = guard
            .continuation()
            .expect("all exact tool results should permit one continuation");
        let (_, batch_rule_usage) = test_rule_checkpoint(&fixture.scope, &[&provider_result], 1);
        let batch_lease = checkpoint_service
            .persist_tool_batch(
                persisted.lease(),
                &provider_result,
                std::slice::from_ref(&persisted),
                &continuation,
                &fixture.scope,
                "tool-loop-test",
                &route,
                &rules,
                batch_rule_usage,
                guard.provider_turns(),
                guard.total_tool_calls(),
            )
            .await
            .expect("complete tool batch should be durably checkpointed");
        let run = AiRunRecord::find_by_id(&fixture.database, &batch_lease.run_id().0)
            .await
            .expect("run lookup should succeed")
            .expect("run should exist");
        let checkpoint_id = run
            .latest_checkpoint_id
            .expect("run should link the protected tool-batch checkpoint");
        let checkpoint = AiRunCheckpointRecord::find_by_id(&fixture.database, &checkpoint_id)
            .await
            .expect("checkpoint lookup should succeed")
            .expect("checkpoint should exist");
        assert_eq!(checkpoint.checkpoint_kind, "tool_batch_persisted");
        assert!(checkpoint.protected_state.is_some());
        let adopted = checkpoint_service
            .adopt_tool_batch(&batch_lease)
            .await
            .expect("current authority should validate the exact tool batch")
            .expect("linked tool batch should be adoptable");
        assert_eq!(adopted.checkpoint_id(), checkpoint_id);
        assert_eq!(adopted.provider_turns(), 1);
        assert_eq!(adopted.total_tool_calls(), 1);
        assert_eq!(adopted.scope(), &fixture.scope);
        let batch_lease = checkpoint_service
            .consume_before_provider(&batch_lease, adopted.checkpoint_id())
            .await
            .expect("validated checkpoint should be consumed once");
        assert_eq!(batch_lease.latest_checkpoint_id(), None);
        assert!(matches!(
            checkpoint_service
                .consume_before_provider(&batch_lease, checkpoint_id)
                .await,
            Err(AiError::Conflict)
        ));
        let descriptor = fixture
            .runtime
            .tool_catalog()
            .descriptor(&AiToolId::parse("records.read").expect("tool ID should parse"))
            .expect("tool should remain registered");
        let mut next_request = ModelRequest {
            model: "mock-model".to_owned(),
            instructions: vec!["Continue after the tool result".to_owned()],
            input: Vec::new(),
            continuation: None,
            continuation_mode: crate::ModelContinuationMode::ProviderRetained,
            tools: vec![ModelToolDefinition {
                tool_id: descriptor.id.as_str().to_owned(),
                provider_name: "records_read".to_owned(),
                fingerprint: descriptor.fingerprint.clone(),
                description: descriptor.description.clone(),
                parameters: descriptor.argument_schema.clone(),
                strict: true,
            }],
            builtin_tools: Vec::new(),
            maximum_builtin_tool_calls: None,
            output_schema: None,
            maximum_output_tokens: Some(100),
        };
        let mut policy = AiToolPolicySet::new(ToolMaturity::ReadOnly);
        policy.bind(AiToolPolicyBinding {
            tool_id: descriptor.id.clone(),
            fingerprint: descriptor.fingerprint.clone(),
            enabled: true,
        });
        let forged_base = plan(&fixture);
        let forged_request = ModelRequest {
            continuation: Some(ModelContinuation::ProviderResponse {
                response_id: "tool-response-1".to_owned(),
            }),
            continuation_mode: crate::ModelContinuationMode::ProviderRetained,
            input: vec![ModelInputBlock::ToolResult {
                call_id: "call-1".to_owned(),
                tool_id: descriptor.id.as_str().to_owned(),
                output: json!({"record": "forged"}),
            }],
            ..next_request.clone()
        };
        assert!(matches!(
            AiProviderCallPlan::new_with_tools(
                forged_base.provider_kind,
                forged_request,
                forged_base.budget,
                forged_base.transfers,
                "forged-continuation",
                fixture.runtime.tool_catalog(),
                &policy,
            ),
            Err(AiError::InvalidInput(_))
        ));
        let base = plan(&fixture);
        let next_plan = AiProviderCallPlan::new_continuation_with_tools(
            base.provider_kind,
            next_request,
            base.budget,
            base.transfers,
            "tool-loop-continuation",
            continuation,
            fixture.runtime.tool_catalog(),
            &policy,
        )
        .expect("continuation should bind request and result manifests");
        next_request = next_plan.request;
        assert!(matches!(
            next_request.continuation,
            Some(ModelContinuation::ProviderResponse { ref response_id })
                if response_id == "tool-response-1"
        ));
        assert!(matches!(
            next_request.input.as_slice(),
            [ModelInputBlock::ToolResult { call_id, tool_id, .. }]
                if call_id == "call-1" && tool_id == "records.read"
        ));
        next_request
            .validate()
            .expect("exact continuation request should validate");
        assert_eq!(
            next_plan
                .transfers
                .iter()
                .filter(|manifest| manifest.capability == AiEgressCapability::ToolResult)
                .count(),
            1
        );

        assert!(matches!(
            fixture.run_service.heartbeat(&fixture.lease).await,
            Err(AiError::Conflict)
        ));
        fixture
            .run_service
            .finish(
                &batch_lease,
                AiRunCompletion::new(AiRunState::Completed, "tool_loop_test", None, None)
                    .expect("test completion should validate"),
            )
            .await
            .expect("renewed tool fence should complete the run");
    }

    #[tokio::test]
    async fn stateless_tool_batch_is_checkpointed_and_consumed_without_response_id() {
        let fixture = fixture(vec![
            ProviderEvent::ResponseStarted { response_id: None },
            ProviderEvent::ToolCallStarted {
                call_id: "stateless-call-1".to_owned(),
                tool_id: "records.read".to_owned(),
            },
            ProviderEvent::ToolCallCompleted {
                call_id: "stateless-call-1".to_owned(),
                arguments: json!({"recordId": "54"}),
            },
            ProviderEvent::Usage {
                input_tokens: 18,
                output_tokens: 5,
                cached_input_tokens: 0,
            },
            ProviderEvent::ResponseCompleted { response_id: None },
        ])
        .await;
        let provider_executor = AiProviderCallExecutor::new(
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            fixture.audit.clone(),
            Arc::new(TestUsageAccounting),
            Arc::new(SystemClock),
            AiProviderCallLimits::new(64, 8_192, 64 * 1_024)
                .expect("test provider limits should validate"),
        );
        let result = provider_executor
            .execute(&fixture.lease, stateless_tool_plan(&fixture))
            .await
            .expect("stateless tool turn should normalize");
        assert_eq!(result.provider_response_id(), None);
        assert!(result.uses_stateless_continuation());

        let mut guard = AiAgentLoopGuard::new(
            &fixture.lease,
            AiAgentLoopLimits::new(4, 4).expect("test loop limits should validate"),
        );
        assert_eq!(
            guard
                .observe_provider_turn(&result)
                .expect("stateless turn should bind"),
            AiAgentLoopTurn::ToolCalls {
                provider_turn_index: 0,
                call_count: 1,
            }
        );
        let route = AiToolResultEgressRoute::new(
            "mock-profile",
            "local-mock",
            AiDestinationTrust::Local,
            "continue_authorized_tool_result",
            "none",
            "egress-v1",
        )
        .expect("tool-result route should validate");
        let rule_resolver = Arc::new(TestRuleResolver::default());
        let checkpoint_service = OrmAiCoordinatorCheckpointService::new(
            fixture.run_service.clone(),
            Arc::new(Resolver(fixture.principal.clone())),
            Arc::new(AllowAccess),
            Arc::new(ProtectionPolicy),
            Arc::new(DatabaseManagedContentProtector),
            rule_resolver.clone(),
            Arc::new(SystemClock),
            AiCoordinatorCheckpointLimits::new(256 * 1_024, Duration::seconds(30))
                .expect("checkpoint limits should validate"),
        );
        let (rules, provider_rule_usage) = test_rule_checkpoint(&fixture.scope, &[&result], 0);
        let lease = checkpoint_service
            .persist_provider_turn(
                &fixture.lease,
                &result,
                &fixture.scope,
                "stateless-tool-loop",
                &route,
                &rules,
                provider_rule_usage,
                guard.provider_turns(),
                guard.total_tool_calls(),
            )
            .await
            .expect("stateless provider turn should checkpoint");
        let tool_service = OrmAiApplicationToolCallService::new(
            fixture.run_service.clone(),
            fixture.runtime.clone(),
            fixture.audit.clone(),
            Arc::new(SystemClock),
            AiApplicationToolCallLimits::new(
                8_192,
                16_384,
                4,
                4,
                Duration::seconds(30),
                Duration::seconds(10),
            )
            .expect("test tool limits should validate"),
        );
        let persisted = tool_service
            .execute_read_only(
                &lease,
                &result,
                AiApplicationToolCallContext::new(
                    0,
                    0,
                    fixture.scope.clone(),
                    "stateless-tool-loop",
                    "stateless-provider-turn-1",
                )
                .expect("tool context should validate"),
                route.clone(),
            )
            .await
            .expect("stateless tool should execute through the resolver");
        guard
            .observe_tool_result(&persisted)
            .expect("tool output should match the stateless turn");
        let continuation = guard
            .continuation()
            .expect("complete stateless batch should continue");
        assert_eq!(
            continuation.checkpoint_value()["continuation"]["type"],
            "stateless_conversation"
        );
        let (_, batch_rule_usage) = test_rule_checkpoint(&fixture.scope, &[&result], 1);
        let lease = checkpoint_service
            .persist_tool_batch(
                persisted.lease(),
                &result,
                std::slice::from_ref(&persisted),
                &continuation,
                &fixture.scope,
                "stateless-tool-loop",
                &route,
                &rules,
                batch_rule_usage,
                guard.provider_turns(),
                guard.total_tool_calls(),
            )
            .await
            .expect("stateless tool batch should checkpoint");
        let checkpoint_id = lease
            .latest_checkpoint_id()
            .expect("stateless checkpoint should be linked");
        let checkpoint = AiRunCheckpointRecord::find_by_id(&fixture.database, &checkpoint_id)
            .await
            .expect("checkpoint lookup should succeed")
            .expect("checkpoint should exist");
        assert_eq!(checkpoint.provider_response_id, None);
        rule_resolver.fingerprint_version.store(1, Ordering::SeqCst);
        assert!(matches!(
            checkpoint_service.adopt_tool_batch(&lease).await,
            Err(AiError::ReauthorizationFailed)
        ));
        rule_resolver.fingerprint_version.store(0, Ordering::SeqCst);
        let adopted = checkpoint_service
            .adopt_tool_batch(&lease)
            .await
            .expect("stateless batch should validate for adoption")
            .expect("linked stateless batch should be adoptable");
        assert_eq!(adopted.provider_turns(), 1);
        assert_eq!(adopted.total_tool_calls(), 1);
        let consumed = checkpoint_service
            .consume_before_provider(&lease, checkpoint_id)
            .await
            .expect("same generation should consume the stateless checkpoint");
        assert_eq!(consumed.latest_checkpoint_id(), None);

        let descriptor = fixture
            .runtime
            .tool_catalog()
            .descriptor(&AiToolId::parse("records.read").expect("tool ID should parse"))
            .expect("tool should remain registered");
        let mut policy = AiToolPolicySet::new(ToolMaturity::ReadOnly);
        policy.bind(AiToolPolicyBinding {
            tool_id: descriptor.id.clone(),
            fingerprint: descriptor.fingerprint.clone(),
            enabled: true,
        });
        let base = plan(&fixture);
        let next = AiProviderCallPlan::new_continuation_with_tools(
            base.provider_kind,
            ModelRequest {
                model: "mock-model".to_owned(),
                instructions: Vec::new(),
                input: Vec::new(),
                continuation: None,
                continuation_mode: ModelContinuationMode::StatelessReplay,
                tools: vec![ModelToolDefinition {
                    tool_id: descriptor.id.as_str().to_owned(),
                    provider_name: "records_read".to_owned(),
                    fingerprint: descriptor.fingerprint.clone(),
                    description: descriptor.description.clone(),
                    parameters: descriptor.argument_schema.clone(),
                    strict: true,
                }],
                builtin_tools: Vec::new(),
                maximum_builtin_tool_calls: None,
                output_schema: None,
                maximum_output_tokens: Some(100),
            },
            base.budget,
            base.transfers,
            "stateless-continuation",
            continuation,
            fixture.runtime.tool_catalog(),
            &policy,
        )
        .expect("stateless continuation plan should remain exact");
        assert!(matches!(
            next.request.continuation,
            Some(ModelContinuation::StatelessConversation { .. })
        ));
        assert_eq!(
            next.transfers
                .iter()
                .filter(|manifest| manifest.capability == AiEgressCapability::ToolResult)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn stateless_checkpoint_adoption_validates_every_historical_tool_turn() {
        let fixture = fixture_with_event_batches([
            vec![
                ProviderEvent::ResponseStarted { response_id: None },
                ProviderEvent::ToolCallStarted {
                    call_id: "stateless-history-call-1".to_owned(),
                    tool_id: "records.read".to_owned(),
                },
                ProviderEvent::ToolCallCompleted {
                    call_id: "stateless-history-call-1".to_owned(),
                    arguments: json!({"recordId": "54"}),
                },
                ProviderEvent::Usage {
                    input_tokens: 18,
                    output_tokens: 5,
                    cached_input_tokens: 0,
                },
                ProviderEvent::ResponseCompleted { response_id: None },
            ],
            vec![
                ProviderEvent::ResponseStarted { response_id: None },
                ProviderEvent::ToolCallStarted {
                    call_id: "stateless-history-call-2".to_owned(),
                    tool_id: "records.read".to_owned(),
                },
                ProviderEvent::ToolCallCompleted {
                    call_id: "stateless-history-call-2".to_owned(),
                    arguments: json!({"recordId": "55"}),
                },
                ProviderEvent::Usage {
                    input_tokens: 28,
                    output_tokens: 6,
                    cached_input_tokens: 0,
                },
                ProviderEvent::ResponseCompleted { response_id: None },
            ],
        ])
        .await;
        let provider_executor = AiProviderCallExecutor::new(
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            fixture.audit.clone(),
            Arc::new(TestUsageAccounting),
            Arc::new(SystemClock),
            AiProviderCallLimits::new(64, 8_192, 64 * 1_024)
                .expect("test provider limits should validate"),
        );
        let tool_service = OrmAiApplicationToolCallService::new(
            fixture.run_service.clone(),
            fixture.runtime.clone(),
            fixture.audit.clone(),
            Arc::new(SystemClock),
            AiApplicationToolCallLimits::new(
                8_192,
                16_384,
                4,
                4,
                Duration::seconds(30),
                Duration::seconds(10),
            )
            .expect("test tool limits should validate"),
        );
        let route = AiToolResultEgressRoute::new(
            "mock-profile",
            "local-mock",
            AiDestinationTrust::Local,
            "continue_authorized_tool_result",
            "none",
            "egress-v1",
        )
        .expect("tool-result route should validate");
        let checkpoint_service = OrmAiCoordinatorCheckpointService::new(
            fixture.run_service.clone(),
            Arc::new(Resolver(fixture.principal.clone())),
            Arc::new(AllowAccess),
            Arc::new(ProtectionPolicy),
            Arc::new(DatabaseManagedContentProtector),
            Arc::new(TestRuleResolver::default()),
            Arc::new(SystemClock),
            AiCoordinatorCheckpointLimits::new(256 * 1_024, Duration::seconds(30))
                .expect("checkpoint limits should validate"),
        );
        let mut guard = AiAgentLoopGuard::new(
            &fixture.lease,
            AiAgentLoopLimits::new(4, 4).expect("test loop limits should validate"),
        );

        let first_result = provider_executor
            .execute(&fixture.lease, stateless_tool_plan(&fixture))
            .await
            .expect("first stateless provider turn should normalize");
        assert_eq!(
            guard
                .observe_provider_turn(&first_result)
                .expect("first stateless tool turn should bind"),
            AiAgentLoopTurn::ToolCalls {
                provider_turn_index: 0,
                call_count: 1,
            }
        );
        let (rules, first_provider_rule_usage) =
            test_rule_checkpoint(&fixture.scope, &[&first_result], 0);
        let first_lease = checkpoint_service
            .persist_provider_turn(
                &fixture.lease,
                &first_result,
                &fixture.scope,
                "stateless-history",
                &route,
                &rules,
                first_provider_rule_usage,
                guard.provider_turns(),
                guard.total_tool_calls(),
            )
            .await
            .expect("first stateless provider turn should checkpoint");
        let first_tool = tool_service
            .execute_read_only(
                &first_lease,
                &first_result,
                AiApplicationToolCallContext::new(
                    0,
                    0,
                    fixture.scope.clone(),
                    "stateless-history",
                    "stateless-history-provider-turn-1",
                )
                .expect("first tool context should validate"),
                route.clone(),
            )
            .await
            .expect("first historical tool should execute through the resolver");
        guard
            .observe_tool_result(&first_tool)
            .expect("first historical output should match");
        let first_continuation = guard
            .continuation()
            .expect("first stateless batch should continue");
        let (_, first_batch_rule_usage) = test_rule_checkpoint(&fixture.scope, &[&first_result], 1);
        let first_batch_lease = checkpoint_service
            .persist_tool_batch(
                first_tool.lease(),
                &first_result,
                std::slice::from_ref(&first_tool),
                &first_continuation,
                &fixture.scope,
                "stateless-history",
                &route,
                &rules,
                first_batch_rule_usage,
                guard.provider_turns(),
                guard.total_tool_calls(),
            )
            .await
            .expect("first stateless batch should checkpoint");
        let first_checkpoint_id = first_batch_lease
            .latest_checkpoint_id()
            .expect("first stateless checkpoint should link");
        let first_consumed_lease = checkpoint_service
            .consume_before_provider(&first_batch_lease, first_checkpoint_id)
            .await
            .expect("first stateless batch should consume exactly once");

        let descriptor = fixture
            .runtime
            .tool_catalog()
            .descriptor(&AiToolId::parse("records.read").expect("tool ID should parse"))
            .expect("tool should remain registered");
        let mut policy = AiToolPolicySet::new(ToolMaturity::ReadOnly);
        policy.bind(AiToolPolicyBinding {
            tool_id: descriptor.id.clone(),
            fingerprint: descriptor.fingerprint.clone(),
            enabled: true,
        });
        let next_request = ModelRequest {
            model: "mock-model".to_owned(),
            instructions: Vec::new(),
            input: Vec::new(),
            continuation: None,
            continuation_mode: ModelContinuationMode::StatelessReplay,
            tools: vec![ModelToolDefinition {
                tool_id: descriptor.id.as_str().to_owned(),
                provider_name: "records_read".to_owned(),
                fingerprint: descriptor.fingerprint.clone(),
                description: descriptor.description.clone(),
                parameters: descriptor.argument_schema.clone(),
                strict: true,
            }],
            builtin_tools: Vec::new(),
            maximum_builtin_tool_calls: None,
            output_schema: None,
            maximum_output_tokens: Some(100),
        };
        let mut next_base = plan(&fixture);
        next_base.budget.idempotency_key = format!("provider:{}:2", fixture.lease.attempt_id());
        let mut second_plan = AiProviderCallPlan::new_continuation_with_tools(
            next_base.provider_kind,
            next_request,
            next_base.budget,
            next_base.transfers,
            "stateless-history-provider-turn-2",
            first_continuation,
            fixture.runtime.tool_catalog(),
            &policy,
        )
        .expect("second stateless plan should bind historical output");
        second_plan.transfers[0].estimated_bytes = second_plan.request.conservative_egress_bytes();
        let second_result = provider_executor
            .execute(&first_consumed_lease, second_plan)
            .await
            .expect("second stateless provider turn should reauthorize replay");
        assert_eq!(
            guard
                .observe_provider_turn(&second_result)
                .expect("second stateless tool turn should bind"),
            AiAgentLoopTurn::ToolCalls {
                provider_turn_index: 1,
                call_count: 1,
            }
        );
        let (_, second_provider_rule_usage) =
            test_rule_checkpoint(&fixture.scope, &[&first_result, &second_result], 1);
        let second_lease = checkpoint_service
            .persist_provider_turn(
                &first_consumed_lease,
                &second_result,
                &fixture.scope,
                "stateless-history",
                &route,
                &rules,
                second_provider_rule_usage,
                guard.provider_turns(),
                guard.total_tool_calls(),
            )
            .await
            .expect("second stateless provider turn should checkpoint");
        let second_tool = tool_service
            .execute_read_only(
                &second_lease,
                &second_result,
                AiApplicationToolCallContext::new(
                    1,
                    0,
                    fixture.scope.clone(),
                    "stateless-history",
                    "stateless-history-provider-turn-2",
                )
                .expect("second tool context should validate"),
                route.clone(),
            )
            .await
            .expect("second historical tool should execute through the resolver");
        guard
            .observe_tool_result(&second_tool)
            .expect("second historical output should match");
        let second_continuation = guard
            .continuation()
            .expect("second stateless batch should continue");
        assert_eq!(second_continuation.replay_transfers().len(), 1);
        let (_, second_batch_rule_usage) =
            test_rule_checkpoint(&fixture.scope, &[&first_result, &second_result], 2);
        let second_batch_lease = checkpoint_service
            .persist_tool_batch(
                second_tool.lease(),
                &second_result,
                std::slice::from_ref(&second_tool),
                &second_continuation,
                &fixture.scope,
                "stateless-history",
                &route,
                &rules,
                second_batch_rule_usage,
                guard.provider_turns(),
                guard.total_tool_calls(),
            )
            .await
            .expect("second stateless batch should checkpoint full history");
        let adopted = checkpoint_service
            .adopt_tool_batch(&second_batch_lease)
            .await
            .expect("every protected historical binding should validate")
            .expect("complete stateless history should be adoptable");
        assert_eq!(adopted.provider_turns(), 2);
        assert_eq!(adopted.total_tool_calls(), 2);
        fixture
            .database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    let run = tx
                        .find_by_id::<AiRunRecord>(&second_batch_lease.run_id().0)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    let outcome = tx
                        .compare_and_swap::<AiRunRecord>(
                            &run.id,
                            run.row_version,
                            AiRunRecordWhereInput::default(),
                            UpdateAiRunRecordInput {
                                lease_expires_at: Some(Some(0)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(outcome, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    Ok(())
                })
            })
            .await
            .expect("generated ORM should expire the recovery fixture");
        let recovery = fixture
            .run_service
            .recover_expired_leases()
            .await
            .expect("stateless checkpoint should reconcile");
        assert_eq!(recovery.checkpoint_requeued, 1);
        assert_eq!(recovery.recovery_required, 0);
        let replacement = fixture
            .run_service
            .claim_next("stateless-history-adopter")
            .await
            .expect("replacement claim should succeed")
            .expect("stateless checkpoint should be immediately eligible");
        assert_eq!(replacement.lease_generation(), 2);
        let restored = checkpoint_service
            .adopt_tool_batch(&replacement)
            .await
            .expect("new fence should revalidate all stateless history")
            .expect("restored stateless checkpoint should be adoptable");
        assert_eq!(restored.provider_turns(), 2);
        assert_eq!(restored.total_tool_calls(), 2);
        fixture
            .database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    let call = tx
                        .find_by_id::<AiToolCallRecord>(&first_tool.id().0)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    let outcome = tx
                        .compare_and_swap::<AiToolCallRecord>(
                            &call.id,
                            call.row_version,
                            AiToolCallRecordWhereInput::default(),
                            UpdateAiToolCallRecordInput {
                                result_egress_manifest_hash: Some(Some("0".repeat(64))),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(outcome, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    Ok(())
                })
            })
            .await
            .expect("generated ORM should mutate the adversarial fixture");
        assert!(matches!(
            checkpoint_service.adopt_tool_batch(&replacement).await,
            Err(AiError::Conflict)
        ));
        assert_eq!(fixture.mock.request_count(), 2);
    }

    #[tokio::test]
    async fn supervised_mutation_requires_preview_approval_and_fresh_resolver_authorization() {
        let fixture = fixture(vec![
            ProviderEvent::ResponseStarted {
                response_id: Some("supervised-response-1".to_owned()),
            },
            ProviderEvent::ToolCallStarted {
                call_id: "supervised-call-1".to_owned(),
                tool_id: "records.update".to_owned(),
            },
            ProviderEvent::ToolArgumentsDelta {
                call_id: "supervised-call-1".to_owned(),
                delta: "{\"recordId\":\"54\"}".to_owned(),
            },
            ProviderEvent::ToolCallCompleted {
                call_id: "supervised-call-1".to_owned(),
                arguments: json!({"recordId": "54"}),
            },
            ProviderEvent::Usage {
                input_tokens: 20,
                output_tokens: 8,
                cached_input_tokens: 0,
            },
            ProviderEvent::ResponseCompleted {
                response_id: Some("supervised-response-1".to_owned()),
            },
        ])
        .await;
        let provider_executor = AiProviderCallExecutor::new(
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            fixture.audit.clone(),
            Arc::new(TestUsageAccounting),
            Arc::new(SystemClock),
            AiProviderCallLimits::new(64, 8_192, 64 * 1_024)
                .expect("test provider limits should validate"),
        );
        let provider_plan = supervised_tool_plan(&fixture);
        let rule_resolution = AiAgentRuleResolution::new(
            test_rules(fixture.scope.clone()),
            OffsetDateTime::now_utc(),
        )
        .expect("supervised rules should resolve");
        assert!(matches!(
            plan(&fixture).project_supervised_rule_usage(
                &rule_resolution,
                AiRuleRunUsage::default(),
                false,
            ),
            Err(AiError::Forbidden)
        ));
        assert!(matches!(
            provider_plan.project_rule_usage(&rule_resolution, AiRuleRunUsage::default(), false,),
            Err(AiError::Forbidden)
        ));
        provider_plan
            .project_supervised_rule_usage(&rule_resolution, AiRuleRunUsage::default(), false)
            .expect("supervised plan should fit exact current rules");
        let provider_result = provider_executor
            .execute(&fixture.lease, provider_plan)
            .await
            .expect("supervised provider call should normalize");
        let approval_service = OrmAiApprovalService::new(
            fixture.database.clone(),
            fixture.run_service.clone(),
            Arc::new(Resolver(fixture.principal.clone())),
            Arc::new(AllowApprovals),
            Arc::new(fixture.runtime.tool_catalog().clone()),
            agql_auth::RecentMfaPolicy {
                maximum_age: Duration::minutes(5),
                clock_skew: Duration::seconds(30),
                allowed_amr: Vec::new(),
                allowed_acr: Vec::new(),
                match_mode: agql_auth::AssuranceMatchMode::All,
            },
            Arc::new(ProtectionPolicy),
            Arc::new(DatabaseManagedContentProtector),
            Arc::new(SystemClock),
        );
        let service = OrmAiConsequentialToolCallService::new(
            fixture.run_service.clone(),
            fixture.runtime.clone(),
            approval_service.clone(),
            Arc::new(PreviewBuilder),
            fixture.audit.clone(),
            Arc::new(SystemClock),
            AiApplicationToolCallLimits::new(
                8_192,
                16_384,
                4,
                4,
                Duration::seconds(30),
                Duration::seconds(10),
            )
            .expect("test tool limits should validate"),
        );
        let context = AiApplicationToolCallContext::new(
            0,
            0,
            fixture.scope.clone(),
            "supervised-tool-test",
            provider_result.budget_reservation_id().0.to_string(),
        )
        .expect("supervised context should validate");
        let supervised_descriptor = fixture
            .runtime
            .tool_catalog()
            .descriptor(&AiToolId::parse("records.update").expect("tool ID should parse"))
            .expect("supervised descriptor should exist");
        let supervised_contract = supervised_descriptor
            .graphql_contract
            .clone()
            .expect("supervised descriptor should bind GraphQL");
        let direct_request = ToolGraphqlRequest {
            document: supervised_descriptor.document.clone(),
            operation_name: supervised_contract.operation_name.clone(),
            contract: supervised_contract,
            variables: json!({"recordId": "54"}),
            invocation: GraphqlInvocationContext {
                run_id: fixture.lease.run_id(),
                tool_call_id: AiToolCallId::new(),
                scope: fixture.scope.clone(),
                correlation_id: "approval-bypass-test".to_owned(),
                causation_id: "approval-bypass-test".to_owned(),
                delegation_reference: None,
                idempotency_key: None,
            },
        };
        assert!(matches!(
            fixture
                .runtime
                .execute_tool(
                    fixture.lease.principal_reference(),
                    &supervised_descriptor.id,
                    direct_request,
                )
                .await,
            Err(AiError::Forbidden)
        ));
        let requested = service
            .request_approval(
                &fixture.lease,
                &provider_result,
                context,
                OffsetDateTime::now_utc() + Duration::minutes(5),
                false,
            )
            .await
            .expect("supervised call should park for approval");
        let pending = AiApprovalRecord::find_by_id(&fixture.database, &requested.approval_id().0)
            .await
            .expect("approval lookup should succeed")
            .expect("approval should exist");
        assert_eq!(pending.state, "pending");
        let decided = approval_service
            .decide_approval(
                &fixture.principal,
                DecideAiApprovalInput {
                    id: requested.approval_id().0,
                    decision: AiApprovalDecision::Approve,
                    expected_version: pending.row_version,
                },
            )
            .await
            .expect("human approval should persist");
        assert_eq!(decided.state, "approved");
        fixture.tool_policy_version.store(2, Ordering::SeqCst);
        let stale_policy_route = AiToolResultEgressRoute::new(
            "mock-profile",
            "local-mock",
            AiDestinationTrust::Local,
            "continue_supervised_tool_result",
            "none",
            "egress-v1",
        )
        .expect("stale-policy route should validate");
        assert!(matches!(
            service
                .execute_approved(
                    requested.lease(),
                    requested.approval_id(),
                    requested.tool_call_id(),
                    stale_policy_route,
                )
                .await,
            Err(AiError::Forbidden)
        ));
        let still_approved =
            AiApprovalRecord::find_by_id(&fixture.database, &requested.approval_id().0)
                .await
                .expect("approval lookup should succeed")
                .expect("approval should remain present");
        assert_eq!(still_approved.state, "approved");
        fixture.tool_policy_version.store(1, Ordering::SeqCst);
        let route = AiToolResultEgressRoute::new(
            "mock-profile",
            "local-mock",
            AiDestinationTrust::Local,
            "continue_supervised_tool_result",
            "none",
            "egress-v1",
        )
        .expect("supervised result route should validate");
        let outcome = service
            .execute_approved(
                requested.lease(),
                requested.approval_id(),
                requested.tool_call_id(),
                route,
            )
            .await
            .expect("approved mutation should execute and persist");
        let persisted = outcome
            .persisted()
            .expect("unambiguous resolver result should persist");
        assert_eq!(persisted.state(), AiApplicationToolCallState::Completed);
        assert!(persisted.model_input().is_some());
        let call = AiToolCallRecord::find_by_id(&fixture.database, &requested.tool_call_id().0)
            .await
            .expect("tool call lookup should succeed")
            .expect("tool call should exist");
        assert_eq!(call.state, "completed");
        assert_eq!(
            call.authorization_policy_version.as_deref(),
            Some("tool-policy-v1")
        );
        assert_eq!(
            call.provider_kind.as_deref(),
            Some(ProviderKind::OpenAiCompatible.as_str())
        );
        assert_eq!(call.provider_model.as_deref(), Some("mock-model"));
        assert_eq!(call.correlation_id.as_deref(), Some("supervised-tool-test"));
        let consumed = AiApprovalRecord::find_by_id(&fixture.database, &requested.approval_id().0)
            .await
            .expect("approval lookup should succeed")
            .expect("approval should remain auditable");
        assert_eq!(consumed.state, "consumed");
        assert_eq!(consumed.consumed_uses, 1);
        let replay_route = AiToolResultEgressRoute::new(
            "mock-profile",
            "local-mock",
            AiDestinationTrust::Local,
            "continue_supervised_tool_result",
            "none",
            "egress-v1",
        )
        .expect("replay route should validate");
        assert!(matches!(
            service
                .execute_approved(
                    requested.lease(),
                    requested.approval_id(),
                    requested.tool_call_id(),
                    replay_route,
                )
                .await,
            Err(AiError::Conflict)
        ));

        fixture
            .run_service
            .finish(
                persisted.lease(),
                AiRunCompletion::new(AiRunState::Completed, "supervised_test", None, None)
                    .expect("test completion should validate"),
            )
            .await
            .expect("renewed supervised fence should complete the run");
    }

    #[tokio::test]
    async fn approved_wait_reopens_exact_provider_turn_and_checkpoints_mutation_continuation() {
        let fixture = fixture(vec![
            ProviderEvent::ResponseStarted {
                response_id: Some("supervised-resume-response".to_owned()),
            },
            ProviderEvent::ToolCallStarted {
                call_id: "supervised-resume-call".to_owned(),
                tool_id: "records.update".to_owned(),
            },
            ProviderEvent::ToolArgumentsDelta {
                call_id: "supervised-resume-call".to_owned(),
                delta: "{\"recordId\":\"56\"}".to_owned(),
            },
            ProviderEvent::ToolCallCompleted {
                call_id: "supervised-resume-call".to_owned(),
                arguments: json!({"recordId": "56"}),
            },
            ProviderEvent::Usage {
                input_tokens: 20,
                output_tokens: 8,
                cached_input_tokens: 0,
            },
            ProviderEvent::ResponseCompleted {
                response_id: Some("supervised-resume-response".to_owned()),
            },
        ])
        .await;
        let provider_executor = AiProviderCallExecutor::new(
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            fixture.audit.clone(),
            Arc::new(TestUsageAccounting),
            Arc::new(SystemClock),
            AiProviderCallLimits::new(64, 8_192, 64 * 1_024)
                .expect("test provider limits should validate"),
        );
        let provider_result = provider_executor
            .execute(&fixture.lease, supervised_tool_plan(&fixture))
            .await
            .expect("supervised provider call should normalize");
        let route = AiToolResultEgressRoute::new(
            "mock-profile",
            "local-mock",
            AiDestinationTrust::Local,
            "continue_supervised_tool_result",
            "none",
            "egress-v1",
        )
        .expect("supervised result route should validate");
        let rule_resolver = Arc::new(TestRuleResolver::default());
        let checkpoint_service = Arc::new(OrmAiCoordinatorCheckpointService::new(
            fixture.run_service.clone(),
            Arc::new(Resolver(fixture.principal.clone())),
            Arc::new(AllowAccess),
            Arc::new(ProtectionPolicy),
            Arc::new(DatabaseManagedContentProtector),
            rule_resolver,
            Arc::new(SystemClock),
            AiCoordinatorCheckpointLimits::new(256 * 1024, Duration::minutes(5))
                .expect("checkpoint limits should validate"),
        ));
        let mut guard = AiAgentLoopGuard::new(
            &fixture.lease,
            AiAgentLoopLimits::new(4, 4).expect("loop limits should validate"),
        );
        assert!(matches!(
            guard
                .observe_provider_turn(&provider_result)
                .expect("provider turn should bind"),
            AiAgentLoopTurn::ToolCalls { call_count: 1, .. }
        ));
        let (rules, usage) = test_rule_checkpoint(&fixture.scope, &[&provider_result], 0);
        let checkpointed = checkpoint_service
            .persist_provider_turn(
                &fixture.lease,
                &provider_result,
                &fixture.scope,
                "supervised-resume-test",
                &route,
                &rules,
                usage,
                guard.provider_turns(),
                guard.total_tool_calls(),
            )
            .await
            .expect("provider turn should be protected before approval parking");
        let approval_service = OrmAiApprovalService::new(
            fixture.database.clone(),
            fixture.run_service.clone(),
            Arc::new(Resolver(fixture.principal.clone())),
            Arc::new(AllowApprovals),
            Arc::new(fixture.runtime.tool_catalog().clone()),
            agql_auth::RecentMfaPolicy {
                maximum_age: Duration::minutes(5),
                clock_skew: Duration::seconds(30),
                allowed_amr: Vec::new(),
                allowed_acr: Vec::new(),
                match_mode: agql_auth::AssuranceMatchMode::All,
            },
            Arc::new(ProtectionPolicy),
            Arc::new(DatabaseManagedContentProtector),
            Arc::new(SystemClock),
        );
        let consequential = Arc::new(OrmAiConsequentialToolCallService::new(
            fixture.run_service.clone(),
            fixture.runtime.clone(),
            approval_service.clone(),
            Arc::new(PreviewBuilder),
            fixture.audit.clone(),
            Arc::new(SystemClock),
            AiApplicationToolCallLimits::new(
                8_192,
                16_384,
                4,
                4,
                Duration::seconds(30),
                Duration::seconds(10),
            )
            .expect("test tool limits should validate"),
        ));
        let context = AiApplicationToolCallContext::new(
            0,
            0,
            fixture.scope.clone(),
            "supervised-resume-test",
            provider_result.budget_reservation_id().0.to_string(),
        )
        .expect("supervised context should validate");
        let requested = consequential
            .request_approval(
                &checkpointed,
                &provider_result,
                context,
                OffsetDateTime::now_utc() + Duration::minutes(5),
                false,
            )
            .await
            .expect("supervised call should park for approval");
        let pending = AiApprovalRecord::find_by_id(&fixture.database, &requested.approval_id().0)
            .await
            .expect("approval lookup should succeed")
            .expect("approval should exist");
        approval_service
            .decide_approval(
                &fixture.principal,
                DecideAiApprovalInput {
                    id: requested.approval_id().0,
                    decision: AiApprovalDecision::Approve,
                    expected_version: pending.row_version,
                },
            )
            .await
            .expect("human approval should persist");
        let claimed = fixture
            .run_service
            .claim_next_approved("supervised-resume-worker")
            .await
            .expect("approved-wait claim should stay safe")
            .expect("approved wait should be claimable");
        let resume = OrmAiSupervisedResumeService::new(
            fixture.run_service.clone(),
            checkpoint_service.clone(),
            consequential,
        );
        let outcome = resume
            .execute_claimed(&claimed)
            .await
            .expect("approved mutation and continuation should complete");
        let protected = outcome
            .checkpointed()
            .expect("continuation should be durably protected");
        assert_eq!(outcome.tool_call_id(), requested.tool_call_id());
        assert_eq!(protected.provider_turns(), 1);
        assert_eq!(protected.total_tool_calls(), 1);
        assert_eq!(protected.rule_usage().provider_calls(), 1);
        assert_eq!(protected.rule_usage().steps(), 2);
        assert_eq!(protected.lease().state(), AiRunState::Running);
        assert_eq!(fixture.mock.request_count(), 1);
        let checkpoint =
            AiRunCheckpointRecord::find_by_id(&fixture.database, &protected.checkpoint_id())
                .await
                .expect("supervised checkpoint lookup should succeed")
                .expect("supervised checkpoint should exist");
        assert_eq!(
            checkpoint.checkpoint_kind,
            "supervised_tool_batch_persisted"
        );
        let call = AiToolCallRecord::find_by_id(&fixture.database, &requested.tool_call_id().0)
            .await
            .expect("supervised tool lookup should succeed")
            .expect("supervised tool should remain durable");
        let swapped_checkpoint_id = Uuid::new_v4();
        let swapped_state = json!({"test": "write-cannot-be-read-only"});
        let swapped_hash = coordinator_checkpoint_hash(
            protected.lease().run_id(),
            protected.lease().attempt_id(),
            protected.lease().lease_generation(),
            swapped_checkpoint_id,
            "tool_batch_persisted",
            call.provider_kind
                .as_deref()
                .expect("provider kind should bind"),
            call.provider_model
                .as_deref()
                .expect("provider model should bind"),
            call.provider_response_id.as_deref(),
            call.budget_reservation_id
                .expect("provider budget should bind"),
            &swapped_state,
        )
        .expect("test checkpoint hash should build");
        assert!(matches!(
            fixture
                .run_service
                .append_coordinator_checkpoint(
                    protected.lease(),
                    PreparedCoordinatorCheckpoint {
                        id: swapped_checkpoint_id,
                        checkpoint_kind: "tool_batch_persisted".to_owned(),
                        provider_kind: call
                            .provider_kind
                            .clone()
                            .expect("provider kind should bind"),
                        provider_model: call
                            .provider_model
                            .clone()
                            .expect("provider model should bind"),
                        provider_response_id: call.provider_response_id.clone(),
                        budget_reservation_id: call
                            .budget_reservation_id
                            .expect("provider budget should bind"),
                        protected_state: swapped_state,
                        checkpoint_hash: swapped_hash,
                        completed_tools: vec![PreparedCoordinatorCheckpointTool {
                            id: call.id,
                            provider_call_id: call.provider_call_id.clone(),
                            tool_id: call.tool_id.clone(),
                            result_egress_manifest_hash: call
                                .result_egress_manifest_hash
                                .clone()
                                .expect("result manifest should bind"),
                        }],
                    },
                )
                .await,
            Err(AiError::Conflict)
        ));
        assert!(matches!(
            checkpoint_service.adopt_tool_batch(protected.lease()).await,
            Err(AiError::Conflict)
        ));
        let run_id = protected.lease().run_id();
        fixture
            .database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    let run = tx
                        .find_by_id::<AiRunRecord>(&run_id.0)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    let outcome = tx
                        .compare_and_swap::<AiRunRecord>(
                            &run.id,
                            run.row_version,
                            AiRunRecordWhereInput::default(),
                            UpdateAiRunRecordInput {
                                lease_expires_at: Some(Some(0)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(outcome, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    Ok(())
                })
            })
            .await
            .expect("generated ORM should expire the supervised checkpoint fixture");
        let recovery = fixture
            .run_service
            .recover_expired_leases()
            .await
            .expect("supervised checkpoint recovery should requeue exact evidence");
        assert_eq!(recovery.recovery_required, 0);
        assert_eq!(recovery.checkpoint_requeued, 1);
        let replacement = fixture
            .run_service
            .claim_next("supervised-checkpoint-adopter")
            .await
            .expect("replacement supervised claim should succeed")
            .expect("checkpointed mutation should be immediately eligible");
        assert_eq!(
            replacement.lease_generation(),
            protected.lease().lease_generation() + 1
        );
        let running = fixture
            .run_service
            .start(&replacement)
            .await
            .expect("replacement supervised claim should start");
        let adopted = checkpoint_service
            .adopt_supervised_tool_batch(&running)
            .await
            .expect("supervised checkpoint should revalidate")
            .expect("supervised checkpoint should be adoptable");
        assert_eq!(adopted.approval_id(), requested.approval_id());
        assert_eq!(adopted.tool_call_id(), requested.tool_call_id());
        assert_eq!(adopted.provider_turns(), 1);
        assert_eq!(adopted.total_tool_calls(), 1);
        assert_eq!(adopted.continuation_result_count(), 1);
        assert_eq!(fixture.mock.request_count(), 1);
        let consumed = checkpoint_service
            .consume_supervised_before_provider(&running, &adopted)
            .await
            .expect("supervised checkpoint should consume once");
        assert!(consumed.latest_checkpoint_id().is_none());
        assert!(matches!(
            checkpoint_service
                .consume_supervised_before_provider(&consumed, &adopted)
                .await,
            Err(AiError::Conflict)
        ));
        let run = AiRunRecord::find_by_id(&fixture.database, &run_id.0)
            .await
            .expect("recovered run lookup should succeed")
            .expect("recovered run should exist");
        assert_eq!(run.state, AiRunState::Running.as_str());
    }

    #[tokio::test]
    async fn ambiguous_supervised_resolver_execution_is_never_replayed() {
        let fixture = fixture(vec![
            ProviderEvent::ResponseStarted {
                response_id: Some("ambiguous-supervised-response".to_owned()),
            },
            ProviderEvent::ToolCallStarted {
                call_id: "ambiguous-supervised-call".to_owned(),
                tool_id: "records.update".to_owned(),
            },
            ProviderEvent::ToolArgumentsDelta {
                call_id: "ambiguous-supervised-call".to_owned(),
                delta: "{\"recordId\":\"55\"}".to_owned(),
            },
            ProviderEvent::ToolCallCompleted {
                call_id: "ambiguous-supervised-call".to_owned(),
                arguments: json!({"recordId": "55"}),
            },
            ProviderEvent::Usage {
                input_tokens: 20,
                output_tokens: 8,
                cached_input_tokens: 0,
            },
            ProviderEvent::ResponseCompleted {
                response_id: Some("ambiguous-supervised-response".to_owned()),
            },
        ])
        .await;
        let (service, requested) = stage_approved_supervised_call(&fixture).await;
        fixture.fail_execution.store(true, Ordering::SeqCst);
        let route = AiToolResultEgressRoute::new(
            "mock-profile",
            "local-mock",
            AiDestinationTrust::Local,
            "continue_supervised_tool_result",
            "none",
            "egress-v1",
        )
        .expect("recovery route should validate");
        let outcome = service
            .execute_approved(
                requested.lease(),
                requested.approval_id(),
                requested.tool_call_id(),
                route,
            )
            .await
            .expect("ambiguous execution should close durably");
        assert!(matches!(
            outcome,
            AiConsequentialToolCallOutcome::RecoveryRequired { tool_call_id }
                if tool_call_id == requested.tool_call_id()
        ));
        let run = AiRunRecord::find_by_id(&fixture.database, &fixture.lease.run_id().0)
            .await
            .expect("run lookup should succeed")
            .expect("run should exist");
        assert_eq!(run.state, AiRunState::RecoveryRequired.as_str());
        let approval = AiApprovalRecord::find_by_id(&fixture.database, &requested.approval_id().0)
            .await
            .expect("approval lookup should succeed")
            .expect("approval should remain auditable");
        assert_eq!(approval.state, "consumed");
        assert_eq!(approval.consumed_uses, 1);
        let call = AiToolCallRecord::find_by_id(&fixture.database, &requested.tool_call_id().0)
            .await
            .expect("tool lookup should succeed")
            .expect("tool should remain auditable");
        assert_eq!(call.state, "executing");
        assert!(call.protected_result.is_none());
        assert!(matches!(
            service
                .execute_approved(
                    requested.lease(),
                    requested.approval_id(),
                    requested.tool_call_id(),
                    AiToolResultEgressRoute::new(
                        "mock-profile",
                        "local-mock",
                        AiDestinationTrust::Local,
                        "continue_supervised_tool_result",
                        "none",
                        "egress-v1",
                    )
                    .expect("replay route should validate"),
                )
                .await,
            Err(AiError::Conflict)
        ));
    }

    #[test]
    fn ordinary_provider_plan_still_rejects_custom_tools() {
        let request = ModelRequest {
            model: "mock-model".to_owned(),
            instructions: vec![],
            input: vec![],
            continuation: None,
            continuation_mode: crate::ModelContinuationMode::ProviderRetained,
            tools: vec![ModelToolDefinition {
                tool_id: "records.read".to_owned(),
                provider_name: "records_read".to_owned(),
                fingerprint: "fingerprint".to_owned(),
                description: "Read records".to_owned(),
                parameters: json!({"type": "object"}),
                strict: true,
            }],
            builtin_tools: vec![],
            maximum_builtin_tool_calls: None,
            output_schema: None,
            maximum_output_tokens: Some(100),
        };
        let session_id = AiSessionId::new();
        let run_id = AiRunId::new();
        let scope = AiScope::new("test", "test");
        let budget = AiBudgetReservationRequest {
            scope: scope.clone(),
            session_id,
            run_id,
            attempt_id: Uuid::new_v4(),
            lease_generation: 1,
            provider_kind: ProviderKind::OpenAiCompatible,
            model: request.model.clone(),
            pricing_policy_version: "test".to_owned(),
            estimate: AiBudgetAmounts {
                runs: 1,
                ..Default::default()
            },
            idempotency_key: "test".to_owned(),
            expires_at: OffsetDateTime::now_utc() + Duration::minutes(1),
        };
        let manifest = AiEgressManifest {
            provider_profile_id: "test".to_owned(),
            provider_kind: ProviderKind::OpenAiCompatible.as_str().to_owned(),
            model: request.model.clone(),
            destination: "local".to_owned(),
            destination_trust: AiDestinationTrust::Local,
            capability: AiEgressCapability::ModelInference,
            scope,
            session_id: Some(session_id),
            run_id: Some(run_id),
            sources: vec![],
            estimated_bytes: 0,
            estimated_tokens: 0,
            attachment_count: 0,
            purpose: "test".to_owned(),
            retention: "none".to_owned(),
            residency: None,
            policy_version: "test".to_owned(),
            consent_reference: None,
        };
        assert!(matches!(
            AiProviderCallPlan::new(
                ProviderKind::OpenAiCompatible,
                request,
                budget,
                vec![manifest],
                "test"
            ),
            Err(AiError::InvalidInput(_))
        ));
    }
}
