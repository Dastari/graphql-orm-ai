//! Security-ordered execution of one provider call for a fenced run.

use std::collections::BTreeMap;
use std::sync::Arc;

use agql_auth::Clock;
use async_trait::async_trait;
use futures::StreamExt;

use crate::{
    AiBudgetAmounts, AiBudgetReconciliation, AiBudgetReconciliationOutcome, AiBudgetReservation,
    AiBudgetReservationId, AiBudgetReservationRequest, AiBudgetService, AiEgressCapability,
    AiEgressDecisionAudit, AiEgressManifest, AiError, AiRunLease, AiRunState, AiRuntime,
    AiSessionAction, AiToolPolicySet, ModelBuiltinTool, ModelContinuation, ModelRequest,
    ProviderEvent, ProviderKind, ProviderRequestContext,
};

/// Deployment-owned bounds for a single normalized provider stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiProviderCallLimits {
    maximum_events: usize,
    maximum_event_bytes: usize,
    maximum_total_event_bytes: usize,
    maximum_tool_calls: usize,
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
    /// absent model-inference manifest, or more than 32 transfers.
    pub fn new(
        provider_kind: ProviderKind,
        request: ModelRequest,
        budget: AiBudgetReservationRequest,
        mut transfers: Vec<AiEgressManifest>,
        correlation_id: impl Into<String>,
    ) -> Result<Self, AiError> {
        request
            .validate()
            .map_err(|_| AiError::InvalidInput("invalid provider call plan".to_owned()))?;
        let correlation_id = correlation_id.into();
        if !request.tools.is_empty()
            || correlation_id.trim().is_empty()
            || correlation_id.len() > 512
            || transfers.is_empty()
            || transfers.len() > 32
            || budget.provider_kind != provider_kind
            || budget.model != request.model
        {
            return Err(AiError::InvalidInput(
                "invalid provider call plan".to_owned(),
            ));
        }
        if transfers.iter().any(|manifest| {
            manifest.provider_kind != provider_kind.as_str()
                || manifest.model != request.model
                || manifest.session_id != Some(budget.session_id)
                || manifest.run_id != Some(budget.run_id)
                || manifest.scope != budget.scope
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
        })
    }

    /// Creates a provider call exposing only explicitly enabled, registered,
    /// read-only application queries.
    ///
    /// The supplied policy set is a current configuration snapshot, not
    /// execution authorization. Each model-requested call is reauthorized with
    /// a fresh principal and then executed through the ordinary GraphQL
    /// resolver path.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidInput`] for ordinary plan-binding failures and
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
        if request.tools.is_empty() {
            return Err(AiError::InvalidInput(
                "provider tool plan has no tools".to_owned(),
            ));
        }
        for definition in &request.tools {
            catalog.validate_read_only_model_definition(definition, policy)?;
        }
        let mut request_without_tools = request.clone();
        request_without_tools.tools.clear();
        let mut plan = Self::new(
            provider_kind,
            request_without_tools,
            budget,
            transfers,
            correlation_id,
        )?;
        plan.request = request;
        Ok(plan)
    }

    /// Creates a subsequent read-only tool turn from one exact bounded-loop
    /// continuation.
    ///
    /// The continuation installs the previous response ID, matched tool-result
    /// blocks, and their immutable exact egress manifests as one unit. The
    /// caller supplies a fresh model-inference manifest and budget request;
    /// every transfer is freshly reauthorized and audited by the executor.
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
        Self::new_with_tools(
            provider_kind,
            request,
            budget,
            transfers,
            correlation_id,
            catalog,
            policy,
        )
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
    tool_calls: Vec<AiProviderToolCall>,
}

/// Authoritative provider usage observation requiring deployment pricing/unit
/// settlement before budget commit.
#[derive(Clone, Debug)]
pub struct AiProviderUsageObservation {
    provider_kind: ProviderKind,
    model: String,
    pricing_policy_version: String,
    input_tokens: u64,
    output_tokens: u64,
    cached_input_tokens: u64,
    builtin_tools: Vec<ModelBuiltinTool>,
}

impl AiProviderUsageObservation {
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

    /// Provider-reported non-cached input tokens.
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

    /// Provider built-ins requested in the exact completed turn.
    pub fn builtin_tools(&self) -> &[ModelBuiltinTool] {
        &self.builtin_tools
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
        }
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
                self.clock.now(),
            )
            .map_err(|_| AiError::BudgetDenied)?;

        let context = match self
            .authorize_and_audit_transfers(lease, &plan, authorized_budget)
            .await
        {
            Ok(context) => context,
            Err(error) => {
                self.release_unstarted(lease, &reservation).await?;
                return Err(error);
            }
        };

        let current = self
            .runtime
            .resolve_current_principal(lease.principal_reference())
            .await?;
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
        self.budget_service
            .reconcile(
                &current,
                AiBudgetReconciliation {
                    reservation_id: reservation.id(),
                    attempt_id: lease.attempt_id(),
                    lease_generation: lease.lease_generation(),
                    actual: None,
                    outcome: AiBudgetReconciliationOutcome::MarkUncertain,
                },
            )
            .await?;

        let provider_model = plan.request.model.clone();
        let builtin_tools = plan.request.builtin_tools.clone();
        let previous_response_id = plan.request.continuation.as_ref().map(|continuation| {
            let ModelContinuation::ProviderResponse { response_id } = continuation;
            response_id.clone()
        });
        let offered_tools = plan
            .request
            .tools
            .iter()
            .map(|tool| {
                (
                    tool.tool_id.clone(),
                    (tool.fingerprint.clone(), tool.parameters.clone()),
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
        while let Some(item) = stream.next().await {
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
                    let Some((fingerprint, argument_schema)) = offered_tools.get(tool_id) else {
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
                            tool_fingerprint: fingerprint.clone(),
                            arguments: arguments.clone(),
                        },
                    );
                }
                _ => {}
            }
            events.push(event);
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
        let mut tool_calls = Vec::with_capacity(tool_call_order.len());
        for call_id in tool_call_order {
            tool_calls.push(
                completed_tool_calls
                    .remove(&call_id)
                    .ok_or(AiError::ProviderFailed)?,
            );
        }

        let observation = AiProviderUsageObservation {
            provider_kind: plan.provider_kind.clone(),
            model: provider_model.clone(),
            pricing_policy_version: plan.budget.pricing_policy_version,
            input_tokens,
            output_tokens,
            cached_input_tokens,
            builtin_tools,
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
            tool_calls,
        })
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
    use super::*;
    use agql_auth::{
        AccessTokenMetadata, AuthPrincipal, AuthUser, CurrentPrincipalResolver, ResolvedPrincipal,
        SessionContext, SystemClock,
    };
    use async_trait::async_trait;
    use graphql_orm::graphql::errors::OrmPublicError;
    use graphql_orm::graphql::orm::{ApplyOptions, OrmSchemaModule, TransactionMode};
    use graphql_orm::prelude::{Database, SqliteBackend};
    use serde_json::json;
    use time::{Duration, OffsetDateTime};
    use uuid::Uuid;

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
            _invocation: &GraphqlInvocationContext,
        ) -> Result<GraphqlRequestContext, ToolExecutionError> {
            Ok(GraphqlRequestContext::new(
                principal.principal().subject().to_owned(),
            ))
        }
    }

    struct Executor;

    #[async_trait]
    impl AuthenticatedGraphqlExecutor for Executor {
        async fn execute(
            &self,
            context: GraphqlRequestContext,
            request: ToolGraphqlRequest,
        ) -> Result<ToolGraphqlResponse, ToolExecutionError> {
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

    struct AllowTools;

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
                "tool-policy-v1",
                format!("auth-state:{}", principal.principal().subject()),
            )
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
                tool_units: 0,
                image_units: 0,
                cost_microunits: 42,
                runs: 1,
            })
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
    }

    async fn fixture(events: Vec<ProviderEvent>) -> Fixture {
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
            },
        )
        .await
        .expect("test run should insert");
        AiBudgetPolicyRecord::insert(
            &database,
            CreateAiBudgetPolicyRecordInput {
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
        let mock = MockProvider::new(events);
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
        let runtime = AiRuntime::builder()
            .principal_resolver(Arc::new(Resolver(principal.clone())))
            .access_policy(Arc::new(AllowAccess))
            .tool_authorization_policy(Arc::new(AllowTools))
            .request_context_factory(Arc::new(ContextFactory))
            .graphql_executor(Arc::new(Executor))
            .graphql_targets(targets)
            .egress_policy(Arc::new(AllowEgress))
            .deployment_egress(AiDeploymentEgressBoundary {
                allowed_destination_trust: [AiDestinationTrust::Local].into_iter().collect(),
                allowed_capabilities: [
                    AiEgressCapability::ModelInference,
                    AiEgressCapability::ToolResult,
                ]
                .into_iter()
                .collect(),
                maximum_classification: DataClassification::Internal,
                maximum_bytes: 16_384,
                maximum_attachments: 0,
            })
            .maximum_tool_maturity(ToolMaturity::ProposalOnly)
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
        }
    }

    fn plan(fixture: &Fixture) -> AiProviderCallPlan {
        let request = ModelRequest {
            model: "mock-model".to_owned(),
            instructions: vec!["Return a bounded test response".to_owned()],
            input: vec![ModelInputBlock::Text {
                text: "hello".to_owned(),
            }],
            continuation: None,
            tools: vec![],
            builtin_tools: vec![],
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
            estimated_bytes: 1_024,
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
        let executor = AiProviderCallExecutor::new(
            fixture.runtime.clone(),
            fixture.budget_service.clone(),
            fixture.audit.clone(),
            Arc::new(TestUsageAccounting),
            Arc::new(SystemClock),
            AiProviderCallLimits::new(64, 8_192, 64 * 1_024)
                .expect("test provider limits should validate"),
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
        let persisted = service
            .execute_read_only(&fixture.lease, &provider_result, call_context, route)
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
                .get("protection")
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
            tools: vec![ModelToolDefinition {
                tool_id: descriptor.id.as_str().to_owned(),
                provider_name: "records_read".to_owned(),
                fingerprint: descriptor.fingerprint.clone(),
                description: descriptor.description.clone(),
                parameters: descriptor.argument_schema.clone(),
                strict: true,
            }],
            builtin_tools: Vec::new(),
            output_schema: None,
            maximum_output_tokens: Some(100),
        };
        let mut policy = AiToolPolicySet::new(ToolMaturity::ReadOnly);
        policy.bind(AiToolPolicyBinding {
            tool_id: descriptor.id.clone(),
            fingerprint: descriptor.fingerprint.clone(),
            enabled: true,
        });
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
                persisted.lease(),
                AiRunCompletion::new(AiRunState::Completed, "tool_loop_test", None, None)
                    .expect("test completion should validate"),
            )
            .await
            .expect("renewed tool fence should complete the run");
    }

    #[test]
    fn ordinary_provider_plan_still_rejects_custom_tools() {
        let request = ModelRequest {
            model: "mock-model".to_owned(),
            instructions: vec![],
            input: vec![],
            continuation: None,
            tools: vec![ModelToolDefinition {
                tool_id: "records.read".to_owned(),
                provider_name: "records_read".to_owned(),
                fingerprint: "fingerprint".to_owned(),
                description: "Read records".to_owned(),
                parameters: json!({"type": "object"}),
                strict: true,
            }],
            builtin_tools: vec![],
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
