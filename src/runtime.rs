//! Runtime construction, hard boundaries, and startup readiness.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use agql_auth::{CurrentPrincipalResolver, PrincipalReference, ResolvedPrincipal};

use crate::{
    AiAccessPolicy, AiApprovalBinding, AiApprovalRule, AiContentProtectionPolicyResolver,
    AiContentProtector, AiDeploymentEgressBoundary, AiDisclosureEvaluation, AiEgressDecision,
    AiEgressManifest, AiEgressPolicy, AiError, AiProposalCatalog, AiProvider, AiSchemaModule,
    AiSecretStore, AiToolAuthorizationDecision, AiToolAuthorizationPolicy, AiToolCatalog,
    AiToolDescriptor, AiToolId, AuthenticatedGraphqlExecutor, AuthenticatedToolBridge,
    ConsumedAiApproval, GraphqlExecutionTargetRegistry, GraphqlRequestContextFactory, ModelRequest,
    ProviderBackgroundBinding, ProviderBackgroundObservation, ProviderBackgroundRetrievalBinding,
    ProviderBackgroundRetrievalContext, ProviderBackgroundSubmission, ProviderError,
    ProviderEventStream, ProviderKind, ProviderRequestContext, ToolGraphqlRequest,
    ToolGraphqlResponse, ToolMaturity,
};
use graphql_orm::graphql::orm::{OrmSchemaModule, SchemaModuleCatalog};

/// Evidence required to open the runtime start gate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiRuntimeReadinessReport {
    /// Compiled module fingerprint validated against managed schema/restore.
    pub module_fingerprint: String,
    /// Finished application schema/executor is bound.
    pub executor_bound: bool,
    /// Restore/reconciliation completed or was not required for a new store.
    pub restore_reconciled: bool,
    /// Fatal validation/recovery issue count.
    pub fatal_issue_count: u64,
}

/// Fail-closed runtime start gate.
#[derive(Debug)]
pub struct AiRuntimeStartGate {
    expected_module_fingerprint: String,
    ready: AtomicBool,
}

impl AiRuntimeStartGate {
    fn new(expected_module_fingerprint: String) -> Self {
        Self {
            expected_module_fingerprint,
            ready: AtomicBool::new(false),
        }
    }

    /// Opens the gate only for matching schema, bound execution, completed
    /// restore reconciliation, and zero fatal issues.
    pub fn open(&self, report: &AiRuntimeReadinessReport) -> Result<(), AiError> {
        if report.module_fingerprint != self.expected_module_fingerprint
            || !report.executor_bound
            || !report.restore_reconciled
            || report.fatal_issue_count != 0
        {
            return Err(AiError::RuntimeNotReady);
        }
        self.ready.store(true, Ordering::Release);
        Ok(())
    }

    /// Closes the gate immediately, for restore, shutdown, or fatal drift.
    pub fn close(&self) {
        self.ready.store(false, Ordering::Release);
    }

    /// Returns whether workers/provider calls may start.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Returns the compiled module fingerprint.
    pub fn expected_module_fingerprint(&self) -> &str {
        &self.expected_module_fingerprint
    }
}

/// Built project-agnostic runtime.
pub struct AiRuntime {
    principal_resolver: Arc<dyn CurrentPrincipalResolver>,
    access_policy: Arc<dyn AiAccessPolicy>,
    tool_bridge: AuthenticatedToolBridge,
    egress_policy: Arc<dyn AiEgressPolicy>,
    deployment_egress: AiDeploymentEgressBoundary,
    maximum_tool_maturity: ToolMaturity,
    tool_catalog: AiToolCatalog,
    proposal_catalog: AiProposalCatalog,
    secret_store: Arc<dyn AiSecretStore>,
    content_protection_policy_resolver: Arc<dyn AiContentProtectionPolicyResolver>,
    content_protector: Arc<dyn AiContentProtector>,
    providers: BTreeMap<ProviderKind, Arc<dyn AiProvider>>,
    start_gate: AiRuntimeStartGate,
}

/// Registered, freshly authorized, and statically disclosure-validated tool result.
#[derive(Clone, Debug)]
pub struct AiToolExecutionResult {
    response: ToolGraphqlResponse,
    disclosure: AiDisclosureEvaluation,
    tool_fingerprint: String,
    policy_version: String,
    authorization_state_digest: String,
}

/// Fresh current-principal host tool-policy proof for one exact registered
/// request.
///
/// This proof is intentionally weaker than resolver authorization and does
/// not prove approval, unchanged application resources, or execution. A
/// consequential workflow binds its version/digest into the canonical action
/// envelope, then recomputes and compares both immediately before invoking the
/// resolver.
#[derive(Clone, Debug)]
pub struct AiToolPreauthorization {
    principal: ResolvedPrincipal,
    tool_fingerprint: String,
    policy_version: String,
    authorization_state_digest: String,
}

impl AiToolPreauthorization {
    /// Freshly resolved principal used by a server-owned preview builder.
    pub fn principal(&self) -> &ResolvedPrincipal {
        &self.principal
    }

    /// Exact registered descriptor fingerprint that was authorized.
    pub fn tool_fingerprint(&self) -> &str {
        &self.tool_fingerprint
    }

    /// Current host tool-policy version.
    pub fn policy_version(&self) -> &str {
        &self.policy_version
    }

    /// Safe current authorization-state digest.
    pub fn authorization_state_digest(&self) -> &str {
        &self.authorization_state_digest
    }
}

impl AiToolExecutionResult {
    /// Returns the bounded projected GraphQL response.
    pub fn response(&self) -> &ToolGraphqlResponse {
        &self.response
    }

    /// Returns the static disclosure evaluation required for egress planning.
    pub const fn disclosure(&self) -> AiDisclosureEvaluation {
        self.disclosure
    }

    /// Returns the exact registered tool fingerprint used for execution.
    pub fn tool_fingerprint(&self) -> &str {
        &self.tool_fingerprint
    }

    /// Returns the current host tool-policy version used for authorization.
    pub fn policy_version(&self) -> &str {
        &self.policy_version
    }

    /// Returns the safe current authorization-state digest for approval binding.
    pub fn authorization_state_digest(&self) -> &str {
        &self.authorization_state_digest
    }

    /// Builds the bounded provider-facing result. Host audit references remain
    /// local and are deliberately excluded.
    pub fn model_output(&self) -> serde_json::Value {
        serde_json::json!({
            "data": self.response.data,
            "errorCodes": self.response.error_codes,
        })
    }
}

impl AiRuntime {
    /// Starts constructing a runtime.
    pub fn builder() -> AiRuntimeBuilder {
        AiRuntimeBuilder::default()
    }

    /// Returns the runtime start gate.
    pub fn start_gate(&self) -> &AiRuntimeStartGate {
        &self.start_gate
    }

    /// Returns registered tool metadata. Exposure still requires a separate
    /// scope policy and the deployment maturity cap.
    pub fn tool_catalog(&self) -> &AiToolCatalog {
        &self.tool_catalog
    }

    /// Returns registered proposal contracts.
    pub fn proposal_catalog(&self) -> &AiProposalCatalog {
        &self.proposal_catalog
    }

    /// Returns the immutable deployment maturity ceiling.
    pub fn maximum_tool_maturity(&self) -> ToolMaturity {
        self.maximum_tool_maturity
    }

    /// Returns the host application access policy used by session/service
    /// implementations.
    pub fn access_policy(&self) -> &Arc<dyn AiAccessPolicy> {
        &self.access_policy
    }

    /// Returns the configured credential/key indirection store.
    pub fn secret_store(&self) -> &Arc<dyn AiSecretStore> {
        &self.secret_store
    }

    /// Returns the configured conversational content protector.
    pub fn content_protector(&self) -> &Arc<dyn AiContentProtector> {
        &self.content_protector
    }

    /// Returns the current per-scope content-protection policy resolver.
    pub fn content_protection_policy_resolver(
        &self,
    ) -> &Arc<dyn AiContentProtectionPolicyResolver> {
        &self.content_protection_policy_resolver
    }

    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) async fn resolve_current_principal(
        &self,
        reference: &PrincipalReference,
    ) -> Result<agql_auth::ResolvedPrincipal, AiError> {
        self.principal_resolver
            .resolve(reference)
            .await
            .map_err(|_| AiError::ReauthorizationFailed)
    }

    /// Applies deployment hard limits and scope policy to an exact egress
    /// manifest after current-principal rehydration.
    pub async fn authorize_egress(
        &self,
        principal_reference: &PrincipalReference,
        manifest: &AiEgressManifest,
    ) -> Result<AiEgressDecision, AiError> {
        if let Err(reason) = self.deployment_egress.evaluate(manifest) {
            return Ok(AiEgressDecision::deny(
                manifest,
                reason,
                "deployment-boundary",
                &principal_reference.subject,
            ));
        }
        let principal = self
            .principal_resolver
            .resolve(principal_reference)
            .await
            .map_err(|_| AiError::ReauthorizationFailed)?;
        Ok(self.egress_policy.authorize(&principal, manifest).await)
    }

    /// Executes an exact registered application tool through fresh host tool
    /// policy and the canonical current-principal request-context path.
    ///
    /// The returned result has passed the tool's static disclosure schema, but
    /// still requires a separate egress decision before external disclosure.
    ///
    /// # Errors
    ///
    /// Fails closed when the runtime is not ready, registration/arguments/
    /// maturity are stale, current tool policy denies, resolver execution
    /// fails, output limits are exceeded, or static disclosure validation
    /// fails.
    pub async fn execute_tool(
        &self,
        principal_reference: &PrincipalReference,
        tool_id: &AiToolId,
        request: ToolGraphqlRequest,
    ) -> Result<AiToolExecutionResult, AiError> {
        if !self.start_gate.is_ready() {
            return Err(AiError::RuntimeNotReady);
        }
        let (descriptor, disclosure_schema) = self.tool_catalog.validate_execution_request(
            tool_id,
            &request,
            self.maximum_tool_maturity,
        )?;
        if descriptor.approval != AiApprovalRule::None {
            return Err(AiError::Forbidden);
        }
        let (response, authorization) = self
            .tool_bridge
            .execute(principal_reference, descriptor, request)
            .await
            .map_err(|_| AiError::ToolExecutionFailed)?;
        self.finish_tool_execution(descriptor, disclosure_schema, response, authorization)
    }

    /// Rehydrates and evaluates current host tool policy for an exact
    /// registered request without invoking its resolver.
    ///
    /// The returned proof is suitable for canonical approval binding only. It
    /// does not replace ordinary resolver authorization or one-shot approval.
    ///
    /// # Errors
    ///
    /// Fails closed when the runtime is not ready, the descriptor/request is
    /// stale or above the deployment maturity cap, arguments are invalid, or
    /// current host tool policy denies the request.
    pub async fn preauthorize_tool(
        &self,
        principal_reference: &PrincipalReference,
        tool_id: &AiToolId,
        request: &ToolGraphqlRequest,
    ) -> Result<AiToolPreauthorization, AiError> {
        if !self.start_gate.is_ready() {
            return Err(AiError::RuntimeNotReady);
        }
        let (descriptor, _) = self.tool_catalog.validate_execution_request(
            tool_id,
            request,
            self.maximum_tool_maturity,
        )?;
        let (principal, authorization) = self
            .tool_bridge
            .preauthorize(principal_reference, descriptor, request)
            .await
            .map_err(|_| AiError::Forbidden)?;
        Ok(AiToolPreauthorization {
            principal,
            tool_fingerprint: descriptor.fingerprint.clone(),
            policy_version: authorization.policy_version,
            authorization_state_digest: authorization.authorization_state_digest,
        })
    }

    /// Executes one exact supervised application mutation after atomic
    /// consumption of its complete one-shot approval envelope.
    ///
    /// The bridge rehydrates and authorizes again, compares the newly computed
    /// policy version and authorization-state digest before invoking the
    /// resolver, then applies the same bounded static disclosure validation as
    /// an ordinary tool result. Approval never substitutes for the resolver's
    /// normal row/field/domain authorization.
    ///
    /// # Errors
    ///
    /// Fails closed for a stale consumption/binding, a non-supervised or
    /// non-one-shot descriptor, changed current policy/authorization state,
    /// resolver ambiguity, output-limit violations, or static disclosure
    /// failure.
    pub async fn execute_approved_tool(
        &self,
        principal_reference: &PrincipalReference,
        tool_id: &AiToolId,
        request: ToolGraphqlRequest,
        approval: &ConsumedAiApproval,
        binding: &AiApprovalBinding,
    ) -> Result<AiToolExecutionResult, AiError> {
        if !self.start_gate.is_ready()
            || approval.binding_hash() != binding.stable_hash()
            || approval.approval_id().0.is_nil()
        {
            return Err(AiError::Forbidden);
        }
        let (descriptor, disclosure_schema) = self.tool_catalog.validate_execution_request(
            tool_id,
            &request,
            self.maximum_tool_maturity,
        )?;
        if !is_supervised_one_shot_mutation(descriptor)
            || descriptor.fingerprint != binding.tool_fingerprint
            || descriptor.graphql_contract.as_ref() != Some(&binding.operation)
        {
            return Err(AiError::Forbidden);
        }
        let (response, authorization) = self
            .tool_bridge
            .execute_bound(
                principal_reference,
                descriptor,
                request,
                &binding.policy_version,
                &binding.authorization_state_digest,
            )
            .await
            .map_err(|_| AiError::ToolExecutionFailed)?;
        self.finish_tool_execution(descriptor, disclosure_schema, response, authorization)
    }

    fn finish_tool_execution(
        &self,
        descriptor: &AiToolDescriptor,
        disclosure_schema: &crate::AiDisclosureSchema,
        response: ToolGraphqlResponse,
        authorization: AiToolAuthorizationDecision,
    ) -> Result<AiToolExecutionResult, AiError> {
        if response.error_codes.len() > 32
            || response.error_codes.iter().any(|code| {
                code.is_empty()
                    || code.len() > 100
                    || !code.bytes().all(|byte| {
                        byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_'
                    })
            })
            || response
                .application_audit_ref
                .as_ref()
                .is_some_and(|reference| {
                    reference.is_empty()
                        || reference.len() > 1_024
                        || !reference.bytes().all(|byte| byte.is_ascii_graphic())
                })
        {
            return Err(AiError::ToolExecutionFailed);
        }
        let response_bytes = serde_json::to_vec(&response.data)
            .map_err(|_| AiError::ToolExecutionFailed)?
            .len() as u64;
        if response_bytes > descriptor.maximum_result_bytes {
            return Err(AiError::ToolExecutionFailed);
        }
        let disclosure = disclosure_schema
            .evaluate(&response.data)
            .map_err(|_| AiError::ToolExecutionFailed)?;
        Ok(AiToolExecutionResult {
            response,
            disclosure,
            tool_fingerprint: descriptor.fingerprint.clone(),
            policy_version: authorization.policy_version,
            authorization_state_digest: authorization.authorization_state_digest,
        })
    }

    /// Calls a registered provider only after start readiness and exact egress
    /// authorization.
    pub async fn stream_provider(
        &self,
        provider_kind: &ProviderKind,
        request: ModelRequest,
        context: ProviderRequestContext,
    ) -> Result<ProviderEventStream, ProviderError> {
        if !self.start_gate.is_ready() {
            return Err(ProviderError::InvalidConfiguration(
                "AI runtime is not ready".to_owned(),
            ));
        }
        context.validate_request(provider_kind, &request)?;
        let provider = self
            .providers
            .get(provider_kind)
            .ok_or(ProviderError::Unsupported)?;
        if provider.provider_kind() != *provider_kind {
            return Err(ProviderError::InvalidConfiguration(
                "provider registry kind mismatch".to_owned(),
            ));
        }
        provider.stream(request, context).await
    }

    /// Starts a registered provider background response after runtime,
    /// capability, budget, and exact egress validation.
    ///
    /// # Errors
    ///
    /// Returns a provider error when the runtime is closed, the provider is
    /// absent or mismatched, background processing is unsupported, or any
    /// request proof fails closed.
    pub async fn submit_provider_background(
        &self,
        provider_kind: &ProviderKind,
        request: ModelRequest,
        context: ProviderRequestContext,
        binding: ProviderBackgroundBinding,
    ) -> Result<ProviderBackgroundSubmission, ProviderError> {
        if !self.start_gate.is_ready() {
            return Err(ProviderError::InvalidConfiguration(
                "AI runtime is not ready".to_owned(),
            ));
        }
        context.validate_request(provider_kind, &request)?;
        let provider = self
            .providers
            .get(provider_kind)
            .ok_or(ProviderError::Unsupported)?;
        if provider.provider_kind() != *provider_kind {
            return Err(ProviderError::InvalidConfiguration(
                "provider registry kind mismatch".to_owned(),
            ));
        }
        if !provider.capabilities().background {
            return Err(ProviderError::Unsupported);
        }
        provider.submit_background(request, context, binding).await
    }

    /// Retrieves one exactly bound provider background response after runtime,
    /// capability, and current egress validation.
    ///
    /// # Errors
    ///
    /// Returns a provider error when the runtime is closed, the provider is
    /// absent or mismatched, background processing is unsupported, or the
    /// exact retrieval proof fails closed.
    pub async fn retrieve_provider_background(
        &self,
        provider_kind: &ProviderKind,
        binding: ProviderBackgroundRetrievalBinding,
        context: ProviderBackgroundRetrievalContext,
    ) -> Result<ProviderBackgroundObservation, ProviderError> {
        if !self.start_gate.is_ready() {
            return Err(ProviderError::InvalidConfiguration(
                "AI runtime is not ready".to_owned(),
            ));
        }
        let provider = self
            .providers
            .get(provider_kind)
            .ok_or(ProviderError::Unsupported)?;
        if provider.provider_kind() != *provider_kind {
            return Err(ProviderError::InvalidConfiguration(
                "provider registry kind mismatch".to_owned(),
            ));
        }
        if !provider.capabilities().background {
            return Err(ProviderError::Unsupported);
        }
        provider.retrieve_background(binding, context).await
    }
}

fn is_supervised_one_shot_mutation(descriptor: &AiToolDescriptor) -> bool {
    descriptor.operation_kind == crate::AiToolOperationKind::Mutation
        && descriptor.operation_domain == crate::AiToolOperationDomain::Application
        && descriptor.maturity == ToolMaturity::SupervisedWrite
        && descriptor.approval == AiApprovalRule::OneShot
        && matches!(
            descriptor.risk,
            crate::AiToolRisk::LowRiskWrite
                | crate::AiToolRisk::NonIdempotentWrite
                | crate::AiToolRisk::HighImpact
        )
}

/// Runtime builder with fail-closed required dependencies.
#[derive(Default)]
#[must_use]
pub struct AiRuntimeBuilder {
    principal_resolver: Option<Arc<dyn CurrentPrincipalResolver>>,
    tool_authorization_policy: Option<Arc<dyn AiToolAuthorizationPolicy>>,
    access_policy: Option<Arc<dyn AiAccessPolicy>>,
    context_factory: Option<Arc<dyn GraphqlRequestContextFactory>>,
    graphql_executor: Option<Arc<dyn AuthenticatedGraphqlExecutor>>,
    graphql_targets: Option<GraphqlExecutionTargetRegistry>,
    egress_policy: Option<Arc<dyn AiEgressPolicy>>,
    deployment_egress: Option<AiDeploymentEgressBoundary>,
    maximum_tool_maturity: Option<ToolMaturity>,
    tool_catalog: AiToolCatalog,
    proposal_catalog: AiProposalCatalog,
    secret_store: Option<Arc<dyn AiSecretStore>>,
    content_protection_policy_resolver: Option<Arc<dyn AiContentProtectionPolicyResolver>>,
    content_protector: Option<Arc<dyn AiContentProtector>>,
    providers: BTreeMap<ProviderKind, Arc<dyn AiProvider>>,
}

impl AiRuntimeBuilder {
    /// Sets current-principal rehydration.
    pub fn principal_resolver(mut self, resolver: Arc<dyn CurrentPrincipalResolver>) -> Self {
        self.principal_resolver = Some(resolver);
        self
    }

    /// Sets host application scope/session access policy.
    pub fn access_policy(mut self, policy: Arc<dyn AiAccessPolicy>) -> Self {
        self.access_policy = Some(policy);
        self
    }

    /// Sets fresh current-principal authorization for registered application tools.
    pub fn tool_authorization_policy(mut self, policy: Arc<dyn AiToolAuthorizationPolicy>) -> Self {
        self.tool_authorization_policy = Some(policy);
        self
    }

    /// Sets the canonical host request-context factory.
    pub fn request_context_factory(
        mut self,
        factory: Arc<dyn GraphqlRequestContextFactory>,
    ) -> Self {
        self.context_factory = Some(factory);
        self
    }

    /// Sets composed host GraphQL execution.
    pub fn graphql_executor(mut self, executor: Arc<dyn AuthenticatedGraphqlExecutor>) -> Self {
        self.graphql_executor = Some(executor);
        self
    }

    /// Sets immutable deployment registration for local/remote GraphQL targets.
    pub fn graphql_targets(mut self, targets: GraphqlExecutionTargetRegistry) -> Self {
        self.graphql_targets = Some(targets);
        self
    }

    /// Sets scope/application egress policy.
    pub fn egress_policy(mut self, policy: Arc<dyn AiEgressPolicy>) -> Self {
        self.egress_policy = Some(policy);
        self
    }

    /// Sets immutable deployment egress limits.
    pub fn deployment_egress(mut self, boundary: AiDeploymentEgressBoundary) -> Self {
        self.deployment_egress = Some(boundary);
        self
    }

    /// Sets immutable deployment tool-maturity cap.
    pub fn maximum_tool_maturity(mut self, maturity: ToolMaturity) -> Self {
        self.maximum_tool_maturity = Some(maturity);
        self
    }

    /// Sets registered tool metadata.
    pub fn tool_catalog(mut self, catalog: AiToolCatalog) -> Self {
        self.tool_catalog = catalog;
        self
    }

    /// Sets registered proposal contracts.
    pub fn proposal_catalog(mut self, catalog: AiProposalCatalog) -> Self {
        self.proposal_catalog = catalog;
        self
    }

    /// Sets provider credential/key indirection.
    pub fn secret_store(mut self, store: Arc<dyn AiSecretStore>) -> Self {
        self.secret_store = Some(store);
        self
    }

    /// Sets per-scope conversational content protection.
    pub fn content_protector(mut self, protector: Arc<dyn AiContentProtector>) -> Self {
        self.content_protector = Some(protector);
        self
    }

    /// Sets authorized GraphQL-managed per-scope protection-policy lookup.
    pub fn content_protection_policy_resolver(
        mut self,
        resolver: Arc<dyn AiContentProtectionPolicyResolver>,
    ) -> Self {
        self.content_protection_policy_resolver = Some(resolver);
        self
    }

    /// Registers a provider adapter.
    pub fn provider(mut self, provider: Arc<dyn AiProvider>) -> Result<Self, AiError> {
        let kind = provider.provider_kind();
        if self.providers.insert(kind.clone(), provider).is_some() {
            return Err(AiError::AlreadyExists(format!("provider {kind:?}")));
        }
        Ok(self)
    }

    /// Validates required security seams and builds a closed runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if a required security dependency or schema-module
    /// contract is missing/invalid.
    pub fn build(self) -> Result<AiRuntime, AiError> {
        let principal_resolver = self.principal_resolver.ok_or_else(|| {
            AiError::InvalidConfiguration("current-principal resolver is required".to_owned())
        })?;
        let access_policy = self.access_policy.ok_or_else(|| {
            AiError::InvalidConfiguration("AI access policy is required".to_owned())
        })?;
        let tool_authorization_policy = self.tool_authorization_policy.ok_or_else(|| {
            AiError::InvalidConfiguration("AI tool authorization policy is required".to_owned())
        })?;
        let context_factory = self.context_factory.ok_or_else(|| {
            AiError::InvalidConfiguration("GraphQL request-context factory is required".to_owned())
        })?;
        let graphql_executor = self.graphql_executor.ok_or_else(|| {
            AiError::InvalidConfiguration("authenticated GraphQL executor is required".to_owned())
        })?;
        let graphql_targets = self.graphql_targets.ok_or_else(|| {
            AiError::InvalidConfiguration("GraphQL target registry is required".to_owned())
        })?;
        let egress_policy = self.egress_policy.ok_or_else(|| {
            AiError::InvalidConfiguration("explicit egress policy is required".to_owned())
        })?;
        let deployment_egress = self.deployment_egress.ok_or_else(|| {
            AiError::InvalidConfiguration("deployment egress boundary is required".to_owned())
        })?;
        let maximum_tool_maturity = self.maximum_tool_maturity.ok_or_else(|| {
            AiError::InvalidConfiguration("deployment tool-maturity cap is required".to_owned())
        })?;
        let secret_store = self.secret_store.ok_or_else(|| {
            AiError::InvalidConfiguration("AI secret store is required".to_owned())
        })?;
        let content_protector = self.content_protector.ok_or_else(|| {
            AiError::InvalidConfiguration("AI content protector is required".to_owned())
        })?;
        let content_protection_policy_resolver =
            self.content_protection_policy_resolver.ok_or_else(|| {
                AiError::InvalidConfiguration(
                    "AI content-protection policy resolver is required".to_owned(),
                )
            })?;

        let schema_module = AiSchemaModule;
        let catalog = SchemaModuleCatalog::compose(&[&schema_module as &dyn OrmSchemaModule])
            .map_err(|error| AiError::InvalidConfiguration(error.to_string()))?;
        let fingerprint = catalog
            .modules()
            .first()
            .ok_or_else(|| AiError::InvalidConfiguration("AI schema module is empty".to_owned()))?
            .fingerprint
            .clone();
        let tool_bridge = AuthenticatedToolBridge::new(
            principal_resolver.clone(),
            tool_authorization_policy,
            context_factory,
            graphql_executor,
            graphql_targets,
        );

        Ok(AiRuntime {
            principal_resolver,
            access_policy,
            tool_bridge,
            egress_policy,
            deployment_egress,
            maximum_tool_maturity,
            tool_catalog: self.tool_catalog,
            proposal_catalog: self.proposal_catalog,
            secret_store,
            content_protection_policy_resolver,
            content_protector,
            providers: self.providers,
            start_gate: AiRuntimeStartGate::new(fingerprint),
        })
    }
}
