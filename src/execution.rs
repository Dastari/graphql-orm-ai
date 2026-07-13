//! Authenticated application GraphQL execution contracts.

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use agql_auth::{CurrentPrincipalResolver, PrincipalReference, ResolvedPrincipal};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    AiRunId, AiScope, AiToolAuthorizationDecision, AiToolAuthorizationPolicy, AiToolCallId,
    AiToolDescriptor,
};

/// Stable deployment-owned logical GraphQL execution target identifier.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GraphqlExecutionTargetId(String);

impl GraphqlExecutionTargetId {
    /// Parses a non-secret logical target ID.
    ///
    /// # Errors
    ///
    /// Returns [`ToolExecutionError::InvalidTarget`] for an empty, overly
    /// long, or non-ASCII identifier.
    pub fn parse(value: impl Into<String>) -> Result<Self, ToolExecutionError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'_' | b'-')
            })
        {
            return Err(ToolExecutionError::InvalidTarget);
        }
        Ok(Self(value))
    }

    /// Returns the logical identifier without resolving a destination URL.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Deployment trust/routing class for authenticated GraphQL execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphqlExecutionTargetClass {
    /// Finished schema executing in the current process.
    Local,
    /// Private routed/composed GraphQL endpoint.
    PrivateRouted,
    /// Private direct service endpoint, disabled unless explicitly registered.
    PrivateDirect,
}

/// Non-secret deployment registration for one logical GraphQL destination.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphqlExecutionTarget {
    /// Stable logical ID used by server-owned tool descriptors.
    pub id: GraphqlExecutionTargetId,
    /// Local/routed/direct trust class.
    pub class: GraphqlExecutionTargetClass,
    /// Credential audience required for a remote target.
    pub audience: Option<String>,
    /// Resource type required for a remote target.
    pub resource_type: Option<String>,
    /// Resource identifier required for a remote target.
    pub resource_id: Option<String>,
    /// Exact compiled or registry schema fingerprint.
    pub schema_fingerprint: String,
}

impl GraphqlExecutionTarget {
    /// Validates a logical target without accepting or exposing a URL.
    ///
    /// # Errors
    ///
    /// Returns [`ToolExecutionError::InvalidTarget`] when a schema
    /// fingerprint is absent or a remote target lacks audience/resource
    /// binding.
    pub fn validate(&self) -> Result<(), ToolExecutionError> {
        if self.schema_fingerprint.trim().is_empty() {
            return Err(ToolExecutionError::InvalidTarget);
        }
        if self.class != GraphqlExecutionTargetClass::Local
            && (self.audience.as_deref().is_none_or(str::is_empty)
                || self.resource_type.as_deref().is_none_or(str::is_empty)
                || self.resource_id.as_deref().is_none_or(str::is_empty))
        {
            return Err(ToolExecutionError::InvalidTarget);
        }
        Ok(())
    }
}

/// Immutable deployment registry for logical GraphQL execution targets.
#[derive(Clone, Debug, Default)]
pub struct GraphqlExecutionTargetRegistry {
    targets: BTreeMap<GraphqlExecutionTargetId, GraphqlExecutionTarget>,
}

impl GraphqlExecutionTargetRegistry {
    /// Creates an empty registry. No target is implicitly trusted.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one validated logical target.
    ///
    /// # Errors
    ///
    /// Returns a safe error for invalid or duplicate target IDs.
    pub fn register(&mut self, target: GraphqlExecutionTarget) -> Result<(), ToolExecutionError> {
        target.validate()?;
        if self.targets.contains_key(&target.id) {
            return Err(ToolExecutionError::InvalidTarget);
        }
        self.targets.insert(target.id.clone(), target);
        Ok(())
    }

    /// Resolves a logical target without making its transport destination model-visible.
    pub fn target(&self, id: &GraphqlExecutionTargetId) -> Option<&GraphqlExecutionTarget> {
        self.targets.get(id)
    }

    fn validate_contract(
        &self,
        contract: &GraphqlOperationContract,
        document: &str,
    ) -> Result<&GraphqlExecutionTarget, ToolExecutionError> {
        let target = self
            .targets
            .get(&contract.target_id)
            .ok_or(ToolExecutionError::InvalidTarget)?;
        if target.schema_fingerprint != contract.schema_fingerprint
            || contract.document_hash != stable_document_hash(document)
            || contract.operation_name.trim().is_empty()
            || contract.result_projection_fingerprint.trim().is_empty()
            || contract.disclosure_schema_fingerprint.trim().is_empty()
        {
            return Err(ToolExecutionError::StaleContract);
        }
        Ok(target)
    }
}

/// Exact static operation binding carried to local or remote executors.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphqlOperationContract {
    /// Deployment-registered logical target.
    pub target_id: GraphqlExecutionTargetId,
    /// Exact target schema fingerprint reviewed with this operation.
    pub schema_fingerprint: String,
    /// Operation name inside the server-authored document.
    pub operation_name: String,
    /// Stable hash of the server-authored document.
    pub document_hash: String,
    /// Stable result-projection fingerprint.
    pub result_projection_fingerprint: String,
    /// Static disclosure-schema fingerprint.
    pub disclosure_schema_fingerprint: String,
}

impl GraphqlOperationContract {
    /// Binds a server-authored operation to its target/schema/projection contracts.
    ///
    /// # Errors
    ///
    /// Returns [`ToolExecutionError::StaleContract`] for missing contract data.
    pub fn new(
        target_id: GraphqlExecutionTargetId,
        schema_fingerprint: impl Into<String>,
        operation_name: impl Into<String>,
        document: &str,
        result_projection_fingerprint: impl Into<String>,
        disclosure_schema_fingerprint: impl Into<String>,
    ) -> Result<Self, ToolExecutionError> {
        let contract = Self {
            target_id,
            schema_fingerprint: schema_fingerprint.into(),
            operation_name: operation_name.into(),
            document_hash: stable_document_hash(document),
            result_projection_fingerprint: result_projection_fingerprint.into(),
            disclosure_schema_fingerprint: disclosure_schema_fingerprint.into(),
        };
        if contract.schema_fingerprint.trim().is_empty()
            || contract.operation_name.trim().is_empty()
            || document.trim().is_empty()
            || contract.result_projection_fingerprint.trim().is_empty()
            || contract.disclosure_schema_fingerprint.trim().is_empty()
        {
            return Err(ToolExecutionError::StaleContract);
        }
        Ok(contract)
    }
}

fn stable_document_hash(document: &str) -> String {
    use sha2::{Digest, Sha256};

    hex::encode(Sha256::digest(document.as_bytes()))
}

/// Invocation metadata linked into the host's normal audit context.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphqlInvocationContext {
    /// Run causing the operation.
    pub run_id: AiRunId,
    /// Tool call causing the operation.
    pub tool_call_id: AiToolCallId,
    /// Application scope in which tool policy and resolver authorization run.
    pub scope: AiScope,
    /// Correlation identifier shared with the outer AI audit.
    pub correlation_id: String,
    /// Causal command/event identifier propagated into application audit.
    pub causation_id: String,
    /// Safe delegation/grant reference; never a bearer credential.
    pub delegation_reference: Option<String>,
    /// Optional idempotency key for a descriptor proven idempotent.
    pub idempotency_key: Option<String>,
}

/// Opaque host request context produced through the same factory used by
/// ordinary GraphQL transports.
#[derive(Clone)]
pub struct GraphqlRequestContext {
    inner: Arc<dyn Any + Send + Sync>,
}

impl GraphqlRequestContext {
    /// Wraps a host-specific request context.
    pub fn new<T>(context: T) -> Self
    where
        T: Any + Send + Sync,
    {
        Self {
            inner: Arc::new(context),
        }
    }

    /// Downcasts to the host-specific context type.
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.inner.downcast_ref()
    }
}

/// Server-authored GraphQL operation request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolGraphqlRequest {
    /// Static server-authored operation document.
    pub document: String,
    /// Operation name.
    pub operation_name: String,
    /// Exact target/schema/document/projection/disclosure binding.
    pub contract: GraphqlOperationContract,
    /// Schema-validated variables.
    pub variables: serde_json::Value,
    /// Invocation/audit metadata.
    pub invocation: GraphqlInvocationContext,
}

/// Bounded normalized GraphQL result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolGraphqlResponse {
    /// Projected JSON result.
    pub data: serde_json::Value,
    /// Safe stable public error codes.
    pub error_codes: Vec<String>,
    /// Host application audit reference, when emitted.
    pub application_audit_ref: Option<String>,
}

/// Safe bridge error.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ToolExecutionError {
    /// Principal could not be rehydrated/currently authorized.
    #[error("tool principal reauthorization failed")]
    Reauthorization,
    /// Current host tool policy denied the exact registered request.
    #[error("tool authorization denied")]
    Authorization,
    /// Host request context could not be built.
    #[error("tool request context unavailable")]
    RequestContext,
    /// Static operation no longer validates against the host schema.
    #[error("tool operation contract is stale")]
    StaleContract,
    /// Logical target is absent, malformed, or not permitted by deployment registration.
    #[error("tool GraphQL execution target is invalid")]
    InvalidTarget,
    /// Host execution failed safely.
    #[error("tool GraphQL execution failed")]
    Execution,
}

/// Canonical host request-context factory shared with normal HTTP/WS paths.
#[async_trait]
pub trait GraphqlRequestContextFactory: Send + Sync {
    /// Builds the complete auth, DB-auth, loader, rate-limit, request, and audit
    /// envelope for an invocation.
    async fn build(
        &self,
        principal: &ResolvedPrincipal,
        target: &GraphqlExecutionTarget,
        invocation: &GraphqlInvocationContext,
    ) -> Result<GraphqlRequestContext, ToolExecutionError>;
}

/// Executes a server-authored operation against the composed host schema.
#[async_trait]
pub trait AuthenticatedGraphqlExecutor: Send + Sync {
    /// Executes with the canonical host request context.
    async fn execute(
        &self,
        context: GraphqlRequestContext,
        request: ToolGraphqlRequest,
    ) -> Result<ToolGraphqlResponse, ToolExecutionError>;
}

/// Security-preserving bridge that always rehydrates before constructing and
/// executing a tool request.
#[derive(Clone)]
pub struct AuthenticatedToolBridge {
    principal_resolver: Arc<dyn CurrentPrincipalResolver>,
    authorization_policy: Arc<dyn AiToolAuthorizationPolicy>,
    context_factory: Arc<dyn GraphqlRequestContextFactory>,
    executor: Arc<dyn AuthenticatedGraphqlExecutor>,
    targets: GraphqlExecutionTargetRegistry,
}

impl AuthenticatedToolBridge {
    /// Creates a bridge from host implementations.
    pub fn new(
        principal_resolver: Arc<dyn CurrentPrincipalResolver>,
        authorization_policy: Arc<dyn AiToolAuthorizationPolicy>,
        context_factory: Arc<dyn GraphqlRequestContextFactory>,
        executor: Arc<dyn AuthenticatedGraphqlExecutor>,
        targets: GraphqlExecutionTargetRegistry,
    ) -> Self {
        Self {
            principal_resolver,
            authorization_policy,
            context_factory,
            executor,
            targets,
        }
    }

    /// Rehydrates the principal, builds the canonical request envelope, and
    /// executes the static request.
    pub async fn execute(
        &self,
        principal_reference: &PrincipalReference,
        descriptor: &AiToolDescriptor,
        request: ToolGraphqlRequest,
    ) -> Result<(ToolGraphqlResponse, AiToolAuthorizationDecision), ToolExecutionError> {
        if request.operation_name != request.contract.operation_name {
            return Err(ToolExecutionError::StaleContract);
        }
        let target = self
            .targets
            .validate_contract(&request.contract, &request.document)?;
        let principal = self
            .principal_resolver
            .resolve(principal_reference)
            .await
            .map_err(|_| ToolExecutionError::Reauthorization)?;
        let authorization = self
            .authorization_policy
            .authorize(
                &principal,
                &request.invocation.scope,
                descriptor,
                &request.variables,
            )
            .await;
        if !authorization.is_complete_allow() {
            return Err(ToolExecutionError::Authorization);
        }
        let context = self
            .context_factory
            .build(&principal, target, &request.invocation)
            .await?;
        let response = self.executor.execute(context, request).await?;
        Ok((response, authorization))
    }

    /// Rehydrates and authorizes an exact registered request without invoking
    /// its resolver.
    ///
    /// This is only a current host tool-policy decision. It does not prove
    /// resolver authorization, unchanged application resources, approval, or
    /// successful execution.
    pub(crate) async fn preauthorize(
        &self,
        principal_reference: &PrincipalReference,
        descriptor: &AiToolDescriptor,
        request: &ToolGraphqlRequest,
    ) -> Result<(ResolvedPrincipal, AiToolAuthorizationDecision), ToolExecutionError> {
        if request.operation_name != request.contract.operation_name {
            return Err(ToolExecutionError::StaleContract);
        }
        self.targets
            .validate_contract(&request.contract, &request.document)?;
        let principal = self
            .principal_resolver
            .resolve(principal_reference)
            .await
            .map_err(|_| ToolExecutionError::Reauthorization)?;
        let authorization = self
            .authorization_policy
            .authorize(
                &principal,
                &request.invocation.scope,
                descriptor,
                &request.variables,
            )
            .await;
        if !authorization.is_complete_allow() {
            return Err(ToolExecutionError::Authorization);
        }
        Ok((principal, authorization))
    }

    /// Executes only when a newly recomputed host policy decision still
    /// matches the policy version and safe authorization-state digest bound to
    /// a consumed one-shot approval.
    pub(crate) async fn execute_bound(
        &self,
        principal_reference: &PrincipalReference,
        descriptor: &AiToolDescriptor,
        request: ToolGraphqlRequest,
        expected_policy_version: &str,
        expected_authorization_state_digest: &str,
    ) -> Result<(ToolGraphqlResponse, AiToolAuthorizationDecision), ToolExecutionError> {
        let (principal, authorization) = self
            .preauthorize(principal_reference, descriptor, &request)
            .await?;
        if authorization.policy_version != expected_policy_version
            || authorization.authorization_state_digest != expected_authorization_state_digest
        {
            return Err(ToolExecutionError::Authorization);
        }
        let target = self
            .targets
            .validate_contract(&request.contract, &request.document)?;
        let context = self
            .context_factory
            .build(&principal, target, &request.invocation)
            .await?;
        let response = self.executor.execute(context, request).await?;
        Ok((response, authorization))
    }
}
