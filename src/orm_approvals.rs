//! ORM-backed canonical-preview approval and one-shot consumption lifecycle.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::sync::Arc;

use agql_auth::{
    AuthPrincipal, Clock, CurrentPrincipalResolver, PrincipalReference, RecentMfaPolicy,
};
use async_trait::async_trait;
use graphql_orm::db::Database;
use graphql_orm::graphql::errors::{OrmErrorCode, OrmPublicError};
use graphql_orm::graphql::filters::{StringFilter, UuidFilter};
use graphql_orm::graphql::orm::{
    ConditionalUpdateOutcome, DefaultWriteBackend, TransactionError, TransactionMode,
};
use graphql_orm::graphql::pagination::{
    KeysetConnectionInput, KeysetWindowDirection, ValidatedKeysetConnection,
};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::orm_runs::{
    PreparedApprovalConsumption, PreparedApprovalRequest, PreparedApprovalWaitCancellation,
    PreparedApprovalWaitOutcome, PreparedApprovalWaitReconciliation, coordinator_checkpoint_hash,
    validate_worker_id,
};
use crate::persistence::*;
use crate::{
    AiApprovalAccessPolicy, AiApprovalAction, AiApprovalBinding, AiApprovalConnection,
    AiApprovalDecision, AiApprovalEdge, AiApprovalGrant, AiApprovalId, AiApprovalService,
    AiApprovalState, AiApprovalView, AiCanonicalActionPreview, AiContentProtectionPolicy,
    AiContentProtectionPolicyResolver, AiContentProtector, AiError, AiRunId, AiRunLease, AiScope,
    AiSessionId, AiSessionWakeup, AiToolCatalog, AiToolId, AiToolOperationDomain,
    AiToolOperationKind, AiToolRisk, ConsumedAiApproval, ContentProtectionContext,
    DecideAiApprovalInput, OrmAiRunService, ProtectedContentEnvelope, RevokeAiApprovalInput,
    ToolMaturity,
};

/// Deployment-owned approval freshness, lifetime, and preview bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiApprovalServiceLimits {
    maximum_principal_age: Duration,
    maximum_approval_lifetime: Duration,
}

impl AiApprovalServiceLimits {
    /// Creates validated approval-service limits.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless both durations are
    /// positive.
    pub fn new(
        maximum_principal_age: Duration,
        maximum_approval_lifetime: Duration,
    ) -> Result<Self, AiError> {
        if !maximum_principal_age.is_positive() || !maximum_approval_lifetime.is_positive() {
            return Err(AiError::InvalidConfiguration(
                "invalid approval-service limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_principal_age,
            maximum_approval_lifetime,
        })
    }
}

impl Default for AiApprovalServiceLimits {
    fn default() -> Self {
        Self {
            maximum_principal_age: Duration::seconds(60),
            maximum_approval_lifetime: Duration::hours(24),
        }
    }
}

/// Current deployment decision for one still-live approval wait.
///
/// This value proves only that the deployment's current wait policy was
/// evaluated. It is not approval authority and cannot authorize resolver or
/// provider execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiApprovalWaitPolicyDecision {
    continue_waiting: bool,
    policy_version: String,
}

impl AiApprovalWaitPolicyDecision {
    /// Allows the exact pending or approved wait to remain parked.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] for an empty, overlong, or
    /// control-character-bearing policy version.
    pub fn continue_waiting(policy_version: impl Into<String>) -> Result<Self, AiError> {
        Self::new(true, policy_version.into())
    }

    /// Cancels the parked wait under current deployment policy.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] for an empty, overlong, or
    /// control-character-bearing policy version.
    pub fn cancel(policy_version: impl Into<String>) -> Result<Self, AiError> {
        Self::new(false, policy_version.into())
    }

    /// Returns whether the wait may remain parked.
    pub const fn may_continue(&self) -> bool {
        self.continue_waiting
    }

    /// Returns the current deployment policy version.
    pub fn policy_version(&self) -> &str {
        &self.policy_version
    }

    fn new(continue_waiting: bool, policy_version: String) -> Result<Self, AiError> {
        if policy_version.trim().is_empty()
            || policy_version.len() > 1_024
            || policy_version.chars().any(char::is_control)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid approval-wait policy version".to_owned(),
            ));
        }
        Ok(Self {
            continue_waiting,
            policy_version,
        })
    }
}

/// Safe current context presented to approval-wait policy.
///
/// The context carries identifiers and durable state only. It contains no
/// approval preview, protected arguments, result content, or credential.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiApprovalWaitPolicyContext {
    scope: AiScope,
    session_id: AiSessionId,
    run_id: AiRunId,
    approval_id: AiApprovalId,
    approval_state: AiApprovalState,
}

impl AiApprovalWaitPolicyContext {
    /// Returns the exact scope of the parked run.
    pub fn scope(&self) -> &AiScope {
        &self.scope
    }

    /// Returns the owning session identifier.
    pub const fn session_id(&self) -> AiSessionId {
        self.session_id
    }

    /// Returns the parked run identifier.
    pub const fn run_id(&self) -> AiRunId {
        self.run_id
    }

    /// Returns the exact approval identifier.
    pub const fn approval_id(&self) -> AiApprovalId {
        self.approval_id
    }

    /// Returns the durable approval state observed by the worker.
    pub const fn approval_state(&self) -> AiApprovalState {
        self.approval_state
    }
}

/// Deployment-owned current policy for live approval waits.
///
/// Implementations must evaluate current scope and principal authority. A
/// positive result only keeps a wait parked; it never resumes, consumes, or
/// executes the approved action.
#[async_trait]
pub trait AiApprovalWaitReconciliationPolicy: Send + Sync {
    /// Evaluates whether one exact pending or approved wait may remain parked.
    async fn evaluate_wait(
        &self,
        principal: &AuthPrincipal,
        context: &AiApprovalWaitPolicyContext,
    ) -> Result<AiApprovalWaitPolicyDecision, AiError>;
}

/// Deployment bounds for one approval-wait reconciliation pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiApprovalWaitReconciliationLimits {
    maximum_principal_age: Duration,
    maximum_pending_duration: Duration,
    maximum_candidate_scan: usize,
}

impl AiApprovalWaitReconciliationLimits {
    /// Creates validated reconciliation bounds.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless both durations are
    /// positive and the scan bound is in `1..=256`.
    pub fn new(
        maximum_principal_age: Duration,
        maximum_pending_duration: Duration,
        maximum_candidate_scan: usize,
    ) -> Result<Self, AiError> {
        if !maximum_principal_age.is_positive()
            || !maximum_pending_duration.is_positive()
            || !(1..=256).contains(&maximum_candidate_scan)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid approval-wait reconciliation limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_principal_age,
            maximum_pending_duration,
            maximum_candidate_scan,
        })
    }

    /// Returns the maximum accepted age of a rehydrated principal.
    pub const fn maximum_principal_age(&self) -> Duration {
        self.maximum_principal_age
    }

    /// Returns the hard deployment cutoff for a parked wait.
    pub const fn maximum_pending_duration(&self) -> Duration {
        self.maximum_pending_duration
    }

    /// Returns the maximum runs inspected by one pass.
    pub const fn maximum_candidate_scan(&self) -> usize {
        self.maximum_candidate_scan
    }
}

impl Default for AiApprovalWaitReconciliationLimits {
    fn default() -> Self {
        Self {
            maximum_principal_age: Duration::seconds(60),
            maximum_pending_duration: Duration::hours(24),
            maximum_candidate_scan: 64,
        }
    }
}

/// Bounded outcome counts from one approval-wait reconciliation pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct AiApprovalWaitReconciliationReport {
    /// Runs cancelled after explicit denial.
    pub cancelled_denied: usize,
    /// Runs cancelled after explicit revocation.
    pub cancelled_revoked: usize,
    /// Runs cancelled after approval or deployment expiry.
    pub cancelled_expired: usize,
    /// Runs cancelled by current deployment policy.
    pub cancelled_policy: usize,
    /// Malformed waits closed for operator recovery.
    pub recovery_required: usize,
    /// Current valid waits left parked.
    pub still_waiting: usize,
    /// Candidates concurrently changed before the transactional decision.
    pub raced: usize,
}

/// Result of parking a fenced run on one exact approval.
#[derive(Clone, Debug)]
pub struct AiRequestedApproval {
    approval_id: AiApprovalId,
    lease: AiRunLease,
}

impl AiRequestedApproval {
    /// Pending approval identifier.
    pub const fn approval_id(&self) -> AiApprovalId {
        self.approval_id
    }

    /// Renewed waiting-approval lease.
    pub fn lease(&self) -> &AiRunLease {
        &self.lease
    }

    /// Consumes the result and returns the renewed lease.
    pub fn into_lease(self) -> AiRunLease {
        self.lease
    }
}

/// Result proving one-shot approval consumption and the renewed running fence.
#[derive(Clone, Debug)]
pub struct AiConsumedApproval {
    approval: ConsumedAiApproval,
    lease: AiRunLease,
}

impl AiConsumedApproval {
    /// Opaque exact consumption proof.
    pub fn approval(&self) -> &ConsumedAiApproval {
        &self.approval
    }

    /// Renewed running lease.
    pub fn lease(&self) -> &AiRunLease {
        &self.lease
    }

    /// Consumes the result into its proof and lease.
    pub fn into_parts(self) -> (ConsumedAiApproval, AiRunLease) {
        (self.approval, self.lease)
    }
}

/// Protected approval lifecycle using generated ORM APIs only.
#[derive(Clone)]
pub struct OrmAiApprovalService {
    database: Database<DefaultWriteBackend>,
    run_service: OrmAiRunService,
    principal_resolver: Arc<dyn CurrentPrincipalResolver>,
    access_policy: Arc<dyn AiApprovalAccessPolicy>,
    tool_catalog: Arc<AiToolCatalog>,
    recent_mfa_policy: RecentMfaPolicy,
    protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
    content_protector: Arc<dyn AiContentProtector>,
    clock: Arc<dyn Clock>,
    limits: AiApprovalServiceLimits,
}

impl OrmAiApprovalService {
    /// Creates a fail-closed approval lifecycle.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database: Database<DefaultWriteBackend>,
        run_service: OrmAiRunService,
        principal_resolver: Arc<dyn CurrentPrincipalResolver>,
        access_policy: Arc<dyn AiApprovalAccessPolicy>,
        tool_catalog: Arc<AiToolCatalog>,
        recent_mfa_policy: RecentMfaPolicy,
        protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
        content_protector: Arc<dyn AiContentProtector>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            database,
            run_service,
            principal_resolver,
            access_policy,
            tool_catalog,
            recent_mfa_policy,
            protection_policy,
            content_protector,
            clock,
            limits: AiApprovalServiceLimits::default(),
        }
    }

    /// Overrides deployment-owned hard limits.
    #[must_use]
    pub fn with_limits(mut self, limits: AiApprovalServiceLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Returns the ORM database handle for host schema wiring.
    pub fn database(&self) -> &Database<DefaultWriteBackend> {
        &self.database
    }

    /// Persists a pending approval and atomically parks the exact current run
    /// and tool call in `WaitingApproval`.
    ///
    /// The preview and binding must be rebuilt by server-owned code. Model
    /// prose is never an approval preview.
    ///
    /// # Errors
    ///
    /// Fails closed for a stale principal/fence, mismatched binding/tool call,
    /// invalid expiry, denied access, unavailable protection, or persistence
    /// ambiguity.
    pub async fn request_approval(
        &self,
        lease: &AiRunLease,
        binding: AiApprovalBinding,
        preview: AiCanonicalActionPreview,
        expires_at: OffsetDateTime,
        recent_mfa_required: bool,
    ) -> Result<AiRequestedApproval, AiError> {
        binding.validate(&preview)?;
        if binding.session_id != lease.session_id()
            || binding.principal_reference_fingerprint
                != AiApprovalBinding::principal_fingerprint(lease.principal_reference())
        {
            return Err(AiError::Conflict);
        }
        validate_binding_fields(&binding)?;
        let now = canonical_second(self.clock.now());
        let expires_at = canonical_second(expires_at);
        if expires_at <= now || expires_at - now > self.limits.maximum_approval_lifetime {
            return Err(AiError::InvalidInput(
                "approval expiry is invalid".to_owned(),
            ));
        }
        let resolved = self.resolve_current(lease.principal_reference()).await?;
        self.require_registered_binding(lease, &binding, "executing")
            .await?;
        if !self
            .access_policy
            .can_access_approval(
                resolved.principal(),
                &binding.scope,
                binding.session_id,
                AiApprovalAction::Request,
            )
            .await
        {
            return Err(AiError::Forbidden);
        }
        let protection = self
            .protection_policy(resolved.principal(), &binding.scope)
            .await?;
        let approval_id = AiApprovalId::new();
        let canonical_resources = canonical_resources(&binding.resources);
        let canonical_preview = canonical_preview(&preview);
        let protected_resources = self
            .protect_value(
                &protection,
                content_context(
                    "graphql_orm_ai_approvals",
                    approval_id.0,
                    "protected_resource_bindings",
                    &binding.scope,
                ),
                serde_json::to_value(&canonical_resources)
                    .map_err(|_| AiError::PersistenceFailed)?,
            )
            .await?;
        let protected_preview = self
            .protect_value(
                &protection,
                content_context(
                    "graphql_orm_ai_approvals",
                    approval_id.0,
                    "protected_action_preview",
                    &binding.scope,
                ),
                serde_json::to_value(&canonical_preview).map_err(|_| AiError::PersistenceFailed)?,
            )
            .await?;
        let event_id = Uuid::new_v4();
        let protected_event = self
            .protect_value(
                &protection,
                content_context(
                    "graphql_orm_ai_session_events",
                    event_id,
                    "protected_payload",
                    &binding.scope,
                ),
                serde_json::json!({
                    "approvalId": approval_id.0,
                    "toolCallId": binding.tool_call_id.0,
                    "state": "pending",
                    "previewHash": binding.preview_hash
                }),
            )
            .await?;
        let (owner_kind, owner_subject) = principal_identity(resolved.principal());
        let binding_hash = binding.stable_hash();
        let prepared = PreparedApprovalRequest {
            id: approval_id.0,
            tool_call_id: binding.tool_call_id.0,
            principal_subject: resolved.principal().subject().to_owned(),
            principal_reference_fingerprint: binding.principal_reference_fingerprint,
            delegated_actor_subject: binding.delegated_actor_subject,
            delegation_reference: binding.delegation_reference,
            argument_hash: binding.argument_hash,
            tool_fingerprint: binding.tool_fingerprint,
            binding_hash,
            execution_target_id: binding.operation.target_id.as_str().to_owned(),
            target_schema_fingerprint: binding.operation.schema_fingerprint,
            operation_name: binding.operation.operation_name,
            operation_document_hash: binding.operation.document_hash,
            result_projection_fingerprint: binding.operation.result_projection_fingerprint,
            disclosure_schema_fingerprint: binding.operation.disclosure_schema_fingerprint,
            policy_version: binding.policy_version,
            authorization_state_digest: binding.authorization_state_digest,
            protected_resource_bindings: protected_resources,
            protected_action_preview: protected_preview,
            action_preview_hash: binding.preview_hash,
            recent_mfa_required,
            expires_at: expires_at.unix_timestamp(),
            event_id,
            protected_event,
            correlation_id: approval_id.0.to_string(),
            expected_owner_principal_kind: owner_kind,
            expected_owner_subject: owner_subject.to_owned(),
            expected_scope_kind: binding.scope.kind,
            expected_scope_id: binding.scope.id,
            expected_tenant_id: binding.scope.tenant_id,
        };
        let lease = self.run_service.request_approval(lease, prepared).await?;
        Ok(AiRequestedApproval { approval_id, lease })
    }

    /// Atomically consumes an approved grant and returns the run to `Running`.
    ///
    /// The caller must build the binding and canonical preview again from
    /// current server-owned policy/resource state. After this call, ordinary
    /// resolver authorization must still run freshly before any side effect.
    ///
    /// # Errors
    ///
    /// Fails closed for expired/revoked/mismatched/already-consumed approval,
    /// stale principal/fence, changed policy/resource/preview binding, denied
    /// access, unavailable protection, or persistence ambiguity.
    pub async fn consume_approval(
        &self,
        lease: &AiRunLease,
        approval_id: AiApprovalId,
        binding: &AiApprovalBinding,
        preview: &AiCanonicalActionPreview,
    ) -> Result<AiConsumedApproval, AiError> {
        binding.validate(preview)?;
        validate_binding_fields(binding)?;
        if binding.session_id != lease.session_id()
            || binding.principal_reference_fingerprint
                != AiApprovalBinding::principal_fingerprint(lease.principal_reference())
        {
            return Err(AiError::Conflict);
        }
        let now = canonical_second(self.clock.now());
        let resolved = self.resolve_current(lease.principal_reference()).await?;
        self.require_registered_binding(lease, binding, "waiting_approval")
            .await?;
        if !self
            .access_policy
            .can_access_approval(
                resolved.principal(),
                &binding.scope,
                binding.session_id,
                AiApprovalAction::Consume,
            )
            .await
        {
            return Err(AiError::Forbidden);
        }
        let record = AiApprovalRecord::find_by_id(&self.database, &approval_id.0)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
            .ok_or(AiError::NotFound)?;
        if !record_matches_binding(&record, binding, preview)
            || record.principal_subject != resolved.principal().subject()
        {
            return Err(AiError::Forbidden);
        }
        let decided_at = record.decided_at.ok_or(AiError::Forbidden)?;
        let grant = AiApprovalGrant {
            id: approval_id,
            binding_hash: record.binding_hash.clone(),
            approver_subject: record.approver_subject.clone().ok_or(AiError::Forbidden)?,
            state: parse_state(&record.state)?,
            approved_at: OffsetDateTime::from_unix_timestamp(decided_at)
                .map_err(|_| AiError::PersistenceFailed)?,
            expires_at: OffsetDateTime::from_unix_timestamp(record.expires_at)
                .map_err(|_| AiError::PersistenceFailed)?,
        };
        let authorized = grant.authorize(binding, now)?;
        let protection = self
            .protection_policy(resolved.principal(), &binding.scope)
            .await?;
        let stored_preview = self
            .open_value(
                &protection,
                content_context(
                    "graphql_orm_ai_approvals",
                    approval_id.0,
                    "protected_action_preview",
                    &binding.scope,
                ),
                &record.protected_action_preview,
            )
            .await?;
        let stored_resources = self
            .open_value(
                &protection,
                content_context(
                    "graphql_orm_ai_approvals",
                    approval_id.0,
                    "protected_resource_bindings",
                    &binding.scope,
                ),
                &record.protected_resource_bindings,
            )
            .await?;
        if stored_preview
            != serde_json::to_value(canonical_preview(preview))
                .map_err(|_| AiError::PersistenceFailed)?
            || stored_resources
                != serde_json::to_value(canonical_resources(&binding.resources))
                    .map_err(|_| AiError::PersistenceFailed)?
        {
            return Err(AiError::Forbidden);
        }
        let event_id = Uuid::new_v4();
        let protected_event = self
            .protect_value(
                &protection,
                content_context(
                    "graphql_orm_ai_session_events",
                    event_id,
                    "protected_payload",
                    &binding.scope,
                ),
                serde_json::json!({
                    "approvalId": approval_id.0,
                    "toolCallId": binding.tool_call_id.0,
                    "state": "consumed"
                }),
            )
            .await?;
        let (owner_kind, owner_subject) = principal_identity(resolved.principal());
        let prepared = PreparedApprovalConsumption {
            approval_id: approval_id.0,
            tool_call_id: binding.tool_call_id.0,
            binding_hash: binding.stable_hash(),
            expected_approval_version: record.row_version,
            event_id,
            protected_event,
            correlation_id: approval_id.0.to_string(),
            expected_owner_principal_kind: owner_kind,
            expected_owner_subject: owner_subject.to_owned(),
            expected_scope_kind: binding.scope.kind.clone(),
            expected_scope_id: binding.scope.id.clone(),
            expected_tenant_id: binding.scope.tenant_id.clone(),
        };
        let lease = self.run_service.consume_approval(lease, prepared).await?;
        Ok(AiConsumedApproval {
            approval: ConsumedAiApproval::new(authorized, now),
            lease,
        })
    }

    async fn resolve_current(
        &self,
        reference: &PrincipalReference,
    ) -> Result<agql_auth::ResolvedPrincipal, AiError> {
        let resolved = self
            .principal_resolver
            .resolve(reference)
            .await
            .map_err(|_| AiError::ReauthorizationFailed)?;
        let checked_at = self.clock.now();
        if resolved.resolved_at() > checked_at
            || checked_at - resolved.resolved_at() > self.limits.maximum_principal_age
            || resolved.reference() != reference
        {
            return Err(AiError::ReauthorizationFailed);
        }
        Ok(resolved)
    }

    async fn require_registered_binding(
        &self,
        lease: &AiRunLease,
        binding: &AiApprovalBinding,
        expected_state: &str,
    ) -> Result<(), AiError> {
        let call = AiToolCallRecord::find_by_id(&self.database, &binding.tool_call_id.0)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
            .ok_or(AiError::NotFound)?;
        let tool_id = AiToolId::parse(call.tool_id.clone())?;
        let descriptor = self
            .tool_catalog
            .descriptor(&tool_id)
            .ok_or(AiError::Forbidden)?;
        if call.run_id != lease.run_id().0
            || call.lease_generation != lease.lease_generation()
            || call.state != expected_state
            || call.argument_hash != binding.argument_hash
            || call.tool_fingerprint != binding.tool_fingerprint
            || descriptor.fingerprint != binding.tool_fingerprint
            || descriptor.graphql_contract.as_ref() != Some(&binding.operation)
            || descriptor.operation_kind != AiToolOperationKind::Mutation
            || descriptor.operation_domain != AiToolOperationDomain::Application
            || descriptor.maturity != ToolMaturity::SupervisedWrite
            || descriptor.approval != crate::AiApprovalRule::OneShot
            || matches!(
                descriptor.risk,
                AiToolRisk::ReadOnly | AiToolRisk::Proposal | AiToolRisk::Secret
            )
        {
            return Err(AiError::Forbidden);
        }
        Ok(())
    }

    async fn current_request_principal(
        &self,
        principal: &AuthPrincipal,
    ) -> Result<AuthPrincipal, AiError> {
        Ok(self
            .resolve_current(&principal.reference())
            .await?
            .into_principal())
    }

    fn require_recent_mfa(&self, principal: &AuthPrincipal) -> Result<(), AiError> {
        let user = principal.as_user().ok_or(AiError::RecentMfaRequired)?;
        self.recent_mfa_policy
            .evaluate(user, self.clock.as_ref())
            .map_err(|_| AiError::RecentMfaRequired)
    }

    async fn protection_policy(
        &self,
        principal: &AuthPrincipal,
        scope: &AiScope,
    ) -> Result<AiContentProtectionPolicy, AiError> {
        let policy = self.protection_policy.resolve(principal, scope).await?;
        if !policy.ready || policy.scope != *scope {
            return Err(AiError::RuntimeNotReady);
        }
        Ok(policy)
    }

    async fn protect_value(
        &self,
        policy: &AiContentProtectionPolicy,
        context: ContentProtectionContext,
        value: serde_json::Value,
    ) -> Result<serde_json::Value, AiError> {
        let envelope = self
            .content_protector
            .protect(policy, &context, value)
            .await
            .map_err(map_protection)?;
        serde_json::to_value(envelope).map_err(|_| AiError::PersistenceFailed)
    }

    async fn open_value(
        &self,
        policy: &AiContentProtectionPolicy,
        context: ContentProtectionContext,
        value: &serde_json::Value,
    ) -> Result<serde_json::Value, AiError> {
        let envelope: ProtectedContentEnvelope =
            serde_json::from_value(value.clone()).map_err(|_| AiError::PersistenceFailed)?;
        self.content_protector
            .open(policy, &context, &envelope)
            .await
            .map_err(map_protection)
    }

    async fn visible_context(
        &self,
        principal: &AuthPrincipal,
        approval: &AiApprovalRecord,
        action: AiApprovalAction,
    ) -> Result<(AiScope, AiContentProtectionPolicy), AiError> {
        let session = AiSessionRecord::find_by_id(&self.database, &approval.session_id)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
            .ok_or(AiError::NotFound)?;
        if session.state == "deleting" || session.deleted_at.is_some() {
            return Err(AiError::NotFound);
        }
        let scope = record_scope(&session);
        if !self
            .access_policy
            .can_access_approval(principal, &scope, AiSessionId(session.id), action)
            .await
        {
            return Err(AiError::NotFound);
        }
        let policy = self.protection_policy(principal, &scope).await?;
        Ok((scope, policy))
    }

    async fn view(
        &self,
        principal: &AuthPrincipal,
        record: &AiApprovalRecord,
    ) -> Result<AiApprovalView, AiError> {
        let (scope, policy) = self
            .visible_context(principal, record, AiApprovalAction::Read)
            .await?;
        let preview = self
            .open_value(
                &policy,
                content_context(
                    "graphql_orm_ai_approvals",
                    record.id,
                    "protected_action_preview",
                    &scope,
                ),
                &record.protected_action_preview,
            )
            .await?;
        let state = if matches!(
            record.state.as_str(),
            "pending" | "approved" | "resume_claimed"
        ) && record.expires_at <= self.clock.now().unix_timestamp()
        {
            "expired".to_owned()
        } else {
            record.state.clone()
        };
        Ok(AiApprovalView {
            id: record.id,
            tool_call_id: record.tool_call_id,
            session_id: record.session_id,
            canonical_preview: async_graphql::Json(preview),
            state,
            recent_mfa_required: record.recent_mfa_required,
            approver_subject: record.approver_subject.clone(),
            created_at: record.created_at,
            expires_at: record.expires_at,
            decided_at: record.decided_at,
            consumed_at: record.consumed_at,
            row_version: record.row_version,
        })
    }

    async fn transition_decision(
        &self,
        principal: &AuthPrincipal,
        id: Uuid,
        expected_version: i64,
        action: AiApprovalAction,
        expected_states: &'static [&'static str],
        next_state: &'static str,
    ) -> Result<AiApprovalView, AiError> {
        if expected_version < 0 {
            return Err(AiError::InvalidInput(
                "invalid approval decision version".to_owned(),
            ));
        }
        let principal = self.current_request_principal(principal).await?;
        let current = AiApprovalRecord::find_by_id(&self.database, &id)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
            .ok_or(AiError::NotFound)?;
        let (scope, policy) = self.visible_context(&principal, &current, action).await?;
        if current.recent_mfa_required {
            self.require_recent_mfa(&principal)?;
        }
        let now = canonical_second(self.clock.now());
        if current.row_version != expected_version
            || !expected_states.contains(&current.state.as_str())
            || current.expires_at <= now.unix_timestamp()
        {
            return Err(AiError::Conflict);
        }
        let event_id = Uuid::new_v4();
        let actor_subject = principal.subject().to_owned();
        let protected_event = self
            .protect_value(
                &policy,
                content_context(
                    "graphql_orm_ai_session_events",
                    event_id,
                    "protected_payload",
                    &scope,
                ),
                serde_json::json!({
                    "approvalId": id,
                    "state": next_state,
                    "actorSubject": actor_subject
                }),
            )
            .await?;
        let record_decision = expected_states.contains(&"pending");
        let event_type = if next_state == "revoked" {
            "approval_revoked"
        } else {
            "approval_decided"
        };
        let updated = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = tx
                        .find_by_id::<AiApprovalRecord>(&id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if current.row_version != expected_version
                        || !expected_states.contains(&current.state.as_str())
                        || current.expires_at <= now.unix_timestamp()
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&current.session_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    let event_sequence = session
                        .stream_head
                        .checked_add(1)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let session_update = tx
                        .compare_and_swap::<AiSessionRecord>(
                            &session.id,
                            session.row_version,
                            AiSessionRecordWhereInput::default(),
                            UpdateAiSessionRecordInput {
                                stream_head: Some(event_sequence),
                                last_activity_at: Some(now.unix_timestamp()),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(session_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let approval_update = tx
                        .compare_and_swap::<AiApprovalRecord>(
                            &current.id,
                            current.row_version,
                            AiApprovalRecordWhereInput::default(),
                            UpdateAiApprovalRecordInput {
                                state: Some(next_state.to_owned()),
                                approver_subject: record_decision
                                    .then_some(Some(actor_subject.clone())),
                                decided_at: record_decision.then_some(Some(now.unix_timestamp())),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    let updated = match approval_update {
                        ConditionalUpdateOutcome::Updated(updated) => updated,
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    tx.insert::<AiSessionEventRecord>(CreateAiSessionEventRecordInput {
                        id: event_id,
                        session_id: session.id,
                        sequence: event_sequence,
                        event_type: event_type.to_owned(),
                        run_id: None,
                        causation_id: Some(id.to_string()),
                        correlation_id: id.to_string(),
                        protected_payload: protected_event,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.queue_event(AiSessionWakeup {
                        session_id: session.id,
                        sequence: event_sequence,
                    });
                    Ok(updated)
                })
            })
            .await
            .map_err(map_transaction)?;
        self.view(&principal, &updated).await
    }
}

#[async_trait]
impl AiApprovalService for OrmAiApprovalService {
    async fn approvals(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
        page: ValidatedKeysetConnection,
    ) -> Result<AiApprovalConnection, AiError> {
        let principal = self.current_request_principal(principal).await?;
        let session = AiSessionRecord::find_by_id(&self.database, &session_id.0)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
            .ok_or(AiError::NotFound)?;
        if session.state == "deleting" || session.deleted_at.is_some() {
            return Err(AiError::NotFound);
        }
        let scope = record_scope(&session);
        if !self
            .access_policy
            .can_access_approval(&principal, &scope, session_id, AiApprovalAction::Read)
            .await
        {
            return Err(AiError::NotFound);
        }
        let connection = AiApprovalRecord::keyset_connection_page(
            &self.database,
            AiApprovalRecordWhereInput {
                session_id: Some(UuidFilter {
                    eq: Some(session_id.0),
                    ..Default::default()
                }),
                ..Default::default()
            },
            page_input(&page),
        )
        .await
        .map_err(map_orm)?;
        let mut edges = Vec::with_capacity(connection.edges.len());
        for edge in connection.edges {
            edges.push(AiApprovalEdge {
                node: self.view(&principal, &edge.node).await?,
                cursor: edge.cursor,
            });
        }
        let mut page_info = connection.page_info;
        page_info.total_count = None;
        Ok(AiApprovalConnection { edges, page_info })
    }

    async fn approval(
        &self,
        principal: &AuthPrincipal,
        approval_id: AiApprovalId,
    ) -> Result<Option<AiApprovalView>, AiError> {
        let principal = self.current_request_principal(principal).await?;
        let Some(record) = AiApprovalRecord::find_by_id(&self.database, &approval_id.0)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
        else {
            return Ok(None);
        };
        match self.view(&principal, &record).await {
            Ok(view) => Ok(Some(view)),
            Err(AiError::NotFound | AiError::Forbidden) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn decide_approval(
        &self,
        principal: &AuthPrincipal,
        input: DecideAiApprovalInput,
    ) -> Result<AiApprovalView, AiError> {
        let next_state = match input.decision {
            AiApprovalDecision::Approve => "approved",
            AiApprovalDecision::Deny => "denied",
        };
        self.transition_decision(
            principal,
            input.id,
            input.expected_version,
            AiApprovalAction::Decide,
            &["pending"],
            next_state,
        )
        .await
    }

    async fn revoke_approval(
        &self,
        principal: &AuthPrincipal,
        input: RevokeAiApprovalInput,
    ) -> Result<AiApprovalView, AiError> {
        self.transition_decision(
            principal,
            input.id,
            input.expected_version,
            AiApprovalAction::Revoke,
            &["approved", "resume_claimed"],
            "revoked",
        )
        .await
    }
}

/// ORM-backed bounded reconciler for live runs parked on approval.
///
/// The worker rehydrates the current principal and current deployment wait
/// policy before retaining a pending or approved wait. It can only leave the
/// wait parked, cancel it, or close malformed linkage as `RecoveryRequired`.
/// It never claims, consumes, resumes, or executes an approval.
#[derive(Clone)]
pub struct OrmAiApprovalWaitReconciliationService {
    database: Database<DefaultWriteBackend>,
    run_service: OrmAiRunService,
    principal_resolver: Arc<dyn CurrentPrincipalResolver>,
    wait_policy: Arc<dyn AiApprovalWaitReconciliationPolicy>,
    protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
    content_protector: Arc<dyn AiContentProtector>,
    clock: Arc<dyn Clock>,
    limits: AiApprovalWaitReconciliationLimits,
}

impl OrmAiApprovalWaitReconciliationService {
    /// Creates a fail-closed live approval-wait reconciler.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database: Database<DefaultWriteBackend>,
        run_service: OrmAiRunService,
        principal_resolver: Arc<dyn CurrentPrincipalResolver>,
        wait_policy: Arc<dyn AiApprovalWaitReconciliationPolicy>,
        protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
        content_protector: Arc<dyn AiContentProtector>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            database,
            run_service,
            principal_resolver,
            wait_policy,
            protection_policy,
            content_protector,
            clock,
            limits: AiApprovalWaitReconciliationLimits::default(),
        }
    }

    /// Overrides deployment-owned hard limits.
    #[must_use]
    pub fn with_limits(mut self, limits: AiApprovalWaitReconciliationLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Returns the ORM database handle for host schema wiring.
    pub fn database(&self) -> &Database<DefaultWriteBackend> {
        &self.database
    }

    /// Reconciles one bounded window of live `WaitingApproval` runs.
    ///
    /// Run this pass before generic expired-lease recovery. Restored waits are
    /// deliberately excluded because restore reconciliation already closes
    /// them as `RecoveryRequired`.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid worker ID, stale or mismatched current
    /// principal, unavailable content protection, malformed ownership state,
    /// policy failure, or persistence ambiguity. Individual CAS races are
    /// counted and do not fail the bounded pass.
    pub async fn reconcile_waits(
        &self,
        worker_id: &str,
    ) -> Result<AiApprovalWaitReconciliationReport, AiError> {
        validate_worker_id(worker_id)?;
        let now = canonical_second(self.clock.now());
        let candidates = self.load_candidates().await?;
        let mut report = AiApprovalWaitReconciliationReport::default();
        for run in candidates {
            let snapshot = self.load_snapshot(run).await?;
            let session = snapshot
                .session
                .as_ref()
                .ok_or(AiError::PersistenceFailed)?;
            let reference: PrincipalReference =
                serde_json::from_value(snapshot.run.principal_reference.clone())
                    .map_err(|_| AiError::PersistenceFailed)?;
            let resolved = self.resolve_current(&reference).await?;
            let (owner_kind, owner_subject) = principal_identity(resolved.principal());
            if session.id != snapshot.run.session_id
                || session.owner_principal_kind != owner_kind
                || session.owner_subject != owner_subject
            {
                return Err(AiError::PersistenceFailed);
            }
            let scope = record_scope(session);
            let protection = self
                .current_protection_policy(resolved.principal(), &scope)
                .await?;
            let linkage_valid = approval_wait_linkage_is_valid(&snapshot, &reference);
            let approval_state = snapshot
                .approval
                .as_ref()
                .and_then(|approval| parse_state(&approval.state).ok());

            let (outcome, outcome_code, policy_version, report_kind) =
                if !linkage_valid || approval_state.is_none() {
                    (
                        PreparedApprovalWaitOutcome::RecoveryRequired {
                            approval_id: snapshot.approval.as_ref().map(|approval| approval.id),
                            tool_call_id: snapshot.call.as_ref().map(|call| call.id),
                        },
                        "approval_wait_linkage_invalid",
                        None,
                        ApprovalWaitReportKind::RecoveryRequired,
                    )
                } else {
                    let approval = snapshot
                        .approval
                        .as_ref()
                        .expect("valid approval linkage has an approval");
                    let state = approval_state.unwrap_or(AiApprovalState::Consumed);
                    let (reason, call_state, next_approval_state, policy_version, report_kind) =
                        match state {
                            AiApprovalState::Denied => (
                                "approval_denied",
                                "approval_denied",
                                None,
                                None,
                                ApprovalWaitReportKind::Denied,
                            ),
                            AiApprovalState::Revoked => (
                                "approval_revoked",
                                "approval_revoked",
                                None,
                                None,
                                ApprovalWaitReportKind::Revoked,
                            ),
                            AiApprovalState::Expired => (
                                "approval_expired",
                                "approval_expired",
                                None,
                                None,
                                ApprovalWaitReportKind::Expired,
                            ),
                            AiApprovalState::Pending | AiApprovalState::Approved
                                if session.state != "active" || session.deleted_at.is_some() =>
                            {
                                (
                                    "approval_wait_session_unavailable",
                                    "approval_expired",
                                    Some("expired".to_owned()),
                                    None,
                                    ApprovalWaitReportKind::Expired,
                                )
                            }
                            AiApprovalState::Pending | AiApprovalState::Approved
                                if approval.expires_at <= now.unix_timestamp() =>
                            {
                                (
                                    "approval_expired",
                                    "approval_expired",
                                    Some("expired".to_owned()),
                                    None,
                                    ApprovalWaitReportKind::Expired,
                                )
                            }
                            AiApprovalState::Pending | AiApprovalState::Approved
                                if approval_wait_cutoff_reached(
                                    approval.created_at,
                                    now,
                                    self.limits.maximum_pending_duration,
                                ) =>
                            {
                                (
                                    "approval_wait_cutoff",
                                    "approval_expired",
                                    Some("expired".to_owned()),
                                    None,
                                    ApprovalWaitReportKind::Expired,
                                )
                            }
                            AiApprovalState::Pending | AiApprovalState::Approved => {
                                let context = AiApprovalWaitPolicyContext {
                                    scope: scope.clone(),
                                    session_id: AiSessionId(session.id),
                                    run_id: AiRunId(snapshot.run.id),
                                    approval_id: AiApprovalId(approval.id),
                                    approval_state: state,
                                };
                                let decision = self
                                    .wait_policy
                                    .evaluate_wait(resolved.principal(), &context)
                                    .await?;
                                if decision.may_continue() {
                                    report.still_waiting += 1;
                                    continue;
                                }
                                (
                                    "approval_wait_policy_cancelled",
                                    "approval_expired",
                                    Some("expired".to_owned()),
                                    Some(decision.policy_version().to_owned()),
                                    ApprovalWaitReportKind::Policy,
                                )
                            }
                            AiApprovalState::ResumeClaimed | AiApprovalState::Consumed => (
                                "approval_wait_linkage_invalid",
                                "approval_expired",
                                None,
                                None,
                                ApprovalWaitReportKind::RecoveryRequired,
                            ),
                        };
                    if report_kind == ApprovalWaitReportKind::RecoveryRequired {
                        (
                            PreparedApprovalWaitOutcome::RecoveryRequired {
                                approval_id: Some(approval.id),
                                tool_call_id: snapshot.call.as_ref().map(|call| call.id),
                            },
                            reason,
                            policy_version,
                            report_kind,
                        )
                    } else {
                        (
                            PreparedApprovalWaitOutcome::Cancelled(Box::new(
                                PreparedApprovalWaitCancellation {
                                    call: snapshot
                                        .call
                                        .clone()
                                        .expect("valid approval linkage has a tool call"),
                                    step: snapshot
                                        .step
                                        .clone()
                                        .expect("valid approval linkage has a run step"),
                                    approval: approval.clone(),
                                    checkpoint: snapshot
                                        .checkpoint
                                        .clone()
                                        .expect("valid approval linkage has a checkpoint"),
                                    next_approval_state,
                                    call_state: call_state.to_owned(),
                                },
                            )),
                            reason,
                            policy_version,
                            report_kind,
                        )
                    }
                };

            let event_id = Uuid::new_v4();
            let protected_event = self
                .protect_event(
                    &protection,
                    event_id,
                    &scope,
                    serde_json::json!({
                        "runId": snapshot.run.id,
                        "approvalId": snapshot.approval.as_ref().map(|approval| approval.id),
                        "toolCallId": snapshot.call.as_ref().map(|call| call.id),
                        "outcomeCode": outcome_code
                    }),
                )
                .await?;
            let prepared = PreparedApprovalWaitReconciliation {
                expected_run: snapshot.run,
                expected_owner_principal_kind: owner_kind,
                expected_owner_subject: owner_subject.to_owned(),
                expected_scope_kind: scope.kind,
                expected_scope_id: scope.id,
                expected_tenant_id: scope.tenant_id,
                outcome,
                outcome_code: outcome_code.to_owned(),
                policy_version,
                event_id,
                protected_event,
                worker_id: worker_id.to_owned(),
            };
            match self.run_service.reconcile_approval_wait(prepared).await {
                Ok(()) => report.record(report_kind),
                Err(AiError::Conflict) => report.raced += 1,
                Err(error) => return Err(error),
            }
        }
        Ok(report)
    }

    async fn load_candidates(&self) -> Result<Vec<AiRunRecord>, AiError> {
        let maximum_candidate_scan = self.limits.maximum_candidate_scan;
        self.database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    let mut runs = tx
                        .query::<AiRunRecord>()
                        .filter(AiRunRecordWhereInput {
                            state: Some(StringFilter {
                                eq: Some("waiting_approval".to_owned()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(maximum_candidate_scan as i64)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    runs.sort_by_key(|run| (run.created_at, run.id));
                    runs.truncate(maximum_candidate_scan);
                    Ok(runs)
                })
            })
            .await
            .map_err(map_transaction)
    }

    async fn load_snapshot(&self, run: AiRunRecord) -> Result<ApprovalWaitSnapshot, AiError> {
        self.database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&run.session_id)
                        .await
                        .map_err(OrmPublicError::from)?;
                    let calls = tx
                        .query::<AiToolCallRecord>()
                        .filter(AiToolCallRecordWhereInput {
                            run_id: Some(UuidFilter {
                                eq: Some(run.id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(4_097)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let waiting_calls = calls
                        .iter()
                        .filter(|call| {
                            call.lease_generation == run.lease_generation
                                && call.state == "waiting_approval"
                        })
                        .collect::<Vec<_>>();
                    let call = (waiting_calls.len() == 1).then(|| waiting_calls[0].clone());
                    let step = match call.as_ref() {
                        Some(call) => tx
                            .find_by_id::<AiRunStepRecord>(&call.id)
                            .await
                            .map_err(OrmPublicError::from)?,
                        None => None,
                    };
                    let approval = match call.as_ref().and_then(|call| call.approval_id) {
                        Some(approval_id) => tx
                            .find_by_id::<AiApprovalRecord>(&approval_id)
                            .await
                            .map_err(OrmPublicError::from)?,
                        None => None,
                    };
                    let checkpoint = match run.latest_checkpoint_id {
                        Some(checkpoint_id) => tx
                            .query::<AiRunCheckpointRecord>()
                            .filter(AiRunCheckpointRecordWhereInput {
                                id: Some(UuidFilter {
                                    eq: Some(checkpoint_id),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .limit(1)
                            .fetch_one()
                            .await
                            .map_err(OrmPublicError::from)?,
                        None => None,
                    };
                    let reservation = match checkpoint
                        .as_ref()
                        .and_then(|checkpoint| checkpoint.budget_reservation_id)
                    {
                        Some(reservation_id) => tx
                            .find_by_id::<AiBudgetReservationRecord>(&reservation_id)
                            .await
                            .map_err(OrmPublicError::from)?,
                        None => None,
                    };
                    Ok(ApprovalWaitSnapshot {
                        run,
                        session,
                        calls,
                        call,
                        step,
                        approval,
                        checkpoint,
                        reservation,
                    })
                })
            })
            .await
            .map_err(map_transaction)
    }

    async fn resolve_current(
        &self,
        reference: &PrincipalReference,
    ) -> Result<agql_auth::ResolvedPrincipal, AiError> {
        let resolved = self
            .principal_resolver
            .resolve(reference)
            .await
            .map_err(|_| AiError::ReauthorizationFailed)?;
        let checked_at = self.clock.now();
        if resolved.resolved_at() > checked_at
            || checked_at - resolved.resolved_at() > self.limits.maximum_principal_age
            || resolved.reference() != reference
        {
            return Err(AiError::ReauthorizationFailed);
        }
        Ok(resolved)
    }

    async fn current_protection_policy(
        &self,
        principal: &AuthPrincipal,
        scope: &AiScope,
    ) -> Result<AiContentProtectionPolicy, AiError> {
        let policy = self.protection_policy.resolve(principal, scope).await?;
        if !policy.ready || policy.scope != *scope {
            return Err(AiError::RuntimeNotReady);
        }
        Ok(policy)
    }

    async fn protect_event(
        &self,
        policy: &AiContentProtectionPolicy,
        event_id: Uuid,
        scope: &AiScope,
        value: serde_json::Value,
    ) -> Result<serde_json::Value, AiError> {
        let envelope = self
            .content_protector
            .protect(
                policy,
                &content_context(
                    "graphql_orm_ai_session_events",
                    event_id,
                    "protected_payload",
                    scope,
                ),
                value,
            )
            .await
            .map_err(map_protection)?;
        serde_json::to_value(envelope).map_err(|_| AiError::PersistenceFailed)
    }
}

struct ApprovalWaitSnapshot {
    run: AiRunRecord,
    session: Option<AiSessionRecord>,
    calls: Vec<AiToolCallRecord>,
    call: Option<AiToolCallRecord>,
    step: Option<AiRunStepRecord>,
    approval: Option<AiApprovalRecord>,
    checkpoint: Option<AiRunCheckpointRecord>,
    reservation: Option<AiBudgetReservationRecord>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ApprovalWaitReportKind {
    Denied,
    Revoked,
    Expired,
    Policy,
    RecoveryRequired,
}

impl AiApprovalWaitReconciliationReport {
    fn record(&mut self, kind: ApprovalWaitReportKind) {
        match kind {
            ApprovalWaitReportKind::Denied => self.cancelled_denied += 1,
            ApprovalWaitReportKind::Revoked => self.cancelled_revoked += 1,
            ApprovalWaitReportKind::Expired => self.cancelled_expired += 1,
            ApprovalWaitReportKind::Policy => self.cancelled_policy += 1,
            ApprovalWaitReportKind::RecoveryRequired => self.recovery_required += 1,
        }
    }
}

fn approval_wait_linkage_is_valid(
    snapshot: &ApprovalWaitSnapshot,
    principal_reference: &PrincipalReference,
) -> bool {
    let (
        Some(session),
        Some(call),
        Some(step),
        Some(approval),
        Some(checkpoint),
        Some(reservation),
    ) = (
        snapshot.session.as_ref(),
        snapshot.call.as_ref(),
        snapshot.step.as_ref(),
        snapshot.approval.as_ref(),
        snapshot.checkpoint.as_ref(),
        snapshot.reservation.as_ref(),
    )
    else {
        return false;
    };
    let Some(attempt_id) = snapshot.run.attempt_id else {
        return false;
    };
    let Some(provider_response_id) = checkpoint
        .provider_response_id
        .as_deref()
        .filter(|value| valid_safe_reference(value))
    else {
        return false;
    };
    let Some(budget_reservation_id) = checkpoint.budget_reservation_id else {
        return false;
    };
    let Some(protected_state) = checkpoint.protected_state.as_ref().filter(|state| {
        serde_json::to_vec(state).is_ok_and(|encoded| encoded.len() <= 64 * 1024 * 1024)
    }) else {
        return false;
    };
    let expected_hash = coordinator_checkpoint_hash(
        AiRunId(snapshot.run.id),
        attempt_id,
        snapshot.run.lease_generation,
        checkpoint.id,
        &checkpoint.checkpoint_kind,
        &reservation.provider_kind,
        &reservation.provider_model,
        Some(provider_response_id),
        budget_reservation_id,
        protected_state,
    );
    let relevant_calls = snapshot
        .calls
        .iter()
        .filter(|candidate| {
            candidate.lease_generation == snapshot.run.lease_generation
                && candidate.provider_response_id.as_deref() == Some(provider_response_id)
                && candidate.budget_reservation_id == Some(budget_reservation_id)
        })
        .collect::<Vec<_>>();
    snapshot.calls.len() < 4_097
        && snapshot.run.state == "waiting_approval"
        && snapshot.run.lease_generation > 0
        && snapshot
            .run
            .lease_owner
            .as_deref()
            .is_some_and(|worker| validate_worker_id(worker).is_ok())
        && snapshot.run.lease_expires_at.is_some()
        && snapshot.run.latest_checkpoint_id == Some(checkpoint.id)
        && checkpoint.run_id == snapshot.run.id
        && checkpoint.attempt_id == attempt_id
        && checkpoint.lease_generation == snapshot.run.lease_generation
        && checkpoint.checkpoint_kind == "provider_turn_persisted"
        && checkpoint.assistant_message_id.is_none()
        && expected_hash.is_ok_and(|expected_hash| checkpoint.checkpoint_hash == expected_hash)
        && reservation.id == budget_reservation_id
        && reservation.scope_kind == session.scope_kind
        && reservation.scope_id == session.scope_id
        && reservation.tenant_id == session.tenant_id
        && reservation.principal_kind == session.owner_principal_kind
        && reservation.principal_subject == session.owner_subject
        && reservation.session_id == session.id
        && reservation.run_id == snapshot.run.id
        && reservation.attempt_id == attempt_id
        && reservation.lease_generation == snapshot.run.lease_generation
        && reservation.state == "committed"
        && reservation.actual_runs == Some(1)
        && reservation.reconciled_at.is_some()
        && relevant_calls.len() == 1
        && relevant_calls[0].id == call.id
        && call.run_id == snapshot.run.id
        && call.lease_generation == snapshot.run.lease_generation
        && call.provider_kind.as_deref() == Some(reservation.provider_kind.as_str())
        && call.provider_model.as_deref() == Some(reservation.provider_model.as_str())
        && call.provider_response_id.as_deref() == Some(provider_response_id)
        && call.budget_reservation_id == Some(budget_reservation_id)
        && call.tool_call_index == 0
        && call.state == "waiting_approval"
        && call.completed_at.is_none()
        && call.protected_result.is_none()
        && call.approval_id == Some(approval.id)
        && call.risk != "read_only"
        && step.id == call.id
        && step.run_id == snapshot.run.id
        && step.lease_generation == snapshot.run.lease_generation
        && step.step_kind == "application_tool"
        && step.state == "running"
        && step.finished_at.is_none()
        && approval.tool_call_id == call.id
        && approval.session_id == session.id
        && approval.principal_subject == principal_reference.subject
        && approval.principal_reference_fingerprint
            == AiApprovalBinding::principal_fingerprint(principal_reference)
        && approval.argument_hash == call.argument_hash
        && approval.tool_fingerprint == call.tool_fingerprint
        && approval.maximum_uses == 1
        && approval.consumed_uses == 0
        && approval.consumed_at.is_none()
}

fn approval_wait_cutoff_reached(
    created_at: i64,
    now: OffsetDateTime,
    maximum_pending_duration: Duration,
) -> bool {
    created_at
        .checked_add(maximum_pending_duration.whole_seconds())
        .is_none_or(|cutoff| cutoff <= now.unix_timestamp())
}

fn valid_safe_reference(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 1_024 && !value.chars().any(char::is_control)
}

fn record_matches_binding(
    record: &AiApprovalRecord,
    binding: &AiApprovalBinding,
    preview: &AiCanonicalActionPreview,
) -> bool {
    record.tool_call_id == binding.tool_call_id.0
        && record.session_id == binding.session_id.0
        && record.principal_reference_fingerprint == binding.principal_reference_fingerprint
        && record.delegated_actor_subject == binding.delegated_actor_subject
        && record.delegation_reference == binding.delegation_reference
        && record.argument_hash == binding.argument_hash
        && record.tool_fingerprint == binding.tool_fingerprint
        && record.binding_hash == binding.stable_hash()
        && record.execution_target_id == binding.operation.target_id.as_str()
        && record.target_schema_fingerprint == binding.operation.schema_fingerprint
        && record.operation_name == binding.operation.operation_name
        && record.operation_document_hash == binding.operation.document_hash
        && record.result_projection_fingerprint == binding.operation.result_projection_fingerprint
        && record.disclosure_schema_fingerprint == binding.operation.disclosure_schema_fingerprint
        && record.policy_version == binding.policy_version
        && record.authorization_state_digest == binding.authorization_state_digest
        && record.action_preview_hash == preview.stable_hash()
        && record.action_preview_hash == binding.preview_hash
}

fn canonical_resources(
    resources: &[crate::AiApprovalResourceBinding],
) -> Vec<crate::AiApprovalResourceBinding> {
    let mut resources = resources.to_vec();
    resources.sort();
    resources
}

fn canonical_preview(preview: &AiCanonicalActionPreview) -> AiCanonicalActionPreview {
    let mut preview = preview.clone();
    preview.targets.sort();
    preview
}

fn validate_binding_fields(binding: &AiApprovalBinding) -> Result<(), AiError> {
    let values = [
        binding.tool_fingerprint.as_str(),
        binding.argument_hash.as_str(),
        binding.operation.schema_fingerprint.as_str(),
        binding.operation.operation_name.as_str(),
        binding.operation.document_hash.as_str(),
        binding.operation.result_projection_fingerprint.as_str(),
        binding.operation.disclosure_schema_fingerprint.as_str(),
        binding.principal_reference_fingerprint.as_str(),
        binding.policy_version.as_str(),
        binding.authorization_state_digest.as_str(),
        binding.preview_hash.as_str(),
    ];
    if values
        .iter()
        .any(|value| value.is_empty() || value.len() > 1_024 || value.chars().any(char::is_control))
        || binding
            .delegation_reference
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > 1_024)
        || binding
            .delegated_actor_subject
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > 512)
    {
        return Err(AiError::InvalidConfiguration(
            "approval binding contains invalid fields".to_owned(),
        ));
    }
    Ok(())
}

fn parse_state(state: &str) -> Result<AiApprovalState, AiError> {
    match state {
        "pending" => Ok(AiApprovalState::Pending),
        "approved" => Ok(AiApprovalState::Approved),
        "resume_claimed" => Ok(AiApprovalState::ResumeClaimed),
        "denied" => Ok(AiApprovalState::Denied),
        "expired" => Ok(AiApprovalState::Expired),
        "revoked" => Ok(AiApprovalState::Revoked),
        "consumed" => Ok(AiApprovalState::Consumed),
        _ => Err(AiError::PersistenceFailed),
    }
}

fn page_input(page: &ValidatedKeysetConnection) -> KeysetConnectionInput {
    match page.direction {
        KeysetWindowDirection::Forward => KeysetConnectionInput {
            after: page.cursor.clone(),
            first: Some(page.limit),
            ..Default::default()
        },
        KeysetWindowDirection::Backward => KeysetConnectionInput {
            before: page.cursor.clone(),
            last: Some(page.limit),
            ..Default::default()
        },
    }
}

fn principal_identity(principal: &AuthPrincipal) -> (String, &str) {
    let kind = match principal {
        AuthPrincipal::User(_) => "user".to_owned(),
        AuthPrincipal::ApiToken(token) => format!("api_token:{}", token.principal_kind.as_str()),
    };
    (kind, principal.subject())
}

fn record_scope(session: &AiSessionRecord) -> AiScope {
    AiScope {
        kind: session.scope_kind.clone(),
        id: session.scope_id.clone(),
        tenant_id: session.tenant_id.clone(),
    }
}

fn content_context(
    entity: &str,
    row_id: Uuid,
    field: &str,
    scope: &AiScope,
) -> ContentProtectionContext {
    ContentProtectionContext {
        entity: entity.to_owned(),
        row_id: row_id.to_string(),
        field: field.to_owned(),
        scope: scope.clone(),
    }
}

fn canonical_second(value: OffsetDateTime) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(value.unix_timestamp())
        .expect("an existing OffsetDateTime timestamp remains representable")
}

fn map_protection(error: crate::ContentProtectionError) -> AiError {
    match error {
        crate::ContentProtectionError::PolicyNotReady => AiError::RuntimeNotReady,
        _ => AiError::PersistenceFailed,
    }
}

fn map_transaction(error: TransactionError) -> AiError {
    map_orm(error.public_error().clone())
}

fn map_orm(error: OrmPublicError) -> AiError {
    match error.code {
        OrmErrorCode::InvalidInput
        | OrmErrorCode::CursorInvalid
        | OrmErrorCode::PageLimitExceeded => AiError::InvalidInput(error.message),
        OrmErrorCode::Unauthenticated | OrmErrorCode::Forbidden => AiError::Forbidden,
        OrmErrorCode::NotFound => AiError::NotFound,
        OrmErrorCode::Conflict | OrmErrorCode::ConstraintViolation => AiError::Conflict,
        OrmErrorCode::ServiceUnavailable
        | OrmErrorCode::InternalError
        | OrmErrorCode::AuthorizationMisconfigured => AiError::PersistenceFailed,
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use agql_auth::{
        AccessTokenMetadata, AssuranceMatchMode, AuthUser, FixedClock, MfaAcceptance,
        ResolvedPrincipal, SessionAssurance, SessionContext,
    };
    use graphql_orm::graphql::orm::{ApplyOptions, OrmSchemaModule};
    use graphql_orm::prelude::{Database, SqliteBackend};

    use crate::orm_runs::{PreparedCoordinatorCheckpoint, PreparedToolCallStart};
    use crate::{
        AiApprovalResourceBinding, AiDisclosureRule, AiDisclosureSchema, AiDisclosureShape,
        AiRunId, AiRunServiceLimits, AiToolCallId, AiToolDescriptor, GraphqlExecutionTargetId,
        GraphqlOperationContract,
    };

    #[derive(Clone)]
    struct Resolver {
        principal: AuthPrincipal,
        clock: Arc<FixedClock>,
    }

    #[async_trait]
    impl CurrentPrincipalResolver for Resolver {
        async fn resolve(
            &self,
            reference: &PrincipalReference,
        ) -> agql_auth::AuthResult<ResolvedPrincipal> {
            ResolvedPrincipal::new(reference.clone(), self.principal.clone(), self.clock.now())
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

    struct Protection(AiScope);

    #[async_trait]
    impl AiContentProtectionPolicyResolver for Protection {
        async fn resolve(
            &self,
            _principal: &AuthPrincipal,
            scope: &AiScope,
        ) -> Result<AiContentProtectionPolicy, AiError> {
            if scope != &self.0 {
                return Err(AiError::Forbidden);
            }
            Ok(AiContentProtectionPolicy {
                scope: scope.clone(),
                mode: crate::AiContentProtectionMode::DatabaseManaged,
                key_policy_reference: None,
                version: 1,
                ready: true,
            })
        }
    }

    struct WaitPolicy {
        may_continue: bool,
    }

    #[async_trait]
    impl AiApprovalWaitReconciliationPolicy for WaitPolicy {
        async fn evaluate_wait(
            &self,
            _principal: &AuthPrincipal,
            _context: &AiApprovalWaitPolicyContext,
        ) -> Result<AiApprovalWaitPolicyDecision, AiError> {
            if self.may_continue {
                AiApprovalWaitPolicyDecision::continue_waiting("wait-policy-v1")
            } else {
                AiApprovalWaitPolicyDecision::cancel("wait-policy-v1")
            }
        }
    }

    struct Fixture {
        database: Database<SqliteBackend>,
        run_service: OrmAiRunService,
        approval_service: OrmAiApprovalService,
        reconciliation_service: OrmAiApprovalWaitReconciliationService,
        clock: Arc<FixedClock>,
        principal: AuthPrincipal,
        scope: AiScope,
        now: OffsetDateTime,
        operation: GraphqlOperationContract,
        tool_fingerprint: String,
    }

    async fn fixture() -> Fixture {
        fixture_with_wait_policy(true).await
    }

    async fn fixture_with_wait_policy(may_continue: bool) -> Fixture {
        let database = Database::<SqliteBackend>::connect_sqlite("sqlite::memory:")
            .await
            .expect("in-memory SQLite should open");
        let module = crate::AiSchemaModule;
        let plan = database
            .schema()
            .plan_migration_to_entities(
                "ai-approval-test-v1",
                "AI approval lifecycle test",
                module.entities(),
            )
            .await
            .expect("AI approval schema should plan");
        database
            .schema()
            .apply_migration(&plan, ApplyOptions::default())
            .await
            .expect("AI approval schema should apply to in-memory SQLite");
        let now = OffsetDateTime::from_unix_timestamp(1_800_000_000)
            .expect("fixed test timestamp should be valid");
        let assurance = SessionAssurance::new(
            now,
            ["otp", "pwd"],
            Some("urn:test:loa:2".to_owned()),
            Some("test".to_owned()),
            MfaAcceptance::Satisfied,
        )
        .expect("test assurance should validate");
        let principal = AuthPrincipal::User(AuthUser {
            user_id: "approval-user".to_owned(),
            session_id: Uuid::new_v4(),
            roles: vec![],
            scopes: vec![],
            session: SessionContext::default().with_assurance(assurance),
            token_claims: AccessTokenMetadata {
                tenant_id: Some("tenant-approval".to_owned()),
                auth_time: Some(now.unix_timestamp()),
                amr: Some(vec!["otp".to_owned(), "pwd".to_owned()]),
                acr: Some("urn:test:loa:2".to_owned()),
                ..AccessTokenMetadata::default()
            },
        });
        let scope = AiScope::new("tenant", "tenant-approval").with_tenant_id("tenant-approval");
        let run_limits = AiRunServiceLimits::new(Duration::hours(1), Duration::hours(1), 16, 2, 8)
            .expect("test run limits should validate");
        let clock = Arc::new(FixedClock::new(now));
        let run_service = OrmAiRunService::new(database.clone(), clock.clone(), run_limits);
        let disclosure = AiDisclosureSchema::new(
            "approval-result-v1",
            AiDisclosureShape::object(
                AiDisclosureRule::exportable(crate::DataClassification::Internal),
                [(
                    "id".to_owned(),
                    AiDisclosureShape::scalar(AiDisclosureRule::exportable(
                        crate::DataClassification::Internal,
                    )),
                )],
            ),
        )
        .expect("test disclosure should validate");
        let document =
            "mutation ApplyReviewedChange($id: ID!) { applyReviewedChange(id: $id) { id } }";
        let operation = GraphqlOperationContract::new(
            GraphqlExecutionTargetId::parse("application-primary")
                .expect("logical target should parse"),
            "schema-v1",
            "ApplyReviewedChange",
            document,
            "projection-v1",
            disclosure.fingerprint.clone(),
        )
        .expect("operation contract should validate");
        let descriptor = AiToolDescriptor::new(
            "application.change",
            "Apply one supervised reviewed change",
            AiToolOperationKind::Mutation,
            document,
            serde_json::json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"id": {"type": "string"}},
                "required": ["id"],
                "additionalProperties": false
            }),
        )
        .expect("test tool descriptor should validate")
        .with_result_projection("projection-v1")
        .with_graphql_contract(operation.clone())
        .with_maturity(ToolMaturity::SupervisedWrite)
        .with_risk(AiToolRisk::HighImpact, crate::AiApprovalRule::OneShot);
        let tool_fingerprint = descriptor.fingerprint.clone();
        let mut tool_catalog = AiToolCatalog::new();
        tool_catalog
            .register_with_disclosure(descriptor, disclosure)
            .expect("supervised tool should register");
        let approval_service = OrmAiApprovalService::new(
            database.clone(),
            run_service.clone(),
            Arc::new(Resolver {
                principal: principal.clone(),
                clock: clock.clone(),
            }),
            Arc::new(AllowApprovals),
            Arc::new(tool_catalog),
            RecentMfaPolicy {
                maximum_age: Duration::minutes(5),
                clock_skew: Duration::seconds(30),
                allowed_amr: vec!["otp".to_owned()],
                allowed_acr: vec!["urn:test:loa:2".to_owned()],
                match_mode: AssuranceMatchMode::All,
            },
            Arc::new(Protection(scope.clone())),
            Arc::new(crate::DatabaseManagedContentProtector),
            clock.clone(),
        );
        let reconciliation_service = OrmAiApprovalWaitReconciliationService::new(
            database.clone(),
            run_service.clone(),
            Arc::new(Resolver {
                principal: principal.clone(),
                clock: clock.clone(),
            }),
            Arc::new(WaitPolicy { may_continue }),
            Arc::new(Protection(scope.clone())),
            Arc::new(crate::DatabaseManagedContentProtector),
            clock.clone(),
        )
        .with_limits(
            AiApprovalWaitReconciliationLimits::new(
                Duration::minutes(5),
                Duration::days(3_650),
                16,
            )
            .expect("test approval-wait limits should validate"),
        );
        Fixture {
            database,
            run_service,
            approval_service,
            reconciliation_service,
            clock,
            principal,
            scope,
            now,
            operation,
            tool_fingerprint,
        }
    }

    async fn seed_running_tool(fixture: &Fixture) -> (AiRunLease, AiToolCallId) {
        let session_id = AiSessionId::new();
        let run_id = AiRunId::new();
        let message_id = Uuid::new_v4();
        AiSessionRecord::insert(
            &fixture.database,
            CreateAiSessionRecordInput {
                id: session_id.0,
                owner_principal_kind: "user".to_owned(),
                owner_subject: fixture.principal.subject().to_owned(),
                tenant_id: fixture.scope.tenant_id.clone(),
                scope_kind: fixture.scope.kind.clone(),
                scope_id: fixture.scope.id.clone(),
                title: "Approval test".to_owned(),
                state: "active".to_owned(),
                stream_head: 0,
                message_head: 0,
                last_activity_at: fixture.now.unix_timestamp(),
                archived_at: None,
                deleted_at: None,
            },
        )
        .await
        .expect("test session should seed");
        AiRunRecord::insert(
            &fixture.database,
            CreateAiRunRecordInput {
                id: run_id.0,
                session_id: session_id.0,
                input_message_id: message_id,
                principal_reference: serde_json::to_value(fixture.principal.reference())
                    .expect("principal reference should serialize"),
                state: "queued".to_owned(),
                attempt_id: None,
                lease_owner: None,
                lease_generation: 0,
                lease_expires_at: None,
                lease_heartbeat_at: None,
                retry_count: 0,
                next_attempt_at: Some(fixture.now.unix_timestamp()),
                error_code: None,
                latest_checkpoint_id: None,
            },
        )
        .await
        .expect("test run should seed");
        let lease = fixture
            .run_service
            .claim_next("approval-worker")
            .await
            .expect("claim should succeed")
            .expect("test run should be claimable");
        let lease = fixture
            .run_service
            .start(&lease)
            .await
            .expect("run should start");
        let budget_reservation = AiBudgetReservationRecord::insert(
            &fixture.database,
            CreateAiBudgetReservationRecordInput {
                budget_counter_ids: serde_json::json!([]),
                scope_kind: fixture.scope.kind.clone(),
                scope_id: fixture.scope.id.clone(),
                tenant_id: fixture.scope.tenant_id.clone(),
                principal_kind: "user".to_owned(),
                principal_subject: fixture.principal.subject().to_owned(),
                session_id: lease.session_id().0,
                run_id: lease.run_id().0,
                attempt_id: lease.attempt_id(),
                lease_generation: lease.lease_generation(),
                provider_kind: "local_harness".to_owned(),
                provider_model: "approval-test".to_owned(),
                pricing_policy_version: "approval-pricing-v1".to_owned(),
                reserved_input_tokens: 1,
                reserved_output_tokens: 1,
                reserved_tool_units: 1,
                reserved_image_units: 0,
                reserved_cost_microunits: 1,
                reserved_runs: 1,
                actual_input_tokens: Some(1),
                actual_cached_input_tokens: Some(0),
                actual_output_tokens: Some(1),
                actual_tool_units: Some(1),
                actual_image_units: Some(0),
                actual_cost_microunits: Some(1),
                actual_runs: Some(1),
                idempotency_key: format!("approval-provider-turn-{}", lease.run_id().0),
                state: "committed".to_owned(),
                expires_at: (fixture.clock.now() + Duration::hours(1)).unix_timestamp(),
                reconciled_at: Some(fixture.clock.now().unix_timestamp()),
            },
        )
        .await
        .expect("committed approval-test budget should insert");
        let checkpoint_id = Uuid::new_v4();
        let protected_state = serde_json::json!({
            "protection": "database_managed",
            "value": {"providerTurn": 1}
        });
        let checkpoint_hash = coordinator_checkpoint_hash(
            lease.run_id(),
            lease.attempt_id(),
            lease.lease_generation(),
            checkpoint_id,
            "provider_turn_persisted",
            "local_harness",
            "approval-test",
            Some("approval-response-1"),
            budget_reservation.id,
            &protected_state,
        )
        .expect("approval provider checkpoint should hash");
        let lease = fixture
            .run_service
            .append_coordinator_checkpoint(
                &lease,
                PreparedCoordinatorCheckpoint {
                    id: checkpoint_id,
                    checkpoint_kind: "provider_turn_persisted".to_owned(),
                    provider_kind: "local_harness".to_owned(),
                    provider_model: "approval-test".to_owned(),
                    provider_response_id: Some("approval-response-1".to_owned()),
                    budget_reservation_id: budget_reservation.id,
                    protected_state,
                    checkpoint_hash,
                    completed_tools: Vec::new(),
                },
            )
            .await
            .expect("approval provider checkpoint should commit");
        let tool_call_id = AiToolCallId::new();
        fixture
            .run_service
            .begin_tool_call(
                &lease,
                PreparedToolCallStart {
                    id: tool_call_id.0,
                    provider_call_key: format!("approval-provider-call-{}", tool_call_id.0),
                    provider_call_id: format!("provider-call-{}", tool_call_id.0),
                    provider_kind: "local_harness".to_owned(),
                    provider_model: "approval-test".to_owned(),
                    provider_response_id: Some("approval-response-1".to_owned()),
                    budget_reservation_id: budget_reservation.id,
                    provider_turn_index: 0,
                    tool_call_index: 0,
                    tool_id: "application.change".to_owned(),
                    tool_fingerprint: fixture.tool_fingerprint.clone(),
                    protected_arguments: serde_json::json!({"protection": "database_managed", "value": {"id": "resource-1"}}),
                    argument_hash: "argument-hash-v1".to_owned(),
                    risk: "high_impact".to_owned(),
                    idempotency_key: Some(tool_call_id.0.to_string()),
                    correlation_id: "approval-correlation".to_owned(),
                    causation_id: "approval-causation".to_owned(),
                    delegation_reference: None,
                    expected_owner_principal_kind: "user".to_owned(),
                    expected_owner_subject: fixture.principal.subject().to_owned(),
                    expected_scope_kind: fixture.scope.kind.clone(),
                    expected_scope_id: fixture.scope.id.clone(),
                    expected_tenant_id: fixture.scope.tenant_id.clone(),
                },
            )
            .await
            .expect("consequential tool call should stage");
        (lease, tool_call_id)
    }

    fn approval_binding(
        fixture: &Fixture,
        lease: &AiRunLease,
        tool_call_id: AiToolCallId,
    ) -> (AiApprovalBinding, AiCanonicalActionPreview) {
        let resource = AiApprovalResourceBinding {
            resource_type: "record".to_owned(),
            resource_id: "resource-1".to_owned(),
            expected_version: "version-7".to_owned(),
        };
        let preview = AiCanonicalActionPreview {
            action_kind: "application.change".to_owned(),
            title: "Apply one reviewed change".to_owned(),
            targets: vec![resource.clone()],
            details: serde_json::json!({"changedFields": ["title"]}),
        };
        let binding = AiApprovalBinding {
            tool_call_id,
            session_id: lease.session_id(),
            scope: fixture.scope.clone(),
            tool_fingerprint: fixture.tool_fingerprint.clone(),
            argument_hash: "argument-hash-v1".to_owned(),
            operation: fixture.operation.clone(),
            principal_reference_fingerprint: AiApprovalBinding::principal_fingerprint(
                lease.principal_reference(),
            ),
            delegated_actor_subject: None,
            delegation_reference: None,
            policy_version: "policy-v1".to_owned(),
            authorization_state_digest: "auth-state-v1".to_owned(),
            resources: vec![resource],
            preview_hash: preview.stable_hash(),
        };
        (binding, preview)
    }

    async fn request_wait(
        fixture: &Fixture,
        expires_at: OffsetDateTime,
    ) -> (AiRequestedApproval, AiToolCallId) {
        let (lease, tool_call_id) = seed_running_tool(fixture).await;
        let (binding, preview) = approval_binding(fixture, &lease, tool_call_id);
        let requested = fixture
            .approval_service
            .request_approval(&lease, binding, preview, expires_at, false)
            .await
            .expect("approval request should park the run");
        (requested, tool_call_id)
    }

    #[tokio::test]
    async fn approval_wait_reconciliation_leaves_current_waits_parked_and_cancels_denials_once() {
        let fixture = fixture().await;
        let (requested, tool_call_id) =
            request_wait(&fixture, fixture.now + Duration::hours(2)).await;
        let waiting = fixture
            .reconciliation_service
            .reconcile_waits("approval-wait-observer")
            .await
            .expect("current pending wait should reconcile");
        assert_eq!(waiting.still_waiting, 1, "{waiting:?}");
        assert_eq!(waiting.cancelled_denied, 0);
        fixture.clock.advance_seconds(3_601);
        let waiting_after_lease_expiry = fixture
            .reconciliation_service
            .reconcile_waits("approval-wait-observer-after-expiry")
            .await
            .expect("valid wait should remain parked without a heartbeat");
        assert_eq!(waiting_after_lease_expiry.still_waiting, 1);
        fixture
            .run_service
            .recover_expired_leases()
            .await
            .expect("generic recovery should exclude live approval waits");
        let parked = AiRunRecord::find_by_id(&fixture.database, &requested.lease().run_id().0)
            .await
            .expect("parked run lookup should succeed")
            .expect("parked run should remain durable");
        assert_eq!(parked.state, "waiting_approval");

        let view = fixture
            .approval_service
            .approval(&fixture.principal, requested.approval_id())
            .await
            .expect("pending approval read should succeed")
            .expect("pending approval should remain visible");
        fixture
            .approval_service
            .decide_approval(
                &fixture.principal,
                DecideAiApprovalInput {
                    id: view.id,
                    decision: AiApprovalDecision::Deny,
                    expected_version: view.row_version,
                },
            )
            .await
            .expect("approval denial should persist");

        let first = fixture.reconciliation_service.clone();
        let second = fixture.reconciliation_service.clone();
        let (first_report, second_report) = tokio::join!(
            first.reconcile_waits("approval-wait-worker-a"),
            second.reconcile_waits("approval-wait-worker-b")
        );
        let first_report = first_report.expect("first concurrent reconciler should stay safe");
        let second_report = second_report.expect("second concurrent reconciler should stay safe");
        assert_eq!(
            first_report.cancelled_denied + second_report.cancelled_denied,
            1
        );

        let run = AiRunRecord::find_by_id(&fixture.database, &requested.lease().run_id().0)
            .await
            .expect("cancelled run lookup should succeed")
            .expect("cancelled run should remain durable");
        assert_eq!(run.state, "cancelled");
        assert!(run.lease_owner.is_none());
        assert!(run.lease_expires_at.is_none());
        assert_eq!(run.error_code.as_deref(), Some("approval_denied"));
        let call = AiToolCallRecord::find_by_id(&fixture.database, &tool_call_id.0)
            .await
            .expect("cancelled tool-call lookup should succeed")
            .expect("cancelled tool call should remain durable");
        assert_eq!(call.state, "approval_denied");
        assert!(call.completed_at.is_some());
        let step = AiRunStepRecord::find_by_id(&fixture.database, &tool_call_id.0)
            .await
            .expect("cancelled run-step lookup should succeed")
            .expect("cancelled run step should remain durable");
        assert_eq!(step.state, "approval_denied");
        assert_eq!(step.error_code.as_deref(), Some("approval_denied"));
        let approval = AiApprovalRecord::find_by_id(&fixture.database, &requested.approval_id().0)
            .await
            .expect("denied approval lookup should succeed")
            .expect("denied approval should remain durable");
        assert_eq!(approval.state, "denied");

        let (events, audits, outcomes) = fixture
            .database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    let events = tx
                        .query::<AiSessionEventRecord>()
                        .limit(100)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let audits = tx
                        .query::<AiAuditEventRecord>()
                        .limit(100)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let outcomes = tx
                        .query::<AiRunAttemptOutcomeRecord>()
                        .limit(100)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    Ok((events, audits, outcomes))
                })
            })
            .await
            .expect("reconciliation facts should be readable");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type == "approval_wait_reconciled")
                .count(),
            1
        );
        assert_eq!(
            audits
                .iter()
                .filter(|audit| {
                    audit.action == "ai.run.approval_wait_reconcile"
                        && audit.reason_code == "approval_denied"
                })
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| {
                    outcome.run_id == requested.lease().run_id().0
                        && outcome.outcome_code == "approval_denied"
                })
                .count(),
            1
        );
        assert!(
            fixture
                .run_service
                .claim_next_approved("approval-wait-resume-probe")
                .await
                .expect("resume probe should remain safe")
                .is_none()
        );
    }

    #[tokio::test]
    async fn approval_wait_reconciliation_expires_stale_pending_authority() {
        let fixture = fixture().await;
        let (requested, tool_call_id) =
            request_wait(&fixture, fixture.now + Duration::seconds(1)).await;
        fixture.clock.advance_seconds(2);
        let report = fixture
            .reconciliation_service
            .reconcile_waits("approval-expiry-worker")
            .await
            .expect("expired pending wait should reconcile");
        assert_eq!(report.cancelled_expired, 1);
        let approval = AiApprovalRecord::find_by_id(&fixture.database, &requested.approval_id().0)
            .await
            .expect("expired approval lookup should succeed")
            .expect("expired approval should remain durable");
        assert_eq!(approval.state, "expired");
        let call = AiToolCallRecord::find_by_id(&fixture.database, &tool_call_id.0)
            .await
            .expect("expired call lookup should succeed")
            .expect("expired call should remain durable");
        assert_eq!(call.state, "approval_expired");
        let run = AiRunRecord::find_by_id(&fixture.database, &requested.lease().run_id().0)
            .await
            .expect("expired run lookup should succeed")
            .expect("expired run should remain durable");
        assert_eq!(run.state, "cancelled");
        assert_eq!(run.error_code.as_deref(), Some("approval_expired"));
    }

    #[tokio::test]
    async fn approval_wait_reconciliation_cancels_revoked_authority() {
        let fixture = fixture().await;
        let (requested, tool_call_id) =
            request_wait(&fixture, fixture.now + Duration::minutes(30)).await;
        let pending = fixture
            .approval_service
            .approval(&fixture.principal, requested.approval_id())
            .await
            .expect("pending approval read should succeed")
            .expect("pending approval should remain visible");
        let approved = fixture
            .approval_service
            .decide_approval(
                &fixture.principal,
                DecideAiApprovalInput {
                    id: pending.id,
                    decision: AiApprovalDecision::Approve,
                    expected_version: pending.row_version,
                },
            )
            .await
            .expect("approval should persist");
        fixture
            .approval_service
            .revoke_approval(
                &fixture.principal,
                RevokeAiApprovalInput {
                    id: approved.id,
                    expected_version: approved.row_version,
                },
            )
            .await
            .expect("revocation should persist");
        let report = fixture
            .reconciliation_service
            .reconcile_waits("approval-revocation-worker")
            .await
            .expect("revoked wait should reconcile");
        assert_eq!(report.cancelled_revoked, 1);
        let run = AiRunRecord::find_by_id(&fixture.database, &requested.lease().run_id().0)
            .await
            .expect("revoked run lookup should succeed")
            .expect("revoked run should remain durable");
        assert_eq!(run.state, "cancelled");
        assert_eq!(run.error_code.as_deref(), Some("approval_revoked"));
        let call = AiToolCallRecord::find_by_id(&fixture.database, &tool_call_id.0)
            .await
            .expect("revoked call lookup should succeed")
            .expect("revoked call should remain durable");
        assert_eq!(call.state, "approval_revoked");
    }

    #[tokio::test]
    async fn approval_wait_reconciliation_enforces_the_deployment_cutoff() {
        let fixture = fixture().await;
        let (requested, _) = request_wait(&fixture, fixture.now + Duration::minutes(30)).await;
        fixture.clock.advance_seconds(2);
        let reconciler = fixture.reconciliation_service.clone().with_limits(
            AiApprovalWaitReconciliationLimits::new(Duration::minutes(5), Duration::seconds(1), 16)
                .expect("cutoff test limits should validate"),
        );
        let report = reconciler
            .reconcile_waits("approval-cutoff-worker")
            .await
            .expect("deployment-cutoff wait should reconcile");
        assert_eq!(report.cancelled_expired, 1);
        let run = AiRunRecord::find_by_id(&fixture.database, &requested.lease().run_id().0)
            .await
            .expect("cutoff run lookup should succeed")
            .expect("cutoff run should remain durable");
        assert_eq!(run.state, "cancelled");
        assert_eq!(run.error_code.as_deref(), Some("approval_wait_cutoff"));
        let approval = AiApprovalRecord::find_by_id(&fixture.database, &requested.approval_id().0)
            .await
            .expect("cutoff approval lookup should succeed")
            .expect("cutoff approval should remain durable");
        assert_eq!(approval.state, "expired");
    }

    #[tokio::test]
    async fn approval_wait_reconciliation_applies_current_scope_policy_without_resuming() {
        let fixture = fixture_with_wait_policy(false).await;
        let (requested, _) = request_wait(&fixture, fixture.now + Duration::minutes(30)).await;
        let report = fixture
            .reconciliation_service
            .reconcile_waits("approval-policy-worker")
            .await
            .expect("policy-cancelled wait should reconcile");
        assert_eq!(report.cancelled_policy, 1, "{report:?}");
        let approval = AiApprovalRecord::find_by_id(&fixture.database, &requested.approval_id().0)
            .await
            .expect("policy-expired approval lookup should succeed")
            .expect("policy-expired approval should remain durable");
        assert_eq!(approval.state, "expired");
        let run = AiRunRecord::find_by_id(&fixture.database, &requested.lease().run_id().0)
            .await
            .expect("policy-cancelled run lookup should succeed")
            .expect("policy-cancelled run should remain durable");
        assert_eq!(run.state, "cancelled");
        assert_eq!(
            run.error_code.as_deref(),
            Some("approval_wait_policy_cancelled")
        );
    }

    #[tokio::test]
    async fn approval_wait_reconciliation_closes_malformed_linkage_without_touching_authority() {
        let fixture = fixture().await;
        let (requested, tool_call_id) =
            request_wait(&fixture, fixture.now + Duration::minutes(30)).await;
        let run_id = requested.lease().run_id().0;
        fixture
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let run = tx
                        .find_by_id::<AiRunRecord>(&run_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    let outcome = tx
                        .compare_and_swap::<AiRunRecord>(
                            &run.id,
                            run.row_version,
                            AiRunRecordWhereInput::default(),
                            UpdateAiRunRecordInput {
                                latest_checkpoint_id: Some(None),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if matches!(outcome, ConditionalUpdateOutcome::Updated(_)) {
                        Ok(())
                    } else {
                        Err(OrmPublicError::new(OrmErrorCode::Conflict))
                    }
                })
            })
            .await
            .expect("test should remove the checkpoint linkage atomically");

        let report = fixture
            .reconciliation_service
            .reconcile_waits("approval-recovery-worker")
            .await
            .expect("malformed approval wait should close for recovery");
        assert_eq!(report.recovery_required, 1);
        let run = AiRunRecord::find_by_id(&fixture.database, &run_id)
            .await
            .expect("recovery run lookup should succeed")
            .expect("recovery run should remain durable");
        assert_eq!(run.state, "recovery_required");
        assert_eq!(
            run.error_code.as_deref(),
            Some("approval_wait_linkage_invalid")
        );
        let approval = AiApprovalRecord::find_by_id(&fixture.database, &requested.approval_id().0)
            .await
            .expect("untouched approval lookup should succeed")
            .expect("untouched approval should remain durable");
        assert_eq!(approval.state, "pending");
        let call = AiToolCallRecord::find_by_id(&fixture.database, &tool_call_id.0)
            .await
            .expect("untouched call lookup should succeed")
            .expect("untouched call should remain durable");
        assert_eq!(call.state, "waiting_approval");
        assert!(call.completed_at.is_none());
    }

    #[tokio::test]
    async fn approval_is_preview_bound_recent_mfa_gated_and_consumed_once() {
        let fixture = fixture().await;
        let (stale_running, stale_tool_call_id) = seed_running_tool(&fixture).await;
        let (stale_binding, stale_preview) =
            approval_binding(&fixture, &stale_running, stale_tool_call_id);
        let stale_requested = fixture
            .approval_service
            .request_approval(
                &stale_running,
                stale_binding,
                stale_preview,
                fixture.now + Duration::seconds(1),
                true,
            )
            .await
            .expect("stale approval request should park its run");
        let stale_view = fixture
            .approval_service
            .approval(&fixture.principal, stale_requested.approval_id())
            .await
            .expect("stale approval read should succeed")
            .expect("stale approval should remain visible");
        fixture
            .approval_service
            .decide_approval(
                &fixture.principal,
                DecideAiApprovalInput {
                    id: stale_view.id,
                    decision: AiApprovalDecision::Approve,
                    expected_version: stale_view.row_version,
                },
            )
            .await
            .expect("stale approval should initially be approved");
        fixture.clock.advance_seconds(2);

        let (running, tool_call_id) = seed_running_tool(&fixture).await;
        let (binding, preview) = approval_binding(&fixture, &running, tool_call_id);
        let requested = fixture
            .approval_service
            .request_approval(
                &running,
                binding.clone(),
                preview.clone(),
                fixture.now + Duration::minutes(30),
                true,
            )
            .await
            .expect("approval request should park the current fence");
        assert_eq!(
            requested.lease().state(),
            crate::AiRunState::WaitingApproval
        );
        assert!(matches!(
            fixture.run_service.heartbeat(&running).await,
            Err(AiError::Conflict)
        ));

        let view = fixture
            .approval_service
            .approval(&fixture.principal, requested.approval_id())
            .await
            .expect("approval read should succeed")
            .expect("approval should remain visible");
        assert_eq!(view.state, "pending");
        let approved = fixture
            .approval_service
            .decide_approval(
                &fixture.principal,
                DecideAiApprovalInput {
                    id: view.id,
                    decision: AiApprovalDecision::Approve,
                    expected_version: view.row_version,
                },
            )
            .await
            .expect("recent-MFA approval should succeed");
        assert_eq!(approved.state, "approved");

        let (first_claim, second_claim) = tokio::join!(
            fixture
                .run_service
                .claim_next_approved("approval-resume-worker-a"),
            fixture
                .run_service
                .claim_next_approved("approval-resume-worker-b")
        );
        let mut claims = Vec::new();
        for claim in [first_claim, second_claim] {
            if let Some(claim) = claim.expect("concurrent approved-wait scan should stay safe") {
                claims.push(claim);
            }
        }
        assert_eq!(claims.len(), 1);
        let resumed = claims.pop().expect("one worker should own the handoff");
        assert_eq!(resumed.approval_id(), requested.approval_id());
        assert_eq!(resumed.tool_call_id(), tool_call_id);
        assert_eq!(resumed.lease().attempt_id(), requested.lease().attempt_id());
        assert_eq!(
            resumed.lease().lease_generation(),
            requested.lease().lease_generation()
        );
        assert!(matches!(
            resumed.lease().worker_id(),
            "approval-resume-worker-a" | "approval-resume-worker-b"
        ));
        assert_eq!(resumed.lease().state(), crate::AiRunState::WaitingTool);
        let claimed_approval =
            AiApprovalRecord::find_by_id(&fixture.database, &requested.approval_id().0)
                .await
                .expect("claimed approval lookup should succeed")
                .expect("claimed approval should remain durable");
        assert_eq!(claimed_approval.state, "resume_claimed");
        let handoff_audits = fixture
            .database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    tx.query::<AiAuditEventRecord>()
                        .limit(100)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("handoff audit lookup should succeed");
        assert_eq!(
            handoff_audits
                .iter()
                .filter(|audit| audit.action == "ai.run.approval_resume_claimed")
                .count(),
            1
        );
        let expired =
            AiApprovalRecord::find_by_id(&fixture.database, &stale_requested.approval_id().0)
                .await
                .expect("expired approval lookup should succeed")
                .expect("expired approval should remain durable");
        assert_eq!(expired.state, "expired");
        assert_eq!(
            handoff_audits
                .iter()
                .filter(|audit| audit.action == "ai.approval.expired")
                .count(),
            1
        );
        assert!(
            fixture
                .run_service
                .claim_next_approved("approval-racing-worker")
                .await
                .expect("a second approved-wait scan should stay safe")
                .is_none()
        );
        assert!(matches!(
            fixture
                .approval_service
                .consume_approval(
                    requested.lease(),
                    requested.approval_id(),
                    &binding,
                    &preview,
                )
                .await,
            Err(AiError::Conflict)
        ));

        let consumed = fixture
            .approval_service
            .consume_approval(resumed.lease(), requested.approval_id(), &binding, &preview)
            .await
            .expect("exact approved binding should consume once");
        assert_eq!(consumed.approval().approval_id(), requested.approval_id());
        assert_eq!(consumed.lease().state(), crate::AiRunState::Running);
        assert!(matches!(
            fixture
                .approval_service
                .consume_approval(
                    requested.lease(),
                    requested.approval_id(),
                    &binding,
                    &preview,
                )
                .await,
            Err(AiError::Conflict | AiError::Forbidden)
        ));
        let record = AiApprovalRecord::find_by_id(&fixture.database, &requested.approval_id().0)
            .await
            .expect("approval lookup should succeed")
            .expect("approval should exist");
        assert_eq!(record.state, "consumed");
        assert_eq!(record.consumed_uses, 1);
        assert!(record.consumed_at.is_some());
    }
}
