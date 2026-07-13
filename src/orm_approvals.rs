//! ORM-backed canonical-preview approval and one-shot consumption lifecycle.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::sync::Arc;

use agql_auth::{
    AuthPrincipal, Clock, CurrentPrincipalResolver, PrincipalReference, RecentMfaPolicy,
};
use async_trait::async_trait;
use graphql_orm::db::Database;
use graphql_orm::graphql::errors::{OrmErrorCode, OrmPublicError};
use graphql_orm::graphql::filters::UuidFilter;
use graphql_orm::graphql::orm::{
    ConditionalUpdateOutcome, DefaultWriteBackend, TransactionError, TransactionMode,
};
use graphql_orm::graphql::pagination::{
    KeysetConnectionInput, KeysetWindowDirection, ValidatedKeysetConnection,
};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::orm_runs::{PreparedApprovalConsumption, PreparedApprovalRequest};
use crate::persistence::*;
use crate::{
    AiApprovalAccessPolicy, AiApprovalAction, AiApprovalBinding, AiApprovalConnection,
    AiApprovalDecision, AiApprovalEdge, AiApprovalGrant, AiApprovalId, AiApprovalService,
    AiApprovalState, AiApprovalView, AiCanonicalActionPreview, AiContentProtectionPolicy,
    AiContentProtectionPolicyResolver, AiContentProtector, AiError, AiRunLease, AiScope,
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
        let state = if matches!(record.state.as_str(), "pending" | "approved")
            && record.expires_at <= self.clock.now().unix_timestamp()
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
        expected_state: &'static str,
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
            || current.state != expected_state
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
        let record_decision = expected_state == "pending";
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
                        || current.state != expected_state
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
            "pending",
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
            "approved",
            "revoked",
        )
        .await
    }
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

    use crate::orm_runs::PreparedToolCallStart;
    use crate::{
        AiApprovalResourceBinding, AiDisclosureRule, AiDisclosureSchema, AiDisclosureShape,
        AiRunId, AiRunServiceLimits, AiToolCallId, AiToolDescriptor, GraphqlExecutionTargetId,
        GraphqlOperationContract,
    };

    #[derive(Clone)]
    struct Resolver {
        principal: AuthPrincipal,
        now: OffsetDateTime,
    }

    #[async_trait]
    impl CurrentPrincipalResolver for Resolver {
        async fn resolve(
            &self,
            reference: &PrincipalReference,
        ) -> agql_auth::AuthResult<ResolvedPrincipal> {
            ResolvedPrincipal::new(reference.clone(), self.principal.clone(), self.now)
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

    struct Fixture {
        database: Database<SqliteBackend>,
        run_service: OrmAiRunService,
        approval_service: OrmAiApprovalService,
        principal: AuthPrincipal,
        scope: AiScope,
        now: OffsetDateTime,
        operation: GraphqlOperationContract,
        tool_fingerprint: String,
    }

    async fn fixture() -> Fixture {
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
                now,
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
            clock,
        );
        Fixture {
            database,
            run_service,
            approval_service,
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
        let tool_call_id = AiToolCallId::new();
        fixture
            .run_service
            .begin_tool_call(
                &lease,
                PreparedToolCallStart {
                    id: tool_call_id.0,
                    provider_call_key: "approval-provider-call".to_owned(),
                    provider_call_id: "provider-call-1".to_owned(),
                    provider_kind: "mock".to_owned(),
                    provider_model: "approval-test".to_owned(),
                    provider_response_id: Some("approval-response-1".to_owned()),
                    budget_reservation_id: Uuid::new_v4(),
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

    #[tokio::test]
    async fn approval_is_preview_bound_recent_mfa_gated_and_consumed_once() {
        let fixture = fixture().await;
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

        let consumed = fixture
            .approval_service
            .consume_approval(
                requested.lease(),
                requested.approval_id(),
                &binding,
                &preview,
            )
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
