//! Protected fenced persistence for provisional visible provider batches.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::sync::Arc;

use agql_auth::{Clock, PrincipalReferenceKind, ResolvedPrincipal};
use async_trait::async_trait;
use graphql_orm::graphql::errors::OrmPublicError;
use serde_json::json;
use time::Duration;
use uuid::Uuid;

use crate::orm_runs::PreparedLiveDeltaEvent;
use crate::persistence::*;
use crate::{
    AiContentProtectionPolicy, AiError, AiLiveDeltaBatch, AiLiveDeltaKind,
    AiLiveDeltaPersistenceContext, AiLiveDeltaSink, AiRunLease, AiRuntime, AiScope,
    AiSessionAction, ContentProtectionContext, OrmAiRunService,
};

/// Deployment bounds for protected provisional provider events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiLiveDeltaPersistenceLimits {
    maximum_batch_bytes: usize,
    maximum_principal_age: Duration,
}

impl AiLiveDeltaPersistenceLimits {
    /// Creates validated persistence bounds.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless a batch is bounded to
    /// `4..=4096` UTF-8 bytes and principal freshness is positive and no more
    /// than one hour.
    pub fn new(
        maximum_batch_bytes: usize,
        maximum_principal_age: Duration,
    ) -> Result<Self, AiError> {
        if !(4..=4_096).contains(&maximum_batch_bytes)
            || !maximum_principal_age.is_positive()
            || maximum_principal_age > Duration::hours(1)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid live-delta persistence limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_batch_bytes,
            maximum_principal_age,
        })
    }

    /// Maximum protected visible batch plaintext size.
    pub const fn maximum_batch_bytes(self) -> usize {
        self.maximum_batch_bytes
    }

    /// Maximum accepted age of each freshly resolved principal.
    pub const fn maximum_principal_age(self) -> Duration {
        self.maximum_principal_age
    }
}

/// ORM-backed protected sink for cursor-addressable provisional visible
/// provider output.
///
/// Every batch independently rehydrates the current principal, checks scope
/// and session write access, protects the plaintext, rechecks policy drift,
/// and commits through a serializable transaction that validates the current
/// run fence and exact uncertain provider budget. It does not renew or rotate
/// the run lease, allowing coordinator heartbeats to remain authoritative.
pub struct OrmAiLiveDeltaService {
    run_service: OrmAiRunService,
    runtime: Arc<AiRuntime>,
    clock: Arc<dyn Clock>,
    limits: AiLiveDeltaPersistenceLimits,
}

impl OrmAiLiveDeltaService {
    /// Creates a protected durable provisional-event sink.
    pub fn new(
        run_service: OrmAiRunService,
        runtime: Arc<AiRuntime>,
        clock: Arc<dyn Clock>,
        limits: AiLiveDeltaPersistenceLimits,
    ) -> Self {
        Self {
            run_service,
            runtime,
            clock,
            limits,
        }
    }

    async fn current_policy(
        &self,
        lease: &AiRunLease,
        scope: &AiScope,
    ) -> Result<(ResolvedPrincipal, AiContentProtectionPolicy), AiError> {
        let principal = self
            .runtime
            .resolve_current_principal(lease.principal_reference())
            .await?;
        let now = self.clock.now();
        if principal.reference() != lease.principal_reference()
            || principal.resolved_at() > now
            || now - principal.resolved_at() >= self.limits.maximum_principal_age
            || principal
                .reference()
                .expires_at
                .is_some_and(|expires_at| expires_at <= now)
        {
            return Err(AiError::ReauthorizationFailed);
        }
        if !self
            .runtime
            .access_policy()
            .can_access_scope(principal.principal(), scope, AiSessionAction::Write)
            .await
            .is_allowed()
            || !self
                .runtime
                .access_policy()
                .can_access_session(
                    principal.principal(),
                    lease.session_id(),
                    AiSessionAction::Write,
                )
                .await
                .is_allowed()
        {
            return Err(AiError::Forbidden);
        }
        let policy = self
            .runtime
            .content_protection_policy_resolver()
            .resolve(principal.principal(), scope)
            .await?;
        if !policy.ready || policy.scope != *scope {
            return Err(AiError::RuntimeNotReady);
        }
        Ok((principal, policy))
    }

    async fn protect(
        &self,
        policy: &AiContentProtectionPolicy,
        context: ContentProtectionContext,
        value: serde_json::Value,
    ) -> Result<serde_json::Value, AiError> {
        let envelope = self
            .runtime
            .content_protector()
            .protect(policy, &context, value)
            .await
            .map_err(|error| match error {
                crate::ContentProtectionError::PolicyNotReady => AiError::RuntimeNotReady,
                _ => AiError::PersistenceFailed,
            })?;
        serde_json::to_value(envelope).map_err(|_| AiError::PersistenceFailed)
    }
}

#[async_trait]
impl AiLiveDeltaSink for OrmAiLiveDeltaService {
    async fn persist_batch(
        &self,
        lease: &AiRunLease,
        context: &AiLiveDeltaPersistenceContext,
        batch: &AiLiveDeltaBatch,
    ) -> Result<(), AiError> {
        if lease.session_id() != context.session_id()
            || lease.run_id() != context.run_id()
            || lease.attempt_id() != context.attempt_id()
            || lease.lease_generation() != context.lease_generation()
            || context.scope().kind.trim().is_empty()
            || context.scope().id.trim().is_empty()
            || !valid_reference(context.correlation_id())
            || context.provider_model().trim().is_empty()
            || context.provider_model().len() > 1_024
            || context
                .provider_response_id()
                .is_some_and(|value| !valid_reference(value))
            || batch.text().is_empty()
            || batch.text().len() > self.limits.maximum_batch_bytes
        {
            return Err(AiError::Conflict);
        }
        let session =
            AiSessionRecord::find_by_id(self.run_service.database(), &lease.session_id().0)
                .await
                .map_err(|error| map_orm(OrmPublicError::from(error)))?
                .ok_or(AiError::NotFound)?;
        validate_session_binding(&session, lease, context.scope())?;
        let (principal, policy) = self.current_policy(lease, context.scope()).await?;
        let event_id = Uuid::new_v4();
        let kind = match batch.kind() {
            AiLiveDeltaKind::Text => "text",
            AiLiveDeltaKind::ReasoningSummary => "reasoning_summary",
        };
        let payload = json!({
            "formatVersion": 1,
            "provisional": true,
            "runId": lease.run_id().0,
            "attemptId": lease.attempt_id(),
            "leaseGeneration": lease.lease_generation(),
            "providerKind": context.provider_kind().as_str(),
            "providerModel": context.provider_model(),
            "providerResponseId": context.provider_response_id(),
            "budgetReservationId": context.budget_reservation_id().0,
            "batchSequence": batch.sequence(),
            "kind": kind,
            "text": batch.text(),
            "byteCount": batch.text().len(),
        });
        let protected_payload = self
            .protect(
                &policy,
                ContentProtectionContext {
                    entity: "graphql_orm_ai_session_events".to_owned(),
                    row_id: event_id.to_string(),
                    field: "protected_payload".to_owned(),
                    scope: context.scope().clone(),
                },
                payload,
            )
            .await?;
        let (current, current_policy) = self.current_policy(lease, context.scope()).await?;
        if current_policy != policy
            || current.reference() != lease.principal_reference()
            || principal.reference() != lease.principal_reference()
        {
            return Err(AiError::ReauthorizationFailed);
        }
        self.run_service
            .append_live_delta_event(
                lease,
                PreparedLiveDeltaEvent {
                    id: event_id,
                    event_type: "provider_live_delta".to_owned(),
                    protected_payload,
                    correlation_id: context.correlation_id().to_owned(),
                    provider_kind: context.provider_kind().as_str().to_owned(),
                    provider_model: context.provider_model().to_owned(),
                    budget_reservation_id: context.budget_reservation_id().0,
                    expected_owner_principal_kind: session.owner_principal_kind,
                    expected_owner_subject: session.owner_subject,
                    expected_scope_kind: session.scope_kind,
                    expected_scope_id: session.scope_id,
                    expected_tenant_id: session.tenant_id,
                },
            )
            .await
    }
}

fn validate_session_binding(
    session: &AiSessionRecord,
    lease: &AiRunLease,
    scope: &AiScope,
) -> Result<(), AiError> {
    let expected_kind = match &lease.principal_reference().kind {
        PrincipalReferenceKind::UserSession => "user".to_owned(),
        PrincipalReferenceKind::ApiToken { principal_kind } => {
            format!("api_token:{principal_kind}")
        }
    };
    if session.id != lease.session_id().0
        || session.state != "active"
        || session.deleted_at.is_some()
        || session.owner_principal_kind != expected_kind
        || session.owner_subject != lease.principal_reference().subject
        || session.scope_kind != scope.kind
        || session.scope_id != scope.id
        || session.tenant_id != scope.tenant_id
        || lease
            .principal_reference()
            .tenant_id
            .as_ref()
            .is_some_and(|tenant_id| scope.tenant_id.as_ref() != Some(tenant_id))
    {
        return Err(AiError::Forbidden);
    }
    Ok(())
}

fn valid_reference(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= 1_024
        && value.bytes().all(|byte| !byte.is_ascii_control())
}

fn map_orm(error: OrmPublicError) -> AiError {
    use graphql_orm::graphql::errors::OrmErrorCode;
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
