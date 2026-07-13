//! Protected fenced checkpoints for replay-safe read-only coordinator phases.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::collections::BTreeSet;
use std::sync::Arc;

use agql_auth::{Clock, CurrentPrincipalResolver, PrincipalReferenceKind, ResolvedPrincipal};
use async_trait::async_trait;
use graphql_orm::graphql::errors::OrmPublicError;
use serde_json::json;
use sha2::{Digest, Sha256};
use time::Duration;
use uuid::Uuid;

use crate::orm_runs::{PreparedCoordinatorCheckpoint, PreparedCoordinatorCheckpointTool};
use crate::persistence::*;
use crate::{
    AiAccessPolicy, AiAgentCheckpointWriter, AiAgentContinuation, AiContentProtectionPolicy,
    AiContentProtectionPolicyResolver, AiContentProtector, AiError, AiPersistedApplicationToolCall,
    AiProviderCallResult, AiRunLease, AiScope, AiSessionAction, AiToolResultEgressRoute,
    ContentProtectionContext, OrmAiRunService,
};

/// Deployment bounds for one protected coordinator checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiCoordinatorCheckpointLimits {
    maximum_state_bytes: usize,
    maximum_principal_age: Duration,
}

impl AiCoordinatorCheckpointLimits {
    /// Creates validated checkpoint bounds.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless state capacity is in
    /// `1..=64 MiB` and principal freshness is positive and no more than one
    /// hour.
    pub fn new(
        maximum_state_bytes: usize,
        maximum_principal_age: Duration,
    ) -> Result<Self, AiError> {
        if !(1..=64 * 1024 * 1024).contains(&maximum_state_bytes)
            || !maximum_principal_age.is_positive()
            || maximum_principal_age > Duration::hours(1)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid coordinator-checkpoint limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_state_bytes,
            maximum_principal_age,
        })
    }

    /// Maximum serialized plaintext and protected-envelope size.
    pub const fn maximum_state_bytes(self) -> usize {
        self.maximum_state_bytes
    }

    /// Maximum accepted age of each freshly resolved principal.
    pub const fn maximum_principal_age(self) -> Duration {
        self.maximum_principal_age
    }
}

/// ORM-backed protected checkpoint writer for completed provider turns and
/// exact model-visible read-only tool batches.
///
/// Checkpoints are append-only and update the run's latest checkpoint through
/// the same current row-version fence. Provider usage must already be
/// authoritatively committed. Tool-batch checkpoints additionally verify every
/// referenced protected tool row and its egress decision in that transaction.
/// These records make later adoption review possible; this writer alone does
/// not authorize a new generation to resume them.
pub struct OrmAiCoordinatorCheckpointService {
    run_service: OrmAiRunService,
    principal_resolver: Arc<dyn CurrentPrincipalResolver>,
    access_policy: Arc<dyn AiAccessPolicy>,
    protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
    content_protector: Arc<dyn AiContentProtector>,
    clock: Arc<dyn Clock>,
    limits: AiCoordinatorCheckpointLimits,
}

impl OrmAiCoordinatorCheckpointService {
    /// Creates a protected fenced coordinator-checkpoint service.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        run_service: OrmAiRunService,
        principal_resolver: Arc<dyn CurrentPrincipalResolver>,
        access_policy: Arc<dyn AiAccessPolicy>,
        protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
        content_protector: Arc<dyn AiContentProtector>,
        clock: Arc<dyn Clock>,
        limits: AiCoordinatorCheckpointLimits,
    ) -> Self {
        Self {
            run_service,
            principal_resolver,
            access_policy,
            protection_policy,
            content_protector,
            clock,
            limits,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn persist(
        &self,
        lease: &AiRunLease,
        result: &AiProviderCallResult,
        scope: &AiScope,
        correlation_id: &str,
        route: &AiToolResultEgressRoute,
        provider_turns: u32,
        total_tool_calls: u32,
        checkpoint_kind: &str,
        completed_tools: &[AiPersistedApplicationToolCall],
        continuation: Option<&AiAgentContinuation>,
    ) -> Result<AiRunLease, AiError> {
        route.validate()?;
        if result.session_id() != lease.session_id()
            || result.run_id() != lease.run_id()
            || result.attempt_id() != lease.attempt_id()
            || result.lease_generation() != lease.lease_generation()
            || provider_turns == 0
            || total_tool_calls < u32::try_from(result.tool_calls().len()).unwrap_or(u32::MAX)
            || !valid_reference(correlation_id)
            || scope.kind.trim().is_empty()
            || scope.id.trim().is_empty()
        {
            return Err(AiError::Conflict);
        }
        match checkpoint_kind {
            "provider_turn_persisted" if completed_tools.is_empty() && continuation.is_none() => {}
            "tool_batch_persisted"
                if !completed_tools.is_empty()
                    && continuation.is_some()
                    && result.provider_response_id().is_some()
                    && completed_tools.len() == result.tool_calls().len() => {}
            _ => return Err(AiError::Conflict),
        }

        let session =
            AiSessionRecord::find_by_id(self.run_service.database(), &lease.session_id().0)
                .await
                .map_err(|error| map_orm(OrmPublicError::from(error)))?
                .ok_or(AiError::NotFound)?;
        validate_session_binding(&session, lease, scope)?;
        let (principal, policy) = self.current_policy(lease, scope).await?;
        let checkpoint_id = Uuid::new_v4();
        let mut protected_tool_values = Vec::with_capacity(completed_tools.len());
        let mut prepared_tools = Vec::with_capacity(completed_tools.len());
        let mut unique_ids = BTreeSet::new();
        let mut unique_provider_calls = BTreeSet::new();
        for (expected, persisted) in result.tool_calls().iter().zip(completed_tools) {
            let manifest = persisted
                .egress_manifest()
                .filter(|manifest| {
                    route.matches_manifest(
                        manifest,
                        lease,
                        scope,
                        result.provider_kind().as_str(),
                        result.provider_model(),
                    ) && manifest.sources.len() == 1
                        && manifest.sources[0].kind == "application_tool_result"
                        && manifest.sources[0].reference == persisted.id().0.to_string()
                })
                .ok_or(AiError::EgressDenied)?;
            if persisted.provider_call_id() != expected.call_id()
                || persisted.model_input().is_none_or(|input| {
                    !matches!(input, crate::ModelInputBlock::ToolResult {
                        call_id,
                        tool_id,
                        ..
                    } if call_id == expected.call_id() && tool_id == expected.tool_id().as_str())
                })
                || !unique_ids.insert(persisted.id())
                || !unique_provider_calls.insert(persisted.provider_call_id())
            {
                return Err(AiError::Conflict);
            }
            protected_tool_values.push(persisted.checkpoint_value().ok_or(AiError::EgressDenied)?);
            prepared_tools.push(PreparedCoordinatorCheckpointTool {
                id: persisted.id().0,
                provider_call_id: expected.call_id().to_owned(),
                tool_id: expected.tool_id().as_str().to_owned(),
                result_egress_manifest_hash: manifest.stable_hash(),
            });
        }
        let payload = json!({
            "formatVersion": 1,
            "checkpointKind": checkpoint_kind,
            "providerTurns": provider_turns,
            "totalToolCalls": total_tool_calls,
            "scope": scope,
            "correlationId": correlation_id,
            "resultEgressRoute": route.checkpoint_value(),
            "providerResult": result.checkpoint_value(),
            "completedTools": protected_tool_values,
            "continuation": continuation.map(AiAgentContinuation::checkpoint_value),
        });
        enforce_size(&payload, self.limits.maximum_state_bytes)?;
        let protected_state = self
            .protect(
                &policy,
                ContentProtectionContext {
                    entity: "graphql_orm_ai_run_checkpoints".to_owned(),
                    row_id: checkpoint_id.to_string(),
                    field: "protected_state".to_owned(),
                    scope: scope.clone(),
                },
                payload,
            )
            .await?;
        enforce_size(&protected_state, self.limits.maximum_state_bytes)?;

        let (current, current_policy) = self.current_policy(lease, scope).await?;
        if current_policy != policy
            || principal.reference() != lease.principal_reference()
            || current.reference() != lease.principal_reference()
        {
            return Err(AiError::ReauthorizationFailed);
        }
        let checkpoint_hash = checkpoint_hash(
            lease,
            checkpoint_kind,
            result,
            checkpoint_id,
            &protected_state,
        )?;
        self.run_service
            .append_coordinator_checkpoint(
                lease,
                PreparedCoordinatorCheckpoint {
                    id: checkpoint_id,
                    checkpoint_kind: checkpoint_kind.to_owned(),
                    provider_kind: result.provider_kind().as_str().to_owned(),
                    provider_model: result.provider_model().to_owned(),
                    provider_response_id: result.provider_response_id().map(str::to_owned),
                    budget_reservation_id: result.budget_reservation_id().0,
                    protected_state,
                    checkpoint_hash,
                    completed_tools: prepared_tools,
                },
            )
            .await
    }

    async fn current_policy(
        &self,
        lease: &AiRunLease,
        scope: &AiScope,
    ) -> Result<(ResolvedPrincipal, AiContentProtectionPolicy), AiError> {
        let principal = self
            .principal_resolver
            .resolve(lease.principal_reference())
            .await
            .map_err(|_| AiError::ReauthorizationFailed)?;
        let now = self.clock.now();
        if principal.resolved_at() > now
            || now - principal.resolved_at() >= self.limits.maximum_principal_age
            || principal
                .reference()
                .expires_at
                .is_some_and(|expires_at| expires_at <= now)
        {
            return Err(AiError::ReauthorizationFailed);
        }
        if !self
            .access_policy
            .can_access_scope(principal.principal(), scope, AiSessionAction::Write)
            .await
            .is_allowed()
            || !self
                .access_policy
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
            .protection_policy
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
            .content_protector
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
impl AiAgentCheckpointWriter for OrmAiCoordinatorCheckpointService {
    async fn persist_provider_turn(
        &self,
        lease: &AiRunLease,
        result: &AiProviderCallResult,
        scope: &AiScope,
        correlation_id: &str,
        route: &AiToolResultEgressRoute,
        provider_turns: u32,
        total_tool_calls: u32,
    ) -> Result<AiRunLease, AiError> {
        self.persist(
            lease,
            result,
            scope,
            correlation_id,
            route,
            provider_turns,
            total_tool_calls,
            "provider_turn_persisted",
            &[],
            None,
        )
        .await
    }

    async fn persist_tool_batch(
        &self,
        lease: &AiRunLease,
        result: &AiProviderCallResult,
        completed_tools: &[AiPersistedApplicationToolCall],
        continuation: &AiAgentContinuation,
        scope: &AiScope,
        correlation_id: &str,
        route: &AiToolResultEgressRoute,
        provider_turns: u32,
        total_tool_calls: u32,
    ) -> Result<AiRunLease, AiError> {
        self.persist(
            lease,
            result,
            scope,
            correlation_id,
            route,
            provider_turns,
            total_tool_calls,
            "tool_batch_persisted",
            completed_tools,
            Some(continuation),
        )
        .await
    }
}

fn checkpoint_hash(
    lease: &AiRunLease,
    kind: &str,
    result: &AiProviderCallResult,
    checkpoint_id: Uuid,
    protected_state: &serde_json::Value,
) -> Result<String, AiError> {
    let protected_state_hash = hex::encode(Sha256::digest(
        serde_json::to_vec(protected_state).map_err(|_| AiError::PersistenceFailed)?,
    ));
    let redacted = json!({
        "checkpointId": checkpoint_id,
        "runId": lease.run_id().0,
        "attemptId": lease.attempt_id(),
        "leaseGeneration": lease.lease_generation(),
        "kind": kind,
        "providerKind": result.provider_kind(),
        "providerModel": result.provider_model(),
        "providerResponseId": result.provider_response_id(),
        "budgetReservationId": result.budget_reservation_id().0,
        "protectedStateHash": protected_state_hash,
    });
    let encoded = serde_json::to_vec(&redacted).map_err(|_| AiError::PersistenceFailed)?;
    Ok(hex::encode(Sha256::digest(encoded)))
}

fn enforce_size(value: &serde_json::Value, maximum: usize) -> Result<(), AiError> {
    if serde_json::to_vec(value)
        .map_err(|_| AiError::PersistenceFailed)?
        .len()
        > maximum
    {
        return Err(AiError::InvalidInput(
            "coordinator checkpoint exceeds deployment limit".to_owned(),
        ));
    }
    Ok(())
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
