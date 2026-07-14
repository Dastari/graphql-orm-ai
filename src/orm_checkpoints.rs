//! Protected fenced checkpoints for replay-safe read-only coordinator phases.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use agql_auth::{Clock, CurrentPrincipalResolver, PrincipalReferenceKind, ResolvedPrincipal};
use async_trait::async_trait;
use graphql_orm::graphql::errors::OrmPublicError;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use time::Duration;
use uuid::Uuid;

use crate::orm_runs::{
    PreparedCoordinatorCheckpoint, PreparedCoordinatorCheckpointTool, coordinator_checkpoint_hash,
};
use crate::persistence::*;
use crate::{
    AiAccessPolicy, AiAdoptedReadOnlyToolBatch, AiAgentCheckpointAdopter, AiAgentCheckpointWriter,
    AiAgentContinuation, AiAgentRuleResolver, AiContentProtectionPolicy,
    AiContentProtectionPolicyResolver, AiContentProtector, AiEgressManifest, AiError,
    AiPersistedApplicationToolCall, AiProviderCallResult, AiResolvedRuleSet, AiRuleRunUsage,
    AiRunLease, AiScope, AiSessionAction, AiToolResultEgressRoute, ContentProtectionContext,
    ModelInputBlock, OrmAiRunService, ProtectedContentEnvelope, ProviderKind,
};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CoordinatorCheckpointPayload {
    format_version: u32,
    checkpoint_kind: String,
    provider_turns: u32,
    total_tool_calls: u32,
    scope: AiScope,
    rule_fingerprint: String,
    rule_usage: AiRuleRunUsage,
    correlation_id: String,
    result_egress_route: serde_json::Value,
    provider_result: ProviderResultSnapshot,
    completed_tools: Vec<ToolResultSnapshot>,
    continuation: Option<serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProviderResultSnapshot {
    format_version: u32,
    session_id: Uuid,
    run_id: Uuid,
    attempt_id: Uuid,
    lease_generation: i64,
    provider_kind: ProviderKind,
    provider_model: String,
    provider_response_id: Option<String>,
    budget_reservation_id: Uuid,
    previous_response_id: Option<String>,
    tool_calls: Vec<ProviderToolSnapshot>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProviderToolSnapshot {
    call_id: String,
    tool_id: String,
    #[serde(default)]
    provider_name: Option<String>,
    tool_fingerprint: String,
    arguments: serde_json::Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ToolResultSnapshot {
    id: Uuid,
    provider_call_id: String,
    state: String,
    model_input: ModelInputBlock,
    egress_manifest: AiEgressManifest,
}

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
/// Provider-retained and bounded stateless records make later adoption review
/// possible. This writer alone never authorizes a new generation to resume:
/// the adopter must reopen and validate every current and historical durable
/// proof under current authority first.
pub struct OrmAiCoordinatorCheckpointService {
    run_service: OrmAiRunService,
    principal_resolver: Arc<dyn CurrentPrincipalResolver>,
    access_policy: Arc<dyn AiAccessPolicy>,
    protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
    content_protector: Arc<dyn AiContentProtector>,
    rule_resolver: Arc<dyn AiAgentRuleResolver>,
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
        rule_resolver: Arc<dyn AiAgentRuleResolver>,
        clock: Arc<dyn Clock>,
        limits: AiCoordinatorCheckpointLimits,
    ) -> Self {
        Self {
            run_service,
            principal_resolver,
            access_policy,
            protection_policy,
            content_protector,
            rule_resolver,
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
        rules: &AiResolvedRuleSet,
        rule_usage: AiRuleRunUsage,
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
            || rules.target_scope() != scope
            || rule_usage.provider_calls() != u64::from(provider_turns)
        {
            return Err(AiError::Conflict);
        }
        let completed_tool_count = match checkpoint_kind {
            "provider_turn_persisted" if completed_tools.is_empty() && continuation.is_none() => {
                total_tool_calls
                    .checked_sub(u32::try_from(result.tool_calls().len()).unwrap_or(u32::MAX))
                    .ok_or(AiError::Conflict)?
            }
            "tool_batch_persisted"
                if !completed_tools.is_empty()
                    && continuation.is_some()
                    && completed_tools.len() == result.tool_calls().len() =>
            {
                total_tool_calls
            }
            _ => return Err(AiError::Conflict),
        };
        if rule_usage.steps()
            != u64::from(provider_turns).saturating_add(u64::from(completed_tool_count))
        {
            return Err(AiError::Conflict);
        }

        let session =
            AiSessionRecord::find_by_id(self.run_service.database(), &lease.session_id().0)
                .await
                .map_err(|error| map_orm(OrmPublicError::from(error)))?
                .ok_or(AiError::NotFound)?;
        validate_session_binding(&session, lease, scope)?;
        let (principal, policy) = self.current_policy(lease, scope).await?;
        let current_rules = self.rule_resolver.resolve_rules(lease, scope).await?;
        if current_rules.rules().fingerprint() != rules.fingerprint()
            || rule_usage.validate(&current_rules).is_err()
        {
            return Err(AiError::ReauthorizationFailed);
        }
        let checkpoint_id = Uuid::new_v4();
        let mut protected_tool_values = Vec::with_capacity(completed_tools.len());
        let mut prepared_tools = Vec::with_capacity(completed_tools.len());
        let mut unique_ids = BTreeSet::new();
        let mut unique_provider_calls = BTreeSet::new();
        let mut unique_manifest_hashes = BTreeSet::new();
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
                || !unique_manifest_hashes.insert(manifest.stable_hash())
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
        if let Some(continuation) = continuation {
            for transfer in continuation.replay_transfers() {
                if !route.matches_manifest(
                    transfer,
                    lease,
                    scope,
                    result.provider_kind().as_str(),
                    result.provider_model(),
                ) || transfer.sources.len() != 1
                    || transfer.sources[0].kind != "application_tool_result"
                    || Uuid::parse_str(&transfer.sources[0].reference).is_err()
                    || !unique_manifest_hashes.insert(transfer.stable_hash())
                {
                    return Err(AiError::EgressDenied);
                }
            }
        }
        let payload = json!({
            "formatVersion": 2,
            "checkpointKind": checkpoint_kind,
            "providerTurns": provider_turns,
            "totalToolCalls": total_tool_calls,
            "scope": scope,
            "ruleFingerprint": rules.fingerprint(),
            "ruleUsage": rule_usage,
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
        let final_rules = self.rule_resolver.resolve_rules(lease, scope).await?;
        if current_policy != policy
            || principal.reference() != lease.principal_reference()
            || current.reference() != lease.principal_reference()
            || final_rules.rules().fingerprint() != rules.fingerprint()
            || rule_usage.validate(&final_rules).is_err()
        {
            return Err(AiError::ReauthorizationFailed);
        }
        let checkpoint_hash = coordinator_checkpoint_hash(
            lease.run_id(),
            lease.attempt_id(),
            lease.lease_generation(),
            checkpoint_id,
            checkpoint_kind,
            result.provider_kind().as_str(),
            result.provider_model(),
            result.provider_response_id(),
            result.budget_reservation_id().0,
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

    async fn open(
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
            .map_err(|error| match error {
                crate::ContentProtectionError::PolicyNotReady => AiError::RuntimeNotReady,
                _ => AiError::PersistenceFailed,
            })
    }

    async fn adopt(
        &self,
        lease: &AiRunLease,
        checkpoint_id: Uuid,
    ) -> Result<AiAdoptedReadOnlyToolBatch, AiError> {
        let session =
            AiSessionRecord::find_by_id(self.run_service.database(), &lease.session_id().0)
                .await
                .map_err(|error| map_orm(OrmPublicError::from(error)))?
                .ok_or(AiError::NotFound)?;
        let scope = AiScope {
            kind: session.scope_kind.clone(),
            id: session.scope_id.clone(),
            tenant_id: session.tenant_id.clone(),
        };
        validate_session_binding(&session, lease, &scope)?;
        let (principal, policy) = self.current_policy(lease, &scope).await?;
        let principal_kind = principal_reference_kind(lease.principal_reference());
        let checkpoint =
            AiRunCheckpointRecord::find_by_id(self.run_service.database(), &checkpoint_id)
                .await
                .map_err(|error| map_orm(OrmPublicError::from(error)))?
                .ok_or(AiError::NotFound)?;
        let provider_response_id = checkpoint.provider_response_id.as_deref();
        if provider_response_id.is_some_and(|value| !valid_reference(value)) {
            return Err(AiError::Conflict);
        }
        let budget_reservation_id = checkpoint.budget_reservation_id.ok_or(AiError::Conflict)?;
        let protected_state = checkpoint
            .protected_state
            .as_ref()
            .ok_or(AiError::Conflict)?;
        if checkpoint.run_id != lease.run_id().0
            || checkpoint.checkpoint_kind != "tool_batch_persisted"
            || checkpoint.assistant_message_id.is_some()
        {
            return Err(AiError::Conflict);
        }
        enforce_size(protected_state, self.limits.maximum_state_bytes)?;
        let reservation = AiBudgetReservationRecord::find_by_id(
            self.run_service.database(),
            &budget_reservation_id,
        )
        .await
        .map_err(|error| map_orm(OrmPublicError::from(error)))?
        .ok_or(AiError::NotFound)?;
        if !checkpoint_budget_matches(
            &reservation,
            lease,
            checkpoint.attempt_id,
            checkpoint.lease_generation,
            &scope,
            &principal_kind,
            &reservation.provider_kind,
            &reservation.provider_model,
        ) {
            return Err(AiError::Conflict);
        }
        let expected_hash = coordinator_checkpoint_hash(
            crate::AiRunId(checkpoint.run_id),
            checkpoint.attempt_id,
            checkpoint.lease_generation,
            checkpoint.id,
            &checkpoint.checkpoint_kind,
            &reservation.provider_kind,
            &reservation.provider_model,
            provider_response_id,
            budget_reservation_id,
            protected_state,
        )?;
        if checkpoint.checkpoint_hash != expected_hash {
            return Err(AiError::Conflict);
        }
        let opened = self
            .open(
                &policy,
                ContentProtectionContext {
                    entity: "graphql_orm_ai_run_checkpoints".to_owned(),
                    row_id: checkpoint_id.to_string(),
                    field: "protected_state".to_owned(),
                    scope: scope.clone(),
                },
                protected_state,
            )
            .await?;
        enforce_size(&opened, self.limits.maximum_state_bytes)?;
        let payload: CoordinatorCheckpointPayload =
            serde_json::from_value(opened).map_err(|_| AiError::PersistenceFailed)?;
        if payload.format_version != 2
            || payload.checkpoint_kind != "tool_batch_persisted"
            || payload.scope != scope
            || payload.rule_fingerprint.len() != 64
            || payload.rule_usage.provider_calls() != u64::from(payload.provider_turns)
            || payload.rule_usage.steps()
                != u64::from(payload.provider_turns)
                    .saturating_add(u64::from(payload.total_tool_calls))
            || !valid_reference(&payload.correlation_id)
            || payload.provider_turns == 0
            || payload.total_tool_calls == 0
            || payload.completed_tools.is_empty()
            || payload.completed_tools.len() > 4_096
            || payload.completed_tools.len() != payload.provider_result.tool_calls.len()
        {
            return Err(AiError::Conflict);
        }
        let provider = &payload.provider_result;
        if provider.format_version != 1
            || provider.session_id != lease.session_id().0
            || provider.run_id != lease.run_id().0
            || provider.attempt_id != checkpoint.attempt_id
            || provider.lease_generation != checkpoint.lease_generation
            || provider.provider_kind.as_str() != reservation.provider_kind
            || provider.provider_model != reservation.provider_model
            || provider.provider_response_id.as_deref() != provider_response_id
            || provider.budget_reservation_id != budget_reservation_id
            || provider.tool_calls.is_empty()
            || provider
                .previous_response_id
                .as_deref()
                .is_some_and(|value| !valid_reference(value))
        {
            return Err(AiError::Conflict);
        }
        let route = AiToolResultEgressRoute::from_checkpoint_value(payload.result_egress_route)?;
        let continuation = AiAgentContinuation::from_checkpoint_value(
            payload.continuation.ok_or(AiError::Conflict)?,
        )?;
        let stateless_evidence = continuation.stateless_evidence()?;
        if continuation.provider_response_id() != provider_response_id
            || continuation.input().len() != payload.completed_tools.len()
            || continuation.transfers().len() != payload.completed_tools.len()
        {
            return Err(AiError::Conflict);
        }
        match (&stateless_evidence, provider_response_id) {
            (None, Some(_)) => {}
            (Some(evidence), None)
                if provider.previous_response_id.is_none()
                    && evidence.provider_turns == payload.provider_turns
                    && u32::try_from(evidence.tools.len()).ok()
                        == Some(payload.total_tool_calls)
                    && evidence.current_tool_count == payload.completed_tools.len()
                    && evidence.tools.len()
                        == continuation
                            .replay_transfers()
                            .len()
                            .saturating_add(continuation.transfers().len()) => {}
            _ => return Err(AiError::Conflict),
        }
        let expected_turn_index = i64::from(
            payload
                .provider_turns
                .checked_sub(1)
                .ok_or(AiError::Conflict)?,
        );
        let mut durable_ids = BTreeSet::new();
        let mut provider_call_ids = BTreeSet::new();
        let mut historical_turn_budgets = BTreeMap::new();
        let mut unique_historical_budgets = BTreeSet::new();
        let current_stateless_evidence = stateless_evidence
            .as_ref()
            .map(|evidence| &evidence.tools[evidence.tools.len() - evidence.current_tool_count..]);
        for (index, (((tool, provider_tool), input), transfer)) in payload
            .completed_tools
            .iter()
            .zip(&provider.tool_calls)
            .zip(continuation.input())
            .zip(continuation.transfers())
            .enumerate()
        {
            if !durable_ids.insert(tool.id)
                || !provider_call_ids.insert(tool.provider_call_id.as_str())
                || tool.provider_call_id != provider_tool.call_id
                || tool.model_input != *input
                || tool.egress_manifest != *transfer
            {
                return Err(AiError::Conflict);
            }
            let ModelInputBlock::ToolResult {
                call_id,
                tool_id,
                output,
            } = input
            else {
                return Err(AiError::Conflict);
            };
            if current_stateless_evidence.is_some_and(|evidence| {
                evidence.get(index).is_none_or(|item| {
                    item.call_id != provider_tool.call_id
                        || item.tool_id != provider_tool.tool_id
                        || provider_tool.provider_name.as_deref()
                            != Some(item.provider_name.as_str())
                        || item.tool_fingerprint != provider_tool.tool_fingerprint
                        || item.arguments != provider_tool.arguments
                        || item.output != *output
                })
            }) || call_id != &provider_tool.call_id
                || tool_id != &provider_tool.tool_id
                || !route.matches_manifest(
                    transfer,
                    lease,
                    &scope,
                    &reservation.provider_kind,
                    &reservation.provider_model,
                )
                || transfer.sources.len() != 1
                || transfer.sources[0].kind != "application_tool_result"
                || transfer.sources[0].reference != tool.id.to_string()
            {
                return Err(AiError::EgressDenied);
            }
            let call = AiToolCallRecord::find_by_id(self.run_service.database(), &tool.id)
                .await
                .map_err(|error| map_orm(OrmPublicError::from(error)))?
                .ok_or(AiError::NotFound)?;
            let step = AiRunStepRecord::find_by_id(self.run_service.database(), &tool.id)
                .await
                .map_err(|error| map_orm(OrmPublicError::from(error)))?
                .ok_or(AiError::NotFound)?;
            let manifest_hash = transfer.stable_hash();
            let classification = classification_value(transfer.maximum_classification());
            if call.run_id != lease.run_id().0
                || call.lease_generation != checkpoint.lease_generation
                || call.provider_call_id != provider_tool.call_id
                || call.provider_kind.as_deref() != Some(reservation.provider_kind.as_str())
                || call.provider_model.as_deref() != Some(reservation.provider_model.as_str())
                || call.provider_response_id.as_deref() != provider_response_id
                || call.budget_reservation_id != Some(budget_reservation_id)
                || call.provider_turn_index != expected_turn_index
                || usize::try_from(call.tool_call_index).ok() != Some(index)
                || call.tool_id != provider_tool.tool_id
                || call.tool_fingerprint != provider_tool.tool_fingerprint
                || call.argument_hash != canonical_json_hash(&provider_tool.arguments)?
                || call.state != tool.state
                || !matches!(call.state.as_str(), "completed" | "execution_failed")
                || call.result_egress_manifest_hash.as_deref() != Some(manifest_hash.as_str())
                || call.result_egress_decision_id.is_none()
                || call.authorization_code.is_none()
                || call.disclosure_schema_fingerprint.is_none()
                || call.result_classification.as_deref() != Some(classification)
                || (call.state == "completed"
                    && (call.authorization_policy_version.is_none()
                        || call.authorization_state_digest.is_none()))
                || call.completed_at.is_none()
                || call.correlation_id.as_deref() != Some(payload.correlation_id.as_str())
                || step.run_id != lease.run_id().0
                || step.lease_generation != checkpoint.lease_generation
                || step.state != call.state
                || step.finished_at.is_none()
            {
                return Err(AiError::Conflict);
            }
            let arguments = self
                .open(
                    &policy,
                    ContentProtectionContext {
                        entity: "graphql_orm_ai_tool_calls".to_owned(),
                        row_id: tool.id.to_string(),
                        field: "protected_arguments".to_owned(),
                        scope: scope.clone(),
                    },
                    &call.protected_arguments,
                )
                .await?;
            let result = self
                .open(
                    &policy,
                    ContentProtectionContext {
                        entity: "graphql_orm_ai_tool_calls".to_owned(),
                        row_id: tool.id.to_string(),
                        field: "protected_result".to_owned(),
                        scope: scope.clone(),
                    },
                    call.protected_result.as_ref().ok_or(AiError::Conflict)?,
                )
                .await?;
            if arguments != provider_tool.arguments || result != *output {
                return Err(AiError::Conflict);
            }
            let decision_id = call.result_egress_decision_id.ok_or(AiError::Conflict)?;
            let event = AiEgressEventRecord::find_by_id(self.run_service.database(), &decision_id)
                .await
                .map_err(|error| map_orm(OrmPublicError::from(error)))?
                .ok_or(AiError::NotFound)?;
            if event.run_id != Some(lease.run_id().0)
                || event.principal_subject != lease.principal_reference().subject
                || event.scope_kind != scope.kind
                || event.scope_id != scope.id
                || event.manifest_hash != manifest_hash
                || event.destination != transfer.destination
                || event.capability != "tool_result"
                || event.classification != classification
                || event.outcome != "allow"
                || u64::try_from(event.estimated_bytes).ok() != Some(transfer.estimated_bytes)
                || u64::try_from(event.estimated_tokens).ok() != Some(transfer.estimated_tokens)
            {
                return Err(AiError::EgressDenied);
            }
        }
        if let Some(evidence) = &stateless_evidence {
            let historical_count = evidence
                .tools
                .len()
                .checked_sub(evidence.current_tool_count)
                .ok_or(AiError::Conflict)?;
            for (item, transfer) in evidence.tools[..historical_count]
                .iter()
                .zip(continuation.replay_transfers())
            {
                if !route.matches_manifest(
                    transfer,
                    lease,
                    &scope,
                    &reservation.provider_kind,
                    &reservation.provider_model,
                ) || transfer.sources.len() != 1
                    || transfer.sources[0].kind != "application_tool_result"
                {
                    return Err(AiError::EgressDenied);
                }
                let tool_id = Uuid::parse_str(&transfer.sources[0].reference)
                    .map_err(|_| AiError::Conflict)?;
                if !durable_ids.insert(tool_id) || !provider_call_ids.insert(item.call_id.as_str())
                {
                    return Err(AiError::Conflict);
                }
                let call = AiToolCallRecord::find_by_id(self.run_service.database(), &tool_id)
                    .await
                    .map_err(|error| map_orm(OrmPublicError::from(error)))?
                    .ok_or(AiError::NotFound)?;
                let step = AiRunStepRecord::find_by_id(self.run_service.database(), &tool_id)
                    .await
                    .map_err(|error| map_orm(OrmPublicError::from(error)))?
                    .ok_or(AiError::NotFound)?;
                let historical_budget_id = call.budget_reservation_id.ok_or(AiError::Conflict)?;
                let historical_budget = AiBudgetReservationRecord::find_by_id(
                    self.run_service.database(),
                    &historical_budget_id,
                )
                .await
                .map_err(|error| map_orm(OrmPublicError::from(error)))?
                .ok_or(AiError::NotFound)?;
                let manifest_hash = transfer.stable_hash();
                let classification = classification_value(transfer.maximum_classification());
                let turn_budget_matches =
                    match historical_turn_budgets.entry(item.provider_turn_index) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            if historical_budget_id == budget_reservation_id
                                || !unique_historical_budgets.insert(historical_budget_id)
                            {
                                false
                            } else {
                                entry.insert(historical_budget_id);
                                true
                            }
                        }
                        std::collections::btree_map::Entry::Occupied(entry) => {
                            *entry.get() == historical_budget_id
                        }
                    };
                if !turn_budget_matches
                    || !checkpoint_budget_matches(
                        &historical_budget,
                        lease,
                        checkpoint.attempt_id,
                        checkpoint.lease_generation,
                        &scope,
                        &principal_kind,
                        &reservation.provider_kind,
                        &reservation.provider_model,
                    )
                    || call.run_id != lease.run_id().0
                    || call.lease_generation != checkpoint.lease_generation
                    || call.provider_call_id != item.call_id
                    || call.provider_kind.as_deref() != Some(reservation.provider_kind.as_str())
                    || call.provider_model.as_deref() != Some(reservation.provider_model.as_str())
                    || call.provider_response_id.is_some()
                    || call.provider_turn_index != item.provider_turn_index
                    || usize::try_from(call.tool_call_index).ok() != Some(item.tool_call_index)
                    || call.tool_id != item.tool_id
                    || call.tool_fingerprint != item.tool_fingerprint
                    || call.argument_hash != canonical_json_hash(&item.arguments)?
                    || !matches!(call.state.as_str(), "completed" | "execution_failed")
                    || call.result_egress_manifest_hash.as_deref() != Some(manifest_hash.as_str())
                    || call.result_egress_decision_id.is_none()
                    || call.authorization_code.is_none()
                    || call.disclosure_schema_fingerprint.is_none()
                    || call.result_classification.as_deref() != Some(classification)
                    || (call.state == "completed"
                        && (call.authorization_policy_version.is_none()
                            || call.authorization_state_digest.is_none()))
                    || call.completed_at.is_none()
                    || call
                        .correlation_id
                        .as_deref()
                        .is_none_or(|value| !valid_reference(value))
                    || step.run_id != lease.run_id().0
                    || step.lease_generation != checkpoint.lease_generation
                    || step.state != call.state
                    || step.finished_at.is_none()
                {
                    return Err(AiError::Conflict);
                }
                let arguments = self
                    .open(
                        &policy,
                        ContentProtectionContext {
                            entity: "graphql_orm_ai_tool_calls".to_owned(),
                            row_id: tool_id.to_string(),
                            field: "protected_arguments".to_owned(),
                            scope: scope.clone(),
                        },
                        &call.protected_arguments,
                    )
                    .await?;
                let result = self
                    .open(
                        &policy,
                        ContentProtectionContext {
                            entity: "graphql_orm_ai_tool_calls".to_owned(),
                            row_id: tool_id.to_string(),
                            field: "protected_result".to_owned(),
                            scope: scope.clone(),
                        },
                        call.protected_result.as_ref().ok_or(AiError::Conflict)?,
                    )
                    .await?;
                if arguments != item.arguments || result != item.output {
                    return Err(AiError::Conflict);
                }
                let decision_id = call.result_egress_decision_id.ok_or(AiError::Conflict)?;
                let event =
                    AiEgressEventRecord::find_by_id(self.run_service.database(), &decision_id)
                        .await
                        .map_err(|error| map_orm(OrmPublicError::from(error)))?
                        .ok_or(AiError::NotFound)?;
                if event.run_id != Some(lease.run_id().0)
                    || event.principal_subject != lease.principal_reference().subject
                    || event.scope_kind != scope.kind
                    || event.scope_id != scope.id
                    || event.manifest_hash != manifest_hash
                    || event.destination != transfer.destination
                    || event.capability != "tool_result"
                    || event.classification != classification
                    || event.outcome != "allow"
                    || u64::try_from(event.estimated_bytes).ok() != Some(transfer.estimated_bytes)
                    || u64::try_from(event.estimated_tokens).ok() != Some(transfer.estimated_tokens)
                {
                    return Err(AiError::EgressDenied);
                }
            }
            if historical_turn_budgets.len()
                != usize::try_from(payload.provider_turns.saturating_sub(1)).unwrap_or(usize::MAX)
            {
                return Err(AiError::Conflict);
            }
        }
        let (current, current_policy) = self.current_policy(lease, &scope).await?;
        let current_rules = self.rule_resolver.resolve_rules(lease, &scope).await?;
        if current_policy != policy
            || principal.reference() != lease.principal_reference()
            || current.reference() != lease.principal_reference()
            || current_rules.rules().fingerprint() != payload.rule_fingerprint
            || payload.rule_usage.validate(&current_rules).is_err()
        {
            return Err(AiError::ReauthorizationFailed);
        }
        Ok(AiAdoptedReadOnlyToolBatch::new(
            checkpoint_id,
            payload.provider_turns,
            payload.total_tool_calls,
            scope,
            continuation,
            payload.rule_fingerprint,
            payload.rule_usage,
        ))
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
        rules: &AiResolvedRuleSet,
        rule_usage: AiRuleRunUsage,
        provider_turns: u32,
        total_tool_calls: u32,
    ) -> Result<AiRunLease, AiError> {
        self.persist(
            lease,
            result,
            scope,
            correlation_id,
            route,
            rules,
            rule_usage,
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
        rules: &AiResolvedRuleSet,
        rule_usage: AiRuleRunUsage,
        provider_turns: u32,
        total_tool_calls: u32,
    ) -> Result<AiRunLease, AiError> {
        self.persist(
            lease,
            result,
            scope,
            correlation_id,
            route,
            rules,
            rule_usage,
            provider_turns,
            total_tool_calls,
            "tool_batch_persisted",
            completed_tools,
            Some(continuation),
        )
        .await
    }
}

#[async_trait]
impl AiAgentCheckpointAdopter for OrmAiCoordinatorCheckpointService {
    async fn adopt_tool_batch(
        &self,
        lease: &AiRunLease,
    ) -> Result<Option<AiAdoptedReadOnlyToolBatch>, AiError> {
        match lease.latest_checkpoint_id() {
            Some(checkpoint_id) => self.adopt(lease, checkpoint_id).await.map(Some),
            None => Ok(None),
        }
    }

    async fn consume_before_provider(
        &self,
        lease: &AiRunLease,
        checkpoint_id: Uuid,
    ) -> Result<AiRunLease, AiError> {
        self.run_service
            .consume_adoption_checkpoint(lease, checkpoint_id)
            .await
    }
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

fn canonical_json_hash(value: &serde_json::Value) -> Result<String, AiError> {
    let encoded =
        serde_json::to_vec(&canonical_json(value)).map_err(|_| AiError::PersistenceFailed)?;
    Ok(hex::encode(Sha256::digest(encoded)))
}

fn canonical_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(canonical_json).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), canonical_json(value)))
                .collect(),
        ),
        _ => value.clone(),
    }
}

const fn classification_value(value: crate::DataClassification) -> &'static str {
    match value {
        crate::DataClassification::Public => "public",
        crate::DataClassification::Internal => "internal",
        crate::DataClassification::Confidential => "confidential",
        crate::DataClassification::Restricted => "restricted",
        crate::DataClassification::Secret => "secret",
    }
}

fn validate_session_binding(
    session: &AiSessionRecord,
    lease: &AiRunLease,
    scope: &AiScope,
) -> Result<(), AiError> {
    let expected_kind = principal_reference_kind(lease.principal_reference());
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

fn principal_reference_kind(reference: &agql_auth::PrincipalReference) -> String {
    match &reference.kind {
        PrincipalReferenceKind::UserSession => "user".to_owned(),
        PrincipalReferenceKind::ApiToken { principal_kind } => {
            format!("api_token:{principal_kind}")
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn checkpoint_budget_matches(
    budget: &AiBudgetReservationRecord,
    lease: &AiRunLease,
    attempt_id: Uuid,
    lease_generation: i64,
    scope: &AiScope,
    principal_kind: &str,
    provider_kind: &str,
    provider_model: &str,
) -> bool {
    budget.session_id == lease.session_id().0
        && budget.run_id == lease.run_id().0
        && budget.attempt_id == attempt_id
        && budget.lease_generation == lease_generation
        && budget.scope_kind == scope.kind
        && budget.scope_id == scope.id
        && budget.tenant_id == scope.tenant_id
        && budget.principal_kind == principal_kind
        && budget.principal_subject == lease.principal_reference().subject
        && budget.provider_kind == provider_kind
        && budget.provider_model == provider_model
        && !budget.pricing_policy_version.trim().is_empty()
        && budget.state == "committed"
        && budget.reserved_input_tokens >= 0
        && budget.reserved_output_tokens >= 0
        && budget.reserved_tool_units >= 0
        && budget.reserved_image_units >= 0
        && budget.reserved_cost_microunits >= 0
        && budget.reserved_runs == 1
        && matches!(
            (
                budget.actual_input_tokens,
                budget.actual_cached_input_tokens,
                budget.actual_output_tokens,
                budget.actual_tool_units,
                budget.actual_image_units,
                budget.actual_cost_microunits,
                budget.actual_runs,
            ),
            (
                Some(input),
                Some(cached),
                Some(output),
                Some(tools),
                Some(images),
                Some(cost),
                Some(1),
            ) if input >= 0
                && cached >= 0
                && cached <= input
                && output >= 0
                && tools >= 0
                && images >= 0
                && cost >= 0
        )
        && budget.reconciled_at.is_some()
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
