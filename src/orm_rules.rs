//! ORM-backed GraphQL management and runtime resolution of hierarchical rules.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::collections::BTreeSet;
use std::sync::Arc;

use agql_auth::{
    AuthPrincipal, Clock, CurrentPrincipalResolver, RecentMfaPolicy, ResolvedPrincipal,
};
use async_trait::async_trait;
use graphql_orm::db::Database;
use graphql_orm::graphql::errors::{OrmErrorCode, OrmPublicError};
use graphql_orm::graphql::orm::{
    ConditionalUpdateOutcome, DefaultWriteBackend, TransactionError, TransactionMode,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::persistence::*;
use crate::{
    AiAgentRuleResolution, AiAgentRuleResolver, AiAppliedRuleLayer, AiError, AiResolvedRuleSet,
    AiRuleAccessPolicy, AiRuleAction, AiRuleApprovalRequirement, AiRuleBudgetCeilings,
    AiRuleConstraints, AiRuleDeploymentLimits, AiRuleHierarchyResolver, AiRulePolicyService,
    AiRulePolicyView, AiRuleProviderCapability, AiRunLease, AiScope, DataClassification,
    ProviderKind, SetAiRulePolicyInput, ToolMaturity,
};

const RULE_FORMAT_VERSION: u32 = 1;

/// Fresh-principal bounds for durable coordinator rule resolution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiCurrentRuleResolverLimits {
    maximum_principal_age: time::Duration,
}

impl AiCurrentRuleResolverLimits {
    /// Creates validated current-rule resolution bounds.
    ///
    /// # Errors
    ///
    /// Returns an error unless principal freshness is positive and no more
    /// than one hour.
    pub fn new(maximum_principal_age: time::Duration) -> Result<Self, AiError> {
        if !maximum_principal_age.is_positive() || maximum_principal_age > time::Duration::hours(1)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid current-rule resolver limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_principal_age,
        })
    }

    /// Maximum accepted age of each principal resolution.
    pub const fn maximum_principal_age(self) -> time::Duration {
        self.maximum_principal_age
    }
}

/// Current-principal adapter from durable leases to hierarchical rule sets.
///
/// The adapter resolves the principal and complete rule lineage twice around
/// the decision. It returns constraint evidence only and grants no provider,
/// tool, resolver, egress, budget, or approval authority.
pub struct OrmAiCurrentRuleResolver {
    principal_resolver: Arc<dyn CurrentPrincipalResolver>,
    rule_service: Arc<dyn AiRulePolicyService>,
    clock: Arc<dyn Clock>,
    limits: AiCurrentRuleResolverLimits,
}

impl OrmAiCurrentRuleResolver {
    /// Creates a fail-closed durable rule resolver.
    pub fn new(
        principal_resolver: Arc<dyn CurrentPrincipalResolver>,
        rule_service: Arc<dyn AiRulePolicyService>,
        clock: Arc<dyn Clock>,
        limits: AiCurrentRuleResolverLimits,
    ) -> Self {
        Self {
            principal_resolver,
            rule_service,
            clock,
            limits,
        }
    }

    async fn current_principal(&self, lease: &AiRunLease) -> Result<ResolvedPrincipal, AiError> {
        let principal = self
            .principal_resolver
            .resolve(lease.principal_reference())
            .await
            .map_err(|_| AiError::ReauthorizationFailed)?;
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
        Ok(principal)
    }
}

#[async_trait]
impl AiAgentRuleResolver for OrmAiCurrentRuleResolver {
    async fn resolve_rules(
        &self,
        lease: &AiRunLease,
        scope: &AiScope,
    ) -> Result<AiAgentRuleResolution, AiError> {
        let first_principal = self.current_principal(lease).await?;
        let first = self
            .rule_service
            .resolve_for_run(first_principal.principal(), scope.clone())
            .await?;
        let second_principal = self.current_principal(lease).await?;
        let second = self
            .rule_service
            .resolve_for_run(second_principal.principal(), scope.clone())
            .await?;
        if first.target_scope() != scope
            || second.target_scope() != scope
            || first.fingerprint() != second.fingerprint()
            || first_principal.reference() != second_principal.reference()
        {
            return Err(AiError::ReauthorizationFailed);
        }
        AiAgentRuleResolution::new(second, self.clock.now())
    }
}

/// Concrete generated-ORM-only hierarchical rule service.
///
/// Scope lineage remains application-defined through
/// [`AiRuleHierarchyResolver`]. Every configured layer is current-principal
/// authorized and intersected with immutable deployment limits. The resolved
/// value grants no capability and must still be combined with ordinary tool,
/// resolver, egress, provider, approval, and budget authorization.
#[derive(Clone)]
pub struct OrmAiRulePolicyService {
    database: Database<DefaultWriteBackend>,
    access_policy: Arc<dyn AiRuleAccessPolicy>,
    hierarchy_resolver: Arc<dyn AiRuleHierarchyResolver>,
    recent_mfa_policy: RecentMfaPolicy,
    clock: Arc<dyn Clock>,
    limits: AiRuleDeploymentLimits,
}

impl OrmAiRulePolicyService {
    /// Creates a fail-closed hierarchical rule service.
    pub fn new(
        database: Database<DefaultWriteBackend>,
        access_policy: Arc<dyn AiRuleAccessPolicy>,
        hierarchy_resolver: Arc<dyn AiRuleHierarchyResolver>,
        recent_mfa_policy: RecentMfaPolicy,
        clock: Arc<dyn Clock>,
        limits: AiRuleDeploymentLimits,
    ) -> Self {
        Self {
            database,
            access_policy,
            hierarchy_resolver,
            recent_mfa_policy,
            clock,
            limits,
        }
    }

    /// Returns the underlying ORM handle for host schema wiring.
    pub fn database(&self) -> &Database<DefaultWriteBackend> {
        &self.database
    }

    async fn require_access(
        &self,
        principal: &AuthPrincipal,
        scope: &AiScope,
        action: AiRuleAction,
    ) -> Result<(), AiError> {
        validate_scope(scope)?;
        if self
            .access_policy
            .can_access_rule(principal, scope, action)
            .await
        {
            Ok(())
        } else {
            Err(AiError::Forbidden)
        }
    }

    fn require_recent_mfa(&self, principal: &AuthPrincipal) -> Result<(), AiError> {
        let user = principal.as_user().ok_or(AiError::RecentMfaRequired)?;
        self.recent_mfa_policy
            .evaluate(user, self.clock.as_ref())
            .map_err(|_| AiError::RecentMfaRequired)
    }

    async fn load_exact(&self, scope: &AiScope) -> Result<Option<AiScopePolicyRecord>, AiError> {
        AiScopePolicyRecord::find_by_id(&self.database, &rule_policy_id(scope))
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))
    }
}

#[async_trait]
impl AiRulePolicyService for OrmAiRulePolicyService {
    async fn policy(
        &self,
        principal: &AuthPrincipal,
        scope: AiScope,
    ) -> Result<Option<AiRulePolicyView>, AiError> {
        self.require_access(principal, &scope, AiRuleAction::Read)
            .await?;
        self.load_exact(&scope)
            .await?
            .map(|record| rule_view(&record, &scope))
            .transpose()
    }

    async fn set_policy(
        &self,
        principal: &AuthPrincipal,
        input: SetAiRulePolicyInput,
    ) -> Result<AiRulePolicyView, AiError> {
        let (scope, constraints, expected_version) = input.into_scope_and_constraints()?;
        self.require_access(principal, &scope, AiRuleAction::Manage)
            .await?;
        self.require_recent_mfa(principal)?;
        if !constraints.is_no_broader_than(self.limits.ceiling())? {
            return Err(AiError::InvalidInput(
                "scope rule exceeds immutable deployment limits".to_owned(),
            ));
        }
        let id = rule_policy_id(&scope);
        let capabilities = stored_policy_value(&scope, &constraints)?;
        let maximum_tool_maturity = maturity_value(constraints.maximum_tool_maturity).to_owned();
        let actor_kind = principal_kind(principal);
        let actor_subject = principal.subject().to_owned();
        let scope_for_tx = scope.clone();
        let record = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = tx
                        .find_by_id::<AiScopePolicyRecord>(&id)
                        .await
                        .map_err(OrmPublicError::from)?;
                    let record = match (current, expected_version) {
                        (None, None) => tx
                            .insert::<AiScopePolicyRecord>(CreateAiScopePolicyRecordInput {
                                id,
                                scope_kind: scope_for_tx.kind.clone(),
                                scope_id: scope_for_tx.id.clone(),
                                tenant_id: scope_for_tx.tenant_id.clone(),
                                enabled: constraints.enabled,
                                maximum_tool_maturity,
                                capabilities,
                            })
                            .await
                            .map_err(OrmPublicError::from)?,
                        (Some(current), Some(expected)) => {
                            validate_record_identity(&current, &scope_for_tx)?;
                            let update = tx
                                .compare_and_swap::<AiScopePolicyRecord>(
                                    &id,
                                    expected,
                                    AiScopePolicyRecordWhereInput::default(),
                                    UpdateAiScopePolicyRecordInput {
                                        enabled: Some(constraints.enabled),
                                        maximum_tool_maturity: Some(maximum_tool_maturity),
                                        capabilities: Some(capabilities),
                                        ..Default::default()
                                    },
                                )
                                .await
                                .map_err(OrmPublicError::from)?;
                            match update {
                                ConditionalUpdateOutcome::Updated(record) => record,
                                ConditionalUpdateOutcome::NotFound
                                | ConditionalUpdateOutcome::Conflict => {
                                    return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                                }
                            }
                        }
                        (None, Some(_)) => return Err(OrmPublicError::not_found()),
                        (Some(_), None) => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                        actor_principal_kind: actor_kind,
                        actor_subject,
                        action: "ai.rule_policy.set".to_owned(),
                        resource_kind: "ai_rule_policy".to_owned(),
                        resource_reference: id.to_string(),
                        outcome: "allowed".to_owned(),
                        reason_code: "hierarchical_rule_narrowed".to_owned(),
                        correlation_id: Uuid::new_v4().to_string(),
                        causation_id: None,
                        policy_version: Some(record.row_version.to_string()),
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    Ok(record)
                })
            })
            .await
            .map_err(map_transaction)?;
        rule_view(&record, &scope)
    }

    async fn resolve_for_run(
        &self,
        principal: &AuthPrincipal,
        target_scope: AiScope,
    ) -> Result<AiResolvedRuleSet, AiError> {
        validate_scope(&target_scope)?;
        let hierarchy = self
            .hierarchy_resolver
            .hierarchy(principal, &target_scope)
            .await?;
        validate_hierarchy(&hierarchy, &target_scope, &self.limits)?;
        for scope in &hierarchy {
            self.require_access(principal, scope, AiRuleAction::ResolveForRun)
                .await?;
        }
        let lookups = hierarchy
            .iter()
            .map(|scope| (rule_policy_id(scope), scope.clone()))
            .collect::<Vec<_>>();
        let records = self
            .database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    let mut records = Vec::with_capacity(lookups.len());
                    for (id, scope) in lookups {
                        let record = tx
                            .find_by_id::<AiScopePolicyRecord>(&id)
                            .await
                            .map_err(OrmPublicError::from)?
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::Conflict))?;
                        validate_record_identity(&record, &scope)?;
                        records.push((record, scope));
                    }
                    Ok(records)
                })
            })
            .await
            .map_err(map_transaction)?;
        let mut effective = self.limits.ceiling().clone();
        let mut applied_layers = Vec::with_capacity(records.len());
        for (record, scope) in records {
            let constraints = parse_record(&record, &scope)?;
            if !constraints.is_no_broader_than(self.limits.ceiling())? {
                return Err(AiError::InvalidConfiguration(
                    "stored scope rule exceeds deployment limits".to_owned(),
                ));
            }
            effective = effective.narrow(&constraints)?;
            applied_layers.push(AiAppliedRuleLayer {
                scope,
                row_version: record.row_version,
            });
        }
        let fingerprint = resolved_fingerprint(&target_scope, &effective, &applied_layers)?;
        Ok(AiResolvedRuleSet::new(
            target_scope,
            effective,
            applied_layers,
            fingerprint,
        ))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredRulePolicy {
    format_version: u32,
    enabled: bool,
    maximum_classification: DataClassification,
    maximum_tool_maturity: ToolMaturity,
    approval_requirement: AiRuleApprovalRequirement,
    allowed_tool_fingerprints: Option<Vec<String>>,
    allowed_provider_kinds: Option<Vec<ProviderKind>>,
    allowed_provider_capabilities: Option<Vec<AiRuleProviderCapability>>,
    allow_provider_retention: bool,
    allow_byok: bool,
    budget: StoredRuleBudget,
    checksum: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredRuleBudget {
    maximum_steps: Option<u64>,
    maximum_duration_seconds: Option<u64>,
    maximum_output_tokens: Option<u64>,
    maximum_cost_microunits: Option<u64>,
    maximum_provider_calls: Option<u64>,
    maximum_tool_units: Option<u64>,
    maximum_image_units: Option<u64>,
}

impl From<&AiRuleBudgetCeilings> for StoredRuleBudget {
    fn from(value: &AiRuleBudgetCeilings) -> Self {
        Self {
            maximum_steps: value.maximum_steps,
            maximum_duration_seconds: value.maximum_duration_seconds,
            maximum_output_tokens: value.maximum_output_tokens,
            maximum_cost_microunits: value.maximum_cost_microunits,
            maximum_provider_calls: value.maximum_provider_calls,
            maximum_tool_units: value.maximum_tool_units,
            maximum_image_units: value.maximum_image_units,
        }
    }
}

impl From<StoredRuleBudget> for AiRuleBudgetCeilings {
    fn from(value: StoredRuleBudget) -> Self {
        Self {
            maximum_steps: value.maximum_steps,
            maximum_duration_seconds: value.maximum_duration_seconds,
            maximum_output_tokens: value.maximum_output_tokens,
            maximum_cost_microunits: value.maximum_cost_microunits,
            maximum_provider_calls: value.maximum_provider_calls,
            maximum_tool_units: value.maximum_tool_units,
            maximum_image_units: value.maximum_image_units,
        }
    }
}

fn stored_policy_value(
    scope: &AiScope,
    constraints: &AiRuleConstraints,
) -> Result<serde_json::Value, AiError> {
    constraints.validate()?;
    let mut stored = StoredRulePolicy {
        format_version: RULE_FORMAT_VERSION,
        enabled: constraints.enabled,
        maximum_classification: constraints.maximum_classification,
        maximum_tool_maturity: constraints.maximum_tool_maturity,
        approval_requirement: constraints.approval_requirement,
        allowed_tool_fingerprints: constraints
            .allowed_tool_fingerprints
            .as_ref()
            .map(|values| values.iter().cloned().collect()),
        allowed_provider_kinds: constraints
            .allowed_provider_kinds
            .as_ref()
            .map(|values| values.iter().cloned().collect()),
        allowed_provider_capabilities: constraints
            .allowed_provider_capabilities
            .as_ref()
            .map(|values| values.iter().copied().collect()),
        allow_provider_retention: constraints.allow_provider_retention,
        allow_byok: constraints.allow_byok,
        budget: (&constraints.budget).into(),
        checksum: String::new(),
    };
    stored.checksum = stored_checksum(scope, &stored)?;
    serde_json::to_value(stored).map_err(|_| AiError::PersistenceFailed)
}

fn parse_record(
    record: &AiScopePolicyRecord,
    scope: &AiScope,
) -> Result<AiRuleConstraints, AiError> {
    validate_record_identity(record, scope).map_err(map_orm)?;
    let stored: StoredRulePolicy = serde_json::from_value(record.capabilities.clone())
        .map_err(|_| AiError::InvalidConfiguration("invalid stored rule policy".to_owned()))?;
    if stored.format_version != RULE_FORMAT_VERSION
        || stored.enabled != record.enabled
        || maturity_value(stored.maximum_tool_maturity) != record.maximum_tool_maturity
        || stored.checksum != stored_checksum(scope, &stored)?
    {
        return Err(AiError::InvalidConfiguration(
            "stored rule policy integrity mismatch".to_owned(),
        ));
    }
    let constraints = AiRuleConstraints {
        enabled: stored.enabled,
        maximum_classification: stored.maximum_classification,
        maximum_tool_maturity: stored.maximum_tool_maturity,
        approval_requirement: stored.approval_requirement,
        allowed_tool_fingerprints: vec_to_unique_set(stored.allowed_tool_fingerprints)?,
        allowed_provider_kinds: vec_to_unique_set(stored.allowed_provider_kinds)?,
        allowed_provider_capabilities: vec_to_unique_set(stored.allowed_provider_capabilities)?,
        allow_provider_retention: stored.allow_provider_retention,
        allow_byok: stored.allow_byok,
        budget: stored.budget.into(),
    };
    constraints
        .validate()
        .map_err(|_| AiError::InvalidConfiguration("invalid stored rule constraints".to_owned()))?;
    Ok(constraints)
}

fn stored_checksum(scope: &AiScope, stored: &StoredRulePolicy) -> Result<String, AiError> {
    let value = serde_json::json!({
        "format": "graphql-orm-ai-rule-policy-v1",
        "scope": scope,
        "format_version": stored.format_version,
        "enabled": stored.enabled,
        "maximum_classification": stored.maximum_classification,
        "maximum_tool_maturity": stored.maximum_tool_maturity,
        "approval_requirement": stored.approval_requirement,
        "allowed_tool_fingerprints": stored.allowed_tool_fingerprints,
        "allowed_provider_kinds": stored.allowed_provider_kinds,
        "allowed_provider_capabilities": stored.allowed_provider_capabilities,
        "allow_provider_retention": stored.allow_provider_retention,
        "allow_byok": stored.allow_byok,
        "budget": stored.budget,
    });
    let bytes = serde_json::to_vec(&value).map_err(|_| AiError::PersistenceFailed)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn resolved_fingerprint(
    target_scope: &AiScope,
    constraints: &AiRuleConstraints,
    layers: &[AiAppliedRuleLayer],
) -> Result<String, AiError> {
    let value = serde_json::json!({
        "format": "graphql-orm-ai-resolved-rules-v1",
        "target_scope": target_scope,
        "constraints": constraints,
        "layers": layers,
    });
    let bytes = serde_json::to_vec(&value).map_err(|_| AiError::PersistenceFailed)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn rule_view(record: &AiScopePolicyRecord, scope: &AiScope) -> Result<AiRulePolicyView, AiError> {
    let constraints = parse_record(record, scope)?;
    Ok(AiRulePolicyView {
        scope_kind: scope.kind.clone(),
        scope_id: scope.id.clone(),
        tenant_id: scope.tenant_id.clone(),
        enabled: constraints.enabled,
        maximum_classification: classification_value(constraints.maximum_classification).to_owned(),
        maximum_tool_maturity: maturity_value(constraints.maximum_tool_maturity).to_owned(),
        approval_requirement: constraints.approval_requirement.as_str().to_owned(),
        allowed_tool_fingerprints: constraints
            .allowed_tool_fingerprints
            .map(|values| values.into_iter().collect()),
        allowed_provider_kinds: constraints.allowed_provider_kinds.map(|values| {
            values
                .into_iter()
                .map(|value| value.as_str().to_owned())
                .collect()
        }),
        allowed_provider_capabilities: constraints.allowed_provider_capabilities.map(|values| {
            values
                .into_iter()
                .map(|value| value.as_str().to_owned())
                .collect()
        }),
        allow_provider_retention: constraints.allow_provider_retention,
        allow_byok: constraints.allow_byok,
        maximum_steps: constraints.budget.maximum_steps,
        maximum_duration_seconds: constraints.budget.maximum_duration_seconds,
        maximum_output_tokens: constraints.budget.maximum_output_tokens,
        maximum_cost_microunits: constraints.budget.maximum_cost_microunits,
        maximum_provider_calls: constraints.budget.maximum_provider_calls,
        maximum_tool_units: constraints.budget.maximum_tool_units,
        maximum_image_units: constraints.budget.maximum_image_units,
        row_version: record.row_version,
        updated_at: record.updated_at,
    })
}

fn validate_hierarchy(
    hierarchy: &[AiScope],
    target: &AiScope,
    limits: &AiRuleDeploymentLimits,
) -> Result<(), AiError> {
    if hierarchy.is_empty()
        || hierarchy.len() > limits.maximum_hierarchy_depth()
        || hierarchy.last() != Some(target)
    {
        return Err(AiError::InvalidConfiguration(
            "invalid hierarchical rule lineage".to_owned(),
        ));
    }
    let mut identities = BTreeSet::new();
    for scope in hierarchy {
        validate_scope(scope)?;
        if !identities.insert(crate::ai_scope_key(scope))
            || match (&target.tenant_id, &scope.tenant_id) {
                (None, Some(_)) => true,
                (Some(target), Some(layer)) => target != layer,
                (None, None) | (Some(_), None) => false,
            }
        {
            return Err(AiError::Forbidden);
        }
    }
    Ok(())
}

fn validate_record_identity(
    record: &AiScopePolicyRecord,
    scope: &AiScope,
) -> Result<(), OrmPublicError> {
    if record.id != rule_policy_id(scope)
        || record.scope_kind != scope.kind
        || record.scope_id != scope.id
        || record.tenant_id != scope.tenant_id
        || record.row_version < 0
    {
        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
    }
    Ok(())
}

fn validate_scope(scope: &AiScope) -> Result<(), AiError> {
    let valid = |value: &str| {
        !value.trim().is_empty() && value.len() <= 512 && !value.chars().any(char::is_control)
    };
    if !valid(&scope.kind)
        || !valid(&scope.id)
        || scope
            .tenant_id
            .as_deref()
            .is_some_and(|value| !valid(value))
    {
        return Err(AiError::InvalidInput("invalid AI rule scope".to_owned()));
    }
    Ok(())
}

fn rule_policy_id(scope: &AiScope) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"graphql-orm-ai/rule-policy/v1\0");
    hash.update(crate::ai_scope_key(scope).as_bytes());
    let digest = hash.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn vec_to_unique_set<T: Ord>(values: Option<Vec<T>>) -> Result<Option<BTreeSet<T>>, AiError> {
    let Some(values) = values else {
        return Ok(None);
    };
    let original_len = values.len();
    let values = values.into_iter().collect::<BTreeSet<_>>();
    if values.len() != original_len {
        return Err(AiError::InvalidConfiguration(
            "duplicate stored hierarchical rule value".to_owned(),
        ));
    }
    Ok(Some(values))
}

fn classification_value(value: DataClassification) -> &'static str {
    match value {
        DataClassification::Public => "public",
        DataClassification::Internal => "internal",
        DataClassification::Confidential => "confidential",
        DataClassification::Restricted => "restricted",
        DataClassification::Secret => "secret",
    }
}

fn maturity_value(value: ToolMaturity) -> &'static str {
    match value {
        ToolMaturity::ReadOnly => "read_only",
        ToolMaturity::ProposalOnly => "proposal_only",
        ToolMaturity::SupervisedWrite => "supervised_write",
        ToolMaturity::AutonomousWrite => "autonomous_write",
    }
}

fn principal_kind(principal: &AuthPrincipal) -> String {
    match principal {
        AuthPrincipal::User(_) => "user".to_owned(),
        AuthPrincipal::ApiToken(token) => {
            format!("api_token:{}", token.principal_kind.as_str())
        }
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use agql_auth::{
        AccessTokenMetadata, AuthUser, FixedClock, PrincipalReference, SessionContext,
    };
    use time::OffsetDateTime;

    use super::*;

    struct CountingPrincipalResolver {
        principal: AuthPrincipal,
        now: OffsetDateTime,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl CurrentPrincipalResolver for CountingPrincipalResolver {
        async fn resolve(
            &self,
            reference: &PrincipalReference,
        ) -> agql_auth::AuthResult<ResolvedPrincipal> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            ResolvedPrincipal::new(reference.clone(), self.principal.clone(), self.now)
        }
    }

    struct FixedRuleService;

    #[async_trait]
    impl AiRulePolicyService for FixedRuleService {
        async fn policy(
            &self,
            _principal: &AuthPrincipal,
            _scope: AiScope,
        ) -> Result<Option<AiRulePolicyView>, AiError> {
            Err(AiError::Forbidden)
        }

        async fn set_policy(
            &self,
            _principal: &AuthPrincipal,
            _input: SetAiRulePolicyInput,
        ) -> Result<AiRulePolicyView, AiError> {
            Err(AiError::Forbidden)
        }

        async fn resolve_for_run(
            &self,
            _principal: &AuthPrincipal,
            target_scope: AiScope,
        ) -> Result<AiResolvedRuleSet, AiError> {
            Ok(AiResolvedRuleSet::new(
                target_scope,
                AiRuleConstraints {
                    enabled: true,
                    maximum_classification: DataClassification::Internal,
                    maximum_tool_maturity: ToolMaturity::ReadOnly,
                    approval_requirement: AiRuleApprovalRequirement::DescriptorPolicy,
                    allowed_tool_fingerprints: None,
                    allowed_provider_kinds: None,
                    allowed_provider_capabilities: None,
                    allow_provider_retention: false,
                    allow_byok: false,
                    budget: AiRuleBudgetCeilings::default(),
                },
                Vec::new(),
                "b".repeat(64),
            ))
        }
    }

    #[tokio::test]
    async fn durable_rule_resolution_rehydrates_around_the_exact_hierarchy() {
        let now = OffsetDateTime::from_unix_timestamp(1_800_000_000)
            .expect("fixed rule time should validate");
        let principal = AuthPrincipal::User(AuthUser {
            user_id: "current-rule-user".to_owned(),
            session_id: Uuid::new_v4(),
            roles: Vec::new(),
            scopes: Vec::new(),
            session: SessionContext::default(),
            token_claims: AccessTokenMetadata::default(),
        });
        let lease = AiRunLease::test_running(principal.reference());
        let principal_resolver = Arc::new(CountingPrincipalResolver {
            principal,
            now,
            calls: AtomicUsize::new(0),
        });
        let resolver = OrmAiCurrentRuleResolver::new(
            principal_resolver.clone(),
            Arc::new(FixedRuleService),
            Arc::new(FixedClock::new(now)),
            AiCurrentRuleResolverLimits::new(time::Duration::minutes(5))
                .expect("current-rule limits should validate"),
        );
        let scope = AiScope::new("test", "durable-rule");

        let resolved = resolver
            .resolve_rules(&lease, &scope)
            .await
            .expect("fresh exact rules should resolve");

        assert_eq!(resolved.rules().target_scope(), &scope);
        assert_eq!(resolved.rules().fingerprint(), "b".repeat(64));
        assert_eq!(principal_resolver.calls.load(Ordering::SeqCst), 2);
    }
}
