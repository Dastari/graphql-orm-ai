//! ORM-backed GraphQL-managed AI configuration service.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::sync::Arc;

use agql_auth::{AuthPrincipal, Clock, RecentMfaPolicy};
use async_trait::async_trait;
use graphql_orm::db::Database;
use graphql_orm::graphql::errors::{OrmErrorCode, OrmPublicError};
use graphql_orm::graphql::filters::StringFilter;
use graphql_orm::graphql::orm::{
    ConditionalUpdateOutcome, DefaultWriteBackend, TransactionError, TransactionMode,
};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use serde_json::json;
use url::Url;
use uuid::Uuid;

use crate::persistence::*;
use crate::{
    AiBudgetAmounts, AiBudgetPolicyView, AiConfigurationAccessPolicy, AiConfigurationAction,
    AiConfigurationService, AiContentProtectionMode, AiContentProtectionPolicy,
    AiContentProtectionPolicyResolver, AiContentProtectionPolicyView, AiError,
    AiOpenAiCompatibleProfileInput, AiOpenAiCompatibleProfileView, AiProviderEndpointPolicy,
    AiProviderKindInput, AiProviderProfileView, AiRetentionPolicyView, AiScope, AiSecretStore,
    RemoveAiProviderCredentialInput, SecretRef, SetAiContentProtectionPolicyInput,
    SetAiRetentionPolicyInput, UpsertAiBudgetPolicyInput, UpsertAiProviderProfileInput,
};

/// Deployment hard bounds for GraphQL-managed budget policies.
///
/// GraphQL values may only narrow these ceilings. This type does not grant
/// configuration authority and does not replace the transactional provider
/// reservation limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiBudgetPolicyManagementLimits {
    maximum_ceiling: AiBudgetAmounts,
    maximum_policies_per_scope: usize,
}

impl AiBudgetPolicyManagementLimits {
    /// Creates validated deployment management bounds.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless the per-scope policy
    /// count is in `1..=100`.
    pub fn new(
        maximum_ceiling: AiBudgetAmounts,
        maximum_policies_per_scope: usize,
    ) -> Result<Self, AiError> {
        if !(1..=100).contains(&maximum_policies_per_scope) {
            return Err(AiError::InvalidConfiguration(
                "invalid budget-policy management limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_ceiling,
            maximum_policies_per_scope,
        })
    }

    /// Returns the greatest ceiling GraphQL may configure in each dimension.
    pub const fn maximum_ceiling(self) -> AiBudgetAmounts {
        self.maximum_ceiling
    }

    /// Returns the maximum number of policies for one exact scope.
    pub const fn maximum_policies_per_scope(self) -> usize {
        self.maximum_policies_per_scope
    }
}

/// Durable configuration backend using generated ORM APIs and a compensating
/// secret-reference saga. Secret plaintext never enters an ORM input.
#[derive(Clone)]
pub struct OrmAiConfigurationService {
    database: Database<DefaultWriteBackend>,
    access_policy: Arc<dyn AiConfigurationAccessPolicy>,
    endpoint_policy: Arc<dyn AiProviderEndpointPolicy>,
    recent_mfa_policy: RecentMfaPolicy,
    clock: Arc<dyn Clock>,
    secret_store: Arc<dyn AiSecretStore>,
    budget_policy_limits: Option<AiBudgetPolicyManagementLimits>,
}

impl OrmAiConfigurationService {
    /// Creates a fail-closed configuration service.
    pub fn new(
        database: Database<DefaultWriteBackend>,
        access_policy: Arc<dyn AiConfigurationAccessPolicy>,
        endpoint_policy: Arc<dyn AiProviderEndpointPolicy>,
        recent_mfa_policy: RecentMfaPolicy,
        clock: Arc<dyn Clock>,
        secret_store: Arc<dyn AiSecretStore>,
    ) -> Self {
        Self {
            database,
            access_policy,
            endpoint_policy,
            recent_mfa_policy,
            clock,
            secret_store,
            budget_policy_limits: None,
        }
    }

    /// Enables GraphQL budget-policy management under deployment hard bounds.
    ///
    /// Without this explicit opt-in, budget-policy reads remain independently
    /// authorized but mutations fail closed as invalid configuration.
    pub fn with_budget_policy_management(mut self, limits: AiBudgetPolicyManagementLimits) -> Self {
        self.budget_policy_limits = Some(limits);
        self
    }

    /// Returns the underlying ORM database handle for host schema wiring.
    pub fn database(&self) -> &Database<DefaultWriteBackend> {
        &self.database
    }

    async fn require_access(
        &self,
        principal: &AuthPrincipal,
        scope: &AiScope,
        action: AiConfigurationAction,
    ) -> Result<(), AiError> {
        validate_scope(scope)?;
        if self
            .access_policy
            .can_configure(principal, scope, action)
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

    async fn profile(&self, id: Uuid) -> Result<AiProviderProfileRecord, AiError> {
        AiProviderProfileRecord::find_by_id(&self.database, &id)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
            .ok_or(AiError::NotFound)
    }

    async fn complete_cleanup(&self, cleanup_id: Option<Uuid>, reference: &SecretRef) {
        let Some(cleanup_id) = cleanup_id else {
            return;
        };
        if self.secret_store.delete(reference).await.is_ok() {
            let _ = AiSecretCleanupRecord::update_by_id(
                &self.database,
                &cleanup_id,
                UpdateAiSecretCleanupRecordInput {
                    state: Some("complete".to_owned()),
                    completed_at: Some(Some(unix_seconds())),
                    ..Default::default()
                },
            )
            .await;
        }
    }
}

#[async_trait]
impl AiConfigurationService for OrmAiConfigurationService {
    async fn provider_profiles(
        &self,
        principal: &AuthPrincipal,
        scope: AiScope,
    ) -> Result<Vec<AiProviderProfileView>, AiError> {
        self.require_access(
            principal,
            &scope,
            AiConfigurationAction::ReadProviderProfiles,
        )
        .await?;
        let scope_key = scope_key(&scope);
        let rows = self
            .database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    tx.query::<AiProviderProfileRecord>()
                        .filter(AiProviderProfileRecordWhereInput {
                            scope_key: Some(StringFilter {
                                eq: Some(scope_key),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .default_order()
                        .limit(101)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .map_err(map_transaction)?;
        if rows.len() > 100 {
            return Err(AiError::InvalidConfiguration(
                "provider profile scope exceeds the bounded limit".to_owned(),
            ));
        }
        rows.iter().map(provider_view).collect()
    }

    async fn content_protection_policy(
        &self,
        principal: &AuthPrincipal,
        scope: AiScope,
    ) -> Result<Option<AiContentProtectionPolicyView>, AiError> {
        self.require_access(
            principal,
            &scope,
            AiConfigurationAction::ReadContentProtection,
        )
        .await?;
        Ok(load_content_policy(&self.database, &scope)
            .await?
            .as_ref()
            .map(content_policy_view))
    }

    async fn retention_policy(
        &self,
        principal: &AuthPrincipal,
        scope: AiScope,
    ) -> Result<Option<AiRetentionPolicyView>, AiError> {
        self.require_access(principal, &scope, AiConfigurationAction::ReadRetention)
            .await?;
        Ok(load_retention_policy(&self.database, &scope)
            .await?
            .as_ref()
            .map(retention_policy_view))
    }

    async fn budget_policies(
        &self,
        principal: &AuthPrincipal,
        scope: AiScope,
    ) -> Result<Vec<AiBudgetPolicyView>, AiError> {
        self.require_access(principal, &scope, AiConfigurationAction::ReadBudgetPolicies)
            .await?;
        let exact_scope_key = scope_key(&scope);
        let rows = self
            .database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    tx.query::<AiBudgetPolicyRecord>()
                        .filter(AiBudgetPolicyRecordWhereInput {
                            scope_key: Some(StringFilter {
                                eq: Some(exact_scope_key),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .default_order()
                        .limit(101)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .map_err(map_transaction)?;
        if rows.len() > 100 {
            return Err(AiError::InvalidConfiguration(
                "budget-policy scope exceeds the bounded read limit".to_owned(),
            ));
        }
        if rows.iter().any(|record| {
            record.scope_key != exact_scope_key_for_record(record)
                || record.scope_kind != scope.kind
                || record.scope_id != scope.id
                || record.tenant_id != scope.tenant_id
        }) {
            return Err(AiError::PersistenceFailed);
        }
        Ok(rows.iter().map(budget_policy_view).collect())
    }

    async fn upsert_provider_profile(
        &self,
        principal: &AuthPrincipal,
        input: UpsertAiProviderProfileInput,
    ) -> Result<AiProviderProfileView, AiError> {
        self.require_recent_mfa(principal)?;
        let scope: AiScope = input.scope.into();
        self.require_access(
            principal,
            &scope,
            AiConfigurationAction::ManageProviderProfiles,
        )
        .await?;
        let display_name = input.display_name.trim().to_owned();
        if display_name.is_empty() || display_name.len() > 200 {
            return Err(AiError::InvalidInput(
                "invalid provider display name".to_owned(),
            ));
        }
        let base_url = normalize_endpoint(
            input.provider_kind,
            input.base_url,
            self.endpoint_policy.as_ref(),
        )?;
        let data_policy = provider_data_policy(input.provider_kind, input.openai_compatible)?;
        let actor_kind = principal_kind(principal);
        let actor_subject = principal.subject().to_owned();
        let scope_hash = scope_key(&scope);
        let provider_kind = input.provider_kind.as_str().to_owned();
        let expected_version = input.expected_version;
        let profile_id = input.id;
        let profile = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let profile = match (profile_id, expected_version) {
                        (None, None) => tx
                            .insert::<AiProviderProfileRecord>(CreateAiProviderProfileRecordInput {
                                scope_key: scope_hash,
                                scope_kind: scope.kind,
                                scope_id: scope.id,
                                tenant_id: scope.tenant_id,
                                provider_kind,
                                display_name,
                                base_url,
                                credential_reference: None,
                                enabled: input.enabled,
                                data_policy,
                                limits: json!({}),
                            })
                            .await
                            .map_err(OrmPublicError::from)?,
                        (Some(id), Some(expected_version)) => {
                            let current = tx
                                .find_by_id::<AiProviderProfileRecord>(&id)
                                .await
                                .map_err(OrmPublicError::from)?
                                .ok_or_else(OrmPublicError::not_found)?;
                            if current.scope_key != scope_hash {
                                return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                            }
                            if provider_kind == AiProviderKindInput::LocalHarness.as_str()
                                && current.credential_reference.is_some()
                            {
                                return Err(OrmPublicError::new(OrmErrorCode::InvalidInput));
                            }
                            match tx
                                .compare_and_swap::<AiProviderProfileRecord>(
                                    &id,
                                    expected_version,
                                    AiProviderProfileRecordWhereInput::default(),
                                    UpdateAiProviderProfileRecordInput {
                                        provider_kind: Some(provider_kind),
                                        display_name: Some(display_name),
                                        base_url: Some(base_url),
                                        data_policy: Some(data_policy),
                                        enabled: Some(input.enabled),
                                        ..Default::default()
                                    },
                                )
                                .await
                                .map_err(OrmPublicError::from)?
                            {
                                ConditionalUpdateOutcome::Updated(profile) => profile,
                                ConditionalUpdateOutcome::NotFound => {
                                    return Err(OrmPublicError::not_found());
                                }
                                ConditionalUpdateOutcome::Conflict => {
                                    return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                                }
                            }
                        }
                        _ => return Err(OrmPublicError::new(OrmErrorCode::InvalidInput)),
                    };
                    insert_audit(
                        tx,
                        AuditFact {
                            actor_principal_kind: &actor_kind,
                            actor_subject: &actor_subject,
                            action: "ai.provider_profile.upsert",
                            resource_kind: "provider_profile",
                            resource_reference: &profile.id.to_string(),
                            outcome: "allowed",
                            reason_code: "configuration_updated",
                            policy_version: None,
                        },
                    )
                    .await?;
                    Ok(profile)
                })
            })
            .await
            .map_err(map_transaction)?;
        provider_view(&profile)
    }

    async fn set_provider_credential(
        &self,
        principal: &AuthPrincipal,
        profile_id: Uuid,
        credential: SecretString,
        expected_version: i64,
    ) -> Result<AiProviderProfileView, AiError> {
        self.require_recent_mfa(principal)?;
        let existing = self.profile(profile_id).await?;
        let scope = profile_scope(&existing);
        self.require_access(
            principal,
            &scope,
            AiConfigurationAction::ManageProviderCredentials,
        )
        .await?;
        if existing.provider_kind == AiProviderKindInput::LocalHarness.as_str() {
            return Err(AiError::InvalidInput(
                "local-harness profiles do not accept provider credentials".to_owned(),
            ));
        }
        if existing.row_version != expected_version {
            return Err(AiError::Conflict);
        }
        let new_reference = self
            .secret_store
            .put(None, credential)
            .await
            .map_err(|_| AiError::PersistenceFailed)?;
        let previous_reference = existing
            .credential_reference
            .as_deref()
            .map(|reference| SecretRef::parse(reference.to_owned()))
            .transpose()
            .map_err(|_| AiError::InvalidConfiguration("invalid secret reference".to_owned()))?;
        let cleanup_id = previous_reference.as_ref().map(|_| Uuid::new_v4());
        let actor_kind = principal_kind(principal);
        let actor_subject = principal.subject().to_owned();
        let new_reference_value = new_reference.as_str().to_owned();
        let previous_for_tx = previous_reference.clone();
        let result = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = tx
                        .find_by_id::<AiProviderProfileRecord>(&profile_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if current.row_version != expected_version {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let profile = match tx
                        .compare_and_swap::<AiProviderProfileRecord>(
                            &profile_id,
                            expected_version,
                            AiProviderProfileRecordWhereInput::default(),
                            UpdateAiProviderProfileRecordInput {
                                credential_reference: Some(Some(new_reference_value)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?
                    {
                        ConditionalUpdateOutcome::Updated(profile) => profile,
                        ConditionalUpdateOutcome::NotFound => {
                            return Err(OrmPublicError::not_found());
                        }
                        ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    if let (Some(cleanup_id), Some(previous)) =
                        (cleanup_id, previous_for_tx.as_ref())
                    {
                        tx.insert::<AiSecretCleanupRecord>(CreateAiSecretCleanupRecordInput {
                            id: cleanup_id,
                            secret_reference: previous.as_str().to_owned(),
                            reason_code: "credential_rotated".to_owned(),
                            state: "pending".to_owned(),
                            retry_count: 0,
                            next_attempt_at: Some(unix_seconds()),
                            completed_at: None,
                        })
                        .await
                        .map_err(OrmPublicError::from)?;
                    }
                    insert_audit(
                        tx,
                        AuditFact {
                            actor_principal_kind: &actor_kind,
                            actor_subject: &actor_subject,
                            action: "ai.provider_credential.set",
                            resource_kind: "provider_profile",
                            resource_reference: &profile_id.to_string(),
                            outcome: "allowed",
                            reason_code: "credential_rotated",
                            policy_version: None,
                        },
                    )
                    .await?;
                    Ok(profile)
                })
            })
            .await;
        let profile = match result {
            Ok(profile) => profile,
            Err(error) => {
                let _ = self.secret_store.delete(&new_reference).await;
                return Err(map_transaction(error));
            }
        };
        if let Some(previous) = previous_reference.as_ref() {
            self.complete_cleanup(cleanup_id, previous).await;
        }
        provider_view(&profile)
    }

    async fn remove_provider_credential(
        &self,
        principal: &AuthPrincipal,
        input: RemoveAiProviderCredentialInput,
    ) -> Result<AiProviderProfileView, AiError> {
        self.require_recent_mfa(principal)?;
        let existing = self.profile(input.profile_id).await?;
        let scope = profile_scope(&existing);
        self.require_access(
            principal,
            &scope,
            AiConfigurationAction::ManageProviderCredentials,
        )
        .await?;
        if existing.row_version != input.expected_version {
            return Err(AiError::Conflict);
        }
        let previous_reference = existing
            .credential_reference
            .as_deref()
            .map(|reference| SecretRef::parse(reference.to_owned()))
            .transpose()
            .map_err(|_| AiError::InvalidConfiguration("invalid secret reference".to_owned()))?;
        let cleanup_id = previous_reference.as_ref().map(|_| Uuid::new_v4());
        let previous_for_tx = previous_reference.clone();
        let actor_kind = principal_kind(principal);
        let actor_subject = principal.subject().to_owned();
        let profile_id = input.profile_id;
        let expected_version = input.expected_version;
        let profile = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let profile = match tx
                        .compare_and_swap::<AiProviderProfileRecord>(
                            &profile_id,
                            expected_version,
                            AiProviderProfileRecordWhereInput::default(),
                            UpdateAiProviderProfileRecordInput {
                                credential_reference: Some(None),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?
                    {
                        ConditionalUpdateOutcome::Updated(profile) => profile,
                        ConditionalUpdateOutcome::NotFound => {
                            return Err(OrmPublicError::not_found());
                        }
                        ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    if let (Some(cleanup_id), Some(previous)) =
                        (cleanup_id, previous_for_tx.as_ref())
                    {
                        tx.insert::<AiSecretCleanupRecord>(CreateAiSecretCleanupRecordInput {
                            id: cleanup_id,
                            secret_reference: previous.as_str().to_owned(),
                            reason_code: "credential_removed".to_owned(),
                            state: "pending".to_owned(),
                            retry_count: 0,
                            next_attempt_at: Some(unix_seconds()),
                            completed_at: None,
                        })
                        .await
                        .map_err(OrmPublicError::from)?;
                    }
                    insert_audit(
                        tx,
                        AuditFact {
                            actor_principal_kind: &actor_kind,
                            actor_subject: &actor_subject,
                            action: "ai.provider_credential.remove",
                            resource_kind: "provider_profile",
                            resource_reference: &profile_id.to_string(),
                            outcome: "allowed",
                            reason_code: "credential_removed",
                            policy_version: None,
                        },
                    )
                    .await?;
                    Ok(profile)
                })
            })
            .await
            .map_err(map_transaction)?;
        if let Some(previous) = previous_reference.as_ref() {
            self.complete_cleanup(cleanup_id, previous).await;
        }
        provider_view(&profile)
    }

    async fn set_content_protection_policy(
        &self,
        principal: &AuthPrincipal,
        input: SetAiContentProtectionPolicyInput,
    ) -> Result<AiContentProtectionPolicyView, AiError> {
        self.require_recent_mfa(principal)?;
        let scope: AiScope = input.scope.into();
        self.require_access(
            principal,
            &scope,
            AiConfigurationAction::ManageContentProtection,
        )
        .await?;
        let mode: AiContentProtectionMode = input.mode.into();
        match (mode, input.key_policy_reference.as_deref()) {
            (AiContentProtectionMode::DatabaseManaged, Some(_))
            | (AiContentProtectionMode::ApplicationEncrypted, None) => {
                return Err(AiError::InvalidInput(
                    "content-protection key policy does not match mode".to_owned(),
                ));
            }
            _ => {}
        }
        let scope_hash = scope_key(&scope);
        let actor_kind = principal_kind(principal);
        let actor_subject = principal.subject().to_owned();
        let expected_version = input.expected_version;
        let protection_mode = protection_mode_value(mode).to_owned();
        let ready = mode == AiContentProtectionMode::DatabaseManaged;
        let migration_state = if ready { "ready" } else { "pending" }.to_owned();
        let key_policy_reference = input.key_policy_reference;
        let now = unix_seconds();
        let record = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let existing = tx
                        .query::<AiContentProtectionPolicyRecord>()
                        .filter(AiContentProtectionPolicyRecordWhereInput {
                            scope_key: Some(StringFilter {
                                eq: Some(scope_hash.clone()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if existing.len() > 1 {
                        return Err(OrmPublicError::new(
                            OrmErrorCode::AuthorizationMisconfigured,
                        ));
                    }
                    let record = match (existing.into_iter().next(), expected_version) {
                        (None, None) => tx
                            .insert::<AiContentProtectionPolicyRecord>(
                                CreateAiContentProtectionPolicyRecordInput {
                                    scope_key: scope_hash,
                                    scope_kind: scope.kind,
                                    scope_id: scope.id,
                                    tenant_id: scope.tenant_id,
                                    protection_mode,
                                    key_policy_reference,
                                    key_version: None,
                                    migration_state,
                                    ready,
                                    effective_at: now,
                                },
                            )
                            .await
                            .map_err(OrmPublicError::from)?,
                        (Some(current), Some(expected_version)) => match tx
                            .compare_and_swap::<AiContentProtectionPolicyRecord>(
                                &current.id,
                                expected_version,
                                AiContentProtectionPolicyRecordWhereInput::default(),
                                UpdateAiContentProtectionPolicyRecordInput {
                                    protection_mode: Some(protection_mode),
                                    key_policy_reference: Some(key_policy_reference),
                                    key_version: Some(None),
                                    migration_state: Some(migration_state),
                                    ready: Some(ready),
                                    effective_at: Some(now),
                                    ..Default::default()
                                },
                            )
                            .await
                            .map_err(OrmPublicError::from)?
                        {
                            ConditionalUpdateOutcome::Updated(record) => record,
                            ConditionalUpdateOutcome::NotFound => {
                                return Err(OrmPublicError::not_found());
                            }
                            ConditionalUpdateOutcome::Conflict => {
                                return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                            }
                        },
                        _ => return Err(OrmPublicError::new(OrmErrorCode::Conflict)),
                    };
                    insert_audit(
                        tx,
                        AuditFact {
                            actor_principal_kind: &actor_kind,
                            actor_subject: &actor_subject,
                            action: "ai.content_protection.set",
                            resource_kind: "content_protection_policy",
                            resource_reference: &record.id.to_string(),
                            outcome: "allowed",
                            reason_code: "content_protection_updated",
                            policy_version: None,
                        },
                    )
                    .await?;
                    Ok(record)
                })
            })
            .await
            .map_err(map_transaction)?;
        Ok(content_policy_view(&record))
    }

    async fn set_retention_policy(
        &self,
        principal: &AuthPrincipal,
        input: SetAiRetentionPolicyInput,
    ) -> Result<AiRetentionPolicyView, AiError> {
        self.require_recent_mfa(principal)?;
        validate_retention_input(&input)?;
        let scope: AiScope = input.scope.into();
        self.require_access(principal, &scope, AiConfigurationAction::ManageRetention)
            .await?;
        let scope_hash = scope_key(&scope);
        let actor_kind = principal_kind(principal);
        let actor_subject = principal.subject().to_owned();
        let expected_version = input.expected_version;
        let record = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let existing = tx
                        .query::<AiRetentionPolicyRecord>()
                        .filter(AiRetentionPolicyRecordWhereInput {
                            scope_key: Some(StringFilter {
                                eq: Some(scope_hash.clone()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if existing.len() > 1 {
                        return Err(OrmPublicError::new(
                            OrmErrorCode::AuthorizationMisconfigured,
                        ));
                    }
                    let record = match (existing.into_iter().next(), expected_version) {
                        (None, None) => tx
                            .insert::<AiRetentionPolicyRecord>(CreateAiRetentionPolicyRecordInput {
                                scope_key: Some(scope_hash),
                                scope_kind: scope.kind,
                                scope_id: scope.id,
                                tenant_id: scope.tenant_id,
                                message_retention_seconds: input.message_retention_seconds,
                                delta_retention_seconds: input.delta_retention_seconds,
                                raw_payload_retention_seconds: input.raw_payload_retention_seconds,
                                audit_retention_seconds: input.audit_retention_seconds,
                                deleted_content_purge_seconds: input.deleted_content_purge_seconds,
                                provider_file_delete_required: input.provider_file_delete_required,
                                inbox_event_retention_seconds: Some(
                                    input.inbox_event_retention_seconds,
                                ),
                                inbox_minimum_events: Some(input.inbox_minimum_events),
                            })
                            .await
                            .map_err(OrmPublicError::from)?,
                        (Some(current), Some(expected_version)) => match tx
                            .compare_and_swap::<AiRetentionPolicyRecord>(
                                &current.id,
                                expected_version,
                                AiRetentionPolicyRecordWhereInput {
                                    scope_key: Some(StringFilter {
                                        eq: Some(scope_hash),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                UpdateAiRetentionPolicyRecordInput {
                                    message_retention_seconds: Some(
                                        input.message_retention_seconds,
                                    ),
                                    delta_retention_seconds: Some(input.delta_retention_seconds),
                                    raw_payload_retention_seconds: Some(
                                        input.raw_payload_retention_seconds,
                                    ),
                                    audit_retention_seconds: Some(input.audit_retention_seconds),
                                    deleted_content_purge_seconds: Some(
                                        input.deleted_content_purge_seconds,
                                    ),
                                    provider_file_delete_required: Some(
                                        input.provider_file_delete_required,
                                    ),
                                    inbox_event_retention_seconds: Some(Some(
                                        input.inbox_event_retention_seconds,
                                    )),
                                    inbox_minimum_events: Some(Some(input.inbox_minimum_events)),
                                    ..Default::default()
                                },
                            )
                            .await
                            .map_err(OrmPublicError::from)?
                        {
                            ConditionalUpdateOutcome::Updated(record) => record,
                            ConditionalUpdateOutcome::NotFound => {
                                return Err(OrmPublicError::not_found());
                            }
                            ConditionalUpdateOutcome::Conflict => {
                                return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                            }
                        },
                        _ => return Err(OrmPublicError::new(OrmErrorCode::Conflict)),
                    };
                    insert_audit(
                        tx,
                        AuditFact {
                            actor_principal_kind: &actor_kind,
                            actor_subject: &actor_subject,
                            action: "ai.retention_policy.set",
                            resource_kind: "retention_policy",
                            resource_reference: &record.id.to_string(),
                            outcome: "allowed",
                            reason_code: "retention_policy_updated",
                            policy_version: Some(record.row_version.to_string()),
                        },
                    )
                    .await?;
                    Ok(record)
                })
            })
            .await
            .map_err(map_transaction)?;
        Ok(retention_policy_view(&record))
    }

    async fn upsert_budget_policy(
        &self,
        principal: &AuthPrincipal,
        mut input: UpsertAiBudgetPolicyInput,
    ) -> Result<AiBudgetPolicyView, AiError> {
        self.require_recent_mfa(principal)?;
        let scope: AiScope = input.scope.clone().into();
        self.require_access(
            principal,
            &scope,
            AiConfigurationAction::ManageBudgetPolicies,
        )
        .await?;
        let limits = self.budget_policy_limits.ok_or_else(|| {
            AiError::InvalidConfiguration("budget-policy management is not enabled".to_owned())
        })?;
        normalize_and_validate_budget_policy_input(&mut input, limits.maximum_ceiling())?;
        let exact_scope_key = scope_key(&scope);
        let actor_kind = principal_kind(principal);
        let actor_subject = principal.subject().to_owned();
        let id = input.id;
        let expected_version = input.expected_version;
        let maximum_policies = i64::try_from(limits.maximum_policies_per_scope())
            .map_err(|_| AiError::InvalidConfiguration("invalid policy limit".to_owned()))?;
        let record = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let record = match (id, expected_version) {
                        (None, None) => {
                            let count = tx
                                .query::<AiBudgetPolicyRecord>()
                                .filter(AiBudgetPolicyRecordWhereInput {
                                    scope_key: Some(StringFilter {
                                        eq: Some(exact_scope_key.clone()),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                })
                                .count()
                                .await
                                .map_err(OrmPublicError::from)?;
                            if count >= maximum_policies {
                                return Err(OrmPublicError::new(OrmErrorCode::InvalidInput));
                            }
                            tx.insert::<AiBudgetPolicyRecord>(CreateAiBudgetPolicyRecordInput {
                                scope_key: exact_scope_key,
                                scope_kind: scope.kind,
                                scope_id: scope.id,
                                tenant_id: scope.tenant_id,
                                principal_kind: input.principal_kind,
                                principal_subject: input.principal_subject,
                                interval_kind: input.interval.as_str().to_owned(),
                                maximum_input_tokens: input.maximum_input_tokens,
                                maximum_output_tokens: input.maximum_output_tokens,
                                maximum_tool_units: input.maximum_tool_units,
                                maximum_image_units: input.maximum_image_units,
                                maximum_cost_microunits: input.maximum_cost_microunits,
                                maximum_runs: input.maximum_runs,
                                enabled: input.enabled,
                            })
                            .await
                            .map_err(OrmPublicError::from)?
                        }
                        (Some(id), Some(expected_version)) if expected_version >= 0 => {
                            let current = tx
                                .find_by_id::<AiBudgetPolicyRecord>(&id)
                                .await
                                .map_err(OrmPublicError::from)?
                                .ok_or_else(OrmPublicError::not_found)?;
                            if current.scope_key != exact_scope_key
                                || current.scope_kind != scope.kind
                                || current.scope_id != scope.id
                                || current.tenant_id != scope.tenant_id
                                || current.principal_kind != input.principal_kind
                                || current.principal_subject != input.principal_subject
                                || current.interval_kind != input.interval.as_str()
                            {
                                return Err(OrmPublicError::not_found());
                            }
                            match tx
                                .compare_and_swap::<AiBudgetPolicyRecord>(
                                    &id,
                                    expected_version,
                                    AiBudgetPolicyRecordWhereInput {
                                        scope_key: Some(StringFilter {
                                            eq: Some(exact_scope_key),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    },
                                    UpdateAiBudgetPolicyRecordInput {
                                        maximum_input_tokens: Some(input.maximum_input_tokens),
                                        maximum_output_tokens: Some(input.maximum_output_tokens),
                                        maximum_tool_units: Some(input.maximum_tool_units),
                                        maximum_image_units: Some(input.maximum_image_units),
                                        maximum_cost_microunits: Some(
                                            input.maximum_cost_microunits,
                                        ),
                                        maximum_runs: Some(input.maximum_runs),
                                        enabled: Some(input.enabled),
                                        ..Default::default()
                                    },
                                )
                                .await
                                .map_err(OrmPublicError::from)?
                            {
                                ConditionalUpdateOutcome::Updated(record) => record,
                                ConditionalUpdateOutcome::NotFound => {
                                    return Err(OrmPublicError::not_found());
                                }
                                ConditionalUpdateOutcome::Conflict => {
                                    return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                                }
                            }
                        }
                        _ => return Err(OrmPublicError::new(OrmErrorCode::Conflict)),
                    };
                    insert_audit(
                        tx,
                        AuditFact {
                            actor_principal_kind: &actor_kind,
                            actor_subject: &actor_subject,
                            action: "ai.budget_policy.upsert",
                            resource_kind: "budget_policy",
                            resource_reference: &record.id.to_string(),
                            outcome: "allowed",
                            reason_code: "budget_policy_updated",
                            policy_version: Some(record.row_version.to_string()),
                        },
                    )
                    .await?;
                    Ok(record)
                })
            })
            .await
            .map_err(map_transaction)?;
        Ok(budget_policy_view(&record))
    }
}

#[async_trait]
impl AiContentProtectionPolicyResolver for OrmAiConfigurationService {
    async fn resolve(
        &self,
        principal: &AuthPrincipal,
        scope: &AiScope,
    ) -> Result<AiContentProtectionPolicy, AiError> {
        self.require_access(
            principal,
            scope,
            AiConfigurationAction::ReadContentProtection,
        )
        .await?;
        let record = load_content_policy(&self.database, scope)
            .await?
            .ok_or(AiError::RuntimeNotReady)?;
        let mode = parse_protection_mode(&record.protection_mode)?;
        Ok(AiContentProtectionPolicy {
            scope: scope.clone(),
            mode,
            key_policy_reference: record.key_policy_reference,
            version: u64::try_from(record.row_version).map_err(|_| AiError::PersistenceFailed)?,
            ready: record.ready && record.migration_state == "ready",
        })
    }
}

async fn load_content_policy(
    database: &Database<DefaultWriteBackend>,
    scope: &AiScope,
) -> Result<Option<AiContentProtectionPolicyRecord>, AiError> {
    let scope_hash = scope_key(scope);
    let rows = database
        .transaction(TransactionMode::Default, move |tx| {
            Box::pin(async move {
                tx.query::<AiContentProtectionPolicyRecord>()
                    .filter(AiContentProtectionPolicyRecordWhereInput {
                        scope_key: Some(StringFilter {
                            eq: Some(scope_hash),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })
                    .limit(2)
                    .fetch_all()
                    .await
                    .map_err(OrmPublicError::from)
            })
        })
        .await
        .map_err(map_transaction)?;
    if rows.len() > 1 {
        return Err(AiError::InvalidConfiguration(
            "multiple content-protection policies exist for one scope".to_owned(),
        ));
    }
    Ok(rows.into_iter().next())
}

async fn load_retention_policy(
    database: &Database<DefaultWriteBackend>,
    scope: &AiScope,
) -> Result<Option<AiRetentionPolicyRecord>, AiError> {
    let scope_hash = scope_key(scope);
    let rows = database
        .transaction(TransactionMode::Default, move |tx| {
            Box::pin(async move {
                tx.query::<AiRetentionPolicyRecord>()
                    .filter(AiRetentionPolicyRecordWhereInput {
                        scope_key: Some(StringFilter {
                            eq: Some(scope_hash),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })
                    .limit(2)
                    .fetch_all()
                    .await
                    .map_err(OrmPublicError::from)
            })
        })
        .await
        .map_err(map_transaction)?;
    if rows.len() > 1 {
        return Err(AiError::InvalidConfiguration(
            "multiple retention policies exist for one scope".to_owned(),
        ));
    }
    let record = rows.into_iter().next();
    if record.as_ref().is_some_and(|record| {
        record.inbox_event_retention_seconds.is_none() || record.inbox_minimum_events.is_none()
    }) {
        return Err(AiError::RuntimeNotReady);
    }
    Ok(record)
}

struct AuditFact<'a> {
    actor_principal_kind: &'a str,
    actor_subject: &'a str,
    action: &'a str,
    resource_kind: &'a str,
    resource_reference: &'a str,
    outcome: &'a str,
    reason_code: &'a str,
    policy_version: Option<String>,
}

async fn insert_audit(
    tx: &mut graphql_orm::graphql::orm::MutationContext<'_, DefaultWriteBackend>,
    fact: AuditFact<'_>,
) -> Result<(), OrmPublicError> {
    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
        actor_principal_kind: fact.actor_principal_kind.to_owned(),
        actor_subject: fact.actor_subject.to_owned(),
        action: fact.action.to_owned(),
        resource_kind: fact.resource_kind.to_owned(),
        resource_reference: fact.resource_reference.to_owned(),
        outcome: fact.outcome.to_owned(),
        reason_code: fact.reason_code.to_owned(),
        correlation_id: Uuid::new_v4().to_string(),
        causation_id: None,
        policy_version: fact.policy_version,
    })
    .await
    .map_err(OrmPublicError::from)?;
    Ok(())
}

const OPENAI_COMPATIBLE_DATA_POLICY_VERSION: u32 = 1;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredProviderDataPolicy {
    openai_compatible: StoredOpenAiCompatiblePolicy,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredOpenAiCompatiblePolicy {
    version: u32,
    retention: String,
    custom_tools: bool,
    parallel_tool_calls: bool,
    structured_output: bool,
    provider_retained_continuation: bool,
}

fn provider_data_policy(
    provider_kind: AiProviderKindInput,
    compatible: Option<AiOpenAiCompatibleProfileInput>,
) -> Result<serde_json::Value, AiError> {
    match (provider_kind, compatible) {
        (AiProviderKindInput::OpenAiCompatible, Some(profile)) => {
            validate_compatible_profile(
                &profile.retention,
                profile.custom_tools,
                profile.parallel_tool_calls,
            )
            .map_err(AiError::InvalidInput)?;
            serde_json::to_value(StoredProviderDataPolicy {
                openai_compatible: StoredOpenAiCompatiblePolicy {
                    version: OPENAI_COMPATIBLE_DATA_POLICY_VERSION,
                    retention: profile.retention,
                    custom_tools: profile.custom_tools,
                    parallel_tool_calls: profile.parallel_tool_calls,
                    structured_output: profile.structured_output,
                    provider_retained_continuation: profile.provider_retained_continuation,
                },
            })
            .map_err(|_| AiError::PersistenceFailed)
        }
        (AiProviderKindInput::OpenAiCompatible, None) => Err(AiError::InvalidInput(
            "OpenAI-compatible profiles require a reviewed capability and retention contract"
                .to_owned(),
        )),
        (_, Some(_)) => Err(AiError::InvalidInput(
            "OpenAI-compatible configuration is invalid for this provider kind".to_owned(),
        )),
        (_, None) => Ok(json!({})),
    }
}

fn provider_view(record: &AiProviderProfileRecord) -> Result<AiProviderProfileView, AiError> {
    let openai_compatible =
        if record.provider_kind == AiProviderKindInput::OpenAiCompatible.as_str() {
            if record
                .data_policy
                .as_object()
                .is_some_and(serde_json::Map::is_empty)
            {
                // Profiles created before the typed contract was introduced remain
                // visible but cannot construct the compatible transport adapter.
                None
            } else {
                let stored: StoredProviderDataPolicy =
                    serde_json::from_value(record.data_policy.clone()).map_err(|_| {
                        AiError::InvalidConfiguration(
                            "invalid OpenAI-compatible provider data policy".to_owned(),
                        )
                    })?;
                if stored.openai_compatible.version != OPENAI_COMPATIBLE_DATA_POLICY_VERSION {
                    return Err(AiError::InvalidConfiguration(
                        "unsupported OpenAI-compatible provider data-policy version".to_owned(),
                    ));
                }
                validate_compatible_profile(
                    &stored.openai_compatible.retention,
                    stored.openai_compatible.custom_tools,
                    stored.openai_compatible.parallel_tool_calls,
                )
                .map_err(AiError::InvalidConfiguration)?;
                Some(AiOpenAiCompatibleProfileView {
                    retention: stored.openai_compatible.retention,
                    custom_tools: stored.openai_compatible.custom_tools,
                    parallel_tool_calls: stored.openai_compatible.parallel_tool_calls,
                    structured_output: stored.openai_compatible.structured_output,
                    provider_retained_continuation: stored
                        .openai_compatible
                        .provider_retained_continuation,
                })
            }
        } else {
            if !record
                .data_policy
                .as_object()
                .is_some_and(serde_json::Map::is_empty)
            {
                return Err(AiError::InvalidConfiguration(
                    "unexpected provider data policy".to_owned(),
                ));
            }
            None
        };
    Ok(AiProviderProfileView {
        id: record.id,
        scope_kind: record.scope_kind.clone(),
        scope_id: record.scope_id.clone(),
        tenant_id: record.tenant_id.clone(),
        provider_kind: record.provider_kind.clone(),
        display_name: record.display_name.clone(),
        base_url: record.base_url.clone(),
        openai_compatible,
        credential_configured: record.credential_reference.is_some(),
        enabled: record.enabled,
        row_version: record.row_version,
        updated_at: record.updated_at,
    })
}

fn validate_compatible_profile(
    retention: &str,
    custom_tools: bool,
    parallel_tool_calls: bool,
) -> Result<(), String> {
    if retention.is_empty()
        || retention.len() > 200
        || !retention
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err("invalid OpenAI-compatible retention label".to_owned());
    }
    if parallel_tool_calls && !custom_tools {
        return Err(
            "parallel OpenAI-compatible tool calls require custom-tool capability".to_owned(),
        );
    }
    Ok(())
}

fn content_policy_view(record: &AiContentProtectionPolicyRecord) -> AiContentProtectionPolicyView {
    AiContentProtectionPolicyView {
        scope_kind: record.scope_kind.clone(),
        scope_id: record.scope_id.clone(),
        tenant_id: record.tenant_id.clone(),
        protection_mode: record.protection_mode.clone(),
        ready: record.ready && record.migration_state == "ready",
        row_version: record.row_version,
        effective_at: record.effective_at,
    }
}

fn retention_policy_view(record: &AiRetentionPolicyRecord) -> AiRetentionPolicyView {
    AiRetentionPolicyView {
        scope_kind: record.scope_kind.clone(),
        scope_id: record.scope_id.clone(),
        tenant_id: record.tenant_id.clone(),
        message_retention_seconds: record.message_retention_seconds,
        delta_retention_seconds: record.delta_retention_seconds,
        raw_payload_retention_seconds: record.raw_payload_retention_seconds,
        audit_retention_seconds: record.audit_retention_seconds,
        deleted_content_purge_seconds: record.deleted_content_purge_seconds,
        provider_file_delete_required: record.provider_file_delete_required,
        inbox_event_retention_seconds: record.inbox_event_retention_seconds.unwrap_or_default(),
        inbox_minimum_events: record.inbox_minimum_events.unwrap_or_default(),
        row_version: record.row_version,
        updated_at: record.updated_at,
    }
}

fn budget_policy_view(record: &AiBudgetPolicyRecord) -> AiBudgetPolicyView {
    AiBudgetPolicyView {
        id: record.id,
        scope_kind: record.scope_kind.clone(),
        scope_id: record.scope_id.clone(),
        tenant_id: record.tenant_id.clone(),
        principal_kind: record.principal_kind.clone(),
        principal_subject: record.principal_subject.clone(),
        interval_kind: record.interval_kind.clone(),
        maximum_input_tokens: record.maximum_input_tokens,
        maximum_output_tokens: record.maximum_output_tokens,
        maximum_tool_units: record.maximum_tool_units,
        maximum_image_units: record.maximum_image_units,
        maximum_cost_microunits: record.maximum_cost_microunits,
        maximum_runs: record.maximum_runs,
        enabled: record.enabled,
        row_version: record.row_version,
        updated_at: record.updated_at,
    }
}

fn exact_scope_key_for_record(record: &AiBudgetPolicyRecord) -> String {
    scope_key(&AiScope {
        kind: record.scope_kind.clone(),
        id: record.scope_id.clone(),
        tenant_id: record.tenant_id.clone(),
    })
}

fn profile_scope(record: &AiProviderProfileRecord) -> AiScope {
    AiScope {
        kind: record.scope_kind.clone(),
        id: record.scope_id.clone(),
        tenant_id: record.tenant_id.clone(),
    }
}

pub(crate) fn scope_key(scope: &AiScope) -> String {
    crate::ai_scope_key(scope)
}

fn validate_retention_input(input: &SetAiRetentionPolicyInput) -> Result<(), AiError> {
    const MINIMUM_RETENTION_SECONDS: i64 = 60;
    const MAXIMUM_RETENTION_SECONDS: i64 = 315_576_000;
    let durations = [
        Some(input.delta_retention_seconds),
        Some(input.raw_payload_retention_seconds),
        Some(input.audit_retention_seconds),
        Some(input.deleted_content_purge_seconds),
        Some(input.inbox_event_retention_seconds),
        input.message_retention_seconds,
    ];
    if durations
        .into_iter()
        .flatten()
        .any(|seconds| !(MINIMUM_RETENTION_SECONDS..=MAXIMUM_RETENTION_SECONDS).contains(&seconds))
        || !(1..=100_000).contains(&input.inbox_minimum_events)
    {
        return Err(AiError::InvalidInput(
            "invalid AI retention policy bounds".to_owned(),
        ));
    }
    Ok(())
}

fn normalize_and_validate_budget_policy_input(
    input: &mut UpsertAiBudgetPolicyInput,
    maximum: AiBudgetAmounts,
) -> Result<(), AiError> {
    if input.id.is_some_and(|id| id.is_nil())
        || input.expected_version.is_some_and(|version| version < 0)
        || input.id.is_some() != input.expected_version.is_some()
        || input.principal_kind.is_some() != input.principal_subject.is_some()
    {
        return Err(AiError::InvalidInput(
            "invalid AI budget policy identity".to_owned(),
        ));
    }
    if let (Some(kind), Some(subject)) = (
        input.principal_kind.as_mut(),
        input.principal_subject.as_mut(),
    ) {
        *kind = kind.trim().to_owned();
        *subject = subject.trim().to_owned();
        let api_token_kind = kind.strip_prefix("api_token:");
        let valid_kind = kind == "user"
            || api_token_kind.is_some_and(|value| {
                !value.is_empty()
                    && value.len() <= 64
                    && value.bytes().all(|byte| {
                        byte.is_ascii_lowercase()
                            || byte.is_ascii_digit()
                            || matches!(byte, b'_' | b'-')
                    })
            });
        if !valid_kind
            || subject.is_empty()
            || subject.len() > 512
            || subject.chars().any(char::is_control)
        {
            return Err(AiError::InvalidInput(
                "invalid AI budget policy principal".to_owned(),
            ));
        }
    }
    let configured = [
        (input.maximum_input_tokens, maximum.input_tokens),
        (input.maximum_output_tokens, maximum.output_tokens),
        (input.maximum_tool_units, maximum.tool_units),
        (input.maximum_image_units, maximum.image_units),
        (input.maximum_cost_microunits, maximum.cost_microunits),
        (input.maximum_runs, maximum.runs),
    ];
    if !configured.iter().any(|(value, _)| value.is_some())
        || configured.into_iter().any(|(value, hard_maximum)| {
            value.is_some_and(|value| {
                u64::try_from(value).map_or(true, |value| value > hard_maximum)
            })
        })
    {
        return Err(AiError::InvalidInput(
            "invalid AI budget policy ceilings".to_owned(),
        ));
    }
    Ok(())
}

fn normalize_endpoint(
    kind: AiProviderKindInput,
    base_url: Option<String>,
    endpoint_policy: &dyn AiProviderEndpointPolicy,
) -> Result<Option<String>, AiError> {
    let configurable = matches!(
        kind,
        AiProviderKindInput::Ollama | AiProviderKindInput::OpenAiCompatible
    );
    if !configurable {
        return if base_url.is_none() {
            Ok(None)
        } else {
            Err(AiError::InvalidInput(
                "native provider endpoints are deployment-fixed".to_owned(),
            ))
        };
    }
    let raw = base_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AiError::InvalidInput("provider endpoint is required".to_owned()))?;
    let mut url = Url::parse(raw)
        .map_err(|_| AiError::InvalidInput("invalid provider endpoint".to_owned()))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(AiError::InvalidInput("unsafe provider endpoint".to_owned()));
    }
    url.set_query(None);
    url.set_fragment(None);
    let normalized = url.to_string();
    if !endpoint_policy.authorize_endpoint(kind, &normalized) {
        return Err(AiError::Forbidden);
    }
    Ok(Some(normalized))
}

fn validate_scope(scope: &AiScope) -> Result<(), AiError> {
    if scope.kind.trim().is_empty()
        || scope.id.trim().is_empty()
        || scope.kind.len() > 128
        || scope.id.len() > 512
        || scope
            .tenant_id
            .as_ref()
            .is_some_and(|tenant| tenant.trim().is_empty() || tenant.len() > 512)
    {
        return Err(AiError::InvalidInput("invalid AI scope".to_owned()));
    }
    Ok(())
}

fn principal_kind(principal: &AuthPrincipal) -> String {
    match principal {
        AuthPrincipal::User(_) => "user".to_owned(),
        AuthPrincipal::ApiToken(token) => {
            format!("api_token:{}", token.principal_kind.as_str())
        }
    }
}

fn protection_mode_value(mode: AiContentProtectionMode) -> &'static str {
    match mode {
        AiContentProtectionMode::DatabaseManaged => "database_managed",
        AiContentProtectionMode::ApplicationEncrypted => "application_encrypted",
    }
}

fn parse_protection_mode(value: &str) -> Result<AiContentProtectionMode, AiError> {
    match value {
        "database_managed" => Ok(AiContentProtectionMode::DatabaseManaged),
        "application_encrypted" => Ok(AiContentProtectionMode::ApplicationEncrypted),
        _ => Err(AiError::InvalidConfiguration(
            "unknown content-protection mode".to_owned(),
        )),
    }
}

fn unix_seconds() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
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
