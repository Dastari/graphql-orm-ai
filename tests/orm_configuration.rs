#![cfg(feature = "sqlite")]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use agql_auth::{
    AccessTokenMetadata, AssuranceMatchMode, AuthPrincipal, AuthUser, FixedClock, MfaAcceptance,
    RecentMfaPolicy, SessionAssurance, SessionContext,
};
use async_trait::async_trait;
use graphql_orm::graphql::orm::{ApplyOptions, OrmSchemaModule};
use graphql_orm::prelude::{Database, SqliteBackend};
use graphql_orm_ai::*;
use secrecy::SecretString;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

struct AllowConfiguration;

#[async_trait]
impl AiConfigurationAccessPolicy for AllowConfiguration {
    async fn can_configure(
        &self,
        _principal: &AuthPrincipal,
        _scope: &AiScope,
        _action: AiConfigurationAction,
    ) -> bool {
        true
    }
}

struct LocalEndpointPolicy;

impl AiProviderEndpointPolicy for LocalEndpointPolicy {
    fn authorize_endpoint(&self, provider_kind: AiProviderKindInput, normalized_url: &str) -> bool {
        provider_kind == AiProviderKindInput::Ollama && normalized_url == "http://127.0.0.1:11434/"
    }
}

#[derive(Default)]
struct MemorySecretStore {
    next: AtomicU64,
    references: std::sync::Mutex<BTreeSet<SecretRef>>,
}

impl MemorySecretStore {
    fn count(&self) -> usize {
        self.references.lock().expect("secret lock").len()
    }
}

#[async_trait]
impl AiSecretStore for MemorySecretStore {
    async fn resolve(&self, _reference: &SecretRef) -> Result<SecretString, SecretError> {
        Err(SecretError::Unavailable)
    }

    async fn put(
        &self,
        reference: Option<&SecretRef>,
        _value: SecretString,
    ) -> Result<SecretRef, SecretError> {
        assert!(
            reference.is_none(),
            "configuration uses fresh secret references"
        );
        let value = self.next.fetch_add(1, Ordering::SeqCst);
        let reference = SecretRef::parse(format!("memory:{value}"))?;
        self.references
            .lock()
            .expect("secret lock")
            .insert(reference.clone());
        Ok(reference)
    }

    async fn delete(&self, reference: &SecretRef) -> Result<(), SecretError> {
        self.references
            .lock()
            .expect("secret lock")
            .remove(reference);
        Ok(())
    }
}

fn recent_principal(now: OffsetDateTime) -> AuthPrincipal {
    let assurance = SessionAssurance::new(
        now,
        ["otp", "pwd"],
        Some("urn:test:loa:2".to_owned()),
        Some("test".to_owned()),
        MfaAcceptance::Satisfied,
    )
    .expect("valid assurance");
    AuthPrincipal::User(AuthUser {
        user_id: "admin-1".to_owned(),
        session_id: Uuid::new_v4(),
        roles: vec!["admin".to_owned()],
        scopes: vec![],
        session: SessionContext::default().with_assurance(assurance),
        token_claims: AccessTokenMetadata {
            auth_time: Some(now.unix_timestamp()),
            amr: Some(vec!["otp".to_owned(), "pwd".to_owned()]),
            acr: Some("urn:test:loa:2".to_owned()),
            tenant_id: Some("tenant-1".to_owned()),
            ..AccessTokenMetadata::default()
        },
    })
}

fn no_mfa_principal() -> AuthPrincipal {
    AuthPrincipal::User(AuthUser {
        user_id: "admin-1".to_owned(),
        session_id: Uuid::new_v4(),
        roles: vec!["admin".to_owned()],
        scopes: vec![],
        session: SessionContext::default(),
        token_claims: AccessTokenMetadata::default(),
    })
}

fn scope() -> AiScope {
    AiScope::new("tenant", "tenant-1").with_tenant_id("tenant-1")
}

fn scope_input() -> AiScopeInput {
    AiScopeInput {
        kind: "tenant".to_owned(),
        id: "tenant-1".to_owned(),
        tenant_id: Some("tenant-1".to_owned()),
    }
}

async fn service() -> (
    OrmAiConfigurationService,
    Arc<MemorySecretStore>,
    OffsetDateTime,
) {
    let database = Database::<SqliteBackend>::connect_sqlite("sqlite::memory:")
        .await
        .expect("in-memory SQLite opens");
    let module = AiSchemaModule;
    let plan = database
        .schema()
        .plan_migration_to_entities(
            "ai-configuration-test-v1",
            "AI configuration service test",
            module.entities(),
        )
        .await
        .expect("schema plans");
    database
        .schema()
        .apply_migration(&plan, ApplyOptions::default())
        .await
        .expect("schema applies to in-memory SQLite");
    let now = OffsetDateTime::from_unix_timestamp(1_800_000_000).expect("fixed time");
    let secrets = Arc::new(MemorySecretStore::default());
    let service = OrmAiConfigurationService::new(
        database,
        Arc::new(AllowConfiguration),
        Arc::new(LocalEndpointPolicy),
        RecentMfaPolicy {
            maximum_age: Duration::minutes(5),
            clock_skew: Duration::seconds(30),
            allowed_amr: vec!["otp".to_owned()],
            allowed_acr: vec!["urn:test:loa:2".to_owned()],
            match_mode: AssuranceMatchMode::All,
        },
        Arc::new(FixedClock::new(now)),
        secrets.clone(),
    )
    .with_budget_policy_management(
        AiBudgetPolicyManagementLimits::new(
            AiBudgetAmounts {
                input_tokens: 1_000_000,
                output_tokens: 1_000_000,
                tool_units: 10_000,
                image_units: 10_000,
                cost_microunits: 1_000_000_000,
                runs: 100_000,
            },
            10,
        )
        .expect("budget management limits should validate"),
    );
    (service, secrets, now)
}

#[tokio::test]
async fn profile_and_credential_mutations_require_recent_mfa_and_cas() {
    let (service, secrets, now) = service().await;
    assert!(matches!(
        service
            .upsert_provider_profile(
                &no_mfa_principal(),
                UpsertAiProviderProfileInput {
                    id: None,
                    scope: scope_input(),
                    provider_kind: AiProviderKindInput::OpenAi,
                    display_name: "OpenAI".to_owned(),
                    base_url: None,
                    enabled: true,
                    expected_version: None,
                },
            )
            .await,
        Err(AiError::RecentMfaRequired)
    ));
    let principal = recent_principal(now);
    let profile = service
        .upsert_provider_profile(
            &principal,
            UpsertAiProviderProfileInput {
                id: None,
                scope: scope_input(),
                provider_kind: AiProviderKindInput::OpenAi,
                display_name: "OpenAI".to_owned(),
                base_url: None,
                enabled: true,
                expected_version: None,
            },
        )
        .await
        .expect("profile is created with recent MFA");
    assert_eq!(profile.row_version, 0);
    assert!(!profile.credential_configured);
    assert!(matches!(
        service
            .upsert_provider_profile(
                &principal,
                UpsertAiProviderProfileInput {
                    id: Some(profile.id),
                    scope: scope_input(),
                    provider_kind: AiProviderKindInput::OpenAi,
                    display_name: "stale".to_owned(),
                    base_url: None,
                    enabled: true,
                    expected_version: Some(99),
                },
            )
            .await,
        Err(AiError::Conflict)
    ));

    let credential = service
        .set_provider_credential(
            &principal,
            profile.id,
            SecretString::from("test-secret-one".to_owned()),
            0,
        )
        .await
        .expect("credential reference is committed");
    assert!(credential.credential_configured);
    assert_eq!(credential.row_version, 1);
    assert_eq!(secrets.count(), 1);
    assert!(matches!(
        service
            .upsert_provider_profile(
                &principal,
                UpsertAiProviderProfileInput {
                    id: Some(profile.id),
                    scope: scope_input(),
                    provider_kind: AiProviderKindInput::LocalHarness,
                    display_name: "must not retain a credential".to_owned(),
                    base_url: None,
                    enabled: true,
                    expected_version: Some(1),
                },
            )
            .await,
        Err(AiError::InvalidInput(_))
    ));
    let rotated = service
        .set_provider_credential(
            &principal,
            profile.id,
            SecretString::from("test-secret-two".to_owned()),
            1,
        )
        .await
        .expect("rotation replaces and cleans the old reference");
    assert_eq!(rotated.row_version, 2);
    assert_eq!(secrets.count(), 1);
    let removed = service
        .remove_provider_credential(
            &principal,
            RemoveAiProviderCredentialInput {
                profile_id: profile.id,
                expected_version: 2,
            },
        )
        .await
        .expect("credential removal records cleanup");
    assert!(!removed.credential_configured);
    assert_eq!(removed.row_version, 3);
    assert_eq!(secrets.count(), 0);

    let profiles = service
        .provider_profiles(&principal, scope())
        .await
        .expect("redacted profile query");
    assert_eq!(profiles.len(), 1);
    assert_eq!(profiles[0].id, profile.id);
}

#[tokio::test]
async fn endpoint_policy_and_content_protection_readiness_fail_closed() {
    let (service, secrets, now) = service().await;
    let principal = recent_principal(now);
    assert!(matches!(
        service
            .upsert_provider_profile(
                &principal,
                UpsertAiProviderProfileInput {
                    id: None,
                    scope: scope_input(),
                    provider_kind: AiProviderKindInput::OpenAiCompatible,
                    display_name: "unsafe".to_owned(),
                    base_url: Some("http://169.254.169.254/latest?token=x".to_owned()),
                    enabled: true,
                    expected_version: None,
                },
            )
            .await,
        Err(AiError::InvalidInput(_)) | Err(AiError::Forbidden)
    ));
    let installed = service
        .upsert_provider_profile(
            &principal,
            UpsertAiProviderProfileInput {
                id: None,
                scope: scope_input(),
                provider_kind: AiProviderKindInput::LocalHarness,
                display_name: "Reviewed installed harness".to_owned(),
                base_url: None,
                enabled: true,
                expected_version: None,
            },
        )
        .await
        .expect("logical installed profile needs no configurable endpoint");
    assert!(installed.base_url.is_none());
    assert!(matches!(
        service
            .set_provider_credential(
                &principal,
                installed.id,
                SecretString::from("must-not-be-stored".to_owned()),
                0,
            )
            .await,
        Err(AiError::InvalidInput(_))
    ));
    assert_eq!(secrets.count(), 0);
    assert!(matches!(
        service
            .upsert_provider_profile(
                &principal,
                UpsertAiProviderProfileInput {
                    id: Some(installed.id),
                    scope: scope_input(),
                    provider_kind: AiProviderKindInput::LocalHarness,
                    display_name: "unsafe endpoint".to_owned(),
                    base_url: Some("http://127.0.0.1:9999".to_owned()),
                    enabled: true,
                    expected_version: Some(0),
                },
            )
            .await,
        Err(AiError::InvalidInput(_))
    ));
    let local = service
        .upsert_provider_profile(
            &principal,
            UpsertAiProviderProfileInput {
                id: None,
                scope: scope_input(),
                provider_kind: AiProviderKindInput::Ollama,
                display_name: "Local Ollama".to_owned(),
                base_url: Some("http://127.0.0.1:11434".to_owned()),
                enabled: true,
                expected_version: None,
            },
        )
        .await
        .expect("deployment policy permits exact local endpoint");
    assert_eq!(local.base_url.as_deref(), Some("http://127.0.0.1:11434/"));

    let database_managed = service
        .set_content_protection_policy(
            &principal,
            SetAiContentProtectionPolicyInput {
                scope: scope_input(),
                mode: AiContentProtectionModeInput::DatabaseManaged,
                key_policy_reference: None,
                expected_version: None,
            },
        )
        .await
        .expect("database-managed policy is immediately ready");
    assert!(database_managed.ready);
    let resolved = AiContentProtectionPolicyResolver::resolve(&service, &principal, &scope())
        .await
        .expect("ready policy resolves");
    assert!(resolved.ready);

    let application_encrypted = service
        .set_content_protection_policy(
            &principal,
            SetAiContentProtectionPolicyInput {
                scope: scope_input(),
                mode: AiContentProtectionModeInput::ApplicationEncrypted,
                key_policy_reference: Some("kms:tenant-1/chat".to_owned()),
                expected_version: Some(0),
            },
        )
        .await
        .expect("mode change records a pending migration");
    assert!(!application_encrypted.ready);
    let resolved = AiContentProtectionPolicyResolver::resolve(&service, &principal, &scope())
        .await
        .expect("pending policy remains inspectable");
    assert!(!resolved.ready);
}

#[tokio::test]
async fn budget_policies_are_recent_mfa_bounded_cas_managed_and_scope_exact() {
    let (service, _secrets, now) = service().await;
    let create = || UpsertAiBudgetPolicyInput {
        id: None,
        scope: scope_input(),
        principal_kind: Some("user".to_owned()),
        principal_subject: Some("member-7".to_owned()),
        interval: AiBudgetIntervalInput::Month,
        maximum_input_tokens: Some(50_000),
        maximum_output_tokens: Some(10_000),
        maximum_tool_units: Some(100),
        maximum_image_units: Some(20),
        maximum_cost_microunits: Some(5_000_000),
        maximum_runs: Some(100),
        enabled: true,
        expected_version: None,
    };
    assert!(matches!(
        service
            .upsert_budget_policy(&no_mfa_principal(), create())
            .await,
        Err(AiError::RecentMfaRequired)
    ));
    let principal = recent_principal(now);
    let policy = service
        .upsert_budget_policy(&principal, create())
        .await
        .expect("budget policy should create under recent MFA");
    assert_eq!(policy.row_version, 0);
    assert_eq!(policy.principal_subject.as_deref(), Some("member-7"));

    let policies = service
        .budget_policies(&principal, scope())
        .await
        .expect("exact scope policies should be visible");
    assert_eq!(policies.len(), 1);
    assert_eq!(policies[0].id, policy.id);

    let updated = service
        .upsert_budget_policy(
            &principal,
            UpsertAiBudgetPolicyInput {
                id: Some(policy.id),
                expected_version: Some(0),
                maximum_runs: Some(50),
                enabled: false,
                ..create()
            },
        )
        .await
        .expect("exact CAS update should succeed");
    assert_eq!(updated.row_version, 1);
    assert_eq!(updated.maximum_runs, Some(50));
    assert!(!updated.enabled);

    assert!(matches!(
        service
            .upsert_budget_policy(
                &principal,
                UpsertAiBudgetPolicyInput {
                    id: Some(policy.id),
                    expected_version: Some(0),
                    ..create()
                },
            )
            .await,
        Err(AiError::Conflict)
    ));
    assert!(matches!(
        service
            .upsert_budget_policy(
                &principal,
                UpsertAiBudgetPolicyInput {
                    id: Some(policy.id),
                    expected_version: Some(1),
                    interval: AiBudgetIntervalInput::Day,
                    ..create()
                },
            )
            .await,
        Err(AiError::NotFound)
    ));
    assert!(matches!(
        service
            .upsert_budget_policy(
                &principal,
                UpsertAiBudgetPolicyInput {
                    maximum_input_tokens: Some(1_000_001),
                    ..create()
                },
            )
            .await,
        Err(AiError::InvalidInput(_))
    ));
    assert!(matches!(
        service
            .upsert_budget_policy(
                &principal,
                UpsertAiBudgetPolicyInput {
                    principal_subject: None,
                    ..create()
                },
            )
            .await,
        Err(AiError::InvalidInput(_))
    ));
    let one_policy_service = service.clone().with_budget_policy_management(
        AiBudgetPolicyManagementLimits::new(
            AiBudgetAmounts {
                input_tokens: 1_000_000,
                output_tokens: 1_000_000,
                tool_units: 10_000,
                image_units: 10_000,
                cost_microunits: 1_000_000_000,
                runs: 100_000,
            },
            1,
        )
        .expect("single-policy limit should validate"),
    );
    assert!(matches!(
        one_policy_service
            .upsert_budget_policy(&principal, create())
            .await,
        Err(AiError::InvalidInput(_))
    ));
}

#[tokio::test]
async fn retention_policy_is_recent_mfa_protected_bounded_and_cas_managed() {
    let (service, _secrets, now) = service().await;
    let input = |expected_version| SetAiRetentionPolicyInput {
        scope: scope_input(),
        message_retention_seconds: Some(31_536_000),
        delta_retention_seconds: 3_600,
        raw_payload_retention_seconds: 86_400,
        audit_retention_seconds: 31_536_000,
        deleted_content_purge_seconds: 86_400,
        provider_file_delete_required: true,
        inbox_event_retention_seconds: 604_800,
        inbox_minimum_events: 100,
        expected_version,
    };
    assert!(matches!(
        service
            .set_retention_policy(&no_mfa_principal(), input(None))
            .await,
        Err(AiError::RecentMfaRequired)
    ));
    let principal = recent_principal(now);
    let created = service
        .set_retention_policy(&principal, input(None))
        .await
        .expect("retention policy is created");
    assert_eq!(created.row_version, 0);
    assert_eq!(created.inbox_minimum_events, 100);
    let loaded = service
        .retention_policy(&principal, scope())
        .await
        .expect("retention query is authorized")
        .expect("retention policy exists");
    assert_eq!(loaded.inbox_event_retention_seconds, 604_800);
    assert!(matches!(
        service
            .set_retention_policy(&principal, input(Some(99)))
            .await,
        Err(AiError::Conflict)
    ));
    assert!(matches!(
        service
            .set_retention_policy(
                &principal,
                SetAiRetentionPolicyInput {
                    inbox_minimum_events: 0,
                    ..input(Some(0))
                },
            )
            .await,
        Err(AiError::InvalidInput(_))
    ));
}
