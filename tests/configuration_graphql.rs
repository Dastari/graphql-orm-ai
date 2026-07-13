use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use agql_auth::{AccessTokenMetadata, AuthPrincipal, AuthUser, SessionContext};
use async_graphql::{EmptySubscription, Request, Schema};
use async_trait::async_trait;
use graphql_orm_ai::*;
use secrecy::{ExposeSecret, SecretString};
use uuid::Uuid;

struct ConfigurationService {
    expected_secret_received: AtomicBool,
}

impl ConfigurationService {
    fn profile(profile_id: Uuid) -> AiProviderProfileView {
        AiProviderProfileView {
            id: profile_id,
            scope_kind: "project".to_owned(),
            scope_id: "project-1".to_owned(),
            tenant_id: Some("tenant-1".to_owned()),
            provider_kind: "openai".to_owned(),
            display_name: "OpenAI".to_owned(),
            base_url: None,
            credential_configured: true,
            enabled: true,
            row_version: 2,
            updated_at: 1,
        }
    }
}

#[async_trait]
impl AiConfigurationService for ConfigurationService {
    async fn provider_profiles(
        &self,
        _principal: &AuthPrincipal,
        _scope: AiScope,
    ) -> Result<Vec<AiProviderProfileView>, AiError> {
        Ok(Vec::new())
    }

    async fn content_protection_policy(
        &self,
        _principal: &AuthPrincipal,
        _scope: AiScope,
    ) -> Result<Option<AiContentProtectionPolicyView>, AiError> {
        Ok(None)
    }

    async fn retention_policy(
        &self,
        _principal: &AuthPrincipal,
        _scope: AiScope,
    ) -> Result<Option<AiRetentionPolicyView>, AiError> {
        Ok(None)
    }

    async fn budget_policies(
        &self,
        _principal: &AuthPrincipal,
        _scope: AiScope,
    ) -> Result<Vec<AiBudgetPolicyView>, AiError> {
        Ok(Vec::new())
    }

    async fn upsert_provider_profile(
        &self,
        _principal: &AuthPrincipal,
        input: UpsertAiProviderProfileInput,
    ) -> Result<AiProviderProfileView, AiError> {
        Ok(Self::profile(input.id.unwrap_or_else(Uuid::new_v4)))
    }

    async fn set_provider_credential(
        &self,
        _principal: &AuthPrincipal,
        profile_id: Uuid,
        credential: SecretString,
        _expected_version: i64,
    ) -> Result<AiProviderProfileView, AiError> {
        self.expected_secret_received.store(
            credential.expose_secret() == "synthetic-test-secret",
            Ordering::Release,
        );
        Ok(Self::profile(profile_id))
    }

    async fn remove_provider_credential(
        &self,
        _principal: &AuthPrincipal,
        input: RemoveAiProviderCredentialInput,
    ) -> Result<AiProviderProfileView, AiError> {
        let mut view = Self::profile(input.profile_id);
        view.credential_configured = false;
        Ok(view)
    }

    async fn set_content_protection_policy(
        &self,
        _principal: &AuthPrincipal,
        input: SetAiContentProtectionPolicyInput,
    ) -> Result<AiContentProtectionPolicyView, AiError> {
        Ok(AiContentProtectionPolicyView {
            scope_kind: input.scope.kind,
            scope_id: input.scope.id,
            tenant_id: input.scope.tenant_id,
            protection_mode: "database_managed".to_owned(),
            ready: true,
            row_version: 1,
            effective_at: 1,
        })
    }

    async fn set_retention_policy(
        &self,
        _principal: &AuthPrincipal,
        input: SetAiRetentionPolicyInput,
    ) -> Result<AiRetentionPolicyView, AiError> {
        Ok(AiRetentionPolicyView {
            scope_kind: input.scope.kind,
            scope_id: input.scope.id,
            tenant_id: input.scope.tenant_id,
            message_retention_seconds: input.message_retention_seconds,
            delta_retention_seconds: input.delta_retention_seconds,
            raw_payload_retention_seconds: input.raw_payload_retention_seconds,
            audit_retention_seconds: input.audit_retention_seconds,
            deleted_content_purge_seconds: input.deleted_content_purge_seconds,
            provider_file_delete_required: input.provider_file_delete_required,
            inbox_event_retention_seconds: input.inbox_event_retention_seconds,
            inbox_minimum_events: input.inbox_minimum_events,
            row_version: 1,
            updated_at: 1,
        })
    }

    async fn upsert_budget_policy(
        &self,
        _principal: &AuthPrincipal,
        input: UpsertAiBudgetPolicyInput,
    ) -> Result<AiBudgetPolicyView, AiError> {
        Ok(AiBudgetPolicyView {
            id: input.id.unwrap_or_else(Uuid::new_v4),
            scope_kind: input.scope.kind,
            scope_id: input.scope.id,
            tenant_id: input.scope.tenant_id,
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
            row_version: 0,
            updated_at: 1,
        })
    }
}

fn principal() -> AuthPrincipal {
    AuthPrincipal::User(AuthUser {
        user_id: "admin-1".to_owned(),
        session_id: Uuid::from_u128(1),
        roles: vec!["admin".to_owned()],
        scopes: vec!["ai:configure".to_owned()],
        session: SessionContext::default(),
        token_claims: AccessTokenMetadata::default(),
    })
}

#[tokio::test]
async fn credential_mutation_returns_only_redacted_state() {
    let service = Arc::new(ConfigurationService {
        expected_secret_received: AtomicBool::new(false),
    });
    let service_data: Arc<dyn AiConfigurationService> = service.clone();
    let schema = Schema::build(
        AiConfigurationQueryRoot,
        AiConfigurationMutationRoot,
        EmptySubscription,
    )
    .data(service_data)
    .finish();
    let profile_id = Uuid::new_v4();
    #[cfg(not(feature = "graphql-case-pascal"))]
    let document = format!(
        "mutation {{ setAiProviderCredential(input: {{ profileId: \"{profile_id}\", credential: \"synthetic-test-secret\", expectedVersion: 1 }}) {{ id credentialConfigured rowVersion }} }}"
    );
    #[cfg(feature = "graphql-case-pascal")]
    let document = format!(
        "mutation {{ SetAiProviderCredential(Input: {{ ProfileId: \"{profile_id}\", Credential: \"synthetic-test-secret\", ExpectedVersion: 1 }}) {{ Id CredentialConfigured RowVersion }} }}"
    );
    let request = Request::new(document).data(principal());
    let response = schema.execute(request).await;

    assert!(response.errors.is_empty());
    assert!(service.expected_secret_received.load(Ordering::Acquire));
    let serialized = serde_json::to_string(&response.data).expect("response should serialize");
    assert!(!serialized.contains("synthetic-test-secret"));
    assert!(!serialized.contains("credentialReference"));
    assert!(!schema.sdl().contains("credentialReference"));
    #[cfg(not(feature = "graphql-case-pascal"))]
    {
        assert!(schema.sdl().contains("aiRetentionPolicy(scope:"));
        assert!(schema.sdl().contains("setAiRetentionPolicy(input:"));
        assert!(schema.sdl().contains("aiBudgetPolicies(scope:"));
        assert!(schema.sdl().contains("upsertAiBudgetPolicy(input:"));
    }
    #[cfg(feature = "graphql-case-pascal")]
    {
        assert!(schema.sdl().contains("AiRetentionPolicy(Scope:"));
        assert!(schema.sdl().contains("SetAiRetentionPolicy(Input:"));
        assert!(schema.sdl().contains("AiBudgetPolicies(Scope:"));
        assert!(schema.sdl().contains("UpsertAiBudgetPolicy(Input:"));
    }
}

#[tokio::test]
async fn configuration_roots_fail_closed_without_authentication() {
    let service: Arc<dyn AiConfigurationService> = Arc::new(ConfigurationService {
        expected_secret_received: AtomicBool::new(false),
    });
    let schema = Schema::build(
        AiConfigurationQueryRoot,
        AiConfigurationMutationRoot,
        EmptySubscription,
    )
    .data(service)
    .finish();
    let response = schema
        .execute("{ aiProviderProfiles(scope: { kind: \"project\", id: \"1\" }) { id } }")
        .await;

    assert!(!response.errors.is_empty());
}

#[tokio::test]
async fn budget_policy_mutation_uses_the_configured_case_and_redacted_view() {
    let service: Arc<dyn AiConfigurationService> = Arc::new(ConfigurationService {
        expected_secret_received: AtomicBool::new(false),
    });
    let schema = Schema::build(
        AiConfigurationQueryRoot,
        AiConfigurationMutationRoot,
        EmptySubscription,
    )
    .data(service)
    .finish();
    #[cfg(not(feature = "graphql-case-pascal"))]
    let document = r#"
        mutation {
            upsertAiBudgetPolicy(input: {
                scope: { kind: "project", id: "project-1", tenantId: "tenant-1" }
                principalKind: "user"
                principalSubject: "member-7"
                interval: MONTH
                maximumInputTokens: 50000
                maximumRuns: 100
                enabled: true
            }) { id intervalKind maximumInputTokens maximumRuns rowVersion }
        }
    "#;
    #[cfg(feature = "graphql-case-pascal")]
    let document = r#"
        mutation {
            UpsertAiBudgetPolicy(Input: {
                Scope: { Kind: "project", Id: "project-1", TenantId: "tenant-1" }
                PrincipalKind: "user"
                PrincipalSubject: "member-7"
                Interval: Month
                MaximumInputTokens: 50000
                MaximumRuns: 100
                Enabled: true
            }) { Id IntervalKind MaximumInputTokens MaximumRuns RowVersion }
        }
    "#;
    let response = schema
        .execute(Request::new(document).data(principal()))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let serialized = serde_json::to_string(&response.data).expect("response should serialize");
    assert!(!serialized.contains("principalSubject"));
    assert!(!serialized.contains("PrincipalSubject"));
}
