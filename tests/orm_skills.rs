#![cfg(feature = "sqlite")]

use std::sync::Arc;

use agql_auth::{
    AccessTokenMetadata, AssuranceMatchMode, AuthPrincipal, AuthUser, FixedClock, MfaAcceptance,
    RecentMfaPolicy, SessionAssurance, SessionContext,
};
use async_trait::async_trait;
use graphql_orm::graphql::orm::{ApplyOptions, OrmSchemaModule};
use graphql_orm::prelude::{Database, SqliteBackend};
use graphql_orm_ai::*;
use serde_json::json;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

struct ExactScopeAccess;

#[async_trait]
impl AiSkillAccessPolicy for ExactScopeAccess {
    async fn can_access_skill(
        &self,
        _principal: &AuthPrincipal,
        scope: &AiScope,
        _action: AiSkillAction,
    ) -> bool {
        scope == &skill_scope()
    }
}

struct ProtectionPolicy;

#[async_trait]
impl AiContentProtectionPolicyResolver for ProtectionPolicy {
    async fn resolve(
        &self,
        _principal: &AuthPrincipal,
        scope: &AiScope,
    ) -> Result<AiContentProtectionPolicy, AiError> {
        Ok(AiContentProtectionPolicy {
            scope: scope.clone(),
            mode: AiContentProtectionMode::DatabaseManaged,
            key_policy_reference: None,
            version: 1,
            ready: true,
        })
    }
}

fn now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_800_000_000).expect("fixed time should validate")
}

fn principal(recent_mfa: bool) -> AuthPrincipal {
    let now = now();
    let session = if recent_mfa {
        let assurance = SessionAssurance::new(
            now,
            ["otp", "pwd"],
            Some("urn:test:loa:2".to_owned()),
            Some("test".to_owned()),
            MfaAcceptance::Satisfied,
        )
        .expect("test assurance should validate");
        SessionContext::default().with_assurance(assurance)
    } else {
        SessionContext::default()
    };
    AuthPrincipal::User(AuthUser {
        user_id: "skill-admin".to_owned(),
        session_id: Uuid::new_v4(),
        roles: vec!["admin".to_owned()],
        scopes: vec![],
        session,
        token_claims: AccessTokenMetadata {
            auth_time: recent_mfa.then_some(now.unix_timestamp()),
            amr: recent_mfa.then(|| vec!["otp".to_owned(), "pwd".to_owned()]),
            acr: recent_mfa.then(|| "urn:test:loa:2".to_owned()),
            tenant_id: Some("tenant-1".to_owned()),
            ..AccessTokenMetadata::default()
        },
    })
}

fn skill_scope() -> AiScope {
    AiScope::new("workspace", "workspace-1").with_tenant_id("tenant-1")
}

fn scope_input() -> AiScopeInput {
    AiScopeInput {
        kind: "workspace".to_owned(),
        id: "workspace-1".to_owned(),
        tenant_id: Some("tenant-1".to_owned()),
    }
}

async fn service() -> OrmAiSkillCatalogService {
    let database = Database::<SqliteBackend>::connect_sqlite("sqlite::memory:")
        .await
        .expect("in-memory SQLite should open");
    let module = AiSchemaModule;
    let plan = database
        .schema()
        .plan_migration_to_entities(
            "ai-skill-test-v1",
            "AI protected skill catalog test",
            module.entities(),
        )
        .await
        .expect("skill schema should plan");
    database
        .schema()
        .apply_migration(&plan, ApplyOptions::default())
        .await
        .expect("skill schema should apply to in-memory SQLite");
    OrmAiSkillCatalogService::new(
        database,
        Arc::new(ExactScopeAccess),
        Arc::new(ProtectionPolicy),
        Arc::new(DatabaseManagedContentProtector),
        RecentMfaPolicy {
            maximum_age: Duration::minutes(5),
            clock_skew: Duration::seconds(30),
            allowed_amr: vec!["otp".to_owned()],
            allowed_acr: vec!["urn:test:loa:2".to_owned()],
            match_mode: AssuranceMatchMode::All,
        },
        Arc::new(FixedClock::new(now())),
    )
}

fn publish_input(skill_id: Uuid, expected_skill_version: i64) -> PublishAiSkillVersionInput {
    PublishAiSkillVersionInput {
        skill_id,
        expected_skill_version,
        version: "2026.07.1".to_owned(),
        instructions: "Summarize only data returned by authorized tools.".to_owned(),
        allowed_tool_fingerprints: vec!["a".repeat(64), "b".repeat(64)],
        maximum_classification: AiSkillClassificationInput::Confidential,
        maximum_tool_maturity: AiSkillMaturityInput::ProposalOnly,
        activation: AiSkillActivationInput::Manual,
        input_schema: async_graphql::Json(json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "additionalProperties": false
        })),
        output_schema: async_graphql::Json(json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": { "summary": { "type": "string" } },
            "required": ["summary"],
            "additionalProperties": false
        })),
        required_provider_capabilities: AiSkillProviderCapabilitiesInput {
            structured_output: true,
            custom_tools: true,
            ..Default::default()
        },
        budget: AiSkillBudgetInput {
            maximum_steps: 8,
            maximum_duration_seconds: 120,
            maximum_output_tokens: 4_096,
            maximum_cost_microunits: Some(1_000_000),
        },
        allowed_proposal_types: vec!["generic.summary".to_owned()],
        allowed_ui_intents: vec![AiSkillUiIntentBindingInput {
            intent_type: "generic.open_resource".to_owned(),
            descriptor_fingerprint: "c".repeat(64),
        }],
        enable: true,
    }
}

#[tokio::test]
async fn skill_publication_is_mfa_cas_scope_and_protection_bound() {
    let service = service().await;
    let denied = service
        .upsert_skill(
            &principal(false),
            UpsertAiSkillInput {
                id: None,
                scope: scope_input(),
                name: "Bounded summary".to_owned(),
                description: "Produces a structured proposal.".to_owned(),
                expected_version: None,
            },
        )
        .await;
    assert!(matches!(denied, Err(AiError::RecentMfaRequired)));

    let principal = principal(true);
    let created = service
        .upsert_skill(
            &principal,
            UpsertAiSkillInput {
                id: None,
                scope: scope_input(),
                name: "Bounded summary".to_owned(),
                description: "Produces a structured proposal.".to_owned(),
                expected_version: None,
            },
        )
        .await
        .expect("recent MFA and exact scope should create safe metadata");
    assert!(!created.enabled);
    assert!(created.current_version.is_none());
    assert_eq!(created.row_version, 0);

    let published = service
        .publish_version(&principal, publish_input(created.id, created.row_version))
        .await
        .expect("protected immutable version should publish");
    assert!(published.enabled);
    assert_eq!(published.row_version, 1);
    let version = published
        .current_version
        .as_ref()
        .expect("published skill should expose redacted current metadata");
    assert_eq!(version.checksum.len(), 64);
    assert_eq!(
        version.allowed_tool_fingerprints,
        vec!["a".repeat(64), "b".repeat(64)]
    );
    assert_eq!(
        version.allowed_ui_intents[0].intent_type,
        "generic.open_resource"
    );

    let listed = service
        .skills(&principal, skill_scope())
        .await
        .expect("exact scope metadata should list");
    assert_eq!(listed.len(), 1);
    let resolved = service
        .resolve_enabled_skills(&principal, skill_scope())
        .await
        .expect("current principal should open the protected version");
    assert_eq!(resolved.len(), 1);
    assert_eq!(
        resolved[0].instructions,
        "Summarize only data returned by authorized tools."
    );
    assert_eq!(
        resolved[0].required_provider_capabilities["custom_tools"],
        true
    );

    assert!(matches!(
        service
            .set_enabled(
                &principal,
                SetAiSkillEnabledInput {
                    skill_id: created.id,
                    enabled: false,
                    expected_version: 0,
                },
            )
            .await,
        Err(AiError::Conflict)
    ));
    let disabled = service
        .set_enabled(
            &principal,
            SetAiSkillEnabledInput {
                skill_id: created.id,
                enabled: false,
                expected_version: published.row_version,
            },
        )
        .await
        .expect("current CAS version should disable selection");
    assert!(!disabled.enabled);
    assert!(
        service
            .resolve_enabled_skills(&principal, skill_scope())
            .await
            .expect("disabled catalog should still resolve safely")
            .is_empty()
    );

    assert!(matches!(
        service
            .skills(
                &principal,
                AiScope::new("workspace", "workspace-2").with_tenant_id("tenant-1"),
            )
            .await,
        Err(AiError::Forbidden)
    ));
}

#[tokio::test]
async fn publication_rejects_duplicate_capability_bindings_and_versions() {
    let service = service().await;
    let principal = principal(true);
    let skill = service
        .upsert_skill(
            &principal,
            UpsertAiSkillInput {
                id: None,
                scope: scope_input(),
                name: "Validation".to_owned(),
                description: "Exercises canonical policy validation.".to_owned(),
                expected_version: None,
            },
        )
        .await
        .expect("skill metadata should create");
    let mut duplicate_binding = publish_input(skill.id, skill.row_version);
    duplicate_binding.allowed_tool_fingerprints = vec!["a".repeat(64), "a".repeat(64)];
    assert!(matches!(
        service.publish_version(&principal, duplicate_binding).await,
        Err(AiError::InvalidInput(_))
    ));

    let published = service
        .publish_version(&principal, publish_input(skill.id, skill.row_version))
        .await
        .expect("canonical policy should publish");
    assert!(matches!(
        service
            .publish_version(&principal, publish_input(skill.id, published.row_version))
            .await,
        Err(AiError::Conflict)
    ));
}
