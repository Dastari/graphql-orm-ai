#![cfg(feature = "sqlite")]

use std::collections::BTreeSet;
use std::sync::Arc;

use agql_auth::{
    AccessTokenMetadata, AssuranceMatchMode, AuthPrincipal, AuthUser, FixedClock, MfaAcceptance,
    RecentMfaPolicy, SessionAssurance, SessionContext,
};
use async_trait::async_trait;
use graphql_orm::graphql::orm::{ApplyOptions, OrmSchemaModule};
use graphql_orm::prelude::{Database, SqliteBackend};
use graphql_orm_ai::*;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

struct AllowRules;

#[async_trait]
impl AiRuleAccessPolicy for AllowRules {
    async fn can_access_rule(
        &self,
        _principal: &AuthPrincipal,
        scope: &AiScope,
        _action: AiRuleAction,
    ) -> bool {
        scope
            .tenant_id
            .as_deref()
            .is_none_or(|tenant| tenant == "tenant-1")
    }
}

struct FixedHierarchy(Vec<AiScope>);

#[async_trait]
impl AiRuleHierarchyResolver for FixedHierarchy {
    async fn hierarchy(
        &self,
        _principal: &AuthPrincipal,
        _target_scope: &AiScope,
    ) -> Result<Vec<AiScope>, AiError> {
        Ok(self.0.clone())
    }
}

fn now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_800_000_000).expect("fixed time should validate")
}

fn principal(recent_mfa: bool) -> AuthPrincipal {
    let now = now();
    let session = if recent_mfa {
        SessionContext::default().with_assurance(
            SessionAssurance::new(
                now,
                ["otp", "pwd"],
                Some("urn:test:loa:2".to_owned()),
                Some("test".to_owned()),
                MfaAcceptance::Satisfied,
            )
            .expect("assurance should validate"),
        )
    } else {
        SessionContext::default()
    };
    AuthPrincipal::User(AuthUser {
        user_id: "rule-admin".to_owned(),
        session_id: Uuid::new_v4(),
        roles: vec!["admin".to_owned()],
        scopes: Vec::new(),
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

fn application_scope() -> AiScope {
    AiScope::new("application", "application-default")
}

fn project_scope() -> AiScope {
    AiScope::new("project", "project-1").with_tenant_id("tenant-1")
}

fn scope_input(scope: &AiScope) -> AiScopeInput {
    AiScopeInput {
        kind: scope.kind.clone(),
        id: scope.id.clone(),
        tenant_id: scope.tenant_id.clone(),
    }
}

fn set(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn provider_set(values: &[ProviderKind]) -> BTreeSet<ProviderKind> {
    values.iter().cloned().collect()
}

fn capability_set(values: &[AiRuleProviderCapability]) -> BTreeSet<AiRuleProviderCapability> {
    values.iter().copied().collect()
}

fn deployment_limits() -> AiRuleDeploymentLimits {
    AiRuleDeploymentLimits::new(
        4,
        AiRuleConstraints {
            enabled: true,
            maximum_classification: DataClassification::Restricted,
            maximum_tool_maturity: ToolMaturity::SupervisedWrite,
            approval_requirement: AiRuleApprovalRequirement::DescriptorPolicy,
            allowed_tool_fingerprints: Some(set(&[
                &"a".repeat(64),
                &"b".repeat(64),
                &"c".repeat(64),
            ])),
            allowed_provider_kinds: Some(provider_set(&[
                ProviderKind::OpenAi,
                ProviderKind::Ollama,
            ])),
            allowed_provider_capabilities: Some(capability_set(&[
                AiRuleProviderCapability::Streaming,
                AiRuleProviderCapability::CustomTools,
                AiRuleProviderCapability::StructuredOutput,
            ])),
            allow_provider_retention: true,
            allow_byok: true,
            budget: AiRuleBudgetCeilings {
                maximum_steps: Some(100),
                maximum_duration_seconds: Some(3_600),
                maximum_output_tokens: Some(16_000),
                maximum_cost_microunits: Some(10_000_000),
                maximum_provider_calls: Some(100),
                maximum_tool_units: Some(100),
                maximum_image_units: Some(10),
            },
        },
    )
    .expect("deployment limits should validate")
}

async fn service(hierarchy: Vec<AiScope>) -> OrmAiRulePolicyService {
    let database = Database::<SqliteBackend>::connect_sqlite("sqlite::memory:")
        .await
        .expect("in-memory SQLite should open");
    let module = AiSchemaModule;
    let plan = database
        .schema()
        .plan_migration_to_entities(
            "ai-rule-test-v1",
            "AI hierarchical rule test",
            module.entities(),
        )
        .await
        .expect("rule schema should plan");
    database
        .schema()
        .apply_migration(&plan, ApplyOptions::default())
        .await
        .expect("rule schema should apply");
    OrmAiRulePolicyService::new(
        database,
        Arc::new(AllowRules),
        Arc::new(FixedHierarchy(hierarchy)),
        RecentMfaPolicy {
            maximum_age: Duration::minutes(5),
            clock_skew: Duration::seconds(30),
            allowed_amr: vec!["otp".to_owned()],
            allowed_acr: vec!["urn:test:loa:2".to_owned()],
            match_mode: AssuranceMatchMode::All,
        },
        Arc::new(FixedClock::new(now())),
        deployment_limits(),
    )
}

fn broad_input() -> SetAiRulePolicyInput {
    SetAiRulePolicyInput {
        scope: scope_input(&application_scope()),
        enabled: true,
        maximum_classification: AiRuleClassificationInput::Confidential,
        maximum_tool_maturity: AiRuleToolMaturityInput::ProposalOnly,
        approval_requirement: AiRuleApprovalRequirement::DescriptorPolicy,
        allowed_tool_fingerprints: Some(vec!["a".repeat(64), "b".repeat(64)]),
        allowed_provider_kinds: Some(vec![
            AiProviderKindInput::OpenAi,
            AiProviderKindInput::Ollama,
        ]),
        allowed_provider_capabilities: Some(vec![
            AiRuleProviderCapability::Streaming,
            AiRuleProviderCapability::CustomTools,
            AiRuleProviderCapability::StructuredOutput,
        ]),
        allow_provider_retention: true,
        allow_byok: true,
        budget: AiRuleBudgetInput {
            maximum_steps: Some(20),
            maximum_duration_seconds: Some(600),
            maximum_output_tokens: Some(4_000),
            maximum_cost_microunits: Some(2_000_000),
            maximum_provider_calls: Some(20),
            maximum_tool_units: Some(20),
            maximum_image_units: Some(4),
        },
        expected_version: None,
    }
}

fn narrow_input() -> SetAiRulePolicyInput {
    SetAiRulePolicyInput {
        scope: scope_input(&project_scope()),
        enabled: true,
        maximum_classification: AiRuleClassificationInput::Internal,
        maximum_tool_maturity: AiRuleToolMaturityInput::ReadOnly,
        approval_requirement: AiRuleApprovalRequirement::OneShotForAllApplicationTools,
        allowed_tool_fingerprints: Some(vec!["b".repeat(64), "c".repeat(64)]),
        allowed_provider_kinds: Some(vec![AiProviderKindInput::Ollama]),
        allowed_provider_capabilities: Some(vec![
            AiRuleProviderCapability::Streaming,
            AiRuleProviderCapability::StructuredOutput,
        ]),
        allow_provider_retention: false,
        allow_byok: false,
        budget: AiRuleBudgetInput {
            maximum_steps: Some(5),
            maximum_duration_seconds: Some(120),
            maximum_output_tokens: Some(1_000),
            maximum_cost_microunits: Some(500_000),
            maximum_provider_calls: Some(5),
            maximum_tool_units: Some(2),
            maximum_image_units: Some(0),
        },
        expected_version: None,
    }
}

#[tokio::test]
async fn hierarchy_intersects_every_authorized_layer_without_granting_authority() {
    let service = service(vec![application_scope(), project_scope()]).await;
    assert!(matches!(
        service.set_policy(&principal(false), broad_input()).await,
        Err(AiError::RecentMfaRequired)
    ));
    let principal = principal(true);
    let broad = service
        .set_policy(&principal, broad_input())
        .await
        .expect("broad rule should create");
    let narrow = service
        .set_policy(&principal, narrow_input())
        .await
        .expect("specific rule should create");
    assert_eq!(broad.row_version, 0);
    assert_eq!(narrow.row_version, 0);

    let resolved = service
        .resolve_for_run(&principal, project_scope())
        .await
        .expect("exact configured hierarchy should resolve");
    assert_eq!(resolved.applied_layers().len(), 2);
    assert_eq!(resolved.fingerprint().len(), 64);
    assert_eq!(
        resolved.constraints().maximum_classification,
        DataClassification::Internal
    );
    assert_eq!(
        resolved.constraints().maximum_tool_maturity,
        ToolMaturity::ReadOnly
    );
    assert_eq!(
        resolved.constraints().allowed_tool_fingerprints,
        Some(set(&[&"b".repeat(64)]))
    );
    assert_eq!(resolved.constraints().budget.maximum_steps, Some(5));
    assert!(!resolved.constraints().allow_provider_retention);
    assert!(!resolved.constraints().allow_byok);
    assert_eq!(
        resolved.constrain_tool(
            &"b".repeat(64),
            ToolMaturity::ReadOnly,
            AiApprovalRule::None
        ),
        Some(AiApprovalRule::OneShot)
    );
    assert_eq!(
        resolved.constrain_tool(
            &"a".repeat(64),
            ToolMaturity::ReadOnly,
            AiApprovalRule::None
        ),
        None
    );
    assert!(resolved.permits_provider_request(
        &ProviderKind::Ollama,
        &capability_set(&[AiRuleProviderCapability::Streaming]),
        DataClassification::Internal,
        false,
        false,
    ));
    assert!(!resolved.permits_provider_request(
        &ProviderKind::OpenAi,
        &capability_set(&[AiRuleProviderCapability::Streaming]),
        DataClassification::Internal,
        false,
        false,
    ));
    assert!(!resolved.permits_provider_request(
        &ProviderKind::Ollama,
        &capability_set(&[AiRuleProviderCapability::Streaming]),
        DataClassification::Confidential,
        false,
        false,
    ));

    let mut stale_update = narrow_input();
    stale_update.expected_version = Some(5);
    assert!(matches!(
        service.set_policy(&principal, stale_update).await,
        Err(AiError::Conflict)
    ));
}

#[tokio::test]
async fn deployment_widening_missing_layers_and_cross_tenant_lineage_fail_closed() {
    let principal = principal(true);
    let rule_service = service(vec![application_scope(), project_scope()]).await;
    let mut widened = broad_input();
    widened.allowed_provider_kinds = Some(vec![AiProviderKindInput::Xai]);
    assert!(matches!(
        rule_service.set_policy(&principal, widened).await,
        Err(AiError::InvalidInput(_))
    ));

    rule_service
        .set_policy(&principal, broad_input())
        .await
        .expect("broad rule should create");
    assert!(matches!(
        rule_service
            .resolve_for_run(&principal, project_scope())
            .await,
        Err(AiError::Conflict)
    ));

    let foreign = AiScope::new("tenant", "tenant-2").with_tenant_id("tenant-2");
    let foreign_service = service(vec![application_scope(), foreign, project_scope()]).await;
    assert!(matches!(
        foreign_service
            .resolve_for_run(&principal, project_scope())
            .await,
        Err(AiError::Forbidden)
    ));
}
