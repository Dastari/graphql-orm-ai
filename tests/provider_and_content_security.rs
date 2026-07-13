use futures::TryStreamExt;
use graphql_orm_ai::*;
use serde_json::json;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

fn request(model: &str) -> ModelRequest {
    ModelRequest {
        model: model.to_owned(),
        instructions: vec!["Use only authorized tools.".to_owned()],
        input: vec![ModelInputBlock::Text {
            text: "synthetic input".to_owned(),
        }],
        continuation: None,
        tools: vec![],
        builtin_tools: vec![],
        output_schema: None,
        maximum_output_tokens: Some(64),
    }
}

fn manifest(
    session_id: AiSessionId,
    run_id: AiRunId,
    model: &str,
    capability: AiEgressCapability,
) -> AiEgressManifest {
    AiEgressManifest {
        provider_profile_id: "test-profile".to_owned(),
        provider_kind: "openai_compatible".to_owned(),
        model: model.to_owned(),
        destination: "local-test".to_owned(),
        destination_trust: AiDestinationTrust::Local,
        capability,
        scope: AiScope::new("test", "scope"),
        session_id: Some(session_id),
        run_id: Some(run_id),
        sources: vec![AiDataSourceRef {
            kind: "message".to_owned(),
            reference: "synthetic".to_owned(),
            classification: DataClassification::Public,
            trust: AiSourceTrust::UserProvided,
        }],
        estimated_bytes: 10_000,
        estimated_tokens: 1_000,
        attachment_count: 0,
        purpose: "test".to_owned(),
        retention: "none".to_owned(),
        residency: None,
        policy_version: "test".to_owned(),
        consent_reference: None,
    }
}

fn proof(manifest: &AiEgressManifest) -> AuthorizedEgress {
    AiEgressDecision::allow(manifest, "test", "test-user")
        .authorize(manifest)
        .expect("test manifest should authorize")
}

fn budget(
    run_id: AiRunId,
    provider_kind: ProviderKind,
    model: &str,
) -> AuthorizedBudgetReservation {
    let attempt_id = Uuid::new_v4();
    AiBudgetReservation::new_reserved(
        AiBudgetReservationId::new(),
        run_id,
        attempt_id,
        1,
        provider_kind.clone(),
        model,
        "test-pricing-v1",
        AiBudgetAmounts {
            input_tokens: 1_000,
            output_tokens: 64,
            runs: 1,
            ..AiBudgetAmounts::default()
        },
        OffsetDateTime::now_utc() + Duration::hours(1),
    )
    .expect("test budget should validate")
    .authorize_provider_call(
        run_id,
        attempt_id,
        1,
        &provider_kind,
        model,
        64,
        OffsetDateTime::now_utc(),
    )
    .expect("test budget should authorize")
}

#[tokio::test]
async fn provider_context_rejects_model_swap_before_mock_receives_request() {
    let session_id = AiSessionId::new();
    let run_id = AiRunId::new();
    let authorized = manifest(
        session_id,
        run_id,
        "authorized-model",
        AiEgressCapability::ModelInference,
    );
    let context = ProviderRequestContext::new(
        session_id,
        run_id,
        "correlation",
        budget(run_id, ProviderKind::OpenAiCompatible, "authorized-model"),
        authorized.clone(),
        proof(&authorized),
    )
    .expect("context should validate");
    let provider = MockProvider::new(vec![ProviderEvent::ResponseCompleted {
        response_id: Some("mock".to_owned()),
    }]);

    assert!(matches!(
        provider.stream(request("swapped-model"), context).await,
        Err(ProviderError::BudgetDenied)
    ));
    assert_eq!(provider.request_count(), 0);
}

#[tokio::test]
async fn each_provider_builtin_requires_its_own_egress_capability() {
    let session_id = AiSessionId::new();
    let run_id = AiRunId::new();
    let inference = manifest(
        session_id,
        run_id,
        "test-model",
        AiEgressCapability::ModelInference,
    );
    let base_context = ProviderRequestContext::new(
        session_id,
        run_id,
        "correlation",
        budget(run_id, ProviderKind::OpenAiCompatible, "test-model"),
        inference.clone(),
        proof(&inference),
    )
    .expect("context should validate");
    let mut web_request = request("test-model");
    web_request.builtin_tools = vec![ModelBuiltinTool::WebSearch {
        allowed_domains: vec!["example.com".to_owned()],
    }];
    let provider = MockProvider::new(vec![ProviderEvent::ResponseCompleted {
        response_id: Some("mock".to_owned()),
    }]);

    assert!(matches!(
        provider
            .stream(web_request.clone(), base_context.clone())
            .await,
        Err(ProviderError::EgressDenied)
    ));

    let web = manifest(
        session_id,
        run_id,
        "test-model",
        AiEgressCapability::WebSearch,
    );
    let context = base_context
        .with_authorized_transfer(web.clone(), proof(&web))
        .expect("separate web grant should bind");
    let events = provider
        .stream(web_request, context)
        .await
        .expect("fully authorized request should start")
        .try_collect::<Vec<_>>()
        .await
        .expect("mock stream should complete");

    assert_eq!(events.len(), 1);
    assert_eq!(provider.request_count(), 1);
}

#[tokio::test]
async fn database_managed_protection_refuses_wrong_mode_and_unready_policy() {
    let protector = DatabaseManagedContentProtector;
    let context = ContentProtectionContext {
        entity: "message_block".to_owned(),
        row_id: "row-1".to_owned(),
        field: "content".to_owned(),
        scope: AiScope::new("test", "scope"),
    };
    let mut policy = AiContentProtectionPolicy {
        scope: context.scope.clone(),
        mode: AiContentProtectionMode::DatabaseManaged,
        key_policy_reference: None,
        version: 1,
        ready: false,
    };
    assert_eq!(
        protector
            .protect(&policy, &context, json!({"text": "private"}))
            .await,
        Err(ContentProtectionError::PolicyNotReady)
    );

    policy.ready = true;
    let envelope = protector
        .protect(&policy, &context, json!({"text": "private"}))
        .await
        .expect("ready database policy should protect");
    assert_eq!(
        protector
            .open(&policy, &context, &envelope)
            .await
            .expect("matching mode should open"),
        json!({"text": "private"})
    );

    policy.mode = AiContentProtectionMode::ApplicationEncrypted;
    assert_eq!(
        protector.open(&policy, &context, &envelope).await,
        Err(ContentProtectionError::ValidationFailed)
    );
}

#[tokio::test]
async fn bootstrap_secret_store_is_explicitly_mapped_and_read_only() {
    let reference = SecretRef::parse("provider/openai/test").expect("reference should parse");
    let store = EnvironmentSecretStore::new();
    assert!(matches!(
        store.resolve(&reference).await,
        Err(SecretError::Unavailable)
    ));
    assert_eq!(store.delete(&reference).await, Err(SecretError::ReadOnly));
}
