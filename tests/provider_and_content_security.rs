use futures::TryStreamExt;
use graphql_orm_ai::*;
use serde_json::json;
use sha2::{Digest, Sha256};
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
        continuation_mode: ModelContinuationMode::ProviderRetained,
        tools: vec![],
        builtin_tools: vec![],
        maximum_builtin_tool_calls: None,
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
            tool_units: 64,
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
    web_request.maximum_builtin_tool_calls = Some(64);
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
async fn provider_metadata_is_bounded_unique_and_included_in_egress_size() {
    let mut raw_file_search = request("test-model");
    raw_file_search.builtin_tools = vec![ModelBuiltinTool::FileSearch {
        store_ids: vec!["vs_caller_authored".to_owned()],
        maximum_results: Some(5),
    }];
    raw_file_search.maximum_builtin_tool_calls = Some(1);
    assert!(matches!(
        raw_file_search.validate(),
        Err(ProviderError::InvalidRequest)
    ));

    let mut mismatched_ceiling = request("test-model");
    mismatched_ceiling.maximum_builtin_tool_calls = Some(1);
    assert!(matches!(
        mismatched_ceiling.validate(),
        Err(ProviderError::InvalidRequest)
    ));

    mismatched_ceiling.builtin_tools = vec![ModelBuiltinTool::WebSearch {
        allowed_domains: vec!["example.com".to_owned()],
    }];
    mismatched_ceiling.maximum_builtin_tool_calls = None;
    assert!(matches!(
        mismatched_ceiling.validate(),
        Err(ProviderError::InvalidRequest)
    ));

    let mut invalid = request("test-model");
    invalid.builtin_tools = vec![
        ModelBuiltinTool::WebSearch {
            allowed_domains: vec!["example.com".to_owned()],
        },
        ModelBuiltinTool::WebSearch {
            allowed_domains: vec!["other.example".to_owned()],
        },
    ];
    invalid.maximum_builtin_tool_calls = Some(2);
    assert!(matches!(
        invalid.validate(),
        Err(ProviderError::InvalidRequest)
    ));

    invalid.builtin_tools = vec![ModelBuiltinTool::WebSearch {
        allowed_domains: vec!["https://example.com/path".to_owned()],
    }];
    invalid.maximum_builtin_tool_calls = Some(1);
    assert!(matches!(
        invalid.validate(),
        Err(ProviderError::InvalidRequest)
    ));

    let session_id = AiSessionId::new();
    let run_id = AiRunId::new();
    let mut inference = manifest(
        session_id,
        run_id,
        "test-model",
        AiEgressCapability::ModelInference,
    );
    inference.estimated_bytes = 10_000;
    let context = ProviderRequestContext::new(
        session_id,
        run_id,
        "correlation",
        budget(run_id, ProviderKind::OpenAiCompatible, "test-model"),
        inference.clone(),
        proof(&inference),
    )
    .expect("context should bind");
    let mut schema_heavy = request("test-model");
    schema_heavy.output_schema = Some(json!({
        "type": "object",
        "description": "x".repeat(20_000),
        "additionalProperties": false
    }));
    let provider = MockProvider::new(vec![]);
    assert!(matches!(
        provider.stream(schema_heavy, context).await,
        Err(ProviderError::EgressDenied)
    ));
    assert_eq!(provider.request_count(), 0);
}

#[tokio::test]
async fn stateless_replay_requires_one_unique_proof_for_every_tool_result() {
    let session_id = AiSessionId::new();
    let run_id = AiRunId::new();
    let model = "test-model";
    let definition = ModelToolDefinition {
        tool_id: "records.read".to_owned(),
        provider_name: "records_read".to_owned(),
        fingerprint: "records-read-v1".to_owned(),
        description: "Read one authorized record".to_owned(),
        parameters: json!({
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "required": ["id"],
            "additionalProperties": false
        }),
        strict: true,
    };
    let stateless_request = ModelRequest {
        model: model.to_owned(),
        instructions: Vec::new(),
        input: vec![ModelInputBlock::ToolResult {
            call_id: "call-2".to_owned(),
            tool_id: "records.read".to_owned(),
            output: json!({"record": 55}),
        }],
        continuation: Some(ModelContinuation::StatelessConversation {
            instructions: vec!["Use only authorized tools.".to_owned()],
            messages: vec![
                ModelConversationMessage::User {
                    content: vec![ModelInputBlock::Text {
                        text: "Read records 54 and 55".to_owned(),
                    }],
                },
                ModelConversationMessage::Assistant {
                    content: String::new(),
                    tool_calls: vec![ModelConversationToolCall {
                        call_id: "call-1".to_owned(),
                        tool_id: "records.read".to_owned(),
                        provider_name: "records_read".to_owned(),
                        tool_fingerprint: "records-read-v1".to_owned(),
                        arguments: json!({"id": 54}),
                    }],
                },
                ModelConversationMessage::Tool {
                    call_id: "call-1".to_owned(),
                    tool_id: "records.read".to_owned(),
                    provider_name: "records_read".to_owned(),
                    output: json!({"record": 54}),
                },
                ModelConversationMessage::Assistant {
                    content: String::new(),
                    tool_calls: vec![ModelConversationToolCall {
                        call_id: "call-2".to_owned(),
                        tool_id: "records.read".to_owned(),
                        provider_name: "records_read".to_owned(),
                        tool_fingerprint: "records-read-v1".to_owned(),
                        arguments: json!({"id": 55}),
                    }],
                },
            ],
        }),
        continuation_mode: ModelContinuationMode::StatelessReplay,
        tools: vec![definition],
        builtin_tools: Vec::new(),
        maximum_builtin_tool_calls: None,
        output_schema: None,
        maximum_output_tokens: Some(64),
    };
    stateless_request
        .validate()
        .expect("bounded stateless history should validate");
    let inference = manifest(
        session_id,
        run_id,
        model,
        AiEgressCapability::ModelInference,
    );
    let base = ProviderRequestContext::new(
        session_id,
        run_id,
        "correlation",
        budget(run_id, ProviderKind::OpenAiCompatible, model),
        inference.clone(),
        proof(&inference),
    )
    .expect("inference proof should bind");
    let provider = MockProvider::new(vec![ProviderEvent::ResponseCompleted { response_id: None }]);
    assert!(matches!(
        provider
            .stream(stateless_request.clone(), base.clone())
            .await,
        Err(ProviderError::EgressDenied)
    ));

    let tool_manifest = |purpose: &str| {
        let mut manifest = manifest(session_id, run_id, model, AiEgressCapability::ToolResult);
        manifest.sources = vec![AiDataSourceRef {
            kind: "application_tool_result".to_owned(),
            reference: Uuid::new_v4().to_string(),
            classification: DataClassification::Internal,
            trust: AiSourceTrust::ResolverResult,
        }];
        manifest.purpose = purpose.to_owned();
        manifest
    };
    let first = tool_manifest("historical-result");
    let one_proof = base
        .clone()
        .with_authorized_transfer(first.clone(), proof(&first))
        .expect("first tool proof should bind");
    assert!(matches!(
        provider.stream(stateless_request.clone(), one_proof).await,
        Err(ProviderError::EgressDenied)
    ));

    let second = tool_manifest("current-result");
    let exact = base
        .with_authorized_transfer(first.clone(), proof(&first))
        .expect("historical proof should bind")
        .with_authorized_transfer(second.clone(), proof(&second))
        .expect("current proof should bind");
    provider
        .stream(stateless_request, exact)
        .await
        .expect("exact replay proofs should pass")
        .try_collect::<Vec<_>>()
        .await
        .expect("mock replay should complete");
    assert_eq!(provider.request_count(), 1);
}

#[tokio::test]
async fn attachment_egress_is_bound_to_exact_id_checksum_and_bytes() {
    let session_id = AiSessionId::new();
    let run_id = AiRunId::new();
    let attachment_id = Uuid::new_v4();
    let attachment_bytes = b"must-not-appear-in-provider-context-debug".to_vec();
    let sha256 = hex::encode(Sha256::digest(&attachment_bytes));
    let attachment_block = ModelInputBlock::Attachment {
        attachment_id: attachment_id.to_string(),
        mime: "image/png".to_owned(),
        byte_count: attachment_bytes.len() as u64,
        sha256: sha256.clone(),
    };
    let exact_reference = attachment_block
        .attachment_egress_reference()
        .expect("attachment should have a canonical source reference");
    let attachment_request = ModelRequest {
        model: "test-model".to_owned(),
        instructions: vec![],
        input: vec![attachment_block],
        continuation: None,
        continuation_mode: ModelContinuationMode::ProviderRetained,
        tools: vec![],
        builtin_tools: vec![],
        maximum_builtin_tool_calls: None,
        output_schema: None,
        maximum_output_tokens: Some(64),
    };
    let mut inference = manifest(
        session_id,
        run_id,
        "test-model",
        AiEgressCapability::ModelInference,
    );
    inference.attachment_count = 1;
    let mut image = manifest(
        session_id,
        run_id,
        "test-model",
        AiEgressCapability::ImageAnalysis,
    );
    image.attachment_count = 1;
    image.sources = vec![AiDataSourceRef {
        kind: "attachment".to_owned(),
        reference: exact_reference,
        classification: DataClassification::Confidential,
        trust: AiSourceTrust::UserProvided,
    }];
    let context = ProviderRequestContext::new(
        session_id,
        run_id,
        "correlation",
        budget(run_id, ProviderKind::OpenAiCompatible, "test-model"),
        inference.clone(),
        proof(&inference),
    )
    .expect("inference proof should bind")
    .with_authorized_transfer(image.clone(), proof(&image))
    .expect("exact image proof should bind")
    .with_resolved_attachments(
        &attachment_request,
        vec![
            AiResolvedProviderAttachment::new(
                AiProviderAttachmentRequest::try_from(&attachment_request.input[0])
                    .expect("attachment request should parse"),
                "test.png",
                attachment_bytes,
            )
            .expect("attachment bytes should bind"),
        ],
    )
    .expect("resolved attachment should bind");
    assert!(!format!("{context:?}").contains("must-not-appear-in-provider-context-debug"));
    let provider = MockProvider::new(vec![ProviderEvent::ResponseCompleted {
        response_id: Some("mock".to_owned()),
    }]);
    provider
        .stream(attachment_request.clone(), context.clone())
        .await
        .expect("exact attachment should pass context validation")
        .try_collect::<Vec<_>>()
        .await
        .expect("mock attachment stream should complete");

    for swapped_block in [
        ModelInputBlock::Attachment {
            attachment_id: attachment_id.to_string(),
            mime: "image/jpeg".to_owned(),
            byte_count: 1_024,
            sha256: sha256.clone(),
        },
        ModelInputBlock::Attachment {
            attachment_id: attachment_id.to_string(),
            mime: "image/png".to_owned(),
            byte_count: 2_048,
            sha256: sha256.clone(),
        },
        ModelInputBlock::Attachment {
            attachment_id: attachment_id.to_string(),
            mime: "image/png".to_owned(),
            byte_count: 1_024,
            sha256: "b".repeat(64),
        },
    ] {
        let mut swapped = attachment_request.clone();
        swapped.input = vec![swapped_block];
        assert!(matches!(
            provider.stream(swapped, context.clone()).await,
            Err(ProviderError::EgressDenied)
        ));
    }
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
