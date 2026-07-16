use std::collections::BTreeSet;

use graphql_orm_ai::*;
use serde_json::json;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

fn source(classification: DataClassification) -> AiDataSourceRef {
    AiDataSourceRef {
        kind: "message_block".to_owned(),
        reference: "block-1".to_owned(),
        classification,
        trust: AiSourceTrust::UserProvided,
    }
}

fn manifest(classification: DataClassification) -> AiEgressManifest {
    AiEgressManifest {
        provider_profile_id: "profile-1".to_owned(),
        provider_kind: "openai".to_owned(),
        model: "model-1".to_owned(),
        destination: "managed-provider".to_owned(),
        destination_trust: AiDestinationTrust::ManagedProvider,
        capability: AiEgressCapability::ModelInference,
        scope: AiScope::new("project", "project-7"),
        session_id: Some(AiSessionId::new()),
        run_id: Some(AiRunId::new()),
        sources: vec![source(classification)],
        estimated_bytes: 100,
        estimated_tokens: 25,
        attachment_count: 0,
        purpose: "assistant_response".to_owned(),
        retention: "zero-retention".to_owned(),
        residency: Some("au".to_owned()),
        policy_version: "policy-1".to_owned(),
        consent_reference: None,
    }
}

fn disclosure_schema() -> AiDisclosureSchema {
    AiDisclosureSchema::new(
        "records-v1",
        AiDisclosureShape::object(
            AiDisclosureRule::exportable(DataClassification::Internal),
            [(
                "records".to_owned(),
                AiDisclosureShape::list(
                    AiDisclosureRule::exportable(DataClassification::Internal),
                    100,
                    AiDisclosureShape::object(
                        AiDisclosureRule::exportable(DataClassification::Internal),
                        [(
                            "id".to_owned(),
                            AiDisclosureShape::scalar(AiDisclosureRule::exportable(
                                DataClassification::Internal,
                            )),
                        )],
                    ),
                ),
            )],
        ),
    )
    .expect("disclosure schema should validate")
}

fn contract(document: &str, disclosure: &AiDisclosureSchema) -> GraphqlOperationContract {
    GraphqlOperationContract::new(
        GraphqlExecutionTargetId::parse("application").expect("target ID"),
        "schema-v1",
        "Search",
        document,
        "records-projection-v1",
        disclosure.fingerprint.clone(),
    )
    .expect("operation contract should validate")
}

#[test]
fn changed_egress_manifest_invalidates_allow_decision() {
    let original = manifest(DataClassification::Internal);
    let decision = AiEgressDecision::allow(&original, "policy-1", "user-1");
    assert!(decision.authorize(&original).is_ok());

    let mut changed = original.clone();
    changed.estimated_bytes += 1;
    assert!(matches!(
        decision.authorize(&changed),
        Err(AiError::EgressDenied)
    ));
}

#[test]
fn remote_graphql_targets_require_exact_audience_resource_and_schema_bindings() {
    let mut targets = GraphqlExecutionTargetRegistry::new();
    assert!(matches!(
        targets.register(GraphqlExecutionTarget {
            id: GraphqlExecutionTargetId::parse("private-router").expect("target ID"),
            class: GraphqlExecutionTargetClass::PrivateRouted,
            audience: None,
            resource_type: Some("project".to_owned()),
            resource_id: Some("project-7".to_owned()),
            schema_fingerprint: "schema-v1".to_owned(),
        }),
        Err(ToolExecutionError::InvalidTarget)
    ));
    targets
        .register(GraphqlExecutionTarget {
            id: GraphqlExecutionTargetId::parse("private-router").expect("target ID"),
            class: GraphqlExecutionTargetClass::PrivateRouted,
            audience: Some("private-graphql".to_owned()),
            resource_type: Some("project".to_owned()),
            resource_id: Some("project-7".to_owned()),
            schema_fingerprint: "schema-v1".to_owned(),
        })
        .expect("fully bound remote target should register");
}

#[test]
fn deployment_boundary_always_denies_secrets() {
    let boundary = AiDeploymentEgressBoundary {
        allowed_destination_trust: BTreeSet::from([AiDestinationTrust::ManagedProvider]),
        allowed_capabilities: BTreeSet::from([AiEgressCapability::ModelInference]),
        maximum_classification: DataClassification::Secret,
        maximum_bytes: u64::MAX,
        maximum_attachments: u32::MAX,
    };

    assert_eq!(
        boundary.evaluate(&manifest(DataClassification::Secret)),
        Err(AiEgressReason::SecretDataDenied)
    );
}

#[test]
fn tool_catalog_is_discovery_not_enablement() {
    let disclosure = disclosure_schema();
    let document = "query Search($term: String!) { records(term: $term) { id } }";
    let descriptor = AiToolDescriptor::new(
        "records.search",
        "Search readable records",
        AiToolOperationKind::Query,
        document,
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": { "term": { "type": "string" } },
            "required": ["term"],
            "additionalProperties": false
        }),
    )
    .expect("descriptor should validate")
    .with_result_projection("records-projection-v1")
    .with_graphql_contract(contract(document, &disclosure));
    let mut catalog = AiToolCatalog::new();
    catalog
        .register_with_disclosure(descriptor.clone(), disclosure)
        .expect("registration should succeed");

    let mut policy = AiToolPolicySet::new(ToolMaturity::ReadOnly);
    assert!(!policy.allows(&descriptor));

    policy.bind(AiToolPolicyBinding {
        tool_id: descriptor.id.clone(),
        fingerprint: "stale".to_owned(),
        enabled: true,
    });
    assert!(!policy.allows(&descriptor));

    policy.bind(AiToolPolicyBinding {
        tool_id: descriptor.id.clone(),
        fingerprint: descriptor.fingerprint.clone(),
        enabled: true,
    });
    assert!(policy.allows(&descriptor));
}

#[test]
fn static_disclosure_rejects_unknown_and_never_export_fields() {
    let schema = disclosure_schema();
    let allowed = schema
        .evaluate(&json!({"records": [{"id": "54"}]}))
        .expect("known projection should validate");
    assert_eq!(allowed.maximum_classification, DataClassification::Internal);
    assert_eq!(
        allowed
            .tighten(DataClassification::Confidential)
            .maximum_classification,
        DataClassification::Confidential
    );
    assert_eq!(
        schema.evaluate(&json!({"records": [{"id": "54", "secret": "no"}]})),
        Err(AiDisclosureError::UnknownField)
    );

    let forbidden = AiDisclosureSchema::new(
        "forbidden-v1",
        AiDisclosureShape::object(
            AiDisclosureRule::exportable(DataClassification::Internal),
            [(
                "credential".to_owned(),
                AiDisclosureShape::scalar(AiDisclosureRule::never_export(
                    DataClassification::Secret,
                )),
            )],
        ),
    )
    .expect("schema should validate");
    assert_eq!(
        forbidden.evaluate(&json!({"credential": null})),
        Err(AiDisclosureError::NeverExport)
    );
}

#[test]
fn tool_catalog_rejects_ai_control_plane_and_introspection() {
    let disclosure = disclosure_schema();
    for document in [
        "query Search { aiSessions { id } }",
        "query Search { __schema { queryType { name } } }",
    ] {
        let descriptor = AiToolDescriptor::new(
            "unsafe.search",
            "Unsafe recursive operation",
            AiToolOperationKind::Query,
            document,
            json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object"
            }),
        )
        .expect("descriptor construction is separate from catalog admission")
        .with_result_projection("records-projection-v1")
        .with_graphql_contract(contract(document, &disclosure));
        let mut catalog = AiToolCatalog::new();
        assert!(matches!(
            catalog.register_with_disclosure(descriptor, disclosure.clone()),
            Err(AiError::InvalidConfiguration(_))
        ));
    }
}

#[test]
fn budget_proof_is_bound_to_exact_provider_model_and_unit_ceilings() {
    let run_id = AiRunId::new();
    let attempt_id = Uuid::new_v4();
    let now = OffsetDateTime::now_utc();
    let reservation = AiBudgetReservation::new_reserved(
        AiBudgetReservationId::new(),
        run_id,
        attempt_id,
        7,
        ProviderKind::OpenAi,
        "model-a",
        "pricing-v1",
        AiBudgetAmounts {
            output_tokens: 256,
            tool_units: 2,
            runs: 1,
            ..AiBudgetAmounts::default()
        },
        now + Duration::minutes(5),
    )
    .expect("reservation should validate");

    assert!(
        reservation
            .authorize_provider_call(
                run_id,
                attempt_id,
                7,
                &ProviderKind::OpenAi,
                "model-a",
                256,
                2,
                now,
            )
            .is_ok()
    );
    assert!(matches!(
        reservation.authorize_provider_call(
            run_id,
            attempt_id,
            7,
            &ProviderKind::OpenAi,
            "model-b",
            256,
            2,
            now,
        ),
        Err(ProviderError::BudgetDenied)
    ));
    assert!(matches!(
        reservation.authorize_provider_call(
            run_id,
            attempt_id,
            7,
            &ProviderKind::OpenAi,
            "model-a",
            257,
            2,
            now,
        ),
        Err(ProviderError::BudgetDenied)
    ));
    assert!(matches!(
        reservation.authorize_provider_call(
            run_id,
            attempt_id,
            7,
            &ProviderKind::OpenAi,
            "model-a",
            256,
            3,
            now,
        ),
        Err(ProviderError::BudgetDenied)
    ));
}

#[test]
fn approval_invalidates_when_resource_or_policy_binding_changes() {
    let disclosure = disclosure_schema();
    let document = "mutation Search($id: ID!) { updateRecord(id: $id) { id } }";
    let resource = AiApprovalResourceBinding {
        resource_type: "record".to_owned(),
        resource_id: "54".to_owned(),
        expected_version: "7".to_owned(),
    };
    let preview = AiCanonicalActionPreview {
        action_kind: "update_record".to_owned(),
        title: "Update record 54".to_owned(),
        targets: vec![resource.clone()],
        details: json!({"fields": ["title"]}),
    };
    let binding = AiApprovalBinding {
        tool_call_id: AiToolCallId::new(),
        session_id: AiSessionId::new(),
        scope: AiScope::new("collection", "9").with_tenant_id("tenant-a"),
        tool_fingerprint: "tool-v1".to_owned(),
        argument_hash: "arguments-v1".to_owned(),
        operation: contract(document, &disclosure),
        principal_reference_fingerprint: "principal-v1".to_owned(),
        delegated_actor_subject: Some("user-a".to_owned()),
        delegation_reference: Some("grant-1".to_owned()),
        policy_version: "policy-v1".to_owned(),
        authorization_state_digest: "auth-v1".to_owned(),
        resources: vec![resource],
        preview_hash: preview.stable_hash(),
    };
    binding.validate(&preview).expect("binding should validate");
    let now = OffsetDateTime::now_utc();
    let grant = AiApprovalGrant {
        id: AiApprovalId::new(),
        binding_hash: binding.stable_hash(),
        approver_subject: "user-a".to_owned(),
        state: AiApprovalState::Approved,
        approved_at: now,
        expires_at: now + Duration::minutes(5),
    };
    assert!(grant.authorize(&binding, now).is_ok());

    let mut changed = binding.clone();
    changed.resources[0].expected_version = "8".to_owned();
    assert!(matches!(
        grant.authorize(&changed, now),
        Err(AiError::Forbidden)
    ));
    changed = binding.clone();
    changed.policy_version = "policy-v2".to_owned();
    assert!(matches!(
        grant.authorize(&changed, now),
        Err(AiError::Forbidden)
    ));
}

#[test]
fn proposal_only_ceiling_rejects_application_mutation_descriptor() {
    let descriptor = AiToolDescriptor::new(
        "records.publish",
        "Publish a record",
        AiToolOperationKind::Mutation,
        "mutation Publish($id: ID!) { publish(id: $id) { id } }",
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object"
        }),
    )
    .expect("descriptor should validate")
    .with_maturity(ToolMaturity::SupervisedWrite)
    .with_risk(AiToolRisk::HighImpact, AiApprovalRule::OneShot);
    let mut policy = AiToolPolicySet::new(ToolMaturity::ProposalOnly);
    policy.bind(AiToolPolicyBinding {
        tool_id: descriptor.id.clone(),
        fingerprint: descriptor.fingerprint.clone(),
        enabled: true,
    });

    assert!(!policy.allows(&descriptor));
}

#[test]
fn proposals_require_schema_and_provenance() {
    let schema = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "title": { "type": "string", "minLength": 1 }
        },
        "required": ["title"],
        "additionalProperties": false
    });
    let descriptor = AiProposalTypeDescriptor::new("records.metadata.v1", "1", schema)
        .expect("proposal schema should compile")
        .with_required_source_kinds(vec!["resolver_result".to_owned()]);
    let proposal_type = descriptor.id.clone();
    let mut catalog = AiProposalCatalog::new();
    catalog
        .register(descriptor)
        .expect("descriptor registration should succeed");

    let invalid = AiProposalDraft {
        proposal_type: proposal_type.clone(),
        session_id: AiSessionId::new(),
        run_id: AiRunId::new(),
        scope: AiScope::new("project", "7"),
        payload: json!({"title": "suggested"}),
        sources: vec![source(DataClassification::Internal)],
        item_count: 1,
    };
    assert!(matches!(
        catalog.validate(invalid),
        Err(AiError::InvalidInput(_))
    ));

    let valid = AiProposalDraft {
        proposal_type,
        session_id: AiSessionId::new(),
        run_id: AiRunId::new(),
        scope: AiScope::new("project", "7"),
        payload: json!({"title": "suggested"}),
        sources: vec![AiDataSourceRef {
            kind: "resolver_result".to_owned(),
            reference: "tool-artifact-1".to_owned(),
            classification: DataClassification::Internal,
            trust: AiSourceTrust::ResolverResult,
        }],
        item_count: 1,
    };
    assert!(catalog.validate(valid).is_ok());
}
