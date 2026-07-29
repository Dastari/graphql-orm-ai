#![cfg(feature = "sqlite")]

use graphql_orm::prelude::*;
use graphql_orm_ai::*;
use serde_json::json;

mod application_surface {
    use graphql_orm::prelude::*;

    #[derive(
        GraphQLEntity, GraphQLOperations, serde::Serialize, serde::Deserialize, Clone, Debug,
    )]
    #[graphql_entity(
        table = "generated_operation_test_records",
        plural = "GeneratedOperationTestRecords"
    )]
    pub struct GeneratedOperationTestRecord {
        #[primary_key]
        pub id: String,
        #[filterable(type = "string")]
        #[sortable]
        pub label: String,
    }

    schema_roots! {
        entities: [GeneratedOperationTestRecord],
    }
}

mod hidden_mutation_surface {
    use graphql_orm::prelude::*;

    #[derive(
        GraphQLEntity, GraphQLOperations, serde::Serialize, serde::Deserialize, Clone, Debug,
    )]
    #[graphql_entity(
        table = "hidden_generated_operation_test_records",
        plural = "HiddenGeneratedOperationTestRecords"
    )]
    pub struct HiddenGeneratedOperationTestRecord {
        #[primary_key]
        pub id: String,
        #[sortable]
        pub label: String,
    }

    schema_roots! {
        generated_mutations: "none",
        entities: [HiddenGeneratedOperationTestRecord],
    }
}

struct AdmitTestApplication;

impl AiGeneratedGraphqlOperationPolicy for AdmitTestApplication {
    fn is_application_operation(&self, operation: &GraphqlResolverOperationDescriptor) -> bool {
        operation.table_name() == "generated_operation_test_records"
    }
}

fn disclosure_schema(root_field: &str) -> AiDisclosureSchema {
    let rule = AiDisclosureRule::exportable(DataClassification::Internal);
    AiDisclosureSchema::new(
        "generated-records-v1",
        AiDisclosureShape::object(
            rule,
            [(
                root_field.to_owned(),
                AiDisclosureShape::object(
                    rule,
                    [(
                        "edges".to_owned(),
                        AiDisclosureShape::list(
                            rule,
                            10,
                            AiDisclosureShape::object(
                                rule,
                                [(
                                    "node".to_owned(),
                                    AiDisclosureShape::object(
                                        rule,
                                        [("id".to_owned(), AiDisclosureShape::scalar(rule))],
                                    ),
                                )],
                            ),
                        ),
                    )],
                ),
            )],
        ),
    )
    .expect("generated disclosure schema should validate")
}

fn list_operation(catalog: &GraphqlOperationCatalog) -> &GraphqlResolverOperationDescriptor {
    catalog
        .exposed_operations()
        .find(|operation| {
            operation.category() == GeneratedGraphqlOperationCategory::List
                && operation.table_name() == "generated_operation_test_records"
        })
        .expect("generated list resolver should be exposed")
}

fn descriptor(document: &str, contract: GraphqlOperationContract) -> AiToolDescriptor {
    AiToolDescriptor::new(
        "generated.records.list",
        "List visible generated records",
        AiToolOperationKind::Query,
        document,
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {},
            "additionalProperties": false
        }),
    )
    .expect("generated descriptor should validate")
    .with_result_projection("generated-records-projection-v1")
    .with_graphql_contract(contract)
}

#[test]
fn custom_contract_wire_shape_remains_backward_compatible() {
    let document = "query ReviewedCustomRoot { reviewedCustomRoot { id } }";
    let contract = GraphqlOperationContract::new(
        GraphqlExecutionTargetId::parse("local-application").expect("target ID"),
        "complete-host-schema-v1",
        "ReviewedCustomRoot",
        document,
        "custom-root-projection-v1",
        "custom-root-disclosure-v1",
    )
    .expect("custom operation contract should validate");
    let mut encoded = serde_json::to_value(&contract).expect("custom contract should serialize");
    assert!(encoded.get("generated_operation").is_none());

    encoded
        .as_object_mut()
        .expect("contract should encode as an object")
        .remove("generated_operation");
    let decoded: GraphqlOperationContract =
        serde_json::from_value(encoded).expect("legacy custom contract should decode");
    assert_eq!(decoded, contract);
}

#[test]
fn generated_registration_binds_exposure_document_and_host_classification() {
    let catalog = application_surface::graphql_orm_operation_catalog();
    let operation = list_operation(catalog);
    let document = format!(
        "query ReviewedGeneratedRecords {{ {} {{ edges {{ node {{ id }} }} }} }}",
        operation.field_name()
    );
    let disclosure = disclosure_schema(operation.field_name());
    let contract = GraphqlOperationContract::new(
        GraphqlExecutionTargetId::parse("local-application").expect("target ID"),
        "complete-host-schema-v1",
        "ReviewedGeneratedRecords",
        &document,
        "generated-records-projection-v1",
        disclosure.fingerprint.clone(),
    )
    .expect("base operation contract should validate")
    .with_generated_operation(
        catalog,
        GraphqlOperationKind::Query,
        operation.field_name(),
        &document,
    )
    .expect("generated operation should bind");

    let binding = contract
        .generated_operation()
        .expect("generated binding should be present");
    assert_eq!(binding.catalog_fingerprint(), catalog.fingerprint());
    assert_eq!(binding.operation_fingerprint(), operation.fingerprint());
    assert_eq!(binding.field_name(), operation.field_name());

    let tool = descriptor(&document, contract);
    let mut tool_catalog = AiToolCatalog::new();
    tool_catalog
        .register_generated_with_disclosure(
            tool.clone(),
            disclosure.clone(),
            catalog,
            &AdmitTestApplication,
        )
        .expect("exact generated application operation should register");
    assert!(tool_catalog.descriptor(&tool.id).is_some());

    let mut legacy_catalog = AiToolCatalog::new();
    assert!(matches!(
        legacy_catalog.register_with_disclosure(tool, disclosure),
        Err(AiError::InvalidConfiguration(_))
    ));
}

#[test]
fn generated_registration_rejects_policy_fingerprint_and_document_drift() {
    let catalog = application_surface::graphql_orm_operation_catalog();
    let operation = list_operation(catalog);
    let document = format!(
        "query ReviewedGeneratedRecords {{ {} {{ edges {{ node {{ id }} }} }} }}",
        operation.field_name()
    );
    let disclosure = disclosure_schema(operation.field_name());
    let contract = GraphqlOperationContract::new(
        GraphqlExecutionTargetId::parse("local-application").expect("target ID"),
        "complete-host-schema-v1",
        "ReviewedGeneratedRecords",
        &document,
        "generated-records-projection-v1",
        disclosure.fingerprint.clone(),
    )
    .expect("base operation contract should validate")
    .with_generated_operation(
        catalog,
        GraphqlOperationKind::Query,
        operation.field_name(),
        &document,
    )
    .expect("generated operation should bind");

    let mut denied_catalog = AiToolCatalog::new();
    assert!(matches!(
        denied_catalog.register_generated_with_disclosure(
            descriptor(&document, contract.clone()),
            disclosure.clone(),
            catalog,
            &DenyAllAiGeneratedGraphqlOperationPolicy,
        ),
        Err(AiError::InvalidConfiguration(_))
    ));

    let mut encoded = serde_json::to_value(&contract).expect("generated contract should serialize");
    encoded["generated_operation"]["operation_fingerprint"] =
        serde_json::Value::String("0".repeat(64));
    let stale: GraphqlOperationContract =
        serde_json::from_value(encoded).expect("tampered contract shape remains decodable");
    let mut stale_catalog = AiToolCatalog::new();
    assert!(matches!(
        stale_catalog.register_generated_with_disclosure(
            descriptor(&document, stale),
            disclosure,
            catalog,
            &AdmitTestApplication,
        ),
        Err(AiError::InvalidConfiguration(_))
    ));

    let extra_root = format!(
        "query ReviewedGeneratedRecords {{ {} {{ edges {{ node {{ id }} }} }} __typename }}",
        operation.field_name()
    );
    let extra_contract = GraphqlOperationContract::new(
        GraphqlExecutionTargetId::parse("local-application").expect("target ID"),
        "complete-host-schema-v1",
        "ReviewedGeneratedRecords",
        &extra_root,
        "generated-records-projection-v1",
        "disclosure-v1",
    )
    .expect("base operation contract should validate");
    assert!(matches!(
        extra_contract.with_generated_operation(
            catalog,
            GraphqlOperationKind::Query,
            operation.field_name(),
            &extra_root,
        ),
        Err(ToolExecutionError::StaleContract)
    ));
}

#[test]
fn hidden_generated_mutation_cannot_be_bound() {
    use hidden_mutation_surface::HiddenGeneratedOperationTestRecord;

    let catalog = hidden_mutation_surface::graphql_orm_operation_catalog();
    let generated_create = HiddenGeneratedOperationTestRecord::generated_graphql_operations()
        .iter()
        .find(|operation| operation.category() == GeneratedGraphqlOperationCategory::Create)
        .expect("create resolver should be generated for diagnostics");
    assert!(
        catalog
            .resolve(
                GraphqlOperationKind::Mutation,
                generated_create.field_name()
            )
            .is_none()
    );
    let document = format!(
        "mutation ReviewedHiddenCreate {{ {}(input: {{ id: \"1\", label: \"hidden\" }}) {{ id }} }}",
        generated_create.field_name()
    );
    let contract = GraphqlOperationContract::new(
        GraphqlExecutionTargetId::parse("local-application").expect("target ID"),
        "complete-host-schema-v1",
        "ReviewedHiddenCreate",
        &document,
        "hidden-create-projection-v1",
        "hidden-create-disclosure-v1",
    )
    .expect("base operation contract should validate");
    assert!(matches!(
        contract.with_generated_operation(
            catalog,
            GraphqlOperationKind::Mutation,
            generated_create.field_name(),
            &document,
        ),
        Err(ToolExecutionError::StaleContract)
    ));
}
