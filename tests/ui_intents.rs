use graphql_orm_ai::{
    AiError, AiUiIntentCatalog, AiUiIntentDraft, AiUiIntentTypeDescriptor, AiUiIntentTypeId,
};
use serde_json::json;

fn ui_descriptor() -> AiUiIntentTypeDescriptor {
    AiUiIntentTypeDescriptor::new(
        "generic.open_resource",
        "1",
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "resourceKind": { "type": "string", "minLength": 1 },
                "resourceId": { "type": "string", "minLength": 1 }
            },
            "required": ["resourceKind", "resourceId"],
            "additionalProperties": false
        }),
    )
    .expect("logical intent descriptor should validate")
    .with_display_metadata(json!({"label": "Open resource"}))
    .expect("safe display metadata should validate")
    .with_maximum_payload_bytes(1_024)
    .expect("bounded payload should validate")
}

#[test]
fn exact_binding_and_schema_are_required_for_ui_intent_suggestions() {
    let descriptor = ui_descriptor();
    let binding = descriptor.binding();
    let intent_type = binding.intent_type.clone();
    let mut catalog = AiUiIntentCatalog::new();
    catalog
        .register(descriptor)
        .expect("valid descriptor should register once");

    let validated = catalog
        .validate_bound(
            &binding,
            AiUiIntentDraft {
                intent_type: intent_type.clone(),
                payload: json!({
                    "resourceKind": "record",
                    "resourceId": "54"
                }),
            },
        )
        .expect("exact bounded payload should validate as a suggestion");
    assert_eq!(validated.binding, binding);

    let mut stale_binding = binding.clone();
    stale_binding.descriptor_fingerprint = "0".repeat(64);
    assert!(matches!(
        catalog.validate_bound(
            &stale_binding,
            AiUiIntentDraft {
                intent_type: intent_type.clone(),
                payload: json!({"resourceKind": "record", "resourceId": "54"}),
            },
        ),
        Err(AiError::Conflict)
    ));
    assert!(matches!(
        catalog.validate_bound(
            &binding,
            AiUiIntentDraft {
                intent_type,
                payload: json!({"resourceKind": "record", "route": "/unsafe"}),
            },
        ),
        Err(AiError::InvalidInput(_))
    ));
}

#[test]
fn registry_is_default_deny_and_rejects_tampered_or_duplicate_descriptors() {
    let descriptor = ui_descriptor();
    let binding = descriptor.binding();
    let empty = AiUiIntentCatalog::new();
    assert!(matches!(
        empty.validate_bound(
            &binding,
            AiUiIntentDraft {
                intent_type: binding.intent_type.clone(),
                payload: json!({"resourceKind": "record", "resourceId": "54"}),
            },
        ),
        Err(AiError::NotFound)
    ));

    let mut catalog = AiUiIntentCatalog::new();
    catalog
        .register(descriptor.clone())
        .expect("first descriptor should register");
    assert!(matches!(
        catalog.register(descriptor),
        Err(AiError::AlreadyExists(_))
    ));

    let mut tampered = ui_descriptor();
    tampered.schema = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object"
    });
    let mut other_catalog = AiUiIntentCatalog::new();
    assert!(matches!(
        other_catalog.register(tampered),
        Err(AiError::InvalidConfiguration(_))
    ));

    assert!(AiUiIntentTypeId::parse("navigate:/resource").is_err());
}
