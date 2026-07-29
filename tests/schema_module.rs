use graphql_orm::graphql::orm::{ColumnBackupPolicy, SchemaModuleCatalog};
use graphql_orm_ai::{AI_SCHEMA_MODULE_VERSION, AI_TABLE_NAMESPACE, AiSchemaModule};

#[test]
fn ai_schema_module_owns_only_reserved_namespace_tables() {
    let module = AiSchemaModule;
    let catalog = SchemaModuleCatalog::compose(&[&module]).expect("AI module should validate");

    assert_eq!(catalog.modules().len(), 1);
    assert_eq!(catalog.modules()[0].version, AI_SCHEMA_MODULE_VERSION);
    assert_eq!(AI_SCHEMA_MODULE_VERSION, "0.51.0");
    assert_eq!(catalog.entities().len(), 40);
    assert!(
        catalog
            .entities()
            .iter()
            .all(|entity| entity.table_name.starts_with(AI_TABLE_NAMESPACE))
    );
    assert!(
        catalog
            .entities()
            .iter()
            .filter(|entity| matches!(
                entity.table_name,
                "graphql_orm_ai_run_attempts"
                    | "graphql_orm_ai_run_attempt_outcomes"
                    | "graphql_orm_ai_run_checkpoints"
                    | "graphql_orm_ai_skill_versions"
                    | "graphql_orm_ai_usage_entries"
                    | "graphql_orm_ai_pricing_policies"
                    | "graphql_orm_ai_audit_events"
                    | "graphql_orm_ai_egress_events"
            ))
            .all(|entity| entity.append_only)
    );
    let retention_entities = catalog
        .entities()
        .iter()
        .filter(|entity| entity.retention_policy.is_some())
        .collect::<Vec<_>>();
    assert_eq!(retention_entities.len(), 1);
    assert_eq!(
        retention_entities[0].table_name,
        "graphql_orm_ai_run_checkpoints"
    );
    assert_eq!(
        retention_entities[0].retention_policy,
        Some("graphql_orm_ai.run_checkpoint.retention_purge")
    );
    assert_eq!(catalog.modules()[0].restore_hooks.len(), 4);

    let schema = catalog.schema_model();
    let counter = schema
        .tables
        .iter()
        .find(|table| table.table_name == "graphql_orm_ai_budget_counters")
        .expect("budget counter table should exist");
    let budget_policy = schema
        .tables
        .iter()
        .find(|table| table.table_name == "graphql_orm_ai_budget_policies")
        .expect("budget policy table should exist");
    assert!(
        budget_policy
            .columns
            .iter()
            .any(|column| { column.name == "scope_key" && !column.nullable })
    );
    let pricing_policy = schema
        .tables
        .iter()
        .find(|table| table.table_name == "graphql_orm_ai_pricing_policies")
        .expect("pricing policy table should exist");
    assert!(pricing_policy.append_only);
    assert!(pricing_policy.columns.iter().any(|column| {
        column.name == "version_reference" && column.is_unique && !column.nullable
    }));
    for expected in ["scope_key", "provider_kind", "provider_model"] {
        assert!(
            pricing_policy
                .indexes
                .iter()
                .any(|index| { index.columns == [expected.to_owned()] && !index.is_unique })
        );
    }
    assert!(
        budget_policy
            .indexes
            .iter()
            .any(|index| { index.columns == ["scope_key"] && !index.is_unique })
    );
    assert!(
        counter.composite_unique_indexes.iter().any(|columns| {
            columns == &["budget_policy_id".to_owned(), "period_key".to_owned()]
        })
    );
    let reservation = schema
        .tables
        .iter()
        .find(|table| table.table_name == "graphql_orm_ai_budget_reservations")
        .expect("budget reservation table should exist");
    assert!(reservation.composite_unique_indexes.iter().any(|columns| {
        columns
            == &[
                "principal_kind".to_owned(),
                "principal_subject".to_owned(),
                "idempotency_key".to_owned(),
            ]
    }));
    assert!(
        reservation
            .columns
            .iter()
            .any(|column| { column.name == "actual_cached_input_tokens" && column.nullable })
    );
    let inbox_stream = schema
        .tables
        .iter()
        .find(|table| table.table_name == "graphql_orm_ai_inbox_streams")
        .expect("principal inbox stream table should exist");
    assert!(inbox_stream.composite_unique_indexes.iter().any(|columns| {
        columns == &["principal_kind".to_owned(), "principal_subject".to_owned()]
    }));
    let inbox_event = schema
        .tables
        .iter()
        .find(|table| table.table_name == "graphql_orm_ai_inbox_events")
        .expect("principal inbox event table should exist");
    assert!(inbox_event.composite_unique_indexes.iter().any(|columns| {
        columns
            == &[
                "principal_kind".to_owned(),
                "principal_subject".to_owned(),
                "sequence".to_owned(),
            ]
    }));
    let message = schema
        .tables
        .iter()
        .find(|table| table.table_name == "graphql_orm_ai_messages")
        .expect("message table should exist");
    assert!(
        message
            .columns
            .iter()
            .any(|column| { column.name == "protected_preview" && column.nullable })
    );
    assert!(
        message
            .columns
            .iter()
            .any(|column| { column.name == "content_purged_at" && column.nullable })
    );
    assert!(
        message
            .columns
            .iter()
            .any(|column| { column.name == "row_version" && !column.nullable })
    );
    let attachment = schema
        .tables
        .iter()
        .find(|table| table.table_name == "graphql_orm_ai_attachments")
        .expect("attachment table should exist");
    assert!(
        attachment
            .indexes
            .iter()
            .any(|index| { index.columns == ["message_id"] && !index.is_unique })
    );
    let attachment_metadata = catalog
        .entities()
        .iter()
        .find(|entity| entity.table_name == "graphql_orm_ai_attachments")
        .expect("attachment metadata should exist");
    assert_eq!(
        attachment_metadata
            .fields
            .iter()
            .find(|field| field.name == "blob_reference")
            .expect("attachment blob reference should exist")
            .backup_policy,
        ColumnBackupPolicy::Include
    );
    for redacted in ["quarantine_blob_reference", "upload_token_hash"] {
        assert_eq!(
            attachment_metadata
                .fields
                .iter()
                .find(|field| field.name == redacted)
                .expect("redacted attachment field should exist")
                .backup_policy,
            ColumnBackupPolicy::Redact
        );
    }
    let artifact_metadata = catalog
        .entities()
        .iter()
        .find(|entity| entity.table_name == "graphql_orm_ai_attachment_artifacts")
        .expect("attachment artifact metadata should exist");
    assert_eq!(
        artifact_metadata
            .fields
            .iter()
            .find(|field| field.name == "blob_reference")
            .expect("artifact blob reference should exist")
            .backup_policy,
        ColumnBackupPolicy::Include
    );
    assert_eq!(
        artifact_metadata
            .fields
            .iter()
            .find(|field| field.name == "provider_reference")
            .expect("artifact provider reference should exist")
            .backup_policy,
        ColumnBackupPolicy::Redact
    );
    assert!(
        inbox_event
            .columns
            .iter()
            .any(|column| column.name == "scope_key" && !column.nullable)
    );
    let retention = schema
        .tables
        .iter()
        .find(|table| table.table_name == "graphql_orm_ai_retention_policies")
        .expect("retention policy table should exist");
    assert!(
        retention
            .columns
            .iter()
            .any(|column| { column.name == "scope_key" && column.nullable && !column.is_unique })
    );
    let usage = schema
        .tables
        .iter()
        .find(|table| table.table_name == "graphql_orm_ai_usage_entries")
        .expect("usage table should exist");
    assert!(usage.columns.iter().any(|column| {
        column.name == "budget_reservation_id" && column.is_unique && !column.nullable
    }));
    assert!(
        usage
            .columns
            .iter()
            .any(|column| column.name == "principal_kind" && !column.nullable)
    );
    let webhook_receipt = schema
        .tables
        .iter()
        .find(|table| table.table_name == "graphql_orm_ai_provider_webhook_receipts")
        .expect("provider webhook receipt table should exist");
    assert_eq!(webhook_receipt.primary_keys(), ["id", "provider_kind"]);
    assert!(webhook_receipt.indexes.iter().any(|index| {
        index.columns
            == [
                "provider_kind".to_owned(),
                "provider_profile_id".to_owned(),
                "provider_response_id".to_owned(),
                "state".to_owned(),
            ]
            && !index.is_unique
    }));
    for required_column in [
        "receipt_key",
        "provider_profile_id",
        "provider_event_kind",
        "provider_created_at",
    ] {
        assert!(
            webhook_receipt
                .columns
                .iter()
                .any(|column| { column.name == required_column && !column.nullable })
        );
    }
    let background_submission = schema
        .tables
        .iter()
        .find(|table| table.table_name == "graphql_orm_ai_provider_background_submissions")
        .expect("provider background submission table should exist");
    for required_column in [
        "reconciliation_owner",
        "reconciliation_generation",
        "reconciliation_lease_expires_at",
        "reconciliation_next_attempt_at",
        "reconciliation_retry_count",
        "reconciliation_deadline",
        "reconciled_at",
        "retrieval_egress_decision_id",
        "terminal_message_id",
    ] {
        assert!(
            background_submission
                .columns
                .iter()
                .any(|column| column.name == required_column)
        );
    }
    for indexed_column in [
        "reconciliation_lease_expires_at",
        "reconciliation_next_attempt_at",
        "reconciliation_deadline",
    ] {
        assert!(
            background_submission
                .indexes
                .iter()
                .any(|index| index.columns == [indexed_column] && !index.is_unique)
        );
    }
    for indexed_column in [
        "scope_kind",
        "scope_id",
        "tenant_id",
        "principal_kind",
        "principal_subject",
        "provider_kind",
        "provider_model",
        "created_at",
    ] {
        assert!(
            usage
                .indexes
                .iter()
                .any(|index| { index.columns == [indexed_column] && !index.is_unique })
        );
    }
    assert!(
        retention
            .indexes
            .iter()
            .any(|index| { index.is_unique && index.columns == ["scope_key"] })
    );
    assert!(
        retention
            .columns
            .iter()
            .any(|column| { column.name == "inbox_event_retention_seconds" && column.nullable })
    );
    let attempt_outcome = schema
        .tables
        .iter()
        .find(|table| table.table_name == "graphql_orm_ai_run_attempt_outcomes")
        .expect("run attempt outcome table should exist");
    assert!(
        attempt_outcome
            .columns
            .iter()
            .any(|column| column.name == "attempt_id" && column.is_unique)
    );
    let run = schema
        .tables
        .iter()
        .find(|table| table.table_name == "graphql_orm_ai_runs")
        .expect("run table should exist");
    assert!(
        run.columns
            .iter()
            .any(|column| column.name == "latest_checkpoint_id" && column.nullable)
    );
    let checkpoint = schema
        .tables
        .iter()
        .find(|table| table.table_name == "graphql_orm_ai_run_checkpoints")
        .expect("run checkpoint table should exist");
    assert!(checkpoint.retention_purge);
    assert!(
        checkpoint
            .columns
            .iter()
            .any(|column| column.name == "protected_state" && column.nullable)
    );
}
