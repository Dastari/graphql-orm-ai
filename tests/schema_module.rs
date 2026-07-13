use graphql_orm::graphql::orm::SchemaModuleCatalog;
use graphql_orm_ai::{AI_SCHEMA_MODULE_VERSION, AI_TABLE_NAMESPACE, AiSchemaModule};

#[test]
fn ai_schema_module_owns_only_reserved_namespace_tables() {
    let module = AiSchemaModule;
    let catalog = SchemaModuleCatalog::compose(&[&module]).expect("AI module should validate");

    assert_eq!(catalog.modules().len(), 1);
    assert_eq!(catalog.modules()[0].version, AI_SCHEMA_MODULE_VERSION);
    assert_eq!(catalog.entities().len(), 37);
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
                    | "graphql_orm_ai_audit_events"
                    | "graphql_orm_ai_egress_events"
            ))
            .all(|entity| entity.append_only)
    );
    assert_eq!(catalog.modules()[0].restore_hooks.len(), 4);

    let schema = catalog.schema_model();
    let counter = schema
        .tables
        .iter()
        .find(|table| table.table_name == "graphql_orm_ai_budget_counters")
        .expect("budget counter table should exist");
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
}
