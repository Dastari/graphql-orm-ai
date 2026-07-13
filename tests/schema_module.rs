use graphql_orm::graphql::orm::SchemaModuleCatalog;
use graphql_orm_ai::{AI_TABLE_NAMESPACE, AiSchemaModule};

#[test]
fn ai_schema_module_owns_only_reserved_namespace_tables() {
    let module = AiSchemaModule;
    let catalog = SchemaModuleCatalog::compose(&[&module]).expect("AI module should validate");

    assert_eq!(catalog.modules().len(), 1);
    assert_eq!(catalog.entities().len(), 35);
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
                    | "graphql_orm_ai_skill_versions"
                    | "graphql_orm_ai_usage_entries"
                    | "graphql_orm_ai_audit_events"
                    | "graphql_orm_ai_egress_events"
            ))
            .all(|entity| entity.append_only)
    );
    assert_eq!(catalog.modules()[0].restore_hooks.len(), 4);
}
