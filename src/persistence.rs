//! Schema-only AI persistence metadata.
//!
//! These entities participate in migrations and backups through
//! [`AiSchemaModule`] without adding generated CRUD fields to the host schema.
//! Generated repository/input helpers are crate-internal even when the derive
//! layer emits them with public visibility.

#![allow(missing_docs)]

use std::sync::OnceLock;

use graphql_orm::graphql::orm::{
    Entity, EntityMetadata, OrmSchemaModule, SchemaModuleDescriptor, SchemaModuleRestoreHook,
    SchemaModuleRestorePhase,
};
use graphql_orm::prelude::*;

/// Per-scope runtime and maturity policy.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_scope_policies",
    plural = "GraphqlOrmAiScopePolicies",
    default_sort = "updated_at DESC"
)]
pub(crate) struct AiScopePolicyRecord {
    /// Policy ID.
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Scope kind.
    pub scope_kind: String,
    /// Scope ID.
    pub scope_id: String,
    /// Optional tenant.
    pub tenant_id: Option<String>,
    /// AI enabled state.
    pub enabled: bool,
    /// Maximum tool maturity enum value.
    pub maximum_tool_maturity: String,
    /// Serialized bounded capability configuration.
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub capabilities: serde_json::Value,
    /// CAS version.
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
    /// Update timestamp (Unix seconds).
    #[sortable]
    pub updated_at: i64,
}

/// Scoped provider endpoint and credential-reference configuration.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_provider_profiles",
    plural = "GraphqlOrmAiProviderProfiles",
    default_sort = "display_name ASC"
)]
pub(crate) struct AiProviderProfileRecord {
    #[primary_key]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Canonical non-secret hash of kind, ID, and tenant boundary.
    #[filterable(type = "string")]
    pub scope_key: String,
    #[filterable(type = "string")]
    pub scope_kind: String,
    #[filterable(type = "string")]
    pub scope_id: String,
    pub tenant_id: Option<String>,
    #[filterable(type = "string")]
    pub provider_kind: String,
    pub display_name: String,
    /// Empty for providers with a deployment-fixed endpoint.
    pub base_url: Option<String>,
    /// Non-secret reference only; credential plaintext never enters this row.
    #[backup(redact)]
    pub credential_reference: Option<String>,
    pub enabled: bool,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub data_policy: serde_json::Value,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub limits: serde_json::Value,
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
    #[sortable]
    pub updated_at: i64,
}

/// Task-to-model route and bounded fallbacks.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_model_routes",
    plural = "GraphqlOrmAiModelRoutes",
    default_sort = "priority ASC"
)]
pub(crate) struct AiModelRouteRecord {
    #[primary_key]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    pub scope_kind: String,
    pub scope_id: String,
    pub tenant_id: Option<String>,
    #[filterable(type = "string")]
    pub task_kind: String,
    #[sortable]
    pub priority: i64,
    pub provider_profile_id: graphql_orm::uuid::Uuid,
    pub model: String,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub fallback_route_ids: serde_json::Value,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub model_parameters: serde_json::Value,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub required_capabilities: serde_json::Value,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub budget: serde_json::Value,
    pub enabled: bool,
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
    pub updated_at: i64,
}

/// Required per-scope content-protection choice and migration readiness.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_content_protection_policies",
    plural = "GraphqlOrmAiContentProtectionPolicies",
    default_sort = "effective_at DESC"
)]
pub(crate) struct AiContentProtectionPolicyRecord {
    #[primary_key]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Canonical non-secret hash of kind, ID, and tenant boundary.
    #[unique]
    #[filterable(type = "string")]
    pub scope_key: String,
    #[filterable(type = "string")]
    pub scope_kind: String,
    #[filterable(type = "string")]
    pub scope_id: String,
    pub tenant_id: Option<String>,
    pub protection_mode: String,
    pub key_policy_reference: Option<String>,
    pub key_version: Option<String>,
    pub migration_state: String,
    pub ready: bool,
    #[sortable]
    pub effective_at: i64,
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Scope-controlled egress restrictions intersected with deployment limits.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_egress_policies",
    plural = "GraphqlOrmAiEgressPolicies",
    default_sort = "updated_at DESC"
)]
pub(crate) struct AiEgressPolicyRecord {
    #[primary_key]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    pub scope_kind: String,
    pub scope_id: String,
    pub tenant_id: Option<String>,
    pub enabled: bool,
    pub maximum_classification: String,
    pub consent_rule: String,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub allowed_destinations: serde_json::Value,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub allowed_capabilities: serde_json::Value,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub residency_retention_limits: serde_json::Value,
    pub policy_version: String,
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
    #[sortable]
    pub updated_at: i64,
}

/// Revocable purpose-bound egress consent containing no transferred content.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_egress_consents",
    plural = "GraphqlOrmAiEgressConsents",
    default_sort = "granted_at DESC"
)]
pub(crate) struct AiEgressConsentRecord {
    #[primary_key]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    #[filterable(type = "string")]
    pub principal_subject: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub tenant_id: Option<String>,
    pub destination: String,
    pub capability: String,
    pub purpose: String,
    pub purpose_grant_reference: String,
    pub manifest_constraints_hash: String,
    pub assurance: String,
    #[sortable]
    pub granted_at: i64,
    pub expires_at: i64,
    pub revoked_at: Option<i64>,
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Default-deny tool exposure policy bound to an exact descriptor fingerprint.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_tool_policies",
    plural = "GraphqlOrmAiToolPolicies",
    default_sort = "updated_at DESC"
)]
pub(crate) struct AiToolPolicyRecord {
    #[primary_key]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    pub scope_kind: String,
    pub scope_id: String,
    pub tenant_id: Option<String>,
    #[filterable(type = "string")]
    pub tool_id: String,
    pub tool_fingerprint: String,
    pub enabled: bool,
    pub maximum_maturity: String,
    pub risk_override: Option<String>,
    pub approval_rule: String,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub constraints: serde_json::Value,
    pub maximum_calls: i64,
    pub maximum_output_bytes: i64,
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
    #[sortable]
    pub updated_at: i64,
}

/// Retention and purge behavior for one scope.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_retention_policies",
    plural = "GraphqlOrmAiRetentionPolicies",
    default_sort = "updated_at DESC",
    unique_index = "scope_key"
)]
pub(crate) struct AiRetentionPolicyRecord {
    #[primary_key]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Stable scope identity. Legacy rows must be explicitly migrated before
    /// they become effective.
    #[filterable(type = "string")]
    pub scope_key: Option<String>,
    pub scope_kind: String,
    pub scope_id: String,
    pub tenant_id: Option<String>,
    pub message_retention_seconds: Option<i64>,
    pub delta_retention_seconds: i64,
    pub raw_payload_retention_seconds: i64,
    pub audit_retention_seconds: i64,
    pub deleted_content_purge_seconds: i64,
    pub provider_file_delete_required: bool,
    /// Cross-session inbox event age, absent on legacy/unconfigured rows.
    pub inbox_event_retention_seconds: Option<i64>,
    /// Recent inbox events retained regardless of age.
    pub inbox_minimum_events: Option<i64>,
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
    #[sortable]
    pub updated_at: i64,
}

/// Scope/user budget counters and hard limits.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_budget_policies",
    plural = "GraphqlOrmAiBudgetPolicies",
    default_sort = "updated_at DESC"
)]
pub(crate) struct AiBudgetPolicyRecord {
    #[primary_key]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Stable non-secret exact scope identity.
    #[filterable(type = "string")]
    pub scope_key: String,
    #[filterable(type = "string")]
    pub scope_kind: String,
    #[filterable(type = "string")]
    pub scope_id: String,
    pub tenant_id: Option<String>,
    pub principal_kind: Option<String>,
    pub principal_subject: Option<String>,
    pub interval_kind: String,
    pub maximum_input_tokens: Option<i64>,
    pub maximum_output_tokens: Option<i64>,
    pub maximum_tool_units: Option<i64>,
    pub maximum_image_units: Option<i64>,
    pub maximum_cost_microunits: Option<i64>,
    pub maximum_runs: Option<i64>,
    #[filterable(type = "boolean")]
    pub enabled: bool,
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
    #[sortable]
    pub updated_at: i64,
}

/// Immutable provider/model token pricing version.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_pricing_policies",
    plural = "GraphqlOrmAiPricingPolicies",
    default_sort = "created_at DESC",
    append_only = true,
    keyset = "created_at desc, id desc"
)]
pub(crate) struct AiPricingPolicyRecord {
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Globally unique immutable pricing reference stored on reservations.
    #[unique]
    #[filterable(type = "string")]
    pub version_reference: String,
    #[filterable(type = "string")]
    pub scope_key: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub tenant_id: Option<String>,
    #[filterable(type = "string")]
    pub provider_kind: String,
    #[filterable(type = "string")]
    pub provider_model: String,
    pub fixed_call_microunits: i64,
    pub input_microunits_per_million: i64,
    pub cached_input_microunits_per_million: i64,
    pub output_microunits_per_million: i64,
    #[graphql_orm(default = "0")]
    pub web_search_microunits_per_call: i64,
    #[graphql_orm(default = "0")]
    pub file_search_microunits_per_call: i64,
    pub created_by_principal_kind: String,
    pub created_by_subject: String,
    #[filterable(type = "number")]
    #[sortable]
    pub created_at: i64,
}

/// Atomically maintained budget usage for one policy/time window.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_budget_counters",
    plural = "GraphqlOrmAiBudgetCounters",
    default_sort = "period_started_at DESC",
    unique_composite = "budget_policy_id, period_key",
    upsert = "budget_policy_id, period_key"
)]
pub(crate) struct AiBudgetCounterRecord {
    #[primary_key]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    #[filterable(type = "uuid")]
    pub budget_policy_id: graphql_orm::uuid::Uuid,
    pub policy_version: i64,
    #[filterable(type = "string")]
    pub period_key: String,
    pub period_started_at: i64,
    pub period_ends_at: i64,
    pub reserved_input_tokens: i64,
    pub reserved_output_tokens: i64,
    pub reserved_tool_units: i64,
    pub reserved_image_units: i64,
    pub reserved_cost_microunits: i64,
    pub reserved_runs: i64,
    pub committed_input_tokens: i64,
    pub committed_output_tokens: i64,
    pub committed_tool_units: i64,
    pub committed_image_units: i64,
    pub committed_cost_microunits: i64,
    pub committed_runs: i64,
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
    #[sortable]
    pub updated_at: i64,
}

/// Exact provider-call capacity held across all applicable budget counters.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_budget_reservations",
    plural = "GraphqlOrmAiBudgetReservations",
    default_sort = "created_at DESC",
    unique_composite = "principal_kind, principal_subject, idempotency_key"
)]
pub(crate) struct AiBudgetReservationRecord {
    #[primary_key]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub budget_counter_ids: serde_json::Value,
    pub scope_kind: String,
    pub scope_id: String,
    pub tenant_id: Option<String>,
    #[filterable(type = "string")]
    pub principal_kind: String,
    #[filterable(type = "string")]
    pub principal_subject: String,
    pub session_id: graphql_orm::uuid::Uuid,
    #[filterable(type = "uuid")]
    pub run_id: graphql_orm::uuid::Uuid,
    pub attempt_id: graphql_orm::uuid::Uuid,
    pub lease_generation: i64,
    pub provider_kind: String,
    pub provider_model: String,
    pub pricing_policy_version: String,
    pub reserved_input_tokens: i64,
    pub reserved_output_tokens: i64,
    pub reserved_tool_units: i64,
    pub reserved_image_units: i64,
    pub reserved_cost_microunits: i64,
    pub reserved_runs: i64,
    pub actual_input_tokens: Option<i64>,
    pub actual_cached_input_tokens: Option<i64>,
    pub actual_output_tokens: Option<i64>,
    pub actual_tool_units: Option<i64>,
    pub actual_image_units: Option<i64>,
    pub actual_cost_microunits: Option<i64>,
    pub actual_runs: Option<i64>,
    #[filterable(type = "string")]
    pub idempotency_key: String,
    #[filterable(type = "string")]
    pub state: String,
    pub expires_at: i64,
    #[sortable]
    pub created_at: i64,
    pub reconciled_at: Option<i64>,
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Per-user conversational session metadata.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_sessions",
    plural = "GraphqlOrmAiSessions",
    default_sort = "last_activity_at DESC",
    keyset = "last_activity_at desc, id desc"
)]
pub(crate) struct AiSessionRecord {
    /// Session ID.
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Owning principal class (`user` or a host-defined API-token kind).
    #[filterable(type = "string")]
    pub owner_principal_kind: String,
    /// Owning principal subject.
    #[filterable(type = "string")]
    pub owner_subject: String,
    /// Optional tenant.
    pub tenant_id: Option<String>,
    /// Scope kind.
    pub scope_kind: String,
    /// Scope ID.
    pub scope_id: String,
    /// User-visible title.
    pub title: String,
    /// Lifecycle state.
    #[filterable(type = "string")]
    pub state: String,
    /// Current durable stream head.
    pub stream_head: i64,
    /// Current durable message head.
    pub message_head: i64,
    /// Last activity timestamp.
    #[sortable]
    pub last_activity_at: i64,
    /// Archive timestamp.
    pub archived_at: Option<i64>,
    /// Deletion timestamp.
    pub deleted_at: Option<i64>,
    /// CAS version.
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Session ownership/participation record.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_session_participants",
    plural = "GraphqlOrmAiSessionParticipants",
    default_sort = "created_at ASC"
)]
pub(crate) struct AiSessionParticipantRecord {
    /// Participant record ID.
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Session ID.
    #[filterable(type = "uuid")]
    pub session_id: graphql_orm::uuid::Uuid,
    /// Principal class.
    #[filterable(type = "string")]
    pub principal_kind: String,
    /// Principal subject.
    #[filterable(type = "string")]
    pub principal_subject: String,
    /// Owner/editor/viewer role.
    pub participant_role: String,
    /// Created timestamp.
    #[sortable]
    pub created_at: i64,
    /// CAS version.
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Durable per-session event row used as the subscription source of truth.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_session_events",
    plural = "GraphqlOrmAiSessionEvents",
    default_sort = "sequence ASC",
    keyset = "sequence asc, id asc"
)]
pub(crate) struct AiSessionEventRecord {
    /// Event ID.
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Session ID.
    #[filterable(type = "uuid")]
    pub session_id: graphql_orm::uuid::Uuid,
    /// Per-session monotonic sequence.
    #[filterable(type = "number")]
    #[sortable]
    pub sequence: i64,
    /// Stable event type.
    #[filterable(type = "string")]
    pub event_type: String,
    /// Optional run.
    pub run_id: Option<graphql_orm::uuid::Uuid>,
    /// Causation reference.
    pub causation_id: Option<String>,
    /// Correlation reference.
    pub correlation_id: String,
    /// Protected/ciphertext payload envelope.
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub protected_payload: serde_json::Value,
    /// Created timestamp.
    pub created_at: i64,
}

/// Durable per-principal cross-session notification stream head.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_inbox_streams",
    plural = "GraphqlOrmAiInboxStreams",
    default_sort = "last_event_at ASC",
    unique_composite = "principal_kind, principal_subject"
)]
pub(crate) struct AiInboxStreamRecord {
    /// Deterministic principal-stream identifier.
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Principal class stream owner.
    #[filterable(type = "string")]
    pub principal_kind: String,
    /// Principal subject stream owner.
    #[filterable(type = "string")]
    pub principal_subject: String,
    /// Highest sequence ever allocated; pruning never rewinds this value.
    pub stream_head: i64,
    /// Lowest sequence that may still be replayed.
    pub minimum_retained_sequence: i64,
    /// Timestamp of the latest appended event.
    #[sortable]
    pub last_event_at: i64,
    /// CAS version serializing append and prune operations.
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Durable per-principal cross-session notification event.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_inbox_events",
    plural = "GraphqlOrmAiInboxEvents",
    default_sort = "sequence ASC",
    keyset = "sequence asc, id asc",
    unique_composite = "principal_kind, principal_subject, sequence"
)]
pub(crate) struct AiInboxEventRecord {
    /// Event ID.
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Principal class stream owner.
    #[filterable(type = "string")]
    pub principal_kind: String,
    /// Principal subject stream owner.
    #[filterable(type = "string")]
    pub principal_subject: String,
    /// Stable scope-policy lookup key captured with the event.
    #[filterable(type = "string")]
    pub scope_key: String,
    /// Scope kind captured with the event.
    pub scope_kind: String,
    /// Scope ID captured with the event.
    pub scope_id: String,
    /// Optional tenant boundary captured with the event.
    pub tenant_id: Option<String>,
    /// Per-principal monotonic sequence.
    #[filterable(type = "number")]
    #[sortable]
    pub sequence: i64,
    /// Optional session.
    #[filterable(type = "uuid")]
    pub session_id: Option<graphql_orm::uuid::Uuid>,
    /// Stable event type.
    pub event_type: String,
    /// Protected/ciphertext payload envelope until session deletion retention.
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub protected_payload: Option<serde_json::Value>,
    /// Trusted timestamp at which session deletion retention removed the
    /// protected payload while preserving stream sequence continuity.
    #[filterable(type = "number")]
    pub payload_purged_at: Option<i64>,
    /// Created timestamp.
    #[filterable(type = "number")]
    pub created_at: i64,
    /// CAS version.
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Bounded message metadata; large content lives in block rows.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_messages",
    plural = "GraphqlOrmAiMessages",
    default_sort = "sequence ASC",
    keyset = "sequence asc, id asc",
    unique_index = "session_id, client_message_id"
)]
pub(crate) struct AiMessageRecord {
    /// Message ID.
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Session ID.
    #[filterable(type = "uuid")]
    pub session_id: graphql_orm::uuid::Uuid,
    /// Stable per-session message order.
    #[filterable(type = "number")]
    #[sortable]
    pub sequence: i64,
    /// User/assistant/tool/system role.
    #[filterable(type = "string")]
    pub message_role: String,
    /// Safe author principal class.
    pub author_principal_kind: Option<String>,
    /// Safe author subject.
    pub author_subject: Option<String>,
    /// Client idempotency reference for user messages.
    #[filterable(type = "uuid")]
    pub client_message_id: Option<graphql_orm::uuid::Uuid>,
    /// Hash binding the idempotency reference to text and attachment IDs.
    pub content_hash: Option<String>,
    /// Producing run.
    pub run_id: Option<graphql_orm::uuid::Uuid>,
    /// Provider kind/model metadata.
    pub provider_kind: Option<String>,
    /// Provider model metadata.
    pub provider_model: Option<String>,
    /// Protected bounded preview envelope.
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub protected_preview: Option<serde_json::Value>,
    /// Number of separately windowed content blocks.
    pub block_count: i64,
    /// Completion state.
    pub completion_state: String,
    /// Created timestamp.
    pub created_at: i64,
    /// Finalized timestamp.
    pub finalized_at: Option<i64>,
    /// Timestamp at which protected preview and block content were removed by
    /// the trusted retention worker.
    #[filterable(type = "number")]
    pub content_purged_at: Option<i64>,
    /// CAS version used to serialize retention with any future message
    /// metadata maintenance.
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Windowable content block capped by runtime policy.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_message_blocks",
    plural = "GraphqlOrmAiMessageBlocks",
    default_sort = "block_index ASC",
    keyset = "block_index asc, id asc"
)]
pub(crate) struct AiMessageBlockRecord {
    /// Block ID.
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Parent message.
    #[filterable(type = "uuid")]
    pub message_id: graphql_orm::uuid::Uuid,
    /// Stable block order.
    #[filterable(type = "number")]
    #[sortable]
    pub block_index: i64,
    /// Text/json/tool/citation block kind.
    pub block_kind: String,
    /// Protected/ciphertext content envelope.
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub protected_content: serde_json::Value,
    /// Original uncompressed byte count.
    pub byte_count: i64,
    /// Original line count.
    pub line_count: i64,
    /// Created timestamp.
    pub created_at: i64,
}

/// Quarantined/final attachment metadata; blob keys remain opaque.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_attachments",
    plural = "GraphqlOrmAiAttachments",
    default_sort = "created_at DESC",
    keyset = "created_at desc, id desc"
)]
pub(crate) struct AiAttachmentRecord {
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    #[filterable(type = "string")]
    pub owner_principal_kind: String,
    #[filterable(type = "string")]
    pub owner_subject: String,
    #[filterable(type = "uuid")]
    pub session_id: graphql_orm::uuid::Uuid,
    #[filterable(type = "uuid")]
    pub message_id: Option<graphql_orm::uuid::Uuid>,
    /// Storage-provider opaque reference, never a user-controlled path.
    #[backup(redact)]
    pub blob_reference: Option<String>,
    /// Pending quarantine object reference, never exposed to clients/models.
    #[backup(redact)]
    pub quarantine_blob_reference: Option<String>,
    pub safe_filename: String,
    pub declared_mime: Option<String>,
    pub detected_mime: Option<String>,
    pub expected_byte_count: Option<i64>,
    pub byte_count: Option<i64>,
    pub sha256: Option<String>,
    /// SHA-256 of the one-time upload capability; never the capability itself.
    #[backup(redact)]
    pub upload_token_hash: Option<String>,
    pub upload_expires_at: Option<i64>,
    #[filterable(type = "string")]
    pub quarantine_state: String,
    pub scan_state: String,
    #[filterable(type = "string")]
    pub processing_state: String,
    /// Deadline after which an interrupted upload/scanner may be reclaimed.
    pub processing_expires_at: Option<i64>,
    /// Monotonic maintenance claim generation.
    pub cleanup_generation: Option<i64>,
    /// Deadline after which another cleanup worker may reclaim the row.
    pub cleanup_lease_expires_at: Option<i64>,
    /// Failed cleanup attempts used for bounded retry backoff.
    pub cleanup_retry_count: Option<i64>,
    /// Earliest retry time after an ambiguous storage operation.
    pub cleanup_next_attempt_at: Option<i64>,
    pub scanner_version: Option<String>,
    pub acceptance_policy_version: Option<String>,
    pub rejection_code: Option<String>,
    #[sortable]
    pub created_at: i64,
    pub finalized_at: Option<i64>,
    #[filterable(type = "number")]
    pub deleted_at: Option<i64>,
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Derived OCR, thumbnail, transcript, extracted text, or provider-file data.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_attachment_artifacts",
    plural = "GraphqlOrmAiAttachmentArtifacts",
    default_sort = "created_at ASC, id ASC",
    keyset = "created_at asc, id asc"
)]
pub(crate) struct AiAttachmentArtifactRecord {
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    #[filterable(type = "uuid")]
    pub attachment_id: graphql_orm::uuid::Uuid,
    pub artifact_kind: String,
    #[backup(redact)]
    pub blob_reference: Option<String>,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub protected_content: Option<serde_json::Value>,
    pub detected_mime: Option<String>,
    pub byte_count: i64,
    pub sha256: Option<String>,
    /// Exact provider family owning `provider_reference`, when present.
    pub provider_kind: Option<String>,
    /// Exact logical provider profile owning `provider_reference`.
    pub provider_profile_id: Option<String>,
    #[backup(redact)]
    pub provider_reference: Option<String>,
    pub provider_expires_at: Option<i64>,
    /// Private retention state; absent means the artifact remains active.
    #[filterable(type = "string")]
    pub cleanup_state: Option<String>,
    /// Monotonic maintenance claim generation.
    pub cleanup_generation: Option<i64>,
    /// Deadline after which another artifact worker may reclaim the row.
    pub cleanup_lease_expires_at: Option<i64>,
    /// Failed exact-reference cleanup attempts used for bounded backoff.
    pub cleanup_retry_count: Option<i64>,
    /// Earliest retry time after ambiguous local or provider deletion.
    pub cleanup_next_attempt_at: Option<i64>,
    #[sortable]
    pub created_at: i64,
    pub deleted_at: Option<i64>,
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Durable agent run and current fenced lease state.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_runs",
    plural = "GraphqlOrmAiRuns",
    default_sort = "created_at ASC"
)]
pub(crate) struct AiRunRecord {
    /// Run ID.
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Session ID.
    #[filterable(type = "uuid")]
    pub session_id: graphql_orm::uuid::Uuid,
    /// User message that initiated this run.
    #[filterable(type = "uuid")]
    pub input_message_id: graphql_orm::uuid::Uuid,
    /// Safe principal reference; never bearer credentials.
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub principal_reference: serde_json::Value,
    /// Durable run state.
    #[filterable(type = "string")]
    pub state: String,
    /// Current attempt ID.
    pub attempt_id: Option<graphql_orm::uuid::Uuid>,
    /// Current lease owner.
    pub lease_owner: Option<String>,
    /// Monotonic lease generation/fencing token.
    pub lease_generation: i64,
    /// Lease expiry timestamp.
    pub lease_expires_at: Option<i64>,
    /// Last heartbeat timestamp.
    pub lease_heartbeat_at: Option<i64>,
    /// Retry count.
    pub retry_count: i64,
    /// Next eligible attempt timestamp.
    pub next_attempt_at: Option<i64>,
    /// Safe error code.
    pub error_code: Option<String>,
    /// Latest exact coordinator checkpoint for recovery classification.
    pub latest_checkpoint_id: Option<graphql_orm::uuid::Uuid>,
    /// Created timestamp.
    #[sortable]
    pub created_at: i64,
    /// CAS version.
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Immutable run-attempt/fence history.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_run_attempts",
    plural = "GraphqlOrmAiRunAttempts",
    default_sort = "claimed_at ASC",
    append_only = true
)]
pub(crate) struct AiRunAttemptRecord {
    /// Attempt ID.
    #[primary_key]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Run ID.
    #[filterable(type = "uuid")]
    pub run_id: graphql_orm::uuid::Uuid,
    /// Fencing generation.
    pub lease_generation: i64,
    /// Worker owner.
    pub worker_id: String,
    /// Claim time.
    #[sortable]
    pub claimed_at: i64,
    /// Finish time.
    pub finished_at: Option<i64>,
    /// Provider response reference.
    pub provider_response_id: Option<String>,
    /// Safe recovery classification/outcome.
    pub outcome_code: Option<String>,
}

/// Immutable terminal/retry/recovery fact for one run attempt.
///
/// Attempt claims and their outcomes are separate append-only rows so worker
/// history never depends on mutating an already-recorded fence claim.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_run_attempt_outcomes",
    plural = "GraphqlOrmAiRunAttemptOutcomes",
    default_sort = "finished_at ASC",
    append_only = true
)]
pub(crate) struct AiRunAttemptOutcomeRecord {
    /// Outcome fact ID.
    #[primary_key]
    pub id: graphql_orm::uuid::Uuid,
    /// Exact claim/attempt receiving this outcome. At most one outcome exists.
    #[unique]
    #[filterable(type = "uuid")]
    pub attempt_id: graphql_orm::uuid::Uuid,
    /// Owning run.
    #[filterable(type = "uuid")]
    pub run_id: graphql_orm::uuid::Uuid,
    /// Exact fencing generation of the claim.
    pub lease_generation: i64,
    /// Worker that owned the claim.
    pub worker_id: String,
    /// Durable run state reached by this attempt.
    #[filterable(type = "string")]
    pub final_state: String,
    /// Safe machine-readable outcome classification.
    pub outcome_code: String,
    /// Optional provider response reference; never response content.
    pub provider_response_id: Option<String>,
    /// Completion/recovery timestamp.
    #[sortable]
    pub finished_at: i64,
}

/// Ordered run step.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_run_steps",
    plural = "GraphqlOrmAiRunSteps",
    default_sort = "step_index ASC"
)]
pub(crate) struct AiRunStepRecord {
    /// Step ID.
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Run ID.
    #[filterable(type = "uuid")]
    pub run_id: graphql_orm::uuid::Uuid,
    /// Stable step order.
    #[filterable(type = "number")]
    #[sortable]
    pub step_index: i64,
    /// Provider/tool/approval/context step kind.
    pub step_kind: String,
    /// Durable state.
    pub state: String,
    /// Attempt/fencing generation that owns the result.
    pub lease_generation: i64,
    /// Start timestamp.
    pub started_at: Option<i64>,
    /// Finish timestamp.
    pub finished_at: Option<i64>,
    /// Safe error code.
    pub error_code: Option<String>,
    /// CAS version.
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Immutable fenced coordinator checkpoint containing redacted recovery
/// metadata and, when present, a protected exact resumable payload.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    graphql_orm(projection(
        name = "AiRunCheckpointRetentionProjection",
        fields = [
            id,
            run_id,
            attempt_id,
            lease_generation,
            checkpoint_kind,
            provider_response_id,
            budget_reservation_id,
            assistant_message_id,
            checkpoint_hash,
            created_at
        ],
        private = true
    ))
)]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    graphql_entity(
        table = "graphql_orm_ai_run_checkpoints",
        plural = "GraphqlOrmAiRunCheckpoints",
        default_sort = "created_at ASC, id ASC",
        append_only = true,
        retention_purge = "graphql_orm_ai.run_checkpoint.retention_purge"
    )
)]
#[cfg_attr(
    feature = "mssql",
    graphql_entity(
        table = "graphql_orm_ai_run_checkpoints",
        plural = "GraphqlOrmAiRunCheckpoints",
        default_sort = "created_at ASC, id ASC",
        append_only = true
    )
)]
pub(crate) struct AiRunCheckpointRecord {
    /// Checkpoint ID, assigned by the owning fenced transaction.
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    #[sortable]
    pub id: graphql_orm::uuid::Uuid,
    /// Owning run.
    #[filterable(type = "uuid")]
    pub run_id: graphql_orm::uuid::Uuid,
    /// Exact attempt that committed the checkpoint.
    #[filterable(type = "uuid")]
    pub attempt_id: graphql_orm::uuid::Uuid,
    /// Exact fencing generation that committed the checkpoint.
    pub lease_generation: i64,
    /// Stable redacted phase value.
    #[filterable(type = "string")]
    pub checkpoint_kind: String,
    /// Safe provider response reference, never response content.
    pub provider_response_id: Option<String>,
    /// Atomic budget/usage correlation for the settled provider turn.
    pub budget_reservation_id: Option<graphql_orm::uuid::Uuid>,
    /// Durable assistant message proving final protected output persistence.
    pub assistant_message_id: Option<graphql_orm::uuid::Uuid>,
    /// Protected provider-turn or completed-tool-batch state. This field is
    /// never exposed through generated reads and is absent for final-output
    /// checkpoints whose message/block rows are the durable proof.
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub protected_state: Option<serde_json::Value>,
    /// Stable hash over every recovery-relevant redacted field.
    pub checkpoint_hash: String,
    /// Checkpoint commit time.
    #[sortable]
    pub created_at: i64,
}

/// Model-requested tool invocation and protected result.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_tool_calls",
    plural = "GraphqlOrmAiToolCalls",
    default_sort = "created_at ASC"
)]
pub(crate) struct AiToolCallRecord {
    /// Tool-call ID.
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Run ID.
    #[filterable(type = "uuid")]
    pub run_id: graphql_orm::uuid::Uuid,
    /// Unique hash of run ID and opaque provider call ID.
    #[unique]
    pub provider_call_key: String,
    /// Opaque provider call ID required for exact continuation binding.
    pub provider_call_id: String,
    /// Exact provider family that emitted the call.
    pub provider_kind: Option<String>,
    /// Exact provider model that emitted the call.
    pub provider_model: Option<String>,
    /// Provider response containing the call, when emitted.
    pub provider_response_id: Option<String>,
    /// Settled provider-turn budget/usage correlation.
    pub budget_reservation_id: Option<graphql_orm::uuid::Uuid>,
    /// Zero-based provider turn within the bounded agent loop.
    pub provider_turn_index: i64,
    /// Zero-based call order within the provider turn.
    pub tool_call_index: i64,
    /// Stable tool ID.
    pub tool_id: String,
    /// Exact descriptor fingerprint.
    pub tool_fingerprint: String,
    /// Protected arguments.
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub protected_arguments: Option<serde_json::Value>,
    /// Canonical argument hash used for approvals/idempotency.
    pub argument_hash: String,
    /// Protected result.
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub protected_result: Option<serde_json::Value>,
    /// Timestamp when retention removed protected content.
    pub payload_purged_at: Option<i64>,
    /// Risk class.
    pub risk: String,
    /// Authorization decision code.
    pub authorization_code: Option<String>,
    /// Current host tool-policy version used at execution.
    pub authorization_policy_version: Option<String>,
    /// Safe current authorization-state digest.
    pub authorization_state_digest: Option<String>,
    /// Exact static result-disclosure schema fingerprint.
    pub disclosure_schema_fingerprint: Option<String>,
    /// Highest static/runtime-tightened result classification.
    pub result_classification: Option<String>,
    /// Immutable egress decision authorizing the result's model disclosure.
    pub result_egress_decision_id: Option<graphql_orm::uuid::Uuid>,
    /// Exact redacted manifest hash bound to the egress decision.
    pub result_egress_manifest_hash: Option<String>,
    /// Safe ordinary application audit correlation, never provider-facing.
    pub application_audit_ref: Option<String>,
    /// Approval ID.
    pub approval_id: Option<graphql_orm::uuid::Uuid>,
    /// Stable idempotency key when supported.
    pub idempotency_key: Option<String>,
    /// Original server-owned correlation reference.
    pub correlation_id: Option<String>,
    /// Original server-owned causation reference.
    pub causation_id: Option<String>,
    /// Safe delegation/grant reference, never a credential.
    pub delegation_reference: Option<String>,
    /// Attempt/fencing generation that owns the result.
    pub lease_generation: i64,
    /// Durable state.
    pub state: String,
    /// Created timestamp.
    #[sortable]
    pub created_at: i64,
    /// Completed timestamp.
    pub completed_at: Option<i64>,
    /// CAS version.
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Expiring, argument-bound tool approval.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_approvals",
    plural = "GraphqlOrmAiApprovals",
    default_sort = "created_at ASC",
    keyset = "created_at asc, id asc"
)]
pub(crate) struct AiApprovalRecord {
    /// Approval ID.
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Tool call.
    #[filterable(type = "uuid")]
    pub tool_call_id: graphql_orm::uuid::Uuid,
    /// Session.
    #[filterable(type = "uuid")]
    pub session_id: graphql_orm::uuid::Uuid,
    /// Principal subject requesting/approving.
    pub principal_subject: String,
    /// Fingerprint of the safe durable principal reference.
    pub principal_reference_fingerprint: String,
    /// Original/delegated actor subject.
    pub delegated_actor_subject: Option<String>,
    /// Safe delegation/grant reference, never a credential.
    pub delegation_reference: Option<String>,
    /// Bound canonical argument hash.
    pub argument_hash: String,
    /// Bound tool fingerprint.
    pub tool_fingerprint: String,
    /// Complete action-envelope hash.
    pub binding_hash: String,
    /// Logical local/remote execution target.
    pub execution_target_id: String,
    /// Exact target schema fingerprint.
    pub target_schema_fingerprint: String,
    /// Exact server-authored operation name.
    pub operation_name: String,
    /// Exact server-authored operation-document hash.
    pub operation_document_hash: String,
    /// Exact result-projection fingerprint.
    pub result_projection_fingerprint: String,
    /// Exact static disclosure-schema fingerprint.
    pub disclosure_schema_fingerprint: String,
    /// Current tool/scope/application policy version.
    pub policy_version: String,
    /// Safe authorization-state/precondition digest.
    pub authorization_state_digest: String,
    /// Protected exact resource/version bindings.
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub protected_resource_bindings: Option<serde_json::Value>,
    /// Protected server-generated canonical action preview.
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub protected_action_preview: Option<serde_json::Value>,
    /// Timestamp when retention removed protected content.
    pub payload_purged_at: Option<i64>,
    /// Canonical action-preview hash.
    pub action_preview_hash: String,
    /// Pending/approved/resume-claimed/denied/expired/revoked/consumed state.
    #[filterable(type = "string")]
    pub state: String,
    /// Recent-MFA requirement.
    pub recent_mfa_required: bool,
    /// Approver subject.
    pub approver_subject: Option<String>,
    /// Created timestamp.
    #[sortable]
    pub created_at: i64,
    /// Expiry timestamp.
    pub expires_at: i64,
    /// Decision timestamp.
    pub decided_at: Option<i64>,
    /// Maximum atomic consumption count; one for one-shot approvals.
    pub maximum_uses: i64,
    /// Current atomic consumption count.
    pub consumed_uses: i64,
    /// One-shot consumption timestamp.
    pub consumed_at: Option<i64>,
    /// CAS version.
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// AI-owned structured suggestion envelope.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_proposals",
    plural = "GraphqlOrmAiProposals",
    default_sort = "created_at DESC",
    keyset = "created_at desc, id desc"
)]
pub(crate) struct AiProposalRecord {
    /// Proposal ID.
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Session ID.
    #[filterable(type = "uuid")]
    pub session_id: graphql_orm::uuid::Uuid,
    /// Run ID.
    #[filterable(type = "uuid")]
    pub run_id: graphql_orm::uuid::Uuid,
    /// Scope kind.
    pub scope_kind: String,
    /// Scope ID.
    pub scope_id: String,
    /// Registered proposal type.
    pub proposal_type: String,
    /// Registered schema version.
    pub schema_version: String,
    /// Validated logical review-item count.
    pub item_count: i64,
    /// Protected/ciphertext structured payload envelope.
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub protected_payload: Option<serde_json::Value>,
    /// Redacted source references.
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub source_references: Option<serde_json::Value>,
    /// Timestamp when deleting-session retention removed protected content.
    pub payload_purged_at: Option<i64>,
    /// Lifecycle state.
    #[filterable(type = "string")]
    pub state: String,
    /// Model/user creator subject.
    pub created_by_subject: String,
    /// Human reviewer subject.
    pub reviewed_by_subject: Option<String>,
    /// Application resource reference after a normal mutation commits.
    pub applied_resource_ref: Option<String>,
    /// Authoritative application audit reference.
    pub application_audit_ref: Option<String>,
    /// Created timestamp.
    #[sortable]
    pub created_at: i64,
    /// Reviewed timestamp.
    pub reviewed_at: Option<i64>,
    /// Expiry timestamp.
    pub expires_at: Option<i64>,
    /// CAS version.
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Optional bounded proposal item for per-field human review.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_proposal_items",
    plural = "GraphqlOrmAiProposalItems",
    default_sort = "item_index ASC",
    keyset = "item_index asc, id asc"
)]
pub(crate) struct AiProposalItemRecord {
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    #[filterable(type = "uuid")]
    pub proposal_id: graphql_orm::uuid::Uuid,
    #[filterable(type = "number")]
    #[sortable]
    pub item_index: i64,
    pub stable_path: String,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub protected_suggested_value: Option<serde_json::Value>,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub protected_rationale: Option<serde_json::Value>,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub source_references: Option<serde_json::Value>,
    pub review_decision: Option<String>,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub protected_review_value: Option<serde_json::Value>,
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Protected compacted context through a stable session sequence.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_context_checkpoints",
    plural = "GraphqlOrmAiContextCheckpoints",
    default_sort = "through_sequence DESC",
    keyset = "through_sequence desc, id desc"
)]
pub(crate) struct AiContextCheckpointRecord {
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    #[filterable(type = "uuid")]
    pub session_id: graphql_orm::uuid::Uuid,
    #[sortable]
    pub through_sequence: i64,
    pub source_hash: String,
    pub token_estimate: i64,
    pub provider_kind: String,
    pub provider_model: String,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub protected_summary: serde_json::Value,
    pub invalidated_at: Option<i64>,
    pub created_at: i64,
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Scoped skill identity. Skill instructions live in immutable versions.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_skills",
    plural = "GraphqlOrmAiSkills",
    default_sort = "name ASC"
)]
pub(crate) struct AiSkillRecord {
    #[primary_key]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    #[filterable(type = "string")]
    pub scope_kind: String,
    #[filterable(type = "string")]
    pub scope_id: String,
    #[filterable(type = "string")]
    pub tenant_id: Option<String>,
    #[filterable(type = "string")]
    #[sortable]
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub current_version_id: Option<graphql_orm::uuid::Uuid>,
    pub created_by_subject: String,
    pub created_at: i64,
    pub updated_at: i64,
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Immutable skill instructions, tool fingerprints, policy, and provenance.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_skill_versions",
    plural = "GraphqlOrmAiSkillVersions",
    default_sort = "created_at DESC",
    append_only = true
)]
pub(crate) struct AiSkillVersionRecord {
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    #[filterable(type = "uuid")]
    pub skill_id: graphql_orm::uuid::Uuid,
    pub version: String,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub protected_instructions: serde_json::Value,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub allowed_tools: serde_json::Value,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub data_policy: serde_json::Value,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub activation_rule: serde_json::Value,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub schemas: serde_json::Value,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub budgets: serde_json::Value,
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub provenance: serde_json::Value,
    pub checksum: String,
    pub published: bool,
    pub author_subject: String,
    #[sortable]
    pub created_at: i64,
}

/// Append-oriented provider/model usage and cost fact.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_usage_entries",
    plural = "GraphqlOrmAiUsageEntries",
    default_sort = "created_at DESC",
    append_only = true,
    keyset = "created_at desc, id desc"
)]
pub(crate) struct AiUsageEntryRecord {
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    #[unique]
    #[filterable(type = "uuid")]
    pub budget_reservation_id: graphql_orm::uuid::Uuid,
    #[filterable(type = "string")]
    pub scope_kind: String,
    #[filterable(type = "string")]
    pub scope_id: String,
    #[filterable(type = "string")]
    pub tenant_id: Option<String>,
    #[filterable(type = "string")]
    pub principal_kind: String,
    #[filterable(type = "string")]
    pub principal_subject: String,
    pub session_id: Option<graphql_orm::uuid::Uuid>,
    #[filterable(type = "uuid")]
    pub run_id: Option<graphql_orm::uuid::Uuid>,
    #[filterable(type = "string")]
    pub provider_kind: String,
    #[filterable(type = "string")]
    pub provider_model: String,
    pub input_tokens: i64,
    pub cached_input_tokens: i64,
    pub output_tokens: i64,
    pub tool_units: i64,
    pub image_units: i64,
    pub cost_microunits: Option<i64>,
    #[filterable(type = "number")]
    #[sortable]
    pub created_at: i64,
}

/// Content-free binding for one fenced provider background submission.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_provider_background_submissions",
    plural = "GraphqlOrmAiProviderBackgroundSubmissions",
    default_sort = "created_at ASC"
)]
pub(crate) struct AiProviderBackgroundSubmissionRecord {
    /// Opaque deterministic submission identity embedded in provider metadata.
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    #[sortable]
    pub id: graphql_orm::uuid::Uuid,
    /// Full SHA-256 collision check for `id`.
    #[unique]
    pub submission_key: String,
    /// Exact owning session.
    #[filterable(type = "uuid")]
    pub session_id: graphql_orm::uuid::Uuid,
    /// Exact owning run.
    #[filterable(type = "uuid")]
    pub run_id: graphql_orm::uuid::Uuid,
    /// Original fenced attempt. One attempt may submit at most one response.
    #[unique]
    #[filterable(type = "uuid")]
    pub attempt_id: graphql_orm::uuid::Uuid,
    /// Original lease generation.
    pub lease_generation: i64,
    /// Exact provider family.
    pub provider_kind: String,
    /// Exact logical provider profile.
    pub provider_profile_id: String,
    /// Exact requested model/routing key.
    pub provider_model: String,
    /// Exact provider-enforced output-token ceiling.
    pub maximum_output_tokens: i64,
    /// Whether an acknowledgement reports durable response storage.
    pub provider_store: Option<bool>,
    /// SHA-256 of the canonical provider-neutral model request; never content.
    pub request_hash: String,
    /// Atomic budget reservation left uncertain until terminal reconciliation.
    #[unique]
    pub budget_reservation_id: graphql_orm::uuid::Uuid,
    /// Durable allow decision for the exact model-inference manifest.
    pub egress_decision_id: graphql_orm::uuid::Uuid,
    /// Exact redacted model-inference manifest hash.
    pub egress_manifest_hash: String,
    /// Provider response reference returned by a successful create call.
    #[unique]
    pub provider_response_id: Option<String>,
    /// Bounded provider status observed in the create acknowledgement.
    pub provider_status: Option<String>,
    /// Durable local lifecycle state.
    #[filterable(type = "string")]
    pub state: String,
    /// Safe redacted failure/recovery code, when classified later.
    pub safe_error_code: Option<String>,
    /// Preparation time before external I/O.
    #[sortable]
    pub created_at: i64,
    /// Provider response creation time from the acknowledgement.
    pub provider_created_at: Option<i64>,
    /// Local time when the acknowledgement was fenced into the wait state.
    pub submitted_at: Option<i64>,
    /// Current reconciliation owner. It grants no provider or run authority.
    pub reconciliation_owner: Option<String>,
    /// Monotonic reconciliation claim generation.
    #[graphql_orm(default = "0")]
    pub reconciliation_generation: i64,
    /// Deadline after which another reconciler may reclaim the submission.
    #[filterable(type = "number")]
    pub reconciliation_lease_expires_at: Option<i64>,
    /// Earliest time at which this row may be claimed or reclaimed.
    #[filterable(type = "number")]
    #[sortable]
    pub reconciliation_next_attempt_at: Option<i64>,
    /// Bounded read-only retrieval retry count.
    #[graphql_orm(default = "0")]
    pub reconciliation_retry_count: i64,
    /// Fixed provider-retention deadline captured when acknowledgement is bound.
    #[filterable(type = "number")]
    #[sortable]
    pub reconciliation_deadline: Option<i64>,
    /// Local time when the complete terminal graph committed.
    pub reconciled_at: Option<i64>,
    /// Current allow decision authorizing exact response retrieval.
    pub retrieval_egress_decision_id: Option<graphql_orm::uuid::Uuid>,
    /// Successful terminal assistant message and output checkpoint.
    pub terminal_message_id: Option<graphql_orm::uuid::Uuid>,
    /// CAS version.
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Idempotent receipt for a provider background/webhook event.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_provider_webhook_receipts",
    plural = "GraphqlOrmAiProviderWebhookReceipts",
    default_sort = "received_at DESC",
    repository_mutations = true
)]
pub(crate) struct AiProviderWebhookReceiptRecord {
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    pub receipt_key: String,
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "string")]
    pub provider_kind: String,
    pub provider_profile_id: String,
    #[filterable(type = "string")]
    pub provider_event_id: String,
    pub provider_event_kind: String,
    pub provider_created_at: i64,
    pub provider_response_id: Option<String>,
    pub run_id: Option<graphql_orm::uuid::Uuid>,
    pub attempt_id: Option<graphql_orm::uuid::Uuid>,
    pub signature_verified: bool,
    pub state: String,
    pub safe_error_code: Option<String>,
    #[sortable]
    pub received_at: i64,
    pub processed_at: Option<i64>,
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Immutable redacted action/audit fact containing no prompts or arguments.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_audit_events",
    plural = "GraphqlOrmAiAuditEvents",
    default_sort = "created_at DESC",
    append_only = true,
    keyset = "created_at desc, id desc"
)]
pub(crate) struct AiAuditEventRecord {
    #[primary_key]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    pub actor_principal_kind: String,
    pub actor_subject: String,
    pub action: String,
    pub resource_kind: String,
    pub resource_reference: String,
    pub outcome: String,
    pub reason_code: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub policy_version: Option<String>,
    #[sortable]
    pub created_at: i64,
}

/// Durable cleanup command for an obsolete or compensating secret reference.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_secret_cleanup",
    plural = "GraphqlOrmAiSecretCleanup",
    default_sort = "created_at ASC"
)]
pub(crate) struct AiSecretCleanupRecord {
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Opaque reference only. It is redacted from backups and never exposed.
    #[backup(redact)]
    pub secret_reference: String,
    pub reason_code: String,
    #[filterable(type = "string")]
    pub state: String,
    pub retry_count: i64,
    pub next_attempt_at: Option<i64>,
    pub completed_at: Option<i64>,
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
    #[sortable]
    pub created_at: i64,
}

/// Redacted immutable external-transfer decision.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_egress_events",
    plural = "GraphqlOrmAiEgressEvents",
    default_sort = "created_at ASC",
    append_only = true
)]
pub(crate) struct AiEgressEventRecord {
    /// Decision ID.
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Optional run.
    #[filterable(type = "uuid")]
    pub run_id: Option<graphql_orm::uuid::Uuid>,
    /// Principal subject.
    pub principal_subject: String,
    /// Scope kind.
    pub scope_kind: String,
    /// Scope ID.
    pub scope_id: String,
    /// Exact redacted manifest hash.
    pub manifest_hash: String,
    /// Provider/destination class.
    pub destination: String,
    /// Capability.
    pub capability: String,
    /// Maximum classification.
    pub classification: String,
    /// Allow/deny outcome.
    pub outcome: String,
    /// Stable reason code.
    pub reason_code: String,
    /// Applied policy version.
    pub policy_version: String,
    /// Estimated bytes.
    pub estimated_bytes: i64,
    /// Estimated tokens.
    pub estimated_tokens: i64,
    /// Created timestamp.
    #[sortable]
    pub created_at: i64,
}

/// Restore/recovery epoch and runtime start gate.
#[cfg_attr(feature = "mssql", derive(GraphQLSchemaEntity))]
#[cfg_attr(
    any(feature = "sqlite", feature = "postgres"),
    derive(GraphQLEntity, GraphQLOperations)
)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[graphql_entity(
    table = "graphql_orm_ai_runtime_recovery",
    plural = "GraphqlOrmAiRuntimeRecovery",
    default_sort = "created_at DESC"
)]
pub(crate) struct AiRuntimeRecoveryRecord {
    /// Recovery epoch ID.
    #[primary_key]
    #[filterable(type = "uuid")]
    pub id: graphql_orm::uuid::Uuid,
    /// Schema module version.
    pub module_version: String,
    /// Schema module fingerprint.
    pub module_fingerprint: String,
    /// Dry-run/applied state.
    pub state: String,
    /// Runtime start gate.
    pub start_gate_open: bool,
    /// Fatal issue count.
    pub fatal_issue_count: i64,
    /// Warning count.
    pub warning_count: i64,
    /// Redacted action counts.
    #[graphql_orm(json, read = false, filter = false, order = false, subscribe = false)]
    pub action_counts: serde_json::Value,
    /// Operator subject.
    pub operator_subject: Option<String>,
    /// Created timestamp.
    #[sortable]
    pub created_at: i64,
    /// Completed timestamp.
    pub completed_at: Option<i64>,
    /// CAS version.
    #[graphql_orm(version, default = "0")]
    pub row_version: i64,
}

/// Stable schema module ID.
pub const AI_SCHEMA_MODULE_ID: &str = "com.dastari.graphql-orm-ai";
/// Current AI schema module version.
pub const AI_SCHEMA_MODULE_VERSION: &str = "0.48.0";
/// Reserved table namespace.
pub const AI_TABLE_NAMESPACE: &str = "graphql_orm_ai_";

static AI_SCHEMA_DESCRIPTOR: SchemaModuleDescriptor = SchemaModuleDescriptor::new(
    AI_SCHEMA_MODULE_ID,
    AI_SCHEMA_MODULE_VERSION,
    AI_TABLE_NAMESPACE,
);

static AI_RESTORE_HOOKS: [SchemaModuleRestoreHook; 4] = [
    SchemaModuleRestoreHook {
        hook_id: "ai-restore-preflight",
        phase: SchemaModuleRestorePhase::Preflight,
    },
    SchemaModuleRestoreHook {
        hook_id: "ai-runtime-reconcile",
        phase: SchemaModuleRestorePhase::Reconcile,
    },
    SchemaModuleRestoreHook {
        hook_id: "ai-runtime-validate",
        phase: SchemaModuleRestorePhase::Validate,
    },
    SchemaModuleRestoreHook {
        hook_id: "ai-runtime-readiness",
        phase: SchemaModuleRestorePhase::Readiness,
    },
];

/// AI-owned migration/backup/restore module.
#[derive(Clone, Copy, Debug, Default)]
pub struct AiSchemaModule;

impl OrmSchemaModule for AiSchemaModule {
    fn descriptor(&self) -> &SchemaModuleDescriptor {
        &AI_SCHEMA_DESCRIPTOR
    }

    fn entities(&self) -> &[&'static EntityMetadata] {
        static ENTITIES: OnceLock<Vec<&'static EntityMetadata>> = OnceLock::new();
        ENTITIES.get_or_init(|| {
            vec![
                AiScopePolicyRecord::metadata(),
                AiProviderProfileRecord::metadata(),
                AiModelRouteRecord::metadata(),
                AiContentProtectionPolicyRecord::metadata(),
                AiEgressPolicyRecord::metadata(),
                AiEgressConsentRecord::metadata(),
                AiToolPolicyRecord::metadata(),
                AiRetentionPolicyRecord::metadata(),
                AiBudgetPolicyRecord::metadata(),
                AiPricingPolicyRecord::metadata(),
                AiBudgetCounterRecord::metadata(),
                AiBudgetReservationRecord::metadata(),
                AiSessionRecord::metadata(),
                AiSessionParticipantRecord::metadata(),
                AiSessionEventRecord::metadata(),
                AiInboxStreamRecord::metadata(),
                AiInboxEventRecord::metadata(),
                AiMessageRecord::metadata(),
                AiMessageBlockRecord::metadata(),
                AiAttachmentRecord::metadata(),
                AiAttachmentArtifactRecord::metadata(),
                AiRunRecord::metadata(),
                AiRunAttemptRecord::metadata(),
                AiRunAttemptOutcomeRecord::metadata(),
                AiRunStepRecord::metadata(),
                AiRunCheckpointRecord::metadata(),
                AiToolCallRecord::metadata(),
                AiApprovalRecord::metadata(),
                AiProposalRecord::metadata(),
                AiProposalItemRecord::metadata(),
                AiContextCheckpointRecord::metadata(),
                AiSkillRecord::metadata(),
                AiSkillVersionRecord::metadata(),
                AiUsageEntryRecord::metadata(),
                AiProviderBackgroundSubmissionRecord::metadata(),
                AiProviderWebhookReceiptRecord::metadata(),
                AiAuditEventRecord::metadata(),
                AiSecretCleanupRecord::metadata(),
                AiEgressEventRecord::metadata(),
                AiRuntimeRecoveryRecord::metadata(),
            ]
        })
    }

    fn restore_hooks(&self) -> &[SchemaModuleRestoreHook] {
        &AI_RESTORE_HOOKS
    }
}
