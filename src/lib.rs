//! Project-agnostic AI agent runtime for `graphql-orm` applications.
//!
//! The crate is intentionally built around default-deny capabilities:
//! resolver metadata is discovery rather than authorization, application work
//! executes through the host's authenticated GraphQL context, and external
//! data egress requires a separate explicit decision.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

#[cfg(not(any(feature = "sqlite", feature = "postgres", feature = "mssql")))]
compile_error!("enable one graphql-orm-ai persistence backend");

#[cfg(any(
    all(feature = "sqlite", feature = "postgres"),
    all(feature = "sqlite", feature = "mssql"),
    all(feature = "postgres", feature = "mssql")
))]
compile_error!(
    "the initial graphql-orm-ai schema module requires exactly one backend; explicit multi-backend schema modules are planned"
);

mod access;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod agent_loop;
mod approvals;
mod attachments;
mod budget;
mod configuration;
mod content_protection;
mod data;
mod disclosure;
mod domain;
mod egress;
mod error;
mod execution;
mod inbox;
mod live_delta;
#[cfg(feature = "local-harness")]
mod local_harness;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_approvals;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_attachments;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_budget;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_checkpoints;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_configuration;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_coordinator;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_egress;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_inbox;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_live_delta;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_pricing;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_proposals;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_provider_output;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_rules;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_runs;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_session_retention;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_sessions;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_skills;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_subscriptions;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_tools;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_ui_intents;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_usage;
mod persistence;
mod pricing;
mod proposals;
mod provider;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod provider_calls;
mod providers;
mod remote_execution;
mod restore;
mod rules;
mod run_state;
mod runtime;
mod secrets;
mod session_retention;
mod sessions;
mod skills;
mod subscriptions;
mod tools;
mod ui_intents;
mod usage;

pub use access::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use agent_loop::*;
pub use approvals::*;
pub use attachments::*;
pub use budget::*;
pub use configuration::*;
pub use content_protection::*;
pub use data::*;
pub use disclosure::*;
pub use domain::*;
pub use egress::*;
pub use error::*;
pub use execution::*;
pub use inbox::*;
pub use live_delta::*;
#[cfg(feature = "local-harness")]
pub use local_harness::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_approvals::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_attachments::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_budget::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_checkpoints::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_configuration::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_coordinator::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_egress::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_inbox::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_live_delta::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_pricing::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_proposals::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_provider_output::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_rules::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_runs::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_session_retention::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_sessions::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_skills::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_subscriptions::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_tools::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_ui_intents::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_usage::*;
pub use persistence::{
    AI_SCHEMA_MODULE_ID, AI_SCHEMA_MODULE_VERSION, AI_TABLE_NAMESPACE, AiSchemaModule,
};
pub use pricing::*;
pub use proposals::*;
pub use provider::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use provider_calls::*;
pub use providers::*;
pub use remote_execution::*;
pub use restore::*;
pub use rules::*;
pub use run_state::*;
pub use runtime::*;
pub use secrets::*;
pub use session_retention::*;
pub use sessions::*;
pub use skills::*;
pub use subscriptions::*;
pub use tools::*;
pub use ui_intents::*;
pub use usage::*;

/// Common imports for host integrations.
pub mod prelude {
    pub use crate::ai_scope_key;
    pub use crate::{
        AiAccessPolicy, AiApprovalAccessPolicy, AiApprovalBinding, AiAttachmentAcceptancePolicy,
        AiAttachmentCleanupReport, AiAttachmentCleanupService, AiAttachmentScanner,
        AiAttachmentService, AiAttachmentUploadService, AiBudgetReservation, AiBudgetService,
        AiContentProtectionPolicy, AiDataSourceRef, AiDisclosureSchema, AiEgressDecision,
        AiEgressDecisionAudit, AiEgressManifest, AiEgressPolicy, AiError, AiInboxPruningService,
        AiInboxService, AiLiveDeltaCoalescerLimits, AiPricingCatalogService, AiPricingQuoteService,
        AiProposalAccessPolicy, AiProposalCatalog, AiProposalTypeDescriptor, AiProvider,
        AiProviderAttachmentRequest, AiProviderAttachmentResolver,
        AiRemoteAuthenticatedGraphqlAdapter, AiRemoteGraphqlAuthority,
        AiRemoteGraphqlAuthorityIssuer, AiRemoteGraphqlDelegationRequest,
        AiRemoteGraphqlExecutionLimits, AiRemoteGraphqlTransport, AiResolvedProviderAttachment,
        AiRuleAccessPolicy, AiRuleDeploymentLimits, AiRuleHierarchyResolver, AiRulePolicyService,
        AiRuntime, AiRuntimeBuilder, AiScope, AiSecretStore, AiSessionRetentionService,
        AiSkillAccessPolicy, AiSkillCatalogService, AiToolAuthorizationPolicy, AiToolCatalog,
        AiToolDescriptor, AiUiIntentCatalog, AiUiIntentTypeDescriptor, AiUsageAccessPolicy,
        AiUsageService, DataClassification, SecretRef, ToolMaturity,
    };
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub use crate::{
        AiAdoptedReadOnlyToolBatch, AiAgentCheckpointAdopter, AiAgentCheckpointWriter,
        AiApplicationToolCallLimits, AiApprovalServiceLimits, AiAttachmentCleanupLimits,
        AiAttachmentServiceLimits, AiBudgetServiceLimits, AiCanonicalActionPreviewBuilder,
        AiConsequentialToolCallOutcome, AiCoordinatorCheckpointLimits, AiInboxPruningLimits,
        AiLiveDeltaPersistenceContext, AiLiveDeltaPersistenceLimits, AiLiveDeltaSink,
        AiProposalServiceLimits, AiProviderAttachmentResolutionLimits, AiProviderCallExecutor,
        AiProviderCallLimits, AiProviderOutputLimits, AiProviderUsageAccounting,
        AiReadOnlyAgentCoordinator, AiReadOnlyAgentCoordinatorLimits, AiReadOnlyAgentTurnPlan,
        AiReadOnlyAgentTurnPlanner, AiRequestedConsequentialToolCall, AiRunServiceLimits,
        AiSessionRetentionLimits, AiUiIntentDeliveryLimits, AiUiIntentDeliveryService,
        OrmAiApplicationToolCallService, OrmAiApprovalService, OrmAiAttachmentService,
        OrmAiBudgetService, OrmAiConsequentialToolCallService, OrmAiCoordinatorCheckpointService,
        OrmAiEgressDecisionAudit, OrmAiInboxPruningService, OrmAiInboxService,
        OrmAiLiveDeltaService, OrmAiPricingService, OrmAiProposalService,
        OrmAiProviderOutputService, OrmAiRulePolicyService, OrmAiRunService,
        OrmAiSessionRetentionService, OrmAiSkillCatalogService, OrmAiUiIntentDeliveryService,
        OrmAiUsageService,
    };
    pub use agql_auth::{CurrentPrincipalResolver, PrincipalReference, ResolvedPrincipal};
}
