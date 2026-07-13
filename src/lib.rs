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
mod budget;
mod configuration;
mod content_protection;
mod data;
mod disclosure;
mod domain;
mod egress;
mod error;
mod execution;
mod live_delta;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_approvals;
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
mod orm_live_delta;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_proposals;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_provider_output;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_runs;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_sessions;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_subscriptions;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_tools;
mod persistence;
mod proposals;
mod provider;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod provider_calls;
mod providers;
mod remote_execution;
mod restore;
mod run_state;
mod runtime;
mod secrets;
mod sessions;
mod subscriptions;
mod tools;

pub use access::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use agent_loop::*;
pub use approvals::*;
pub use budget::*;
pub use configuration::*;
pub use content_protection::*;
pub use data::*;
pub use disclosure::*;
pub use domain::*;
pub use egress::*;
pub use error::*;
pub use execution::*;
pub use live_delta::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_approvals::*;
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
pub use orm_live_delta::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_proposals::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_provider_output::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_runs::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_sessions::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_subscriptions::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_tools::*;
pub use persistence::*;
pub use proposals::*;
pub use provider::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use provider_calls::*;
pub use providers::*;
pub use remote_execution::*;
pub use restore::*;
pub use run_state::*;
pub use runtime::*;
pub use secrets::*;
pub use sessions::*;
pub use subscriptions::*;
pub use tools::*;

/// Common imports for host integrations.
pub mod prelude {
    pub use crate::{
        AiAccessPolicy, AiApprovalAccessPolicy, AiApprovalBinding, AiBudgetReservation,
        AiBudgetService, AiContentProtectionPolicy, AiDataSourceRef, AiDisclosureSchema,
        AiEgressDecision, AiEgressDecisionAudit, AiEgressManifest, AiEgressPolicy, AiError,
        AiLiveDeltaCoalescerLimits, AiProposalAccessPolicy, AiProposalCatalog,
        AiProposalTypeDescriptor, AiProvider, AiRemoteAuthenticatedGraphqlAdapter,
        AiRemoteGraphqlAuthority, AiRemoteGraphqlAuthorityIssuer, AiRemoteGraphqlDelegationRequest,
        AiRemoteGraphqlExecutionLimits, AiRemoteGraphqlTransport, AiRuntime, AiRuntimeBuilder,
        AiScope, AiSecretStore, AiToolAuthorizationPolicy, AiToolCatalog, AiToolDescriptor,
        DataClassification, SecretRef, ToolMaturity,
    };
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub use crate::{
        AiAdoptedReadOnlyToolBatch, AiAgentCheckpointAdopter, AiAgentCheckpointWriter,
        AiApplicationToolCallLimits, AiApprovalServiceLimits, AiBudgetServiceLimits,
        AiCanonicalActionPreviewBuilder, AiConsequentialToolCallOutcome,
        AiCoordinatorCheckpointLimits, AiLiveDeltaPersistenceContext, AiLiveDeltaPersistenceLimits,
        AiLiveDeltaSink, AiProposalServiceLimits, AiProviderCallExecutor, AiProviderCallLimits,
        AiProviderOutputLimits, AiProviderUsageAccounting, AiReadOnlyAgentCoordinator,
        AiReadOnlyAgentCoordinatorLimits, AiReadOnlyAgentTurnPlan, AiReadOnlyAgentTurnPlanner,
        AiRequestedConsequentialToolCall, AiRunServiceLimits, OrmAiApplicationToolCallService,
        OrmAiApprovalService, OrmAiBudgetService, OrmAiConsequentialToolCallService,
        OrmAiCoordinatorCheckpointService, OrmAiEgressDecisionAudit, OrmAiLiveDeltaService,
        OrmAiProposalService, OrmAiProviderOutputService, OrmAiRunService,
    };
    pub use agql_auth::{CurrentPrincipalResolver, PrincipalReference, ResolvedPrincipal};
}
