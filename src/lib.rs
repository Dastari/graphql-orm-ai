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
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_configuration;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_sessions;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod orm_subscriptions;
mod persistence;
mod proposals;
mod provider;
mod providers;
mod restore;
mod run_state;
mod runtime;
mod secrets;
mod sessions;
mod subscriptions;
mod tools;

pub use access::*;
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
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_configuration::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_sessions::*;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use orm_subscriptions::*;
pub use persistence::*;
pub use proposals::*;
pub use provider::*;
pub use providers::*;
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
        AiAccessPolicy, AiApprovalBinding, AiBudgetReservation, AiContentProtectionPolicy,
        AiDataSourceRef, AiDisclosureSchema, AiEgressDecision, AiEgressManifest, AiEgressPolicy,
        AiError, AiProposalCatalog, AiProposalTypeDescriptor, AiProvider, AiRuntime,
        AiRuntimeBuilder, AiScope, AiSecretStore, AiToolAuthorizationPolicy, AiToolCatalog,
        AiToolDescriptor, DataClassification, SecretRef, ToolMaturity,
    };
    pub use agql_auth::{CurrentPrincipalResolver, PrincipalReference, ResolvedPrincipal};
}
