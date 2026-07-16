//! Immutable integer-only provider pricing contracts.

use agql_auth::AuthPrincipal;
use async_graphql::{InputObject, SimpleObject};
use async_trait::async_trait;
use uuid::Uuid;

use crate::{AiBudgetAmounts, AiError, AiProviderKindInput, AiScope, AiScopeInput, ProviderKind};

/// Immutable pricing catalog entry exposed to authorized administrators.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiPricingPolicyView {
    /// Persistent pricing version ID.
    pub id: Uuid,
    /// Globally unique exact version reference stored on budget reservations.
    pub version_reference: String,
    /// Exact scope kind.
    pub scope_kind: String,
    /// Exact scope ID.
    pub scope_id: String,
    /// Optional tenant boundary.
    pub tenant_id: Option<String>,
    /// Stable provider family.
    pub provider_kind: String,
    /// Exact provider or logical model.
    pub provider_model: String,
    /// Fixed deployment-defined microunits charged per call.
    pub fixed_call_microunits: i64,
    /// Non-cached input-token microunits per one million tokens.
    pub input_microunits_per_million: i64,
    /// Cached input-token microunits per one million tokens.
    pub cached_input_microunits_per_million: i64,
    /// Output-token microunits per one million tokens.
    pub output_microunits_per_million: i64,
    /// Provider-hosted web-search microunits per completed call.
    pub web_search_microunits_per_call: i64,
    /// Provider-hosted file-search microunits per completed call.
    pub file_search_microunits_per_call: i64,
    /// Immutable creation time in Unix seconds.
    pub created_at: i64,
}

/// Creates one immutable pricing version.
#[derive(Clone, Debug, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct CreateAiPricingPolicyInput {
    /// Exact owning scope.
    pub scope: AiScopeInput,
    /// Provider family.
    pub provider_kind: AiProviderKindInput,
    /// Exact provider or logical model.
    pub provider_model: String,
    /// Fixed deployment-defined microunits charged per call.
    pub fixed_call_microunits: i64,
    /// Non-cached input-token microunits per one million tokens.
    pub input_microunits_per_million: i64,
    /// Cached input-token microunits per one million tokens. This must be no
    /// greater than the ordinary input rate so estimates remain conservative.
    pub cached_input_microunits_per_million: i64,
    /// Output-token microunits per one million tokens.
    pub output_microunits_per_million: i64,
    /// Provider-hosted web-search microunits per completed call.
    pub web_search_microunits_per_call: i64,
    /// Provider-hosted file-search microunits per completed call.
    pub file_search_microunits_per_call: i64,
}

/// Provider-hosted built-in whose completed calls have an exact unit price.
///
/// This type identifies a pricing dimension only. It does not enable the
/// built-in, authorize provider egress, or prove that a provider call ran.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum AiPricedBuiltinToolKind {
    /// Provider-hosted web search.
    WebSearch,
    /// Provider-hosted search over an already-authorized file store.
    FileSearch,
}

/// Exact immutable pricing quote request used before budget reservation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiPricingQuoteRequest {
    /// Exact application scope.
    pub scope: AiScope,
    /// Provider family.
    pub provider_kind: ProviderKind,
    /// Exact provider or logical model.
    pub provider_model: String,
    /// Exact immutable pricing version reference.
    pub version_reference: String,
    /// Conservative total input-token estimate.
    pub input_tokens: u64,
    /// Requested maximum output tokens.
    pub output_tokens: u64,
    /// Distinct billable built-ins enabled on the exact model request.
    pub builtin_tools: Vec<AiPricedBuiltinToolKind>,
    /// Shared provider-enforced ceiling across the enabled built-ins.
    ///
    /// Use the same value as `maximum_builtin_tool_calls` on the exact
    /// [`crate::ModelRequest`]. A zero value is valid only when
    /// `builtin_tools` is empty.
    pub maximum_builtin_tool_calls: u64,
}

/// Authenticated immutable pricing catalog management.
#[async_trait]
pub trait AiPricingCatalogService: Send + Sync {
    /// Lists at most 100 immutable versions for one exact scope/provider/model.
    ///
    /// # Errors
    ///
    /// Returns an error for denied access, invalid scope/model, corrupt stored
    /// bindings, an excessive result set, or persistence failure.
    async fn pricing_policies(
        &self,
        principal: &AuthPrincipal,
        scope: AiScope,
        provider_kind: AiProviderKindInput,
        provider_model: String,
    ) -> Result<Vec<AiPricingPolicyView>, AiError>;

    /// Creates one immediately effective immutable pricing version.
    ///
    /// # Errors
    ///
    /// Returns an error unless current recent-MFA administration, exact scope,
    /// model/rate bounds, per-route cardinality, persistence, and atomic audit
    /// all succeed.
    async fn create_pricing_policy(
        &self,
        principal: &AuthPrincipal,
        input: CreateAiPricingPolicyInput,
    ) -> Result<AiPricingPolicyView, AiError>;
}

/// Exact-version conservative budget quote service.
#[async_trait]
pub trait AiPricingQuoteService: Send + Sync {
    /// Prices pre-transport token and supported built-in ceilings under one
    /// exact immutable version.
    ///
    /// The quote assumes every input token is non-cached and every permitted
    /// built-in call uses the greatest enabled per-call rate, ensuring the
    /// result remains conservative.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown/swapped scope, provider, model, version,
    /// invalid input, corrupt catalog row, or arithmetic/storage overflow.
    async fn quote(&self, request: AiPricingQuoteRequest) -> Result<AiBudgetAmounts, AiError>;
}
