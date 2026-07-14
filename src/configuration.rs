//! Redacted GraphQL-managed AI configuration contracts.

use std::sync::Arc;

use agql_auth::AuthPrincipal;
use async_graphql::{Context, Enum, ErrorExtensions, InputObject, Object, SimpleObject};
use async_trait::async_trait;
use secrecy::SecretString;
use uuid::Uuid;

use crate::{
    AiContentProtectionMode, AiError, AiPricingCatalogService, AiPricingPolicyView, AiScope,
    AiScopeInput, CreateAiPricingPolicyInput,
};

/// Administrative configuration action evaluated by the host.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AiConfigurationAction {
    /// Read redacted provider configuration.
    ReadProviderProfiles,
    /// Create or alter provider routing/endpoint metadata.
    ManageProviderProfiles,
    /// Store, rotate, or remove provider credentials.
    ManageProviderCredentials,
    /// Read content-protection readiness.
    ReadContentProtection,
    /// Change content-protection mode or key policy.
    ManageContentProtection,
    /// Read scope retention and purge settings.
    ReadRetention,
    /// Change scope retention and purge settings.
    ManageRetention,
    /// Read redacted budget policies.
    ReadBudgetPolicies,
    /// Create, alter, enable, or disable budget policies.
    ManageBudgetPolicies,
    /// Read immutable provider/model pricing versions.
    ReadPricingCatalog,
    /// Append immutable provider/model pricing versions.
    ManagePricingCatalog,
}

/// Host-owned administrative authorization for GraphQL-managed AI settings.
/// Scope naming and wildcard semantics remain entirely in the host policy.
#[async_trait]
pub trait AiConfigurationAccessPolicy: Send + Sync {
    /// Returns whether the current principal may perform the exact action in
    /// the exact scope. Implementations must fail closed on dependency errors.
    async fn can_configure(
        &self,
        principal: &AuthPrincipal,
        scope: &AiScope,
        action: AiConfigurationAction,
    ) -> bool;
}

/// Deployment-owned validation for configurable provider endpoints.
///
/// The library performs basic URL safety validation first. This policy then
/// enforces network zones, allowed hosts/ports, local-provider rules, and any
/// SSRF protections specific to the deployment.
pub trait AiProviderEndpointPolicy: Send + Sync {
    /// Authorizes a normalized endpoint for a provider kind.
    fn authorize_endpoint(&self, provider_kind: AiProviderKindInput, normalized_url: &str) -> bool;
}

/// Provider family accepted by GraphQL configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_items = "PascalCase"))]
pub enum AiProviderKindInput {
    /// OpenAI native Responses API.
    OpenAi,
    /// Anthropic native API.
    Anthropic,
    /// xAI native API.
    Xai,
    /// Local/native Ollama API.
    Ollama,
    /// Explicit capability-profiled compatible endpoint.
    OpenAiCompatible,
    /// Deployment-registered installed local harness.
    LocalHarness,
}

impl AiProviderKindInput {
    /// Stable persistence/configuration value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Anthropic => "anthropic",
            Self::Xai => "xai",
            Self::Ollama => "ollama",
            Self::OpenAiCompatible => "openai_compatible",
            Self::LocalHarness => "local_harness",
        }
    }
}

/// Redacted provider profile. Credential references and values are omitted.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiProviderProfileView {
    /// Profile ID.
    pub id: Uuid,
    /// Scope kind.
    pub scope_kind: String,
    /// Scope ID.
    pub scope_id: String,
    /// Optional tenant boundary.
    pub tenant_id: Option<String>,
    /// Stable provider kind.
    pub provider_kind: String,
    /// Administrative display name.
    pub display_name: String,
    /// Reviewed endpoint; native providers omit it. This is administrative
    /// configuration and must not be exposed to untrusted model input.
    pub base_url: Option<String>,
    /// Reviewed OpenAI-compatible capability/retention profile when configured.
    pub openai_compatible: Option<AiOpenAiCompatibleProfileView>,
    /// Whether a credential reference is configured.
    pub credential_configured: bool,
    /// Whether routing may select this profile.
    pub enabled: bool,
    /// CAS version.
    pub row_version: i64,
    /// Update time in Unix seconds.
    pub updated_at: i64,
}

/// Redacted reviewed contract for an OpenAI-compatible provider profile.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiOpenAiCompatibleProfileView {
    /// Exact retention label required in every egress manifest.
    pub retention: String,
    /// Whether strict custom application tools are supported.
    pub custom_tools: bool,
    /// Whether parallel custom tool calls are supported.
    pub parallel_tool_calls: bool,
    /// Whether JSON-schema structured output is supported.
    pub structured_output: bool,
    /// Whether provider-retained response-ID continuation is supported.
    pub provider_retained_continuation: bool,
}

/// Redacted scope content-protection state.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiContentProtectionPolicyView {
    /// Scope kind.
    pub scope_kind: String,
    /// Scope ID.
    pub scope_id: String,
    /// Optional tenant boundary.
    pub tenant_id: Option<String>,
    /// Stable selected mode.
    pub protection_mode: String,
    /// Whether migration/re-protection is ready.
    pub ready: bool,
    /// CAS version.
    pub row_version: i64,
    /// Effective time in Unix seconds.
    pub effective_at: i64,
}

/// Redacted retention and purge settings for one application scope.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiRetentionPolicyView {
    /// Scope kind.
    pub scope_kind: String,
    /// Scope ID.
    pub scope_id: String,
    /// Optional tenant boundary.
    pub tenant_id: Option<String>,
    /// Optional message retention in seconds; absent means no automatic
    /// message-age purge is configured.
    pub message_retention_seconds: Option<i64>,
    /// Live-delta retention in seconds.
    pub delta_retention_seconds: i64,
    /// Raw provider/tool payload retention in seconds.
    pub raw_payload_retention_seconds: i64,
    /// Audit-fact retention in seconds.
    pub audit_retention_seconds: i64,
    /// Delay before deleted content may be physically purged, in seconds.
    pub deleted_content_purge_seconds: i64,
    /// Whether provider-persistent files must be deleted during purge.
    pub provider_file_delete_required: bool,
    /// Cross-session inbox-event retention in seconds.
    pub inbox_event_retention_seconds: i64,
    /// Most recent cross-session inbox events retained regardless of age.
    pub inbox_minimum_events: i64,
    /// CAS version.
    pub row_version: i64,
    /// Update time in Unix seconds.
    pub updated_at: i64,
}

/// Stable budget reset interval.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_items = "PascalCase"))]
pub enum AiBudgetIntervalInput {
    /// Fixed UTC-aligned minute.
    Minute,
    /// Fixed UTC-aligned hour.
    Hour,
    /// Fixed UTC-aligned day.
    Day,
    /// UTC calendar month.
    Month,
    /// Never resets automatically.
    Lifetime,
}

impl AiBudgetIntervalInput {
    /// Stable persistence value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Minute => "minute",
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Month => "month",
            Self::Lifetime => "lifetime",
        }
    }
}

/// Redacted scope/principal budget ceiling.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiBudgetPolicyView {
    /// Policy ID.
    pub id: Uuid,
    /// Exact scope kind.
    pub scope_kind: String,
    /// Exact scope ID.
    pub scope_id: String,
    /// Optional tenant boundary. Absence is a tenant wildcard for matching
    /// scope kind/ID and requires host authorization to manage.
    pub tenant_id: Option<String>,
    /// Optional exact principal kind; present only with principal subject.
    pub principal_kind: Option<String>,
    /// Optional exact principal subject; present only with principal kind.
    pub principal_subject: Option<String>,
    /// Stable reset interval.
    pub interval_kind: String,
    /// Maximum total input tokens.
    pub maximum_input_tokens: Option<i64>,
    /// Maximum output tokens.
    pub maximum_output_tokens: Option<i64>,
    /// Maximum tool units.
    pub maximum_tool_units: Option<i64>,
    /// Maximum image units.
    pub maximum_image_units: Option<i64>,
    /// Maximum deployment-defined cost microunits.
    pub maximum_cost_microunits: Option<i64>,
    /// Maximum provider runs/calls.
    pub maximum_runs: Option<i64>,
    /// Whether this policy participates in new reservations.
    pub enabled: bool,
    /// CAS version.
    pub row_version: i64,
    /// Update time in Unix seconds.
    pub updated_at: i64,
}

/// Provider profile CAS upsert.
#[derive(InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct UpsertAiProviderProfileInput {
    /// Existing profile ID, or absent to create.
    pub id: Option<Uuid>,
    /// Owning scope.
    pub scope: AiScopeInput,
    /// Provider family.
    pub provider_kind: AiProviderKindInput,
    /// Administrative display name.
    pub display_name: String,
    /// Endpoint for explicitly configurable providers.
    pub base_url: Option<String>,
    /// Required reviewed contract only for `OpenAiCompatible` profiles.
    pub openai_compatible: Option<AiOpenAiCompatibleProfileInput>,
    /// Enable routing after all other policy gates pass.
    pub enabled: bool,
    /// Expected CAS version for an update.
    pub expected_version: Option<i64>,
}

/// Reviewed GraphQL configuration for an OpenAI-compatible profile.
#[derive(InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiOpenAiCompatibleProfileInput {
    /// Exact provider retention label used by egress policy.
    pub retention: String,
    /// Permit strict custom application tools.
    pub custom_tools: bool,
    /// Permit parallel custom tool calls.
    pub parallel_tool_calls: bool,
    /// Permit JSON-schema structured output.
    pub structured_output: bool,
    /// Permit provider-retained response-ID continuation.
    pub provider_retained_continuation: bool,
}

/// Credential rotation input. This type deliberately does not derive `Debug`,
/// `Clone`, serialization, or equality.
#[derive(InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct SetAiProviderCredentialInput {
    /// Provider profile.
    pub profile_id: Uuid,
    /// Provider credential plaintext. It is converted to [`SecretString`]
    /// immediately in the resolver and must never be persisted in this form.
    #[graphql(secret)]
    pub credential: String,
    /// Expected provider-profile CAS version.
    pub expected_version: i64,
}

/// Credential removal input.
#[derive(Clone, Debug, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct RemoveAiProviderCredentialInput {
    /// Provider profile.
    pub profile_id: Uuid,
    /// Expected provider-profile CAS version.
    pub expected_version: i64,
}

/// Content-protection policy CAS input.
#[derive(Clone, Debug, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct SetAiContentProtectionPolicyInput {
    /// Owning scope.
    pub scope: AiScopeInput,
    /// Database-managed or application-level envelope encryption.
    pub mode: AiContentProtectionModeInput,
    /// Non-secret key-policy reference for application encryption.
    pub key_policy_reference: Option<String>,
    /// Expected CAS version, or absent to create.
    pub expected_version: Option<i64>,
}

/// Scope retention-policy CAS input.
#[derive(Clone, Debug, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct SetAiRetentionPolicyInput {
    /// Owning scope.
    pub scope: AiScopeInput,
    /// Optional message retention in seconds.
    pub message_retention_seconds: Option<i64>,
    /// Live-delta retention in seconds.
    pub delta_retention_seconds: i64,
    /// Raw provider/tool payload retention in seconds.
    pub raw_payload_retention_seconds: i64,
    /// Audit-fact retention in seconds.
    pub audit_retention_seconds: i64,
    /// Delay before deleted content may be physically purged, in seconds.
    pub deleted_content_purge_seconds: i64,
    /// Require provider-persistent file deletion during purge.
    pub provider_file_delete_required: bool,
    /// Cross-session inbox-event retention in seconds.
    pub inbox_event_retention_seconds: i64,
    /// Most recent cross-session inbox events retained regardless of age.
    pub inbox_minimum_events: i64,
    /// Expected CAS version, or absent to create.
    pub expected_version: Option<i64>,
}

/// Budget-policy CAS create/update input.
#[derive(Clone, Debug, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct UpsertAiBudgetPolicyInput {
    /// Existing policy ID, or absent to create.
    pub id: Option<Uuid>,
    /// Exact owning scope. On update this immutable binding must match.
    pub scope: AiScopeInput,
    /// Optional exact principal kind. Kind and subject must be supplied or
    /// omitted together and are immutable after creation.
    pub principal_kind: Option<String>,
    /// Optional exact principal subject.
    pub principal_subject: Option<String>,
    /// Reset interval, immutable after creation.
    pub interval: AiBudgetIntervalInput,
    /// Optional total input-token ceiling.
    pub maximum_input_tokens: Option<i64>,
    /// Optional output-token ceiling.
    pub maximum_output_tokens: Option<i64>,
    /// Optional tool-unit ceiling.
    pub maximum_tool_units: Option<i64>,
    /// Optional image-unit ceiling.
    pub maximum_image_units: Option<i64>,
    /// Optional deployment-defined cost ceiling in microunits.
    pub maximum_cost_microunits: Option<i64>,
    /// Optional provider run/call ceiling.
    pub maximum_runs: Option<i64>,
    /// Whether this policy participates in new reservations.
    pub enabled: bool,
    /// Expected CAS version for an update; absent on create.
    pub expected_version: Option<i64>,
}

/// GraphQL content-protection mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_items = "PascalCase"))]
pub enum AiContentProtectionModeInput {
    /// Deployment database/storage encryption at rest.
    DatabaseManaged,
    /// Application-level authenticated encryption before ORM persistence.
    ApplicationEncrypted,
}

impl From<AiContentProtectionModeInput> for AiContentProtectionMode {
    fn from(value: AiContentProtectionModeInput) -> Self {
        match value {
            AiContentProtectionModeInput::DatabaseManaged => Self::DatabaseManaged,
            AiContentProtectionModeInput::ApplicationEncrypted => Self::ApplicationEncrypted,
        }
    }
}

/// Authenticated configuration backend.
///
/// Implementations must enforce administrative authorization and scope/tenant
/// isolation for every method. Mutations must use CAS, append a redacted audit
/// event, and require current recent MFA for credential, content-protection,
/// retention, and budget-policy changes. A failed audit append fails the
/// mutation.
#[async_trait]
pub trait AiConfigurationService: Send + Sync {
    /// Lists at most 100 visible redacted profiles for a scope.
    async fn provider_profiles(
        &self,
        principal: &AuthPrincipal,
        scope: AiScope,
    ) -> Result<Vec<AiProviderProfileView>, AiError>;

    /// Loads the redacted content-protection state for a visible scope.
    async fn content_protection_policy(
        &self,
        principal: &AuthPrincipal,
        scope: AiScope,
    ) -> Result<Option<AiContentProtectionPolicyView>, AiError>;

    /// Loads the redacted retention state for a visible scope.
    async fn retention_policy(
        &self,
        principal: &AuthPrincipal,
        scope: AiScope,
    ) -> Result<Option<AiRetentionPolicyView>, AiError>;

    /// Lists at most 100 redacted budget policies for one exact scope.
    async fn budget_policies(
        &self,
        principal: &AuthPrincipal,
        scope: AiScope,
    ) -> Result<Vec<AiBudgetPolicyView>, AiError>;

    /// Creates or CAS-updates a provider profile and audits the change.
    async fn upsert_provider_profile(
        &self,
        principal: &AuthPrincipal,
        input: UpsertAiProviderProfileInput,
    ) -> Result<AiProviderProfileView, AiError>;

    /// Stores/rotates a credential through [`crate::AiSecretStore`], updates
    /// only its reference transactionally, and audits without the reference.
    async fn set_provider_credential(
        &self,
        principal: &AuthPrincipal,
        profile_id: Uuid,
        credential: SecretString,
        expected_version: i64,
    ) -> Result<AiProviderProfileView, AiError>;

    /// Removes/revokes a credential reference and audits the change.
    async fn remove_provider_credential(
        &self,
        principal: &AuthPrincipal,
        input: RemoveAiProviderCredentialInput,
    ) -> Result<AiProviderProfileView, AiError>;

    /// Creates or CAS-updates required scope content protection.
    async fn set_content_protection_policy(
        &self,
        principal: &AuthPrincipal,
        input: SetAiContentProtectionPolicyInput,
    ) -> Result<AiContentProtectionPolicyView, AiError>;

    /// Creates or CAS-updates required scope retention and purge settings.
    async fn set_retention_policy(
        &self,
        principal: &AuthPrincipal,
        input: SetAiRetentionPolicyInput,
    ) -> Result<AiRetentionPolicyView, AiError>;

    /// Creates or CAS-updates a budget policy and appends a redacted audit in
    /// the same transaction.
    async fn upsert_budget_policy(
        &self,
        principal: &AuthPrincipal,
        input: UpsertAiBudgetPolicyInput,
    ) -> Result<AiBudgetPolicyView, AiError>;
}

/// Composable redacted configuration query root.
#[derive(Clone, Copy, Debug, Default)]
pub struct AiConfigurationQueryRoot;

#[cfg_attr(
    feature = "graphql-case-pascal",
    Object(rename_fields = "PascalCase", rename_args = "PascalCase")
)]
#[cfg_attr(not(feature = "graphql-case-pascal"), Object)]
impl AiConfigurationQueryRoot {
    /// Lists bounded redacted provider profiles.
    async fn ai_provider_profiles(
        &self,
        context: &Context<'_>,
        scope: AiScopeInput,
    ) -> async_graphql::Result<Vec<AiProviderProfileView>> {
        let principal = agql_auth::principal_from_ctx(context)?;
        let profiles = configuration_service(context)?
            .provider_profiles(&principal, scope.into())
            .await
            .map_err(extend)?;
        if profiles.len() > 100 {
            return Err(AiError::InvalidConfiguration(
                "configuration service returned an unbounded profile list".to_owned(),
            )
            .extend());
        }
        Ok(profiles)
    }

    /// Loads redacted scope content-protection readiness.
    async fn ai_content_protection_policy(
        &self,
        context: &Context<'_>,
        scope: AiScopeInput,
    ) -> async_graphql::Result<Option<AiContentProtectionPolicyView>> {
        let principal = agql_auth::principal_from_ctx(context)?;
        configuration_service(context)?
            .content_protection_policy(&principal, scope.into())
            .await
            .map_err(extend)
    }

    /// Loads redacted scope retention and purge settings.
    async fn ai_retention_policy(
        &self,
        context: &Context<'_>,
        scope: AiScopeInput,
    ) -> async_graphql::Result<Option<AiRetentionPolicyView>> {
        let principal = agql_auth::principal_from_ctx(context)?;
        configuration_service(context)?
            .retention_policy(&principal, scope.into())
            .await
            .map_err(extend)
    }

    /// Lists bounded redacted budget policies for one exact scope.
    async fn ai_budget_policies(
        &self,
        context: &Context<'_>,
        scope: AiScopeInput,
    ) -> async_graphql::Result<Vec<AiBudgetPolicyView>> {
        let principal = agql_auth::principal_from_ctx(context)?;
        let policies = configuration_service(context)?
            .budget_policies(&principal, scope.into())
            .await
            .map_err(extend)?;
        if policies.len() > 100 {
            return Err(AiError::InvalidConfiguration(
                "configuration service returned an unbounded budget-policy list".to_owned(),
            )
            .extend());
        }
        Ok(policies)
    }

    /// Lists bounded immutable pricing versions for one exact route.
    async fn ai_pricing_policies(
        &self,
        context: &Context<'_>,
        scope: AiScopeInput,
        provider_kind: AiProviderKindInput,
        provider_model: String,
    ) -> async_graphql::Result<Vec<AiPricingPolicyView>> {
        let principal = agql_auth::principal_from_ctx(context)?;
        let policies = pricing_catalog_service(context)?
            .pricing_policies(&principal, scope.into(), provider_kind, provider_model)
            .await
            .map_err(extend)?;
        if policies.len() > 100 {
            return Err(AiError::InvalidConfiguration(
                "pricing catalog returned an unbounded version list".to_owned(),
            )
            .extend());
        }
        Ok(policies)
    }
}

/// Composable configuration mutation root.
#[derive(Clone, Copy, Debug, Default)]
pub struct AiConfigurationMutationRoot;

#[cfg_attr(
    feature = "graphql-case-pascal",
    Object(rename_fields = "PascalCase", rename_args = "PascalCase")
)]
#[cfg_attr(not(feature = "graphql-case-pascal"), Object)]
impl AiConfigurationMutationRoot {
    /// Creates or CAS-updates a provider profile.
    async fn upsert_ai_provider_profile(
        &self,
        context: &Context<'_>,
        input: UpsertAiProviderProfileInput,
    ) -> async_graphql::Result<AiProviderProfileView> {
        let principal = agql_auth::principal_from_ctx(context)?;
        configuration_service(context)?
            .upsert_provider_profile(&principal, input)
            .await
            .map_err(extend)
    }

    /// Stores or rotates a provider credential; no secret value/reference is
    /// returned.
    async fn set_ai_provider_credential(
        &self,
        context: &Context<'_>,
        input: SetAiProviderCredentialInput,
    ) -> async_graphql::Result<AiProviderProfileView> {
        let principal = agql_auth::principal_from_ctx(context)?;
        let credential = SecretString::from(input.credential);
        configuration_service(context)?
            .set_provider_credential(
                &principal,
                input.profile_id,
                credential,
                input.expected_version,
            )
            .await
            .map_err(extend)
    }

    /// Removes/revokes a provider credential.
    async fn remove_ai_provider_credential(
        &self,
        context: &Context<'_>,
        input: RemoveAiProviderCredentialInput,
    ) -> async_graphql::Result<AiProviderProfileView> {
        let principal = agql_auth::principal_from_ctx(context)?;
        configuration_service(context)?
            .remove_provider_credential(&principal, input)
            .await
            .map_err(extend)
    }

    /// Sets required per-scope content protection.
    async fn set_ai_content_protection_policy(
        &self,
        context: &Context<'_>,
        input: SetAiContentProtectionPolicyInput,
    ) -> async_graphql::Result<AiContentProtectionPolicyView> {
        let principal = agql_auth::principal_from_ctx(context)?;
        configuration_service(context)?
            .set_content_protection_policy(&principal, input)
            .await
            .map_err(extend)
    }

    /// Sets required per-scope retention and purge settings.
    async fn set_ai_retention_policy(
        &self,
        context: &Context<'_>,
        input: SetAiRetentionPolicyInput,
    ) -> async_graphql::Result<AiRetentionPolicyView> {
        let principal = agql_auth::principal_from_ctx(context)?;
        configuration_service(context)?
            .set_retention_policy(&principal, input)
            .await
            .map_err(extend)
    }

    /// Creates or CAS-updates a budget policy.
    async fn upsert_ai_budget_policy(
        &self,
        context: &Context<'_>,
        input: UpsertAiBudgetPolicyInput,
    ) -> async_graphql::Result<AiBudgetPolicyView> {
        let principal = agql_auth::principal_from_ctx(context)?;
        configuration_service(context)?
            .upsert_budget_policy(&principal, input)
            .await
            .map_err(extend)
    }

    /// Appends one immediately effective immutable pricing version.
    async fn create_ai_pricing_policy(
        &self,
        context: &Context<'_>,
        input: CreateAiPricingPolicyInput,
    ) -> async_graphql::Result<AiPricingPolicyView> {
        let principal = agql_auth::principal_from_ctx(context)?;
        pricing_catalog_service(context)?
            .create_pricing_policy(&principal, input)
            .await
            .map_err(extend)
    }
}

fn configuration_service(
    context: &Context<'_>,
) -> async_graphql::Result<Arc<dyn AiConfigurationService>> {
    context
        .data_opt::<Arc<dyn AiConfigurationService>>()
        .cloned()
        .ok_or_else(|| {
            AiError::InvalidConfiguration("AI configuration service is missing".to_owned()).extend()
        })
}

fn pricing_catalog_service(
    context: &Context<'_>,
) -> async_graphql::Result<Arc<dyn AiPricingCatalogService>> {
    context
        .data_opt::<Arc<dyn AiPricingCatalogService>>()
        .cloned()
        .ok_or_else(|| {
            AiError::InvalidConfiguration("AI pricing catalog service is missing".to_owned())
                .extend()
        })
}

fn extend(error: AiError) -> async_graphql::Error {
    error.extend()
}
