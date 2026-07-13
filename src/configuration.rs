//! Redacted GraphQL-managed AI configuration contracts.

use std::sync::Arc;

use agql_auth::AuthPrincipal;
use async_graphql::{Context, Enum, ErrorExtensions, InputObject, Object, SimpleObject};
use async_trait::async_trait;
use secrecy::SecretString;
use uuid::Uuid;

use crate::{AiContentProtectionMode, AiError, AiScope, AiScopeInput};

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
    /// Redacted endpoint; native providers may omit it.
    pub base_url: Option<String>,
    /// Whether a credential reference is configured.
    pub credential_configured: bool,
    /// Whether routing may select this profile.
    pub enabled: bool,
    /// CAS version.
    pub row_version: i64,
    /// Update time in Unix seconds.
    pub updated_at: i64,
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
    /// Enable routing after all other policy gates pass.
    pub enabled: bool,
    /// Expected CAS version for an update.
    pub expected_version: Option<i64>,
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
/// event, and require current recent MFA for credential and content-protection
/// changes. A failed audit append fails the mutation.
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

fn extend(error: AiError) -> async_graphql::Error {
    error.extend()
}
