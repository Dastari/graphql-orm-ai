//! Protected, versioned, non-executable skill and rule contracts.

use std::sync::Arc;

use agql_auth::AuthPrincipal;
use async_graphql::{Context, Enum, ErrorExtensions, InputObject, Object, SimpleObject};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{AiError, AiScope, AiScopeInput};

/// Administrative or runtime skill-catalog action evaluated by the host.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AiSkillAction {
    /// Read redacted skill/version metadata.
    Read,
    /// Create or rename a logical skill.
    ManageSkill,
    /// Publish an immutable protected instruction version.
    PublishVersion,
    /// Enable or disable selection of the current published version.
    SetEnabled,
    /// Resolve protected instructions for a freshly authorized run.
    ResolveForRun,
}

/// Host-owned authorization for the exact skill action and scope.
#[async_trait]
pub trait AiSkillAccessPolicy: Send + Sync {
    /// Returns whether the current principal may perform the action. A skill
    /// policy decision never grants any tool, resolver, egress, or budget
    /// capability described by that skill.
    async fn can_access_skill(
        &self,
        principal: &AuthPrincipal,
        scope: &AiScope,
        action: AiSkillAction,
    ) -> bool;
}

/// Server-recognized skill activation rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Enum)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_items = "PascalCase"))]
pub enum AiSkillActivationInput {
    /// A trusted planner must select the skill explicitly.
    Manual,
    /// The skill is considered for every run in its exact scope, then still
    /// intersected with current host/tool/egress/budget policy.
    AlwaysForScope,
}

impl AiSkillActivationInput {
    /// Stable persistence value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::AlwaysForScope => "always_for_scope",
        }
    }
}

/// Maximum data classification a skill may request from current policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Enum)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_items = "PascalCase"))]
pub enum AiSkillClassificationInput {
    /// Public data only.
    Public,
    /// Internal application data or lower.
    Internal,
    /// Confidential data or lower.
    Confidential,
    /// Restricted data or lower. Secret material is never selectable.
    Restricted,
}

impl AiSkillClassificationInput {
    /// Stable persistence value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Internal => "internal",
            Self::Confidential => "confidential",
            Self::Restricted => "restricted",
        }
    }
}

/// Maximum tool maturity a skill may request from current policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Enum)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_items = "PascalCase"))]
pub enum AiSkillMaturityInput {
    /// Read-only application operations.
    ReadOnly,
    /// Read-only operations plus AI-owned proposal staging.
    ProposalOnly,
    /// Explicitly registered writes with independent one-shot approval.
    SupervisedWrite,
}

impl AiSkillMaturityInput {
    /// Stable persistence value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::ProposalOnly => "proposal_only",
            Self::SupervisedWrite => "supervised_write",
        }
    }
}

/// Provider capabilities a skill may require; all are requests that routing
/// policy may further narrow or reject.
#[derive(Clone, Debug, Default, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiSkillProviderCapabilitiesInput {
    /// Requires model image input.
    pub image_input: bool,
    /// Requires model file input.
    pub file_input: bool,
    /// Requires structured JSON-schema output.
    pub structured_output: bool,
    /// Requires custom application tools.
    pub custom_tools: bool,
    /// Requires separately authorized provider web search.
    pub web_search: bool,
    /// Requires provider image generation.
    pub image_generation: bool,
}

/// Hard per-run ceilings embedded in one immutable skill version.
#[derive(Clone, Debug, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiSkillBudgetInput {
    /// Maximum model/tool steps.
    pub maximum_steps: i64,
    /// Maximum wall-clock duration in seconds.
    pub maximum_duration_seconds: i64,
    /// Maximum total model output tokens.
    pub maximum_output_tokens: i64,
    /// Optional maximum deployment-defined cost in microunits.
    pub maximum_cost_microunits: Option<i64>,
}

/// Exact registered UI-intent contract requested by one skill version.
///
/// This binding grants no navigation or referenced-resource authority.
#[derive(Clone, Debug, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiSkillUiIntentBindingInput {
    /// Stable logical intent type.
    pub intent_type: String,
    /// Exact registered descriptor fingerprint.
    pub descriptor_fingerprint: String,
}

/// Creates or CAS-updates safe logical skill metadata. New skills are always
/// disabled until a version is published.
#[derive(Clone, Debug, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct UpsertAiSkillInput {
    /// Existing skill ID, or absent to create.
    pub id: Option<Uuid>,
    /// Exact owning application scope.
    pub scope: AiScopeInput,
    /// User-visible skill name.
    pub name: String,
    /// Safe user-visible description; not model instructions.
    pub description: String,
    /// Expected CAS version for update; absent only on create.
    pub expected_version: Option<i64>,
}

/// Publishes one immutable protected skill version and makes it current.
#[derive(InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct PublishAiSkillVersionInput {
    /// Logical skill receiving the version.
    pub skill_id: Uuid,
    /// Expected current skill CAS version.
    pub expected_skill_version: i64,
    /// Stable application-defined version label.
    pub version: String,
    /// Trusted instruction text protected before ORM persistence.
    pub instructions: String,
    /// Exact registered tool descriptor fingerprints this version may request.
    pub allowed_tool_fingerprints: Vec<String>,
    /// Maximum data-classification request; current egress policy still wins.
    pub maximum_classification: AiSkillClassificationInput,
    /// Maximum tool maturity request; deployment/scope policy still wins.
    pub maximum_tool_maturity: AiSkillMaturityInput,
    /// Server-recognized activation rule.
    pub activation: AiSkillActivationInput,
    /// JSON Schema 2020-12 for optional skill input.
    pub input_schema: async_graphql::Json<serde_json::Value>,
    /// JSON Schema 2020-12 for optional structured skill output.
    pub output_schema: async_graphql::Json<serde_json::Value>,
    /// Provider capability requests.
    pub required_provider_capabilities: AiSkillProviderCapabilitiesInput,
    /// Exact per-run skill ceilings.
    pub budget: AiSkillBudgetInput,
    /// Registered proposal type IDs this version may select.
    pub allowed_proposal_types: Vec<String>,
    /// Exact registered logical UI-intent contracts this version may suggest.
    pub allowed_ui_intents: Vec<AiSkillUiIntentBindingInput>,
    /// Whether to enable selection after the version commits.
    pub enable: bool,
}

/// CAS input for enabling or disabling a skill's current version.
#[derive(Clone, Debug, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct SetAiSkillEnabledInput {
    /// Logical skill ID.
    pub skill_id: Uuid,
    /// Enabled state.
    pub enabled: bool,
    /// Expected current skill CAS version.
    pub expected_version: i64,
}

/// Redacted immutable skill-version metadata.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiSkillVersionView {
    /// Immutable version row ID.
    pub id: Uuid,
    /// Application-defined version label.
    pub version: String,
    /// Stable content checksum.
    pub checksum: String,
    /// Exact tool descriptor fingerprints.
    pub allowed_tool_fingerprints: Vec<String>,
    /// Stable maximum classification value.
    pub maximum_classification: String,
    /// Stable maximum maturity value.
    pub maximum_tool_maturity: String,
    /// Stable activation value.
    pub activation: String,
    /// Registered proposal type IDs.
    pub allowed_proposal_types: Vec<String>,
    /// Registered logical UI-intent bindings; never frontend routes.
    pub allowed_ui_intents: Vec<AiSkillUiIntentBindingView>,
    /// Maximum model/tool steps.
    pub maximum_steps: i64,
    /// Maximum wall-clock duration in seconds.
    pub maximum_duration_seconds: i64,
    /// Maximum output tokens.
    pub maximum_output_tokens: i64,
    /// Optional maximum cost in microunits.
    pub maximum_cost_microunits: Option<i64>,
    /// Author subject.
    pub author_subject: String,
    /// Publication timestamp in Unix seconds.
    pub created_at: i64,
}

/// Redacted exact UI-intent binding on an immutable skill version.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiSkillUiIntentBindingView {
    /// Stable logical intent type.
    pub intent_type: String,
    /// Exact registered descriptor fingerprint.
    pub descriptor_fingerprint: String,
}

/// Redacted logical skill with its current published version metadata.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiSkillView {
    /// Skill ID.
    pub id: Uuid,
    /// Exact scope kind.
    pub scope_kind: String,
    /// Exact scope ID.
    pub scope_id: String,
    /// Optional tenant boundary.
    pub tenant_id: Option<String>,
    /// User-visible name.
    pub name: String,
    /// Safe description.
    pub description: String,
    /// Whether selection is enabled.
    pub enabled: bool,
    /// Current immutable version metadata.
    pub current_version: Option<AiSkillVersionView>,
    /// Skill CAS version.
    pub row_version: i64,
    /// Update timestamp in Unix seconds.
    pub updated_at: i64,
}

/// Protected current skill resolved for a freshly authorized run.
///
/// This value proves only that one enabled, published, checksum-valid version
/// was opened under the current scope protection policy. It grants no tool,
/// resolver, mutation, egress, provider, budget, proposal, or UI authority.
/// Every listed request must be intersected with current independent policy.
#[derive(Clone, Debug)]
pub struct AiResolvedSkill {
    /// Logical skill metadata.
    pub skill: AiSkillView,
    /// Trusted protected instruction text.
    pub instructions: String,
    /// Input JSON schema.
    pub input_schema: serde_json::Value,
    /// Output JSON schema.
    pub output_schema: serde_json::Value,
    /// Redacted provider capability request object.
    pub required_provider_capabilities: serde_json::Value,
}

/// Authenticated, GraphQL-managed skill catalog.
#[async_trait]
pub trait AiSkillCatalogService: Send + Sync {
    /// Lists at most 100 redacted skills in one exact scope.
    async fn skills(
        &self,
        principal: &AuthPrincipal,
        scope: AiScope,
    ) -> Result<Vec<AiSkillView>, AiError>;

    /// Creates or CAS-updates safe logical metadata.
    async fn upsert_skill(
        &self,
        principal: &AuthPrincipal,
        input: UpsertAiSkillInput,
    ) -> Result<AiSkillView, AiError>;

    /// Publishes an immutable protected version and makes it current.
    async fn publish_version(
        &self,
        principal: &AuthPrincipal,
        input: PublishAiSkillVersionInput,
    ) -> Result<AiSkillView, AiError>;

    /// Enables or disables selection of the current published version.
    async fn set_enabled(
        &self,
        principal: &AuthPrincipal,
        input: SetAiSkillEnabledInput,
    ) -> Result<AiSkillView, AiError>;

    /// Resolves enabled exact-scope versions for a current, freshly
    /// rehydrated principal. Callers must independently revalidate every
    /// requested capability before use.
    async fn resolve_enabled_skills(
        &self,
        principal: &AuthPrincipal,
        scope: AiScope,
    ) -> Result<Vec<AiResolvedSkill>, AiError>;
}

/// Composable redacted skill query root.
#[derive(Clone, Copy, Debug, Default)]
pub struct AiSkillQueryRoot;

#[cfg_attr(
    feature = "graphql-case-pascal",
    Object(rename_fields = "PascalCase", rename_args = "PascalCase")
)]
#[cfg_attr(not(feature = "graphql-case-pascal"), Object)]
impl AiSkillQueryRoot {
    /// Lists bounded redacted skill metadata for one exact scope.
    async fn ai_skills(
        &self,
        context: &Context<'_>,
        scope: AiScopeInput,
    ) -> async_graphql::Result<Vec<AiSkillView>> {
        let principal = agql_auth::principal_from_ctx(context)?;
        let skills = skill_service(context)?
            .skills(&principal, scope.into())
            .await
            .map_err(extend)?;
        if skills.len() > 100 {
            return Err(AiError::InvalidConfiguration(
                "skill service returned an unbounded list".to_owned(),
            )
            .extend());
        }
        Ok(skills)
    }
}

/// Composable skill-management mutation root.
#[derive(Clone, Copy, Debug, Default)]
pub struct AiSkillMutationRoot;

#[cfg_attr(
    feature = "graphql-case-pascal",
    Object(rename_fields = "PascalCase", rename_args = "PascalCase")
)]
#[cfg_attr(not(feature = "graphql-case-pascal"), Object)]
impl AiSkillMutationRoot {
    /// Creates or CAS-updates safe logical skill metadata.
    async fn upsert_ai_skill(
        &self,
        context: &Context<'_>,
        input: UpsertAiSkillInput,
    ) -> async_graphql::Result<AiSkillView> {
        let principal = agql_auth::principal_from_ctx(context)?;
        skill_service(context)?
            .upsert_skill(&principal, input)
            .await
            .map_err(extend)
    }

    /// Publishes one protected immutable skill version.
    async fn publish_ai_skill_version(
        &self,
        context: &Context<'_>,
        input: PublishAiSkillVersionInput,
    ) -> async_graphql::Result<AiSkillView> {
        let principal = agql_auth::principal_from_ctx(context)?;
        skill_service(context)?
            .publish_version(&principal, input)
            .await
            .map_err(extend)
    }

    /// Enables or disables the current published version.
    async fn set_ai_skill_enabled(
        &self,
        context: &Context<'_>,
        input: SetAiSkillEnabledInput,
    ) -> async_graphql::Result<AiSkillView> {
        let principal = agql_auth::principal_from_ctx(context)?;
        skill_service(context)?
            .set_enabled(&principal, input)
            .await
            .map_err(extend)
    }
}

fn skill_service(context: &Context<'_>) -> async_graphql::Result<Arc<dyn AiSkillCatalogService>> {
    context
        .data_opt::<Arc<dyn AiSkillCatalogService>>()
        .cloned()
        .ok_or_else(|| {
            AiError::InvalidConfiguration("AI skill catalog service is missing".to_owned()).extend()
        })
}

fn extend(error: AiError) -> async_graphql::Error {
    error.extend()
}
