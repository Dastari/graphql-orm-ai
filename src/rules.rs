//! Hierarchical, project-neutral AI rule narrowing contracts.

use std::collections::BTreeSet;
use std::sync::Arc;

use agql_auth::AuthPrincipal;
use async_graphql::{Context, Enum, ErrorExtensions, InputObject, Object, SimpleObject};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{
    AiApprovalRule, AiError, AiProviderKindInput, AiScope, AiScopeInput, DataClassification,
    ProviderKind, ToolMaturity,
};

/// Administrative or runtime action for one hierarchical rule layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AiRuleAction {
    /// Read redacted rule configuration for one exact scope.
    Read,
    /// Create or compare-and-swap an exact scope rule.
    Manage,
    /// Apply a rule during current-principal run planning.
    ResolveForRun,
}

/// Host authorization for exact rule scopes and actions.
///
/// Allowing rule resolution only permits a constraint to narrow a run. It
/// does not grant any tool, resolver, egress, provider, approval, or budget
/// authority.
#[async_trait]
pub trait AiRuleAccessPolicy: Send + Sync {
    /// Evaluates the exact current principal, scope, and action.
    async fn can_access_rule(
        &self,
        principal: &AuthPrincipal,
        scope: &AiScope,
        action: AiRuleAction,
    ) -> bool;
}

/// Host-owned resolver for the broadest-to-most-specific rule lineage.
///
/// Scope kinds and parent relationships are application-defined. The runtime
/// validates bounds, target identity, duplicates, and tenant isolation after
/// this resolver returns. Implementations must derive user-specific layers
/// from the current principal rather than accepting a model/client-authored
/// hierarchy.
#[async_trait]
pub trait AiRuleHierarchyResolver: Send + Sync {
    /// Resolves the complete lineage ending at `target_scope`.
    ///
    /// # Errors
    ///
    /// Returns an error when current application hierarchy state cannot be
    /// resolved exactly. Callers must fail closed rather than omit a layer.
    async fn hierarchy(
        &self,
        principal: &AuthPrincipal,
        target_scope: &AiScope,
    ) -> Result<Vec<AiScope>, AiError>;
}

/// GraphQL classification ceiling. Secret material is deliberately absent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_items = "PascalCase"))]
pub enum AiRuleClassificationInput {
    /// Public data only.
    Public,
    /// Internal data or lower.
    Internal,
    /// Confidential data or lower.
    Confidential,
    /// Restricted data or lower; secrets remain impossible.
    Restricted,
}

impl From<AiRuleClassificationInput> for DataClassification {
    fn from(value: AiRuleClassificationInput) -> Self {
        match value {
            AiRuleClassificationInput::Public => Self::Public,
            AiRuleClassificationInput::Internal => Self::Internal,
            AiRuleClassificationInput::Confidential => Self::Confidential,
            AiRuleClassificationInput::Restricted => Self::Restricted,
        }
    }
}

/// GraphQL tool-maturity ceiling. Autonomous writes are deliberately absent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_items = "PascalCase"))]
pub enum AiRuleToolMaturityInput {
    /// Read-only application tools.
    ReadOnly,
    /// Read-only tools plus AI-owned proposals.
    ProposalOnly,
    /// Explicitly registered supervised writes with independent approval.
    SupervisedWrite,
}

impl From<AiRuleToolMaturityInput> for ToolMaturity {
    fn from(value: AiRuleToolMaturityInput) -> Self {
        match value {
            AiRuleToolMaturityInput::ReadOnly => Self::ReadOnly,
            AiRuleToolMaturityInput::ProposalOnly => Self::ProposalOnly,
            AiRuleToolMaturityInput::SupervisedWrite => Self::SupervisedWrite,
        }
    }
}

/// Minimum approval behavior imposed by a rule hierarchy.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Enum,
)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_items = "PascalCase"))]
pub enum AiRuleApprovalRequirement {
    /// Preserve the registered descriptor and current host approval policy.
    DescriptorPolicy,
    /// Require a one-shot approval for every otherwise allowed application
    /// tool, including read-only tools.
    OneShotForAllApplicationTools,
    /// No application tool is model-callable under this rule set.
    NeverApplicationTools,
}

impl AiRuleApprovalRequirement {
    /// Stable persistence value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DescriptorPolicy => "descriptor_policy",
            Self::OneShotForAllApplicationTools => "one_shot_for_all_application_tools",
            Self::NeverApplicationTools => "never_application_tools",
        }
    }

    /// Applies this minimum to an independently registered tool rule.
    pub const fn apply(self, descriptor: AiApprovalRule) -> AiApprovalRule {
        match self {
            Self::DescriptorPolicy => descriptor,
            Self::OneShotForAllApplicationTools => match descriptor {
                AiApprovalRule::Never => AiApprovalRule::Never,
                AiApprovalRule::None | AiApprovalRule::Policy | AiApprovalRule::OneShot => {
                    AiApprovalRule::OneShot
                }
            },
            Self::NeverApplicationTools => AiApprovalRule::Never,
        }
    }
}

/// Optional provider capability constrained by hierarchical rules.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Enum,
)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_items = "PascalCase"))]
pub enum AiRuleProviderCapability {
    /// Streaming model output.
    Streaming,
    /// Image input.
    ImageInput,
    /// File input.
    FileInput,
    /// Custom application tools.
    CustomTools,
    /// Parallel custom application tools.
    ParallelToolCalls,
    /// JSON-schema structured output.
    StructuredOutput,
    /// Provider web search.
    WebSearch,
    /// Provider file search or retained file indexes.
    FileSearch,
    /// Provider code execution.
    CodeExecution,
    /// Provider image generation.
    ImageGeneration,
    /// Embedding generation.
    Embeddings,
    /// Background provider execution or webhooks.
    Background,
    /// Provider-retained response continuation.
    ProviderRetainedContinuation,
    /// Provider-independent stateless conversation replay.
    StatelessContinuation,
}

impl AiRuleProviderCapability {
    /// Stable persistence value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Streaming => "streaming",
            Self::ImageInput => "image_input",
            Self::FileInput => "file_input",
            Self::CustomTools => "custom_tools",
            Self::ParallelToolCalls => "parallel_tool_calls",
            Self::StructuredOutput => "structured_output",
            Self::WebSearch => "web_search",
            Self::FileSearch => "file_search",
            Self::CodeExecution => "code_execution",
            Self::ImageGeneration => "image_generation",
            Self::Embeddings => "embeddings",
            Self::Background => "background",
            Self::ProviderRetainedContinuation => "provider_retained_continuation",
            Self::StatelessContinuation => "stateless_continuation",
        }
    }
}

/// Optional run ceilings contributed by one rule layer.
///
/// `None` means that this layer adds no ceiling for that dimension; it never
/// removes a ceiling established by the deployment or a broader layer. A
/// value of zero explicitly permits no capacity in that dimension.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiRuleBudgetCeilings {
    /// Maximum model/tool steps.
    pub maximum_steps: Option<u64>,
    /// Maximum wall-clock duration in seconds.
    pub maximum_duration_seconds: Option<u64>,
    /// Maximum total output tokens.
    pub maximum_output_tokens: Option<u64>,
    /// Maximum deployment-defined cost in microunits.
    pub maximum_cost_microunits: Option<u64>,
    /// Maximum provider calls.
    pub maximum_provider_calls: Option<u64>,
    /// Maximum provider/application tool units.
    pub maximum_tool_units: Option<u64>,
    /// Maximum image units.
    pub maximum_image_units: Option<u64>,
}

impl AiRuleBudgetCeilings {
    fn validate(&self) -> Result<(), AiError> {
        if self.maximum_steps.is_some_and(|value| value > 10_000)
            || self
                .maximum_duration_seconds
                .is_some_and(|value| value > 604_800)
            || self
                .maximum_output_tokens
                .is_some_and(|value| value > 100_000_000)
            || self
                .maximum_cost_microunits
                .is_some_and(|value| value > 1_000_000_000_000_000)
            || self
                .maximum_provider_calls
                .is_some_and(|value| value > 10_000)
            || self
                .maximum_tool_units
                .is_some_and(|value| value > 1_000_000)
            || self
                .maximum_image_units
                .is_some_and(|value| value > 1_000_000)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid hierarchical rule budget ceilings".to_owned(),
            ));
        }
        Ok(())
    }

    fn narrow(&self, other: &Self) -> Self {
        Self {
            maximum_steps: minimum_optional(self.maximum_steps, other.maximum_steps),
            maximum_duration_seconds: minimum_optional(
                self.maximum_duration_seconds,
                other.maximum_duration_seconds,
            ),
            maximum_output_tokens: minimum_optional(
                self.maximum_output_tokens,
                other.maximum_output_tokens,
            ),
            maximum_cost_microunits: minimum_optional(
                self.maximum_cost_microunits,
                other.maximum_cost_microunits,
            ),
            maximum_provider_calls: minimum_optional(
                self.maximum_provider_calls,
                other.maximum_provider_calls,
            ),
            maximum_tool_units: minimum_optional(self.maximum_tool_units, other.maximum_tool_units),
            maximum_image_units: minimum_optional(
                self.maximum_image_units,
                other.maximum_image_units,
            ),
        }
    }
}

/// Complete constraints contributed by a deployment or one stored rule layer.
///
/// An absent allowlist means this layer does not constrain that dimension. It
/// does not grant access: ordinary registration, enablement, authorization,
/// egress, approval, and budget checks remain independently mandatory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiRuleConstraints {
    /// Whether AI remains enabled under this layer.
    pub enabled: bool,
    /// Maximum model-facing data classification.
    pub maximum_classification: DataClassification,
    /// Maximum application-tool maturity.
    pub maximum_tool_maturity: ToolMaturity,
    /// Minimum independent approval behavior.
    pub approval_requirement: AiRuleApprovalRequirement,
    /// Optional exact tool-descriptor fingerprint allowlist.
    pub allowed_tool_fingerprints: Option<BTreeSet<String>>,
    /// Optional provider-family allowlist.
    pub allowed_provider_kinds: Option<BTreeSet<ProviderKind>>,
    /// Optional provider-capability allowlist.
    pub allowed_provider_capabilities: Option<BTreeSet<AiRuleProviderCapability>>,
    /// Whether provider-retained content/state remains eligible for separate
    /// egress authorization.
    pub allow_provider_retention: bool,
    /// Whether user-owned provider credentials remain eligible under separate
    /// secret/configuration policy.
    pub allow_byok: bool,
    /// Run ceilings.
    pub budget: AiRuleBudgetCeilings,
}

impl AiRuleConstraints {
    /// Validates bounded, non-secret, non-autonomous constraints.
    ///
    /// # Errors
    ///
    /// Returns an error for secret classification, autonomous-write maturity,
    /// malformed tool fingerprints, oversized provider/capability allowlists,
    /// invalid capability combinations, or invalid budget ceilings.
    pub fn validate(&self) -> Result<(), AiError> {
        if self.maximum_classification == DataClassification::Secret
            || self.maximum_tool_maturity == ToolMaturity::AutonomousWrite
        {
            return Err(AiError::InvalidConfiguration(
                "hierarchical rules cannot permit secrets or autonomous writes".to_owned(),
            ));
        }
        if self
            .allowed_tool_fingerprints
            .as_ref()
            .is_some_and(|values| {
                values.len() > 256
                    || values.iter().any(|value| {
                        value.len() != 64
                            || !value
                                .bytes()
                                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    })
            })
            || self
                .allowed_provider_kinds
                .as_ref()
                .is_some_and(|values| values.len() > 16)
            || self
                .allowed_provider_capabilities
                .as_ref()
                .is_some_and(|values| values.len() > 32)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid hierarchical rule allowlist".to_owned(),
            ));
        }
        if self
            .allowed_provider_capabilities
            .as_ref()
            .is_some_and(|values| {
                values.contains(&AiRuleProviderCapability::ParallelToolCalls)
                    && !values.contains(&AiRuleProviderCapability::CustomTools)
            })
        {
            return Err(AiError::InvalidConfiguration(
                "parallel tools require custom-tool capability".to_owned(),
            ));
        }
        self.budget.validate()
    }

    /// Intersects two independently valid constraint sets.
    ///
    /// # Errors
    ///
    /// Returns an error when either input violates the rule contract.
    pub fn narrow(&self, other: &Self) -> Result<Self, AiError> {
        self.validate()?;
        other.validate()?;
        let narrowed = Self {
            enabled: self.enabled && other.enabled,
            maximum_classification: self
                .maximum_classification
                .min(other.maximum_classification),
            maximum_tool_maturity: self.maximum_tool_maturity.min(other.maximum_tool_maturity),
            approval_requirement: self.approval_requirement.max(other.approval_requirement),
            allowed_tool_fingerprints: intersect_optional_sets(
                &self.allowed_tool_fingerprints,
                &other.allowed_tool_fingerprints,
            ),
            allowed_provider_kinds: intersect_optional_sets(
                &self.allowed_provider_kinds,
                &other.allowed_provider_kinds,
            ),
            allowed_provider_capabilities: intersect_optional_sets(
                &self.allowed_provider_capabilities,
                &other.allowed_provider_capabilities,
            ),
            allow_provider_retention: self.allow_provider_retention
                && other.allow_provider_retention,
            allow_byok: self.allow_byok && other.allow_byok,
            budget: self.budget.narrow(&other.budget),
        };
        narrowed.validate()?;
        Ok(narrowed)
    }

    /// Returns whether this set is no broader than `ceiling` in every
    /// dimension.
    ///
    /// # Errors
    ///
    /// Returns an error when either set is invalid.
    pub fn is_no_broader_than(&self, ceiling: &Self) -> Result<bool, AiError> {
        self.validate()?;
        ceiling.validate()?;
        Ok((!self.enabled || ceiling.enabled)
            && self.maximum_classification <= ceiling.maximum_classification
            && self.maximum_tool_maturity <= ceiling.maximum_tool_maturity
            && self.approval_requirement >= ceiling.approval_requirement
            && optional_set_is_subset(
                &self.allowed_tool_fingerprints,
                &ceiling.allowed_tool_fingerprints,
            )
            && optional_set_is_subset(
                &self.allowed_provider_kinds,
                &ceiling.allowed_provider_kinds,
            )
            && optional_set_is_subset(
                &self.allowed_provider_capabilities,
                &ceiling.allowed_provider_capabilities,
            )
            && (!self.allow_provider_retention || ceiling.allow_provider_retention)
            && (!self.allow_byok || ceiling.allow_byok)
            && budget_is_no_broader(&self.budget, &ceiling.budget))
    }
}

/// Deployment-owned hard bounds for hierarchy resolution and GraphQL writes.
///
/// The deployment ceiling is immutable process configuration and cannot be
/// introduced or widened through GraphQL.
#[derive(Clone, Debug)]
pub struct AiRuleDeploymentLimits {
    maximum_hierarchy_depth: usize,
    ceiling: AiRuleConstraints,
}

impl AiRuleDeploymentLimits {
    /// Creates validated deployment rule limits.
    ///
    /// # Errors
    ///
    /// Returns an error unless depth is in `1..=16` and the ceiling is valid.
    pub fn new(
        maximum_hierarchy_depth: usize,
        ceiling: AiRuleConstraints,
    ) -> Result<Self, AiError> {
        if !(1..=16).contains(&maximum_hierarchy_depth) {
            return Err(AiError::InvalidConfiguration(
                "invalid rule hierarchy depth".to_owned(),
            ));
        }
        ceiling.validate()?;
        Ok(Self {
            maximum_hierarchy_depth,
            ceiling,
        })
    }

    /// Maximum broad-to-specific layers.
    pub const fn maximum_hierarchy_depth(&self) -> usize {
        self.maximum_hierarchy_depth
    }

    /// Immutable deployment constraint ceiling.
    pub fn ceiling(&self) -> &AiRuleConstraints {
        &self.ceiling
    }
}

/// One exact persisted layer/version contributing to a resolved rule set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiAppliedRuleLayer {
    /// Exact scope.
    pub scope: AiScope,
    /// Compare-and-swap version read during resolution.
    pub row_version: i64,
}

/// Effective intersection of deployment and every host-resolved rule layer.
///
/// This value proves only rule narrowing and exact layer versions. It never
/// proves current resource authorization, tool enablement, egress approval,
/// provider routing, spend reservation, or one-shot approval consumption.
#[derive(Clone, Debug)]
pub struct AiResolvedRuleSet {
    target_scope: AiScope,
    constraints: AiRuleConstraints,
    applied_layers: Vec<AiAppliedRuleLayer>,
    fingerprint: String,
}

/// Fresh current-principal rule resolution used at a durable agent boundary.
///
/// The contained rule set is still only constraint evidence. The evaluation
/// timestamp lets bounded coordinators enforce elapsed-time ceilings without
/// trusting wall-clock values supplied by a model or planner.
#[derive(Clone, Debug)]
pub struct AiAgentRuleResolution {
    rules: AiResolvedRuleSet,
    evaluated_at: OffsetDateTime,
}

impl AiAgentRuleResolution {
    /// Creates a rule resolution from a trusted current-principal resolver.
    ///
    /// Alternative resolver implementations must use their trusted clock and
    /// must not accept this timestamp from GraphQL or model input.
    ///
    /// # Errors
    ///
    /// Returns an error when the timestamp cannot be represented as a whole
    /// Unix second or the rule fingerprint is malformed.
    pub fn new(rules: AiResolvedRuleSet, evaluated_at: OffsetDateTime) -> Result<Self, AiError> {
        let evaluated_at = OffsetDateTime::from_unix_timestamp(evaluated_at.unix_timestamp())
            .map_err(|_| {
                AiError::InvalidConfiguration("invalid rule evaluation time".to_owned())
            })?;
        if rules.fingerprint.len() != 64
            || !rules
                .fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(AiError::InvalidConfiguration(
                "invalid resolved rule fingerprint".to_owned(),
            ));
        }
        Ok(Self {
            rules,
            evaluated_at,
        })
    }

    /// Current effective constraint evidence.
    pub fn rules(&self) -> &AiResolvedRuleSet {
        &self.rules
    }

    /// Trusted whole-second evaluation time.
    pub const fn evaluated_at(&self) -> OffsetDateTime {
        self.evaluated_at
    }
}

/// Cumulative rule-budget usage bound into coordinator checkpoints.
///
/// Values are derived only from authoritative provider usage and exact durable
/// application-tool counts. They do not replace atomic budget reservations or
/// authoritative pricing settlement.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiRuleRunUsage {
    started_at_unix: Option<i64>,
    provider_calls: u64,
    steps: u64,
    output_tokens: u64,
    cost_microunits: u64,
    tool_units: u64,
    image_units: u64,
}

impl AiRuleRunUsage {
    /// Trusted first rule-evaluation time, when execution has started.
    pub const fn started_at_unix(self) -> Option<i64> {
        self.started_at_unix
    }

    /// Accepted provider-call count.
    pub const fn provider_calls(self) -> u64 {
        self.provider_calls
    }

    /// Accepted provider turns plus exact application-tool calls.
    pub const fn steps(self) -> u64 {
        self.steps
    }

    /// Authoritative cumulative output tokens.
    pub const fn output_tokens(self) -> u64 {
        self.output_tokens
    }

    /// Authoritative cumulative deployment-defined cost.
    pub const fn cost_microunits(self) -> u64 {
        self.cost_microunits
    }

    /// Authoritative cumulative provider/tool billing units.
    pub const fn tool_units(self) -> u64 {
        self.tool_units
    }

    /// Authoritative cumulative image billing units.
    pub const fn image_units(self) -> u64 {
        self.image_units
    }

    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) fn projected_provider(
        self,
        estimated: crate::AiBudgetAmounts,
        resolution: &AiAgentRuleResolution,
    ) -> Result<Self, AiError> {
        self.add_provider(estimated, resolution)
    }

    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) fn accept_provider(
        self,
        actual: crate::AiBudgetAmounts,
        resolution: &AiAgentRuleResolution,
    ) -> Result<Self, AiError> {
        self.add_provider(actual, resolution)
    }

    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) fn accept_tool_calls(
        mut self,
        call_count: usize,
        resolution: &AiAgentRuleResolution,
    ) -> Result<Self, AiError> {
        self.ensure_identity(resolution)?;
        self.steps = self
            .steps
            .checked_add(u64::try_from(call_count).map_err(|_| AiError::BudgetDenied)?)
            .ok_or(AiError::BudgetDenied)?;
        self.validate(resolution)
    }

    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) fn validate(mut self, resolution: &AiAgentRuleResolution) -> Result<Self, AiError> {
        self.ensure_identity(resolution)?;
        let constraints = resolution.rules().constraints();
        let elapsed = resolution
            .evaluated_at()
            .unix_timestamp()
            .checked_sub(self.started_at_unix.ok_or(AiError::BudgetDenied)?)
            .filter(|elapsed| *elapsed >= 0)
            .and_then(|elapsed| u64::try_from(elapsed).ok())
            .ok_or(AiError::BudgetDenied)?;
        let within = |value: u64, ceiling: Option<u64>| ceiling.is_none_or(|limit| value <= limit);
        if !constraints.enabled
            || !within(
                self.provider_calls,
                constraints.budget.maximum_provider_calls,
            )
            || !within(self.steps, constraints.budget.maximum_steps)
            || !within(elapsed, constraints.budget.maximum_duration_seconds)
            || !within(self.output_tokens, constraints.budget.maximum_output_tokens)
            || !within(
                self.cost_microunits,
                constraints.budget.maximum_cost_microunits,
            )
            || !within(self.tool_units, constraints.budget.maximum_tool_units)
            || !within(self.image_units, constraints.budget.maximum_image_units)
        {
            return Err(AiError::BudgetDenied);
        }
        Ok(self)
    }

    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    fn add_provider(
        mut self,
        amounts: crate::AiBudgetAmounts,
        resolution: &AiAgentRuleResolution,
    ) -> Result<Self, AiError> {
        if amounts.runs != 1 {
            return Err(AiError::BudgetDenied);
        }
        self.ensure_identity(resolution)?;
        self.provider_calls = self
            .provider_calls
            .checked_add(1)
            .ok_or(AiError::BudgetDenied)?;
        self.steps = self.steps.checked_add(1).ok_or(AiError::BudgetDenied)?;
        self.output_tokens = self
            .output_tokens
            .checked_add(amounts.output_tokens)
            .ok_or(AiError::BudgetDenied)?;
        self.cost_microunits = self
            .cost_microunits
            .checked_add(amounts.cost_microunits)
            .ok_or(AiError::BudgetDenied)?;
        self.tool_units = self
            .tool_units
            .checked_add(amounts.tool_units)
            .ok_or(AiError::BudgetDenied)?;
        self.image_units = self
            .image_units
            .checked_add(amounts.image_units)
            .ok_or(AiError::BudgetDenied)?;
        self.validate(resolution)
    }

    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    fn ensure_identity(&mut self, resolution: &AiAgentRuleResolution) -> Result<(), AiError> {
        let evaluated_at = resolution.evaluated_at().unix_timestamp();
        match self.started_at_unix {
            Some(started_at) if started_at > evaluated_at => Err(AiError::BudgetDenied),
            Some(_) => Ok(()),
            None if self.provider_calls == 0
                && self.steps == 0
                && self.output_tokens == 0
                && self.cost_microunits == 0
                && self.tool_units == 0
                && self.image_units == 0 =>
            {
                self.started_at_unix = Some(evaluated_at);
                Ok(())
            }
            None => Err(AiError::BudgetDenied),
        }
    }
}

impl AiResolvedRuleSet {
    /// Exact target scope.
    pub fn target_scope(&self) -> &AiScope {
        &self.target_scope
    }

    /// Effective rule constraints.
    pub fn constraints(&self) -> &AiRuleConstraints {
        &self.constraints
    }

    /// Broadest-to-most-specific persisted layer versions.
    pub fn applied_layers(&self) -> &[AiAppliedRuleLayer] {
        &self.applied_layers
    }

    /// Canonical SHA-256 fingerprint of the target, effective constraints, and
    /// exact applied versions.
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// Evaluates only the hierarchical rule constraints for one registered
    /// tool fingerprint/maturity/approval rule.
    ///
    /// `None` means rules reject the tool. A returned approval rule is still
    /// subject to ordinary tool authorization and one-shot approval handling.
    pub fn constrain_tool(
        &self,
        descriptor_fingerprint: &str,
        maturity: ToolMaturity,
        descriptor_approval: AiApprovalRule,
    ) -> Option<AiApprovalRule> {
        if !self.constraints.enabled
            || maturity > self.constraints.maximum_tool_maturity
            || self
                .constraints
                .allowed_tool_fingerprints
                .as_ref()
                .is_some_and(|values| !values.contains(descriptor_fingerprint))
        {
            return None;
        }
        let approval = self
            .constraints
            .approval_requirement
            .apply(descriptor_approval);
        (approval != AiApprovalRule::Never).then_some(approval)
    }

    /// Evaluates only hierarchical provider, capability, classification,
    /// retention, and BYOK constraints.
    ///
    /// A `true` result grants no provider or disclosure authority. The caller
    /// must still prove current principal, route, egress, budget reservation,
    /// profile, attachment, and provider-specific requirements.
    #[allow(clippy::too_many_arguments)]
    pub fn permits_provider_request(
        &self,
        provider_kind: &ProviderKind,
        capabilities: &BTreeSet<AiRuleProviderCapability>,
        classification: DataClassification,
        uses_provider_retention: bool,
        uses_byok: bool,
    ) -> bool {
        self.constraints.enabled
            && classification <= self.constraints.maximum_classification
            && self
                .constraints
                .allowed_provider_kinds
                .as_ref()
                .is_none_or(|values| values.contains(provider_kind))
            && self
                .constraints
                .allowed_provider_capabilities
                .as_ref()
                .is_none_or(|values| capabilities.is_subset(values))
            && (!uses_provider_retention || self.constraints.allow_provider_retention)
            && (!uses_byok || self.constraints.allow_byok)
    }

    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) fn new(
        target_scope: AiScope,
        constraints: AiRuleConstraints,
        applied_layers: Vec<AiAppliedRuleLayer>,
        fingerprint: String,
    ) -> Self {
        Self {
            target_scope,
            constraints,
            applied_layers,
            fingerprint,
        }
    }
}

/// GraphQL budget ceilings for one exact rule layer.
///
/// Absence inherits the effective broader ceiling. Zero explicitly permits no
/// capacity in that dimension.
#[derive(Clone, Debug, Default, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiRuleBudgetInput {
    /// Optional maximum steps; absence inherits broader limits.
    pub maximum_steps: Option<i64>,
    /// Optional maximum duration seconds.
    pub maximum_duration_seconds: Option<i64>,
    /// Optional maximum output tokens.
    pub maximum_output_tokens: Option<i64>,
    /// Optional maximum cost microunits.
    pub maximum_cost_microunits: Option<i64>,
    /// Optional maximum provider calls.
    pub maximum_provider_calls: Option<i64>,
    /// Optional maximum tool units.
    pub maximum_tool_units: Option<i64>,
    /// Optional maximum image units.
    pub maximum_image_units: Option<i64>,
}

/// GraphQL compare-and-swap input for one exact rule layer.
#[derive(Clone, Debug, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct SetAiRulePolicyInput {
    /// Exact application-defined scope.
    pub scope: AiScopeInput,
    /// Whether AI remains eligible at this layer.
    pub enabled: bool,
    /// Maximum model-facing classification.
    pub maximum_classification: AiRuleClassificationInput,
    /// Maximum tool maturity.
    pub maximum_tool_maturity: AiRuleToolMaturityInput,
    /// Minimum approval behavior.
    pub approval_requirement: AiRuleApprovalRequirement,
    /// Optional exact tool-fingerprint allowlist; absence inherits, empty
    /// denies every tool.
    pub allowed_tool_fingerprints: Option<Vec<String>>,
    /// Optional provider-family allowlist; absence inherits, empty denies all.
    pub allowed_provider_kinds: Option<Vec<AiProviderKindInput>>,
    /// Optional provider-capability allowlist; absence inherits.
    pub allowed_provider_capabilities: Option<Vec<AiRuleProviderCapability>>,
    /// Whether separately authorized provider retention remains eligible.
    pub allow_provider_retention: bool,
    /// Whether separately authorized BYOK profiles remain eligible.
    pub allow_byok: bool,
    /// Optional run ceilings.
    pub budget: AiRuleBudgetInput,
    /// Expected CAS version, absent only when creating the scope layer.
    pub expected_version: Option<i64>,
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
impl SetAiRulePolicyInput {
    pub(crate) fn into_scope_and_constraints(
        self,
    ) -> Result<(AiScope, AiRuleConstraints, Option<i64>), AiError> {
        let scope = self.scope.into();
        let constraints = AiRuleConstraints {
            enabled: self.enabled,
            maximum_classification: self.maximum_classification.into(),
            maximum_tool_maturity: self.maximum_tool_maturity.into(),
            approval_requirement: self.approval_requirement,
            allowed_tool_fingerprints: normalize_optional_set(
                self.allowed_tool_fingerprints,
                "tool fingerprints",
            )?,
            allowed_provider_kinds: normalize_optional_provider_kinds(self.allowed_provider_kinds)?,
            allowed_provider_capabilities: normalize_optional_set(
                self.allowed_provider_capabilities,
                "provider capabilities",
            )?,
            allow_provider_retention: self.allow_provider_retention,
            allow_byok: self.allow_byok,
            budget: AiRuleBudgetCeilings {
                maximum_steps: nonnegative(self.budget.maximum_steps)?,
                maximum_duration_seconds: nonnegative(self.budget.maximum_duration_seconds)?,
                maximum_output_tokens: nonnegative(self.budget.maximum_output_tokens)?,
                maximum_cost_microunits: nonnegative(self.budget.maximum_cost_microunits)?,
                maximum_provider_calls: nonnegative(self.budget.maximum_provider_calls)?,
                maximum_tool_units: nonnegative(self.budget.maximum_tool_units)?,
                maximum_image_units: nonnegative(self.budget.maximum_image_units)?,
            },
        };
        constraints.validate().map_err(|_| {
            AiError::InvalidInput("invalid hierarchical rule constraints".to_owned())
        })?;
        if self.expected_version.is_some_and(|value| value < 0) {
            return Err(AiError::InvalidInput(
                "invalid hierarchical rule CAS version".to_owned(),
            ));
        }
        Ok((scope, constraints, self.expected_version))
    }
}

/// Redacted GraphQL view of one exact stored rule layer.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiRulePolicyView {
    /// Scope kind.
    pub scope_kind: String,
    /// Scope identifier.
    pub scope_id: String,
    /// Optional tenant boundary.
    pub tenant_id: Option<String>,
    /// Whether AI remains enabled at this layer.
    pub enabled: bool,
    /// Maximum classification.
    pub maximum_classification: String,
    /// Maximum tool maturity.
    pub maximum_tool_maturity: String,
    /// Minimum approval behavior.
    pub approval_requirement: String,
    /// Optional exact tool fingerprints.
    pub allowed_tool_fingerprints: Option<Vec<String>>,
    /// Optional provider kinds.
    pub allowed_provider_kinds: Option<Vec<String>>,
    /// Optional provider capabilities.
    pub allowed_provider_capabilities: Option<Vec<String>>,
    /// Whether provider retention remains eligible.
    pub allow_provider_retention: bool,
    /// Whether BYOK remains eligible.
    pub allow_byok: bool,
    /// Optional maximum steps.
    pub maximum_steps: Option<u64>,
    /// Optional maximum duration seconds.
    pub maximum_duration_seconds: Option<u64>,
    /// Optional maximum output tokens.
    pub maximum_output_tokens: Option<u64>,
    /// Optional maximum cost microunits.
    pub maximum_cost_microunits: Option<u64>,
    /// Optional maximum provider calls.
    pub maximum_provider_calls: Option<u64>,
    /// Optional maximum tool units.
    pub maximum_tool_units: Option<u64>,
    /// Optional maximum image units.
    pub maximum_image_units: Option<u64>,
    /// CAS version.
    pub row_version: i64,
    /// Update timestamp in Unix seconds.
    pub updated_at: i64,
}

/// Authenticated GraphQL management and trusted runtime resolution service.
#[async_trait]
pub trait AiRulePolicyService: Send + Sync {
    /// Loads one redacted exact-scope layer.
    async fn policy(
        &self,
        principal: &AuthPrincipal,
        scope: AiScope,
    ) -> Result<Option<AiRulePolicyView>, AiError>;

    /// Creates or compare-and-swap updates one layer under deployment limits.
    async fn set_policy(
        &self,
        principal: &AuthPrincipal,
        input: SetAiRulePolicyInput,
    ) -> Result<AiRulePolicyView, AiError>;

    /// Resolves and intersects the complete host-authored hierarchy.
    async fn resolve_for_run(
        &self,
        principal: &AuthPrincipal,
        target_scope: AiScope,
    ) -> Result<AiResolvedRuleSet, AiError>;
}

/// Composable redacted hierarchical-rule query root.
#[derive(Clone, Copy, Debug, Default)]
pub struct AiRuleQueryRoot;

#[cfg_attr(
    feature = "graphql-case-pascal",
    Object(rename_fields = "PascalCase", rename_args = "PascalCase")
)]
#[cfg_attr(not(feature = "graphql-case-pascal"), Object)]
impl AiRuleQueryRoot {
    /// Loads one redacted exact-scope rule layer.
    async fn ai_rule_policy(
        &self,
        context: &Context<'_>,
        scope: AiScopeInput,
    ) -> async_graphql::Result<Option<AiRulePolicyView>> {
        let principal = agql_auth::principal_from_ctx(context)?;
        rule_service(context)?
            .policy(&principal, scope.into())
            .await
            .map_err(extend)
    }
}

/// Composable hierarchical-rule mutation root.
#[derive(Clone, Copy, Debug, Default)]
pub struct AiRuleMutationRoot;

#[cfg_attr(
    feature = "graphql-case-pascal",
    Object(rename_fields = "PascalCase", rename_args = "PascalCase")
)]
#[cfg_attr(not(feature = "graphql-case-pascal"), Object)]
impl AiRuleMutationRoot {
    /// Creates or compare-and-swap updates one exact rule layer.
    async fn set_ai_rule_policy(
        &self,
        context: &Context<'_>,
        input: SetAiRulePolicyInput,
    ) -> async_graphql::Result<AiRulePolicyView> {
        let principal = agql_auth::principal_from_ctx(context)?;
        rule_service(context)?
            .set_policy(&principal, input)
            .await
            .map_err(extend)
    }
}

fn rule_service(context: &Context<'_>) -> async_graphql::Result<Arc<dyn AiRulePolicyService>> {
    context
        .data_opt::<Arc<dyn AiRulePolicyService>>()
        .cloned()
        .ok_or_else(|| {
            AiError::InvalidConfiguration("AI rule policy service is missing".to_owned()).extend()
        })
}

fn extend(error: AiError) -> async_graphql::Error {
    error.extend()
}

fn minimum_optional(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn intersect_optional_sets<T: Clone + Ord>(
    left: &Option<BTreeSet<T>>,
    right: &Option<BTreeSet<T>>,
) -> Option<BTreeSet<T>> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.intersection(right).cloned().collect()),
        (Some(values), None) | (None, Some(values)) => Some(values.clone()),
        (None, None) => None,
    }
}

fn optional_set_is_subset<T: Ord>(
    value: &Option<BTreeSet<T>>,
    ceiling: &Option<BTreeSet<T>>,
) -> bool {
    match (value, ceiling) {
        (None, _) | (Some(_), None) => true,
        (Some(value), Some(ceiling)) => value.is_subset(ceiling),
    }
}

fn budget_is_no_broader(value: &AiRuleBudgetCeilings, ceiling: &AiRuleBudgetCeilings) -> bool {
    let within = |value: Option<u64>, ceiling: Option<u64>| match (value, ceiling) {
        (None, _) | (Some(_), None) => true,
        (Some(value), Some(ceiling)) => value <= ceiling,
    };
    within(value.maximum_steps, ceiling.maximum_steps)
        && within(
            value.maximum_duration_seconds,
            ceiling.maximum_duration_seconds,
        )
        && within(value.maximum_output_tokens, ceiling.maximum_output_tokens)
        && within(
            value.maximum_cost_microunits,
            ceiling.maximum_cost_microunits,
        )
        && within(value.maximum_provider_calls, ceiling.maximum_provider_calls)
        && within(value.maximum_tool_units, ceiling.maximum_tool_units)
        && within(value.maximum_image_units, ceiling.maximum_image_units)
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn normalize_optional_set<T: Ord>(
    values: Option<Vec<T>>,
    field: &str,
) -> Result<Option<BTreeSet<T>>, AiError> {
    let Some(values) = values else {
        return Ok(None);
    };
    let original_len = values.len();
    let values = values.into_iter().collect::<BTreeSet<_>>();
    if values.len() != original_len {
        return Err(AiError::InvalidInput(format!(
            "duplicate hierarchical rule {field}"
        )));
    }
    Ok(Some(values))
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn normalize_optional_provider_kinds(
    values: Option<Vec<AiProviderKindInput>>,
) -> Result<Option<BTreeSet<ProviderKind>>, AiError> {
    normalize_optional_set(
        values.map(|values| {
            values
                .into_iter()
                .map(|value| match value {
                    AiProviderKindInput::OpenAi => ProviderKind::OpenAi,
                    AiProviderKindInput::Anthropic => ProviderKind::Anthropic,
                    AiProviderKindInput::Xai => ProviderKind::Xai,
                    AiProviderKindInput::Ollama => ProviderKind::Ollama,
                    AiProviderKindInput::OpenAiCompatible => ProviderKind::OpenAiCompatible,
                    AiProviderKindInput::LocalHarness => ProviderKind::LocalHarness,
                })
                .collect()
        }),
        "provider kinds",
    )
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn nonnegative(value: Option<i64>) -> Result<Option<u64>, AiError> {
    value
        .map(|value| {
            u64::try_from(value)
                .map_err(|_| AiError::InvalidInput("negative hierarchical rule budget".to_owned()))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules() -> AiResolvedRuleSet {
        AiResolvedRuleSet::new(
            AiScope::new("test", "rule-usage"),
            AiRuleConstraints {
                enabled: true,
                maximum_classification: DataClassification::Internal,
                maximum_tool_maturity: ToolMaturity::ReadOnly,
                approval_requirement: AiRuleApprovalRequirement::DescriptorPolicy,
                allowed_tool_fingerprints: None,
                allowed_provider_kinds: None,
                allowed_provider_capabilities: None,
                allow_provider_retention: false,
                allow_byok: false,
                budget: AiRuleBudgetCeilings {
                    maximum_steps: Some(2),
                    maximum_duration_seconds: Some(10),
                    maximum_output_tokens: Some(100),
                    maximum_cost_microunits: Some(1_000),
                    maximum_provider_calls: Some(1),
                    maximum_tool_units: Some(2),
                    maximum_image_units: Some(1),
                },
            },
            Vec::new(),
            "a".repeat(64),
        )
    }

    fn resolution(at: i64) -> AiAgentRuleResolution {
        AiAgentRuleResolution::new(
            rules(),
            OffsetDateTime::from_unix_timestamp(at).expect("test time should validate"),
        )
        .expect("test rule resolution should validate")
    }

    #[test]
    fn cumulative_rule_usage_enforces_estimate_actual_steps_and_elapsed_time() {
        let first = resolution(1_800_000_000);
        let started = AiRuleRunUsage::default()
            .validate(&first)
            .expect("zero usage should start at trusted evaluation time");
        let estimated = crate::AiBudgetAmounts {
            output_tokens: 80,
            tool_units: 1,
            image_units: 1,
            cost_microunits: 800,
            runs: 1,
            ..crate::AiBudgetAmounts::default()
        };
        assert_eq!(
            started
                .projected_provider(estimated, &first)
                .expect("estimate should fit")
                .provider_calls(),
            1
        );
        let actual = crate::AiBudgetAmounts {
            output_tokens: 70,
            tool_units: 1,
            cost_microunits: 700,
            runs: 1,
            ..crate::AiBudgetAmounts::default()
        };
        let accepted = started
            .accept_provider(actual, &resolution(1_800_000_004))
            .and_then(|usage| usage.accept_tool_calls(1, &resolution(1_800_000_005)))
            .expect("authoritative usage and one tool should fit");
        assert_eq!(accepted.steps(), 2);
        assert!(matches!(
            accepted.validate(&resolution(1_800_000_011)),
            Err(AiError::BudgetDenied)
        ));
        assert!(matches!(
            started.accept_provider(
                crate::AiBudgetAmounts {
                    output_tokens: 101,
                    runs: 1,
                    ..crate::AiBudgetAmounts::default()
                },
                &first,
            ),
            Err(AiError::BudgetDenied)
        ));
    }
}
