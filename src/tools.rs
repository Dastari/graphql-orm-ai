//! Default-deny GraphQL tool descriptors and policy.

use std::collections::BTreeMap;

use agql_auth::ResolvedPrincipal;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[cfg(any(feature = "sqlite", feature = "postgres"))]
use crate::ModelToolDefinition;
use crate::{AiDisclosureSchema, AiError, AiScope, DataClassification};
use crate::{GraphqlOperationContract, ToolGraphqlRequest};

const JSON_SCHEMA_2020_12: &str = "https://json-schema.org/draft/2020-12/schema";

/// Stable validated tool identifier.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AiToolId(String);

impl AiToolId {
    /// Parses a stable lower-case namespaced tool identifier.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] for an empty or unsafe ID.
    pub fn parse(value: impl Into<String>) -> Result<Self, AiError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'_' | b'-')
            });
        if !valid {
            return Err(AiError::InvalidConfiguration(
                "tool IDs must be lower-case ASCII names".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    /// Returns the identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// GraphQL operation kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiToolOperationKind {
    /// Query operation.
    Query,
    /// Mutation operation.
    Mutation,
    /// Subscription/watch operation.
    Subscription,
    /// AI-owned internal operation such as proposal emission.
    Internal,
}

/// Ownership domain used to prevent recursive AI control-plane invocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiToolOperationDomain {
    /// Host application operation executed through ordinary authorization.
    Application,
    /// AI-owned structured proposal staging operation.
    ProposalStaging,
    /// AI session/configuration/approval/tool-discovery control plane.
    AiControlPlane,
    /// GraphQL schema introspection or discovery operation.
    SchemaIntrospection,
}

/// Rollout maturity ceiling for agent capabilities.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolMaturity {
    /// Read-only application operations.
    ReadOnly,
    /// Writes only AI-owned structured proposals.
    ProposalOnly,
    /// Explicitly registered, supervised application mutation.
    SupervisedWrite,
    /// Future autonomous application writes; disabled by default.
    AutonomousWrite,
}

/// Default risk class.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiToolRisk {
    /// Bounded internal read.
    ReadOnly,
    /// AI-owned proposal staging.
    Proposal,
    /// Proven idempotent low-impact write.
    LowRiskWrite,
    /// Non-idempotent application write.
    NonIdempotentWrite,
    /// Publish, delete, permission, external send, or similar impact.
    HighImpact,
    /// Credential or secret operation; not model-callable by default.
    Secret,
}

/// Approval rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiApprovalRule {
    /// No per-call approval after explicit tool enablement.
    None,
    /// Policy decides using context.
    Policy,
    /// Expiring argument-bound one-shot approval.
    OneShot,
    /// Operation is never model-callable.
    Never,
}

/// Server-authored application tool descriptor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AiToolDescriptor {
    /// Stable ID.
    pub id: AiToolId,
    /// Human/model-facing description without sensitive schema details.
    pub description: String,
    /// Operation kind.
    pub operation_kind: AiToolOperationKind,
    /// Owning operation domain used for recursion prevention.
    pub operation_domain: AiToolOperationDomain,
    /// Server-authored GraphQL document. Empty only for internal tools.
    pub document: String,
    /// JSON Schema 2020-12 argument schema.
    pub argument_schema: serde_json::Value,
    /// Result projection identifier/expression controlled by the server.
    pub result_projection: String,
    /// Exact local/remote GraphQL contract for non-internal tools.
    pub graphql_contract: Option<GraphqlOperationContract>,
    /// Capability maturity.
    pub maturity: ToolMaturity,
    /// Default risk.
    pub risk: AiToolRisk,
    /// Approval rule.
    pub approval: AiApprovalRule,
    /// Maximum result bytes before artifacting/truncation.
    pub maximum_result_bytes: u64,
    /// Maximum result records.
    pub maximum_result_records: u32,
    /// Maximum model-facing data classification.
    pub maximum_classification: DataClassification,
    /// Whether retries are safe with a stable idempotency key.
    pub idempotent: bool,
    /// Stable fingerprint over the complete contract.
    pub fingerprint: String,
}

impl AiToolDescriptor {
    /// Creates a descriptor with secure defaults.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid ID, missing description, or a
    /// non-internal operation without a server-authored document.
    pub fn new(
        id: impl Into<String>,
        description: impl Into<String>,
        operation_kind: AiToolOperationKind,
        document: impl Into<String>,
        argument_schema: serde_json::Value,
    ) -> Result<Self, AiError> {
        let id = AiToolId::parse(id)?;
        let description = description.into();
        let document = document.into();
        if description.trim().is_empty() {
            return Err(AiError::InvalidConfiguration(
                "tool description must not be empty".to_owned(),
            ));
        }
        if operation_kind != AiToolOperationKind::Internal && document.trim().is_empty() {
            return Err(AiError::InvalidConfiguration(
                "GraphQL tools require a server-authored document".to_owned(),
            ));
        }

        let mut descriptor = Self {
            id,
            description,
            operation_kind,
            operation_domain: if operation_kind == AiToolOperationKind::Internal {
                AiToolOperationDomain::ProposalStaging
            } else {
                AiToolOperationDomain::Application
            },
            document,
            argument_schema,
            result_projection: String::new(),
            graphql_contract: None,
            maturity: ToolMaturity::ReadOnly,
            risk: AiToolRisk::ReadOnly,
            approval: AiApprovalRule::None,
            maximum_result_bytes: 64 * 1024,
            maximum_result_records: 100,
            maximum_classification: DataClassification::Internal,
            idempotent: true,
            fingerprint: String::new(),
        };
        descriptor.refresh_fingerprint();
        Ok(descriptor)
    }

    /// Sets maturity and refreshes the fingerprint.
    pub fn with_maturity(mut self, maturity: ToolMaturity) -> Self {
        self.maturity = maturity;
        self.refresh_fingerprint();
        self
    }

    /// Sets risk and approval behavior.
    pub fn with_risk(mut self, risk: AiToolRisk, approval: AiApprovalRule) -> Self {
        self.risk = risk;
        self.approval = approval;
        self.refresh_fingerprint();
        self
    }

    /// Sets a bounded result projection.
    pub fn with_result_projection(mut self, projection: impl Into<String>) -> Self {
        self.result_projection = projection.into();
        self.refresh_fingerprint();
        self
    }

    /// Binds the tool to an exact local/remote target and static operation contract.
    pub fn with_graphql_contract(mut self, contract: GraphqlOperationContract) -> Self {
        self.graphql_contract = Some(contract);
        self.refresh_fingerprint();
        self
    }

    /// Sets the reviewed operation ownership domain.
    pub fn with_operation_domain(mut self, domain: AiToolOperationDomain) -> Self {
        self.operation_domain = domain;
        self.refresh_fingerprint();
        self
    }

    /// Sets output limits.
    pub fn with_output_limits(mut self, bytes: u64, records: u32) -> Self {
        self.maximum_result_bytes = bytes;
        self.maximum_result_records = records;
        self.refresh_fingerprint();
        self
    }

    fn refresh_fingerprint(&mut self) {
        self.fingerprint.clear();
        let encoded = serde_json::to_vec(self)
            .expect("AiToolDescriptor consists only of serializable values");
        self.fingerprint = hex::encode(Sha256::digest(encoded));
    }
}

/// Registered tool catalog. Registration does not enable tools.
#[derive(Clone, Debug, Default)]
pub struct AiToolCatalog {
    tools: BTreeMap<AiToolId, RegisteredAiTool>,
}

#[derive(Clone, Debug)]
struct RegisteredAiTool {
    descriptor: AiToolDescriptor,
    disclosure_schema: Option<AiDisclosureSchema>,
}

impl AiToolCatalog {
    /// Creates an empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a descriptor without exposing it.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::AlreadyExists`] for duplicate stable IDs.
    pub fn register(&mut self, descriptor: AiToolDescriptor) -> Result<(), AiError> {
        if descriptor.operation_kind != AiToolOperationKind::Internal {
            return Err(AiError::InvalidConfiguration(
                "application tools require a static disclosure schema".to_owned(),
            ));
        }
        self.register_validated(descriptor, None)
    }

    /// Registers a GraphQL tool with its exact static disclosure schema.
    /// Registration remains discovery only and does not enable the tool.
    ///
    /// # Errors
    ///
    /// Returns a safe error for duplicate IDs, forbidden operation domains,
    /// introspection/AI-control-plane documents, or stale contract bindings.
    pub fn register_with_disclosure(
        &mut self,
        descriptor: AiToolDescriptor,
        disclosure_schema: AiDisclosureSchema,
    ) -> Result<(), AiError> {
        if descriptor.operation_kind == AiToolOperationKind::Internal {
            return Err(AiError::InvalidConfiguration(
                "internal tools do not accept GraphQL disclosure schemas".to_owned(),
            ));
        }
        let contract = descriptor.graphql_contract.as_ref().ok_or_else(|| {
            AiError::InvalidConfiguration(
                "application tools require an exact GraphQL operation contract".to_owned(),
            )
        })?;
        if contract.disclosure_schema_fingerprint != disclosure_schema.fingerprint
            || contract.operation_name.trim().is_empty()
            || descriptor.result_projection.trim().is_empty()
            || disclosure_schema.maximum_list_bound() > descriptor.maximum_result_records
        {
            return Err(AiError::InvalidConfiguration(
                "tool disclosure or projection contract is stale".to_owned(),
            ));
        }
        self.register_validated(descriptor, Some(disclosure_schema))
    }

    fn register_validated(
        &mut self,
        descriptor: AiToolDescriptor,
        disclosure_schema: Option<AiDisclosureSchema>,
    ) -> Result<(), AiError> {
        if descriptor
            .argument_schema
            .get("$schema")
            .and_then(serde_json::Value::as_str)
            != Some(JSON_SCHEMA_2020_12)
            || jsonschema::validator_for(&descriptor.argument_schema).is_err()
        {
            return Err(AiError::InvalidConfiguration(
                "tool arguments must use a valid JSON Schema 2020-12 contract".to_owned(),
            ));
        }
        if matches!(
            descriptor.operation_domain,
            AiToolOperationDomain::AiControlPlane | AiToolOperationDomain::SchemaIntrospection
        ) || contains_forbidden_graphql_name(&descriptor.document)
        {
            return Err(AiError::InvalidConfiguration(
                "AI control-plane and introspection operations cannot be tools".to_owned(),
            ));
        }
        if self.tools.contains_key(&descriptor.id) {
            return Err(AiError::AlreadyExists(descriptor.id.as_str().to_owned()));
        }
        self.tools.insert(
            descriptor.id.clone(),
            RegisteredAiTool {
                descriptor,
                disclosure_schema,
            },
        );
        Ok(())
    }

    /// Returns a descriptor by ID. This is discovery, not authorization.
    pub fn descriptor(&self, id: &AiToolId) -> Option<&AiToolDescriptor> {
        self.tools.get(id).map(|tool| &tool.descriptor)
    }

    /// Returns the static disclosure schema for a registered GraphQL tool.
    pub fn disclosure_schema(&self, id: &AiToolId) -> Option<&AiDisclosureSchema> {
        self.tools
            .get(id)
            .and_then(|tool| tool.disclosure_schema.as_ref())
    }

    /// Returns all registered descriptors.
    pub fn descriptors(&self) -> impl Iterator<Item = &AiToolDescriptor> {
        self.tools.values().map(|tool| &tool.descriptor)
    }

    pub(crate) fn validate_execution_request(
        &self,
        id: &AiToolId,
        request: &ToolGraphqlRequest,
        maximum_maturity: ToolMaturity,
    ) -> Result<(&AiToolDescriptor, &AiDisclosureSchema), AiError> {
        let registered = self.tools.get(id).ok_or(AiError::Forbidden)?;
        let descriptor = &registered.descriptor;
        let disclosure = registered
            .disclosure_schema
            .as_ref()
            .ok_or(AiError::Forbidden)?;
        if descriptor.maturity > maximum_maturity
            || descriptor.operation_kind == AiToolOperationKind::Internal
            || descriptor.document != request.document
            || descriptor.graphql_contract.as_ref() != Some(&request.contract)
            || request.operation_name != request.contract.operation_name
            || descriptor.result_projection != request.contract.result_projection_fingerprint
            || disclosure.fingerprint != request.contract.disclosure_schema_fingerprint
        {
            return Err(AiError::Forbidden);
        }
        let validator = jsonschema::validator_for(&descriptor.argument_schema).map_err(|_| {
            AiError::InvalidConfiguration("registered tool argument schema is invalid".to_owned())
        })?;
        if !validator.is_valid(&request.variables) {
            return Err(AiError::InvalidInput(
                "tool arguments do not match the registered schema".to_owned(),
            ));
        }
        Ok((descriptor, disclosure))
    }

    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) fn validate_read_only_model_definition(
        &self,
        definition: &ModelToolDefinition,
        policy: &AiToolPolicySet,
    ) -> Result<(), AiError> {
        let id = AiToolId::parse(definition.tool_id.clone())?;
        let descriptor = self.descriptor(&id).ok_or(AiError::Forbidden)?;
        if !policy.allows(descriptor)
            || descriptor.operation_kind != AiToolOperationKind::Query
            || descriptor.operation_domain != AiToolOperationDomain::Application
            || descriptor.maturity != ToolMaturity::ReadOnly
            || descriptor.risk != AiToolRisk::ReadOnly
            || descriptor.approval != AiApprovalRule::None
            || !descriptor.idempotent
            || definition.fingerprint != descriptor.fingerprint
            || definition.description != descriptor.description
            || definition.parameters != descriptor.argument_schema
        {
            return Err(AiError::Forbidden);
        }
        Ok(())
    }

    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) fn validate_supervised_model_definition(
        &self,
        definition: &ModelToolDefinition,
        policy: &AiToolPolicySet,
    ) -> Result<(), AiError> {
        let id = AiToolId::parse(definition.tool_id.clone())?;
        let descriptor = self.descriptor(&id).ok_or(AiError::Forbidden)?;
        let safe_read = descriptor.operation_kind == AiToolOperationKind::Query
            && descriptor.operation_domain == AiToolOperationDomain::Application
            && descriptor.maturity == ToolMaturity::ReadOnly
            && descriptor.risk == AiToolRisk::ReadOnly
            && descriptor.approval == AiApprovalRule::None
            && descriptor.idempotent;
        let supervised_write = descriptor.operation_kind == AiToolOperationKind::Mutation
            && descriptor.operation_domain == AiToolOperationDomain::Application
            && descriptor.maturity == ToolMaturity::SupervisedWrite
            && matches!(
                descriptor.risk,
                AiToolRisk::LowRiskWrite | AiToolRisk::NonIdempotentWrite | AiToolRisk::HighImpact
            )
            && descriptor.approval == AiApprovalRule::OneShot;
        if !policy.allows(descriptor)
            || !(safe_read || supervised_write)
            || definition.fingerprint != descriptor.fingerprint
            || definition.description != descriptor.description
            || definition.parameters != descriptor.argument_schema
        {
            return Err(AiError::Forbidden);
        }
        Ok(())
    }
}

pub(crate) fn contains_forbidden_graphql_name(document: &str) -> bool {
    const FORBIDDEN: &[&str] = &[
        "aisessions",
        "aisession",
        "aimessages",
        "aimessageblocks",
        "aisessioneventpage",
        "aisessionevents",
        "aiproviderprofiles",
        "aicontentprotectionpolicy",
        "createaisession",
        "archiveaisession",
        "restoreaisession",
        "deleteaisession",
        "sendaimessage",
        "upsertaiproviderprofile",
        "setaiprovidercredential",
        "removeaiprovidercredential",
        "setaicontentprotectionpolicy",
        "aitooldiscovery",
        "aitools",
        "aiapprovals",
    ];

    graphql_names(document).any(|name| {
        if name.starts_with("__") && name != "__typename" {
            return true;
        }
        let normalized: String = name
            .bytes()
            .filter(|byte| *byte != b'_')
            .map(|byte| byte.to_ascii_lowercase() as char)
            .collect();
        FORBIDDEN.contains(&normalized.as_str())
    })
}

fn graphql_names(document: &str) -> impl Iterator<Item = &str> {
    let bytes = document.as_bytes();
    let mut names = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'#' => {
                index += 1;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'"' => {
                let triple = bytes.get(index..index + 3) == Some(b"\"\"\"");
                index += if triple { 3 } else { 1 };
                while index < bytes.len() {
                    if triple && bytes.get(index..index + 3) == Some(b"\"\"\"") {
                        index += 3;
                        break;
                    }
                    if !triple && bytes[index] == b'"' {
                        index += 1;
                        break;
                    }
                    if bytes[index] == b'\\' && !triple {
                        index = (index + 2).min(bytes.len());
                    } else {
                        index += 1;
                    }
                }
            }
            byte if byte == b'_' || byte.is_ascii_alphabetic() => {
                let start = index;
                index += 1;
                while index < bytes.len()
                    && (bytes[index] == b'_'
                        || bytes[index].is_ascii_alphabetic()
                        || bytes[index].is_ascii_digit())
                {
                    index += 1;
                }
                names.push(&document[start..index]);
            }
            _ => index += 1,
        }
    }
    names.into_iter()
}

/// Persisted policy binding for one exact descriptor fingerprint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiToolPolicyBinding {
    /// Stable tool ID.
    pub tool_id: AiToolId,
    /// Reviewed descriptor fingerprint.
    pub fingerprint: String,
    /// Explicit enablement.
    pub enabled: bool,
}

/// Current host authorization outcome for one exact registered tool request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiToolAuthorizationDecision {
    allowed: bool,
    /// Stable non-sensitive reason code for audit and diagnostics.
    pub reason_code: String,
    /// Current host policy version used for the decision.
    pub policy_version: String,
    /// Current safe authorization-state digest used by approval workflows.
    pub authorization_state_digest: String,
}

impl AiToolAuthorizationDecision {
    /// Creates an allowed current-principal decision.
    pub fn allow(
        reason_code: impl Into<String>,
        policy_version: impl Into<String>,
        authorization_state_digest: impl Into<String>,
    ) -> Self {
        Self {
            allowed: true,
            reason_code: reason_code.into(),
            policy_version: policy_version.into(),
            authorization_state_digest: authorization_state_digest.into(),
        }
    }

    /// Creates a denied current-principal decision.
    pub fn deny(reason_code: impl Into<String>, policy_version: impl Into<String>) -> Self {
        Self {
            allowed: false,
            reason_code: reason_code.into(),
            policy_version: policy_version.into(),
            authorization_state_digest: String::new(),
        }
    }

    /// Returns whether current host policy allowed this exact request.
    pub const fn is_allowed(&self) -> bool {
        self.allowed
    }

    pub(crate) fn is_complete_allow(&self) -> bool {
        self.allowed
            && !self.reason_code.trim().is_empty()
            && !self.policy_version.trim().is_empty()
            && !self.authorization_state_digest.trim().is_empty()
    }
}

/// Fresh, principal-aware host policy for registered application tool calls.
///
/// This is evaluated after principal rehydration for every execution. The
/// ordinary resolver authorization path still runs afterward and remains
/// authoritative.
#[async_trait]
pub trait AiToolAuthorizationPolicy: Send + Sync {
    /// Authorizes the exact registered descriptor, scope, and validated
    /// variables using the freshly resolved principal.
    async fn authorize(
        &self,
        principal: &ResolvedPrincipal,
        scope: &AiScope,
        descriptor: &AiToolDescriptor,
        variables: &serde_json::Value,
    ) -> AiToolAuthorizationDecision;
}

/// Fail-closed tool policy suitable as an explicit disabled implementation.
#[derive(Clone, Copy, Debug, Default)]
pub struct DenyAllAiToolAuthorizationPolicy;

#[async_trait]
impl AiToolAuthorizationPolicy for DenyAllAiToolAuthorizationPolicy {
    async fn authorize(
        &self,
        _principal: &ResolvedPrincipal,
        _scope: &AiScope,
        _descriptor: &AiToolDescriptor,
        _variables: &serde_json::Value,
    ) -> AiToolAuthorizationDecision {
        AiToolAuthorizationDecision::deny("default_deny", "deny-all")
    }
}

/// Scope tool policy. Absence always means disabled.
#[derive(Clone, Debug)]
pub struct AiToolPolicySet {
    maximum_maturity: ToolMaturity,
    bindings: BTreeMap<AiToolId, AiToolPolicyBinding>,
}

impl AiToolPolicySet {
    /// Creates an empty, default-deny policy with a deployment/scope maturity
    /// ceiling.
    pub fn new(maximum_maturity: ToolMaturity) -> Self {
        Self {
            maximum_maturity,
            bindings: BTreeMap::new(),
        }
    }

    /// Adds/replaces an explicit policy binding.
    pub fn bind(&mut self, binding: AiToolPolicyBinding) {
        self.bindings.insert(binding.tool_id.clone(), binding);
    }

    /// Returns whether the exact current descriptor is enabled within the
    /// maturity ceiling.
    pub fn allows(&self, descriptor: &AiToolDescriptor) -> bool {
        descriptor.maturity <= self.maximum_maturity
            && self.bindings.get(&descriptor.id).is_some_and(|binding| {
                binding.enabled && binding.fingerprint == descriptor.fingerprint
            })
    }
}
