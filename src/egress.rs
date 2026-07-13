//! Explicit external data-egress authorization.

use std::collections::BTreeSet;

use agql_auth::ResolvedPrincipal;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    AiDataSourceRef, AiEgressDecisionId, AiError, AiRunId, AiScope, AiSessionId, DataClassification,
};

/// External processing capability receiving application data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiEgressCapability {
    /// General model inference.
    ModelInference,
    /// Provider-hosted web search.
    WebSearch,
    /// Image understanding.
    ImageAnalysis,
    /// Image generation using supplied context.
    ImageGeneration,
    /// Provider-hosted file search or file retention.
    ProviderFile,
    /// Provider-hosted code execution.
    CodeExecution,
    /// Remote MCP server/tool.
    RemoteMcp,
    /// Tool result returned to a remote model.
    ToolResult,
}

/// Deployment trust class for a destination.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiDestinationTrust {
    /// Loopback/local model explicitly allowed by deployment policy.
    Local,
    /// Contracted external model provider.
    ManagedProvider,
    /// Other allowlisted external processor.
    ExternalProcessor,
}

/// Redacted exact manifest for a proposed transfer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiEgressManifest {
    /// Provider-profile reference.
    pub provider_profile_id: String,
    /// Provider kind.
    pub provider_kind: String,
    /// Model or processing route.
    pub model: String,
    /// Redacted destination identifier or endpoint trust name.
    pub destination: String,
    /// Destination trust class.
    pub destination_trust: AiDestinationTrust,
    /// Processing capability.
    pub capability: AiEgressCapability,
    /// Session scope.
    pub scope: AiScope,
    /// Session reference.
    pub session_id: Option<AiSessionId>,
    /// Run reference.
    pub run_id: Option<AiRunId>,
    /// Exact source references and classifications; never plaintext.
    pub sources: Vec<AiDataSourceRef>,
    /// Approximate outbound bytes.
    pub estimated_bytes: u64,
    /// Approximate outbound model tokens.
    pub estimated_tokens: u64,
    /// Attachment count.
    pub attachment_count: u32,
    /// Purpose limitation.
    pub purpose: String,
    /// Provider retention class.
    pub retention: String,
    /// Processing residency/region class.
    pub residency: Option<String>,
    /// Egress-policy version.
    pub policy_version: String,
    /// Optional purpose-bound consent/grant reference.
    pub consent_reference: Option<String>,
}

impl AiEgressManifest {
    /// Returns the highest classification in the manifest.
    pub fn maximum_classification(&self) -> DataClassification {
        self.sources
            .iter()
            .map(|source| source.classification)
            .max()
            .unwrap_or(DataClassification::Public)
    }

    /// Computes a stable hash over the redacted manifest.
    ///
    /// Source ordering does not affect the hash.
    pub fn stable_hash(&self) -> String {
        let mut canonical = self.clone();
        canonical.sources.sort();
        let encoded = serde_json::to_vec(&canonical)
            .expect("AiEgressManifest consists only of serializable values");
        hex::encode(Sha256::digest(encoded))
    }
}

/// Stable allow/deny outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiEgressOutcome {
    /// Transfer may proceed exactly as manifested.
    Allow,
    /// Transfer must not occur.
    Deny,
}

/// Stable redacted reason code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiEgressReason {
    /// All boundaries allowed the exact transfer.
    Allowed,
    /// Deployment boundary denied the destination or capability.
    DeploymentDenied,
    /// Scope/provider policy denied the transfer.
    PolicyDenied,
    /// Current principal was not authorized.
    PrincipalDenied,
    /// Data classification exceeds the destination ceiling.
    ClassificationDenied,
    /// Secret data can never leave the trust boundary.
    SecretDataDenied,
    /// Required consent is missing, expired, revoked, or mismatched.
    ConsentRequired,
    /// Budget or size boundary denied the transfer.
    LimitExceeded,
}

/// Auditable decision over one exact manifest hash.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiEgressDecision {
    /// Decision identifier.
    pub id: AiEgressDecisionId,
    /// Exact manifest hash.
    pub manifest_hash: String,
    /// Outcome.
    pub outcome: AiEgressOutcome,
    /// Stable reason.
    pub reason: AiEgressReason,
    /// Applied policy version.
    pub policy_version: String,
    /// Safe principal subject reference.
    pub principal_subject: String,
}

impl AiEgressDecision {
    /// Creates an allowed decision.
    pub fn allow(
        manifest: &AiEgressManifest,
        policy_version: impl Into<String>,
        principal_subject: impl Into<String>,
    ) -> Self {
        Self {
            id: AiEgressDecisionId::new(),
            manifest_hash: manifest.stable_hash(),
            outcome: AiEgressOutcome::Allow,
            reason: AiEgressReason::Allowed,
            policy_version: policy_version.into(),
            principal_subject: principal_subject.into(),
        }
    }

    /// Creates a denied decision.
    pub fn deny(
        manifest: &AiEgressManifest,
        reason: AiEgressReason,
        policy_version: impl Into<String>,
        principal_subject: impl Into<String>,
    ) -> Self {
        Self {
            id: AiEgressDecisionId::new(),
            manifest_hash: manifest.stable_hash(),
            outcome: AiEgressOutcome::Deny,
            reason,
            policy_version: policy_version.into(),
            principal_subject: principal_subject.into(),
        }
    }

    /// Converts an allowed, unchanged decision into the token required by a
    /// provider call.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::EgressDenied`] for a denial or changed manifest.
    pub fn authorize(&self, manifest: &AiEgressManifest) -> Result<AuthorizedEgress, AiError> {
        if self.outcome != AiEgressOutcome::Allow || self.manifest_hash != manifest.stable_hash() {
            return Err(AiError::EgressDenied);
        }
        Ok(AuthorizedEgress {
            decision_id: self.id,
            manifest_hash: self.manifest_hash.clone(),
        })
    }
}

/// Proof that an exact manifest passed egress policy.
///
/// Fields are private so providers cannot be called with an arbitrary string
/// in place of an allow decision.
#[derive(Clone, Debug)]
pub struct AuthorizedEgress {
    decision_id: AiEgressDecisionId,
    manifest_hash: String,
}

impl AuthorizedEgress {
    /// Returns the decision identifier for auditing.
    pub fn decision_id(&self) -> AiEgressDecisionId {
        self.decision_id
    }

    /// Returns the exact allowed manifest hash.
    pub fn manifest_hash(&self) -> &str {
        &self.manifest_hash
    }
}

/// Application/scope egress policy. Deployment hard boundaries must be
/// intersected by the implementation and cannot be relaxed through GraphQL.
#[async_trait]
pub trait AiEgressPolicy: Send + Sync {
    /// Authorizes an exact redacted manifest for the current principal.
    async fn authorize(
        &self,
        principal: &ResolvedPrincipal,
        manifest: &AiEgressManifest,
    ) -> AiEgressDecision;
}

/// Fail-closed default policy.
#[derive(Clone, Copy, Debug, Default)]
pub struct DenyAllEgressPolicy;

#[async_trait]
impl AiEgressPolicy for DenyAllEgressPolicy {
    async fn authorize(
        &self,
        principal: &ResolvedPrincipal,
        manifest: &AiEgressManifest,
    ) -> AiEgressDecision {
        AiEgressDecision::deny(
            manifest,
            AiEgressReason::PolicyDenied,
            "deny-all",
            principal.principal().subject(),
        )
    }
}

/// Immutable deployment hard boundary used by policy implementations.
#[derive(Clone, Debug)]
pub struct AiDeploymentEgressBoundary {
    /// Allowed destination trust classes.
    pub allowed_destination_trust: BTreeSet<AiDestinationTrust>,
    /// Allowed processing capabilities.
    pub allowed_capabilities: BTreeSet<AiEgressCapability>,
    /// Maximum outbound classification. `Secret` is denied regardless.
    pub maximum_classification: DataClassification,
    /// Maximum bytes per transfer.
    pub maximum_bytes: u64,
    /// Maximum attachments per transfer.
    pub maximum_attachments: u32,
}

impl Default for AiDeploymentEgressBoundary {
    fn default() -> Self {
        Self {
            allowed_destination_trust: BTreeSet::new(),
            allowed_capabilities: BTreeSet::new(),
            maximum_classification: DataClassification::Public,
            maximum_bytes: 0,
            maximum_attachments: 0,
        }
    }
}

impl AiDeploymentEgressBoundary {
    /// Applies hard deployment limits to a manifest.
    pub fn evaluate(&self, manifest: &AiEgressManifest) -> Result<(), AiEgressReason> {
        let classification = manifest.maximum_classification();
        if classification == DataClassification::Secret {
            return Err(AiEgressReason::SecretDataDenied);
        }
        if !self
            .allowed_destination_trust
            .contains(&manifest.destination_trust)
            || !self.allowed_capabilities.contains(&manifest.capability)
        {
            return Err(AiEgressReason::DeploymentDenied);
        }
        if classification > self.maximum_classification {
            return Err(AiEgressReason::ClassificationDenied);
        }
        if manifest.estimated_bytes > self.maximum_bytes
            || manifest.attachment_count > self.maximum_attachments
        {
            return Err(AiEgressReason::LimitExceeded);
        }
        Ok(())
    }
}
