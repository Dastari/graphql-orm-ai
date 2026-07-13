//! AI-owned structured proposal staging contracts.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{AiDataSourceRef, AiError, AiProposalId, AiRunId, AiScope, AiSessionId};

const JSON_SCHEMA_2020_12: &str = "https://json-schema.org/draft/2020-12/schema";

/// Stable validated proposal-type identifier.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AiProposalTypeId(String);

impl AiProposalTypeId {
    /// Parses a lower-case namespaced proposal type.
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
                "proposal type IDs must be lower-case ASCII names".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    /// Returns the type identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Host-registered project-specific proposal contract.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AiProposalTypeDescriptor {
    /// Stable proposal type.
    pub id: AiProposalTypeId,
    /// Immutable schema version.
    pub schema_version: String,
    /// JSON Schema 2020-12 payload contract.
    pub schema: serde_json::Value,
    /// Safe UI labels/hints; never route code.
    pub display_metadata: serde_json::Value,
    /// Maximum serialized payload bytes.
    pub maximum_payload_bytes: u64,
    /// Maximum logical review items.
    pub maximum_items: u32,
    /// Required source kinds.
    pub required_source_kinds: Vec<String>,
}

impl AiProposalTypeDescriptor {
    /// Creates and validates a proposal descriptor.
    ///
    /// # Errors
    ///
    /// Returns an error unless the schema explicitly declares JSON Schema
    /// 2020-12 and compiles successfully.
    pub fn new(
        id: impl Into<String>,
        schema_version: impl Into<String>,
        schema: serde_json::Value,
    ) -> Result<Self, AiError> {
        let id = AiProposalTypeId::parse(id)?;
        let schema_version = schema_version.into();
        if schema_version.trim().is_empty() {
            return Err(AiError::InvalidConfiguration(
                "proposal schema version must not be empty".to_owned(),
            ));
        }
        if schema.get("$schema").and_then(serde_json::Value::as_str) != Some(JSON_SCHEMA_2020_12) {
            return Err(AiError::InvalidConfiguration(
                "proposal schemas must declare JSON Schema 2020-12".to_owned(),
            ));
        }
        jsonschema::validator_for(&schema).map_err(|_| {
            AiError::InvalidConfiguration("proposal JSON Schema is invalid".to_owned())
        })?;

        Ok(Self {
            id,
            schema_version,
            schema,
            display_metadata: serde_json::json!({}),
            maximum_payload_bytes: 256 * 1024,
            maximum_items: 100,
            required_source_kinds: Vec::new(),
        })
    }

    /// Sets safe display metadata.
    pub fn with_display_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.display_metadata = metadata;
        self
    }

    /// Sets payload/item limits.
    pub fn with_limits(mut self, maximum_payload_bytes: u64, maximum_items: u32) -> Self {
        self.maximum_payload_bytes = maximum_payload_bytes;
        self.maximum_items = maximum_items;
        self
    }

    /// Requires provenance from the listed source kinds.
    pub fn with_required_source_kinds(mut self, kinds: Vec<String>) -> Self {
        self.required_source_kinds = kinds;
        self
    }
}

/// AI-produced proposal draft before schema/provenance validation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AiProposalDraft {
    /// Registered proposal type.
    pub proposal_type: AiProposalTypeId,
    /// Session.
    pub session_id: AiSessionId,
    /// Run.
    pub run_id: AiRunId,
    /// Application scope.
    pub scope: AiScope,
    /// Structured suggestion payload.
    pub payload: serde_json::Value,
    /// Redacted provenance references.
    pub sources: Vec<AiDataSourceRef>,
    /// Logical item count supplied by the runtime adapter.
    pub item_count: u32,
}

/// Schema/provenance-validated proposal ready for protected persistence.
#[derive(Clone, Debug)]
pub struct ValidatedAiProposal {
    /// Assigned proposal ID.
    pub id: AiProposalId,
    /// Registered descriptor.
    pub descriptor: AiProposalTypeDescriptor,
    /// Validated draft.
    pub draft: AiProposalDraft,
}

/// Proposal registry. Registration never grants application mutation access.
#[derive(Clone, Debug, Default)]
pub struct AiProposalCatalog {
    descriptors: BTreeMap<AiProposalTypeId, AiProposalTypeDescriptor>,
}

impl AiProposalCatalog {
    /// Creates an empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a proposal contract.
    pub fn register(&mut self, descriptor: AiProposalTypeDescriptor) -> Result<(), AiError> {
        if self.descriptors.contains_key(&descriptor.id) {
            return Err(AiError::AlreadyExists(descriptor.id.as_str().to_owned()));
        }
        self.descriptors.insert(descriptor.id.clone(), descriptor);
        Ok(())
    }

    /// Validates model output and provenance against a registered contract.
    pub fn validate(&self, draft: AiProposalDraft) -> Result<ValidatedAiProposal, AiError> {
        let descriptor = self
            .descriptors
            .get(&draft.proposal_type)
            .ok_or(AiError::NotFound)?;
        let payload_bytes = serde_json::to_vec(&draft.payload).map_err(|_| {
            AiError::InvalidInput("proposal payload is not serializable".to_owned())
        })?;
        if payload_bytes.len() as u64 > descriptor.maximum_payload_bytes
            || draft.item_count > descriptor.maximum_items
        {
            return Err(AiError::InvalidInput(
                "proposal payload exceeds configured limits".to_owned(),
            ));
        }

        let validator = jsonschema::validator_for(&descriptor.schema).map_err(|_| {
            AiError::InvalidConfiguration("registered proposal schema is invalid".to_owned())
        })?;
        if !validator.is_valid(&draft.payload) {
            return Err(AiError::InvalidInput(
                "proposal payload does not match the registered schema".to_owned(),
            ));
        }

        for required_kind in &descriptor.required_source_kinds {
            if !draft
                .sources
                .iter()
                .any(|source| source.kind == *required_kind)
            {
                return Err(AiError::InvalidInput(
                    "proposal is missing required provenance".to_owned(),
                ));
            }
        }

        Ok(ValidatedAiProposal {
            id: AiProposalId::new(),
            descriptor: descriptor.clone(),
            draft,
        })
    }
}

/// Human review state for a proposal/item.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiProposalReviewDecision {
    /// Accepted as proposed.
    Accept,
    /// Accepted after a human edit.
    AcceptEdited,
    /// Rejected.
    Reject,
}

/// Trusted application outcome after its normal domain mutation commits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiProposalAppliedOutcome {
    /// Proposal being linked.
    pub proposal_id: AiProposalId,
    /// Application resource type.
    pub resource_type: String,
    /// Application resource ID.
    pub resource_id: String,
    /// Authoritative application audit reference.
    pub application_audit_ref: String,
    /// Current human reviewer/applying subject.
    pub applied_by_subject: String,
}

/// Trusted service for recording an outcome after the host's ordinary domain
/// mutation succeeds. It never performs the domain mutation itself.
#[async_trait]
pub trait AiProposalOutcomeRecorder: Send + Sync {
    /// Links a committed domain outcome to its reviewed proposal.
    async fn record_applied_outcome(
        &self,
        outcome: AiProposalAppliedOutcome,
    ) -> Result<(), AiError>;
}
