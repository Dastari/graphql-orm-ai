//! AI-owned structured proposal staging contracts.

use std::{collections::BTreeMap, sync::Arc};

use agql_auth::AuthPrincipal;
use async_graphql::{Context, Enum, ErrorExtensions, InputObject, Object, SimpleObject};
use async_trait::async_trait;
use graphql_orm::graphql::pagination::{
    KeysetConnectionInput, PageInfo, ValidatedKeysetConnection,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
        if descriptor.maximum_payload_bytes == 0
            || descriptor.maximum_payload_bytes > 16 * 1024 * 1024
            || descriptor.maximum_items > 10_000
            || descriptor
                .required_source_kinds
                .iter()
                .any(|kind| kind.is_empty() || kind.len() > 200)
        {
            return Err(AiError::InvalidConfiguration(
                "proposal descriptor limits are invalid".to_owned(),
            ));
        }
        self.descriptors.insert(descriptor.id.clone(), descriptor);
        Ok(())
    }

    /// Returns one registered proposal descriptor.
    pub fn descriptor(&self, id: &AiProposalTypeId) -> Option<&AiProposalTypeDescriptor> {
        self.descriptors.get(id)
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Enum)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_items = "PascalCase"))]
pub enum AiProposalReviewDecision {
    /// Accepted as proposed.
    Accept,
    /// Accepted after a human edit.
    AcceptEdited,
    /// Rejected.
    Reject,
}

/// Proposal lifecycle action evaluated by the host.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiProposalAction {
    /// Persist a validated model-produced proposal for a current run.
    Create,
    /// Read proposal shells and protected structured content.
    Read,
    /// Accept, edit, or reject a staged proposal.
    Review,
    /// Link an already-committed ordinary application mutation.
    RecordAppliedOutcome,
}

/// Host-owned proposal authorization policy.
///
/// Repository session/scope/tenant checks remain mandatory after this policy
/// allows an action.
#[async_trait]
pub trait AiProposalAccessPolicy: Send + Sync {
    /// Decides one exact proposal lifecycle action.
    async fn can_access_proposal(
        &self,
        principal: &AuthPrincipal,
        scope: &AiScope,
        session_id: AiSessionId,
        action: AiProposalAction,
    ) -> bool;
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
        principal: &AuthPrincipal,
        outcome: AiProposalAppliedOutcome,
    ) -> Result<(), AiError>;
}

/// Bounded proposal shell and authorized structured content.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiProposalView {
    /// Proposal identifier.
    pub id: Uuid,
    /// Owning session.
    pub session_id: Uuid,
    /// Producing run.
    pub run_id: Uuid,
    /// Scope kind.
    pub scope_kind: String,
    /// Scope identifier.
    pub scope_id: String,
    /// Registered proposal type.
    pub proposal_type: String,
    /// Registered schema version.
    pub schema_version: String,
    /// Authorized/decrypted structured proposal payload.
    pub payload: async_graphql::Json<serde_json::Value>,
    /// Redacted provenance references.
    pub sources: async_graphql::Json<serde_json::Value>,
    /// Validated logical review-item count.
    pub item_count: i64,
    /// Pending/accepted/rejected/applied/expired lifecycle state.
    pub state: String,
    /// Safe creator subject.
    pub created_by_subject: String,
    /// Human reviewer subject.
    pub reviewed_by_subject: Option<String>,
    /// Application resource reference recorded after normal mutation commit.
    pub applied_resource_ref: Option<String>,
    /// Authoritative ordinary application audit reference.
    pub application_audit_ref: Option<String>,
    /// Creation time in Unix seconds.
    pub created_at: i64,
    /// Review time in Unix seconds.
    pub reviewed_at: Option<i64>,
    /// Optional exclusive expiry in Unix seconds.
    pub expires_at: Option<i64>,
    /// Current CAS version.
    pub row_version: i64,
}

/// Proposal connection edge.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiProposalEdge {
    /// Proposal node.
    pub node: AiProposalView,
    /// Opaque keyset cursor.
    pub cursor: String,
}

/// Bounded proposal connection.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiProposalConnection {
    /// Bounded edges.
    pub edges: Vec<AiProposalEdge>,
    /// Relay page metadata.
    pub page_info: PageInfo,
}

/// Whole-proposal review input.
#[derive(Clone, Debug, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct ReviewAiProposalInput {
    /// Proposal identifier.
    pub id: Uuid,
    /// Accept, accept with an edited structured payload, or reject.
    pub decision: AiProposalReviewDecision,
    /// Human-edited replacement payload, required only for `AcceptEdited`.
    pub edited_payload: Option<async_graphql::Json<serde_json::Value>>,
    /// Logical item count for an edited replacement payload.
    pub edited_item_count: Option<i64>,
    /// Exact CAS version displayed to the reviewer.
    pub expected_version: i64,
}

/// Authenticated, scope-aware proposal lifecycle.
///
/// Implementations stage suggestions only. They must never perform an
/// application domain mutation while reviewing or recording a proposal.
#[async_trait]
pub trait AiProposalService: AiProposalOutcomeRecorder + Send + Sync {
    /// Lists a bounded proposal window visible in one session.
    async fn proposals(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
        page: ValidatedKeysetConnection,
    ) -> Result<AiProposalConnection, AiError>;

    /// Loads one visible proposal.
    async fn proposal(
        &self,
        principal: &AuthPrincipal,
        proposal_id: AiProposalId,
    ) -> Result<Option<AiProposalView>, AiError>;

    /// Applies one CAS-bound human review decision to staged AI-owned data.
    async fn review_proposal(
        &self,
        principal: &AuthPrincipal,
        input: ReviewAiProposalInput,
    ) -> Result<AiProposalView, AiError>;
}

/// Composable proposal query root.
#[derive(Clone, Copy, Debug, Default)]
pub struct AiProposalQueryRoot;

#[cfg_attr(
    feature = "graphql-case-pascal",
    Object(rename_fields = "PascalCase", rename_args = "PascalCase")
)]
#[cfg_attr(not(feature = "graphql-case-pascal"), Object)]
impl AiProposalQueryRoot {
    /// Returns a bounded proposal window for one visible session.
    async fn ai_proposals(
        &self,
        context: &Context<'_>,
        session_id: Uuid,
        #[graphql(default)] page: KeysetConnectionInput,
    ) -> async_graphql::Result<AiProposalConnection> {
        let principal = agql_auth::principal_from_ctx(context)?;
        let page = page.validate(50, 200).map_err(|error| (&error).extend())?;
        proposal_service(context)?
            .proposals(&principal, AiSessionId(session_id), page)
            .await
            .map_err(extend)
    }

    /// Returns one visible structured proposal.
    async fn ai_proposal(
        &self,
        context: &Context<'_>,
        id: Uuid,
    ) -> async_graphql::Result<Option<AiProposalView>> {
        let principal = agql_auth::principal_from_ctx(context)?;
        proposal_service(context)?
            .proposal(&principal, AiProposalId(id))
            .await
            .map_err(extend)
    }
}

/// Composable proposal mutation root.
#[derive(Clone, Copy, Debug, Default)]
pub struct AiProposalMutationRoot;

#[cfg_attr(
    feature = "graphql-case-pascal",
    Object(rename_fields = "PascalCase", rename_args = "PascalCase")
)]
#[cfg_attr(not(feature = "graphql-case-pascal"), Object)]
impl AiProposalMutationRoot {
    /// Reviews staged AI-owned data; it never performs the domain mutation.
    async fn review_ai_proposal(
        &self,
        context: &Context<'_>,
        input: ReviewAiProposalInput,
    ) -> async_graphql::Result<AiProposalView> {
        let principal = agql_auth::principal_from_ctx(context)?;
        proposal_service(context)?
            .review_proposal(&principal, input)
            .await
            .map_err(extend)
    }
}

fn proposal_service(context: &Context<'_>) -> async_graphql::Result<Arc<dyn AiProposalService>> {
    context
        .data::<Arc<dyn AiProposalService>>()
        .cloned()
        .map_err(|_| {
            AiError::InvalidConfiguration("AI proposal service is not installed".to_owned())
                .extend()
        })
}

fn extend(error: AiError) -> async_graphql::Error {
    error.extend()
}
