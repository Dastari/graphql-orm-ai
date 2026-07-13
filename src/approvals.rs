//! Exact, expiring, one-shot approval bindings for consequential tool calls.

use agql_auth::PrincipalReference;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

use crate::{AiApprovalId, AiError, AiScope, AiSessionId, AiToolCallId, GraphqlOperationContract};

/// Opaque application resource and optimistic-concurrency precondition.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AiApprovalResourceBinding {
    /// Host-defined resource type.
    pub resource_type: String,
    /// Opaque resource identifier.
    pub resource_id: String,
    /// Expected row version, ETag, or host-generated precondition digest.
    pub expected_version: String,
}

/// Server-generated canonical action preview shown to an approver.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AiCanonicalActionPreview {
    /// Stable host-defined action kind.
    pub action_kind: String,
    /// Server-authored concise title.
    pub title: String,
    /// Typed target/precondition bindings included in the action.
    pub targets: Vec<AiApprovalResourceBinding>,
    /// Server-generated bounded structured diff/impact facts.
    pub details: serde_json::Value,
}

impl AiCanonicalActionPreview {
    /// Returns a stable hash suitable for approval binding.
    pub fn stable_hash(&self) -> String {
        let mut canonical = self.clone();
        canonical.targets.sort();
        let encoded = serde_json::to_vec(&canonical)
            .expect("AiCanonicalActionPreview consists only of serializable values");
        hex::encode(Sha256::digest(encoded))
    }
}

/// Complete action envelope to which one approval is bound.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AiApprovalBinding {
    /// Tool call awaiting approval.
    pub tool_call_id: AiToolCallId,
    /// Session owning the action.
    pub session_id: AiSessionId,
    /// Scope and optional tenant boundary.
    pub scope: AiScope,
    /// Exact reviewed tool descriptor fingerprint.
    pub tool_fingerprint: String,
    /// Canonical validated variables/arguments hash.
    pub argument_hash: String,
    /// Exact local/remote GraphQL target and operation contract.
    pub operation: GraphqlOperationContract,
    /// Fingerprint of the safe durable principal reference.
    pub principal_reference_fingerprint: String,
    /// Original/delegated actor subject when applicable.
    pub delegated_actor_subject: Option<String>,
    /// Safe delegation/grant reference, never a token.
    pub delegation_reference: Option<String>,
    /// Current tool/scope/application policy version.
    pub policy_version: String,
    /// Host-generated safe authorization-state/precondition digest.
    pub authorization_state_digest: String,
    /// Exact target resources and optimistic-concurrency preconditions.
    pub resources: Vec<AiApprovalResourceBinding>,
    /// Hash of the server-generated canonical action preview.
    pub preview_hash: String,
}

impl AiApprovalBinding {
    /// Computes a stable hash over the complete approval envelope.
    pub fn stable_hash(&self) -> String {
        let mut canonical = self.clone();
        canonical.resources.sort();
        let encoded = serde_json::to_vec(&canonical)
            .expect("AiApprovalBinding consists only of serializable values");
        hex::encode(Sha256::digest(encoded))
    }

    /// Validates that required policy and operation bindings are present.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] when a required binding is
    /// empty, target resources are duplicated, or the preview hash is stale.
    pub fn validate(&self, preview: &AiCanonicalActionPreview) -> Result<(), AiError> {
        if self.tool_fingerprint.trim().is_empty()
            || self.argument_hash.trim().is_empty()
            || self.policy_version.trim().is_empty()
            || self.authorization_state_digest.trim().is_empty()
            || self.preview_hash != preview.stable_hash()
        {
            return Err(AiError::InvalidConfiguration(
                "approval binding is incomplete or stale".to_owned(),
            ));
        }
        let mut resources = self.resources.clone();
        resources.sort();
        if resources.iter().any(|resource| {
            resource.resource_type.trim().is_empty()
                || resource.resource_id.trim().is_empty()
                || resource.expected_version.trim().is_empty()
        }) || resources.windows(2).any(|window| window[0] == window[1])
        {
            return Err(AiError::InvalidConfiguration(
                "approval resource binding is invalid".to_owned(),
            ));
        }
        let mut preview_targets = preview.targets.clone();
        preview_targets.sort();
        if resources != preview_targets {
            return Err(AiError::InvalidConfiguration(
                "approval preview targets do not match action resources".to_owned(),
            ));
        }
        Ok(())
    }

    /// Fingerprints a safe principal reference without preserving roles,
    /// scopes, or any credential material.
    pub fn principal_fingerprint(reference: &PrincipalReference) -> String {
        let encoded = serde_json::to_vec(reference)
            .expect("PrincipalReference consists only of serializable values");
        hex::encode(Sha256::digest(encoded))
    }
}

/// Persisted lifecycle state for a one-shot approval.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiApprovalState {
    /// Awaiting an authorized human decision.
    Pending,
    /// Approved for one exact future consumption.
    Approved,
    /// Explicitly denied.
    Denied,
    /// Binding or time window is no longer current.
    Expired,
    /// Previously approved authority was revoked.
    Revoked,
    /// The exact approved action was consumed once.
    Consumed,
}

/// Exact approved decision before transactional one-shot consumption.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiApprovalGrant {
    /// Approval identifier.
    pub id: AiApprovalId,
    /// Complete action-envelope hash.
    pub binding_hash: String,
    /// Human approver subject.
    pub approver_subject: String,
    /// Current approval state.
    pub state: AiApprovalState,
    /// Decision timestamp.
    pub approved_at: OffsetDateTime,
    /// Exclusive expiry timestamp.
    pub expires_at: OffsetDateTime,
}

impl AiApprovalGrant {
    /// Validates this grant against a freshly rebuilt action envelope.
    ///
    /// This check does not consume the approval and does not replace fresh
    /// resolver authorization. Persistence must atomically transition the
    /// matching approved row to `Consumed` before executing a side effect.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Forbidden`] for a stale, expired, mismatched,
    /// or non-approved grant.
    pub fn authorize(
        &self,
        current_binding: &AiApprovalBinding,
        now: OffsetDateTime,
    ) -> Result<AuthorizedAiApproval, AiError> {
        if self.state != AiApprovalState::Approved
            || now < self.approved_at
            || now >= self.expires_at
            || self.binding_hash != current_binding.stable_hash()
        {
            return Err(AiError::Forbidden);
        }
        Ok(AuthorizedAiApproval {
            approval_id: self.id,
            binding_hash: self.binding_hash.clone(),
        })
    }
}

/// Opaque proof that an unexpired grant matched a freshly rebuilt action envelope.
#[derive(Clone, Debug)]
pub struct AuthorizedAiApproval {
    approval_id: AiApprovalId,
    binding_hash: String,
}

impl AuthorizedAiApproval {
    /// Returns the approval identifier for atomic consumption and audit linkage.
    pub const fn approval_id(&self) -> AiApprovalId {
        self.approval_id
    }

    /// Returns the exact action-envelope hash.
    pub fn binding_hash(&self) -> &str {
        &self.binding_hash
    }
}
