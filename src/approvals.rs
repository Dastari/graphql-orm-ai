//! Exact, expiring, one-shot approval bindings for consequential tool calls.

use std::sync::Arc;

use agql_auth::{AuthPrincipal, PrincipalReference};
use async_graphql::{Context, Enum, ErrorExtensions, InputObject, Object, SimpleObject};
use async_trait::async_trait;
use graphql_orm::graphql::pagination::{
    KeysetConnectionInput, PageInfo, ValidatedKeysetConnection,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use uuid::Uuid;

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
        let preview_bytes = serde_json::to_vec(&preview.details)
            .map_err(|_| AiError::InvalidConfiguration("approval preview is invalid".to_owned()))?;
        if preview.action_kind.trim().is_empty()
            || preview.action_kind.len() > 200
            || preview.title.trim().is_empty()
            || preview.title.len() > 1_024
            || preview.targets.len() > 100
            || preview_bytes.len() > 256 * 1024
            || self.tool_fingerprint.trim().is_empty()
            || self.argument_hash.trim().is_empty()
            || self.policy_version.trim().is_empty()
            || self.authorization_state_digest.trim().is_empty()
            || self.resources.len() > 100
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
                || resource.resource_type.len() > 200
                || resource.resource_id.trim().is_empty()
                || resource.resource_id.len() > 1_024
                || resource.expected_version.trim().is_empty()
                || resource.expected_version.len() > 1_024
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

/// Opaque proof that the exact grant was atomically consumed once.
///
/// This proves only one-shot approval consumption. It does not prove current
/// resolver authorization, unchanged resource versions, or successful domain
/// mutation execution.
#[derive(Clone, Debug)]
pub struct ConsumedAiApproval {
    approval_id: AiApprovalId,
    binding_hash: String,
    consumed_at: OffsetDateTime,
}

impl ConsumedAiApproval {
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) fn new(authorized: AuthorizedAiApproval, consumed_at: OffsetDateTime) -> Self {
        Self {
            approval_id: authorized.approval_id,
            binding_hash: authorized.binding_hash,
            consumed_at,
        }
    }

    /// Consumed approval identifier.
    pub const fn approval_id(&self) -> AiApprovalId {
        self.approval_id
    }

    /// Exact action-envelope hash that was consumed.
    pub fn binding_hash(&self) -> &str {
        &self.binding_hash
    }

    /// Atomic consumption timestamp.
    pub const fn consumed_at(&self) -> OffsetDateTime {
        self.consumed_at
    }
}

/// Approval lifecycle action evaluated by the host.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiApprovalAction {
    /// Persist a pending approval for a current consequential tool call.
    Request,
    /// Read pending or historical approval state and canonical preview.
    Read,
    /// Approve or deny a pending request.
    Decide,
    /// Revoke a previously approved request before consumption.
    Revoke,
    /// Consume the exact grant immediately before fresh resolver execution.
    Consume,
}

/// Host-owned approval authorization policy.
#[async_trait]
pub trait AiApprovalAccessPolicy: Send + Sync {
    /// Decides one exact approval action for the current principal and scope.
    async fn can_access_approval(
        &self,
        principal: &AuthPrincipal,
        scope: &AiScope,
        session_id: AiSessionId,
        action: AiApprovalAction,
    ) -> bool;
}

/// Human approval decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_items = "PascalCase"))]
pub enum AiApprovalDecision {
    /// Approve one exact future consumption.
    Approve,
    /// Deny the pending action.
    Deny,
}

/// Authorized/decrypted approval view.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiApprovalView {
    /// Approval identifier.
    pub id: Uuid,
    /// Bound tool-call identifier.
    pub tool_call_id: Uuid,
    /// Owning session.
    pub session_id: Uuid,
    /// Server-generated canonical action preview.
    pub canonical_preview: async_graphql::Json<serde_json::Value>,
    /// Pending/approved/denied/expired/revoked/consumed state.
    pub state: String,
    /// Whether approval required recent MFA.
    pub recent_mfa_required: bool,
    /// Human approver subject after a decision.
    pub approver_subject: Option<String>,
    /// Creation time in Unix seconds.
    pub created_at: i64,
    /// Exclusive expiry in Unix seconds.
    pub expires_at: i64,
    /// Decision time in Unix seconds.
    pub decided_at: Option<i64>,
    /// One-shot consumption time in Unix seconds.
    pub consumed_at: Option<i64>,
    /// Current CAS version.
    pub row_version: i64,
}

/// Approval connection edge.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiApprovalEdge {
    /// Approval node.
    pub node: AiApprovalView,
    /// Opaque keyset cursor.
    pub cursor: String,
}

/// Bounded approval connection.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiApprovalConnection {
    /// Bounded edges.
    pub edges: Vec<AiApprovalEdge>,
    /// Relay page metadata.
    pub page_info: PageInfo,
}

/// CAS-bound approval decision input.
#[derive(Clone, Debug, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct DecideAiApprovalInput {
    /// Approval identifier.
    pub id: Uuid,
    /// Approve or deny.
    pub decision: AiApprovalDecision,
    /// Exact row version rendered with the canonical preview.
    pub expected_version: i64,
}

/// CAS-bound approval revocation input.
#[derive(Clone, Debug, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct RevokeAiApprovalInput {
    /// Approval identifier.
    pub id: Uuid,
    /// Exact row version observed by the revoker.
    pub expected_version: i64,
}

/// Authenticated, scope-aware approval lifecycle exposed to GraphQL.
#[async_trait]
pub trait AiApprovalService: Send + Sync {
    /// Lists a bounded approval window visible in one session.
    async fn approvals(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
        page: ValidatedKeysetConnection,
    ) -> Result<AiApprovalConnection, AiError>;

    /// Loads one visible approval.
    async fn approval(
        &self,
        principal: &AuthPrincipal,
        approval_id: AiApprovalId,
    ) -> Result<Option<AiApprovalView>, AiError>;

    /// Applies one CAS-bound human approval decision.
    async fn decide_approval(
        &self,
        principal: &AuthPrincipal,
        input: DecideAiApprovalInput,
    ) -> Result<AiApprovalView, AiError>;

    /// Revokes an approved but unconsumed grant.
    async fn revoke_approval(
        &self,
        principal: &AuthPrincipal,
        input: RevokeAiApprovalInput,
    ) -> Result<AiApprovalView, AiError>;
}

/// Composable approval query root.
#[derive(Clone, Copy, Debug, Default)]
pub struct AiApprovalQueryRoot;

#[cfg_attr(
    feature = "graphql-case-pascal",
    Object(rename_fields = "PascalCase", rename_args = "PascalCase")
)]
#[cfg_attr(not(feature = "graphql-case-pascal"), Object)]
impl AiApprovalQueryRoot {
    /// Returns a bounded approval window for one visible session.
    async fn ai_approvals(
        &self,
        context: &Context<'_>,
        session_id: Uuid,
        #[graphql(default)] page: KeysetConnectionInput,
    ) -> async_graphql::Result<AiApprovalConnection> {
        let principal = agql_auth::principal_from_ctx(context)?;
        let page = page.validate(50, 200).map_err(|error| (&error).extend())?;
        approval_service(context)?
            .approvals(&principal, AiSessionId(session_id), page)
            .await
            .map_err(extend)
    }

    /// Returns one visible approval and its canonical preview.
    async fn ai_approval(
        &self,
        context: &Context<'_>,
        id: Uuid,
    ) -> async_graphql::Result<Option<AiApprovalView>> {
        let principal = agql_auth::principal_from_ctx(context)?;
        approval_service(context)?
            .approval(&principal, AiApprovalId(id))
            .await
            .map_err(extend)
    }
}

/// Composable approval mutation root.
#[derive(Clone, Copy, Debug, Default)]
pub struct AiApprovalMutationRoot;

#[cfg_attr(
    feature = "graphql-case-pascal",
    Object(rename_fields = "PascalCase", rename_args = "PascalCase")
)]
#[cfg_attr(not(feature = "graphql-case-pascal"), Object)]
impl AiApprovalMutationRoot {
    /// Approves or denies one exact pending action.
    async fn decide_ai_approval(
        &self,
        context: &Context<'_>,
        input: DecideAiApprovalInput,
    ) -> async_graphql::Result<AiApprovalView> {
        let principal = agql_auth::principal_from_ctx(context)?;
        approval_service(context)?
            .decide_approval(&principal, input)
            .await
            .map_err(extend)
    }

    /// Revokes an approved, unconsumed grant.
    async fn revoke_ai_approval(
        &self,
        context: &Context<'_>,
        input: RevokeAiApprovalInput,
    ) -> async_graphql::Result<AiApprovalView> {
        let principal = agql_auth::principal_from_ctx(context)?;
        approval_service(context)?
            .revoke_approval(&principal, input)
            .await
            .map_err(extend)
    }
}

fn approval_service(context: &Context<'_>) -> async_graphql::Result<Arc<dyn AiApprovalService>> {
    context
        .data::<Arc<dyn AiApprovalService>>()
        .cloned()
        .map_err(|_| {
            AiError::InvalidConfiguration("AI approval service is not installed".to_owned())
                .extend()
        })
}

fn extend(error: AiError) -> async_graphql::Error {
    error.extend()
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
