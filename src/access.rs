//! Application-owned session/scope access policy.

use agql_auth::AuthPrincipal;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{AiScope, AiSessionId};

/// Session/scope action evaluated by the host application.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiSessionAction {
    /// List session shells.
    List,
    /// Read metadata/history/events.
    Read,
    /// Create a session in a scope.
    Create,
    /// Send a message or update session metadata.
    Write,
    /// Archive/restore.
    Archive,
    /// Delete and purge.
    Delete,
    /// Subscribe to durable events.
    Subscribe,
}

/// Stable access outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiAccessOutcome {
    /// Access is allowed subject to repository owner/tenant filters.
    Allow,
    /// Access is denied.
    Deny,
}

/// Redacted host access decision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiAccessDecision {
    /// Outcome.
    pub outcome: AiAccessOutcome,
    /// Stable host reason code.
    pub reason_code: String,
    /// Safe policy version.
    pub policy_version: String,
}

impl AiAccessDecision {
    /// Creates an allowed decision.
    pub fn allow(reason_code: impl Into<String>, policy_version: impl Into<String>) -> Self {
        Self {
            outcome: AiAccessOutcome::Allow,
            reason_code: reason_code.into(),
            policy_version: policy_version.into(),
        }
    }

    /// Creates a denied decision.
    pub fn deny(reason_code: impl Into<String>, policy_version: impl Into<String>) -> Self {
        Self {
            outcome: AiAccessOutcome::Deny,
            reason_code: reason_code.into(),
            policy_version: policy_version.into(),
        }
    }

    /// Returns whether access is allowed.
    pub fn is_allowed(&self) -> bool {
        self.outcome == AiAccessOutcome::Allow
    }
}

/// Host application access policy. Repository owner/tenant predicates remain
/// mandatory even after this policy allows an action.
#[async_trait]
pub trait AiAccessPolicy: Send + Sync {
    /// Evaluates whether the principal may perform an action in a scope.
    async fn can_access_scope(
        &self,
        principal: &AuthPrincipal,
        scope: &AiScope,
        action: AiSessionAction,
    ) -> AiAccessDecision;

    /// Evaluates whether the principal may perform an action on a session.
    async fn can_access_session(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
        action: AiSessionAction,
    ) -> AiAccessDecision;
}

/// Fail-closed default application policy.
#[derive(Clone, Copy, Debug, Default)]
pub struct DenyAllAiAccessPolicy;

#[async_trait]
impl AiAccessPolicy for DenyAllAiAccessPolicy {
    async fn can_access_scope(
        &self,
        _principal: &AuthPrincipal,
        _scope: &AiScope,
        _action: AiSessionAction,
    ) -> AiAccessDecision {
        AiAccessDecision::deny("default_deny", "deny-all")
    }

    async fn can_access_session(
        &self,
        _principal: &AuthPrincipal,
        _session_id: AiSessionId,
        _action: AiSessionAction,
    ) -> AiAccessDecision {
        AiAccessDecision::deny("default_deny", "deny-all")
    }
}
