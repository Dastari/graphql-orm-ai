//! Project-agnostic scope and identifier types.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Application-defined scope boundary for sessions, policy, and egress.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AiScope {
    /// Host-defined scope kind, such as `application`, `tenant`, or `project`.
    pub kind: String,
    /// Host-defined stable scope identifier.
    pub id: String,
    /// Optional tenant boundary.
    pub tenant_id: Option<String>,
}

impl AiScope {
    /// Creates a scope.
    pub fn new(kind: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            id: id.into(),
            tenant_id: None,
        }
    }

    /// Adds a tenant boundary.
    pub fn with_tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }
}

/// Returns the stable non-secret persistence identity for an AI scope.
///
/// This value supports dependency-owned lookup and migration diagnostics. It
/// proves neither scope validity nor caller authorization and must never be
/// used in place of host access policy.
pub fn ai_scope_key(scope: &AiScope) -> String {
    let mut hash = Sha256::new();
    hash.update(b"graphql-orm-ai/scope/v1\0");
    for value in [
        Some(scope.kind.as_str()),
        Some(scope.id.as_str()),
        scope.tenant_id.as_deref(),
    ] {
        match value {
            Some(value) => {
                hash.update([1]);
                hash.update((value.len() as u64).to_be_bytes());
                hash.update(value.as_bytes());
            }
            None => hash.update([0]),
        }
    }
    hex::encode(hash.finalize())
}

macro_rules! uuid_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl $name {
            /// Generates a new random identifier.
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }
    };
}

uuid_id!(AiSessionId, "AI session identifier.");
uuid_id!(AiRunId, "AI run identifier.");
uuid_id!(AiToolCallId, "AI tool-call identifier.");
uuid_id!(AiApprovalId, "AI approval identifier.");
uuid_id!(AiBudgetReservationId, "AI budget-reservation identifier.");
uuid_id!(AiProposalId, "AI proposal identifier.");
uuid_id!(AiEgressDecisionId, "AI egress-decision identifier.");
