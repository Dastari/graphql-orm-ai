//! Durable run states and fenced worker transitions.

use graphql_orm::graphql::orm::{FencedLeaseState, LeaseError, LeaseProof};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Durable agent-run state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiRunState {
    /// Eligible for a worker claim.
    Queued,
    /// Claimed but provider work has not started.
    Leased,
    /// Provider/orchestration work is active.
    Running,
    /// Waiting for an argument-bound approval.
    WaitingApproval,
    /// Waiting for an application/internal tool result.
    WaitingTool,
    /// Waiting for the principal to reauthenticate.
    WaitingReauth,
    /// Eligible after a retry deadline.
    RetryScheduled,
    /// Restore/crash left an uncertain side effect requiring review.
    RecoveryRequired,
    /// Successful terminal state.
    Completed,
    /// Failed terminal state.
    Failed,
    /// Cancelled terminal state.
    Cancelled,
}

impl AiRunState {
    /// Stable durable storage value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Leased => "leased",
            Self::Running => "running",
            Self::WaitingApproval => "waiting_approval",
            Self::WaitingTool => "waiting_tool",
            Self::WaitingReauth => "waiting_reauth",
            Self::RetryScheduled => "retry_scheduled",
            Self::RecoveryRequired => "recovery_required",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "leased" => Some(Self::Leased),
            "running" => Some(Self::Running),
            "waiting_approval" => Some(Self::WaitingApproval),
            "waiting_tool" => Some(Self::WaitingTool),
            "waiting_reauth" => Some(Self::WaitingReauth),
            "retry_scheduled" => Some(Self::RetryScheduled),
            "recovery_required" => Some(Self::RecoveryRequired),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    /// Returns whether no further worker transition is allowed.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    /// Returns whether a worker may perform the transition.
    pub const fn can_transition_to(self, next: Self) -> bool {
        match self {
            Self::Queued | Self::RetryScheduled => matches!(next, Self::Leased | Self::Cancelled),
            Self::Leased => matches!(next, Self::Running | Self::Cancelled | Self::Failed),
            Self::Running => matches!(
                next,
                Self::WaitingApproval
                    | Self::WaitingTool
                    | Self::WaitingReauth
                    | Self::RetryScheduled
                    | Self::Completed
                    | Self::Failed
                    | Self::Cancelled
                    | Self::RecoveryRequired
            ),
            Self::WaitingApproval => matches!(
                next,
                Self::Running
                    | Self::WaitingReauth
                    | Self::Cancelled
                    | Self::Failed
                    | Self::RecoveryRequired
            ),
            Self::WaitingTool => matches!(
                next,
                Self::Running
                    | Self::RetryScheduled
                    | Self::Cancelled
                    | Self::Failed
                    | Self::RecoveryRequired
            ),
            Self::WaitingReauth => matches!(
                next,
                Self::Queued | Self::Cancelled | Self::Failed | Self::RecoveryRequired
            ),
            Self::RecoveryRequired | Self::Completed | Self::Failed | Self::Cancelled => false,
        }
    }
}

/// Pure representation of a durable run row's state and fenced lease fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiRunLeaseMachine {
    /// Current run state.
    pub state: AiRunState,
    /// Portable lease/CAS state.
    pub lease: FencedLeaseState,
}

impl AiRunLeaseMachine {
    /// Creates a queued run.
    pub fn queued(run_id: impl Into<String>, row_version: i64) -> Self {
        Self {
            state: AiRunState::Queued,
            lease: FencedLeaseState::new(run_id, row_version),
        }
    }

    /// Claims queued/retry-scheduled work and transitions it to `Leased`.
    pub fn claim(
        &mut self,
        worker_id: impl Into<String>,
        attempt_id: Uuid,
        now_ms: i64,
        lease_ttl_ms: i64,
        expected_row_version: i64,
    ) -> Result<LeaseProof, AiRunTransitionError> {
        if !matches!(self.state, AiRunState::Queued | AiRunState::RetryScheduled) {
            return Err(AiRunTransitionError::InvalidTransition {
                from: self.state,
                to: AiRunState::Leased,
            });
        }
        let proof = self.lease.claim(
            worker_id,
            attempt_id,
            now_ms,
            lease_ttl_ms,
            expected_row_version,
        )?;
        self.state = AiRunState::Leased;
        Ok(proof)
    }

    /// Applies a fenced durable state transition.
    pub fn transition(
        &mut self,
        proof: &LeaseProof,
        next: AiRunState,
        now_ms: i64,
        expected_row_version: i64,
    ) -> Result<i64, AiRunTransitionError> {
        if !self.state.can_transition_to(next) {
            return Err(AiRunTransitionError::InvalidTransition {
                from: self.state,
                to: next,
            });
        }
        let version = self
            .lease
            .commit_fenced_write(proof, now_ms, expected_row_version)?;
        self.state = next;
        Ok(version)
    }

    /// Authorizes and versions a durable event/tool/provider child append
    /// without changing run state.
    pub fn commit_child_write(
        &mut self,
        proof: &LeaseProof,
        now_ms: i64,
        expected_row_version: i64,
    ) -> Result<i64, AiRunTransitionError> {
        Ok(self
            .lease
            .commit_fenced_write(proof, now_ms, expected_row_version)?)
    }
}

/// Run transition error.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum AiRunTransitionError {
    /// State transition is not allowed.
    #[error("invalid AI run transition from {from:?} to {to:?}")]
    InvalidTransition {
        /// Current state.
        from: AiRunState,
        /// Requested state.
        to: AiRunState,
    },
    /// Lease/CAS/fencing validation failed.
    #[error(transparent)]
    Lease(#[from] LeaseError),
}
