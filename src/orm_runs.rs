//! Durable ORM-backed run leasing, fencing, and recovery.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::collections::BTreeSet;
use std::sync::Arc;

use agql_auth::{Clock, PrincipalReference};
use graphql_orm::db::Database;
use graphql_orm::graphql::errors::{OrmErrorCode, OrmPublicError};
use graphql_orm::graphql::filters::{IntFilter, StringFilter, UuidFilter};
use graphql_orm::graphql::orm::{
    ConditionalUpdateOutcome, DefaultWriteBackend, MutationContext, TransactionError,
    TransactionMode,
};
use sha2::{Digest, Sha256};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::orm_inbox::{PreparedAiInboxEvent, append_inbox_event};
use crate::persistence::*;
use crate::{
    AiApprovalId, AiBudgetAmounts, AiError, AiRunId, AiRunState, AiSessionId, AiSessionWakeup,
    AiToolCallId,
};

const MAXIMUM_WORKER_ID_BYTES: usize = 256;
const MAXIMUM_SAFE_CODE_BYTES: usize = 200;
const MAXIMUM_PROVIDER_REFERENCE_BYTES: usize = 1_024;

/// Deployment-owned hard limits for durable run workers.
///
/// These limits bound every candidate scan, lease, retry, and serialization
/// retry independently of GraphQL-managed configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiRunServiceLimits {
    lease_ttl: Duration,
    maximum_retry_delay: Duration,
    maximum_candidate_scan: usize,
    maximum_run_retries: u32,
    maximum_transaction_retries: usize,
}

impl AiRunServiceLimits {
    /// Creates validated worker limits.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] when a duration is not
    /// positive, a candidate scan is outside `1..=256`, more than 100 run
    /// retries are allowed, or more than 16 serialization retries are used.
    pub fn new(
        lease_ttl: Duration,
        maximum_retry_delay: Duration,
        maximum_candidate_scan: usize,
        maximum_run_retries: u32,
        maximum_transaction_retries: usize,
    ) -> Result<Self, AiError> {
        if !lease_ttl.is_positive()
            || !maximum_retry_delay.is_positive()
            || !(1..=256).contains(&maximum_candidate_scan)
            || maximum_run_retries > 100
            || maximum_transaction_retries > 16
        {
            return Err(AiError::InvalidConfiguration(
                "invalid deployment run-service limits".to_owned(),
            ));
        }
        Ok(Self {
            lease_ttl,
            maximum_retry_delay,
            maximum_candidate_scan,
            maximum_run_retries,
            maximum_transaction_retries,
        })
    }

    /// Returns the duration of every newly issued or renewed lease.
    pub const fn lease_ttl(&self) -> Duration {
        self.lease_ttl
    }

    /// Returns the maximum number of rows considered by one claim/recovery pass.
    pub const fn maximum_candidate_scan(&self) -> usize {
        self.maximum_candidate_scan
    }
}

/// Exact in-memory proof of one current durable run claim.
///
/// Fields are private so callers cannot manufacture a lease. The value alone
/// does not authorize a write: every service operation re-reads the run and
/// verifies its run ID, attempt ID, generation, owner, expiry, state, linked
/// checkpoint, and row version in a state-machine transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiRunLease {
    run_id: AiRunId,
    session_id: AiSessionId,
    input_message_id: Uuid,
    principal_reference: PrincipalReference,
    attempt_id: Uuid,
    worker_id: String,
    lease_generation: i64,
    lease_expires_at: OffsetDateTime,
    row_version: i64,
    state: AiRunState,
    retry_count: u32,
    latest_checkpoint_id: Option<Uuid>,
}

impl AiRunLease {
    /// Run identifier.
    pub const fn run_id(&self) -> AiRunId {
        self.run_id
    }

    /// Session identifier.
    pub const fn session_id(&self) -> AiSessionId {
        self.session_id
    }

    /// User message that initiated the run.
    pub const fn input_message_id(&self) -> Uuid {
        self.input_message_id
    }

    /// Safe durable principal reference requiring fresh rehydration before
    /// provider egress, tools, approvals, and long-running checkpoints.
    pub fn principal_reference(&self) -> &PrincipalReference {
        &self.principal_reference
    }

    /// Current attempt identifier.
    pub const fn attempt_id(&self) -> Uuid {
        self.attempt_id
    }

    /// Worker owner of the claim.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Monotonic durable fencing generation.
    pub const fn lease_generation(&self) -> i64 {
        self.lease_generation
    }

    /// Current lease expiry.
    pub const fn lease_expires_at(&self) -> OffsetDateTime {
        self.lease_expires_at
    }

    /// Current run state observed by the service.
    pub const fn state(&self) -> AiRunState {
        self.state
    }

    /// Number of previously scheduled retries.
    pub const fn retry_count(&self) -> u32 {
        self.retry_count
    }

    /// Latest fenced coordinator checkpoint linked to this lease, if any.
    ///
    /// A checkpoint ID alone is not resume authority. It must be validated and
    /// adopted by the protected checkpoint service under the current fence.
    pub const fn latest_checkpoint_id(&self) -> Option<Uuid> {
        self.latest_checkpoint_id
    }

    #[cfg(test)]
    pub(crate) fn test_running(principal_reference: PrincipalReference) -> Self {
        Self {
            run_id: AiRunId::new(),
            session_id: AiSessionId::new(),
            input_message_id: Uuid::new_v4(),
            principal_reference,
            attempt_id: Uuid::new_v4(),
            worker_id: "coordinator-test-worker".to_owned(),
            lease_generation: 1,
            lease_expires_at: OffsetDateTime::now_utc() + Duration::minutes(5),
            row_version: 1,
            state: AiRunState::Running,
            retry_count: 0,
            latest_checkpoint_id: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn test_with_checkpoint(&self, checkpoint_id: Uuid) -> Self {
        let mut lease = self.clone();
        lease.latest_checkpoint_id = Some(checkpoint_id);
        lease
    }

    #[cfg(test)]
    pub(crate) fn test_without_checkpoint(&self) -> Self {
        let mut lease = self.clone();
        lease.latest_checkpoint_id = None;
        lease
    }

    #[cfg(test)]
    pub(crate) fn test_with_state(&self, state: AiRunState) -> Self {
        let mut lease = self.clone();
        lease.state = state;
        lease
    }
}

/// Server-authored immutable outcome for a claimed run attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiRunCompletion {
    final_state: AiRunState,
    outcome_code: String,
    error_code: Option<String>,
    provider_response_id: Option<String>,
}

impl AiRunCompletion {
    /// Creates a terminal or recovery-required attempt outcome.
    ///
    /// Outcome and error values must be stable, redacted machine codes. They
    /// must never contain provider diagnostics, prompts, tool arguments, or
    /// response content.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidInput`] for a non-final state, malformed safe
    /// code, oversized provider reference, or inconsistent success/error pair.
    pub fn new(
        final_state: AiRunState,
        outcome_code: impl Into<String>,
        error_code: Option<String>,
        provider_response_id: Option<String>,
    ) -> Result<Self, AiError> {
        let outcome_code = outcome_code.into();
        if !matches!(
            final_state,
            AiRunState::Completed
                | AiRunState::Failed
                | AiRunState::Cancelled
                | AiRunState::RecoveryRequired
        ) || !valid_safe_code(&outcome_code)
            || error_code
                .as_deref()
                .is_some_and(|code| !valid_safe_code(code))
            || provider_response_id.as_ref().is_some_and(|value| {
                value.is_empty() || value.len() > MAXIMUM_PROVIDER_REFERENCE_BYTES
            })
            || (final_state == AiRunState::Completed && error_code.is_some())
        {
            return Err(AiError::InvalidInput(
                "invalid redacted run completion".to_owned(),
            ));
        }
        Ok(Self {
            final_state,
            outcome_code,
            error_code,
            provider_response_id,
        })
    }

    /// Durable run state produced by the outcome.
    pub const fn final_state(&self) -> AiRunState {
        self.final_state
    }
}

/// Bounded result of expired-lease reconciliation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AiRunRecoveryReport {
    /// Claims that expired before provider execution and were safely requeued.
    pub requeued: u32,
    /// Expired running attempts whose exact completed read-only tool batch was
    /// retained for current-principal validation and one fenced adoption.
    pub checkpoint_requeued: u32,
    /// Claims moved to manual/privileged recovery because external execution
    /// may have happened.
    pub recovery_required: u32,
    /// Pre-provider claims failed because their retry ceiling was exhausted.
    pub failed: u32,
    /// Expired attempts finalized successfully because an exact same-transaction
    /// assistant-output checkpoint proved final protected output persistence.
    pub completed: u32,
}

/// Fenced worker handoff for one human-approved waiting run.
///
/// The handoff preserves the original attempt and generation because the
/// approval, provider usage, and staged tool call are bound to them. It still
/// fences the former worker by atomically replacing the lease owner, expiry,
/// heartbeat, and row-version proof. This value is not approval consumption
/// and grants no resolver, rule, egress, or tool authority.
#[derive(Clone, Debug)]
pub struct AiApprovedRunClaim {
    approval_id: AiApprovalId,
    tool_call_id: AiToolCallId,
    lease: AiRunLease,
}

impl AiApprovedRunClaim {
    /// Exact approved, unconsumed one-shot request selected by the handoff.
    pub const fn approval_id(&self) -> AiApprovalId {
        self.approval_id
    }

    /// Exact staged consequential tool selected by the handoff.
    pub const fn tool_call_id(&self) -> AiToolCallId {
        self.tool_call_id
    }

    /// New owner/row-version fence in `WaitingTool` while approval is
    /// revalidated for consumption.
    pub fn lease(&self) -> &AiRunLease {
        &self.lease
    }

    /// Consumes the claim into its exact IDs and waiting lease.
    pub fn into_parts(self) -> (AiApprovalId, AiToolCallId, AiRunLease) {
        (self.approval_id, self.tool_call_id, self.lease)
    }

    #[cfg(test)]
    pub(crate) fn test_claim(lease: AiRunLease) -> Self {
        Self {
            approval_id: AiApprovalId::new(),
            tool_call_id: AiToolCallId::new(),
            lease,
        }
    }
}

/// Durable worker queue implemented only through generated `graphql-orm`
/// repositories and state-machine transactions.
#[derive(Clone)]
pub struct OrmAiRunService {
    database: Database<DefaultWriteBackend>,
    clock: Arc<dyn Clock>,
    limits: AiRunServiceLimits,
}

#[derive(Clone)]
pub(crate) struct PreparedProviderBlock {
    pub id: Uuid,
    pub kind: String,
    pub protected_content: serde_json::Value,
    pub byte_count: i64,
    pub line_count: i64,
}

#[derive(Clone)]
pub(crate) struct PreparedProviderOutput {
    pub message_id: Uuid,
    pub event_id: Uuid,
    pub inbox_event_id: Uuid,
    pub provider_kind: String,
    pub provider_model: String,
    pub protected_preview: serde_json::Value,
    pub protected_event: serde_json::Value,
    pub protected_inbox_event: serde_json::Value,
    pub blocks: Vec<PreparedProviderBlock>,
    pub correlation_id: String,
    pub provider_response_id: Option<String>,
    pub budget_reservation_id: Uuid,
    pub checkpoint_hash: String,
    pub expected_owner_principal_kind: String,
    pub expected_owner_subject: String,
    pub expected_scope_kind: String,
    pub expected_scope_id: String,
    pub expected_tenant_id: Option<String>,
}

pub(crate) struct PreparedContextCheckpointSource {
    pub message: AiMessageRecord,
    pub blocks: Vec<AiMessageBlockRecord>,
}

pub(crate) struct PreparedContextCheckpoint {
    pub id: Uuid,
    pub through_sequence: i64,
    pub source_hash: String,
    pub token_estimate: i64,
    pub provider_kind: String,
    pub provider_model: String,
    pub protected_summary: serde_json::Value,
    pub expected_parent: Option<AiContextCheckpointRecord>,
    pub sources: Vec<PreparedContextCheckpointSource>,
    pub maximum_checkpoints_per_session: usize,
    pub expected_owner_principal_kind: String,
    pub expected_owner_subject: String,
    pub expected_scope_kind: String,
    pub expected_scope_id: String,
    pub expected_tenant_id: Option<String>,
}

pub(crate) struct PreparedCoordinatorCheckpointTool {
    pub id: Uuid,
    pub provider_call_id: String,
    pub tool_id: String,
    pub result_egress_manifest_hash: String,
}

pub(crate) struct PreparedCoordinatorCheckpoint {
    pub id: Uuid,
    pub checkpoint_kind: String,
    pub provider_kind: String,
    pub provider_model: String,
    pub provider_response_id: Option<String>,
    pub budget_reservation_id: Uuid,
    pub protected_state: serde_json::Value,
    pub checkpoint_hash: String,
    pub completed_tools: Vec<PreparedCoordinatorCheckpointTool>,
}

pub(crate) struct PreparedLiveDeltaEvent {
    pub id: Uuid,
    pub event_type: String,
    pub protected_payload: serde_json::Value,
    pub correlation_id: String,
    pub provider_kind: String,
    pub provider_model: String,
    pub budget_reservation_id: Uuid,
    pub expected_owner_principal_kind: String,
    pub expected_owner_subject: String,
    pub expected_scope_kind: String,
    pub expected_scope_id: String,
    pub expected_tenant_id: Option<String>,
}

pub(crate) struct PreparedUiIntentEvent {
    pub id: Uuid,
    pub inbox_event_id: Uuid,
    pub protected_payload: serde_json::Value,
    pub protected_inbox_payload: serde_json::Value,
    pub correlation_id: String,
    pub provider_kind: String,
    pub provider_model: String,
    pub provider_response_id: Option<String>,
    pub budget_reservation_id: Uuid,
    pub usage: AiBudgetAmounts,
    pub cached_input_tokens: u64,
    pub expected_owner_principal_kind: String,
    pub expected_owner_subject: String,
    pub expected_scope_kind: String,
    pub expected_scope_id: String,
    pub expected_tenant_id: Option<String>,
}

pub(crate) struct PreparedProposal {
    pub id: Uuid,
    pub proposal_type: String,
    pub schema_version: String,
    pub item_count: i64,
    pub protected_payload: serde_json::Value,
    pub source_references: serde_json::Value,
    pub created_by_subject: String,
    pub expires_at: Option<i64>,
    pub event_id: Uuid,
    pub protected_event: serde_json::Value,
    pub correlation_id: String,
    pub expected_owner_principal_kind: String,
    pub expected_owner_subject: String,
    pub expected_scope_kind: String,
    pub expected_scope_id: String,
    pub expected_tenant_id: Option<String>,
}

pub(crate) struct PreparedApprovalRequest {
    pub id: Uuid,
    pub tool_call_id: Uuid,
    pub principal_subject: String,
    pub principal_reference_fingerprint: String,
    pub delegated_actor_subject: Option<String>,
    pub delegation_reference: Option<String>,
    pub argument_hash: String,
    pub tool_fingerprint: String,
    pub binding_hash: String,
    pub execution_target_id: String,
    pub target_schema_fingerprint: String,
    pub operation_name: String,
    pub operation_document_hash: String,
    pub result_projection_fingerprint: String,
    pub disclosure_schema_fingerprint: String,
    pub policy_version: String,
    pub authorization_state_digest: String,
    pub protected_resource_bindings: serde_json::Value,
    pub protected_action_preview: serde_json::Value,
    pub action_preview_hash: String,
    pub recent_mfa_required: bool,
    pub expires_at: i64,
    pub event_id: Uuid,
    pub protected_event: serde_json::Value,
    pub correlation_id: String,
    pub expected_owner_principal_kind: String,
    pub expected_owner_subject: String,
    pub expected_scope_kind: String,
    pub expected_scope_id: String,
    pub expected_tenant_id: Option<String>,
}

pub(crate) struct PreparedApprovalConsumption {
    pub approval_id: Uuid,
    pub tool_call_id: Uuid,
    pub binding_hash: String,
    pub expected_approval_version: i64,
    pub event_id: Uuid,
    pub protected_event: serde_json::Value,
    pub correlation_id: String,
    pub expected_owner_principal_kind: String,
    pub expected_owner_subject: String,
    pub expected_scope_kind: String,
    pub expected_scope_id: String,
    pub expected_tenant_id: Option<String>,
}

#[derive(Clone)]
pub(crate) enum PreparedApprovalWaitOutcome {
    Cancelled(Box<PreparedApprovalWaitCancellation>),
    RecoveryRequired {
        approval_id: Option<Uuid>,
        tool_call_id: Option<Uuid>,
    },
}

#[derive(Clone)]
pub(crate) struct PreparedApprovalWaitCancellation {
    pub call: AiToolCallRecord,
    pub step: AiRunStepRecord,
    pub approval: AiApprovalRecord,
    pub checkpoint: AiRunCheckpointRecord,
    pub next_approval_state: Option<String>,
    pub call_state: String,
}

#[derive(Clone)]
pub(crate) struct PreparedApprovalWaitReconciliation {
    pub expected_run: AiRunRecord,
    pub expected_owner_principal_kind: String,
    pub expected_owner_subject: String,
    pub expected_scope_kind: String,
    pub expected_scope_id: String,
    pub expected_tenant_id: Option<String>,
    pub outcome: PreparedApprovalWaitOutcome,
    pub outcome_code: String,
    pub policy_version: Option<String>,
    pub event_id: Uuid,
    pub protected_event: serde_json::Value,
    pub worker_id: String,
}

pub(crate) struct PreparedToolCallStart {
    pub id: Uuid,
    pub provider_call_key: String,
    pub provider_call_id: String,
    pub provider_kind: String,
    pub provider_model: String,
    pub provider_response_id: Option<String>,
    pub budget_reservation_id: Uuid,
    pub provider_turn_index: i64,
    pub tool_call_index: i64,
    pub tool_id: String,
    pub tool_fingerprint: String,
    pub protected_arguments: serde_json::Value,
    pub argument_hash: String,
    pub risk: String,
    pub idempotency_key: Option<String>,
    pub correlation_id: String,
    pub causation_id: String,
    pub delegation_reference: Option<String>,
    pub expected_owner_principal_kind: String,
    pub expected_owner_subject: String,
    pub expected_scope_kind: String,
    pub expected_scope_id: String,
    pub expected_tenant_id: Option<String>,
}

pub(crate) struct PreparedToolCallFinish {
    pub id: Uuid,
    pub state: String,
    pub protected_result: serde_json::Value,
    pub authorization_code: String,
    pub authorization_policy_version: Option<String>,
    pub authorization_state_digest: Option<String>,
    pub disclosure_schema_fingerprint: String,
    pub result_classification: String,
    pub result_egress_decision_id: Option<Uuid>,
    pub result_egress_manifest_hash: Option<String>,
    pub application_audit_ref: Option<String>,
    pub event_id: Uuid,
    pub protected_event: serde_json::Value,
    pub correlation_id: String,
    pub expected_provider_call_key: String,
    pub expected_tool_fingerprint: String,
    pub expected_owner_principal_kind: String,
    pub expected_owner_subject: String,
    pub expected_scope_kind: String,
    pub expected_scope_id: String,
    pub expected_tenant_id: Option<String>,
}

impl OrmAiRunService {
    /// Creates an ORM-backed run service.
    pub fn new(
        database: Database<DefaultWriteBackend>,
        clock: Arc<dyn Clock>,
        limits: AiRunServiceLimits,
    ) -> Self {
        Self {
            database,
            clock,
            limits,
        }
    }

    /// Returns the ORM database handle for host schema composition.
    pub fn database(&self) -> &Database<DefaultWriteBackend> {
        &self.database
    }

    #[cfg(all(
        any(feature = "sqlite", feature = "postgres"),
        feature = "provider-openai"
    ))]
    pub(crate) const fn lease_ttl(&self) -> Duration {
        self.limits.lease_ttl
    }

    /// Claims the oldest eligible queued/retry-scheduled run.
    ///
    /// The claim and immutable attempt fact commit atomically. Concurrent
    /// workers can never receive the same attempt/generation pair.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid worker ID, malformed durable state,
    /// fencing overflow, or persistence failure.
    pub async fn claim_next(&self, worker_id: &str) -> Result<Option<AiRunLease>, AiError> {
        validate_worker_id(worker_id)?;
        let now = canonical_second(self.clock.now());
        for retry in 0..=self.limits.maximum_transaction_retries {
            match self.claim_once(worker_id.to_owned(), now).await {
                Ok(result) => return result,
                Err(TransactionError::Retryable(_))
                    if retry < self.limits.maximum_transaction_retries =>
                {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(map_transaction(error)),
            }
        }
        Err(AiError::PersistenceFailed)
    }

    /// Claims one approved, unconsumed `WaitingApproval` run for immediate
    /// fresh validation and one-shot consumption.
    ///
    /// This is an in-attempt handoff, not a new provider attempt. The existing
    /// attempt and generation remain exact while the owner and row-version
    /// proof rotate atomically, fencing the worker that staged the request.
    /// Expired approved rows encountered in the bounded window are atomically
    /// marked expired and audited so they cannot permanently block newer
    /// eligible approvals.
    /// The caller must rehydrate current principal/rules, rebuild the preview,
    /// consume the approval, and execute the ordinary resolver; this claim
    /// authorizes none of those actions.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid worker ID, malformed approval/tool/run
    /// linkage, an invalid durable lease, lease-time overflow, or persistence
    /// failure.
    pub async fn claim_next_approved(
        &self,
        worker_id: &str,
    ) -> Result<Option<AiApprovedRunClaim>, AiError> {
        validate_worker_id(worker_id)?;
        let now = canonical_second(self.clock.now());
        for retry in 0..=self.limits.maximum_transaction_retries {
            match self.claim_approved_once(worker_id.to_owned(), now).await {
                Ok(result) => return Ok(result),
                Err(TransactionError::Retryable(_))
                    if retry < self.limits.maximum_transaction_retries =>
                {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(map_transaction(error)),
            }
        }
        Err(AiError::PersistenceFailed)
    }

    pub(crate) async fn reconcile_approval_wait(
        &self,
        reconciliation: PreparedApprovalWaitReconciliation,
    ) -> Result<(), AiError> {
        validate_worker_id(&reconciliation.worker_id)?;
        if !valid_safe_code(&reconciliation.outcome_code)
            || reconciliation
                .policy_version
                .as_deref()
                .is_some_and(|value| {
                    value.trim().is_empty()
                        || value.len() > 1_024
                        || value.chars().any(char::is_control)
                })
        {
            return Err(AiError::InvalidInput(
                "invalid approval-wait reconciliation".to_owned(),
            ));
        }
        let now = canonical_second(self.clock.now());
        for retry in 0..=self.limits.maximum_transaction_retries {
            match self
                .reconcile_approval_wait_once(reconciliation.clone(), now)
                .await
            {
                Ok(()) => return Ok(()),
                Err(TransactionError::Retryable(_))
                    if retry < self.limits.maximum_transaction_retries =>
                {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(map_transaction(error)),
            }
        }
        Err(AiError::PersistenceFailed)
    }

    /// Renews a current leased/running claim and returns its new row-version
    /// proof.
    ///
    /// # Errors
    ///
    /// Fails closed for an expired, superseded, malformed, non-active, or
    /// otherwise stale fence, and for persistence failures.
    pub async fn heartbeat(&self, lease: &AiRunLease) -> Result<AiRunLease, AiError> {
        let now = canonical_second(self.clock.now());
        self.update_active_lease(lease, LeaseUpdate::Heartbeat, now)
            .await
    }

    /// Transitions a freshly claimed run from `Leased` to `Running`.
    ///
    /// A running run must be treated as externally uncertain after its budget
    /// reservation crosses the provider-transport boundary.
    ///
    /// # Errors
    ///
    /// Fails closed for an expired or stale fence, invalid state, or
    /// persistence failure.
    pub async fn start(&self, lease: &AiRunLease) -> Result<AiRunLease, AiError> {
        let now = canonical_second(self.clock.now());
        self.update_active_lease(lease, LeaseUpdate::Start, now)
            .await
    }

    /// Completes a current attempt and appends its immutable outcome in the
    /// same fenced transaction.
    ///
    /// Budget usage must be reconciled before a successful/failed terminal
    /// completion. Ambiguous provider execution must use `RecoveryRequired`.
    ///
    /// # Errors
    ///
    /// Fails closed for an expired/stale fence, invalid transition, duplicate
    /// outcome, or persistence failure.
    pub async fn finish(
        &self,
        lease: &AiRunLease,
        completion: AiRunCompletion,
    ) -> Result<(), AiError> {
        let now = canonical_second(self.clock.now());
        let lease = lease.clone();
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = load_and_validate_active_lease(tx, &lease, now).await?;
                    let current_state = persisted_state(&current)?;
                    if !current_state.can_transition_to(completion.final_state) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let outcome = tx
                        .compare_and_swap::<AiRunRecord>(
                            &current.id,
                            current.row_version,
                            exact_state(&current.state),
                            UpdateAiRunRecordInput {
                                state: Some(completion.final_state.as_str().to_owned()),
                                lease_owner: Some(None),
                                lease_expires_at: Some(None),
                                lease_heartbeat_at: Some(None),
                                next_attempt_at: Some(None),
                                error_code: Some(completion.error_code.clone()),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(outcome, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    append_attempt_outcome(
                        tx,
                        &lease,
                        completion.final_state,
                        completion.outcome_code,
                        completion.provider_response_id,
                        now,
                    )
                    .await
                })
            })
            .await
            .map_err(map_transaction)
    }

    /// Schedules a bounded retry and relinquishes the current fence.
    ///
    /// # Errors
    ///
    /// Fails for an invalid delay/code, exhausted retry ceiling, an expired or
    /// stale fence, an invalid transition, or persistence failure.
    pub async fn schedule_retry(
        &self,
        lease: &AiRunLease,
        delay: Duration,
        error_code: impl Into<String>,
    ) -> Result<(), AiError> {
        let error_code = error_code.into();
        if delay.is_negative()
            || delay > self.limits.maximum_retry_delay
            || !valid_safe_code(&error_code)
        {
            return Err(AiError::InvalidInput(
                "invalid redacted run retry".to_owned(),
            ));
        }
        if lease.retry_count >= self.limits.maximum_run_retries {
            return Err(AiError::Conflict);
        }
        let now = canonical_second(self.clock.now());
        let eligible_at = now
            .checked_add(delay)
            .ok_or_else(|| AiError::InvalidConfiguration("run retry time overflow".to_owned()))?;
        let lease = lease.clone();
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = load_and_validate_active_lease(tx, &lease, now).await?;
                    let current_state = persisted_state(&current)?;
                    if !current_state.can_transition_to(AiRunState::RetryScheduled)
                        || current.retry_count < 0
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let retry_count = current
                        .retry_count
                        .checked_add(1)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::Conflict))?;
                    let outcome = tx
                        .compare_and_swap::<AiRunRecord>(
                            &current.id,
                            current.row_version,
                            exact_state(&current.state),
                            UpdateAiRunRecordInput {
                                state: Some(AiRunState::RetryScheduled.as_str().to_owned()),
                                attempt_id: Some(None),
                                lease_owner: Some(None),
                                lease_expires_at: Some(None),
                                lease_heartbeat_at: Some(None),
                                retry_count: Some(retry_count),
                                next_attempt_at: Some(Some(eligible_at.unix_timestamp())),
                                error_code: Some(Some(error_code.clone())),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(outcome, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    append_attempt_outcome(
                        tx,
                        &lease,
                        AiRunState::RetryScheduled,
                        error_code,
                        None,
                        now,
                    )
                    .await
                })
            })
            .await
            .map_err(map_transaction)
    }

    /// Reconciles a bounded window of expired active leases.
    ///
    /// A `Leased` claim is known not to have started provider orchestration and
    /// can be requeued. A `Running` attempt with an exact same-transaction
    /// protected-output checkpoint is safely finalized. An exact completed
    /// read-only tool-batch checkpoint can be requeued for protected adoption;
    /// it remains unusable until current-principal revalidation and is consumed
    /// before the next provider call. Live `WaitingApproval` runs are excluded
    /// because the bounded approval-wait reconciler owns their current policy,
    /// decision, and cutoff handling without heartbeating the human wait. Every
    /// other running or waiting state becomes `RecoveryRequired`; it is never
    /// silently replayed. Malformed checkpoint/active-lease data fails the
    /// whole pass so startup remains closed.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed active state, duplicate outcomes, or
    /// persistence failure.
    pub async fn recover_expired_leases(&self) -> Result<AiRunRecoveryReport, AiError> {
        let now = canonical_second(self.clock.now());
        let limits = self.limits;
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let mut candidates = Vec::new();
                    for state in [
                        AiRunState::Leased,
                        AiRunState::Running,
                        AiRunState::WaitingTool,
                        AiRunState::WaitingReauth,
                    ] {
                        let mut rows = tx
                            .query::<AiRunRecord>()
                            .filter(exact_state(state.as_str()))
                            .limit(limits.maximum_candidate_scan as i64)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        candidates.append(&mut rows);
                    }
                    candidates.sort_by_key(|run| (run.created_at, run.id));
                    candidates.truncate(limits.maximum_candidate_scan);

                    let mut report = AiRunRecoveryReport::default();
                    for current in candidates {
                        let Some(expires_at) = current.lease_expires_at else {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        };
                        let lease = lease_from_record(&current)?;
                        if expires_at > now.unix_timestamp() {
                            continue;
                        }
                        let current_state = persisted_state(&current)?;
                        let (final_checkpoint, adoptable_tool_batch) = if let Some(checkpoint_id) =
                            current.latest_checkpoint_id
                        {
                            let checkpoint = tx
                                .query::<AiRunCheckpointRecord>()
                                .filter(AiRunCheckpointRecordWhereInput {
                                    id: Some(UuidFilter {
                                        eq: Some(checkpoint_id),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                })
                                .limit(1)
                                .fetch_one()
                                .await
                                .map_err(OrmPublicError::from)?
                                .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                            if checkpoint.run_id != current.id
                                || checkpoint.attempt_id != lease.attempt_id
                                || checkpoint.lease_generation != lease.lease_generation
                            {
                                return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                            }
                            if checkpoint.checkpoint_kind == "assistant_output_persisted" {
                                let message_id =
                                    checkpoint.assistant_message_id.ok_or_else(|| {
                                        OrmPublicError::new(OrmErrorCode::InternalError)
                                    })?;
                                let budget_reservation_id =
                                    checkpoint.budget_reservation_id.ok_or_else(|| {
                                        OrmPublicError::new(OrmErrorCode::InternalError)
                                    })?;
                                let reservation = tx
                                    .find_by_id::<AiBudgetReservationRecord>(&budget_reservation_id)
                                    .await
                                    .map_err(OrmPublicError::from)?
                                    .ok_or_else(|| {
                                        OrmPublicError::new(OrmErrorCode::InternalError)
                                    })?;
                                if reservation.session_id != current.session_id
                                    || reservation.run_id != current.id
                                    || reservation.attempt_id != lease.attempt_id
                                    || reservation.lease_generation != lease.lease_generation
                                    || reservation.state != "committed"
                                    || reservation.actual_runs != Some(1)
                                    || reservation.reconciled_at.is_none()
                                {
                                    return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                                }
                                let expected_hash = final_output_checkpoint_hash(
                                    lease.run_id,
                                    lease.attempt_id,
                                    lease.lease_generation,
                                    message_id,
                                    checkpoint.provider_response_id.as_deref(),
                                    budget_reservation_id,
                                );
                                if checkpoint.checkpoint_hash != expected_hash {
                                    return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                                }
                                let message = tx
                                    .find_by_id::<AiMessageRecord>(&message_id)
                                    .await
                                    .map_err(OrmPublicError::from)?
                                    .ok_or_else(|| {
                                        OrmPublicError::new(OrmErrorCode::InternalError)
                                    })?;
                                if message.session_id != current.session_id
                                    || message.run_id != Some(current.id)
                                    || message.message_role != "assistant"
                                    || message.provider_kind.as_deref()
                                        != Some(reservation.provider_kind.as_str())
                                    || message.provider_model.as_deref()
                                        != Some(reservation.provider_model.as_str())
                                    || message.completion_state != "complete"
                                    || message.finalized_at.is_none()
                                    || !(1..=4_096).contains(&message.block_count)
                                {
                                    return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                                }
                                let mut blocks = tx
                                    .query::<AiMessageBlockRecord>()
                                    .filter(AiMessageBlockRecordWhereInput {
                                        message_id: Some(UuidFilter {
                                            eq: Some(message_id),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    })
                                    .limit(message.block_count + 1)
                                    .fetch_all()
                                    .await
                                    .map_err(OrmPublicError::from)?;
                                blocks.sort_by_key(|block| block.block_index);
                                if i64::try_from(blocks.len()).ok() != Some(message.block_count)
                                    || blocks.iter().enumerate().any(|(index, block)| {
                                        i64::try_from(index).ok() != Some(block.block_index)
                                            || block.byte_count < 0
                                            || block.line_count < 1
                                    })
                                {
                                    return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                                }
                                (Some(checkpoint.provider_response_id), false)
                            } else if matches!(
                                checkpoint.checkpoint_kind.as_str(),
                                "tool_batch_persisted" | "supervised_tool_batch_persisted"
                            ) {
                                let supervised =
                                    checkpoint.checkpoint_kind == "supervised_tool_batch_persisted";
                                let provider_response_id =
                                    checkpoint.provider_response_id.as_deref();
                                if provider_response_id
                                    .is_some_and(|value| !valid_provider_reference(value))
                                {
                                    return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                                }
                                let budget_reservation_id =
                                    checkpoint.budget_reservation_id.ok_or_else(|| {
                                        OrmPublicError::new(OrmErrorCode::InternalError)
                                    })?;
                                let protected_state = checkpoint
                                    .protected_state
                                    .as_ref()
                                    .filter(|state| {
                                        serde_json::to_vec(state)
                                            .is_ok_and(|encoded| encoded.len() <= 64 * 1024 * 1024)
                                    })
                                    .ok_or_else(|| {
                                        OrmPublicError::new(OrmErrorCode::InternalError)
                                    })?;
                                if checkpoint.assistant_message_id.is_some() {
                                    return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                                }
                                let reservation = tx
                                    .find_by_id::<AiBudgetReservationRecord>(&budget_reservation_id)
                                    .await
                                    .map_err(OrmPublicError::from)?
                                    .ok_or_else(|| {
                                        OrmPublicError::new(OrmErrorCode::InternalError)
                                    })?;
                                if reservation.session_id != current.session_id
                                    || reservation.run_id != current.id
                                    || reservation.attempt_id != lease.attempt_id
                                    || reservation.lease_generation != lease.lease_generation
                                    || reservation.state != "committed"
                                    || reservation.actual_runs != Some(1)
                                    || reservation.reconciled_at.is_none()
                                    || reservation.provider_kind.trim().is_empty()
                                    || reservation.provider_model.trim().is_empty()
                                {
                                    return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                                }
                                let expected_hash = coordinator_checkpoint_hash(
                                    lease.run_id,
                                    lease.attempt_id,
                                    lease.lease_generation,
                                    checkpoint.id,
                                    &checkpoint.checkpoint_kind,
                                    &reservation.provider_kind,
                                    &reservation.provider_model,
                                    provider_response_id,
                                    budget_reservation_id,
                                    protected_state,
                                )
                                .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
                                if checkpoint.checkpoint_hash != expected_hash {
                                    return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                                }
                                let calls = tx
                                    .query::<AiToolCallRecord>()
                                    .filter(AiToolCallRecordWhereInput {
                                        run_id: Some(UuidFilter {
                                            eq: Some(current.id),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    })
                                    .limit(4_097)
                                    .fetch_all()
                                    .await
                                    .map_err(OrmPublicError::from)?;
                                let relevant = calls
                                    .iter()
                                    .filter(|call| {
                                        call.lease_generation == lease.lease_generation
                                            && call.provider_response_id.as_deref()
                                                == provider_response_id
                                            && call.budget_reservation_id
                                                == Some(budget_reservation_id)
                                    })
                                    .collect::<Vec<_>>();
                                if relevant.is_empty()
                                    || relevant.len() > 4_096
                                    || (supervised && relevant.len() != 1)
                                    || relevant.iter().any(|call| {
                                        !matches!(
                                            call.state.as_str(),
                                            "completed" | "execution_failed"
                                        ) || call.protected_result.is_none()
                                            || call.result_egress_decision_id.is_none()
                                            || call.result_egress_manifest_hash.is_none()
                                            || call.completed_at.is_none()
                                    })
                                {
                                    return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                                }
                                for call in relevant {
                                    let authorization_matches = if supervised {
                                        let Some(approval_id) = call.approval_id else {
                                            return Err(OrmPublicError::new(
                                                OrmErrorCode::InternalError,
                                            ));
                                        };
                                        let approval = tx
                                            .find_by_id::<AiApprovalRecord>(&approval_id)
                                            .await
                                            .map_err(OrmPublicError::from)?
                                            .ok_or_else(|| {
                                                OrmPublicError::new(OrmErrorCode::InternalError)
                                            })?;
                                        matches!(
                                            call.risk.as_str(),
                                            "low_risk_write"
                                                | "non_idempotent_write"
                                                | "high_impact"
                                        ) && approval.tool_call_id == call.id
                                            && approval.session_id == current.session_id
                                            && approval.state == "consumed"
                                            && approval.maximum_uses == 1
                                            && approval.consumed_uses == 1
                                            && approval.consumed_at.is_some()
                                            && approval.argument_hash == call.argument_hash
                                            && approval.tool_fingerprint == call.tool_fingerprint
                                            && call.authorization_policy_version.as_deref()
                                                == Some(approval.policy_version.as_str())
                                            && call.authorization_state_digest.as_deref()
                                                == Some(
                                                    approval.authorization_state_digest.as_str(),
                                                )
                                    } else {
                                        call.risk == "read_only" && call.approval_id.is_none()
                                    };
                                    let step = tx
                                        .find_by_id::<AiRunStepRecord>(&call.id)
                                        .await
                                        .map_err(OrmPublicError::from)?
                                        .ok_or_else(|| {
                                            OrmPublicError::new(OrmErrorCode::InternalError)
                                        })?;
                                    if step.run_id != current.id
                                        || step.lease_generation != lease.lease_generation
                                        || step.state != call.state
                                        || step.finished_at.is_none()
                                        || !authorization_matches
                                    {
                                        return Err(OrmPublicError::new(
                                            OrmErrorCode::InternalError,
                                        ));
                                    }
                                }
                                // Read-only provider-retained/stateless batches
                                // and the narrow supervised provider-retained
                                // batch have complete durable
                                // tool/approval/step/budget proof here. The
                                // adopter still opens and validates every
                                // protected row under current authority before
                                // transport.
                                (None, true)
                            } else {
                                (None, false)
                            }
                        } else {
                            (None, false)
                        };
                        if (final_checkpoint.is_some() || adoptable_tool_batch)
                            && current_state != AiRunState::Running
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                        let (
                            next_state,
                            outcome_code,
                            next_retry_count,
                            next_attempt_at,
                            provider_response_id,
                        ) = if current_state == AiRunState::Leased {
                            let retry_count = u32::try_from(current.retry_count)
                                .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
                            if retry_count >= limits.maximum_run_retries {
                                report.failed = report.failed.saturating_add(1);
                                (
                                    AiRunState::Failed,
                                    "lease_expired_retry_exhausted",
                                    current.retry_count,
                                    None,
                                    None,
                                )
                            } else {
                                report.requeued = report.requeued.saturating_add(1);
                                (
                                    AiRunState::RetryScheduled,
                                    "lease_expired_before_start",
                                    current.retry_count.checked_add(1).ok_or_else(|| {
                                        OrmPublicError::new(OrmErrorCode::InternalError)
                                    })?,
                                    Some(now.unix_timestamp()),
                                    None,
                                )
                            }
                        } else if let Some(provider_response_id) = final_checkpoint {
                            report.completed = report.completed.saturating_add(1);
                            (
                                AiRunState::Completed,
                                "lease_expired_after_output_persisted",
                                current.retry_count,
                                None,
                                provider_response_id,
                            )
                        } else if adoptable_tool_batch {
                            let retry_count = u32::try_from(current.retry_count)
                                .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
                            if retry_count >= limits.maximum_run_retries {
                                report.recovery_required =
                                    report.recovery_required.saturating_add(1);
                                (
                                    AiRunState::RecoveryRequired,
                                    "checkpoint_adoption_retry_exhausted",
                                    current.retry_count,
                                    None,
                                    None,
                                )
                            } else {
                                report.checkpoint_requeued =
                                    report.checkpoint_requeued.saturating_add(1);
                                (
                                    AiRunState::RetryScheduled,
                                    "checkpoint_adoption_ready",
                                    current.retry_count.checked_add(1).ok_or_else(|| {
                                        OrmPublicError::new(OrmErrorCode::InternalError)
                                    })?,
                                    Some(now.unix_timestamp()),
                                    None,
                                )
                            }
                        } else {
                            report.recovery_required = report.recovery_required.saturating_add(1);
                            (
                                AiRunState::RecoveryRequired,
                                "lease_expired_after_start",
                                current.retry_count,
                                None,
                                None,
                            )
                        };
                        let update = tx
                            .compare_and_swap::<AiRunRecord>(
                                &current.id,
                                current.row_version,
                                exact_state(&current.state),
                                UpdateAiRunRecordInput {
                                    state: Some(next_state.as_str().to_owned()),
                                    attempt_id: Some(None),
                                    lease_owner: Some(None),
                                    lease_expires_at: Some(None),
                                    lease_heartbeat_at: Some(None),
                                    retry_count: Some(next_retry_count),
                                    next_attempt_at: Some(next_attempt_at),
                                    error_code: Some(
                                        (next_state != AiRunState::Completed)
                                            .then_some(outcome_code.to_owned()),
                                    ),
                                    ..Default::default()
                                },
                            )
                            .await
                            .map_err(OrmPublicError::from)?;
                        if !matches!(update, ConditionalUpdateOutcome::Updated(_)) {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                        append_attempt_outcome(
                            tx,
                            &lease,
                            next_state,
                            outcome_code.to_owned(),
                            provider_response_id,
                            now,
                        )
                        .await?;
                    }
                    Ok(report)
                })
            })
            .await
            .map_err(map_transaction)
    }

    pub(crate) async fn append_provider_output(
        &self,
        lease: &AiRunLease,
        output: PreparedProviderOutput,
    ) -> Result<AiRunLease, AiError> {
        let now = canonical_second(self.clock.now());
        let lease_ttl = self.limits.lease_ttl;
        let lease = lease.clone();
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = load_and_validate_active_lease(tx, &lease, now).await?;
                    if persisted_state(&current)? != AiRunState::Running || output.blocks.is_empty()
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&lease.session_id.0)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if session.state != "active"
                        || session.deleted_at.is_some()
                        || session.owner_principal_kind != output.expected_owner_principal_kind
                        || session.owner_subject != output.expected_owner_subject
                        || session.scope_kind != output.expected_scope_kind
                        || session.scope_id != output.expected_scope_id
                        || session.tenant_id != output.expected_tenant_id
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let message_sequence = session
                        .message_head
                        .checked_add(1)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let event_sequence = session
                        .stream_head
                        .checked_add(1)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let session_update = tx
                        .compare_and_swap::<AiSessionRecord>(
                            &session.id,
                            session.row_version,
                            AiSessionRecordWhereInput::default(),
                            UpdateAiSessionRecordInput {
                                message_head: Some(message_sequence),
                                stream_head: Some(event_sequence),
                                last_activity_at: Some(now.unix_timestamp()),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(session_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let expiry = now
                        .checked_add(lease_ttl)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let run_update = tx
                        .compare_and_swap::<AiRunRecord>(
                            &current.id,
                            current.row_version,
                            exact_state(AiRunState::Running.as_str()),
                            UpdateAiRunRecordInput {
                                lease_expires_at: Some(Some(expiry.unix_timestamp())),
                                lease_heartbeat_at: Some(Some(now.unix_timestamp())),
                                latest_checkpoint_id: Some(Some(output.message_id)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    let updated_run = match run_update {
                        ConditionalUpdateOutcome::Updated(updated) => updated,
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    tx.insert::<AiMessageRecord>(CreateAiMessageRecordInput {
                        id: output.message_id,
                        session_id: lease.session_id.0,
                        sequence: message_sequence,
                        message_role: "assistant".to_owned(),
                        author_principal_kind: None,
                        author_subject: None,
                        client_message_id: None,
                        content_hash: None,
                        run_id: Some(lease.run_id.0),
                        provider_kind: Some(output.provider_kind),
                        provider_model: Some(output.provider_model),
                        protected_preview: Some(output.protected_preview),
                        block_count: i64::try_from(output.blocks.len())
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::InvalidInput))?,
                        completion_state: "complete".to_owned(),
                        finalized_at: Some(now.unix_timestamp()),
                        content_purged_at: None,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.insert::<AiRunCheckpointRecord>(CreateAiRunCheckpointRecordInput {
                        id: output.message_id,
                        run_id: lease.run_id.0,
                        attempt_id: lease.attempt_id,
                        lease_generation: lease.lease_generation,
                        checkpoint_kind: "assistant_output_persisted".to_owned(),
                        provider_response_id: output.provider_response_id,
                        budget_reservation_id: Some(output.budget_reservation_id),
                        assistant_message_id: Some(output.message_id),
                        protected_state: None,
                        checkpoint_hash: output.checkpoint_hash,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    for (block_index, block) in output.blocks.into_iter().enumerate() {
                        tx.insert::<AiMessageBlockRecord>(CreateAiMessageBlockRecordInput {
                            id: block.id,
                            message_id: output.message_id,
                            block_index: i64::try_from(block_index)
                                .map_err(|_| OrmPublicError::new(OrmErrorCode::InvalidInput))?,
                            block_kind: block.kind,
                            protected_content: block.protected_content,
                            byte_count: block.byte_count,
                            line_count: block.line_count,
                        })
                        .await
                        .map_err(OrmPublicError::from)?;
                    }
                    tx.insert::<AiSessionEventRecord>(CreateAiSessionEventRecordInput {
                        id: output.event_id,
                        session_id: lease.session_id.0,
                        sequence: event_sequence,
                        event_type: "assistant_message_completed".to_owned(),
                        run_id: Some(lease.run_id.0),
                        causation_id: Some(lease.input_message_id.to_string()),
                        correlation_id: output.correlation_id,
                        protected_payload: output.protected_event,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.queue_event(AiSessionWakeup {
                        session_id: lease.session_id.0,
                        sequence: event_sequence,
                    });
                    append_inbox_event(
                        tx,
                        PreparedAiInboxEvent {
                            id: output.inbox_event_id,
                            principal_kind: output.expected_owner_principal_kind,
                            principal_subject: output.expected_owner_subject,
                            scope: crate::AiScope {
                                kind: output.expected_scope_kind,
                                id: output.expected_scope_id,
                                tenant_id: output.expected_tenant_id,
                            },
                            session_id: lease.session_id.0,
                            event_type: "assistant_message_completed".to_owned(),
                            protected_payload: output.protected_inbox_event,
                            created_at: now.unix_timestamp(),
                        },
                    )
                    .await?;
                    lease_from_record(&updated_run)
                })
            })
            .await
            .map_err(map_transaction)
    }

    pub(crate) async fn append_context_checkpoint(
        &self,
        lease: &AiRunLease,
        checkpoint: PreparedContextCheckpoint,
    ) -> Result<AiRunLease, AiError> {
        let now = canonical_second(self.clock.now());
        let lease_ttl = self.limits.lease_ttl;
        let lease = lease.clone();
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = load_and_validate_active_lease(tx, &lease, now).await?;
                    if persisted_state(&current)? != AiRunState::Running
                        || checkpoint.id.is_nil()
                        || checkpoint.through_sequence <= 0
                        || checkpoint.source_hash.len() != 64
                        || !checkpoint
                            .source_hash
                            .bytes()
                            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
                        || checkpoint.token_estimate <= 0
                        || checkpoint.provider_kind.trim().is_empty()
                        || checkpoint.provider_model.trim().is_empty()
                        || checkpoint.sources.is_empty()
                        || !(1..=5_000).contains(&checkpoint.maximum_checkpoints_per_session)
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&lease.session_id.0)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if session.state != "active"
                        || session.deleted_at.is_some()
                        || session.message_head < checkpoint.through_sequence
                        || session.owner_principal_kind != checkpoint.expected_owner_principal_kind
                        || session.owner_subject != checkpoint.expected_owner_subject
                        || session.scope_kind != checkpoint.expected_scope_kind
                        || session.scope_id != checkpoint.expected_scope_id
                        || session.tenant_id != checkpoint.expected_tenant_id
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }

                    let checkpoint_limit =
                        i64::try_from(checkpoint.maximum_checkpoints_per_session.saturating_add(1))
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::InvalidInput))?;
                    let existing = tx
                        .query::<AiContextCheckpointRecord>()
                        .filter(AiContextCheckpointRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session.id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .default_order()
                        .limit(checkpoint_limit)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if existing.len() > checkpoint.maximum_checkpoints_per_session {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let current_parent = existing
                        .iter()
                        .find(|candidate| candidate.invalidated_at.is_none());
                    if current_parent != checkpoint.expected_parent.as_ref()
                        || current_parent.is_some_and(|parent| {
                            parent.through_sequence >= checkpoint.through_sequence
                        })
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }

                    let expected_start = current_parent
                        .map_or(1, |parent| parent.through_sequence.saturating_add(1));
                    let expected_source_count = checkpoint
                        .through_sequence
                        .checked_sub(expected_start)
                        .and_then(|value| value.checked_add(1))
                        .and_then(|value| usize::try_from(value).ok())
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::Conflict))?;
                    if checkpoint.sources.len() != expected_source_count {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let start = i32::try_from(expected_start)
                        .map_err(|_| OrmPublicError::new(OrmErrorCode::Conflict))?;
                    let through = i32::try_from(checkpoint.through_sequence)
                        .map_err(|_| OrmPublicError::new(OrmErrorCode::Conflict))?;
                    let message_limit = i64::try_from(expected_source_count.saturating_add(1))
                        .map_err(|_| OrmPublicError::new(OrmErrorCode::Conflict))?;
                    let messages = tx
                        .query::<AiMessageRecord>()
                        .filter(AiMessageRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session.id),
                                ..Default::default()
                            }),
                            sequence: Some(IntFilter {
                                gte: Some(start),
                                lte: Some(through),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .default_order()
                        .limit(message_limit)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if messages.len() != expected_source_count {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    for (offset, (message, expected)) in
                        messages.iter().zip(&checkpoint.sources).enumerate()
                    {
                        let sequence = expected_start
                            .checked_add(
                                i64::try_from(offset)
                                    .map_err(|_| OrmPublicError::new(OrmErrorCode::Conflict))?,
                            )
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::Conflict))?;
                        if message != &expected.message
                            || message.sequence != sequence
                            || message.content_purged_at.is_some()
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                        let block_limit = i64::try_from(expected.blocks.len().saturating_add(1))
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::Conflict))?;
                        let blocks = tx
                            .query::<AiMessageBlockRecord>()
                            .filter(AiMessageBlockRecordWhereInput {
                                message_id: Some(UuidFilter {
                                    eq: Some(message.id),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .default_order()
                            .limit(block_limit)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        if blocks != expected.blocks {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    }

                    let expiry = now
                        .checked_add(lease_ttl)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let run_update = tx
                        .compare_and_swap::<AiRunRecord>(
                            &current.id,
                            current.row_version,
                            exact_state(AiRunState::Running.as_str()),
                            UpdateAiRunRecordInput {
                                lease_expires_at: Some(Some(expiry.unix_timestamp())),
                                lease_heartbeat_at: Some(Some(now.unix_timestamp())),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    let updated_run = match run_update {
                        ConditionalUpdateOutcome::Updated(updated) => updated,
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    tx.insert::<AiContextCheckpointRecord>(CreateAiContextCheckpointRecordInput {
                        id: checkpoint.id,
                        session_id: session.id,
                        through_sequence: checkpoint.through_sequence,
                        source_hash: checkpoint.source_hash,
                        token_estimate: checkpoint.token_estimate,
                        provider_kind: checkpoint.provider_kind,
                        provider_model: checkpoint.provider_model,
                        protected_summary: checkpoint.protected_summary,
                        invalidated_at: None,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    lease_from_record(&updated_run)
                })
            })
            .await
            .map_err(map_transaction)
    }

    pub(crate) async fn append_coordinator_checkpoint(
        &self,
        lease: &AiRunLease,
        checkpoint: PreparedCoordinatorCheckpoint,
    ) -> Result<AiRunLease, AiError> {
        let now = canonical_second(self.clock.now());
        let lease_ttl = self.limits.lease_ttl;
        let lease = lease.clone();
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = load_and_validate_active_lease(tx, &lease, now).await?;
                    let valid_kind = match checkpoint.checkpoint_kind.as_str() {
                        "provider_turn_persisted" => checkpoint.completed_tools.is_empty(),
                        "tool_batch_persisted" => !checkpoint.completed_tools.is_empty(),
                        "supervised_tool_batch_persisted" => checkpoint.completed_tools.len() == 1,
                        _ => false,
                    };
                    if persisted_state(&current)? != AiRunState::Running
                        || !valid_kind
                        || checkpoint.provider_kind.trim().is_empty()
                        || checkpoint.provider_model.trim().is_empty()
                        || checkpoint.checkpoint_hash.len() != 64
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let reservation = tx
                        .find_by_id::<AiBudgetReservationRecord>(&checkpoint.budget_reservation_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if reservation.session_id != lease.session_id.0
                        || reservation.run_id != lease.run_id.0
                        || reservation.attempt_id != lease.attempt_id
                        || reservation.lease_generation != lease.lease_generation
                        || reservation.provider_kind != checkpoint.provider_kind
                        || reservation.provider_model != checkpoint.provider_model
                        || reservation.state != "committed"
                        || reservation.actual_runs != Some(1)
                        || reservation.reconciled_at.is_none()
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    for expected in &checkpoint.completed_tools {
                        let call = tx
                            .find_by_id::<AiToolCallRecord>(&expected.id)
                            .await
                            .map_err(OrmPublicError::from)?
                            .ok_or_else(OrmPublicError::not_found)?;
                        let step = tx
                            .find_by_id::<AiRunStepRecord>(&expected.id)
                            .await
                            .map_err(OrmPublicError::from)?
                            .ok_or_else(OrmPublicError::not_found)?;
                        let authorization_matches = match checkpoint.checkpoint_kind.as_str() {
                            "tool_batch_persisted" => {
                                call.risk == "read_only" && call.approval_id.is_none()
                            }
                            "supervised_tool_batch_persisted" => {
                                let Some(approval_id) = call.approval_id else {
                                    return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                                };
                                let approval = tx
                                    .find_by_id::<AiApprovalRecord>(&approval_id)
                                    .await
                                    .map_err(OrmPublicError::from)?
                                    .ok_or_else(OrmPublicError::not_found)?;
                                matches!(
                                    call.risk.as_str(),
                                    "low_risk_write" | "non_idempotent_write" | "high_impact"
                                ) && approval.tool_call_id == call.id
                                    && approval.session_id == lease.session_id.0
                                    && approval.state == "consumed"
                                    && approval.maximum_uses == 1
                                    && approval.consumed_uses == 1
                                    && approval.consumed_at.is_some()
                                    && approval.argument_hash == call.argument_hash
                                    && approval.tool_fingerprint == call.tool_fingerprint
                                    && call.authorization_policy_version.as_deref()
                                        == Some(approval.policy_version.as_str())
                                    && call.authorization_state_digest.as_deref()
                                        == Some(approval.authorization_state_digest.as_str())
                            }
                            _ => false,
                        };
                        if call.run_id != lease.run_id.0
                            || call.lease_generation != lease.lease_generation
                            || call.provider_call_id != expected.provider_call_id
                            || call.tool_id != expected.tool_id
                            || call.provider_kind.as_deref()
                                != Some(checkpoint.provider_kind.as_str())
                            || call.provider_model.as_deref()
                                != Some(checkpoint.provider_model.as_str())
                            || call.provider_response_id != checkpoint.provider_response_id
                            || call.budget_reservation_id != Some(checkpoint.budget_reservation_id)
                            || !matches!(call.state.as_str(), "completed" | "execution_failed")
                            || call.protected_result.is_none()
                            || call.result_egress_decision_id.is_none()
                            || call.result_egress_manifest_hash.as_deref()
                                != Some(expected.result_egress_manifest_hash.as_str())
                            || call.completed_at.is_none()
                            || step.run_id != lease.run_id.0
                            || step.lease_generation != lease.lease_generation
                            || step.state != call.state
                            || step.finished_at.is_none()
                            || !authorization_matches
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    }
                    if matches!(
                        checkpoint.checkpoint_kind.as_str(),
                        "tool_batch_persisted" | "supervised_tool_batch_persisted"
                    ) {
                        let expected_ids = checkpoint
                            .completed_tools
                            .iter()
                            .map(|tool| tool.id)
                            .collect::<BTreeSet<_>>();
                        let calls = tx
                            .query::<AiToolCallRecord>()
                            .filter(AiToolCallRecordWhereInput {
                                run_id: Some(UuidFilter {
                                    eq: Some(lease.run_id.0),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .limit(4_097)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        let actual_ids = calls
                            .iter()
                            .filter(|call| {
                                call.lease_generation == lease.lease_generation
                                    && call.provider_response_id.as_deref()
                                        == checkpoint.provider_response_id.as_deref()
                                    && call.budget_reservation_id
                                        == Some(checkpoint.budget_reservation_id)
                            })
                            .map(|call| call.id)
                            .collect::<BTreeSet<_>>();
                        if actual_ids != expected_ids {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    }
                    let expiry = now
                        .checked_add(lease_ttl)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let run_update = tx
                        .compare_and_swap::<AiRunRecord>(
                            &current.id,
                            current.row_version,
                            exact_state(AiRunState::Running.as_str()),
                            UpdateAiRunRecordInput {
                                lease_expires_at: Some(Some(expiry.unix_timestamp())),
                                lease_heartbeat_at: Some(Some(now.unix_timestamp())),
                                latest_checkpoint_id: Some(Some(checkpoint.id)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    let updated_run = match run_update {
                        ConditionalUpdateOutcome::Updated(updated) => updated,
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    tx.insert::<AiRunCheckpointRecord>(CreateAiRunCheckpointRecordInput {
                        id: checkpoint.id,
                        run_id: lease.run_id.0,
                        attempt_id: lease.attempt_id,
                        lease_generation: lease.lease_generation,
                        checkpoint_kind: checkpoint.checkpoint_kind,
                        provider_response_id: checkpoint.provider_response_id,
                        budget_reservation_id: Some(checkpoint.budget_reservation_id),
                        assistant_message_id: None,
                        protected_state: Some(checkpoint.protected_state),
                        checkpoint_hash: checkpoint.checkpoint_hash,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    lease_from_record(&updated_run)
                })
            })
            .await
            .map_err(map_transaction)
    }

    pub(crate) async fn consume_adoption_checkpoint(
        &self,
        lease: &AiRunLease,
        checkpoint_id: Uuid,
        expected_kind: &'static str,
    ) -> Result<AiRunLease, AiError> {
        if !matches!(
            expected_kind,
            "tool_batch_persisted" | "supervised_tool_batch_persisted"
        ) {
            return Err(AiError::Conflict);
        }
        let now = canonical_second(self.clock.now());
        let lease_ttl = self.limits.lease_ttl;
        let lease = lease.clone();
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = load_and_validate_active_lease(tx, &lease, now).await?;
                    if persisted_state(&current)? != AiRunState::Running
                        || current.latest_checkpoint_id != Some(checkpoint_id)
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let checkpoint = tx
                        .query::<AiRunCheckpointRecord>()
                        .filter(AiRunCheckpointRecordWhereInput {
                            id: Some(UuidFilter {
                                eq: Some(checkpoint_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(1)
                        .fetch_one()
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if checkpoint.run_id != current.id
                        || checkpoint.checkpoint_kind != expected_kind
                        || checkpoint.budget_reservation_id.is_none()
                        || checkpoint.assistant_message_id.is_some()
                        || checkpoint.protected_state.is_none()
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let expiry = now
                        .checked_add(lease_ttl)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let update = tx
                        .compare_and_swap::<AiRunRecord>(
                            &current.id,
                            current.row_version,
                            exact_state(AiRunState::Running.as_str()),
                            UpdateAiRunRecordInput {
                                latest_checkpoint_id: Some(None),
                                lease_expires_at: Some(Some(expiry.unix_timestamp())),
                                lease_heartbeat_at: Some(Some(now.unix_timestamp())),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    match update {
                        ConditionalUpdateOutcome::Updated(updated) => lease_from_record(&updated),
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            Err(OrmPublicError::new(OrmErrorCode::Conflict))
                        }
                    }
                })
            })
            .await
            .map_err(map_transaction)
    }

    pub(crate) async fn append_live_delta_event(
        &self,
        lease: &AiRunLease,
        event: PreparedLiveDeltaEvent,
    ) -> Result<(), AiError> {
        if event.event_type != "provider_live_delta"
            || !valid_provider_kind(&event.provider_kind)
            || !valid_provider_reference(&event.provider_model)
            || !valid_provider_reference(&event.correlation_id)
            || event.expected_owner_principal_kind.trim().is_empty()
            || event.expected_owner_subject.trim().is_empty()
            || event.expected_scope_kind.trim().is_empty()
            || event.expected_scope_id.trim().is_empty()
        {
            return Err(AiError::InvalidInput(
                "invalid protected live-delta event".to_owned(),
            ));
        }
        let now = canonical_second(self.clock.now());
        let lease = lease.clone();
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = tx
                        .find_by_id::<AiRunRecord>(&lease.run_id.0)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    let stored_reference: PrincipalReference =
                        serde_json::from_value(current.principal_reference.clone())
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    if persisted_state(&current)? != AiRunState::Running
                        || current.session_id != lease.session_id.0
                        || current.input_message_id != lease.input_message_id
                        || stored_reference != lease.principal_reference
                        || current.attempt_id != Some(lease.attempt_id)
                        || current.lease_owner.as_deref() != Some(lease.worker_id.as_str())
                        || current.lease_generation != lease.lease_generation
                        || current
                            .lease_expires_at
                            .is_none_or(|expires_at| expires_at <= now.unix_timestamp())
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let reservation = tx
                        .find_by_id::<AiBudgetReservationRecord>(&event.budget_reservation_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if reservation.session_id != lease.session_id.0
                        || reservation.run_id != lease.run_id.0
                        || reservation.attempt_id != lease.attempt_id
                        || reservation.lease_generation != lease.lease_generation
                        || reservation.provider_kind != event.provider_kind
                        || reservation.provider_model != event.provider_model
                        || reservation.state != "uncertain"
                        || reservation.actual_runs.is_some()
                        || reservation.reconciled_at.is_none()
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&lease.session_id.0)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if session.state != "active"
                        || session.deleted_at.is_some()
                        || session.owner_principal_kind != event.expected_owner_principal_kind
                        || session.owner_subject != event.expected_owner_subject
                        || session.scope_kind != event.expected_scope_kind
                        || session.scope_id != event.expected_scope_id
                        || session.tenant_id != event.expected_tenant_id
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let event_sequence = session
                        .stream_head
                        .checked_add(1)
                        .filter(|sequence| *sequence <= i64::from(i32::MAX))
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let update = tx
                        .compare_and_swap::<AiSessionRecord>(
                            &session.id,
                            session.row_version,
                            AiSessionRecordWhereInput::default(),
                            UpdateAiSessionRecordInput {
                                stream_head: Some(event_sequence),
                                last_activity_at: Some(now.unix_timestamp()),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    tx.insert::<AiSessionEventRecord>(CreateAiSessionEventRecordInput {
                        id: event.id,
                        session_id: session.id,
                        sequence: event_sequence,
                        event_type: event.event_type,
                        run_id: Some(lease.run_id.0),
                        causation_id: Some(lease.input_message_id.to_string()),
                        correlation_id: event.correlation_id,
                        protected_payload: event.protected_payload,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.queue_event(AiSessionWakeup {
                        session_id: session.id,
                        sequence: event_sequence,
                    });
                    Ok(())
                })
            })
            .await
            .map_err(map_transaction)
    }

    pub(crate) async fn append_ui_intent_event(
        &self,
        lease: &AiRunLease,
        event: PreparedUiIntentEvent,
    ) -> Result<(AiRunLease, i64), AiError> {
        if !valid_provider_kind(&event.provider_kind)
            || !valid_provider_reference(&event.provider_model)
            || event
                .provider_response_id
                .as_deref()
                .is_some_and(|value| !valid_provider_reference(value))
            || !valid_provider_reference(&event.correlation_id)
            || event.expected_owner_principal_kind.trim().is_empty()
            || event.expected_owner_subject.trim().is_empty()
            || event.expected_scope_kind.trim().is_empty()
            || event.expected_scope_id.trim().is_empty()
        {
            return Err(AiError::InvalidInput(
                "invalid protected UI intent event".to_owned(),
            ));
        }
        let now = canonical_second(self.clock.now());
        let lease_ttl = self.limits.lease_ttl;
        let lease = lease.clone();
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = tx
                        .find_by_id::<AiRunRecord>(&lease.run_id.0)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    let stored_reference: PrincipalReference =
                        serde_json::from_value(current.principal_reference.clone())
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    if persisted_state(&current)? != AiRunState::Running
                        || current.session_id != lease.session_id.0
                        || current.input_message_id != lease.input_message_id
                        || stored_reference != lease.principal_reference
                        || current.attempt_id != Some(lease.attempt_id)
                        || current.lease_owner.as_deref() != Some(lease.worker_id.as_str())
                        || current.lease_generation != lease.lease_generation
                        || current.retry_count != i64::from(lease.retry_count)
                        || current.latest_checkpoint_id != lease.latest_checkpoint_id
                        || current
                            .lease_expires_at
                            .is_none_or(|expiry| expiry <= now.unix_timestamp())
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }

                    let checkpoint_id = current
                        .latest_checkpoint_id
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::Conflict))?;
                    let checkpoint = tx
                        .query::<AiRunCheckpointRecord>()
                        .filter(AiRunCheckpointRecordWhereInput {
                            id: Some(UuidFilter {
                                eq: Some(checkpoint_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(1)
                        .fetch_one()
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::Conflict))?;
                    if checkpoint.run_id != lease.run_id.0
                        || checkpoint.attempt_id != lease.attempt_id
                        || checkpoint.lease_generation != lease.lease_generation
                        || checkpoint.checkpoint_kind != "assistant_output_persisted"
                        || checkpoint.provider_response_id != event.provider_response_id
                        || checkpoint.budget_reservation_id != Some(event.budget_reservation_id)
                        || checkpoint.assistant_message_id != Some(checkpoint_id)
                        || checkpoint.protected_state.is_some()
                        || checkpoint.checkpoint_hash
                            != final_output_checkpoint_hash(
                                lease.run_id,
                                lease.attempt_id,
                                lease.lease_generation,
                                checkpoint_id,
                                event.provider_response_id.as_deref(),
                                event.budget_reservation_id,
                            )
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }

                    if let Some(existing) = tx
                        .find_by_id::<AiSessionEventRecord>(&event.id)
                        .await
                        .map_err(OrmPublicError::from)?
                    {
                        let inbox = tx
                            .find_by_id::<AiInboxEventRecord>(&event.inbox_event_id)
                            .await
                            .map_err(OrmPublicError::from)?
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::Conflict))?;
                        if lease.row_version.checked_add(1) != Some(current.row_version)
                            || existing.session_id != lease.session_id.0
                            || existing.event_type != "ui_intent_suggested"
                            || existing.run_id != Some(lease.run_id.0)
                            || existing.correlation_id != event.correlation_id
                            || inbox.session_id != Some(lease.session_id.0)
                            || inbox.event_type != "ui_intent_suggested"
                            || inbox.principal_kind != event.expected_owner_principal_kind
                            || inbox.principal_subject != event.expected_owner_subject
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                        return Ok((lease_from_record(&current)?, existing.sequence));
                    }
                    if current.row_version != lease.row_version {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }

                    let reservation = tx
                        .find_by_id::<AiBudgetReservationRecord>(&event.budget_reservation_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if reservation.scope_kind != event.expected_scope_kind
                        || reservation.scope_id != event.expected_scope_id
                        || reservation.tenant_id != event.expected_tenant_id
                        || reservation.principal_kind != event.expected_owner_principal_kind
                        || reservation.principal_subject != event.expected_owner_subject
                        || reservation.session_id != lease.session_id.0
                        || reservation.run_id != lease.run_id.0
                        || reservation.attempt_id != lease.attempt_id
                        || reservation.lease_generation != lease.lease_generation
                        || reservation.provider_kind != event.provider_kind
                        || reservation.provider_model != event.provider_model
                        || reservation.state != "committed"
                        || reservation.reconciled_at.is_none()
                        || !reservation_usage_matches(
                            &reservation,
                            event.usage,
                            event.cached_input_tokens,
                        )?
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&lease.session_id.0)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if session.state != "active"
                        || session.deleted_at.is_some()
                        || session.owner_principal_kind != event.expected_owner_principal_kind
                        || session.owner_subject != event.expected_owner_subject
                        || session.scope_kind != event.expected_scope_kind
                        || session.scope_id != event.expected_scope_id
                        || session.tenant_id != event.expected_tenant_id
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let event_sequence = session
                        .stream_head
                        .checked_add(1)
                        .filter(|sequence| *sequence <= i64::from(i32::MAX))
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    if !matches!(
                        tx.compare_and_swap::<AiSessionRecord>(
                            &session.id,
                            session.row_version,
                            AiSessionRecordWhereInput::default(),
                            UpdateAiSessionRecordInput {
                                stream_head: Some(event_sequence),
                                last_activity_at: Some(now.unix_timestamp()),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?,
                        ConditionalUpdateOutcome::Updated(_)
                    ) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let expiry = now
                        .checked_add(lease_ttl)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let updated_run = match tx
                        .compare_and_swap::<AiRunRecord>(
                            &current.id,
                            current.row_version,
                            exact_state(AiRunState::Running.as_str()),
                            UpdateAiRunRecordInput {
                                lease_expires_at: Some(Some(expiry.unix_timestamp())),
                                lease_heartbeat_at: Some(Some(now.unix_timestamp())),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?
                    {
                        ConditionalUpdateOutcome::Updated(updated) => updated,
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    tx.insert::<AiSessionEventRecord>(CreateAiSessionEventRecordInput {
                        id: event.id,
                        session_id: session.id,
                        sequence: event_sequence,
                        event_type: "ui_intent_suggested".to_owned(),
                        run_id: Some(lease.run_id.0),
                        causation_id: Some(lease.input_message_id.to_string()),
                        correlation_id: event.correlation_id.clone(),
                        protected_payload: event.protected_payload,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.queue_event(AiSessionWakeup {
                        session_id: session.id,
                        sequence: event_sequence,
                    });
                    append_inbox_event(
                        tx,
                        PreparedAiInboxEvent {
                            id: event.inbox_event_id,
                            principal_kind: event.expected_owner_principal_kind,
                            principal_subject: event.expected_owner_subject.clone(),
                            scope: crate::AiScope {
                                kind: event.expected_scope_kind,
                                id: event.expected_scope_id,
                                tenant_id: event.expected_tenant_id,
                            },
                            session_id: session.id,
                            event_type: "ui_intent_suggested".to_owned(),
                            protected_payload: event.protected_inbox_payload,
                            created_at: now.unix_timestamp(),
                        },
                    )
                    .await?;
                    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                        actor_principal_kind: reservation.principal_kind,
                        actor_subject: event.expected_owner_subject,
                        action: "ai.ui_intent.persist".to_owned(),
                        resource_kind: "ai_ui_intent".to_owned(),
                        resource_reference: event.id.to_string(),
                        outcome: "allowed".to_owned(),
                        reason_code: "validated_ui_intent_suggested".to_owned(),
                        correlation_id: event.correlation_id,
                        causation_id: Some(lease.input_message_id.to_string()),
                        policy_version: None,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    Ok((lease_from_record(&updated_run)?, event_sequence))
                })
            })
            .await
            .map_err(map_transaction)
    }

    pub(crate) async fn begin_tool_call(
        &self,
        lease: &AiRunLease,
        call: PreparedToolCallStart,
    ) -> Result<(), AiError> {
        let now = canonical_second(self.clock.now());
        let lease = lease.clone();
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = load_and_validate_active_lease(tx, &lease, now).await?;
                    if persisted_state(&current)? != AiRunState::Running {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&lease.session_id.0)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if !session_matches_tool_start(&session, &call) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    tx.insert::<AiRunStepRecord>(CreateAiRunStepRecordInput {
                        id: call.id,
                        run_id: lease.run_id.0,
                        step_index: call
                            .provider_turn_index
                            .checked_mul(64)
                            .and_then(|value| value.checked_add(call.tool_call_index))
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InvalidInput))?,
                        step_kind: "application_tool".to_owned(),
                        state: "running".to_owned(),
                        lease_generation: lease.lease_generation,
                        started_at: Some(now.unix_timestamp()),
                        finished_at: None,
                        error_code: None,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.insert::<AiToolCallRecord>(CreateAiToolCallRecordInput {
                        id: call.id,
                        run_id: lease.run_id.0,
                        provider_call_key: call.provider_call_key,
                        provider_call_id: call.provider_call_id,
                        provider_kind: Some(call.provider_kind),
                        provider_model: Some(call.provider_model),
                        provider_response_id: call.provider_response_id,
                        budget_reservation_id: Some(call.budget_reservation_id),
                        provider_turn_index: call.provider_turn_index,
                        tool_call_index: call.tool_call_index,
                        tool_id: call.tool_id,
                        tool_fingerprint: call.tool_fingerprint,
                        protected_arguments: Some(call.protected_arguments),
                        argument_hash: call.argument_hash,
                        protected_result: None,
                        payload_purged_at: None,
                        risk: call.risk,
                        authorization_code: None,
                        authorization_policy_version: None,
                        authorization_state_digest: None,
                        disclosure_schema_fingerprint: None,
                        result_classification: None,
                        result_egress_decision_id: None,
                        result_egress_manifest_hash: None,
                        application_audit_ref: None,
                        approval_id: None,
                        idempotency_key: call.idempotency_key,
                        correlation_id: Some(call.correlation_id),
                        causation_id: Some(call.causation_id),
                        delegation_reference: call.delegation_reference,
                        lease_generation: lease.lease_generation,
                        state: "executing".to_owned(),
                        completed_at: None,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    Ok(())
                })
            })
            .await
            .map_err(map_transaction)
    }

    pub(crate) async fn append_proposal(
        &self,
        lease: &AiRunLease,
        proposal: PreparedProposal,
    ) -> Result<AiRunLease, AiError> {
        let now = canonical_second(self.clock.now());
        let lease_ttl = self.limits.lease_ttl;
        let lease = lease.clone();
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = load_and_validate_active_lease(tx, &lease, now).await?;
                    if persisted_state(&current)? != AiRunState::Running {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&lease.session_id.0)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if session.state != "active"
                        || session.deleted_at.is_some()
                        || session.owner_principal_kind != proposal.expected_owner_principal_kind
                        || session.owner_subject != proposal.expected_owner_subject
                        || session.scope_kind != proposal.expected_scope_kind
                        || session.scope_id != proposal.expected_scope_id
                        || session.tenant_id != proposal.expected_tenant_id
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let event_sequence = session
                        .stream_head
                        .checked_add(1)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let session_update = tx
                        .compare_and_swap::<AiSessionRecord>(
                            &session.id,
                            session.row_version,
                            AiSessionRecordWhereInput::default(),
                            UpdateAiSessionRecordInput {
                                stream_head: Some(event_sequence),
                                last_activity_at: Some(now.unix_timestamp()),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(session_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let expiry = now
                        .checked_add(lease_ttl)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let run_update = tx
                        .compare_and_swap::<AiRunRecord>(
                            &current.id,
                            current.row_version,
                            exact_state(AiRunState::Running.as_str()),
                            UpdateAiRunRecordInput {
                                lease_expires_at: Some(Some(expiry.unix_timestamp())),
                                lease_heartbeat_at: Some(Some(now.unix_timestamp())),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    let updated_run = match run_update {
                        ConditionalUpdateOutcome::Updated(updated) => updated,
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    tx.insert::<AiProposalRecord>(CreateAiProposalRecordInput {
                        id: proposal.id,
                        session_id: lease.session_id.0,
                        run_id: lease.run_id.0,
                        scope_kind: proposal.expected_scope_kind,
                        scope_id: proposal.expected_scope_id,
                        proposal_type: proposal.proposal_type,
                        schema_version: proposal.schema_version,
                        item_count: proposal.item_count,
                        protected_payload: Some(proposal.protected_payload),
                        source_references: Some(proposal.source_references),
                        payload_purged_at: None,
                        state: "pending_review".to_owned(),
                        created_by_subject: proposal.created_by_subject,
                        reviewed_by_subject: None,
                        applied_resource_ref: None,
                        application_audit_ref: None,
                        reviewed_at: None,
                        expires_at: proposal.expires_at,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.insert::<AiSessionEventRecord>(CreateAiSessionEventRecordInput {
                        id: proposal.event_id,
                        session_id: lease.session_id.0,
                        sequence: event_sequence,
                        event_type: "proposal_created".to_owned(),
                        run_id: Some(lease.run_id.0),
                        causation_id: Some(lease.input_message_id.to_string()),
                        correlation_id: proposal.correlation_id,
                        protected_payload: proposal.protected_event,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.queue_event(AiSessionWakeup {
                        session_id: lease.session_id.0,
                        sequence: event_sequence,
                    });
                    lease_from_record(&updated_run)
                })
            })
            .await
            .map_err(map_transaction)
    }

    pub(crate) async fn request_approval(
        &self,
        lease: &AiRunLease,
        approval: PreparedApprovalRequest,
    ) -> Result<AiRunLease, AiError> {
        let now = canonical_second(self.clock.now());
        let lease_ttl = self.limits.lease_ttl;
        let lease = lease.clone();
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = load_and_validate_active_lease(tx, &lease, now).await?;
                    if persisted_state(&current)? != AiRunState::Running
                        || approval.expires_at <= now.unix_timestamp()
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&lease.session_id.0)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if session.state != "active"
                        || session.deleted_at.is_some()
                        || session.owner_principal_kind != approval.expected_owner_principal_kind
                        || session.owner_subject != approval.expected_owner_subject
                        || session.scope_kind != approval.expected_scope_kind
                        || session.scope_id != approval.expected_scope_id
                        || session.tenant_id != approval.expected_tenant_id
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let call = tx
                        .find_by_id::<AiToolCallRecord>(&approval.tool_call_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if call.run_id != lease.run_id.0
                        || call.lease_generation != lease.lease_generation
                        || call.state != "executing"
                        || call.approval_id.is_some()
                        || call.argument_hash != approval.argument_hash
                        || call.tool_fingerprint != approval.tool_fingerprint
                        || call.risk == "read_only"
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let event_sequence = session
                        .stream_head
                        .checked_add(1)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let session_update = tx
                        .compare_and_swap::<AiSessionRecord>(
                            &session.id,
                            session.row_version,
                            AiSessionRecordWhereInput::default(),
                            UpdateAiSessionRecordInput {
                                stream_head: Some(event_sequence),
                                last_activity_at: Some(now.unix_timestamp()),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(session_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let call_update = tx
                        .compare_and_swap::<AiToolCallRecord>(
                            &call.id,
                            call.row_version,
                            AiToolCallRecordWhereInput::default(),
                            UpdateAiToolCallRecordInput {
                                approval_id: Some(Some(approval.id)),
                                state: Some("waiting_approval".to_owned()),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(call_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let expiry = now
                        .checked_add(lease_ttl)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let run_update = tx
                        .compare_and_swap::<AiRunRecord>(
                            &current.id,
                            current.row_version,
                            exact_state(AiRunState::Running.as_str()),
                            UpdateAiRunRecordInput {
                                state: Some(AiRunState::WaitingApproval.as_str().to_owned()),
                                lease_expires_at: Some(Some(expiry.unix_timestamp())),
                                lease_heartbeat_at: Some(Some(now.unix_timestamp())),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    let updated_run = match run_update {
                        ConditionalUpdateOutcome::Updated(updated) => updated,
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    tx.insert::<AiApprovalRecord>(CreateAiApprovalRecordInput {
                        id: approval.id,
                        tool_call_id: approval.tool_call_id,
                        session_id: lease.session_id.0,
                        principal_subject: approval.principal_subject,
                        principal_reference_fingerprint: approval.principal_reference_fingerprint,
                        delegated_actor_subject: approval.delegated_actor_subject,
                        delegation_reference: approval.delegation_reference,
                        argument_hash: approval.argument_hash,
                        tool_fingerprint: approval.tool_fingerprint,
                        binding_hash: approval.binding_hash,
                        execution_target_id: approval.execution_target_id,
                        target_schema_fingerprint: approval.target_schema_fingerprint,
                        operation_name: approval.operation_name,
                        operation_document_hash: approval.operation_document_hash,
                        result_projection_fingerprint: approval.result_projection_fingerprint,
                        disclosure_schema_fingerprint: approval.disclosure_schema_fingerprint,
                        policy_version: approval.policy_version,
                        authorization_state_digest: approval.authorization_state_digest,
                        protected_resource_bindings: Some(approval.protected_resource_bindings),
                        protected_action_preview: Some(approval.protected_action_preview),
                        payload_purged_at: None,
                        action_preview_hash: approval.action_preview_hash,
                        state: "pending".to_owned(),
                        recent_mfa_required: approval.recent_mfa_required,
                        approver_subject: None,
                        expires_at: approval.expires_at,
                        decided_at: None,
                        maximum_uses: 1,
                        consumed_uses: 0,
                        consumed_at: None,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.insert::<AiSessionEventRecord>(CreateAiSessionEventRecordInput {
                        id: approval.event_id,
                        session_id: session.id,
                        sequence: event_sequence,
                        event_type: "approval_requested".to_owned(),
                        run_id: Some(lease.run_id.0),
                        causation_id: Some(approval.tool_call_id.to_string()),
                        correlation_id: approval.correlation_id,
                        protected_payload: approval.protected_event,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.queue_event(AiSessionWakeup {
                        session_id: session.id,
                        sequence: event_sequence,
                    });
                    lease_from_record(&updated_run)
                })
            })
            .await
            .map_err(map_transaction)
    }

    pub(crate) async fn consume_approval(
        &self,
        lease: &AiRunLease,
        consumption: PreparedApprovalConsumption,
    ) -> Result<AiRunLease, AiError> {
        let now = canonical_second(self.clock.now());
        let lease_ttl = self.limits.lease_ttl;
        let lease = lease.clone();
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = load_and_validate_active_lease(tx, &lease, now).await?;
                    if !matches!(
                        persisted_state(&current)?,
                        AiRunState::WaitingApproval | AiRunState::WaitingTool
                    ) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&lease.session_id.0)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if session.state != "active"
                        || session.deleted_at.is_some()
                        || session.owner_principal_kind != consumption.expected_owner_principal_kind
                        || session.owner_subject != consumption.expected_owner_subject
                        || session.scope_kind != consumption.expected_scope_kind
                        || session.scope_id != consumption.expected_scope_id
                        || session.tenant_id != consumption.expected_tenant_id
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let call = tx
                        .find_by_id::<AiToolCallRecord>(&consumption.tool_call_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if call.run_id != lease.run_id.0
                        || call.lease_generation != lease.lease_generation
                        || call.approval_id != Some(consumption.approval_id)
                        || call.state != "waiting_approval"
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let approval = tx
                        .find_by_id::<AiApprovalRecord>(&consumption.approval_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if approval.tool_call_id != call.id
                        || approval.session_id != session.id
                        || approval.row_version != consumption.expected_approval_version
                        || !matches!(approval.state.as_str(), "approved" | "resume_claimed")
                        || approval.binding_hash != consumption.binding_hash
                        || approval.maximum_uses != 1
                        || approval.consumed_uses != 0
                        || approval.consumed_at.is_some()
                        || approval.decided_at.is_none()
                        || approval.expires_at <= now.unix_timestamp()
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let event_sequence = session
                        .stream_head
                        .checked_add(1)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let session_update = tx
                        .compare_and_swap::<AiSessionRecord>(
                            &session.id,
                            session.row_version,
                            AiSessionRecordWhereInput::default(),
                            UpdateAiSessionRecordInput {
                                stream_head: Some(event_sequence),
                                last_activity_at: Some(now.unix_timestamp()),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(session_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let approval_update = tx
                        .compare_and_swap::<AiApprovalRecord>(
                            &approval.id,
                            approval.row_version,
                            AiApprovalRecordWhereInput::default(),
                            UpdateAiApprovalRecordInput {
                                state: Some("consumed".to_owned()),
                                consumed_uses: Some(1),
                                consumed_at: Some(Some(now.unix_timestamp())),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(approval_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let call_update = tx
                        .compare_and_swap::<AiToolCallRecord>(
                            &call.id,
                            call.row_version,
                            AiToolCallRecordWhereInput::default(),
                            UpdateAiToolCallRecordInput {
                                state: Some("executing".to_owned()),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(call_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let expiry = now
                        .checked_add(lease_ttl)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let run_update = tx
                        .compare_and_swap::<AiRunRecord>(
                            &current.id,
                            current.row_version,
                            exact_state(&current.state),
                            UpdateAiRunRecordInput {
                                state: Some(AiRunState::Running.as_str().to_owned()),
                                lease_expires_at: Some(Some(expiry.unix_timestamp())),
                                lease_heartbeat_at: Some(Some(now.unix_timestamp())),
                                error_code: Some(None),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    let updated_run = match run_update {
                        ConditionalUpdateOutcome::Updated(updated) => updated,
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    tx.insert::<AiSessionEventRecord>(CreateAiSessionEventRecordInput {
                        id: consumption.event_id,
                        session_id: session.id,
                        sequence: event_sequence,
                        event_type: "approval_consumed".to_owned(),
                        run_id: Some(lease.run_id.0),
                        causation_id: Some(consumption.approval_id.to_string()),
                        correlation_id: consumption.correlation_id,
                        protected_payload: consumption.protected_event,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.queue_event(AiSessionWakeup {
                        session_id: session.id,
                        sequence: event_sequence,
                    });
                    lease_from_record(&updated_run)
                })
            })
            .await
            .map_err(map_transaction)
    }

    pub(crate) async fn finish_tool_call(
        &self,
        lease: &AiRunLease,
        finish: PreparedToolCallFinish,
    ) -> Result<AiRunLease, AiError> {
        let now = canonical_second(self.clock.now());
        let lease_ttl = self.limits.lease_ttl;
        let lease = lease.clone();
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = load_and_validate_active_lease(tx, &lease, now).await?;
                    if persisted_state(&current)? != AiRunState::Running {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&lease.session_id.0)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if !session_matches_tool_finish(&session, &finish) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let call = tx
                        .find_by_id::<AiToolCallRecord>(&finish.id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if call.run_id != lease.run_id.0
                        || call.lease_generation != lease.lease_generation
                        || call.provider_call_key != finish.expected_provider_call_key
                        || call.tool_fingerprint != finish.expected_tool_fingerprint
                        || call.state != "executing"
                        || call.protected_result.is_some()
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let step = tx
                        .find_by_id::<AiRunStepRecord>(&finish.id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if step.run_id != lease.run_id.0
                        || step.lease_generation != lease.lease_generation
                        || step.state != "running"
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let call_update = tx
                        .compare_and_swap::<AiToolCallRecord>(
                            &call.id,
                            call.row_version,
                            AiToolCallRecordWhereInput::default(),
                            UpdateAiToolCallRecordInput {
                                protected_result: Some(Some(finish.protected_result)),
                                authorization_code: Some(Some(finish.authorization_code.clone())),
                                authorization_policy_version: Some(
                                    finish.authorization_policy_version,
                                ),
                                authorization_state_digest: Some(finish.authorization_state_digest),
                                disclosure_schema_fingerprint: Some(Some(
                                    finish.disclosure_schema_fingerprint,
                                )),
                                result_classification: Some(Some(finish.result_classification)),
                                result_egress_decision_id: Some(finish.result_egress_decision_id),
                                result_egress_manifest_hash: Some(
                                    finish.result_egress_manifest_hash,
                                ),
                                application_audit_ref: Some(finish.application_audit_ref),
                                state: Some(finish.state.clone()),
                                completed_at: Some(Some(now.unix_timestamp())),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(call_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let step_update = tx
                        .compare_and_swap::<AiRunStepRecord>(
                            &step.id,
                            step.row_version,
                            AiRunStepRecordWhereInput::default(),
                            UpdateAiRunStepRecordInput {
                                state: Some(finish.state.clone()),
                                finished_at: Some(Some(now.unix_timestamp())),
                                error_code: (finish.state != "completed")
                                    .then_some(Some(finish.authorization_code)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(step_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let event_sequence = session
                        .stream_head
                        .checked_add(1)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let session_update = tx
                        .compare_and_swap::<AiSessionRecord>(
                            &session.id,
                            session.row_version,
                            AiSessionRecordWhereInput::default(),
                            UpdateAiSessionRecordInput {
                                stream_head: Some(event_sequence),
                                last_activity_at: Some(now.unix_timestamp()),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(session_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    tx.insert::<AiSessionEventRecord>(CreateAiSessionEventRecordInput {
                        id: finish.event_id,
                        session_id: lease.session_id.0,
                        sequence: event_sequence,
                        event_type: "application_tool_completed".to_owned(),
                        run_id: Some(lease.run_id.0),
                        causation_id: Some(finish.id.to_string()),
                        correlation_id: finish.correlation_id,
                        protected_payload: finish.protected_event,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.queue_event(AiSessionWakeup {
                        session_id: lease.session_id.0,
                        sequence: event_sequence,
                    });
                    let expiry = now
                        .checked_add(lease_ttl)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let run_update = tx
                        .compare_and_swap::<AiRunRecord>(
                            &current.id,
                            current.row_version,
                            exact_state(AiRunState::Running.as_str()),
                            UpdateAiRunRecordInput {
                                lease_expires_at: Some(Some(expiry.unix_timestamp())),
                                lease_heartbeat_at: Some(Some(now.unix_timestamp())),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    match run_update {
                        ConditionalUpdateOutcome::Updated(updated) => lease_from_record(&updated),
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            Err(OrmPublicError::new(OrmErrorCode::Conflict))
                        }
                    }
                })
            })
            .await
            .map_err(map_transaction)
    }

    async fn claim_once(
        &self,
        worker_id: String,
        now: OffsetDateTime,
    ) -> Result<Result<Option<AiRunLease>, AiError>, TransactionError> {
        let database = self.database.clone();
        let limits = self.limits;
        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let mut candidates = Vec::new();
                    for state in [AiRunState::Queued, AiRunState::RetryScheduled] {
                        let mut rows = tx
                            .query::<AiRunRecord>()
                            .filter(exact_state(state.as_str()))
                            .limit(limits.maximum_candidate_scan as i64)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        candidates.append(&mut rows);
                    }
                    candidates.retain(|run| {
                        run.next_attempt_at
                            .is_none_or(|eligible_at| eligible_at <= now.unix_timestamp())
                    });
                    candidates.sort_by_key(|run| (run.created_at, run.id));
                    let Some(current) = candidates.into_iter().next() else {
                        return Ok(Ok(None));
                    };
                    if current.lease_generation < 0
                        || current.retry_count < 0
                        || current.attempt_id.is_some()
                        || current.lease_owner.is_some()
                        || current.lease_expires_at.is_some()
                        || (current.error_code.as_deref() == Some("checkpoint_adoption_ready")
                            && current.latest_checkpoint_id.is_none())
                    {
                        return Ok(Err(AiError::PersistenceFailed));
                    }
                    let generation = match current.lease_generation.checked_add(1) {
                        Some(generation) => generation,
                        None => return Ok(Err(AiError::PersistenceFailed)),
                    };
                    let expiry = match now.checked_add(limits.lease_ttl) {
                        Some(expiry) => expiry,
                        None => return Ok(Err(AiError::PersistenceFailed)),
                    };
                    let attempt = tx
                        .insert::<AiRunAttemptRecord>(CreateAiRunAttemptRecordInput {
                            run_id: current.id,
                            lease_generation: generation,
                            worker_id: worker_id.clone(),
                            claimed_at: now.unix_timestamp(),
                            finished_at: None,
                            provider_response_id: None,
                            outcome_code: None,
                        })
                        .await
                        .map_err(OrmPublicError::from)?;
                    let outcome = tx
                        .compare_and_swap::<AiRunRecord>(
                            &current.id,
                            current.row_version,
                            exact_state(&current.state),
                            UpdateAiRunRecordInput {
                                state: Some(AiRunState::Leased.as_str().to_owned()),
                                attempt_id: Some(Some(attempt.id)),
                                lease_owner: Some(Some(worker_id)),
                                lease_generation: Some(generation),
                                lease_expires_at: Some(Some(expiry.unix_timestamp())),
                                lease_heartbeat_at: Some(Some(now.unix_timestamp())),
                                next_attempt_at: Some(None),
                                error_code: Some(None),
                                latest_checkpoint_id: Some(
                                    (current.error_code.as_deref()
                                        == Some("checkpoint_adoption_ready"))
                                    .then_some(current.latest_checkpoint_id)
                                    .flatten(),
                                ),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    match outcome {
                        ConditionalUpdateOutcome::Updated(updated) => {
                            match lease_from_record(&updated) {
                                Ok(lease) => Ok(Ok(Some(lease))),
                                Err(_) => Ok(Err(AiError::PersistenceFailed)),
                            }
                        }
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            Err(OrmPublicError::new(OrmErrorCode::Conflict))
                        }
                    }
                })
            })
            .await
    }

    async fn claim_approved_once(
        &self,
        worker_id: String,
        now: OffsetDateTime,
    ) -> Result<Option<AiApprovedRunClaim>, TransactionError> {
        let database = self.database.clone();
        let limits = self.limits;
        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let mut approvals = tx
                        .query::<AiApprovalRecord>()
                        .filter(AiApprovalRecordWhereInput {
                            state: Some(StringFilter {
                                eq: Some("approved".to_owned()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(limits.maximum_candidate_scan as i64)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    approvals.sort_by_key(|approval| (approval.created_at, approval.id));

                    for approval in approvals {
                        if approval.expires_at <= now.unix_timestamp() {
                            let expired = tx
                                .compare_and_swap::<AiApprovalRecord>(
                                    &approval.id,
                                    approval.row_version,
                                    AiApprovalRecordWhereInput {
                                        state: Some(StringFilter {
                                            eq: Some("approved".to_owned()),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    },
                                    UpdateAiApprovalRecordInput {
                                        state: Some("expired".to_owned()),
                                        ..Default::default()
                                    },
                                )
                                .await
                                .map_err(OrmPublicError::from)?;
                            if !matches!(expired, ConditionalUpdateOutcome::Updated(_)) {
                                return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                            }
                            tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                                actor_principal_kind: "ai_worker".to_owned(),
                                actor_subject: worker_id.clone(),
                                action: "ai.approval.expired".to_owned(),
                                resource_kind: "ai_approval".to_owned(),
                                resource_reference: approval.id.to_string(),
                                outcome: "denied".to_owned(),
                                reason_code: "approval_expired_before_handoff".to_owned(),
                                correlation_id: approval.id.to_string(),
                                causation_id: Some(approval.tool_call_id.to_string()),
                                policy_version: Some(approval.policy_version),
                            })
                            .await
                            .map_err(OrmPublicError::from)?;
                            continue;
                        }
                        if approval.maximum_uses != 1
                            || approval.consumed_uses != 0
                            || approval.consumed_at.is_some()
                            || approval.decided_at.is_none()
                            || approval.approver_subject.is_none()
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                        let call = tx
                            .find_by_id::<AiToolCallRecord>(&approval.tool_call_id)
                            .await
                            .map_err(OrmPublicError::from)?
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                        let run = tx
                            .find_by_id::<AiRunRecord>(&call.run_id)
                            .await
                            .map_err(OrmPublicError::from)?
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                        if approval.session_id != run.session_id
                            || call.approval_id != Some(approval.id)
                            || call.state != "waiting_approval"
                            || call.completed_at.is_some()
                            || call.protected_result.is_some()
                            || call.risk == "read_only"
                            || persisted_state(&run)? != AiRunState::WaitingApproval
                            || run.attempt_id.is_none()
                            || run.lease_generation <= 0
                            || call.lease_generation != run.lease_generation
                            || run
                                .lease_owner
                                .as_deref()
                                .is_none_or(|owner| validate_worker_id(owner).is_err())
                            || run.lease_expires_at.is_none()
                            || run.error_code.is_some()
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                        let expiry = now
                            .checked_add(limits.lease_ttl)
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                        let approval_update = tx
                            .compare_and_swap::<AiApprovalRecord>(
                                &approval.id,
                                approval.row_version,
                                AiApprovalRecordWhereInput::default(),
                                UpdateAiApprovalRecordInput {
                                    state: Some("resume_claimed".to_owned()),
                                    ..Default::default()
                                },
                            )
                            .await
                            .map_err(OrmPublicError::from)?;
                        if !matches!(approval_update, ConditionalUpdateOutcome::Updated(_)) {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                        let outcome = tx
                            .compare_and_swap::<AiRunRecord>(
                                &run.id,
                                run.row_version,
                                exact_state(AiRunState::WaitingApproval.as_str()),
                                UpdateAiRunRecordInput {
                                    state: Some(AiRunState::WaitingTool.as_str().to_owned()),
                                    lease_owner: Some(Some(worker_id.clone())),
                                    lease_expires_at: Some(Some(expiry.unix_timestamp())),
                                    lease_heartbeat_at: Some(Some(now.unix_timestamp())),
                                    error_code: Some(Some("approval_resume_claimed".to_owned())),
                                    ..Default::default()
                                },
                            )
                            .await
                            .map_err(OrmPublicError::from)?;
                        let updated = match outcome {
                            ConditionalUpdateOutcome::Updated(updated) => updated,
                            ConditionalUpdateOutcome::NotFound
                            | ConditionalUpdateOutcome::Conflict => {
                                return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                            }
                        };
                        tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                            actor_principal_kind: "ai_worker".to_owned(),
                            actor_subject: worker_id.clone(),
                            action: "ai.run.approval_resume_claimed".to_owned(),
                            resource_kind: "ai_run".to_owned(),
                            resource_reference: run.id.to_string(),
                            outcome: "allowed".to_owned(),
                            reason_code: "approved_wait_handoff".to_owned(),
                            correlation_id: approval.id.to_string(),
                            causation_id: Some(call.id.to_string()),
                            policy_version: Some(approval.policy_version),
                        })
                        .await
                        .map_err(OrmPublicError::from)?;
                        return Ok(Some(AiApprovedRunClaim {
                            approval_id: AiApprovalId(approval.id),
                            tool_call_id: AiToolCallId(call.id),
                            lease: lease_from_record(&updated)?,
                        }));
                    }
                    Ok(None)
                })
            })
            .await
    }

    async fn reconcile_approval_wait_once(
        &self,
        reconciliation: PreparedApprovalWaitReconciliation,
        now: OffsetDateTime,
    ) -> Result<(), TransactionError> {
        let database = self.database.clone();
        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = tx
                        .find_by_id::<AiRunRecord>(&reconciliation.expected_run.id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if current != reconciliation.expected_run
                        || persisted_state(&current)? != AiRunState::WaitingApproval
                        || current.attempt_id.is_none()
                        || current.lease_generation <= 0
                        || current
                            .lease_owner
                            .as_deref()
                            .is_none_or(|owner| validate_worker_id(owner).is_err())
                        || current.lease_expires_at.is_none()
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let lease = lease_from_record(&current)?;
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&current.session_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if session.owner_principal_kind != reconciliation.expected_owner_principal_kind
                        || session.owner_subject != reconciliation.expected_owner_subject
                        || session.scope_kind != reconciliation.expected_scope_kind
                        || session.scope_id != reconciliation.expected_scope_id
                        || session.tenant_id != reconciliation.expected_tenant_id
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }

                    let (
                        final_state,
                        approval_id,
                        tool_call_id,
                        provider_response_id,
                        audit_outcome,
                    ) = match reconciliation.outcome {
                        PreparedApprovalWaitOutcome::Cancelled(cancellation) => {
                            let PreparedApprovalWaitCancellation {
                                call,
                                step,
                                approval,
                                checkpoint,
                                next_approval_state,
                                call_state,
                            } = *cancellation;
                            let current_call = tx
                                .find_by_id::<AiToolCallRecord>(&call.id)
                                .await
                                .map_err(OrmPublicError::from)?
                                .ok_or_else(OrmPublicError::not_found)?;
                            let current_step = tx
                                .find_by_id::<AiRunStepRecord>(&step.id)
                                .await
                                .map_err(OrmPublicError::from)?
                                .ok_or_else(OrmPublicError::not_found)?;
                            let current_approval = tx
                                .find_by_id::<AiApprovalRecord>(&approval.id)
                                .await
                                .map_err(OrmPublicError::from)?
                                .ok_or_else(OrmPublicError::not_found)?;
                            let current_checkpoint = tx
                                .query::<AiRunCheckpointRecord>()
                                .filter(AiRunCheckpointRecordWhereInput {
                                    id: Some(UuidFilter {
                                        eq: Some(checkpoint.id),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                })
                                .limit(1)
                                .fetch_one()
                                .await
                                .map_err(OrmPublicError::from)?
                                .ok_or_else(OrmPublicError::not_found)?;
                            if current_call != call
                                || current_step != step
                                || current_approval != approval
                                || current_checkpoint != checkpoint
                            {
                                return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                            }
                            let provider_response_id = checkpoint
                                .provider_response_id
                                .as_deref()
                                .filter(|value| valid_provider_reference(value))
                                .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                            let budget_reservation_id = checkpoint
                                .budget_reservation_id
                                .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                            let protected_state = checkpoint
                                .protected_state
                                .as_ref()
                                .filter(|state| {
                                    serde_json::to_vec(state)
                                        .is_ok_and(|encoded| encoded.len() <= 64 * 1024 * 1024)
                                })
                                .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                            let reservation = tx
                                .find_by_id::<AiBudgetReservationRecord>(&budget_reservation_id)
                                .await
                                .map_err(OrmPublicError::from)?
                                .ok_or_else(OrmPublicError::not_found)?;
                            let principal_reference: PrincipalReference =
                                serde_json::from_value(current.principal_reference.clone())
                                    .map_err(|_| {
                                        OrmPublicError::new(OrmErrorCode::InternalError)
                                    })?;
                            let principal_fingerprint = hex::encode(Sha256::digest(
                                serde_json::to_vec(&principal_reference).map_err(|_| {
                                    OrmPublicError::new(OrmErrorCode::InternalError)
                                })?,
                            ));
                            let expected_hash = coordinator_checkpoint_hash(
                                lease.run_id,
                                lease.attempt_id,
                                lease.lease_generation,
                                checkpoint.id,
                                &checkpoint.checkpoint_kind,
                                &reservation.provider_kind,
                                &reservation.provider_model,
                                Some(provider_response_id),
                                budget_reservation_id,
                                protected_state,
                            )
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
                            let calls = tx
                                .query::<AiToolCallRecord>()
                                .filter(AiToolCallRecordWhereInput {
                                    run_id: Some(UuidFilter {
                                        eq: Some(current.id),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                })
                                .limit(4_097)
                                .fetch_all()
                                .await
                                .map_err(OrmPublicError::from)?;
                            let relevant = calls
                                .iter()
                                .filter(|candidate| {
                                    candidate.lease_generation == current.lease_generation
                                        && candidate.provider_response_id.as_deref()
                                            == Some(provider_response_id)
                                        && candidate.budget_reservation_id
                                            == Some(budget_reservation_id)
                                })
                                .collect::<Vec<_>>();
                            let approval_state_is_terminal =
                                matches!(approval.state.as_str(), "denied" | "revoked" | "expired")
                                    && next_approval_state.is_none();
                            let approval_state_expires =
                                matches!(approval.state.as_str(), "pending" | "approved")
                                    && next_approval_state.as_deref() == Some("expired");
                            if current.latest_checkpoint_id != Some(checkpoint.id)
                                || checkpoint.run_id != current.id
                                || checkpoint.attempt_id != lease.attempt_id
                                || checkpoint.lease_generation != lease.lease_generation
                                || checkpoint.checkpoint_kind != "provider_turn_persisted"
                                || checkpoint.assistant_message_id.is_some()
                                || checkpoint.checkpoint_hash != expected_hash
                                || reservation.session_id != session.id
                                || reservation.scope_kind != reconciliation.expected_scope_kind
                                || reservation.scope_id != reconciliation.expected_scope_id
                                || reservation.tenant_id != reconciliation.expected_tenant_id
                                || reservation.principal_kind
                                    != reconciliation.expected_owner_principal_kind
                                || reservation.principal_subject
                                    != reconciliation.expected_owner_subject
                                || reservation.run_id != current.id
                                || reservation.attempt_id != lease.attempt_id
                                || reservation.lease_generation != lease.lease_generation
                                || reservation.state != "committed"
                                || reservation.actual_runs != Some(1)
                                || reservation.reconciled_at.is_none()
                                || calls.len() >= 4_097
                                || relevant.len() != 1
                                || relevant[0].id != call.id
                                || call.run_id != current.id
                                || call.lease_generation != current.lease_generation
                                || call.provider_kind.as_deref()
                                    != Some(reservation.provider_kind.as_str())
                                || call.provider_model.as_deref()
                                    != Some(reservation.provider_model.as_str())
                                || call.provider_response_id.as_deref()
                                    != Some(provider_response_id)
                                || call.budget_reservation_id != Some(budget_reservation_id)
                                || call.tool_call_index != 0
                                || call.state != "waiting_approval"
                                || call.completed_at.is_some()
                                || call.protected_result.is_some()
                                || call.approval_id != Some(approval.id)
                                || call.risk == "read_only"
                                || step.id != call.id
                                || step.run_id != current.id
                                || step.lease_generation != current.lease_generation
                                || step.step_kind != "application_tool"
                                || step.state != "running"
                                || step.finished_at.is_some()
                                || approval.tool_call_id != call.id
                                || approval.session_id != session.id
                                || approval.principal_subject != principal_reference.subject
                                || approval.principal_reference_fingerprint != principal_fingerprint
                                || approval.argument_hash != call.argument_hash
                                || approval.tool_fingerprint != call.tool_fingerprint
                                || approval.maximum_uses != 1
                                || approval.consumed_uses != 0
                                || approval.consumed_at.is_some()
                                || !(approval_state_is_terminal || approval_state_expires)
                                || !matches!(
                                    call_state.as_str(),
                                    "approval_denied" | "approval_revoked" | "approval_expired"
                                )
                            {
                                return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                            }
                            if let Some(next_state) = next_approval_state {
                                let approval_update = tx
                                    .compare_and_swap::<AiApprovalRecord>(
                                        &approval.id,
                                        approval.row_version,
                                        AiApprovalRecordWhereInput::default(),
                                        UpdateAiApprovalRecordInput {
                                            state: Some(next_state),
                                            ..Default::default()
                                        },
                                    )
                                    .await
                                    .map_err(OrmPublicError::from)?;
                                if !matches!(approval_update, ConditionalUpdateOutcome::Updated(_))
                                {
                                    return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                                }
                            }
                            let call_update = tx
                                .compare_and_swap::<AiToolCallRecord>(
                                    &call.id,
                                    call.row_version,
                                    AiToolCallRecordWhereInput::default(),
                                    UpdateAiToolCallRecordInput {
                                        state: Some(call_state.clone()),
                                        completed_at: Some(Some(now.unix_timestamp())),
                                        ..Default::default()
                                    },
                                )
                                .await
                                .map_err(OrmPublicError::from)?;
                            if !matches!(call_update, ConditionalUpdateOutcome::Updated(_)) {
                                return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                            }
                            let step_update = tx
                                .compare_and_swap::<AiRunStepRecord>(
                                    &step.id,
                                    step.row_version,
                                    AiRunStepRecordWhereInput::default(),
                                    UpdateAiRunStepRecordInput {
                                        state: Some(call_state),
                                        finished_at: Some(Some(now.unix_timestamp())),
                                        error_code: Some(Some(reconciliation.outcome_code.clone())),
                                        ..Default::default()
                                    },
                                )
                                .await
                                .map_err(OrmPublicError::from)?;
                            if !matches!(step_update, ConditionalUpdateOutcome::Updated(_)) {
                                return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                            }
                            (
                                AiRunState::Cancelled,
                                Some(approval.id),
                                Some(call.id),
                                Some(provider_response_id.to_owned()),
                                "cancelled",
                            )
                        }
                        PreparedApprovalWaitOutcome::RecoveryRequired {
                            approval_id,
                            tool_call_id,
                        } => (
                            AiRunState::RecoveryRequired,
                            approval_id,
                            tool_call_id,
                            None,
                            "recovery_required",
                        ),
                    };

                    let event_sequence = session
                        .stream_head
                        .checked_add(1)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let session_update = tx
                        .compare_and_swap::<AiSessionRecord>(
                            &session.id,
                            session.row_version,
                            AiSessionRecordWhereInput::default(),
                            UpdateAiSessionRecordInput {
                                stream_head: Some(event_sequence),
                                last_activity_at: Some(now.unix_timestamp()),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(session_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let run_update = tx
                        .compare_and_swap::<AiRunRecord>(
                            &current.id,
                            current.row_version,
                            exact_state(AiRunState::WaitingApproval.as_str()),
                            UpdateAiRunRecordInput {
                                state: Some(final_state.as_str().to_owned()),
                                lease_owner: Some(None),
                                lease_expires_at: Some(None),
                                lease_heartbeat_at: Some(None),
                                next_attempt_at: Some(None),
                                error_code: Some(Some(reconciliation.outcome_code.clone())),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(run_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    tx.insert::<AiSessionEventRecord>(CreateAiSessionEventRecordInput {
                        id: reconciliation.event_id,
                        session_id: session.id,
                        sequence: event_sequence,
                        event_type: "approval_wait_reconciled".to_owned(),
                        run_id: Some(current.id),
                        causation_id: tool_call_id.or(approval_id).map(|id| id.to_string()),
                        correlation_id: approval_id.unwrap_or(current.id).to_string(),
                        protected_payload: reconciliation.protected_event,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                        actor_principal_kind: "ai_worker".to_owned(),
                        actor_subject: reconciliation.worker_id,
                        action: "ai.run.approval_wait_reconcile".to_owned(),
                        resource_kind: "ai_run".to_owned(),
                        resource_reference: current.id.to_string(),
                        outcome: audit_outcome.to_owned(),
                        reason_code: reconciliation.outcome_code.clone(),
                        correlation_id: approval_id.unwrap_or(current.id).to_string(),
                        causation_id: tool_call_id.map(|id| id.to_string()),
                        policy_version: reconciliation.policy_version,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    append_attempt_outcome(
                        tx,
                        &lease,
                        final_state,
                        reconciliation.outcome_code,
                        provider_response_id,
                        now,
                    )
                    .await?;
                    tx.queue_event(AiSessionWakeup {
                        session_id: session.id,
                        sequence: event_sequence,
                    });
                    Ok(())
                })
            })
            .await
    }

    async fn update_active_lease(
        &self,
        lease: &AiRunLease,
        update: LeaseUpdate,
        now: OffsetDateTime,
    ) -> Result<AiRunLease, AiError> {
        let lease = lease.clone();
        let lease_ttl = self.limits.lease_ttl;
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = load_and_validate_active_lease(tx, &lease, now).await?;
                    let current_state = persisted_state(&current)?;
                    let next_state = match update {
                        LeaseUpdate::Heartbeat
                            if matches!(
                                current_state,
                                AiRunState::Leased | AiRunState::Running
                            ) =>
                        {
                            current_state
                        }
                        LeaseUpdate::Start if current_state == AiRunState::Leased => {
                            AiRunState::Running
                        }
                        _ => return Err(OrmPublicError::new(OrmErrorCode::Conflict)),
                    };
                    let expiry = now
                        .checked_add(lease_ttl)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let outcome = tx
                        .compare_and_swap::<AiRunRecord>(
                            &current.id,
                            current.row_version,
                            exact_state(&current.state),
                            UpdateAiRunRecordInput {
                                state: Some(next_state.as_str().to_owned()),
                                lease_expires_at: Some(Some(expiry.unix_timestamp())),
                                lease_heartbeat_at: Some(Some(now.unix_timestamp())),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    match outcome {
                        ConditionalUpdateOutcome::Updated(updated) => lease_from_record(&updated),
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            Err(OrmPublicError::new(OrmErrorCode::Conflict))
                        }
                    }
                })
            })
            .await
            .map_err(map_transaction)
    }
}

#[derive(Clone, Copy)]
enum LeaseUpdate {
    Heartbeat,
    Start,
}

fn session_matches_tool_start(session: &AiSessionRecord, call: &PreparedToolCallStart) -> bool {
    session.state == "active"
        && session.deleted_at.is_none()
        && session.owner_principal_kind == call.expected_owner_principal_kind
        && session.owner_subject == call.expected_owner_subject
        && session.scope_kind == call.expected_scope_kind
        && session.scope_id == call.expected_scope_id
        && session.tenant_id == call.expected_tenant_id
}

fn session_matches_tool_finish(session: &AiSessionRecord, finish: &PreparedToolCallFinish) -> bool {
    session.state == "active"
        && session.deleted_at.is_none()
        && session.owner_principal_kind == finish.expected_owner_principal_kind
        && session.owner_subject == finish.expected_owner_subject
        && session.scope_kind == finish.expected_scope_kind
        && session.scope_id == finish.expected_scope_id
        && session.tenant_id == finish.expected_tenant_id
}

pub(crate) fn exact_state(state: &str) -> AiRunRecordWhereInput {
    AiRunRecordWhereInput {
        state: Some(StringFilter {
            eq: Some(state.to_owned()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub(crate) async fn load_and_validate_active_lease(
    tx: &mut MutationContext<'_, DefaultWriteBackend>,
    lease: &AiRunLease,
    now: OffsetDateTime,
) -> Result<AiRunRecord, OrmPublicError> {
    let current = tx
        .find_by_id::<AiRunRecord>(&lease.run_id.0)
        .await
        .map_err(OrmPublicError::from)?
        .ok_or_else(OrmPublicError::not_found)?;
    let state = persisted_state(&current)?;
    let stored_reference: PrincipalReference =
        serde_json::from_value(current.principal_reference.clone())
            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
    if current.session_id != lease.session_id.0
        || current.input_message_id != lease.input_message_id
        || stored_reference != lease.principal_reference
        || current.attempt_id != Some(lease.attempt_id)
        || current.lease_owner.as_deref() != Some(lease.worker_id.as_str())
        || current.lease_generation != lease.lease_generation
        || current.row_version != lease.row_version
        || state != lease.state
        || current.retry_count != i64::from(lease.retry_count)
        || current.latest_checkpoint_id != lease.latest_checkpoint_id
        || current
            .lease_expires_at
            .is_none_or(|expires_at| expires_at <= now.unix_timestamp())
    {
        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
    }
    Ok(current)
}

pub(crate) fn lease_from_record(record: &AiRunRecord) -> Result<AiRunLease, OrmPublicError> {
    let principal_reference = serde_json::from_value(record.principal_reference.clone())
        .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let attempt_id = record
        .attempt_id
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let worker_id = record
        .lease_owner
        .clone()
        .filter(|worker| validate_worker_id(worker).is_ok())
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let expires_at = record
        .lease_expires_at
        .and_then(|value| OffsetDateTime::from_unix_timestamp(value).ok())
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    if record.lease_generation <= 0 || record.row_version < 0 || record.retry_count < 0 {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    Ok(AiRunLease {
        run_id: AiRunId(record.id),
        session_id: AiSessionId(record.session_id),
        input_message_id: record.input_message_id,
        principal_reference,
        attempt_id,
        worker_id,
        lease_generation: record.lease_generation,
        lease_expires_at: expires_at,
        row_version: record.row_version,
        state: persisted_state(record)?,
        retry_count: u32::try_from(record.retry_count)
            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?,
        latest_checkpoint_id: record.latest_checkpoint_id,
    })
}

fn persisted_state(record: &AiRunRecord) -> Result<AiRunState, OrmPublicError> {
    AiRunState::from_persisted(&record.state)
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))
}

fn reservation_usage_matches(
    reservation: &AiBudgetReservationRecord,
    usage: AiBudgetAmounts,
    cached_input_tokens: u64,
) -> Result<bool, OrmPublicError> {
    let as_i64 = |value: u64| {
        i64::try_from(value).map_err(|_| OrmPublicError::new(OrmErrorCode::InvalidInput))
    };
    Ok(
        reservation.actual_input_tokens == Some(as_i64(usage.input_tokens)?)
            && reservation.actual_cached_input_tokens == Some(as_i64(cached_input_tokens)?)
            && reservation.actual_output_tokens == Some(as_i64(usage.output_tokens)?)
            && reservation.actual_tool_units == Some(as_i64(usage.tool_units)?)
            && reservation.actual_image_units == Some(as_i64(usage.image_units)?)
            && reservation.actual_cost_microunits == Some(as_i64(usage.cost_microunits)?)
            && reservation.actual_runs == Some(as_i64(usage.runs)?),
    )
}

pub(crate) async fn append_attempt_outcome(
    tx: &mut MutationContext<'_, DefaultWriteBackend>,
    lease: &AiRunLease,
    final_state: AiRunState,
    outcome_code: String,
    provider_response_id: Option<String>,
    now: OffsetDateTime,
) -> Result<(), OrmPublicError> {
    tx.insert::<AiRunAttemptOutcomeRecord>(CreateAiRunAttemptOutcomeRecordInput {
        attempt_id: lease.attempt_id,
        run_id: lease.run_id.0,
        lease_generation: lease.lease_generation,
        worker_id: lease.worker_id.clone(),
        final_state: final_state.as_str().to_owned(),
        outcome_code,
        provider_response_id,
        finished_at: now.unix_timestamp(),
    })
    .await
    .map_err(OrmPublicError::from)?;
    Ok(())
}

pub(crate) fn validate_worker_id(worker_id: &str) -> Result<(), AiError> {
    if worker_id.trim().is_empty()
        || worker_id.len() > MAXIMUM_WORKER_ID_BYTES
        || worker_id.chars().any(char::is_control)
    {
        return Err(AiError::InvalidInput("invalid AI worker ID".to_owned()));
    }
    Ok(())
}

fn valid_provider_reference(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAXIMUM_PROVIDER_REFERENCE_BYTES
        && value.bytes().all(|byte| !byte.is_ascii_control())
}

fn valid_provider_kind(value: &str) -> bool {
    matches!(
        value,
        "openai" | "anthropic" | "xai" | "ollama" | "openai_compatible" | "local_harness"
    )
}

pub(crate) fn final_output_checkpoint_hash(
    run_id: AiRunId,
    attempt_id: Uuid,
    lease_generation: i64,
    message_id: Uuid,
    provider_response_id: Option<&str>,
    budget_reservation_id: Uuid,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(run_id.0.as_bytes());
    hasher.update([0]);
    hasher.update(attempt_id.as_bytes());
    hasher.update([0]);
    hasher.update(lease_generation.to_be_bytes());
    hasher.update([0]);
    hasher.update(message_id.as_bytes());
    hasher.update([0]);
    if let Some(response_id) = provider_response_id {
        hasher.update(response_id.as_bytes());
    }
    hasher.update([0]);
    hasher.update(budget_reservation_id.as_bytes());
    hex::encode(hasher.finalize())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn coordinator_checkpoint_hash(
    run_id: AiRunId,
    attempt_id: Uuid,
    lease_generation: i64,
    checkpoint_id: Uuid,
    kind: &str,
    provider_kind: &str,
    provider_model: &str,
    provider_response_id: Option<&str>,
    budget_reservation_id: Uuid,
    protected_state: &serde_json::Value,
) -> Result<String, AiError> {
    let provider_kind_hash_value = match provider_kind {
        "openai" => "open_ai",
        "openai_compatible" => "open_ai_compatible",
        "anthropic" | "xai" | "ollama" | "local_harness" => provider_kind,
        _ => return Err(AiError::PersistenceFailed),
    };
    let protected_state_hash = hex::encode(Sha256::digest(
        serde_json::to_vec(protected_state).map_err(|_| AiError::PersistenceFailed)?,
    ));
    let redacted = serde_json::json!({
        "checkpointId": checkpoint_id,
        "runId": run_id.0,
        "attemptId": attempt_id,
        "leaseGeneration": lease_generation,
        "kind": kind,
        // Preserve the Serde representation used by the original 0.6.0
        // checkpoint writer, while persistence/configuration use `as_str()`.
        "providerKind": provider_kind_hash_value,
        "providerModel": provider_model,
        "providerResponseId": provider_response_id,
        "budgetReservationId": budget_reservation_id,
        "protectedStateHash": protected_state_hash,
    });
    let encoded = serde_json::to_vec(&redacted).map_err(|_| AiError::PersistenceFailed)?;
    Ok(hex::encode(Sha256::digest(encoded)))
}

fn valid_safe_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= MAXIMUM_SAFE_CODE_BYTES
        && code.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'_' | b'-' | b'.' | b':')
        })
}

pub(crate) fn canonical_second(value: OffsetDateTime) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(value.unix_timestamp())
        .expect("an existing OffsetDateTime timestamp remains representable")
}

fn map_transaction(error: TransactionError) -> AiError {
    let public = error.public_error();
    match public.code {
        OrmErrorCode::InvalidInput
        | OrmErrorCode::CursorInvalid
        | OrmErrorCode::PageLimitExceeded => AiError::InvalidInput(public.message.clone()),
        OrmErrorCode::Unauthenticated | OrmErrorCode::Forbidden => AiError::Forbidden,
        OrmErrorCode::NotFound => AiError::NotFound,
        OrmErrorCode::Conflict | OrmErrorCode::ConstraintViolation => AiError::Conflict,
        OrmErrorCode::ServiceUnavailable
        | OrmErrorCode::InternalError
        | OrmErrorCode::AuthorizationMisconfigured => AiError::PersistenceFailed,
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use agql_auth::{AccessTokenMetadata, AuthPrincipal, AuthUser, FixedClock, SessionContext};
    use graphql_orm::graphql::orm::{ApplyOptions, OrmSchemaModule};
    use graphql_orm::prelude::{Database, SqliteBackend};

    struct Fixture {
        service: OrmAiRunService,
        database: Database<SqliteBackend>,
        clock: FixedClock,
        principal_reference: PrincipalReference,
    }

    async fn fixture() -> Fixture {
        let database = Database::<SqliteBackend>::connect_sqlite("sqlite::memory:")
            .await
            .expect("in-memory SQLite should open");
        let module = crate::AiSchemaModule;
        let plan = database
            .schema()
            .plan_migration_to_entities("ai-run-test-v1", "AI run service test", module.entities())
            .await
            .expect("AI schema should plan");
        database
            .schema()
            .apply_migration(&plan, ApplyOptions::default())
            .await
            .expect("AI schema should apply");

        let now = OffsetDateTime::from_unix_timestamp(1_800_000_000)
            .expect("test timestamp should be valid");
        let clock = FixedClock::new(now);
        let principal = AuthPrincipal::User(AuthUser {
            user_id: "run-user".to_owned(),
            session_id: Uuid::new_v4(),
            roles: vec![],
            scopes: vec![],
            session: SessionContext::default(),
            token_claims: AccessTokenMetadata {
                tenant_id: Some("tenant-run".to_owned()),
                ..AccessTokenMetadata::default()
            },
        });
        let limits = AiRunServiceLimits::new(Duration::seconds(60), Duration::hours(1), 16, 2, 8)
            .expect("test run limits should validate");
        let service = OrmAiRunService::new(database.clone(), Arc::new(clock.clone()), limits);
        Fixture {
            service,
            database,
            clock,
            principal_reference: principal.reference(),
        }
    }

    async fn seed_queued(fixture: &Fixture) -> AiRunId {
        let run_id = AiRunId::new();
        let session_id = AiSessionId::new();
        AiSessionRecord::insert(
            &fixture.database,
            CreateAiSessionRecordInput {
                id: session_id.0,
                owner_principal_kind: "user".to_owned(),
                owner_subject: "run-user".to_owned(),
                tenant_id: Some("tenant-run".to_owned()),
                scope_kind: "tenant".to_owned(),
                scope_id: "tenant-run".to_owned(),
                title: "Run service test".to_owned(),
                state: "active".to_owned(),
                stream_head: 0,
                message_head: 0,
                last_activity_at: fixture.clock.now().unix_timestamp(),
                archived_at: None,
                deleted_at: None,
            },
        )
        .await
        .expect("test session should insert");
        AiRunRecord::insert(
            &fixture.database,
            CreateAiRunRecordInput {
                id: run_id.0,
                session_id: session_id.0,
                input_message_id: Uuid::new_v4(),
                principal_reference: serde_json::to_value(&fixture.principal_reference)
                    .expect("principal reference should serialize"),
                state: AiRunState::Queued.as_str().to_owned(),
                attempt_id: None,
                lease_owner: None,
                lease_generation: 0,
                lease_expires_at: None,
                lease_heartbeat_at: None,
                retry_count: 0,
                next_attempt_at: Some(fixture.clock.now().unix_timestamp()),
                error_code: None,
                latest_checkpoint_id: None,
            },
        )
        .await
        .expect("queued test run should insert");
        run_id
    }

    async fn run_record(fixture: &Fixture, run_id: AiRunId) -> AiRunRecord {
        AiRunRecord::find_by_id(&fixture.database, &run_id.0)
            .await
            .expect("run lookup should succeed")
            .expect("run should exist")
    }

    async fn attempt_outcomes(fixture: &Fixture) -> Vec<AiRunAttemptOutcomeRecord> {
        fixture
            .database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    tx.query::<AiRunAttemptOutcomeRecord>()
                        .limit(16)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("attempt outcomes should query")
    }

    #[tokio::test]
    async fn concurrent_workers_cannot_claim_the_same_run() {
        let fixture = fixture().await;
        let run_id = seed_queued(&fixture).await;

        let (first, second) = tokio::join!(
            fixture.service.claim_next("worker-a"),
            fixture.service.claim_next("worker-b"),
        );
        let first = first.expect("first claim should not fail");
        let second = second.expect("second claim should not fail");
        let claims: Vec<_> = [first, second].into_iter().flatten().collect();
        assert_eq!(claims.len(), 1, "only one worker may own the run");
        assert_eq!(claims[0].run_id(), run_id);
        assert_eq!(claims[0].lease_generation(), 1);

        let attempts = fixture
            .database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    tx.query::<AiRunAttemptRecord>()
                        .limit(4)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("attempt history should query");
        assert_eq!(attempts.len(), 1, "one immutable claim fact is written");
        assert_eq!(attempts[0].id, claims[0].attempt_id());
    }

    #[tokio::test]
    async fn heartbeat_rotates_row_version_and_rejects_the_old_lease() {
        let fixture = fixture().await;
        seed_queued(&fixture).await;
        let old = fixture
            .service
            .claim_next("worker-heartbeat")
            .await
            .expect("claim should succeed")
            .expect("run should be eligible");
        fixture.clock.advance_seconds(10);
        let renewed = fixture
            .service
            .heartbeat(&old)
            .await
            .expect("current lease should renew");
        assert!(renewed.lease_expires_at() > old.lease_expires_at());
        assert!(matches!(
            fixture.service.start(&old).await,
            Err(AiError::Conflict)
        ));
        let running = fixture
            .service
            .start(&renewed)
            .await
            .expect("renewed lease should start");
        assert_eq!(running.state(), AiRunState::Running);
    }

    #[tokio::test]
    async fn recovery_requeues_only_pre_provider_claims_and_fences_old_workers() {
        let fixture = fixture().await;
        let safe_run_id = seed_queued(&fixture).await;
        let safe_lease = fixture
            .service
            .claim_next("worker-safe")
            .await
            .expect("safe claim should succeed")
            .expect("safe run should be eligible");
        let uncertain_run_id = seed_queued(&fixture).await;
        let uncertain_lease = fixture
            .service
            .claim_next("worker-uncertain")
            .await
            .expect("uncertain claim should succeed")
            .expect("uncertain run should be eligible");
        let uncertain_lease = fixture
            .service
            .start(&uncertain_lease)
            .await
            .expect("uncertain claim should start");

        fixture.clock.advance_seconds(61);
        let report = fixture
            .service
            .recover_expired_leases()
            .await
            .expect("expired leases should reconcile");
        assert_eq!(report.requeued, 1);
        assert_eq!(report.recovery_required, 1);
        assert_eq!(report.failed, 0);
        assert_eq!(report.completed, 0);
        assert_eq!(
            run_record(&fixture, safe_run_id).await.state,
            AiRunState::RetryScheduled.as_str()
        );
        assert_eq!(
            run_record(&fixture, uncertain_run_id).await.state,
            AiRunState::RecoveryRequired.as_str()
        );
        assert!(matches!(
            fixture.service.heartbeat(&safe_lease).await,
            Err(AiError::Conflict)
        ));
        assert!(matches!(
            fixture.service.heartbeat(&uncertain_lease).await,
            Err(AiError::Conflict)
        ));

        let reclaimed = fixture
            .service
            .claim_next("worker-replacement")
            .await
            .expect("reclaim should succeed")
            .expect("safe retry should be eligible");
        assert_eq!(reclaimed.run_id(), safe_run_id);
        assert_eq!(reclaimed.lease_generation(), 2);
        let outcomes = attempt_outcomes(&fixture).await;
        assert_eq!(outcomes.len(), 2);
        assert!(
            outcomes
                .iter()
                .any(|outcome| outcome.outcome_code == "lease_expired_before_start")
        );
        assert!(
            outcomes
                .iter()
                .any(|outcome| outcome.outcome_code == "lease_expired_after_start")
        );
    }

    #[tokio::test]
    async fn recovery_finalizes_an_exact_persisted_output_checkpoint() {
        let fixture = fixture().await;
        let run_id = seed_queued(&fixture).await;
        let lease = fixture
            .service
            .claim_next("worker-output-checkpoint")
            .await
            .expect("claim should succeed")
            .expect("run should be eligible");
        let lease = fixture
            .service
            .start(&lease)
            .await
            .expect("claim should start");
        let message_id = Uuid::new_v4();
        let provider_response_id = "response-checkpoint";
        let budget_reservation = AiBudgetReservationRecord::insert(
            &fixture.database,
            CreateAiBudgetReservationRecordInput {
                budget_counter_ids: serde_json::json!([]),
                scope_kind: "tenant".to_owned(),
                scope_id: "tenant-run".to_owned(),
                tenant_id: Some("tenant-run".to_owned()),
                principal_kind: "user".to_owned(),
                principal_subject: "run-user".to_owned(),
                session_id: lease.session_id().0,
                run_id: lease.run_id().0,
                attempt_id: lease.attempt_id(),
                lease_generation: lease.lease_generation(),
                provider_kind: "mock".to_owned(),
                provider_model: "checkpoint-test".to_owned(),
                pricing_policy_version: "checkpoint-pricing-v1".to_owned(),
                reserved_input_tokens: 1,
                reserved_output_tokens: 1,
                reserved_tool_units: 0,
                reserved_image_units: 0,
                reserved_cost_microunits: 1,
                reserved_runs: 1,
                actual_input_tokens: Some(1),
                actual_cached_input_tokens: Some(0),
                actual_output_tokens: Some(1),
                actual_tool_units: Some(0),
                actual_image_units: Some(0),
                actual_cost_microunits: Some(1),
                actual_runs: Some(1),
                idempotency_key: "output-checkpoint-test".to_owned(),
                state: "committed".to_owned(),
                expires_at: (fixture.clock.now() + Duration::minutes(5)).unix_timestamp(),
                reconciled_at: Some(fixture.clock.now().unix_timestamp()),
            },
        )
        .await
        .expect("committed test budget should insert");
        let budget_reservation_id = budget_reservation.id;
        let checkpoint_hash = final_output_checkpoint_hash(
            lease.run_id(),
            lease.attempt_id(),
            lease.lease_generation(),
            message_id,
            Some(provider_response_id),
            budget_reservation_id,
        );
        fixture
            .service
            .append_provider_output(
                &lease,
                PreparedProviderOutput {
                    message_id,
                    event_id: Uuid::new_v4(),
                    inbox_event_id: Uuid::new_v4(),
                    provider_kind: "mock".to_owned(),
                    provider_model: "checkpoint-test".to_owned(),
                    protected_preview: serde_json::json!({"protected": true}),
                    protected_event: serde_json::json!({"protected": true}),
                    protected_inbox_event: serde_json::json!({"protected": true}),
                    blocks: vec![PreparedProviderBlock {
                        id: Uuid::new_v4(),
                        kind: "text".to_owned(),
                        protected_content: serde_json::json!({"protected": true}),
                        byte_count: 4,
                        line_count: 1,
                    }],
                    correlation_id: budget_reservation_id.to_string(),
                    provider_response_id: Some(provider_response_id.to_owned()),
                    budget_reservation_id,
                    checkpoint_hash,
                    expected_owner_principal_kind: "user".to_owned(),
                    expected_owner_subject: "run-user".to_owned(),
                    expected_scope_kind: "tenant".to_owned(),
                    expected_scope_id: "tenant-run".to_owned(),
                    expected_tenant_id: Some("tenant-run".to_owned()),
                },
            )
            .await
            .expect("output and checkpoint should commit atomically");

        fixture.clock.advance_seconds(61);
        let report = fixture
            .service
            .recover_expired_leases()
            .await
            .expect("exact output checkpoint should reconcile");

        assert_eq!(report.completed, 1);
        assert_eq!(report.recovery_required, 0);
        assert_eq!(run_record(&fixture, run_id).await.state, "completed");
        let outcomes = attempt_outcomes(&fixture).await;
        assert_eq!(outcomes.len(), 1);
        assert_eq!(
            outcomes[0].outcome_code,
            "lease_expired_after_output_persisted"
        );
        assert_eq!(
            outcomes[0].provider_response_id.as_deref(),
            Some(provider_response_id)
        );
    }

    async fn assert_recovery_requeues_completed_tool_batch(provider_response_id: Option<&str>) {
        let fixture = fixture().await;
        let run_id = seed_queued(&fixture).await;
        let lease = fixture
            .service
            .claim_next("worker-tool-checkpoint")
            .await
            .expect("claim should succeed")
            .expect("run should be eligible");
        let lease = fixture
            .service
            .start(&lease)
            .await
            .expect("claim should start");
        let reservation = AiBudgetReservationRecord::insert(
            &fixture.database,
            CreateAiBudgetReservationRecordInput {
                budget_counter_ids: serde_json::json!([]),
                scope_kind: "tenant".to_owned(),
                scope_id: "tenant-run".to_owned(),
                tenant_id: Some("tenant-run".to_owned()),
                principal_kind: "user".to_owned(),
                principal_subject: "run-user".to_owned(),
                session_id: lease.session_id().0,
                run_id: lease.run_id().0,
                attempt_id: lease.attempt_id(),
                lease_generation: lease.lease_generation(),
                provider_kind: "openai".to_owned(),
                provider_model: "checkpoint-test".to_owned(),
                pricing_policy_version: "checkpoint-pricing-v1".to_owned(),
                reserved_input_tokens: 1,
                reserved_output_tokens: 1,
                reserved_tool_units: 1,
                reserved_image_units: 0,
                reserved_cost_microunits: 1,
                reserved_runs: 1,
                actual_input_tokens: Some(1),
                actual_cached_input_tokens: Some(0),
                actual_output_tokens: Some(1),
                actual_tool_units: Some(1),
                actual_image_units: Some(0),
                actual_cost_microunits: Some(1),
                actual_runs: Some(1),
                idempotency_key: "tool-checkpoint-test".to_owned(),
                state: "committed".to_owned(),
                expires_at: (fixture.clock.now() + Duration::minutes(5)).unix_timestamp(),
                reconciled_at: Some(fixture.clock.now().unix_timestamp()),
            },
        )
        .await
        .expect("committed test budget should insert");
        let tool_call_id = Uuid::new_v4();
        AiRunStepRecord::insert(
            &fixture.database,
            CreateAiRunStepRecordInput {
                id: tool_call_id,
                run_id: lease.run_id().0,
                step_index: 0,
                step_kind: "application_tool".to_owned(),
                state: "completed".to_owned(),
                lease_generation: lease.lease_generation(),
                started_at: Some(fixture.clock.now().unix_timestamp()),
                finished_at: Some(fixture.clock.now().unix_timestamp()),
                error_code: None,
            },
        )
        .await
        .expect("completed tool step should insert");
        AiToolCallRecord::insert(
            &fixture.database,
            CreateAiToolCallRecordInput {
                id: tool_call_id,
                run_id: lease.run_id().0,
                provider_call_key: "tool-checkpoint-call-key".to_owned(),
                provider_call_id: "call-tool-checkpoint".to_owned(),
                provider_kind: Some("openai".to_owned()),
                provider_model: Some("checkpoint-test".to_owned()),
                provider_response_id: provider_response_id.map(str::to_owned),
                budget_reservation_id: Some(reservation.id),
                provider_turn_index: 0,
                tool_call_index: 0,
                tool_id: "records.read".to_owned(),
                tool_fingerprint: "tool-fingerprint".to_owned(),
                protected_arguments: Some(serde_json::json!({"protected": true})),
                argument_hash: "argument-hash".to_owned(),
                protected_result: Some(serde_json::json!({"protected": true})),
                payload_purged_at: None,
                risk: "read_only".to_owned(),
                authorization_code: Some("allowed".to_owned()),
                authorization_policy_version: Some("tool-policy-v1".to_owned()),
                authorization_state_digest: Some("authorization-state".to_owned()),
                disclosure_schema_fingerprint: Some("disclosure-v1".to_owned()),
                result_classification: Some("internal".to_owned()),
                result_egress_decision_id: Some(Uuid::new_v4()),
                result_egress_manifest_hash: Some("manifest-hash".to_owned()),
                application_audit_ref: Some("application-audit".to_owned()),
                approval_id: None,
                idempotency_key: None,
                correlation_id: Some("tool-checkpoint-correlation".to_owned()),
                causation_id: Some(lease.input_message_id().to_string()),
                delegation_reference: None,
                lease_generation: lease.lease_generation(),
                state: "completed".to_owned(),
                completed_at: Some(fixture.clock.now().unix_timestamp()),
            },
        )
        .await
        .expect("completed tool call should insert");
        let checkpoint_id = Uuid::new_v4();
        let protected_state = serde_json::json!({
            "protection": "database_managed",
            "value": {"bounded": true},
        });
        let checkpoint_hash = coordinator_checkpoint_hash(
            lease.run_id(),
            lease.attempt_id(),
            lease.lease_generation(),
            checkpoint_id,
            "tool_batch_persisted",
            "openai",
            "checkpoint-test",
            provider_response_id,
            reservation.id,
            &protected_state,
        )
        .expect("checkpoint hash should encode");
        let checkpointed = fixture
            .service
            .append_coordinator_checkpoint(
                &lease,
                PreparedCoordinatorCheckpoint {
                    id: checkpoint_id,
                    checkpoint_kind: "tool_batch_persisted".to_owned(),
                    provider_kind: "openai".to_owned(),
                    provider_model: "checkpoint-test".to_owned(),
                    provider_response_id: provider_response_id.map(str::to_owned),
                    budget_reservation_id: reservation.id,
                    protected_state,
                    checkpoint_hash,
                    completed_tools: vec![PreparedCoordinatorCheckpointTool {
                        id: tool_call_id,
                        provider_call_id: "call-tool-checkpoint".to_owned(),
                        tool_id: "records.read".to_owned(),
                        result_egress_manifest_hash: "manifest-hash".to_owned(),
                    }],
                },
            )
            .await
            .expect("tool batch checkpoint should commit");
        assert_eq!(checkpointed.latest_checkpoint_id(), Some(checkpoint_id));

        fixture.clock.advance_seconds(61);
        let report = fixture
            .service
            .recover_expired_leases()
            .await
            .expect("exact tool checkpoint should requeue for adoption");
        assert_eq!(report.checkpoint_requeued, 1);
        assert_eq!(report.recovery_required, 0);
        let retry = run_record(&fixture, run_id).await;
        assert_eq!(retry.state, AiRunState::RetryScheduled.as_str());
        assert_eq!(retry.latest_checkpoint_id, Some(checkpoint_id));
        assert_eq!(
            retry.error_code.as_deref(),
            Some("checkpoint_adoption_ready")
        );

        let replacement = fixture
            .service
            .claim_next("worker-tool-adopter")
            .await
            .expect("replacement claim should succeed")
            .expect("checkpoint retry should be eligible");
        assert_eq!(replacement.lease_generation(), 2);
        assert_eq!(replacement.latest_checkpoint_id(), Some(checkpoint_id));
    }

    #[tokio::test]
    async fn recovery_requeues_stateful_and_stateless_completed_tool_batches() {
        assert_recovery_requeues_completed_tool_batch(Some("response-tool-checkpoint")).await;
        assert_recovery_requeues_completed_tool_batch(None).await;
    }

    #[tokio::test]
    async fn terminal_completion_is_fenced_and_records_one_immutable_outcome() {
        let fixture = fixture().await;
        let run_id = seed_queued(&fixture).await;
        let leased = fixture
            .service
            .claim_next("worker-complete")
            .await
            .expect("claim should succeed")
            .expect("run should be eligible");
        let running = fixture
            .service
            .start(&leased)
            .await
            .expect("run should start");
        let completion = AiRunCompletion::new(
            AiRunState::Completed,
            "provider_completed",
            None,
            Some("response-safe-reference".to_owned()),
        )
        .expect("completion should validate");
        fixture
            .service
            .finish(&running, completion)
            .await
            .expect("current fence should finish");

        let record = run_record(&fixture, run_id).await;
        assert_eq!(record.state, AiRunState::Completed.as_str());
        assert!(record.lease_owner.is_none());
        let outcomes = attempt_outcomes(&fixture).await;
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].attempt_id, running.attempt_id());
        assert_eq!(outcomes[0].final_state, AiRunState::Completed.as_str());
        assert_eq!(
            outcomes[0].provider_response_id.as_deref(),
            Some("response-safe-reference")
        );
        assert!(matches!(
            fixture
                .service
                .finish(
                    &running,
                    AiRunCompletion::new(AiRunState::Failed, "late_failure", None, None)
                        .expect("failure should validate")
                )
                .await,
            Err(AiError::Conflict)
        ));
    }

    #[tokio::test]
    async fn scheduled_retry_relinquishes_fence_and_uses_a_new_generation() {
        let fixture = fixture().await;
        let run_id = seed_queued(&fixture).await;
        let running = fixture
            .service
            .claim_next("worker-retry")
            .await
            .expect("claim should succeed")
            .expect("run should be eligible");
        let running = fixture
            .service
            .start(&running)
            .await
            .expect("run should start");
        fixture
            .service
            .schedule_retry(&running, Duration::seconds(30), "provider_unavailable")
            .await
            .expect("bounded retry should schedule");
        assert_eq!(
            run_record(&fixture, run_id).await.state,
            AiRunState::RetryScheduled.as_str()
        );
        assert!(
            fixture
                .service
                .claim_next("worker-too-early")
                .await
                .expect("early claim should query")
                .is_none()
        );
        fixture.clock.advance_seconds(30);
        let retry = fixture
            .service
            .claim_next("worker-retry-next")
            .await
            .expect("retry claim should succeed")
            .expect("retry should now be eligible");
        assert_eq!(retry.run_id(), run_id);
        assert_eq!(retry.retry_count(), 1);
        assert_eq!(retry.lease_generation(), 2);
    }
}
