//! Fenced OpenAI background-response submission and durable binding.

#![cfg(all(
    any(feature = "sqlite", feature = "postgres"),
    feature = "provider-openai"
))]

use std::sync::Arc;

use agql_auth::{Clock, PrincipalReference, PrincipalReferenceKind};
use graphql_orm::db::Database;
use graphql_orm::graphql::errors::{OrmErrorCode, OrmPublicError};
use graphql_orm::graphql::filters::{IntFilter, StringFilter, UuidFilter};
use graphql_orm::graphql::orm::{
    ConditionalUpdateOutcome, DefaultWriteBackend, MutationContext, OrderDirection,
    TransactionError, TransactionMode,
};
use sha2::{Digest, Sha256};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::orm_runs::{
    append_attempt_outcome, canonical_second, exact_state, lease_from_record,
    load_and_validate_active_lease, validate_worker_id,
};
use crate::persistence::*;
use crate::{
    AI_EGRESS_RETENTION_PROVIDER_RESPONSE, AiBudgetReconciliation, AiBudgetReconciliationOutcome,
    AiBudgetReservationId, AiBudgetService, AiEgressCapability, AiEgressDecisionAudit, AiError,
    AiProviderCallPlan, AiRunId, AiRunLease, AiRunState, AiRuntime, AiSessionAction,
    ModelContinuationMode, ModelInputBlock, OrmAiRunService, ProviderBackgroundBinding,
    ProviderBackgroundSubmission, ProviderKind, ProviderRequestContext,
};

const MAXIMUM_TEMPORARY_RESPONSE_WINDOW: Duration = Duration::minutes(10);
const MAXIMUM_STORED_RESPONSE_WINDOW: Duration = Duration::days(30);
const MAXIMUM_RECONCILIATION_LEASE_TTL: Duration = Duration::minutes(5);
const MAXIMUM_RECONCILIATION_RETRY_DELAY: Duration = Duration::hours(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BackgroundSubmissionState {
    Prepared,
    WaitingProvider,
    Reconciling,
    Completed,
    Failed,
    Cancelled,
    RecoveryRequired,
}

impl BackgroundSubmissionState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::WaitingProvider => "waiting_provider",
            Self::Reconciling => "reconciling",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::RecoveryRequired => "recovery_required",
        }
    }

    fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "prepared" => Some(Self::Prepared),
            "waiting_provider" => Some(Self::WaitingProvider),
            "reconciling" => Some(Self::Reconciling),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            "recovery_required" => Some(Self::RecoveryRequired),
            _ => None,
        }
    }
}

/// Deployment-reviewed OpenAI response-availability windows used to capture a
/// fixed terminal-reconciliation deadline.
///
/// These windows are deliberately shorter than or equal to OpenAI's documented
/// provider-side application-state periods. They are availability limits, not
/// authorization to retain or disclose provider output. The exact
/// `provider_response` egress and retention proof remains mandatory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiOpenAiBackgroundReconciliationWindows {
    temporary_response: Duration,
    stored_response: Duration,
}

impl AiOpenAiBackgroundReconciliationWindows {
    /// Creates fixed response-availability windows for new submissions.
    ///
    /// The temporary window applies when the acknowledgement reports
    /// `store: false`; the stored window applies to `store: true`.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless both windows are at
    /// least one second and do not exceed the compiled ten-minute temporary
    /// and thirty-day stored-response ceilings.
    pub fn new(temporary_response: Duration, stored_response: Duration) -> Result<Self, AiError> {
        if temporary_response < Duration::SECOND
            || temporary_response > MAXIMUM_TEMPORARY_RESPONSE_WINDOW
            || stored_response < Duration::SECOND
            || stored_response > MAXIMUM_STORED_RESPONSE_WINDOW
        {
            return Err(AiError::InvalidConfiguration(
                "invalid OpenAI background reconciliation windows".to_owned(),
            ));
        }
        Ok(Self {
            temporary_response,
            stored_response,
        })
    }

    fn for_storage(self, stored: bool) -> Duration {
        if stored {
            self.stored_response
        } else {
            self.temporary_response
        }
    }
}

impl Default for AiOpenAiBackgroundReconciliationWindows {
    fn default() -> Self {
        Self {
            temporary_response: Duration::minutes(5),
            stored_response: Duration::days(29),
        }
    }
}

/// Durable content-free result of one accepted OpenAI background submission.
///
/// The result proves an exact original run/attempt/fence/profile/response
/// binding. It does not grant provider retrieval, receipt processing, budget
/// settlement, output persistence, or run-completion authority.
#[derive(Clone, PartialEq, Eq)]
pub struct AiOpenAiBackgroundSubmission {
    submission_id: Uuid,
    run_id: AiRunId,
    attempt_id: Uuid,
    lease_generation: i64,
    provider_profile_id: String,
    provider_model: String,
    maximum_output_tokens: u64,
    provider_store: bool,
    provider_response_id: String,
    provider_status: String,
    budget_reservation_id: AiBudgetReservationId,
}

impl AiOpenAiBackgroundSubmission {
    /// Opaque durable submission identifier echoed in provider metadata.
    pub const fn submission_id(&self) -> Uuid {
        self.submission_id
    }

    /// Owning run.
    pub const fn run_id(&self) -> AiRunId {
        self.run_id
    }

    /// Original attempt that crossed the provider boundary.
    pub const fn attempt_id(&self) -> Uuid {
        self.attempt_id
    }

    /// Original monotonically increasing fencing generation.
    pub const fn lease_generation(&self) -> i64 {
        self.lease_generation
    }

    /// Exact logical provider profile.
    pub fn provider_profile_id(&self) -> &str {
        &self.provider_profile_id
    }

    /// Exact requested model/routing key.
    pub fn provider_model(&self) -> &str {
        &self.provider_model
    }

    /// Exact provider-enforced output-token ceiling.
    pub const fn maximum_output_tokens(&self) -> u64 {
        self.maximum_output_tokens
    }

    /// Whether the provider reports durable response storage.
    pub const fn provider_store(&self) -> bool {
        self.provider_store
    }

    /// Provider response identifier returned by the create acknowledgement.
    pub fn provider_response_id(&self) -> &str {
        &self.provider_response_id
    }

    /// Provider status returned by the create acknowledgement.
    pub fn provider_status(&self) -> &str {
        &self.provider_status
    }

    /// Atomic reservation held uncertain until terminal reconciliation.
    pub const fn budget_reservation_id(&self) -> AiBudgetReservationId {
        self.budget_reservation_id
    }
}

impl std::fmt::Debug for AiOpenAiBackgroundSubmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AiOpenAiBackgroundSubmission")
            .field("submission_id", &"[REDACTED]")
            .field("run_id", &"[REDACTED]")
            .field("attempt_id", &"[REDACTED]")
            .field("lease_generation", &self.lease_generation)
            .field("provider_profile_id", &"[REDACTED]")
            .field("provider_model", &self.provider_model)
            .field("maximum_output_tokens", &self.maximum_output_tokens)
            .field("provider_store", &self.provider_store)
            .field("provider_response_id", &"[REDACTED]")
            .field("provider_status", &self.provider_status)
            .field("budget_reservation_id", &"[REDACTED]")
            .finish()
    }
}

/// Deployment-owned bounds for durable OpenAI background reconciliation
/// claims.
///
/// These limits bound database scans, lease lifetimes, nonterminal retry
/// scheduling, and serialization retries. They do not authorize provider
/// retrieval or extend a submission's immutable response-availability
/// deadline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiOpenAiBackgroundReconciliationLimits {
    lease_ttl: Duration,
    maximum_retry_delay: Duration,
    maximum_candidate_scan: usize,
    maximum_retries: u32,
    maximum_transaction_retries: usize,
}

impl AiOpenAiBackgroundReconciliationLimits {
    /// Creates validated background reconciliation worker limits.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless the lease and retry
    /// delay are at least one second and no greater than five minutes and one
    /// hour respectively, the candidate scan is within `1..=256`, the retry
    /// ceiling is at most 100, and transaction retries are at most 16.
    pub fn new(
        lease_ttl: Duration,
        maximum_retry_delay: Duration,
        maximum_candidate_scan: usize,
        maximum_retries: u32,
        maximum_transaction_retries: usize,
    ) -> Result<Self, AiError> {
        if lease_ttl < Duration::SECOND
            || lease_ttl > MAXIMUM_RECONCILIATION_LEASE_TTL
            || maximum_retry_delay < Duration::SECOND
            || maximum_retry_delay > MAXIMUM_RECONCILIATION_RETRY_DELAY
            || !(1..=256).contains(&maximum_candidate_scan)
            || maximum_retries > 100
            || maximum_transaction_retries > 16
        {
            return Err(AiError::InvalidConfiguration(
                "invalid OpenAI background reconciliation limits".to_owned(),
            ));
        }
        Ok(Self {
            lease_ttl,
            maximum_retry_delay,
            maximum_candidate_scan,
            maximum_retries,
            maximum_transaction_retries,
        })
    }

    /// Returns the lifetime of each newly issued or renewed claim.
    pub const fn lease_ttl(&self) -> Duration {
        self.lease_ttl
    }

    /// Returns the maximum rows considered by one claim pass.
    pub const fn maximum_candidate_scan(&self) -> usize {
        self.maximum_candidate_scan
    }
}

impl Default for AiOpenAiBackgroundReconciliationLimits {
    fn default() -> Self {
        Self {
            lease_ttl: Duration::minutes(1),
            maximum_retry_delay: Duration::minutes(5),
            maximum_candidate_scan: 64,
            maximum_retries: 16,
            maximum_transaction_retries: 8,
        }
    }
}

/// Exact owner/generation/row-version proof for one background reconciliation
/// claim.
///
/// Fields are private so a caller cannot manufacture or alter a claim. The
/// value grants no provider credential, retrieval, egress, budget, output, or
/// run-mutation authority. Every operation reloads the durable row and checks
/// the owner, generation, expiry, immutable response binding, and row version.
/// A later retrieval service must independently rehydrate the stored current
/// principal before provider egress.
#[derive(Clone, PartialEq, Eq)]
pub struct AiOpenAiBackgroundReconciliationClaim {
    submission_id: Uuid,
    session_id: crate::AiSessionId,
    run_id: AiRunId,
    attempt_id: Uuid,
    original_lease_generation: i64,
    principal_reference: PrincipalReference,
    worker_id: String,
    reconciliation_generation: i64,
    reconciliation_lease_expires_at: OffsetDateTime,
    row_version: i64,
    retry_count: u32,
    reconciliation_deadline: OffsetDateTime,
    provider_profile_id: String,
    provider_model: String,
    maximum_output_tokens: u64,
    provider_store: bool,
    provider_response_id: String,
    provider_created_at: i64,
    request_hash: String,
    budget_reservation_id: AiBudgetReservationId,
    original_egress_decision_id: Uuid,
    original_egress_manifest_hash: String,
    scope_kind: String,
    scope_id: String,
    tenant_id: Option<String>,
}

impl AiOpenAiBackgroundReconciliationClaim {
    /// Opaque durable background submission identifier.
    pub const fn submission_id(&self) -> Uuid {
        self.submission_id
    }

    /// Owning run.
    pub const fn run_id(&self) -> AiRunId {
        self.run_id
    }

    /// Original provider-crossing attempt.
    pub const fn attempt_id(&self) -> Uuid {
        self.attempt_id
    }

    /// Original run fencing generation bound to the provider request.
    pub const fn original_lease_generation(&self) -> i64 {
        self.original_lease_generation
    }

    /// Current reconciliation worker owner.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Monotonic reconciliation fencing generation.
    pub const fn reconciliation_generation(&self) -> i64 {
        self.reconciliation_generation
    }

    /// Current claim expiry.
    pub const fn reconciliation_lease_expires_at(&self) -> OffsetDateTime {
        self.reconciliation_lease_expires_at
    }

    /// Number of prior nonterminal releases.
    pub const fn retry_count(&self) -> u32 {
        self.retry_count
    }

    /// Immutable provider-response availability deadline.
    pub const fn reconciliation_deadline(&self) -> OffsetDateTime {
        self.reconciliation_deadline
    }
}

impl std::fmt::Debug for AiOpenAiBackgroundReconciliationClaim {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AiOpenAiBackgroundReconciliationClaim")
            .field("submission_id", &"[REDACTED]")
            .field("session_id", &"[REDACTED]")
            .field("run_id", &"[REDACTED]")
            .field("attempt_id", &"[REDACTED]")
            .field("original_lease_generation", &self.original_lease_generation)
            .field("principal_reference", &"[REDACTED]")
            .field("worker_id", &self.worker_id)
            .field("reconciliation_generation", &self.reconciliation_generation)
            .field(
                "reconciliation_lease_expires_at",
                &self.reconciliation_lease_expires_at,
            )
            .field("retry_count", &self.retry_count)
            .field("reconciliation_deadline", &self.reconciliation_deadline)
            .field("provider_profile_id", &"[REDACTED]")
            .field("provider_model", &self.provider_model)
            .field("maximum_output_tokens", &self.maximum_output_tokens)
            .field("provider_store", &self.provider_store)
            .field("provider_response_id", &"[REDACTED]")
            .field("provider_created_at", &self.provider_created_at)
            .field("request_hash", &"[REDACTED]")
            .field("budget_reservation_id", &"[REDACTED]")
            .field("original_egress_decision_id", &"[REDACTED]")
            .field("original_egress_manifest_hash", &"[REDACTED]")
            .field("scope_kind", &self.scope_kind)
            .field("scope_id", &"[REDACTED]")
            .field("tenant_id", &"[REDACTED]")
            .finish()
    }
}

/// Generated-ORM worker queue for content-free OpenAI background
/// reconciliation claims.
///
/// This service can claim, reclaim, heartbeat, and voluntarily release only
/// the durable reconciliation fence. It cannot retrieve provider output,
/// resolve credentials, match webhook content, settle usage, persist output,
/// or mutate the parked run.
#[derive(Clone)]
pub struct OrmAiOpenAiBackgroundReconciliationService {
    database: Database<DefaultWriteBackend>,
    clock: Arc<dyn Clock>,
    limits: AiOpenAiBackgroundReconciliationLimits,
}

/// ORM-backed executor for one exact OpenAI background response submission.
///
/// This executor intentionally supports only an initial, provider-retained,
/// tool-free and attachment-free turn. The restriction keeps asynchronous
/// tool execution, provider files, and mixed continuation outside the
/// submission boundary until their independent reconciliation contracts are
/// implemented.
pub struct OrmAiOpenAiBackgroundSubmissionService {
    run_service: OrmAiRunService,
    runtime: Arc<AiRuntime>,
    budget_service: Arc<dyn AiBudgetService>,
    egress_audit: Arc<dyn AiEgressDecisionAudit>,
    clock: Arc<dyn Clock>,
    reconciliation_windows: AiOpenAiBackgroundReconciliationWindows,
}

impl OrmAiOpenAiBackgroundSubmissionService {
    /// Creates a fenced OpenAI background-submission service.
    ///
    /// The egress audit must durably include the generated-ORM allow event in
    /// the run service's database. [`crate::OrmAiEgressDecisionAudit`] is the
    /// supplied implementation; a composite audit may add sinks but must not
    /// replace that authoritative row.
    pub fn new(
        run_service: OrmAiRunService,
        runtime: Arc<AiRuntime>,
        budget_service: Arc<dyn AiBudgetService>,
        egress_audit: Arc<dyn AiEgressDecisionAudit>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            run_service,
            runtime,
            budget_service,
            egress_audit,
            clock,
            reconciliation_windows: AiOpenAiBackgroundReconciliationWindows::default(),
        }
    }

    /// Overrides the fixed provider-response availability windows captured for
    /// subsequently accepted submissions.
    ///
    /// Narrower deployment values are appropriate when a logical provider
    /// profile has a shorter reviewed retention contract. This does not alter
    /// already persisted deadlines.
    #[must_use]
    pub fn with_reconciliation_windows(
        mut self,
        windows: AiOpenAiBackgroundReconciliationWindows,
    ) -> Self {
        self.reconciliation_windows = windows;
        self
    }

    /// Submits one exactly authorized OpenAI request for background execution.
    ///
    /// The service first inserts a content-free `prepared` binding and renews
    /// the current fence in one transaction. It then rehydrates authority,
    /// marks the atomic budget reservation uncertain, and crosses the provider
    /// boundary once. A bounded exact acknowledgement is fenced into the
    /// binding while the run becomes lease-free `WaitingProvider`.
    ///
    /// A failure after the provider boundary is never retried automatically.
    /// The prepared binding, uncertain budget, and eventual webhook metadata
    /// allow a later independently authorized reconciler or operator to
    /// classify the ambiguity without replaying the request.
    ///
    /// # Errors
    ///
    /// Fails closed for runtime readiness, a stale lease or swapped plan,
    /// tools/attachments/continuations, absent exact temporary-retention
    /// authorization, current access or egress denial, budget failure,
    /// malformed provider acknowledgement, or any persistence conflict.
    pub async fn submit(
        &self,
        lease: &AiRunLease,
        plan: AiProviderCallPlan,
    ) -> Result<AiOpenAiBackgroundSubmission, AiError> {
        validate_plan(lease, &plan)?;
        if !self.runtime.start_gate().is_ready() {
            return Err(AiError::RuntimeNotReady);
        }

        let principal = self
            .runtime
            .resolve_current_principal(lease.principal_reference())
            .await?;
        require_access(&self.runtime, &principal, lease, plan.budget_request()).await?;
        let reservation = self
            .budget_service
            .reserve(&principal, plan.budget_request().clone())
            .await?;
        let authorized_budget = match reservation.authorize_provider_call(
            lease.run_id(),
            lease.attempt_id(),
            lease.lease_generation(),
            plan.provider_kind_ref(),
            &plan.request_ref().model,
            plan.request_ref().maximum_output_tokens.unwrap_or(0),
            plan.request_ref().maximum_builtin_tool_calls(),
            self.clock.now(),
        ) {
            Ok(authorized) => authorized,
            Err(_) => {
                self.release_unstarted(&principal, lease, reservation.id())
                    .await?;
                return Err(AiError::BudgetDenied);
            }
        };

        let manifest = plan.transfers()[0].clone();
        let decision = match self
            .runtime
            .authorize_egress(lease.principal_reference(), &manifest)
            .await
        {
            Ok(decision) => decision,
            Err(error) => {
                self.release_unstarted(&principal, lease, reservation.id())
                    .await?;
                return Err(error);
            }
        };
        if let Err(error) = self.egress_audit.record(&manifest, &decision).await {
            self.release_unstarted(&principal, lease, reservation.id())
                .await?;
            return Err(error);
        }
        let proof = match decision.authorize(&manifest) {
            Ok(proof) => proof,
            Err(error) => {
                self.release_unstarted(&principal, lease, reservation.id())
                    .await?;
                return Err(error);
            }
        };
        let context = match ProviderRequestContext::new(
            lease.session_id(),
            lease.run_id(),
            plan.correlation_id(),
            authorized_budget,
            manifest.clone(),
            proof,
        ) {
            Ok(context) => context,
            Err(error) => {
                self.release_unstarted(&principal, lease, reservation.id())
                    .await?;
                return Err(error);
            }
        };

        let current = match self
            .runtime
            .resolve_current_principal(lease.principal_reference())
            .await
        {
            Ok(current) => current,
            Err(error) => {
                self.release_unstarted(&principal, lease, reservation.id())
                    .await?;
                return Err(error);
            }
        };
        if let Err(error) =
            require_access(&self.runtime, &current, lease, plan.budget_request()).await
        {
            self.release_unstarted(&current, lease, reservation.id())
                .await?;
            return Err(error);
        }

        let request_hash = request_hash(plan.request_ref())?;
        let (submission_key, submission_id) = submission_identity(
            lease,
            &manifest.provider_profile_id,
            &plan.request_ref().model,
            &request_hash,
            reservation.id(),
            &decision.manifest_hash,
        );
        let prepared = PreparedBackgroundSubmission {
            submission_id,
            submission_key: submission_key.clone(),
            provider_profile_id: manifest.provider_profile_id.clone(),
            provider_model: plan.request_ref().model.clone(),
            maximum_output_tokens: plan
                .request_ref()
                .maximum_output_tokens
                .ok_or(AiError::Conflict)?,
            request_hash,
            budget_reservation_id: reservation.id(),
            egress_decision_id: decision.id.0,
            egress_manifest_hash: decision.manifest_hash,
            scope_kind: plan.budget_request().scope.kind.clone(),
            scope_id: plan.budget_request().scope.id.clone(),
            tenant_id: plan.budget_request().scope.tenant_id.clone(),
        };
        let mut renewed = match self.prepare(lease, &prepared).await {
            Ok(renewed) => renewed,
            Err(error) => {
                self.release_unstarted(&current, lease, reservation.id())
                    .await?;
                return Err(error);
            }
        };

        if let Err(error) = self
            .budget_service
            .reconcile(
                &current,
                AiBudgetReconciliation {
                    reservation_id: reservation.id(),
                    attempt_id: renewed.attempt_id(),
                    lease_generation: renewed.lease_generation(),
                    actual: None,
                    cached_input_tokens: None,
                    outcome: AiBudgetReconciliationOutcome::MarkUncertain,
                },
            )
            .await
        {
            let release = self
                .release_unstarted(&current, &renewed, reservation.id())
                .await;
            self.close_ambiguity(&renewed, &prepared, "budget_uncertainty_not_persisted")
                .await?;
            release?;
            return Err(error);
        }

        let acknowledgement = match self
            .submit_with_heartbeats(
                &mut renewed,
                &ProviderKind::OpenAi,
                plan.request_ref().clone(),
                context,
                ProviderBackgroundBinding::new(
                    submission_id,
                    submission_key,
                    manifest.provider_profile_id,
                ),
            )
            .await
        {
            Ok(acknowledgement) => acknowledgement,
            Err(_) => {
                self.close_ambiguity(&renewed, &prepared, "provider_submission_outcome_uncertain")
                    .await?;
                return Err(AiError::ProviderFailed);
            }
        };
        match self
            .accept(&renewed, prepared.clone(), acknowledgement)
            .await
        {
            Ok(accepted) => Ok(accepted),
            Err(error) => {
                self.close_ambiguity(
                    &renewed,
                    &prepared,
                    "provider_acknowledgement_not_persisted",
                )
                .await?;
                Err(error)
            }
        }
    }

    async fn submit_with_heartbeats(
        &self,
        lease: &mut AiRunLease,
        provider_kind: &ProviderKind,
        request: crate::ModelRequest,
        context: ProviderRequestContext,
        binding: ProviderBackgroundBinding,
    ) -> Result<ProviderBackgroundSubmission, AiError> {
        let submission =
            self.runtime
                .submit_provider_background(provider_kind, request, context, binding);
        tokio::pin!(submission);
        let heartbeat_delay = std::cmp::max(
            self.run_service.lease_ttl().unsigned_abs() / 3,
            std::time::Duration::from_nanos(1),
        );
        loop {
            let heartbeat = tokio::time::sleep(heartbeat_delay);
            tokio::pin!(heartbeat);
            tokio::select! {
                result = &mut submission => {
                    return result.map_err(|_| AiError::ProviderFailed);
                }
                () = &mut heartbeat => {
                    *lease = self.run_service.heartbeat(lease).await?;
                }
            }
        }
    }

    async fn prepare(
        &self,
        lease: &AiRunLease,
        prepared: &PreparedBackgroundSubmission,
    ) -> Result<AiRunLease, AiError> {
        let now = canonical_second(self.clock.now());
        let expiry = now
            .checked_add(self.run_service.lease_ttl())
            .ok_or(AiError::PersistenceFailed)?;
        let lease = lease.clone();
        let prepared = prepared.clone();
        self.run_service
            .database()
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = load_and_validate_active_lease(tx, &lease, now).await?;
                    if AiRunState::from_persisted(&current.state) != Some(AiRunState::Running) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&lease.session_id().0)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if session.state != "active"
                        || session.deleted_at.is_some()
                        || session.scope_kind != prepared.scope_kind
                        || session.scope_id != prepared.scope_id
                        || session.tenant_id != prepared.tenant_id
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let budget = tx
                        .find_by_id::<AiBudgetReservationRecord>(&prepared.budget_reservation_id.0)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if budget.session_id != lease.session_id().0
                        || budget.run_id != lease.run_id().0
                        || budget.attempt_id != lease.attempt_id()
                        || budget.lease_generation != lease.lease_generation()
                        || budget.provider_kind != "openai"
                        || budget.provider_model != prepared.provider_model
                        || budget.state != "reserved"
                        || budget.reconciled_at.is_some()
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let egress = tx
                        .query::<AiEgressEventRecord>()
                        .filter(AiEgressEventRecordWhereInput {
                            id: Some(UuidFilter {
                                eq: Some(prepared.egress_decision_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(1)
                        .fetch_one()
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if egress.run_id != Some(lease.run_id().0)
                        || egress.manifest_hash != prepared.egress_manifest_hash
                        || egress.capability != "model_inference"
                        || egress.outcome != "allow"
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    tx.insert::<AiProviderBackgroundSubmissionRecord>(
                        CreateAiProviderBackgroundSubmissionRecordInput {
                            id: prepared.submission_id,
                            submission_key: prepared.submission_key,
                            session_id: lease.session_id().0,
                            run_id: lease.run_id().0,
                            attempt_id: lease.attempt_id(),
                            lease_generation: lease.lease_generation(),
                            provider_kind: "openai".to_owned(),
                            provider_profile_id: prepared.provider_profile_id,
                            provider_model: prepared.provider_model,
                            maximum_output_tokens: i64::try_from(prepared.maximum_output_tokens)
                                .map_err(|_| OrmPublicError::new(OrmErrorCode::InvalidInput))?,
                            provider_store: None,
                            request_hash: prepared.request_hash,
                            budget_reservation_id: prepared.budget_reservation_id.0,
                            egress_decision_id: prepared.egress_decision_id,
                            egress_manifest_hash: prepared.egress_manifest_hash,
                            provider_response_id: None,
                            provider_status: None,
                            state: BackgroundSubmissionState::Prepared.as_str().to_owned(),
                            safe_error_code: None,
                            provider_created_at: None,
                            submitted_at: None,
                            reconciliation_owner: None,
                            reconciliation_generation: 0,
                            reconciliation_lease_expires_at: None,
                            reconciliation_next_attempt_at: None,
                            reconciliation_retry_count: 0,
                            reconciliation_deadline: None,
                            reconciled_at: None,
                            retrieval_egress_decision_id: None,
                            terminal_message_id: None,
                        },
                    )
                    .await
                    .map_err(OrmPublicError::from)?;
                    let update = tx
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
                    let updated = match update {
                        ConditionalUpdateOutcome::Updated(updated) => updated,
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                        actor_principal_kind: "system".to_owned(),
                        actor_subject: "provider-background".to_owned(),
                        action: "prepare_provider_background_submission".to_owned(),
                        resource_kind: "ai_provider_background_submission".to_owned(),
                        resource_reference: prepared.submission_id.to_string(),
                        outcome: "prepared".to_owned(),
                        reason_code: "exact_submission_bound".to_owned(),
                        correlation_id: prepared.submission_id.to_string(),
                        causation_id: Some(lease.run_id().0.to_string()),
                        policy_version: Some("provider-background-v1".to_owned()),
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    lease_from_record(&updated)
                })
            })
            .await
            .map_err(map_transaction)
    }

    async fn accept(
        &self,
        lease: &AiRunLease,
        prepared: PreparedBackgroundSubmission,
        acknowledgement: ProviderBackgroundSubmission,
    ) -> Result<AiOpenAiBackgroundSubmission, AiError> {
        if !valid_response_id(acknowledgement.response_id())
            || !valid_provider_status(acknowledgement.status())
            || acknowledgement.created_at() <= 0
            || acknowledgement.provider_model() != prepared.provider_model
            || acknowledgement.maximum_output_tokens() != prepared.maximum_output_tokens
        {
            return Err(AiError::ProviderFailed);
        }
        let now = canonical_second(self.clock.now());
        let response_window = self
            .reconciliation_windows
            .for_storage(acknowledgement.provider_store())
            .whole_seconds();
        let provider_deadline = acknowledgement
            .created_at()
            .checked_add(response_window)
            .ok_or(AiError::ProviderFailed)?;
        let local_deadline = now
            .unix_timestamp()
            .checked_add(response_window)
            .ok_or(AiError::ProviderFailed)?;
        let reconciliation_deadline = provider_deadline.min(local_deadline);
        if reconciliation_deadline <= now.unix_timestamp() {
            return Err(AiError::ProviderFailed);
        }
        let result_run_id = lease.run_id();
        let result_attempt_id = lease.attempt_id();
        let result_lease_generation = lease.lease_generation();
        let lease = lease.clone();
        let acknowledgement_for_tx = acknowledgement.clone();
        let prepared_for_tx = prepared.clone();
        self.run_service
            .database()
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = load_and_validate_active_lease(tx, &lease, now).await?;
                    if AiRunState::from_persisted(&current.state) != Some(AiRunState::Running) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let submission = tx
                        .find_by_id::<AiProviderBackgroundSubmissionRecord>(
                            &prepared_for_tx.submission_id,
                        )
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if !submission_matches(&submission, &lease, &prepared_for_tx)
                        || BackgroundSubmissionState::from_persisted(&submission.state)
                            != Some(BackgroundSubmissionState::Prepared)
                        || submission.provider_response_id.is_some()
                        || submission.provider_status.is_some()
                        || submission.provider_store.is_some()
                        || submission.provider_created_at.is_some()
                        || submission.submitted_at.is_some()
                        || submission.reconciliation_owner.is_some()
                        || submission.reconciliation_generation != 0
                        || submission.reconciliation_lease_expires_at.is_some()
                        || submission.reconciliation_next_attempt_at.is_some()
                        || submission.reconciliation_retry_count != 0
                        || submission.reconciliation_deadline.is_some()
                        || submission.reconciled_at.is_some()
                        || submission.retrieval_egress_decision_id.is_some()
                        || submission.terminal_message_id.is_some()
                        || submission.row_version != 0
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let submission_update = tx
                        .compare_and_swap::<AiProviderBackgroundSubmissionRecord>(
                            &submission.id,
                            submission.row_version,
                            AiProviderBackgroundSubmissionRecordWhereInput::default(),
                            UpdateAiProviderBackgroundSubmissionRecordInput {
                                provider_response_id: Some(Some(
                                    acknowledgement_for_tx.response_id().to_owned(),
                                )),
                                provider_status: Some(Some(
                                    acknowledgement_for_tx.status().to_owned(),
                                )),
                                provider_store: Some(Some(acknowledgement_for_tx.provider_store())),
                                state: Some(
                                    BackgroundSubmissionState::WaitingProvider
                                        .as_str()
                                        .to_owned(),
                                ),
                                provider_created_at: Some(Some(
                                    acknowledgement_for_tx.created_at(),
                                )),
                                submitted_at: Some(Some(now.unix_timestamp())),
                                reconciliation_next_attempt_at: Some(Some(now.unix_timestamp())),
                                reconciliation_deadline: Some(Some(reconciliation_deadline)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    let updated_submission = match submission_update {
                        ConditionalUpdateOutcome::Updated(updated) => updated,
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    if !valid_waiting_submission(&updated_submission) {
                        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                    }
                    let run_update = tx
                        .compare_and_swap::<AiRunRecord>(
                            &current.id,
                            current.row_version,
                            exact_state(AiRunState::Running.as_str()),
                            UpdateAiRunRecordInput {
                                state: Some(AiRunState::WaitingProvider.as_str().to_owned()),
                                lease_owner: Some(None),
                                lease_expires_at: Some(None),
                                lease_heartbeat_at: Some(None),
                                next_attempt_at: Some(None),
                                error_code: Some(None),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(run_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                        actor_principal_kind: "system".to_owned(),
                        actor_subject: "provider-background".to_owned(),
                        action: "accept_provider_background_submission".to_owned(),
                        resource_kind: "ai_provider_background_submission".to_owned(),
                        resource_reference: prepared_for_tx.submission_id.to_string(),
                        outcome: "waiting_provider".to_owned(),
                        reason_code: "provider_acknowledgement_bound".to_owned(),
                        correlation_id: prepared_for_tx.submission_id.to_string(),
                        causation_id: Some(lease.run_id().0.to_string()),
                        policy_version: Some("provider-background-v1".to_owned()),
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    Ok(())
                })
            })
            .await
            .map_err(map_transaction)?;

        Ok(AiOpenAiBackgroundSubmission {
            submission_id: prepared.submission_id,
            run_id: result_run_id,
            attempt_id: result_attempt_id,
            lease_generation: result_lease_generation,
            provider_profile_id: prepared.provider_profile_id,
            provider_model: prepared.provider_model,
            maximum_output_tokens: prepared.maximum_output_tokens,
            provider_store: acknowledgement.provider_store(),
            provider_response_id: acknowledgement.response_id().to_owned(),
            provider_status: acknowledgement.status().to_owned(),
            budget_reservation_id: prepared.budget_reservation_id,
        })
    }

    async fn close_ambiguity(
        &self,
        lease: &AiRunLease,
        prepared: &PreparedBackgroundSubmission,
        safe_error_code: &'static str,
    ) -> Result<(), AiError> {
        let now = canonical_second(self.clock.now());
        let lease = lease.clone();
        let prepared = prepared.clone();
        self.run_service
            .database()
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = load_and_validate_active_lease(tx, &lease, now).await?;
                    if AiRunState::from_persisted(&current.state) != Some(AiRunState::Running) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let submission = tx
                        .find_by_id::<AiProviderBackgroundSubmissionRecord>(&prepared.submission_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if !submission_matches(&submission, &lease, &prepared)
                        || BackgroundSubmissionState::from_persisted(&submission.state)
                            != Some(BackgroundSubmissionState::Prepared)
                        || submission.provider_response_id.is_some()
                        || submission.provider_status.is_some()
                        || submission.provider_store.is_some()
                        || submission.provider_created_at.is_some()
                        || submission.submitted_at.is_some()
                        || submission.safe_error_code.is_some()
                        || submission.reconciliation_owner.is_some()
                        || submission.reconciliation_generation != 0
                        || submission.reconciliation_lease_expires_at.is_some()
                        || submission.reconciliation_next_attempt_at.is_some()
                        || submission.reconciliation_retry_count != 0
                        || submission.reconciliation_deadline.is_some()
                        || submission.reconciled_at.is_some()
                        || submission.retrieval_egress_decision_id.is_some()
                        || submission.terminal_message_id.is_some()
                        || submission.row_version != 0
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let submission_update = tx
                        .compare_and_swap::<AiProviderBackgroundSubmissionRecord>(
                            &submission.id,
                            submission.row_version,
                            AiProviderBackgroundSubmissionRecordWhereInput::default(),
                            UpdateAiProviderBackgroundSubmissionRecordInput {
                                state: Some(
                                    BackgroundSubmissionState::RecoveryRequired
                                        .as_str()
                                        .to_owned(),
                                ),
                                safe_error_code: Some(Some(safe_error_code.to_owned())),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    let updated_submission = match submission_update {
                        ConditionalUpdateOutcome::Updated(updated) => updated,
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    if BackgroundSubmissionState::from_persisted(&updated_submission.state)
                        != Some(BackgroundSubmissionState::RecoveryRequired)
                        || updated_submission.safe_error_code.as_deref() != Some(safe_error_code)
                        || !reconciliation_fields_are_empty(&updated_submission)
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                    }
                    let run_update = tx
                        .compare_and_swap::<AiRunRecord>(
                            &current.id,
                            current.row_version,
                            exact_state(AiRunState::Running.as_str()),
                            UpdateAiRunRecordInput {
                                state: Some(AiRunState::RecoveryRequired.as_str().to_owned()),
                                lease_owner: Some(None),
                                lease_expires_at: Some(None),
                                lease_heartbeat_at: Some(None),
                                next_attempt_at: Some(None),
                                error_code: Some(Some(safe_error_code.to_owned())),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(run_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    append_attempt_outcome(
                        tx,
                        &lease,
                        AiRunState::RecoveryRequired,
                        safe_error_code.to_owned(),
                        None,
                        now,
                    )
                    .await?;
                    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                        actor_principal_kind: "system".to_owned(),
                        actor_subject: "provider-background".to_owned(),
                        action: "close_provider_background_submission_ambiguity".to_owned(),
                        resource_kind: "ai_provider_background_submission".to_owned(),
                        resource_reference: prepared.submission_id.to_string(),
                        outcome: "recovery_required".to_owned(),
                        reason_code: safe_error_code.to_owned(),
                        correlation_id: prepared.submission_id.to_string(),
                        causation_id: Some(lease.run_id().0.to_string()),
                        policy_version: Some("provider-background-v1".to_owned()),
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    Ok(())
                })
            })
            .await
            .map_err(map_transaction)
    }

    async fn release_unstarted(
        &self,
        principal: &agql_auth::ResolvedPrincipal,
        lease: &AiRunLease,
        reservation_id: AiBudgetReservationId,
    ) -> Result<(), AiError> {
        self.budget_service
            .reconcile(
                principal,
                AiBudgetReconciliation {
                    reservation_id,
                    attempt_id: lease.attempt_id(),
                    lease_generation: lease.lease_generation(),
                    actual: None,
                    cached_input_tokens: None,
                    outcome: AiBudgetReconciliationOutcome::ReleaseUnused,
                },
            )
            .await?;
        Ok(())
    }
}

impl OrmAiOpenAiBackgroundReconciliationService {
    /// Creates a bounded background reconciliation claim service.
    pub fn new(
        database: Database<DefaultWriteBackend>,
        clock: Arc<dyn Clock>,
        limits: AiOpenAiBackgroundReconciliationLimits,
    ) -> Self {
        Self {
            database,
            clock,
            limits,
        }
    }

    /// Returns the ORM database handle for host composition.
    pub const fn database(&self) -> &Database<DefaultWriteBackend> {
        &self.database
    }

    /// Claims the oldest eligible accepted submission or reclaims an expired
    /// reconciliation lease.
    ///
    /// The generated state-machine transaction revalidates the exact
    /// submission, run, session, original attempt, uncertain budget, and
    /// original egress allow before it CAS-increments the reconciliation
    /// generation. A verified webhook is not required and grants no priority
    /// or authority at this boundary.
    ///
    /// The returned claim still grants no provider retrieval authority. The
    /// future retrieval service must rehydrate current authority and audit a
    /// new exact egress allow before any provider request.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid worker ID, malformed or conflicting
    /// durable bindings, generation/time overflow, or persistence failure.
    pub async fn claim_next(
        &self,
        worker_id: &str,
    ) -> Result<Option<AiOpenAiBackgroundReconciliationClaim>, AiError> {
        validate_worker_id(worker_id)?;
        let now = canonical_second(self.clock.now());
        let filter_time =
            i32::try_from(now.unix_timestamp()).map_err(|_| AiError::PersistenceFailed)?;
        for retry in 0..=self.limits.maximum_transaction_retries {
            match self
                .claim_once(worker_id.to_owned(), now, filter_time)
                .await
            {
                Ok(claim) => return Ok(claim),
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

    /// Renews an unexpired reconciliation claim and returns its new
    /// row-version proof.
    ///
    /// Renewal can never extend the immutable response deadline. The
    /// next-eligible timestamp is rotated with the lease expiry so an expired
    /// claim becomes reclaimable through the same bounded queue.
    ///
    /// # Errors
    ///
    /// Fails closed for an expired deadline or lease, a stale owner,
    /// generation, or row version, malformed durable state, or persistence
    /// failure.
    pub async fn heartbeat(
        &self,
        claim: &AiOpenAiBackgroundReconciliationClaim,
    ) -> Result<AiOpenAiBackgroundReconciliationClaim, AiError> {
        let now = canonical_second(self.clock.now());
        for retry in 0..=self.limits.maximum_transaction_retries {
            match self.heartbeat_once(claim.clone(), now).await {
                Ok(updated) => return Ok(updated),
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

    /// Relinquishes a current claim before provider retrieval and schedules a
    /// bounded later attempt.
    ///
    /// This method is only for shutdown, local backpressure, or another
    /// condition known to precede provider I/O. Once a response retrieval has
    /// been attempted, only the future exact-response normalizer may classify
    /// and release the observation. The retry count increments atomically;
    /// the caller cannot schedule at or beyond the immutable deadline.
    ///
    /// # Errors
    ///
    /// Returns an error when the delay is shorter than one second, exceeds the
    /// deployment maximum, reaches the deadline, exhausts the retry ceiling,
    /// the claim is stale or expired, or persistence fails.
    pub async fn release_before_retrieval(
        &self,
        claim: &AiOpenAiBackgroundReconciliationClaim,
        delay: Duration,
    ) -> Result<(), AiError> {
        if delay < Duration::SECOND || delay > self.limits.maximum_retry_delay {
            return Err(AiError::InvalidInput(
                "invalid OpenAI background reconciliation delay".to_owned(),
            ));
        }
        if claim.retry_count >= self.limits.maximum_retries {
            return Err(AiError::Conflict);
        }
        let now = canonical_second(self.clock.now());
        let eligible_at = now.checked_add(delay).ok_or_else(|| {
            AiError::InvalidConfiguration(
                "background reconciliation retry time overflow".to_owned(),
            )
        })?;
        if eligible_at >= claim.reconciliation_deadline {
            return Err(AiError::Conflict);
        }
        for retry in 0..=self.limits.maximum_transaction_retries {
            match self
                .release_before_retrieval_once(claim.clone(), now, eligible_at)
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

    async fn claim_once(
        &self,
        worker_id: String,
        now: OffsetDateTime,
        filter_time: i32,
    ) -> Result<Option<AiOpenAiBackgroundReconciliationClaim>, TransactionError> {
        let database = self.database.clone();
        let limits = self.limits;
        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let candidates = tx
                        .query::<AiProviderBackgroundSubmissionRecord>()
                        .filter(AiProviderBackgroundSubmissionRecordWhereInput {
                            state: Some(StringFilter {
                                in_list: Some(vec![
                                    BackgroundSubmissionState::WaitingProvider
                                        .as_str()
                                        .to_owned(),
                                    BackgroundSubmissionState::Reconciling.as_str().to_owned(),
                                ]),
                                ..Default::default()
                            }),
                            reconciliation_next_attempt_at: Some(IntFilter {
                                lte: Some(filter_time),
                                ..Default::default()
                            }),
                            reconciliation_deadline: Some(IntFilter {
                                gt: Some(filter_time),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .order_by(AiProviderBackgroundSubmissionRecordOrderByInput {
                            reconciliation_next_attempt_at: Some(OrderDirection::Asc),
                            created_at: Some(OrderDirection::Asc),
                            id: Some(OrderDirection::Asc),
                            ..Default::default()
                        })
                        .limit(
                            i64::try_from(limits.maximum_candidate_scan)
                                .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?,
                        )
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let Some(current) = candidates.into_iter().next() else {
                        return Ok(None);
                    };
                    validate_claimable_submission(&current, now, limits.maximum_retries)?;
                    let graph = load_background_claim_graph(tx, &current).await?;
                    let generation = current
                        .reconciliation_generation
                        .checked_add(1)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let configured_expiry = now
                        .checked_add(limits.lease_ttl)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?
                        .unix_timestamp();
                    let deadline = current
                        .reconciliation_deadline
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let expiry = configured_expiry.min(deadline);
                    if expiry <= now.unix_timestamp() {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let previous_state = BackgroundSubmissionState::from_persisted(&current.state)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let outcome = tx
                        .compare_and_swap::<AiProviderBackgroundSubmissionRecord>(
                            &current.id,
                            current.row_version,
                            AiProviderBackgroundSubmissionRecordWhereInput {
                                state: Some(StringFilter {
                                    eq: Some(current.state.clone()),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            },
                            UpdateAiProviderBackgroundSubmissionRecordInput {
                                state: Some(
                                    BackgroundSubmissionState::Reconciling.as_str().to_owned(),
                                ),
                                reconciliation_owner: Some(Some(worker_id.clone())),
                                reconciliation_generation: Some(generation),
                                reconciliation_lease_expires_at: Some(Some(expiry)),
                                reconciliation_next_attempt_at: Some(Some(expiry)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    let updated = match outcome {
                        ConditionalUpdateOutcome::Updated(updated) => updated,
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    validate_active_reconciliation_record(&updated, &worker_id, generation, now)?;
                    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                        actor_principal_kind: "system".to_owned(),
                        actor_subject: worker_id,
                        action: "claim_provider_background_reconciliation".to_owned(),
                        resource_kind: "ai_provider_background_submission".to_owned(),
                        resource_reference: updated.id.to_string(),
                        outcome: "reconciling".to_owned(),
                        reason_code: match previous_state {
                            BackgroundSubmissionState::WaitingProvider => {
                                "eligible_submission_claimed"
                            }
                            BackgroundSubmissionState::Reconciling => {
                                "expired_reconciliation_reclaimed"
                            }
                            BackgroundSubmissionState::Prepared
                            | BackgroundSubmissionState::Completed
                            | BackgroundSubmissionState::Failed
                            | BackgroundSubmissionState::Cancelled
                            | BackgroundSubmissionState::RecoveryRequired => {
                                return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                            }
                        }
                        .to_owned(),
                        correlation_id: updated.id.to_string(),
                        causation_id: Some(updated.run_id.to_string()),
                        policy_version: Some("provider-background-reconciliation-v1".to_owned()),
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    claim_from_records(&updated, graph)
                        .map(Some)
                        .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))
                })
            })
            .await
    }

    async fn heartbeat_once(
        &self,
        claim: AiOpenAiBackgroundReconciliationClaim,
        now: OffsetDateTime,
    ) -> Result<AiOpenAiBackgroundReconciliationClaim, TransactionError> {
        let database = self.database.clone();
        let lease_ttl = self.limits.lease_ttl;
        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = tx
                        .find_by_id::<AiProviderBackgroundSubmissionRecord>(&claim.submission_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    validate_current_reconciliation_claim(&current, &claim, now)?;
                    let configured_expiry = now
                        .checked_add(lease_ttl)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?
                        .unix_timestamp();
                    let expiry =
                        configured_expiry.min(claim.reconciliation_deadline.unix_timestamp());
                    if expiry <= now.unix_timestamp() {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let outcome = tx
                        .compare_and_swap::<AiProviderBackgroundSubmissionRecord>(
                            &current.id,
                            current.row_version,
                            exact_background_state(BackgroundSubmissionState::Reconciling.as_str()),
                            UpdateAiProviderBackgroundSubmissionRecordInput {
                                reconciliation_lease_expires_at: Some(Some(expiry)),
                                reconciliation_next_attempt_at: Some(Some(expiry)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    let updated = match outcome {
                        ConditionalUpdateOutcome::Updated(updated) => updated,
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    validate_active_reconciliation_record(
                        &updated,
                        &claim.worker_id,
                        claim.reconciliation_generation,
                        now,
                    )?;
                    let mut renewed = claim;
                    renewed.reconciliation_lease_expires_at =
                        OffsetDateTime::from_unix_timestamp(expiry)
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    renewed.row_version = updated.row_version;
                    Ok(renewed)
                })
            })
            .await
    }

    async fn release_before_retrieval_once(
        &self,
        claim: AiOpenAiBackgroundReconciliationClaim,
        now: OffsetDateTime,
        eligible_at: OffsetDateTime,
    ) -> Result<(), TransactionError> {
        let database = self.database.clone();
        let maximum_retries = self.limits.maximum_retries;
        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = tx
                        .find_by_id::<AiProviderBackgroundSubmissionRecord>(&claim.submission_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    validate_current_reconciliation_claim(&current, &claim, now)?;
                    let retry_count = current
                        .reconciliation_retry_count
                        .checked_add(1)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::Conflict))?;
                    if u32::try_from(retry_count).map_or(true, |count| count > maximum_retries)
                        || eligible_at.unix_timestamp()
                            >= claim.reconciliation_deadline.unix_timestamp()
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let outcome = tx
                        .compare_and_swap::<AiProviderBackgroundSubmissionRecord>(
                            &current.id,
                            current.row_version,
                            exact_background_state(BackgroundSubmissionState::Reconciling.as_str()),
                            UpdateAiProviderBackgroundSubmissionRecordInput {
                                state: Some(
                                    BackgroundSubmissionState::WaitingProvider
                                        .as_str()
                                        .to_owned(),
                                ),
                                reconciliation_owner: Some(None),
                                reconciliation_lease_expires_at: Some(None),
                                reconciliation_next_attempt_at: Some(Some(
                                    eligible_at.unix_timestamp(),
                                )),
                                reconciliation_retry_count: Some(retry_count),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    let updated = match outcome {
                        ConditionalUpdateOutcome::Updated(updated) => updated,
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    validate_released_reconciliation_record(
                        &updated,
                        claim.reconciliation_generation,
                        retry_count,
                        eligible_at,
                    )?;
                    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                        actor_principal_kind: "system".to_owned(),
                        actor_subject: claim.worker_id,
                        action: "release_provider_background_reconciliation".to_owned(),
                        resource_kind: "ai_provider_background_submission".to_owned(),
                        resource_reference: updated.id.to_string(),
                        outcome: "waiting_provider".to_owned(),
                        reason_code: "worker_relinquished_before_retrieval".to_owned(),
                        correlation_id: updated.id.to_string(),
                        causation_id: Some(updated.run_id.to_string()),
                        policy_version: Some("provider-background-reconciliation-v1".to_owned()),
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    Ok(())
                })
            })
            .await
    }
}

struct BackgroundClaimGraph {
    principal_reference: PrincipalReference,
    scope_kind: String,
    scope_id: String,
    tenant_id: Option<String>,
}

async fn load_background_claim_graph(
    tx: &mut MutationContext<'_, DefaultWriteBackend>,
    submission: &AiProviderBackgroundSubmissionRecord,
) -> Result<BackgroundClaimGraph, OrmPublicError> {
    let run = tx
        .find_by_id::<AiRunRecord>(&submission.run_id)
        .await
        .map_err(OrmPublicError::from)?
        .ok_or_else(OrmPublicError::not_found)?;
    let principal_reference: PrincipalReference =
        serde_json::from_value(run.principal_reference.clone())
            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let session = tx
        .find_by_id::<AiSessionRecord>(&submission.session_id)
        .await
        .map_err(OrmPublicError::from)?
        .ok_or_else(OrmPublicError::not_found)?;
    let attempts = tx
        .query::<AiRunAttemptRecord>()
        .filter(AiRunAttemptRecordWhereInput {
            id: Some(UuidFilter {
                eq: Some(submission.attempt_id),
                ..Default::default()
            }),
            ..Default::default()
        })
        .limit(2)
        .fetch_all()
        .await
        .map_err(OrmPublicError::from)?;
    let [attempt] = attempts.as_slice() else {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    };
    let budget = tx
        .find_by_id::<AiBudgetReservationRecord>(&submission.budget_reservation_id)
        .await
        .map_err(OrmPublicError::from)?
        .ok_or_else(OrmPublicError::not_found)?;
    let egress_events = tx
        .query::<AiEgressEventRecord>()
        .filter(AiEgressEventRecordWhereInput {
            id: Some(UuidFilter {
                eq: Some(submission.egress_decision_id),
                ..Default::default()
            }),
            ..Default::default()
        })
        .limit(2)
        .fetch_all()
        .await
        .map_err(OrmPublicError::from)?;
    let [egress] = egress_events.as_slice() else {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    };
    let outcomes = tx
        .query::<AiRunAttemptOutcomeRecord>()
        .filter(AiRunAttemptOutcomeRecordWhereInput {
            attempt_id: Some(UuidFilter {
                eq: Some(submission.attempt_id),
                ..Default::default()
            }),
            ..Default::default()
        })
        .limit(2)
        .fetch_all()
        .await
        .map_err(OrmPublicError::from)?;
    let expected_principal_kind = match &principal_reference.kind {
        PrincipalReferenceKind::UserSession => "user".to_owned(),
        PrincipalReferenceKind::ApiToken { principal_kind } => {
            format!("api_token:{principal_kind}")
        }
    };
    if run.id != submission.run_id
        || run.session_id != submission.session_id
        || run.state != AiRunState::WaitingProvider.as_str()
        || run.attempt_id != Some(submission.attempt_id)
        || run.lease_generation != submission.lease_generation
        || run.lease_owner.is_some()
        || run.lease_expires_at.is_some()
        || run.lease_heartbeat_at.is_some()
        || run.next_attempt_at.is_some()
        || run.error_code.is_some()
        || run.latest_checkpoint_id.is_some()
        || run.row_version < 0
        || session.id != submission.session_id
        || session.state != "active"
        || session.deleted_at.is_some()
        || session.owner_principal_kind != expected_principal_kind
        || session.owner_subject != principal_reference.subject
        || session.scope_kind != budget.scope_kind
        || session.scope_id != budget.scope_id
        || session.tenant_id != budget.tenant_id
        || principal_reference
            .tenant_id
            .as_ref()
            .is_some_and(|tenant_id| session.tenant_id.as_ref() != Some(tenant_id))
        || attempt.id != submission.attempt_id
        || attempt.run_id != submission.run_id
        || attempt.lease_generation != submission.lease_generation
        || validate_worker_id(&attempt.worker_id).is_err()
        || attempt.claimed_at <= 0
        || attempt.finished_at.is_some()
        || attempt.provider_response_id.is_some()
        || attempt.outcome_code.is_some()
        || !outcomes.is_empty()
        || budget.id != submission.budget_reservation_id
        || budget.session_id != submission.session_id
        || budget.run_id != submission.run_id
        || budget.attempt_id != submission.attempt_id
        || budget.lease_generation != submission.lease_generation
        || budget.scope_kind != session.scope_kind
        || budget.scope_id != session.scope_id
        || budget.tenant_id != session.tenant_id
        || budget.principal_kind != session.owner_principal_kind
        || budget.principal_subject != session.owner_subject
        || budget.provider_kind != "openai"
        || budget.provider_model != submission.provider_model
        || budget.reserved_input_tokens < 0
        || budget.reserved_output_tokens < submission.maximum_output_tokens
        || budget.reserved_tool_units != 0
        || budget.reserved_image_units != 0
        || budget.reserved_cost_microunits < 0
        || budget.reserved_runs != 1
        || budget.state != "uncertain"
        || budget.reconciled_at.is_none()
        || budget.actual_input_tokens.is_some()
        || budget.actual_cached_input_tokens.is_some()
        || budget.actual_output_tokens.is_some()
        || budget.actual_tool_units.is_some()
        || budget.actual_image_units.is_some()
        || budget.actual_cost_microunits.is_some()
        || budget.actual_runs.is_some()
        || egress.id != submission.egress_decision_id
        || egress.run_id != Some(submission.run_id)
        || egress.principal_subject != principal_reference.subject
        || egress.scope_kind != session.scope_kind
        || egress.scope_id != session.scope_id
        || egress.manifest_hash != submission.egress_manifest_hash
        || egress.destination.trim().is_empty()
        || egress.capability != "model_inference"
        || egress.outcome != "allow"
        || egress.policy_version.trim().is_empty()
        || egress.estimated_bytes < 0
        || egress.estimated_tokens < 0
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    Ok(BackgroundClaimGraph {
        principal_reference,
        scope_kind: session.scope_kind,
        scope_id: session.scope_id,
        tenant_id: session.tenant_id,
    })
}

fn validate_claimable_submission(
    submission: &AiProviderBackgroundSubmissionRecord,
    now: OffsetDateTime,
    maximum_retries: u32,
) -> Result<(), OrmPublicError> {
    let state = BackgroundSubmissionState::from_persisted(&submission.state)
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let next_attempt_at = submission
        .reconciliation_next_attempt_at
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let deadline = submission
        .reconciliation_deadline
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let submitted_at = submission
        .submitted_at
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let provider_response_id = submission
        .provider_response_id
        .as_deref()
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let provider_status = submission
        .provider_status
        .as_deref()
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let provider_store = submission
        .provider_store
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let provider_created_at = submission
        .provider_created_at
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let maximum_window = if provider_store {
        MAXIMUM_STORED_RESPONSE_WINDOW
    } else {
        MAXIMUM_TEMPORARY_RESPONSE_WINDOW
    };
    let maximum_deadline = provider_created_at
        .min(submitted_at)
        .checked_add(maximum_window.whole_seconds())
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let (submission_key, submission_id) = submission_identity_from_parts(
        submission.run_id,
        submission.attempt_id,
        submission.lease_generation,
        &submission.provider_profile_id,
        &submission.provider_model,
        &submission.request_hash,
        submission.budget_reservation_id,
        &submission.egress_manifest_hash,
    );
    if submission.id != submission_id
        || submission.submission_key != submission_key
        || submission.provider_kind != "openai"
        || submission.provider_profile_id.trim().is_empty()
        || submission.provider_profile_id.len() > 200
        || submission.provider_model.trim().is_empty()
        || submission.provider_model.len() > 1_024
        || submission.maximum_output_tokens <= 0
        || !valid_sha256_hex(&submission.request_hash)
        || !valid_sha256_hex(&submission.egress_manifest_hash)
        || !valid_response_id(provider_response_id)
        || !valid_provider_status(provider_status)
        || submission.created_at <= 0
        || provider_created_at <= 0
        || submitted_at <= 0
        || submitted_at < submission.created_at
        || next_attempt_at < submitted_at
        || next_attempt_at > now.unix_timestamp()
        || deadline <= now.unix_timestamp()
        || deadline <= submitted_at
        || deadline > maximum_deadline
        || submission.safe_error_code.is_some()
        || submission.reconciliation_generation < 0
        || submission.reconciliation_retry_count < 0
        || submission.reconciliation_retry_count > submission.reconciliation_generation
        || u32::try_from(submission.reconciliation_retry_count)
            .map_or(true, |count| count > maximum_retries)
        || submission.reconciled_at.is_some()
        || submission.retrieval_egress_decision_id.is_some()
        || submission.terminal_message_id.is_some()
        || submission.row_version < 1
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    match state {
        BackgroundSubmissionState::WaitingProvider
            if submission.reconciliation_owner.is_none()
                && submission.reconciliation_lease_expires_at.is_none()
                && ((submission.reconciliation_generation == 0
                    && submission.reconciliation_retry_count == 0)
                    || (submission.reconciliation_generation > 0
                        && submission.reconciliation_retry_count > 0)) =>
        {
            Ok(())
        }
        BackgroundSubmissionState::Reconciling
            if submission.reconciliation_generation > 0
                && submission
                    .reconciliation_owner
                    .as_deref()
                    .is_some_and(|owner| validate_worker_id(owner).is_ok())
                && submission.reconciliation_lease_expires_at == Some(next_attempt_at)
                && next_attempt_at <= now.unix_timestamp() =>
        {
            Ok(())
        }
        BackgroundSubmissionState::Prepared
        | BackgroundSubmissionState::WaitingProvider
        | BackgroundSubmissionState::Reconciling
        | BackgroundSubmissionState::Completed
        | BackgroundSubmissionState::Failed
        | BackgroundSubmissionState::Cancelled
        | BackgroundSubmissionState::RecoveryRequired => {
            Err(OrmPublicError::new(OrmErrorCode::InternalError))
        }
    }
}

fn validate_active_reconciliation_record(
    submission: &AiProviderBackgroundSubmissionRecord,
    worker_id: &str,
    generation: i64,
    now: OffsetDateTime,
) -> Result<(), OrmPublicError> {
    let expiry = submission
        .reconciliation_lease_expires_at
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    if BackgroundSubmissionState::from_persisted(&submission.state)
        != Some(BackgroundSubmissionState::Reconciling)
        || submission.reconciliation_owner.as_deref() != Some(worker_id)
        || submission.reconciliation_generation != generation
        || submission.reconciliation_next_attempt_at != Some(expiry)
        || expiry <= now.unix_timestamp()
        || submission
            .reconciliation_deadline
            .is_none_or(|deadline| expiry > deadline)
        || submission.safe_error_code.is_some()
        || submission.reconciled_at.is_some()
        || submission.retrieval_egress_decision_id.is_some()
        || submission.terminal_message_id.is_some()
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    Ok(())
}

fn validate_current_reconciliation_claim(
    submission: &AiProviderBackgroundSubmissionRecord,
    claim: &AiOpenAiBackgroundReconciliationClaim,
    now: OffsetDateTime,
) -> Result<(), OrmPublicError> {
    if submission.id != claim.submission_id
        || submission.session_id != claim.session_id.0
        || submission.run_id != claim.run_id.0
        || submission.attempt_id != claim.attempt_id
        || submission.lease_generation != claim.original_lease_generation
        || submission.provider_profile_id != claim.provider_profile_id
        || submission.provider_model != claim.provider_model
        || u64::try_from(submission.maximum_output_tokens).ok() != Some(claim.maximum_output_tokens)
        || submission.provider_store != Some(claim.provider_store)
        || submission.provider_response_id.as_deref() != Some(claim.provider_response_id.as_str())
        || submission.provider_created_at != Some(claim.provider_created_at)
        || submission.request_hash != claim.request_hash
        || submission.budget_reservation_id != claim.budget_reservation_id.0
        || submission.egress_decision_id != claim.original_egress_decision_id
        || submission.egress_manifest_hash != claim.original_egress_manifest_hash
        || submission.reconciliation_owner.as_deref() != Some(&claim.worker_id)
        || submission.reconciliation_generation != claim.reconciliation_generation
        || submission.reconciliation_lease_expires_at
            != Some(claim.reconciliation_lease_expires_at.unix_timestamp())
        || submission.reconciliation_next_attempt_at != submission.reconciliation_lease_expires_at
        || submission.reconciliation_retry_count != i64::from(claim.retry_count)
        || submission.reconciliation_deadline
            != Some(claim.reconciliation_deadline.unix_timestamp())
        || submission.row_version != claim.row_version
        || claim.reconciliation_lease_expires_at <= now
        || claim.reconciliation_deadline <= now
    {
        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
    }
    validate_active_reconciliation_record(
        submission,
        &claim.worker_id,
        claim.reconciliation_generation,
        now,
    )
}

fn validate_released_reconciliation_record(
    submission: &AiProviderBackgroundSubmissionRecord,
    generation: i64,
    retry_count: i64,
    eligible_at: OffsetDateTime,
) -> Result<(), OrmPublicError> {
    if BackgroundSubmissionState::from_persisted(&submission.state)
        != Some(BackgroundSubmissionState::WaitingProvider)
        || submission.reconciliation_owner.is_some()
        || submission.reconciliation_generation != generation
        || submission.reconciliation_lease_expires_at.is_some()
        || submission.reconciliation_next_attempt_at != Some(eligible_at.unix_timestamp())
        || submission.reconciliation_retry_count != retry_count
        || submission
            .reconciliation_deadline
            .is_none_or(|deadline| eligible_at.unix_timestamp() >= deadline)
        || submission.safe_error_code.is_some()
        || submission.reconciled_at.is_some()
        || submission.retrieval_egress_decision_id.is_some()
        || submission.terminal_message_id.is_some()
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    Ok(())
}

fn claim_from_records(
    submission: &AiProviderBackgroundSubmissionRecord,
    graph: BackgroundClaimGraph,
) -> Result<AiOpenAiBackgroundReconciliationClaim, AiError> {
    Ok(AiOpenAiBackgroundReconciliationClaim {
        submission_id: submission.id,
        session_id: crate::AiSessionId(submission.session_id),
        run_id: AiRunId(submission.run_id),
        attempt_id: submission.attempt_id,
        original_lease_generation: submission.lease_generation,
        principal_reference: graph.principal_reference,
        worker_id: submission
            .reconciliation_owner
            .clone()
            .ok_or(AiError::PersistenceFailed)?,
        reconciliation_generation: submission.reconciliation_generation,
        reconciliation_lease_expires_at: submission
            .reconciliation_lease_expires_at
            .and_then(|value| OffsetDateTime::from_unix_timestamp(value).ok())
            .ok_or(AiError::PersistenceFailed)?,
        row_version: submission.row_version,
        retry_count: u32::try_from(submission.reconciliation_retry_count)
            .map_err(|_| AiError::PersistenceFailed)?,
        reconciliation_deadline: submission
            .reconciliation_deadline
            .and_then(|value| OffsetDateTime::from_unix_timestamp(value).ok())
            .ok_or(AiError::PersistenceFailed)?,
        provider_profile_id: submission.provider_profile_id.clone(),
        provider_model: submission.provider_model.clone(),
        maximum_output_tokens: u64::try_from(submission.maximum_output_tokens)
            .map_err(|_| AiError::PersistenceFailed)?,
        provider_store: submission
            .provider_store
            .ok_or(AiError::PersistenceFailed)?,
        provider_response_id: submission
            .provider_response_id
            .clone()
            .ok_or(AiError::PersistenceFailed)?,
        provider_created_at: submission
            .provider_created_at
            .ok_or(AiError::PersistenceFailed)?,
        request_hash: submission.request_hash.clone(),
        budget_reservation_id: AiBudgetReservationId(submission.budget_reservation_id),
        original_egress_decision_id: submission.egress_decision_id,
        original_egress_manifest_hash: submission.egress_manifest_hash.clone(),
        scope_kind: graph.scope_kind,
        scope_id: graph.scope_id,
        tenant_id: graph.tenant_id,
    })
}

fn exact_background_state(state: &str) -> AiProviderBackgroundSubmissionRecordWhereInput {
    AiProviderBackgroundSubmissionRecordWhereInput {
        state: Some(StringFilter {
            eq: Some(state.to_owned()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn valid_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Clone)]
struct PreparedBackgroundSubmission {
    submission_id: Uuid,
    submission_key: String,
    provider_profile_id: String,
    provider_model: String,
    maximum_output_tokens: u64,
    request_hash: String,
    budget_reservation_id: AiBudgetReservationId,
    egress_decision_id: Uuid,
    egress_manifest_hash: String,
    scope_kind: String,
    scope_id: String,
    tenant_id: Option<String>,
}

fn validate_plan(lease: &AiRunLease, plan: &AiProviderCallPlan) -> Result<(), AiError> {
    let request = plan.request_ref();
    let budget = plan.budget_request();
    let manifests = plan.transfers();
    if lease.state() != AiRunState::Running
        || plan.provider_kind_ref() != &ProviderKind::OpenAi
        || budget.session_id != lease.session_id()
        || budget.run_id != lease.run_id()
        || budget.attempt_id != lease.attempt_id()
        || budget.lease_generation != lease.lease_generation()
        || request.continuation_mode != ModelContinuationMode::ProviderRetained
        || request.continuation.is_some()
        || !request.tools.is_empty()
        || !request.builtin_tools.is_empty()
        || request.maximum_output_tokens.is_none()
        || request
            .input
            .iter()
            .any(|block| matches!(block, ModelInputBlock::Attachment { .. }))
        || manifests.len() != 1
        || manifests[0].provider_kind != ProviderKind::OpenAi.as_str()
        || manifests[0].model != request.model
        || manifests[0].capability != AiEgressCapability::ModelInference
        || manifests[0].retention != AI_EGRESS_RETENTION_PROVIDER_RESPONSE
        || manifests[0].provider_profile_id.trim().is_empty()
        || manifests[0].provider_profile_id.len() > 200
    {
        return Err(AiError::Conflict);
    }
    Ok(())
}

async fn require_access(
    runtime: &AiRuntime,
    principal: &agql_auth::ResolvedPrincipal,
    lease: &AiRunLease,
    budget: &crate::AiBudgetReservationRequest,
) -> Result<(), AiError> {
    if !runtime
        .access_policy()
        .can_access_scope(principal.principal(), &budget.scope, AiSessionAction::Write)
        .await
        .is_allowed()
        || !runtime
            .access_policy()
            .can_access_session(
                principal.principal(),
                lease.session_id(),
                AiSessionAction::Write,
            )
            .await
            .is_allowed()
    {
        return Err(AiError::Forbidden);
    }
    Ok(())
}

fn request_hash(request: &crate::ModelRequest) -> Result<String, AiError> {
    let encoded = serde_json::to_vec(request).map_err(|_| AiError::PersistenceFailed)?;
    Ok(hex::encode(Sha256::digest(encoded)))
}

fn submission_identity(
    lease: &AiRunLease,
    provider_profile_id: &str,
    provider_model: &str,
    request_hash: &str,
    budget_reservation_id: AiBudgetReservationId,
    egress_manifest_hash: &str,
) -> (String, Uuid) {
    submission_identity_from_parts(
        lease.run_id().0,
        lease.attempt_id(),
        lease.lease_generation(),
        provider_profile_id,
        provider_model,
        request_hash,
        budget_reservation_id.0,
        egress_manifest_hash,
    )
}

#[allow(clippy::too_many_arguments)]
fn submission_identity_from_parts(
    run_id: Uuid,
    attempt_id: Uuid,
    lease_generation: i64,
    provider_profile_id: &str,
    provider_model: &str,
    request_hash: &str,
    budget_reservation_id: Uuid,
    egress_manifest_hash: &str,
) -> (String, Uuid) {
    let mut hasher = Sha256::new();
    hasher.update(b"graphql-orm-ai/provider-background-submission/v1\0");
    hasher.update(run_id.as_bytes());
    hasher.update(attempt_id.as_bytes());
    hasher.update(lease_generation.to_be_bytes());
    hasher.update(provider_profile_id.as_bytes());
    hasher.update([0]);
    hasher.update(provider_model.as_bytes());
    hasher.update([0]);
    hasher.update(request_hash.as_bytes());
    hasher.update(budget_reservation_id.as_bytes());
    hasher.update(egress_manifest_hash.as_bytes());
    let digest = hasher.finalize();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest[..16]);
    (hex::encode(digest), Uuid::from_bytes(id))
}

fn submission_matches(
    submission: &AiProviderBackgroundSubmissionRecord,
    lease: &AiRunLease,
    prepared: &PreparedBackgroundSubmission,
) -> bool {
    submission.id == prepared.submission_id
        && submission.submission_key == prepared.submission_key
        && submission.session_id == lease.session_id().0
        && submission.run_id == lease.run_id().0
        && submission.attempt_id == lease.attempt_id()
        && submission.lease_generation == lease.lease_generation()
        && submission.provider_kind == "openai"
        && submission.provider_profile_id == prepared.provider_profile_id
        && submission.provider_model == prepared.provider_model
        && u64::try_from(submission.maximum_output_tokens).ok()
            == Some(prepared.maximum_output_tokens)
        && submission.request_hash == prepared.request_hash
        && submission.budget_reservation_id == prepared.budget_reservation_id.0
        && submission.egress_decision_id == prepared.egress_decision_id
        && submission.egress_manifest_hash == prepared.egress_manifest_hash
}

fn reconciliation_fields_are_empty(submission: &AiProviderBackgroundSubmissionRecord) -> bool {
    submission.reconciliation_owner.is_none()
        && submission.reconciliation_generation == 0
        && submission.reconciliation_lease_expires_at.is_none()
        && submission.reconciliation_next_attempt_at.is_none()
        && submission.reconciliation_retry_count == 0
        && submission.reconciliation_deadline.is_none()
        && submission.reconciled_at.is_none()
        && submission.retrieval_egress_decision_id.is_none()
        && submission.terminal_message_id.is_none()
}

fn valid_waiting_submission(submission: &AiProviderBackgroundSubmissionRecord) -> bool {
    let Some(submitted_at) = submission.submitted_at else {
        return false;
    };
    let Some(next_attempt_at) = submission.reconciliation_next_attempt_at else {
        return false;
    };
    let Some(deadline) = submission.reconciliation_deadline else {
        return false;
    };
    BackgroundSubmissionState::from_persisted(&submission.state)
        == Some(BackgroundSubmissionState::WaitingProvider)
        && submission.provider_store.is_some()
        && submission.provider_response_id.is_some()
        && submission.provider_status.is_some()
        && submission.provider_created_at.is_some()
        && submission.safe_error_code.is_none()
        && submission.reconciliation_owner.is_none()
        && submission.reconciliation_generation == 0
        && submission.reconciliation_lease_expires_at.is_none()
        && submission.reconciliation_retry_count == 0
        && submitted_at <= next_attempt_at
        && next_attempt_at < deadline
        && submission.reconciled_at.is_none()
        && submission.retrieval_egress_decision_id.is_none()
        && submission.terminal_message_id.is_none()
}

fn valid_response_id(value: &str) -> bool {
    value.starts_with("resp_")
        && (6..=200).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn valid_provider_status(value: &str) -> bool {
    matches!(
        value,
        "queued" | "in_progress" | "completed" | "failed" | "incomplete" | "cancelled"
    )
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
