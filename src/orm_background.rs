//! Fenced OpenAI background-response submission and durable binding.

#![cfg(all(
    any(feature = "sqlite", feature = "postgres"),
    feature = "provider-openai"
))]

use std::sync::Arc;

use agql_auth::Clock;
use graphql_orm::graphql::errors::{OrmErrorCode, OrmPublicError};
use graphql_orm::graphql::filters::UuidFilter;
use graphql_orm::graphql::orm::{ConditionalUpdateOutcome, TransactionError, TransactionMode};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::orm_runs::{
    append_attempt_outcome, canonical_second, exact_state, lease_from_record,
    load_and_validate_active_lease,
};
use crate::persistence::*;
use crate::{
    AI_EGRESS_RETENTION_PROVIDER_RESPONSE, AiBudgetReconciliation, AiBudgetReconciliationOutcome,
    AiBudgetReservationId, AiBudgetService, AiEgressCapability, AiEgressDecisionAudit, AiError,
    AiProviderCallPlan, AiRunId, AiRunLease, AiRunState, AiRuntime, AiSessionAction,
    ModelContinuationMode, ModelInputBlock, OrmAiRunService, ProviderBackgroundBinding,
    ProviderBackgroundSubmission, ProviderKind, ProviderRequestContext,
};

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
        }
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
                            state: "prepared".to_owned(),
                            safe_error_code: None,
                            provider_created_at: None,
                            submitted_at: None,
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
                        || submission.state != "prepared"
                        || submission.provider_response_id.is_some()
                        || submission.provider_status.is_some()
                        || submission.provider_store.is_some()
                        || submission.provider_created_at.is_some()
                        || submission.submitted_at.is_some()
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
                                state: Some("waiting_provider".to_owned()),
                                provider_created_at: Some(Some(
                                    acknowledgement_for_tx.created_at(),
                                )),
                                submitted_at: Some(Some(now.unix_timestamp())),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(submission_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
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
                        || submission.state != "prepared"
                        || submission.provider_response_id.is_some()
                        || submission.provider_status.is_some()
                        || submission.provider_store.is_some()
                        || submission.provider_created_at.is_some()
                        || submission.submitted_at.is_some()
                        || submission.safe_error_code.is_some()
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
                                state: Some("recovery_required".to_owned()),
                                safe_error_code: Some(Some(safe_error_code.to_owned())),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(submission_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
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
    let mut hasher = Sha256::new();
    hasher.update(b"graphql-orm-ai/provider-background-submission/v1\0");
    hasher.update(lease.run_id().0.as_bytes());
    hasher.update(lease.attempt_id().as_bytes());
    hasher.update(lease.lease_generation().to_be_bytes());
    hasher.update(provider_profile_id.as_bytes());
    hasher.update([0]);
    hasher.update(provider_model.as_bytes());
    hasher.update([0]);
    hasher.update(request_hash.as_bytes());
    hasher.update(budget_reservation_id.0.as_bytes());
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
