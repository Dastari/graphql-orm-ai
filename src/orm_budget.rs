//! ORM-backed atomic budget reservations and reconciliation.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::collections::BTreeSet;
use std::sync::Arc;

use agql_auth::{AuthPrincipal, Clock, PrincipalReference, ResolvedPrincipal};
use async_trait::async_trait;
use graphql_orm::db::Database;
use graphql_orm::graphql::errors::{OrmErrorCode, OrmPublicError};
use graphql_orm::graphql::filters::{BoolFilter, StringFilter, UuidFilter};
use graphql_orm::graphql::orm::{
    ConditionalUpdateOutcome, DefaultWriteBackend, TransactionError, TransactionMode,
};
use time::{Date, Duration, Month, OffsetDateTime};
use uuid::Uuid;

use crate::persistence::*;
use crate::{
    AiBudgetAmounts, AiBudgetReconciliation, AiBudgetReconciliationOutcome,
    AiBudgetReconciliationResult, AiBudgetReservation, AiBudgetReservationId,
    AiBudgetReservationRequest, AiBudgetReservationState, AiBudgetService, AiError, AiRunId,
    AiScope, ProviderKind,
};

/// Deployment-owned hard limits for the durable budget service.
///
/// GraphQL-managed policies may only narrow these values. They cannot increase
/// the maximum size or lifetime of one provider reservation, accept an old
/// principal resolution, or cause an unbounded policy query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiBudgetServiceLimits {
    maximum_per_call: AiBudgetAmounts,
    maximum_reservation_lifetime: Duration,
    maximum_principal_age: Duration,
    maximum_applicable_policies: usize,
    maximum_transaction_retries: usize,
}

impl AiBudgetServiceLimits {
    /// Creates validated deployment hard limits.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] when calls cannot reserve one
    /// run, durations are not positive, the policy bound is outside `1..=256`,
    /// or more than 16 serialization retries are requested.
    pub fn new(
        maximum_per_call: AiBudgetAmounts,
        maximum_reservation_lifetime: Duration,
        maximum_principal_age: Duration,
        maximum_applicable_policies: usize,
        maximum_transaction_retries: usize,
    ) -> Result<Self, AiError> {
        if maximum_per_call.runs == 0
            || !maximum_reservation_lifetime.is_positive()
            || !maximum_principal_age.is_positive()
            || !(1..=256).contains(&maximum_applicable_policies)
            || maximum_transaction_retries > 16
        {
            return Err(AiError::InvalidConfiguration(
                "invalid deployment budget service limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_per_call,
            maximum_reservation_lifetime,
            maximum_principal_age,
            maximum_applicable_policies,
            maximum_transaction_retries,
        })
    }

    /// Returns the deployment maximum for one provider call.
    pub const fn maximum_per_call(&self) -> AiBudgetAmounts {
        self.maximum_per_call
    }

    /// Returns the maximum number of simultaneously applicable policies.
    pub const fn maximum_applicable_policies(&self) -> usize {
        self.maximum_applicable_policies
    }
}

/// Durable budget service implemented only through generated `graphql-orm`
/// repositories and state-machine transactions.
///
/// A reservation is created only after the current run fence, safe principal
/// reference, every applicable counter, idempotency binding, deployment hard
/// limit, and policy ceiling have been checked in one transaction.
#[derive(Clone)]
pub struct OrmAiBudgetService {
    database: Database<DefaultWriteBackend>,
    clock: Arc<dyn Clock>,
    limits: AiBudgetServiceLimits,
}

impl OrmAiBudgetService {
    /// Creates an ORM-backed budget service with deployment hard limits.
    pub fn new(
        database: Database<DefaultWriteBackend>,
        clock: Arc<dyn Clock>,
        limits: AiBudgetServiceLimits,
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

    async fn reserve_once(
        &self,
        principal: ResolvedPrincipal,
        request: AiBudgetReservationRequest,
        now: OffsetDateTime,
    ) -> Result<Result<AiBudgetReservation, AiError>, TransactionError> {
        let limits = self.limits;
        let (principal_kind, principal_subject) = principal_identity(principal.principal());
        let principal_subject = principal_subject.to_owned();
        let principal_reference = principal.reference().clone();
        let canonical_expiry = canonical_second(request.expires_at);
        let scope = request.scope.clone();
        let mut policy_scope_keys = vec![crate::ai_scope_key(&scope)];
        if scope.tenant_id.is_some() {
            let mut tenant_wildcard_scope = scope.clone();
            tenant_wildcard_scope.tenant_id = None;
            policy_scope_keys.push(crate::ai_scope_key(&tenant_wildcard_scope));
        }
        let idempotency_key = request.idempotency_key.clone();
        let database = self.database.clone();

        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let run = tx
                        .find_by_id::<AiRunRecord>(&request.run_id.0)
                        .await
                        .map_err(OrmPublicError::from)?;
                    let Some(run) = run else {
                        return Ok(Err(AiError::Conflict));
                    };
                    if let Err(error) = validate_run_fence(
                        &run,
                        &principal_reference,
                        &request,
                        now,
                        canonical_expiry,
                    ) {
                        return Ok(Err(error));
                    }
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&request.session_id.0)
                        .await
                        .map_err(OrmPublicError::from)?;
                    let Some(session) = session else {
                        return Ok(Err(AiError::Conflict));
                    };
                    if let Err(error) =
                        validate_budget_session(&session, &principal_reference, &request.scope)
                    {
                        return Ok(Err(error));
                    }

                    let existing = tx
                        .query::<AiBudgetReservationRecord>()
                        .filter(AiBudgetReservationRecordWhereInput {
                            principal_kind: Some(StringFilter {
                                eq: Some(principal_kind.clone()),
                                ..Default::default()
                            }),
                            principal_subject: Some(StringFilter {
                                eq: Some(principal_subject.clone()),
                                ..Default::default()
                            }),
                            idempotency_key: Some(StringFilter {
                                eq: Some(idempotency_key),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if existing.len() > 1 {
                        return Ok(Err(AiError::PersistenceFailed));
                    }
                    if let Some(existing) = existing.into_iter().next() {
                        if !reservation_matches_request(
                            &existing,
                            &principal_kind,
                            &principal_subject,
                            &request,
                            canonical_expiry,
                        ) || existing.state != "reserved"
                        {
                            return Ok(Err(AiError::Conflict));
                        }
                        return Ok(record_to_reservation(&existing));
                    }

                    let policies = tx
                        .query::<AiBudgetPolicyRecord>()
                        .filter(AiBudgetPolicyRecordWhereInput {
                            scope_key: Some(StringFilter {
                                in_list: Some(policy_scope_keys),
                                ..Default::default()
                            }),
                            enabled: Some(BoolFilter {
                                eq: Some(true),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit((limits.maximum_applicable_policies + 1) as i64)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if policies.len() > limits.maximum_applicable_policies {
                        return Ok(Err(AiError::InvalidConfiguration(
                            "too many AI budget policies for one scope".to_owned(),
                        )));
                    }
                    if policies
                        .iter()
                        .any(|policy| !policy_scope_integrity(policy, &scope))
                    {
                        return Ok(Err(AiError::InvalidConfiguration(
                            "invalid AI budget policy scope binding".to_owned(),
                        )));
                    }
                    let policies = policies
                        .into_iter()
                        .filter(|policy| {
                            policy_applies(policy, &scope, &principal_kind, &principal_subject)
                        })
                        .collect::<Vec<_>>();
                    if policies.is_empty() {
                        return Ok(Err(AiError::BudgetDenied));
                    }
                    if policies.len() > limits.maximum_applicable_policies {
                        return Ok(Err(AiError::InvalidConfiguration(
                            "too many applicable AI budget policies".to_owned(),
                        )));
                    }

                    let mut plans = Vec::with_capacity(policies.len());
                    for policy in policies {
                        let period = match budget_period(&policy.interval_kind, now) {
                            Ok(period) => period,
                            Err(error) => return Ok(Err(error)),
                        };
                        let counters = tx
                            .query::<AiBudgetCounterRecord>()
                            .filter(AiBudgetCounterRecordWhereInput {
                                budget_policy_id: Some(UuidFilter {
                                    eq: Some(policy.id),
                                    ..Default::default()
                                }),
                                period_key: Some(StringFilter {
                                    eq: Some(period.key.clone()),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .limit(2)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        if counters.len() > 1 {
                            return Ok(Err(AiError::PersistenceFailed));
                        }
                        let existing = counters.into_iter().next();
                        let reserved = match existing.as_ref().map(counter_reserved).transpose() {
                            Ok(value) => value.unwrap_or_default(),
                            Err(error) => return Ok(Err(error)),
                        };
                        let committed = match existing.as_ref().map(counter_committed).transpose() {
                            Ok(value) => value.unwrap_or_default(),
                            Err(error) => return Ok(Err(error)),
                        };
                        let next_reserved = match checked_add(reserved, request.estimate) {
                            Ok(value) => value,
                            Err(error) => return Ok(Err(error)),
                        };
                        if let Err(error) =
                            validate_policy_capacity(&policy, next_reserved, committed)
                        {
                            return Ok(Err(error));
                        }
                        plans.push(CounterPlan {
                            policy,
                            period,
                            existing,
                            next_reserved,
                            committed,
                        });
                    }

                    let mut counter_ids = Vec::with_capacity(plans.len());
                    for plan in plans {
                        let updated = if let Some(existing) = plan.existing {
                            let update = counter_update(
                                plan.policy.row_version,
                                &plan.period,
                                plan.next_reserved,
                                plan.committed,
                            )?;
                            match tx
                                .compare_and_swap::<AiBudgetCounterRecord>(
                                    &existing.id,
                                    existing.row_version,
                                    AiBudgetCounterRecordWhereInput::default(),
                                    update,
                                )
                                .await
                                .map_err(OrmPublicError::from)?
                            {
                                ConditionalUpdateOutcome::Updated(record) => record,
                                ConditionalUpdateOutcome::NotFound
                                | ConditionalUpdateOutcome::Conflict => {
                                    return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                                }
                            }
                        } else {
                            tx.insert::<AiBudgetCounterRecord>(counter_create(
                                plan.policy.id,
                                plan.policy.row_version,
                                &plan.period,
                                plan.next_reserved,
                            )?)
                            .await
                            .map_err(OrmPublicError::from)?
                        };
                        counter_ids.push(updated.id);
                    }

                    let budget_counter_ids = serde_json::to_value(counter_ids)
                        .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let record = tx
                        .insert::<AiBudgetReservationRecord>(CreateAiBudgetReservationRecordInput {
                            budget_counter_ids,
                            scope_kind: request.scope.kind,
                            scope_id: request.scope.id,
                            tenant_id: request.scope.tenant_id,
                            principal_kind,
                            principal_subject,
                            session_id: request.session_id.0,
                            run_id: request.run_id.0,
                            attempt_id: request.attempt_id,
                            lease_generation: request.lease_generation,
                            provider_kind: request.provider_kind.as_str().to_owned(),
                            provider_model: request.model,
                            pricing_policy_version: request.pricing_policy_version,
                            reserved_input_tokens: amount_to_i64(request.estimate.input_tokens)?,
                            reserved_output_tokens: amount_to_i64(request.estimate.output_tokens)?,
                            reserved_tool_units: amount_to_i64(request.estimate.tool_units)?,
                            reserved_image_units: amount_to_i64(request.estimate.image_units)?,
                            reserved_cost_microunits: amount_to_i64(
                                request.estimate.cost_microunits,
                            )?,
                            reserved_runs: amount_to_i64(request.estimate.runs)?,
                            actual_input_tokens: None,
                            actual_cached_input_tokens: None,
                            actual_output_tokens: None,
                            actual_tool_units: None,
                            actual_image_units: None,
                            actual_cost_microunits: None,
                            actual_runs: None,
                            idempotency_key: request.idempotency_key,
                            state: "reserved".to_owned(),
                            expires_at: canonical_expiry.unix_timestamp(),
                            reconciled_at: None,
                        })
                        .await
                        .map_err(OrmPublicError::from)?;
                    Ok(record_to_reservation(&record))
                })
            })
            .await
    }

    async fn reconcile_once(
        &self,
        principal: ResolvedPrincipal,
        reconciliation: AiBudgetReconciliation,
        now: OffsetDateTime,
    ) -> Result<Result<AiBudgetReconciliationResult, AiError>, TransactionError> {
        let (principal_kind, principal_subject) = principal_identity(principal.principal());
        let principal_subject = principal_subject.to_owned();
        let principal_reference = principal.reference().clone();
        let database = self.database.clone();

        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let record = tx
                        .find_by_id::<AiBudgetReservationRecord>(&reconciliation.reservation_id.0)
                        .await
                        .map_err(OrmPublicError::from)?;
                    let Some(record) = record else {
                        return Ok(Err(AiError::NotFound));
                    };
                    if record.principal_kind != principal_kind
                        || record.principal_subject != principal_subject
                    {
                        return Ok(Err(AiError::NotFound));
                    }
                    let state = match parse_reservation_state(&record.state) {
                        Ok(state) => state,
                        Err(error) => return Ok(Err(error)),
                    };
                    if let Some(result) =
                        match_existing_reconciliation(&record, state, &reconciliation)
                    {
                        return Ok(result);
                    }
                    if !matches!(
                        state,
                        AiBudgetReservationState::Reserved | AiBudgetReservationState::Uncertain
                    ) {
                        return Ok(Err(AiError::Conflict));
                    }
                    if state == AiBudgetReservationState::Uncertain
                        && reconciliation.outcome == AiBudgetReconciliationOutcome::ReleaseUnused
                    {
                        return Ok(Err(AiError::Conflict));
                    }

                    let run = tx
                        .find_by_id::<AiRunRecord>(&record.run_id)
                        .await
                        .map_err(OrmPublicError::from)?;
                    let Some(run) = run else {
                        return Ok(Err(AiError::Conflict));
                    };
                    if let Err(error) = validate_reconciliation_fence(
                        &run,
                        &record,
                        &principal_reference,
                        &reconciliation,
                        now,
                    ) {
                        return Ok(Err(error));
                    }

                    let reserved = match reservation_amounts(&record) {
                        Ok(value) => value,
                        Err(error) => return Ok(Err(error)),
                    };
                    let actual = match validate_reconciliation_actual(
                        reserved,
                        reconciliation.actual,
                        reconciliation.cached_input_tokens,
                        reconciliation.outcome,
                    ) {
                        Ok(value) => value,
                        Err(error) => return Ok(Err(error)),
                    };
                    let counter_ids = match reservation_counter_ids(&record) {
                        Ok(value) => value,
                        Err(error) => return Ok(Err(error)),
                    };

                    let mut counter_plans = Vec::with_capacity(counter_ids.len());
                    if reconciliation.outcome != AiBudgetReconciliationOutcome::MarkUncertain {
                        for counter_id in counter_ids {
                            let counter = tx
                                .find_by_id::<AiBudgetCounterRecord>(&counter_id)
                                .await
                                .map_err(OrmPublicError::from)?;
                            let Some(counter) = counter else {
                                return Ok(Err(AiError::PersistenceFailed));
                            };
                            let counter_reserved = match counter_reserved(&counter) {
                                Ok(value) => value,
                                Err(error) => return Ok(Err(error)),
                            };
                            let counter_committed = match counter_committed(&counter) {
                                Ok(value) => value,
                                Err(error) => return Ok(Err(error)),
                            };
                            let next_reserved = match checked_sub(counter_reserved, reserved) {
                                Ok(value) => value,
                                Err(error) => return Ok(Err(error)),
                            };
                            let next_committed = if let Some(actual) = actual {
                                match checked_add(counter_committed, actual) {
                                    Ok(value) => value,
                                    Err(error) => return Ok(Err(error)),
                                }
                            } else {
                                counter_committed
                            };
                            counter_plans.push((counter, next_reserved, next_committed));
                        }
                    }

                    let now_seconds = now.unix_timestamp();
                    for (counter, next_reserved, next_committed) in counter_plans {
                        let update = counter_amount_update(next_reserved, next_committed)?;
                        match tx
                            .compare_and_swap::<AiBudgetCounterRecord>(
                                &counter.id,
                                counter.row_version,
                                AiBudgetCounterRecordWhereInput::default(),
                                update,
                            )
                            .await
                            .map_err(OrmPublicError::from)?
                        {
                            ConditionalUpdateOutcome::Updated(_) => {}
                            ConditionalUpdateOutcome::NotFound
                            | ConditionalUpdateOutcome::Conflict => {
                                return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                            }
                        }
                    }

                    let next_state = match reconciliation.outcome {
                        AiBudgetReconciliationOutcome::Commit => "committed",
                        AiBudgetReconciliationOutcome::ReleaseUnused => "released",
                        AiBudgetReconciliationOutcome::MarkUncertain => "uncertain",
                    };
                    let actual_input = actual
                        .map(|value| amount_to_i64(value.input_tokens))
                        .transpose()?;
                    let actual_cached_input = reconciliation
                        .cached_input_tokens
                        .map(amount_to_i64)
                        .transpose()?;
                    let actual_output = actual
                        .map(|value| amount_to_i64(value.output_tokens))
                        .transpose()?;
                    let actual_tools = actual
                        .map(|value| amount_to_i64(value.tool_units))
                        .transpose()?;
                    let actual_images = actual
                        .map(|value| amount_to_i64(value.image_units))
                        .transpose()?;
                    let actual_cost = actual
                        .map(|value| amount_to_i64(value.cost_microunits))
                        .transpose()?;
                    let actual_runs = actual.map(|value| amount_to_i64(value.runs)).transpose()?;
                    if let Some(actual) = actual
                        && reconciliation.outcome == AiBudgetReconciliationOutcome::Commit
                    {
                        tx.insert::<AiUsageEntryRecord>(CreateAiUsageEntryRecordInput {
                            id: Uuid::new_v4(),
                            budget_reservation_id: record.id,
                            scope_kind: record.scope_kind.clone(),
                            scope_id: record.scope_id.clone(),
                            tenant_id: record.tenant_id.clone(),
                            principal_kind: record.principal_kind.clone(),
                            principal_subject: record.principal_subject.clone(),
                            session_id: Some(record.session_id),
                            run_id: Some(record.run_id),
                            provider_kind: record.provider_kind.clone(),
                            provider_model: record.provider_model.clone(),
                            input_tokens: amount_to_i64(actual.input_tokens)?,
                            cached_input_tokens: actual_cached_input.unwrap_or_default(),
                            output_tokens: amount_to_i64(actual.output_tokens)?,
                            tool_units: amount_to_i64(actual.tool_units)?,
                            image_units: amount_to_i64(actual.image_units)?,
                            cost_microunits: Some(amount_to_i64(actual.cost_microunits)?),
                        })
                        .await
                        .map_err(OrmPublicError::from)?;
                    }
                    let updated = match tx
                        .compare_and_swap::<AiBudgetReservationRecord>(
                            &record.id,
                            record.row_version,
                            AiBudgetReservationRecordWhereInput::default(),
                            UpdateAiBudgetReservationRecordInput {
                                actual_input_tokens: Some(actual_input),
                                actual_cached_input_tokens: Some(actual_cached_input),
                                actual_output_tokens: Some(actual_output),
                                actual_tool_units: Some(actual_tools),
                                actual_image_units: Some(actual_images),
                                actual_cost_microunits: Some(actual_cost),
                                actual_runs: Some(actual_runs),
                                state: Some(next_state.to_owned()),
                                reconciled_at: Some(Some(now_seconds)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?
                    {
                        ConditionalUpdateOutcome::Updated(record) => record,
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    Ok(record_to_reconciliation_result(&updated))
                })
            })
            .await
    }

    fn validate_fresh_principal(
        &self,
        principal: &ResolvedPrincipal,
        now: OffsetDateTime,
    ) -> Result<(), AiError> {
        let resolved_at = principal.resolved_at();
        if resolved_at > now
            || now - resolved_at > self.limits.maximum_principal_age
            || principal
                .reference()
                .expires_at
                .is_some_and(|expires_at| expires_at <= now)
        {
            return Err(AiError::ReauthorizationFailed);
        }
        Ok(())
    }
}

#[async_trait]
impl AiBudgetService for OrmAiBudgetService {
    async fn reserve(
        &self,
        principal: &ResolvedPrincipal,
        request: AiBudgetReservationRequest,
    ) -> Result<AiBudgetReservation, AiError> {
        let now = self.clock.now();
        self.validate_fresh_principal(principal, now)?;
        validate_reservation_request(principal, &request, now, self.limits)?;
        for retry in 0..=self.limits.maximum_transaction_retries {
            match self
                .reserve_once(principal.clone(), request.clone(), now)
                .await
            {
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

    async fn reconcile(
        &self,
        principal: &ResolvedPrincipal,
        reconciliation: AiBudgetReconciliation,
    ) -> Result<AiBudgetReconciliationResult, AiError> {
        let now = self.clock.now();
        self.validate_fresh_principal(principal, now)?;
        if reconciliation.attempt_id.is_nil() || reconciliation.lease_generation <= 0 {
            return Err(AiError::InvalidInput(
                "invalid budget reconciliation fence".to_owned(),
            ));
        }
        for retry in 0..=self.limits.maximum_transaction_retries {
            match self
                .reconcile_once(principal.clone(), reconciliation.clone(), now)
                .await
            {
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
}

#[derive(Clone, Debug)]
struct BudgetPeriod {
    key: String,
    started_at: i64,
    ends_at: i64,
}

#[derive(Clone, Debug)]
struct CounterPlan {
    policy: AiBudgetPolicyRecord,
    period: BudgetPeriod,
    existing: Option<AiBudgetCounterRecord>,
    next_reserved: AiBudgetAmounts,
    committed: AiBudgetAmounts,
}

/// Commits authoritative terminal usage for an already uncertain background
/// reservation inside the caller's wider state-machine transaction.
///
/// The caller owns validation of the exact background submission/run/attempt
/// graph and current authorization. This helper owns the existing counter
/// arithmetic, usage insertion, and reservation CAS so those budget semantics
/// cannot diverge from ordinary provider reconciliation.
#[cfg(feature = "provider-openai")]
pub(crate) async fn commit_uncertain_background_budget(
    tx: &mut graphql_orm::graphql::orm::MutationContext<'_, DefaultWriteBackend>,
    record: &AiBudgetReservationRecord,
    actual: AiBudgetAmounts,
    cached_input_tokens: u64,
    now: OffsetDateTime,
) -> Result<AiBudgetReservationRecord, OrmPublicError> {
    if parse_reservation_state(&record.state)
        .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?
        != AiBudgetReservationState::Uncertain
        || record.reconciled_at.is_none()
    {
        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
    }
    let reserved = reservation_amounts(record)
        .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let actual = validate_reconciliation_actual(
        reserved,
        Some(actual),
        Some(cached_input_tokens),
        AiBudgetReconciliationOutcome::Commit,
    )
    .map_err(|_| OrmPublicError::new(OrmErrorCode::Conflict))?
    .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let counter_ids = reservation_counter_ids(record)
        .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
    for counter_id in counter_ids {
        let counter = tx
            .find_by_id::<AiBudgetCounterRecord>(&counter_id)
            .await
            .map_err(OrmPublicError::from)?
            .ok_or_else(OrmPublicError::not_found)?;
        let reserved_counter = counter_reserved(&counter)
            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
        let committed_counter = counter_committed(&counter)
            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
        let next_reserved = checked_sub(reserved_counter, reserved)
            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
        let next_committed = checked_add(committed_counter, actual)
            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
        let update = counter_amount_update(next_reserved, next_committed)?;
        if !matches!(
            tx.compare_and_swap::<AiBudgetCounterRecord>(
                &counter.id,
                counter.row_version,
                AiBudgetCounterRecordWhereInput::default(),
                update,
            )
            .await
            .map_err(OrmPublicError::from)?,
            ConditionalUpdateOutcome::Updated(_)
        ) {
            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
        }
    }
    tx.insert::<AiUsageEntryRecord>(CreateAiUsageEntryRecordInput {
        id: background_usage_identity(record.id),
        budget_reservation_id: record.id,
        scope_kind: record.scope_kind.clone(),
        scope_id: record.scope_id.clone(),
        tenant_id: record.tenant_id.clone(),
        principal_kind: record.principal_kind.clone(),
        principal_subject: record.principal_subject.clone(),
        session_id: Some(record.session_id),
        run_id: Some(record.run_id),
        provider_kind: record.provider_kind.clone(),
        provider_model: record.provider_model.clone(),
        input_tokens: amount_to_i64(actual.input_tokens)?,
        cached_input_tokens: amount_to_i64(cached_input_tokens)?,
        output_tokens: amount_to_i64(actual.output_tokens)?,
        tool_units: amount_to_i64(actual.tool_units)?,
        image_units: amount_to_i64(actual.image_units)?,
        cost_microunits: Some(amount_to_i64(actual.cost_microunits)?),
    })
    .await
    .map_err(OrmPublicError::from)?;
    let updated = match tx
        .compare_and_swap::<AiBudgetReservationRecord>(
            &record.id,
            record.row_version,
            AiBudgetReservationRecordWhereInput {
                state: Some(StringFilter {
                    eq: Some("uncertain".to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            UpdateAiBudgetReservationRecordInput {
                actual_input_tokens: Some(Some(amount_to_i64(actual.input_tokens)?)),
                actual_cached_input_tokens: Some(Some(amount_to_i64(cached_input_tokens)?)),
                actual_output_tokens: Some(Some(amount_to_i64(actual.output_tokens)?)),
                actual_tool_units: Some(Some(amount_to_i64(actual.tool_units)?)),
                actual_image_units: Some(Some(amount_to_i64(actual.image_units)?)),
                actual_cost_microunits: Some(Some(amount_to_i64(actual.cost_microunits)?)),
                actual_runs: Some(Some(amount_to_i64(actual.runs)?)),
                state: Some("committed".to_owned()),
                reconciled_at: Some(Some(now.unix_timestamp())),
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
    Ok(updated)
}

#[cfg(feature = "provider-openai")]
fn background_usage_identity(reservation_id: Uuid) -> Uuid {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(b"graphql-orm-ai/background-usage/v1\0");
    hasher.update(reservation_id.as_bytes());
    let digest = hasher.finalize();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(id)
}

fn validate_reservation_request(
    principal: &ResolvedPrincipal,
    request: &AiBudgetReservationRequest,
    now: OffsetDateTime,
    limits: AiBudgetServiceLimits,
) -> Result<(), AiError> {
    validate_scope(&request.scope)?;
    if request.session_id.0.is_nil()
        || request.run_id.0.is_nil()
        || request.attempt_id.is_nil()
        || request.lease_generation <= 0
        || request.model.trim().is_empty()
        || request.model.len() > 200
        || request.pricing_policy_version.trim().is_empty()
        || request.pricing_policy_version.len() > 200
        || request.idempotency_key.trim().is_empty()
        || request.idempotency_key.len() > 256
        || request.estimate.runs != 1
        || !request.estimate.fits_within(limits.maximum_per_call)
    {
        return Err(AiError::InvalidInput(
            "invalid budget reservation request".to_owned(),
        ));
    }
    let expires_at = canonical_second(request.expires_at);
    let maximum_expiry = now
        .checked_add(limits.maximum_reservation_lifetime)
        .ok_or_else(|| AiError::InvalidConfiguration("budget time overflow".to_owned()))?;
    if expires_at <= now || expires_at > maximum_expiry {
        return Err(AiError::InvalidInput(
            "invalid budget reservation expiry".to_owned(),
        ));
    }
    if let Some(tenant_id) = principal.reference().tenant_id.as_deref()
        && request.scope.tenant_id.as_deref() != Some(tenant_id)
    {
        return Err(AiError::Forbidden);
    }
    Ok(())
}

fn validate_scope(scope: &AiScope) -> Result<(), AiError> {
    if scope.kind.trim().is_empty()
        || scope.kind.len() > 128
        || scope.id.trim().is_empty()
        || scope.id.len() > 512
        || scope
            .tenant_id
            .as_ref()
            .is_some_and(|tenant| tenant.trim().is_empty() || tenant.len() > 512)
    {
        return Err(AiError::InvalidInput("invalid AI scope".to_owned()));
    }
    Ok(())
}

fn validate_run_fence(
    run: &AiRunRecord,
    principal_reference: &PrincipalReference,
    request: &AiBudgetReservationRequest,
    now: OffsetDateTime,
    canonical_expiry: OffsetDateTime,
) -> Result<(), AiError> {
    let stored_reference: PrincipalReference =
        serde_json::from_value(run.principal_reference.clone())
            .map_err(|_| AiError::PersistenceFailed)?;
    if run.session_id != request.session_id.0
        || run.id != request.run_id.0
        || stored_reference != *principal_reference
        || run.state != "running"
        || run.attempt_id != Some(request.attempt_id)
        || run.lease_generation != request.lease_generation
        || run.lease_owner.as_deref().is_none_or(str::is_empty)
        || run
            .lease_expires_at
            .is_none_or(|expires| expires <= now.unix_timestamp())
        || run
            .lease_expires_at
            .is_none_or(|expires| canonical_expiry.unix_timestamp() > expires)
    {
        return Err(AiError::Conflict);
    }
    Ok(())
}

fn validate_budget_session(
    session: &AiSessionRecord,
    principal_reference: &PrincipalReference,
    scope: &AiScope,
) -> Result<(), AiError> {
    let expected_principal_kind = match &principal_reference.kind {
        agql_auth::PrincipalReferenceKind::UserSession => "user".to_owned(),
        agql_auth::PrincipalReferenceKind::ApiToken { principal_kind } => {
            format!("api_token:{principal_kind}")
        }
    };
    if session.state != "active"
        || session.deleted_at.is_some()
        || session.owner_principal_kind != expected_principal_kind
        || session.owner_subject != principal_reference.subject
        || session.scope_kind != scope.kind
        || session.scope_id != scope.id
        || session.tenant_id != scope.tenant_id
        || principal_reference
            .tenant_id
            .as_ref()
            .is_some_and(|tenant_id| session.tenant_id.as_ref() != Some(tenant_id))
    {
        return Err(AiError::Forbidden);
    }
    Ok(())
}

fn validate_reconciliation_fence(
    run: &AiRunRecord,
    reservation: &AiBudgetReservationRecord,
    principal_reference: &PrincipalReference,
    reconciliation: &AiBudgetReconciliation,
    now: OffsetDateTime,
) -> Result<(), AiError> {
    let stored_reference: PrincipalReference =
        serde_json::from_value(run.principal_reference.clone())
            .map_err(|_| AiError::PersistenceFailed)?;
    if run.id != reservation.run_id
        || run.session_id != reservation.session_id
        || stored_reference != *principal_reference
        || run.state != "running"
        || run.attempt_id != Some(reconciliation.attempt_id)
        || reservation.attempt_id != reconciliation.attempt_id
        || run.lease_generation != reconciliation.lease_generation
        || reservation.lease_generation != reconciliation.lease_generation
        || run.lease_owner.as_deref().is_none_or(str::is_empty)
        || run
            .lease_expires_at
            .is_none_or(|expires| expires <= now.unix_timestamp())
    {
        return Err(AiError::Conflict);
    }
    Ok(())
}

fn policy_applies(
    policy: &AiBudgetPolicyRecord,
    scope: &AiScope,
    principal_kind: &str,
    principal_subject: &str,
) -> bool {
    policy_scope_integrity(policy, scope)
        && policy
            .principal_kind
            .as_deref()
            .is_none_or(|kind| kind == principal_kind)
        && policy
            .principal_subject
            .as_deref()
            .is_none_or(|subject| subject == principal_subject)
}

fn policy_scope_integrity(policy: &AiBudgetPolicyRecord, scope: &AiScope) -> bool {
    policy.scope_key
        == crate::ai_scope_key(&AiScope {
            kind: policy.scope_kind.clone(),
            id: policy.scope_id.clone(),
            tenant_id: policy.tenant_id.clone(),
        })
        && policy.scope_kind == scope.kind
        && policy.scope_id == scope.id
        && (policy.tenant_id.is_none() || policy.tenant_id == scope.tenant_id)
}

fn validate_policy_capacity(
    policy: &AiBudgetPolicyRecord,
    reserved: AiBudgetAmounts,
    committed: AiBudgetAmounts,
) -> Result<(), AiError> {
    let total = checked_add(reserved, committed)?;
    let limits = [
        (policy.maximum_input_tokens, total.input_tokens),
        (policy.maximum_output_tokens, total.output_tokens),
        (policy.maximum_tool_units, total.tool_units),
        (policy.maximum_image_units, total.image_units),
        (policy.maximum_cost_microunits, total.cost_microunits),
        (policy.maximum_runs, total.runs),
    ];
    let mut has_limit = false;
    for (maximum, value) in limits {
        if let Some(maximum) = maximum {
            has_limit = true;
            let maximum = u64::try_from(maximum).map_err(|_| {
                AiError::InvalidConfiguration("negative AI budget policy limit".to_owned())
            })?;
            if value > maximum {
                return Err(AiError::BudgetDenied);
            }
        }
    }
    if !has_limit {
        return Err(AiError::InvalidConfiguration(
            "AI budget policy has no ceiling".to_owned(),
        ));
    }
    Ok(())
}

fn budget_period(interval: &str, now: OffsetDateTime) -> Result<BudgetPeriod, AiError> {
    let timestamp = now.unix_timestamp();
    match interval {
        "minute" => fixed_period("minute", timestamp, 60),
        "hour" => fixed_period("hour", timestamp, 3_600),
        "day" => fixed_period("day", timestamp, 86_400),
        "month" => month_period(now),
        "lifetime" => Ok(BudgetPeriod {
            key: "lifetime".to_owned(),
            started_at: 0,
            ends_at: i64::MAX,
        }),
        _ => Err(AiError::InvalidConfiguration(
            "unknown AI budget interval".to_owned(),
        )),
    }
}

fn fixed_period(prefix: &str, timestamp: i64, seconds: i64) -> Result<BudgetPeriod, AiError> {
    let started_at = timestamp
        .div_euclid(seconds)
        .checked_mul(seconds)
        .ok_or_else(|| AiError::InvalidConfiguration("AI budget period overflow".to_owned()))?;
    let ends_at = started_at
        .checked_add(seconds)
        .ok_or_else(|| AiError::InvalidConfiguration("AI budget period overflow".to_owned()))?;
    Ok(BudgetPeriod {
        key: format!("{prefix}:{started_at}"),
        started_at,
        ends_at,
    })
}

fn month_period(now: OffsetDateTime) -> Result<BudgetPeriod, AiError> {
    let date = now.date();
    let year = date.year();
    let month = date.month();
    let start = Date::from_calendar_date(year, month, 1)
        .map_err(|_| AiError::InvalidConfiguration("invalid AI budget month".to_owned()))?
        .midnight()
        .assume_utc();
    let (next_year, next_month) = if month == Month::December {
        (
            year.checked_add(1).ok_or_else(|| {
                AiError::InvalidConfiguration("AI budget month overflow".to_owned())
            })?,
            Month::January,
        )
    } else {
        (year, month.next())
    };
    let end = Date::from_calendar_date(next_year, next_month, 1)
        .map_err(|_| AiError::InvalidConfiguration("invalid AI budget month".to_owned()))?
        .midnight()
        .assume_utc();
    Ok(BudgetPeriod {
        key: format!("month:{year:04}-{:02}", u8::from(month)),
        started_at: start.unix_timestamp(),
        ends_at: end.unix_timestamp(),
    })
}

fn reservation_matches_request(
    record: &AiBudgetReservationRecord,
    principal_kind: &str,
    principal_subject: &str,
    request: &AiBudgetReservationRequest,
    canonical_expiry: OffsetDateTime,
) -> bool {
    record.scope_kind == request.scope.kind
        && record.scope_id == request.scope.id
        && record.tenant_id == request.scope.tenant_id
        && record.principal_kind == principal_kind
        && record.principal_subject == principal_subject
        && record.session_id == request.session_id.0
        && record.run_id == request.run_id.0
        && record.attempt_id == request.attempt_id
        && record.lease_generation == request.lease_generation
        && record.provider_kind == request.provider_kind.as_str()
        && record.provider_model == request.model
        && record.pricing_policy_version == request.pricing_policy_version
        && reservation_amounts(record).is_ok_and(|amounts| amounts == request.estimate)
        && record.expires_at == canonical_expiry.unix_timestamp()
}

fn match_existing_reconciliation(
    record: &AiBudgetReservationRecord,
    state: AiBudgetReservationState,
    reconciliation: &AiBudgetReconciliation,
) -> Option<Result<AiBudgetReconciliationResult, AiError>> {
    let expected_state = match reconciliation.outcome {
        AiBudgetReconciliationOutcome::Commit => AiBudgetReservationState::Committed,
        AiBudgetReconciliationOutcome::ReleaseUnused => AiBudgetReservationState::Released,
        AiBudgetReconciliationOutcome::MarkUncertain => AiBudgetReservationState::Uncertain,
    };
    if state != expected_state {
        return None;
    }
    let actual = match reservation_actual(record) {
        Ok(actual) => actual,
        Err(error) => return Some(Err(error)),
    };
    if actual != reconciliation.actual {
        return Some(Err(AiError::Conflict));
    }
    let expected_cached_input = match reconciliation
        .cached_input_tokens
        .map(i64::try_from)
        .transpose()
    {
        Ok(value) => value,
        Err(_) => {
            return Some(Err(AiError::InvalidInput(
                "cached input exceeds storage".to_owned(),
            )));
        }
    };
    if record.actual_cached_input_tokens != expected_cached_input {
        return Some(Err(AiError::Conflict));
    }
    Some(record_to_reconciliation_result(record))
}

fn validate_reconciliation_actual(
    reserved: AiBudgetAmounts,
    actual: Option<AiBudgetAmounts>,
    cached_input_tokens: Option<u64>,
    outcome: AiBudgetReconciliationOutcome,
) -> Result<Option<AiBudgetAmounts>, AiError> {
    match outcome {
        AiBudgetReconciliationOutcome::Commit => {
            let actual = actual.ok_or_else(|| {
                AiError::InvalidInput("committed budget usage is required".to_owned())
            })?;
            if actual.runs != reserved.runs {
                return Err(AiError::InvalidInput(
                    "actual budget run count does not match reservation".to_owned(),
                ));
            }
            let cached_input_tokens = cached_input_tokens.ok_or_else(|| {
                AiError::InvalidInput("committed cached-token usage is required".to_owned())
            })?;
            if cached_input_tokens > actual.input_tokens {
                return Err(AiError::InvalidInput(
                    "cached input exceeds total input usage".to_owned(),
                ));
            }
            stored_amounts(actual)?;
            Ok(Some(actual))
        }
        AiBudgetReconciliationOutcome::ReleaseUnused => {
            if actual.is_some() || cached_input_tokens.is_some() {
                return Err(AiError::InvalidInput(
                    "unused budget release cannot include usage".to_owned(),
                ));
            }
            Ok(None)
        }
        AiBudgetReconciliationOutcome::MarkUncertain => {
            if let Some(actual) = actual {
                if actual.runs != reserved.runs {
                    return Err(AiError::InvalidInput(
                        "uncertain budget run count does not match reservation".to_owned(),
                    ));
                }
                if cached_input_tokens.is_some_and(|cached| cached > actual.input_tokens) {
                    return Err(AiError::InvalidInput(
                        "cached input exceeds uncertain input usage".to_owned(),
                    ));
                }
                stored_amounts(actual)?;
            } else if cached_input_tokens.is_some() {
                return Err(AiError::InvalidInput(
                    "cached input requires uncertain usage".to_owned(),
                ));
            }
            Ok(actual)
        }
    }
}

fn record_to_reservation(
    record: &AiBudgetReservationRecord,
) -> Result<AiBudgetReservation, AiError> {
    AiBudgetReservation::from_persisted(
        AiBudgetReservationId(record.id),
        AiRunId(record.run_id),
        record.attempt_id,
        record.lease_generation,
        parse_provider_kind(&record.provider_kind)?,
        record.provider_model.clone(),
        record.pricing_policy_version.clone(),
        reservation_amounts(record)?,
        reservation_actual(record)?,
        parse_reservation_state(&record.state)?,
        OffsetDateTime::from_unix_timestamp(record.expires_at)
            .map_err(|_| AiError::PersistenceFailed)?,
    )
}

fn record_to_reconciliation_result(
    record: &AiBudgetReservationRecord,
) -> Result<AiBudgetReconciliationResult, AiError> {
    let reservation = record_to_reservation(record)?;
    let reserved = reservation.reserved();
    let actual = reservation.actual();
    let (committed, released, held) = match reservation.state() {
        AiBudgetReservationState::Committed => {
            let actual = actual.ok_or(AiError::PersistenceFailed)?;
            (
                actual,
                saturating_sub(reserved, actual),
                AiBudgetAmounts::default(),
            )
        }
        AiBudgetReservationState::Released | AiBudgetReservationState::Expired => (
            AiBudgetAmounts::default(),
            reserved,
            AiBudgetAmounts::default(),
        ),
        AiBudgetReservationState::Uncertain => (
            AiBudgetAmounts::default(),
            AiBudgetAmounts::default(),
            reserved,
        ),
        AiBudgetReservationState::Reserved => (
            AiBudgetAmounts::default(),
            AiBudgetAmounts::default(),
            reserved,
        ),
    };
    Ok(AiBudgetReconciliationResult {
        reservation,
        committed,
        released,
        held,
    })
}

fn reservation_counter_ids(record: &AiBudgetReservationRecord) -> Result<Vec<Uuid>, AiError> {
    let ids: Vec<Uuid> = serde_json::from_value(record.budget_counter_ids.clone())
        .map_err(|_| AiError::PersistenceFailed)?;
    let unique = ids.iter().copied().collect::<BTreeSet<_>>();
    if ids.is_empty() || unique.len() != ids.len() {
        return Err(AiError::PersistenceFailed);
    }
    Ok(ids)
}

fn reservation_amounts(record: &AiBudgetReservationRecord) -> Result<AiBudgetAmounts, AiError> {
    stored_amounts(AiBudgetAmounts {
        input_tokens: amount_from_i64(record.reserved_input_tokens)?,
        output_tokens: amount_from_i64(record.reserved_output_tokens)?,
        tool_units: amount_from_i64(record.reserved_tool_units)?,
        image_units: amount_from_i64(record.reserved_image_units)?,
        cost_microunits: amount_from_i64(record.reserved_cost_microunits)?,
        runs: amount_from_i64(record.reserved_runs)?,
    })
}

fn reservation_actual(
    record: &AiBudgetReservationRecord,
) -> Result<Option<AiBudgetAmounts>, AiError> {
    let values = [
        record.actual_input_tokens,
        record.actual_output_tokens,
        record.actual_tool_units,
        record.actual_image_units,
        record.actual_cost_microunits,
        record.actual_runs,
    ];
    if values.iter().all(Option::is_none) {
        return Ok(None);
    }
    if values.iter().any(Option::is_none) {
        return Err(AiError::PersistenceFailed);
    }
    Ok(Some(stored_amounts(AiBudgetAmounts {
        input_tokens: amount_from_i64(
            record
                .actual_input_tokens
                .ok_or(AiError::PersistenceFailed)?,
        )?,
        output_tokens: amount_from_i64(
            record
                .actual_output_tokens
                .ok_or(AiError::PersistenceFailed)?,
        )?,
        tool_units: amount_from_i64(record.actual_tool_units.ok_or(AiError::PersistenceFailed)?)?,
        image_units: amount_from_i64(
            record
                .actual_image_units
                .ok_or(AiError::PersistenceFailed)?,
        )?,
        cost_microunits: amount_from_i64(
            record
                .actual_cost_microunits
                .ok_or(AiError::PersistenceFailed)?,
        )?,
        runs: amount_from_i64(record.actual_runs.ok_or(AiError::PersistenceFailed)?)?,
    })?))
}

fn counter_reserved(record: &AiBudgetCounterRecord) -> Result<AiBudgetAmounts, AiError> {
    stored_amounts(AiBudgetAmounts {
        input_tokens: amount_from_i64(record.reserved_input_tokens)?,
        output_tokens: amount_from_i64(record.reserved_output_tokens)?,
        tool_units: amount_from_i64(record.reserved_tool_units)?,
        image_units: amount_from_i64(record.reserved_image_units)?,
        cost_microunits: amount_from_i64(record.reserved_cost_microunits)?,
        runs: amount_from_i64(record.reserved_runs)?,
    })
}

fn counter_committed(record: &AiBudgetCounterRecord) -> Result<AiBudgetAmounts, AiError> {
    stored_amounts(AiBudgetAmounts {
        input_tokens: amount_from_i64(record.committed_input_tokens)?,
        output_tokens: amount_from_i64(record.committed_output_tokens)?,
        tool_units: amount_from_i64(record.committed_tool_units)?,
        image_units: amount_from_i64(record.committed_image_units)?,
        cost_microunits: amount_from_i64(record.committed_cost_microunits)?,
        runs: amount_from_i64(record.committed_runs)?,
    })
}

fn stored_amounts(amounts: AiBudgetAmounts) -> Result<AiBudgetAmounts, AiError> {
    ensure_storable(amounts.input_tokens)?;
    ensure_storable(amounts.output_tokens)?;
    ensure_storable(amounts.tool_units)?;
    ensure_storable(amounts.image_units)?;
    ensure_storable(amounts.cost_microunits)?;
    ensure_storable(amounts.runs)?;
    Ok(amounts)
}

fn checked_add(left: AiBudgetAmounts, right: AiBudgetAmounts) -> Result<AiBudgetAmounts, AiError> {
    stored_amounts(AiBudgetAmounts {
        input_tokens: checked_component_add(left.input_tokens, right.input_tokens)?,
        output_tokens: checked_component_add(left.output_tokens, right.output_tokens)?,
        tool_units: checked_component_add(left.tool_units, right.tool_units)?,
        image_units: checked_component_add(left.image_units, right.image_units)?,
        cost_microunits: checked_component_add(left.cost_microunits, right.cost_microunits)?,
        runs: checked_component_add(left.runs, right.runs)?,
    })
}

fn checked_sub(left: AiBudgetAmounts, right: AiBudgetAmounts) -> Result<AiBudgetAmounts, AiError> {
    Ok(AiBudgetAmounts {
        input_tokens: checked_component_sub(left.input_tokens, right.input_tokens)?,
        output_tokens: checked_component_sub(left.output_tokens, right.output_tokens)?,
        tool_units: checked_component_sub(left.tool_units, right.tool_units)?,
        image_units: checked_component_sub(left.image_units, right.image_units)?,
        cost_microunits: checked_component_sub(left.cost_microunits, right.cost_microunits)?,
        runs: checked_component_sub(left.runs, right.runs)?,
    })
}

fn saturating_sub(left: AiBudgetAmounts, right: AiBudgetAmounts) -> AiBudgetAmounts {
    AiBudgetAmounts {
        input_tokens: left.input_tokens.saturating_sub(right.input_tokens),
        output_tokens: left.output_tokens.saturating_sub(right.output_tokens),
        tool_units: left.tool_units.saturating_sub(right.tool_units),
        image_units: left.image_units.saturating_sub(right.image_units),
        cost_microunits: left.cost_microunits.saturating_sub(right.cost_microunits),
        runs: left.runs.saturating_sub(right.runs),
    }
}

fn checked_component_add(left: u64, right: u64) -> Result<u64, AiError> {
    left.checked_add(right)
        .ok_or_else(|| AiError::InvalidConfiguration("AI budget amount overflow".to_owned()))
}

fn checked_component_sub(left: u64, right: u64) -> Result<u64, AiError> {
    left.checked_sub(right).ok_or(AiError::PersistenceFailed)
}

fn ensure_storable(value: u64) -> Result<(), AiError> {
    i64::try_from(value)
        .map(|_| ())
        .map_err(|_| AiError::InvalidConfiguration("AI budget amount exceeds storage".to_owned()))
}

fn amount_from_i64(value: i64) -> Result<u64, AiError> {
    u64::try_from(value).map_err(|_| AiError::PersistenceFailed)
}

fn amount_to_i64(value: u64) -> Result<i64, OrmPublicError> {
    i64::try_from(value).map_err(|_| OrmPublicError::new(OrmErrorCode::InvalidInput))
}

fn counter_create(
    policy_id: Uuid,
    policy_version: i64,
    period: &BudgetPeriod,
    reserved: AiBudgetAmounts,
) -> Result<CreateAiBudgetCounterRecordInput, OrmPublicError> {
    Ok(CreateAiBudgetCounterRecordInput {
        budget_policy_id: policy_id,
        policy_version,
        period_key: period.key.clone(),
        period_started_at: period.started_at,
        period_ends_at: period.ends_at,
        reserved_input_tokens: amount_to_i64(reserved.input_tokens)?,
        reserved_output_tokens: amount_to_i64(reserved.output_tokens)?,
        reserved_tool_units: amount_to_i64(reserved.tool_units)?,
        reserved_image_units: amount_to_i64(reserved.image_units)?,
        reserved_cost_microunits: amount_to_i64(reserved.cost_microunits)?,
        reserved_runs: amount_to_i64(reserved.runs)?,
        committed_input_tokens: 0,
        committed_output_tokens: 0,
        committed_tool_units: 0,
        committed_image_units: 0,
        committed_cost_microunits: 0,
        committed_runs: 0,
    })
}

fn counter_update(
    policy_version: i64,
    period: &BudgetPeriod,
    reserved: AiBudgetAmounts,
    committed: AiBudgetAmounts,
) -> Result<UpdateAiBudgetCounterRecordInput, OrmPublicError> {
    let mut update = counter_amount_update(reserved, committed)?;
    update.policy_version = Some(policy_version);
    update.period_started_at = Some(period.started_at);
    update.period_ends_at = Some(period.ends_at);
    Ok(update)
}

fn counter_amount_update(
    reserved: AiBudgetAmounts,
    committed: AiBudgetAmounts,
) -> Result<UpdateAiBudgetCounterRecordInput, OrmPublicError> {
    Ok(UpdateAiBudgetCounterRecordInput {
        reserved_input_tokens: Some(amount_to_i64(reserved.input_tokens)?),
        reserved_output_tokens: Some(amount_to_i64(reserved.output_tokens)?),
        reserved_tool_units: Some(amount_to_i64(reserved.tool_units)?),
        reserved_image_units: Some(amount_to_i64(reserved.image_units)?),
        reserved_cost_microunits: Some(amount_to_i64(reserved.cost_microunits)?),
        reserved_runs: Some(amount_to_i64(reserved.runs)?),
        committed_input_tokens: Some(amount_to_i64(committed.input_tokens)?),
        committed_output_tokens: Some(amount_to_i64(committed.output_tokens)?),
        committed_tool_units: Some(amount_to_i64(committed.tool_units)?),
        committed_image_units: Some(amount_to_i64(committed.image_units)?),
        committed_cost_microunits: Some(amount_to_i64(committed.cost_microunits)?),
        committed_runs: Some(amount_to_i64(committed.runs)?),
        ..Default::default()
    })
}

fn parse_provider_kind(value: &str) -> Result<ProviderKind, AiError> {
    match value {
        "openai" => Ok(ProviderKind::OpenAi),
        "anthropic" => Ok(ProviderKind::Anthropic),
        "xai" => Ok(ProviderKind::Xai),
        "ollama" => Ok(ProviderKind::Ollama),
        "openai_compatible" => Ok(ProviderKind::OpenAiCompatible),
        "local_harness" => Ok(ProviderKind::LocalHarness),
        _ => Err(AiError::PersistenceFailed),
    }
}

fn parse_reservation_state(value: &str) -> Result<AiBudgetReservationState, AiError> {
    match value {
        "reserved" => Ok(AiBudgetReservationState::Reserved),
        "committed" => Ok(AiBudgetReservationState::Committed),
        "released" => Ok(AiBudgetReservationState::Released),
        "uncertain" => Ok(AiBudgetReservationState::Uncertain),
        "expired" => Ok(AiBudgetReservationState::Expired),
        _ => Err(AiError::PersistenceFailed),
    }
}

fn principal_identity(principal: &AuthPrincipal) -> (String, &str) {
    let kind = match principal {
        AuthPrincipal::User(_) => "user".to_owned(),
        AuthPrincipal::ApiToken(token) => {
            format!("api_token:{}", token.principal_kind.as_str())
        }
    };
    (kind, principal.subject())
}

fn canonical_second(value: OffsetDateTime) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(value.unix_timestamp())
        .expect("an existing OffsetDateTime unix timestamp remains representable")
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
    use agql_auth::{AccessTokenMetadata, AuthUser, FixedClock, SessionContext};
    use graphql_orm::graphql::orm::{ApplyOptions, OrmSchemaModule};
    use graphql_orm::prelude::{Database, SqliteBackend};

    const TENANT: &str = "tenant-test";
    const SUBJECT: &str = "budget-user";

    struct Fixture {
        service: OrmAiBudgetService,
        database: Database<SqliteBackend>,
        principal: ResolvedPrincipal,
        now: OffsetDateTime,
    }

    async fn fixture(maximum_input_tokens: i64) -> Fixture {
        let database = Database::<SqliteBackend>::connect_sqlite("sqlite::memory:")
            .await
            .expect("in-memory SQLite should open");
        let module = crate::AiSchemaModule;
        let plan = database
            .schema()
            .plan_migration_to_entities(
                "ai-budget-test-v1",
                "AI budget service test",
                module.entities(),
            )
            .await
            .expect("AI schema migration should plan");
        database
            .schema()
            .apply_migration(&plan, ApplyOptions::default())
            .await
            .expect("AI schema migration should apply to in-memory SQLite");

        AiBudgetPolicyRecord::insert(
            &database,
            CreateAiBudgetPolicyRecordInput {
                scope_key: crate::ai_scope_key(&AiScope::new("tenant", TENANT)),
                scope_kind: "tenant".to_owned(),
                scope_id: TENANT.to_owned(),
                tenant_id: None,
                principal_kind: None,
                principal_subject: None,
                interval_kind: "day".to_owned(),
                maximum_input_tokens: Some(maximum_input_tokens),
                maximum_output_tokens: Some(1_000),
                maximum_tool_units: Some(100),
                maximum_image_units: Some(100),
                maximum_cost_microunits: Some(10_000),
                maximum_runs: Some(100),
                enabled: true,
            },
        )
        .await
        .expect("budget policy should seed through the generated repository");

        let now = OffsetDateTime::from_unix_timestamp(1_800_000_000)
            .expect("fixed test timestamp should be valid");
        let principal = resolved_principal(now);
        let limits = AiBudgetServiceLimits::new(
            AiBudgetAmounts {
                input_tokens: 1_000,
                output_tokens: 1_000,
                tool_units: 100,
                image_units: 100,
                cost_microunits: 10_000,
                runs: 1,
            },
            Duration::minutes(5),
            Duration::seconds(30),
            16,
            8,
        )
        .expect("test deployment limits should validate");
        let service =
            OrmAiBudgetService::new(database.clone(), Arc::new(FixedClock::new(now)), limits);
        Fixture {
            service,
            database,
            principal,
            now,
        }
    }

    fn resolved_principal(now: OffsetDateTime) -> ResolvedPrincipal {
        let principal = AuthPrincipal::User(AuthUser {
            user_id: SUBJECT.to_owned(),
            session_id: Uuid::new_v4(),
            roles: vec![],
            scopes: vec![],
            session: SessionContext::default(),
            token_claims: AccessTokenMetadata {
                tenant_id: Some(TENANT.to_owned()),
                ..AccessTokenMetadata::default()
            },
        });
        ResolvedPrincipal::new(principal.reference(), principal, now)
            .expect("matching fresh principal should resolve")
    }

    async fn seed_running_run(
        database: &Database<SqliteBackend>,
        principal: &ResolvedPrincipal,
        now: OffsetDateTime,
    ) -> (crate::AiSessionId, AiRunId, Uuid) {
        let session_id = crate::AiSessionId::new();
        let run_id = AiRunId::new();
        let attempt_id = Uuid::new_v4();
        let (owner_principal_kind, owner_subject) = principal_identity(principal.principal());
        AiSessionRecord::insert(
            database,
            CreateAiSessionRecordInput {
                id: session_id.0,
                owner_principal_kind,
                owner_subject: owner_subject.to_owned(),
                tenant_id: Some(TENANT.to_owned()),
                scope_kind: "tenant".to_owned(),
                scope_id: TENANT.to_owned(),
                title: "Budget test".to_owned(),
                state: "active".to_owned(),
                stream_head: 0,
                message_head: 0,
                last_activity_at: now.unix_timestamp(),
                archived_at: None,
                deleted_at: None,
            },
        )
        .await
        .expect("budget test session should seed through the generated repository");
        AiRunRecord::insert(
            database,
            CreateAiRunRecordInput {
                id: run_id.0,
                session_id: session_id.0,
                input_message_id: Uuid::new_v4(),
                principal_reference: serde_json::to_value(principal.reference())
                    .expect("principal reference should serialize"),
                state: "running".to_owned(),
                attempt_id: Some(attempt_id),
                lease_owner: Some("worker-test".to_owned()),
                lease_generation: 1,
                lease_expires_at: Some((now + Duration::minutes(4)).unix_timestamp()),
                lease_heartbeat_at: Some(now.unix_timestamp()),
                retry_count: 0,
                next_attempt_at: None,
                error_code: None,
                latest_checkpoint_id: None,
            },
        )
        .await
        .expect("running test run should seed through the generated repository");
        (session_id, run_id, attempt_id)
    }

    fn request(
        session_id: crate::AiSessionId,
        run_id: AiRunId,
        attempt_id: Uuid,
        input_tokens: u64,
        idempotency_key: &str,
        now: OffsetDateTime,
    ) -> AiBudgetReservationRequest {
        AiBudgetReservationRequest {
            scope: AiScope::new("tenant", TENANT).with_tenant_id(TENANT),
            session_id,
            run_id,
            attempt_id,
            lease_generation: 1,
            provider_kind: ProviderKind::OpenAi,
            model: "test-model".to_owned(),
            pricing_policy_version: "pricing-test-v1".to_owned(),
            estimate: AiBudgetAmounts {
                input_tokens,
                output_tokens: 10,
                tool_units: 0,
                image_units: 0,
                cost_microunits: 100,
                runs: 1,
            },
            idempotency_key: idempotency_key.to_owned(),
            expires_at: now + Duration::minutes(2),
        }
    }

    async fn only_counter(database: &Database<SqliteBackend>) -> AiBudgetCounterRecord {
        database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    let counters = tx
                        .query::<AiBudgetCounterRecord>()
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if counters.len() != 1 {
                        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                    }
                    Ok(counters
                        .into_iter()
                        .next()
                        .expect("one counter was checked"))
                })
            })
            .await
            .expect("counter query should succeed")
    }

    async fn counter_count(database: &Database<SqliteBackend>) -> i64 {
        database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    tx.query::<AiBudgetCounterRecord>()
                        .count()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("counter count should succeed")
    }

    async fn usage_entries(database: &Database<SqliteBackend>) -> Vec<AiUsageEntryRecord> {
        database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    tx.query::<AiUsageEntryRecord>()
                        .limit(10)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("usage query should succeed")
    }

    #[tokio::test]
    async fn concurrent_reservations_cannot_overspend_one_counter() {
        let fixture = fixture(100).await;
        let first = seed_running_run(&fixture.database, &fixture.principal, fixture.now).await;
        let second = seed_running_run(&fixture.database, &fixture.principal, fixture.now).await;
        let first_request = request(first.0, first.1, first.2, 60, "concurrent-1", fixture.now);
        let second_request = request(
            second.0,
            second.1,
            second.2,
            60,
            "concurrent-2",
            fixture.now,
        );

        let (first_result, second_result) = tokio::join!(
            fixture.service.reserve(&fixture.principal, first_request),
            fixture.service.reserve(&fixture.principal, second_request),
        );
        let successful = usize::from(first_result.is_ok()) + usize::from(second_result.is_ok());
        let denied = usize::from(matches!(first_result, Err(AiError::BudgetDenied)))
            + usize::from(matches!(second_result, Err(AiError::BudgetDenied)));
        assert_eq!(successful, 1, "exactly one reservation should fit");
        assert_eq!(denied, 1, "the racing overspend should be denied");

        let counter = only_counter(&fixture.database).await;
        assert_eq!(counter.reserved_input_tokens, 60);
        assert_eq!(counter.reserved_runs, 1);
        assert_eq!(counter.committed_input_tokens, 0);
    }

    #[tokio::test]
    async fn every_applicable_policy_is_checked_before_any_counter_is_written() {
        let fixture = fixture(100).await;
        AiBudgetPolicyRecord::insert(
            &fixture.database,
            CreateAiBudgetPolicyRecordInput {
                scope_key: crate::ai_scope_key(
                    &AiScope::new("tenant", TENANT).with_tenant_id(TENANT),
                ),
                scope_kind: "tenant".to_owned(),
                scope_id: TENANT.to_owned(),
                tenant_id: Some(TENANT.to_owned()),
                principal_kind: Some("user".to_owned()),
                principal_subject: Some(SUBJECT.to_owned()),
                interval_kind: "day".to_owned(),
                maximum_input_tokens: Some(50),
                maximum_output_tokens: Some(1_000),
                maximum_tool_units: Some(100),
                maximum_image_units: Some(100),
                maximum_cost_microunits: Some(10_000),
                maximum_runs: Some(100),
                enabled: true,
            },
        )
        .await
        .expect("principal policy should seed");
        let run = seed_running_run(&fixture.database, &fixture.principal, fixture.now).await;

        assert!(matches!(
            fixture
                .service
                .reserve(
                    &fixture.principal,
                    request(run.0, run.1, run.2, 60, "multi-policy", fixture.now),
                )
                .await,
            Err(AiError::BudgetDenied)
        ));
        assert_eq!(
            counter_count(&fixture.database).await,
            0,
            "a denied later policy must not leave a partial counter reservation"
        );
    }

    #[tokio::test]
    async fn reservation_requires_fresh_principal_tenant_binding_and_current_fence() {
        let fixture = fixture(100).await;
        let run = seed_running_run(&fixture.database, &fixture.principal, fixture.now).await;
        let valid = request(run.0, run.1, run.2, 20, "security", fixture.now);
        let stale = resolved_principal(fixture.now - Duration::minutes(1));
        assert!(matches!(
            fixture.service.reserve(&stale, valid.clone()).await,
            Err(AiError::ReauthorizationFailed)
        ));

        let mut wrong_tenant = valid.clone();
        wrong_tenant.scope.tenant_id = Some("different-tenant".to_owned());
        assert!(matches!(
            fixture
                .service
                .reserve(&fixture.principal, wrong_tenant)
                .await,
            Err(AiError::Forbidden)
        ));

        let mut swapped_scope = valid.clone();
        swapped_scope.scope.id = "different-scope".to_owned();
        assert!(matches!(
            fixture
                .service
                .reserve(&fixture.principal, swapped_scope)
                .await,
            Err(AiError::Forbidden)
        ));

        let mut stale_fence = valid;
        stale_fence.lease_generation = 2;
        assert!(matches!(
            fixture
                .service
                .reserve(&fixture.principal, stale_fence)
                .await,
            Err(AiError::Conflict)
        ));
        assert_eq!(counter_count(&fixture.database).await, 0);
    }

    #[tokio::test]
    async fn reservation_and_commit_reconciliation_are_idempotent_and_release_unused_capacity() {
        let fixture = fixture(100).await;
        let run = seed_running_run(&fixture.database, &fixture.principal, fixture.now).await;
        let reservation_request = request(run.0, run.1, run.2, 60, "idempotent", fixture.now);
        let reservation = fixture
            .service
            .reserve(&fixture.principal, reservation_request.clone())
            .await
            .expect("first reservation should succeed");
        let replay = fixture
            .service
            .reserve(&fixture.principal, reservation_request.clone())
            .await
            .expect("exact reservation replay should be idempotent");
        assert_eq!(reservation.id(), replay.id());

        let mut changed = reservation_request;
        changed.model = "different-model".to_owned();
        assert!(matches!(
            fixture.service.reserve(&fixture.principal, changed).await,
            Err(AiError::Conflict)
        ));

        let reconciliation = AiBudgetReconciliation {
            reservation_id: reservation.id(),
            attempt_id: run.2,
            lease_generation: 1,
            actual: Some(AiBudgetAmounts {
                input_tokens: 40,
                output_tokens: 5,
                tool_units: 0,
                image_units: 0,
                cost_microunits: 80,
                runs: 1,
            }),
            cached_input_tokens: Some(4),
            outcome: AiBudgetReconciliationOutcome::Commit,
        };
        let committed = fixture
            .service
            .reconcile(&fixture.principal, reconciliation.clone())
            .await
            .expect("authoritative usage should commit");
        let replay = fixture
            .service
            .reconcile(&fixture.principal, reconciliation)
            .await
            .expect("same reconciliation should be idempotent");
        assert_eq!(committed, replay);
        assert_eq!(committed.committed.input_tokens, 40);
        assert_eq!(committed.released.input_tokens, 20);
        assert_eq!(committed.released.output_tokens, 5);

        let usage = usage_entries(&fixture.database).await;
        assert_eq!(usage.len(), 1, "an exact replay must not duplicate usage");
        assert_eq!(usage[0].budget_reservation_id, reservation.id().0);
        assert_eq!(usage[0].principal_kind, "user");
        assert_eq!(usage[0].principal_subject, SUBJECT);
        assert_eq!(usage[0].input_tokens, 40);
        assert_eq!(usage[0].cached_input_tokens, 4);
        assert_eq!(usage[0].output_tokens, 5);
        assert_eq!(usage[0].cost_microunits, Some(80));

        let counter = only_counter(&fixture.database).await;
        assert_eq!(counter.reserved_input_tokens, 0);
        assert_eq!(counter.committed_input_tokens, 40);
        assert_eq!(counter.committed_runs, 1);

        let next = seed_running_run(&fixture.database, &fixture.principal, fixture.now).await;
        fixture
            .service
            .reserve(
                &fixture.principal,
                request(
                    next.0,
                    next.1,
                    next.2,
                    60,
                    "remaining-capacity",
                    fixture.now,
                ),
            )
            .await
            .expect("proven unused capacity should be reusable");
    }

    #[tokio::test]
    async fn uncertain_usage_remains_held_until_authoritative_commit() {
        let fixture = fixture(100).await;
        let run = seed_running_run(&fixture.database, &fixture.principal, fixture.now).await;
        let reservation = fixture
            .service
            .reserve(
                &fixture.principal,
                request(run.0, run.1, run.2, 80, "uncertain", fixture.now),
            )
            .await
            .expect("reservation should succeed");
        let uncertain = AiBudgetReconciliation {
            reservation_id: reservation.id(),
            attempt_id: run.2,
            lease_generation: 1,
            actual: None,
            cached_input_tokens: None,
            outcome: AiBudgetReconciliationOutcome::MarkUncertain,
        };
        let held = fixture
            .service
            .reconcile(&fixture.principal, uncertain.clone())
            .await
            .expect("uncertain external execution should remain held");
        assert_eq!(held.held.input_tokens, 80);
        assert_eq!(
            fixture
                .service
                .reconcile(&fixture.principal, uncertain)
                .await
                .expect("uncertain replay should be idempotent"),
            held
        );

        let blocked = seed_running_run(&fixture.database, &fixture.principal, fixture.now).await;
        assert!(matches!(
            fixture
                .service
                .reserve(
                    &fixture.principal,
                    request(blocked.0, blocked.1, blocked.2, 30, "blocked", fixture.now),
                )
                .await,
            Err(AiError::BudgetDenied)
        ));
        assert!(matches!(
            fixture
                .service
                .reconcile(
                    &fixture.principal,
                    AiBudgetReconciliation {
                        reservation_id: reservation.id(),
                        attempt_id: run.2,
                        lease_generation: 1,
                        actual: None,
                        cached_input_tokens: None,
                        outcome: AiBudgetReconciliationOutcome::ReleaseUnused,
                    },
                )
                .await,
            Err(AiError::Conflict)
        ));

        fixture
            .service
            .reconcile(
                &fixture.principal,
                AiBudgetReconciliation {
                    reservation_id: reservation.id(),
                    attempt_id: run.2,
                    lease_generation: 1,
                    actual: Some(AiBudgetAmounts {
                        input_tokens: 50,
                        output_tokens: 5,
                        tool_units: 0,
                        image_units: 0,
                        cost_microunits: 80,
                        runs: 1,
                    }),
                    cached_input_tokens: Some(5),
                    outcome: AiBudgetReconciliationOutcome::Commit,
                },
            )
            .await
            .expect("authoritative recovery usage should settle an uncertain call");

        let remaining = seed_running_run(&fixture.database, &fixture.principal, fixture.now).await;
        fixture
            .service
            .reserve(
                &fixture.principal,
                request(
                    remaining.0,
                    remaining.1,
                    remaining.2,
                    50,
                    "after-recovery",
                    fixture.now,
                ),
            )
            .await
            .expect("only authoritative unused capacity should be reusable");
    }
}
