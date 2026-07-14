//! Bounded top-level coordination for sequential supervised mutations.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::sync::Arc;

use agql_auth::Clock;
use async_trait::async_trait;
use time::Duration;
use uuid::Uuid;

use crate::{
    AiAdoptedSupervisedToolBatch, AiAgentCheckpointWriter, AiAgentLoopGuard, AiAgentLoopLimits,
    AiAgentLoopTurn, AiAgentProviderOutputWriter, AiAgentProviderTurnExecutor,
    AiAgentRecoveryPhase, AiAgentRuleResolver, AiAgentRunControl, AiApplicationToolCallContext,
    AiApprovalId, AiError, AiProviderCallPlan, AiRequestedConsequentialToolCall, AiResolvedRuleSet,
    AiRuleRunUsage, AiRunCompletion, AiRunLease, AiRunState, AiScope, AiSupervisedResumeOutcome,
    AiToolCallId, AiToolResultEgressRoute, OrmAiConsequentialToolCallService,
    OrmAiCoordinatorCheckpointService, OrmAiSupervisedResumeService,
};

/// Deployment bounds for the sequential supervised coordinator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiSupervisedAgentCoordinatorLimits {
    loop_limits: AiAgentLoopLimits,
    heartbeat_interval: Duration,
    approval_ttl: Duration,
    recent_mfa_required: bool,
}

impl AiSupervisedAgentCoordinatorLimits {
    /// Creates supervised coordinator limits.
    ///
    /// The heartbeat interval must remain comfortably below the run-service
    /// lease TTL. Approval TTL is measured from durable staging, not provider
    /// planning time.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless heartbeat is positive
    /// and at most five minutes and approval TTL is positive and at most one
    /// day.
    pub fn new(
        loop_limits: AiAgentLoopLimits,
        heartbeat_interval: Duration,
        approval_ttl: Duration,
        recent_mfa_required: bool,
    ) -> Result<Self, AiError> {
        if !heartbeat_interval.is_positive()
            || heartbeat_interval > Duration::minutes(5)
            || !approval_ttl.is_positive()
            || approval_ttl > Duration::days(1)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid supervised coordinator limits".to_owned(),
            ));
        }
        Ok(Self {
            loop_limits,
            heartbeat_interval,
            approval_ttl,
            recent_mfa_required,
        })
    }
}

/// One host-planned provider turn exposing only supervised one-shot mutations.
///
/// Construction proves the plan is provider-retained, contains only immutable
/// `SupervisedWrite`/`OneShot` bindings, targets the exact resolved-rule scope,
/// and has a valid server-owned result route. It grants no provider, budget,
/// egress, approval, mutation, or resolver authority.
pub struct AiSupervisedAgentTurnPlan {
    provider_call: AiProviderCallPlan,
    result_egress_route: AiToolResultEgressRoute,
    rules: AiResolvedRuleSet,
    uses_byok: bool,
}

impl AiSupervisedAgentTurnPlan {
    /// Creates an exact supervised-only provider turn.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidInput`] unless the provider plan contains
    /// only supervised one-shot tools, uses provider-retained continuation,
    /// matches the rule target, and the result route is valid.
    pub fn new(
        provider_call: AiProviderCallPlan,
        result_egress_route: AiToolResultEgressRoute,
        rules: AiResolvedRuleSet,
        uses_byok: bool,
    ) -> Result<Self, AiError> {
        if !provider_call.has_only_supervised_tools()
            || !provider_call.uses_provider_retained_continuation()
            || provider_call.scope() != rules.target_scope()
        {
            return Err(AiError::InvalidInput(
                "supervised turn is not exactly bound".to_owned(),
            ));
        }
        result_egress_route.validate()?;
        Ok(Self {
            provider_call,
            result_egress_route,
            rules,
            uses_byok,
        })
    }

    fn is_continuation(&self) -> bool {
        self.provider_call.is_continuation()
    }

    fn into_parts(
        self,
    ) -> (
        AiProviderCallPlan,
        AiScope,
        String,
        AiToolResultEgressRoute,
        AiResolvedRuleSet,
        bool,
    ) {
        let scope = self.provider_call.scope().clone();
        let correlation_id = self.provider_call.correlation_id().to_owned();
        (
            self.provider_call,
            scope,
            correlation_id,
            self.result_egress_route,
            self.rules,
            self.uses_byok,
        )
    }
}

/// Host-owned construction of supervised initial and continuation turns.
///
/// Implementations select configuration, exact registered definitions,
/// provider/model, current rule evidence, atomic-budget estimate, and every
/// egress manifest. Continuation implementations must use
/// [`AiProviderCallPlan::new_supervised_continuation_with_tools`].
#[async_trait]
pub trait AiSupervisedAgentTurnPlanner: Send + Sync {
    /// Builds the first supervised-only provider turn.
    ///
    /// # Errors
    ///
    /// Returns a safe error when current configuration cannot produce an
    /// exact initial turn.
    async fn initial_plan(&self, lease: &AiRunLease) -> Result<AiSupervisedAgentTurnPlan, AiError>;

    /// Builds the next turn from one opaque approved mutation result.
    ///
    /// # Errors
    ///
    /// Returns a safe error when the exact continuation cannot be bound to a
    /// fresh provider, budget, egress, route, and rule plan.
    async fn continuation_plan(
        &self,
        lease: &AiRunLease,
        provider_turns: u32,
        continuation: crate::AiAgentContinuation,
    ) -> Result<AiSupervisedAgentTurnPlan, AiError>;
}

/// Durable wait created for one provider-requested supervised mutation.
#[derive(Clone, Debug)]
pub struct AiSupervisedApprovalWait {
    approval_id: AiApprovalId,
    tool_call_id: AiToolCallId,
    lease: AiRunLease,
}

impl AiSupervisedApprovalWait {
    fn from_requested(requested: AiRequestedConsequentialToolCall) -> Self {
        Self {
            approval_id: requested.approval_id(),
            tool_call_id: requested.tool_call_id(),
            lease: requested.into_lease(),
        }
    }

    /// Exact pending approval.
    pub const fn approval_id(&self) -> AiApprovalId {
        self.approval_id
    }

    /// Exact staged consequential tool call.
    pub const fn tool_call_id(&self) -> AiToolCallId {
        self.tool_call_id
    }

    /// Durable waiting lease.
    pub fn lease(&self) -> &AiRunLease {
        &self.lease
    }
}

/// Approval-staging boundary used by the coordinator.
#[async_trait]
pub trait AiAgentSupervisedApprovalStager: Send + Sync {
    /// Stages one exact provider-requested mutation for human review.
    ///
    /// # Errors
    ///
    /// Returns a safe error for invalid tool count/binding, preview or current
    /// authorization denial, protection failure, or persistence ambiguity.
    async fn stage(
        &self,
        lease: &AiRunLease,
        result: &crate::AiProviderCallResult,
        context: AiApplicationToolCallContext,
        expires_at: time::OffsetDateTime,
        recent_mfa_required: bool,
    ) -> Result<AiSupervisedApprovalWait, AiError>;
}

#[async_trait]
impl AiAgentSupervisedApprovalStager for OrmAiConsequentialToolCallService {
    async fn stage(
        &self,
        lease: &AiRunLease,
        result: &crate::AiProviderCallResult,
        context: AiApplicationToolCallContext,
        expires_at: time::OffsetDateTime,
        recent_mfa_required: bool,
    ) -> Result<AiSupervisedApprovalWait, AiError> {
        self.request_approval(lease, result, context, expires_at, recent_mfa_required)
            .await
            .map(AiSupervisedApprovalWait::from_requested)
    }
}

/// Protected supervised-checkpoint adoption and one-shot consumption.
#[async_trait]
pub trait AiAgentSupervisedCheckpointControl: Send + Sync {
    /// Reopens the exact linked supervised result under current authority.
    ///
    /// # Errors
    ///
    /// Returns a safe error for malformed, stale, unauthorized, or unprovable
    /// protected checkpoint state.
    async fn adopt(
        &self,
        lease: &AiRunLease,
    ) -> Result<Option<AiAdoptedSupervisedToolBatch>, AiError>;

    /// Consumes the exact adoption proof before provider transport.
    ///
    /// # Errors
    ///
    /// Returns a safe error unless the proof remains linked through the
    /// current row-version fence.
    async fn consume(
        &self,
        lease: &AiRunLease,
        adopted: &AiAdoptedSupervisedToolBatch,
    ) -> Result<AiRunLease, AiError>;
}

#[async_trait]
impl AiAgentSupervisedCheckpointControl for OrmAiCoordinatorCheckpointService {
    async fn adopt(
        &self,
        lease: &AiRunLease,
    ) -> Result<Option<AiAdoptedSupervisedToolBatch>, AiError> {
        self.adopt_supervised_tool_batch(lease).await
    }

    async fn consume(
        &self,
        lease: &AiRunLease,
        adopted: &AiAdoptedSupervisedToolBatch,
    ) -> Result<AiRunLease, AiError> {
        self.consume_supervised_before_provider(lease, adopted)
            .await
    }
}

/// Approved-wait execution boundary used by the top-level coordinator.
#[async_trait]
pub trait AiAgentSupervisedResumeExecutor: Send + Sync {
    /// Executes and protects one exact approved claim without provider I/O.
    ///
    /// # Errors
    ///
    /// Returns a safe error before resolver ambiguity for stale or denied
    /// evidence; consequential ambiguity is returned durably in the outcome.
    async fn resume(
        &self,
        claim: &crate::AiApprovedRunClaim,
    ) -> Result<AiSupervisedResumeOutcome, AiError>;
}

#[async_trait]
impl AiAgentSupervisedResumeExecutor for OrmAiSupervisedResumeService {
    async fn resume(
        &self,
        claim: &crate::AiApprovedRunClaim,
    ) -> Result<AiSupervisedResumeOutcome, AiError> {
        self.execute_claimed(claim).await
    }
}

/// Durable result of one supervised coordinator invocation.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AiSupervisedAgentRunOutcome {
    /// Final assistant output and terminal run completion are durable.
    Completed {
        /// Persisted assistant message.
        message_id: Uuid,
        /// Accepted provider-turn count.
        provider_turns: u32,
        /// Completed/requested application-tool count.
        total_tool_calls: u32,
    },
    /// One exact mutation is durably waiting for a human decision.
    WaitingApproval {
        /// Pending approval.
        approval_id: AiApprovalId,
        /// Staged consequential tool call.
        tool_call_id: AiToolCallId,
        /// Accepted provider-turn count.
        provider_turns: u32,
        /// Requested application-tool count.
        total_tool_calls: u32,
    },
    /// A proof/configuration failure was durably classified as safe failure.
    Failed {
        /// Accepted provider-turn count.
        provider_turns: u32,
        /// Completed/requested application-tool count.
        total_tool_calls: u32,
    },
    /// An external boundary was durably closed for privileged recovery.
    RecoveryRequired {
        /// Ambiguous phase.
        phase: AiAgentRecoveryPhase,
        /// Accepted provider-turn count.
        provider_turns: u32,
        /// Completed/requested application-tool count.
        total_tool_calls: u32,
    },
}

/// Top-level coordinator for sequential, human-approved mutations.
///
/// Each provider turn may finish normally or request exactly one supervised
/// mutation. The coordinator checkpoints the provider turn before staging a
/// server-previewed approval and returns without heartbeating the human wait.
/// A later approved claim executes through [`OrmAiSupervisedResumeService`],
/// adopts the exact protected result, consumes it once before transport, and
/// continues. Read-only, mixed, parallel, stateless, autonomous, and
/// model-authored GraphQL tool paths remain closed.
pub struct AiSupervisedAgentCoordinator {
    run_control: Arc<dyn AiAgentRunControl>,
    provider_executor: Arc<dyn AiAgentProviderTurnExecutor>,
    output_writer: Arc<dyn AiAgentProviderOutputWriter>,
    checkpoint_writer: Arc<dyn AiAgentCheckpointWriter>,
    checkpoint_control: Arc<dyn AiAgentSupervisedCheckpointControl>,
    approval_stager: Arc<dyn AiAgentSupervisedApprovalStager>,
    resume_executor: Arc<dyn AiAgentSupervisedResumeExecutor>,
    rule_resolver: Arc<dyn AiAgentRuleResolver>,
    planner: Arc<dyn AiSupervisedAgentTurnPlanner>,
    clock: Arc<dyn Clock>,
    limits: AiSupervisedAgentCoordinatorLimits,
}

impl AiSupervisedAgentCoordinator {
    /// Creates the supervised coordinator from proof-preserving boundaries.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        run_control: Arc<dyn AiAgentRunControl>,
        provider_executor: Arc<dyn AiAgentProviderTurnExecutor>,
        output_writer: Arc<dyn AiAgentProviderOutputWriter>,
        checkpoint_writer: Arc<dyn AiAgentCheckpointWriter>,
        checkpoint_control: Arc<dyn AiAgentSupervisedCheckpointControl>,
        approval_stager: Arc<dyn AiAgentSupervisedApprovalStager>,
        resume_executor: Arc<dyn AiAgentSupervisedResumeExecutor>,
        rule_resolver: Arc<dyn AiAgentRuleResolver>,
        planner: Arc<dyn AiSupervisedAgentTurnPlanner>,
        clock: Arc<dyn Clock>,
        limits: AiSupervisedAgentCoordinatorLimits,
    ) -> Self {
        Self {
            run_control,
            provider_executor,
            output_writer,
            checkpoint_writer,
            checkpoint_control,
            approval_stager,
            resume_executor,
            rule_resolver,
            planner,
            clock,
            limits,
        }
    }

    /// Starts a fresh or checkpoint-requeued claim and advances one turn.
    ///
    /// # Errors
    ///
    /// Returns an error for a stale claim, lost fence, or failure to durably
    /// classify an otherwise safe failure/recovery outcome.
    pub async fn execute_claimed(
        &self,
        claimed: &AiRunLease,
    ) -> Result<AiSupervisedAgentRunOutcome, AiError> {
        let lease = self.run_control.start(claimed).await?;
        if lease.latest_checkpoint_id().is_some() {
            let adopted = match self.checkpoint_control.adopt(&lease).await {
                Ok(Some(adopted)) => adopted,
                Ok(None) | Err(_) => {
                    let guard = AiAgentLoopGuard::new(&lease, self.limits.loop_limits);
                    return self
                        .finish_recovery(
                            &lease,
                            &guard,
                            AiAgentRecoveryPhase::ApplicationTool,
                            "supervised_checkpoint_adoption_failed",
                            None,
                        )
                        .await;
                }
            };
            self.continue_from_adopted(lease, adopted).await
        } else {
            let guard = AiAgentLoopGuard::new(&lease, self.limits.loop_limits);
            let plan = match self.planner.initial_plan(&lease).await {
                Ok(plan) if !plan.is_continuation() => plan,
                _ => {
                    return self
                        .finish_failed(&lease, &guard, "supervised_initial_plan_invalid")
                        .await;
                }
            };
            self.execute_turn(lease, guard, plan, AiRuleRunUsage::default(), None)
                .await
        }
    }

    /// Executes one exact approved claim and advances its next provider turn.
    ///
    /// The mutation is executed and checkpointed before any provider call. A
    /// recovery-required mutation outcome is returned without replay.
    ///
    /// # Errors
    ///
    /// Returns a safe error for stale/denied pre-execution evidence, lost
    /// fencing, or failure to persist a terminal classification.
    pub async fn execute_approved_claim(
        &self,
        claim: &crate::AiApprovedRunClaim,
    ) -> Result<AiSupervisedAgentRunOutcome, AiError> {
        let resumed = self.resume_executor.resume(claim).await?;
        if let AiSupervisedResumeOutcome::RecoveryRequired {
            provider_turns,
            total_tool_calls,
            ..
        } = resumed
        {
            return Ok(AiSupervisedAgentRunOutcome::RecoveryRequired {
                phase: AiAgentRecoveryPhase::ApplicationTool,
                provider_turns,
                total_tool_calls,
            });
        }
        let checkpointed = resumed.checkpointed().ok_or(AiError::Conflict)?;
        let lease = checkpointed.lease().clone();
        let adopted = match self.checkpoint_control.adopt(&lease).await {
            Ok(Some(adopted)) => adopted,
            Ok(None) | Err(_) => {
                return self
                    .finish_recovery_counts(
                        &lease,
                        AiAgentRecoveryPhase::ApplicationTool,
                        "supervised_checkpoint_adoption_failed",
                        None,
                        checkpointed.provider_turns(),
                        checkpointed.total_tool_calls(),
                    )
                    .await;
            }
        };
        self.continue_from_adopted(lease, adopted).await
    }

    async fn continue_from_adopted(
        &self,
        lease: AiRunLease,
        adopted: AiAdoptedSupervisedToolBatch,
    ) -> Result<AiSupervisedAgentRunOutcome, AiError> {
        let reference = adopted.continuation().chain_reference()?;
        let guard = AiAgentLoopGuard::resume_after_tool_batch(
            &lease,
            self.limits.loop_limits,
            adopted.provider_turns(),
            adopted.total_tool_calls(),
            &reference,
        )?;
        let plan = match self
            .planner
            .continuation_plan(
                &lease,
                adopted.provider_turns(),
                adopted.continuation().clone(),
            )
            .await
        {
            Ok(plan)
                if plan.is_continuation()
                    && plan.provider_call.scope() == adopted.scope()
                    && plan.rules.fingerprint() == adopted.rule_fingerprint() =>
            {
                plan
            }
            _ => {
                return self
                    .finish_failed(&lease, &guard, "supervised_continuation_plan_invalid")
                    .await;
            }
        };
        let usage = adopted.rule_usage();
        self.execute_turn(lease, guard, plan, usage, Some(&adopted))
            .await
    }

    async fn execute_turn(
        &self,
        mut lease: AiRunLease,
        mut guard: AiAgentLoopGuard,
        plan: AiSupervisedAgentTurnPlan,
        mut rule_usage: AiRuleRunUsage,
        adopted: Option<&AiAdoptedSupervisedToolBatch>,
    ) -> Result<AiSupervisedAgentRunOutcome, AiError> {
        if !guard.can_begin_provider_turn() {
            return self
                .finish_failed(&lease, &guard, "supervised_provider_turn_limit_reached")
                .await;
        }
        let (provider_plan, scope, correlation_id, route, planned_rules, uses_byok) =
            plan.into_parts();
        let resolution = match self.rule_resolver.resolve_rules(&lease, &scope).await {
            Ok(resolution) if resolution.rules().fingerprint() == planned_rules.fingerprint() => {
                resolution
            }
            _ => {
                return self
                    .finish_failed(&lease, &guard, "supervised_rule_plan_stale")
                    .await;
            }
        };
        let started_usage = match rule_usage.validate(&resolution) {
            Ok(usage) => usage,
            Err(_) => {
                return self
                    .finish_failed(&lease, &guard, "supervised_rule_duration_exceeded")
                    .await;
            }
        };
        if provider_plan
            .project_supervised_rule_usage(&resolution, started_usage, uses_byok)
            .is_err()
        {
            return self
                .finish_failed(&lease, &guard, "supervised_rule_plan_denied")
                .await;
        }
        rule_usage = started_usage;
        if let Some(adopted) = adopted {
            lease = match self.checkpoint_control.consume(&lease, adopted).await {
                Ok(renewed) => renewed,
                Err(_) => {
                    return self
                        .finish_recovery(
                            &lease,
                            &guard,
                            AiAgentRecoveryPhase::ApplicationTool,
                            "supervised_checkpoint_consumption_failed",
                            None,
                        )
                        .await;
                }
            };
            let final_rules = match self.rule_resolver.resolve_rules(&lease, &scope).await {
                Ok(current)
                    if current.rules().fingerprint() == planned_rules.fingerprint()
                        && rule_usage.validate(&current).is_ok() =>
                {
                    current
                }
                _ => {
                    return self
                        .finish_failed(&lease, &guard, "supervised_rule_changed_after_consume")
                        .await;
                }
            };
            if provider_plan
                .project_supervised_rule_usage(&final_rules, rule_usage, uses_byok)
                .is_err()
            {
                return self
                    .finish_failed(&lease, &guard, "supervised_rule_denied_after_consume")
                    .await;
            }
        }
        let result = match self
            .execute_provider_with_heartbeats(&mut lease, provider_plan)
            .await
        {
            Ok(result) => result,
            Err(SupervisedProviderTurnFailure::Provider) => {
                return self
                    .finish_recovery(
                        &lease,
                        &guard,
                        AiAgentRecoveryPhase::ProviderTurn,
                        "supervised_provider_turn_uncertain",
                        None,
                    )
                    .await;
            }
            Err(SupervisedProviderTurnFailure::LeaseLost(error)) => return Err(error),
        };
        let observed = match guard.observe_provider_turn(&result) {
            Ok(observed) => observed,
            Err(_) => {
                return self
                    .finish_recovery(
                        &lease,
                        &guard,
                        AiAgentRecoveryPhase::ProviderTurn,
                        "supervised_provider_turn_binding_failed",
                        result.provider_response_id(),
                    )
                    .await;
            }
        };
        let current_rules = match self.rule_resolver.resolve_rules(&lease, &scope).await {
            Ok(current) if current.rules().fingerprint() == planned_rules.fingerprint() => current,
            _ => {
                return self
                    .finish_recovery(
                        &lease,
                        &guard,
                        AiAgentRecoveryPhase::ProviderTurn,
                        "supervised_rule_changed_after_provider",
                        result.provider_response_id(),
                    )
                    .await;
            }
        };
        rule_usage = match rule_usage.accept_provider(result.usage(), &current_rules) {
            Ok(usage) => usage,
            Err(_) => {
                return self
                    .finish_recovery(
                        &lease,
                        &guard,
                        AiAgentRecoveryPhase::ProviderTurn,
                        "supervised_rule_budget_exceeded",
                        result.provider_response_id(),
                    )
                    .await;
            }
        };
        lease = match self
            .checkpoint_writer
            .persist_provider_turn(
                &lease,
                &result,
                &scope,
                &correlation_id,
                &route,
                &planned_rules,
                rule_usage,
                guard.provider_turns(),
                guard.total_tool_calls(),
            )
            .await
        {
            Ok(renewed) => renewed,
            Err(_) => {
                return self
                    .finish_recovery(
                        &lease,
                        &guard,
                        AiAgentRecoveryPhase::ProviderTurn,
                        "supervised_provider_checkpoint_uncertain",
                        result.provider_response_id(),
                    )
                    .await;
            }
        };
        match observed {
            AiAgentLoopTurn::Completed => {
                let persisted = match self.output_writer.persist_output(&lease, &result).await {
                    Ok(persisted) => persisted,
                    Err(_) => {
                        return self
                            .finish_recovery(
                                &lease,
                                &guard,
                                AiAgentRecoveryPhase::ProviderOutput,
                                "supervised_output_uncertain",
                                result.provider_response_id(),
                            )
                            .await;
                    }
                };
                let message_id = persisted.message_id();
                lease = persisted.into_lease();
                let completion = AiRunCompletion::new(
                    AiRunState::Completed,
                    "supervised_agent_completed",
                    None,
                    result.provider_response_id().map(str::to_owned),
                )?;
                self.run_control.finish(&lease, completion).await?;
                Ok(AiSupervisedAgentRunOutcome::Completed {
                    message_id,
                    provider_turns: guard.provider_turns(),
                    total_tool_calls: guard.total_tool_calls(),
                })
            }
            AiAgentLoopTurn::ToolCalls {
                provider_turn_index,
                call_count,
            } => {
                if call_count != 1 || result.provider_response_id().is_none() {
                    return self
                        .finish_failed(&lease, &guard, "supervised_tool_batch_unsupported")
                        .await;
                }
                if !guard.has_provider_turn_capacity() {
                    return self
                        .finish_failed(&lease, &guard, "supervised_continuation_limit_reached")
                        .await;
                }
                let rules = match self.rule_resolver.resolve_rules(&lease, &scope).await {
                    Ok(current)
                        if current.rules().fingerprint() == planned_rules.fingerprint()
                            && current.rules().constrain_tool(
                                result.tool_calls()[0].tool_fingerprint(),
                                crate::ToolMaturity::SupervisedWrite,
                                crate::AiApprovalRule::OneShot,
                            ) == Some(crate::AiApprovalRule::OneShot) =>
                    {
                        current
                    }
                    _ => {
                        return self
                            .finish_failed(&lease, &guard, "supervised_tool_rule_denied")
                            .await;
                    }
                };
                if rule_usage.accept_tool_calls(1, &rules).is_err() {
                    return self
                        .finish_failed(&lease, &guard, "supervised_rule_steps_exceeded")
                        .await;
                }
                let context = AiApplicationToolCallContext::new(
                    provider_turn_index,
                    0,
                    scope,
                    correlation_id,
                    result.budget_reservation_id().0.to_string(),
                )?;
                let expires_at = self
                    .clock
                    .now()
                    .checked_add(self.limits.approval_ttl)
                    .ok_or(AiError::PersistenceFailed)?;
                let wait = match self
                    .approval_stager
                    .stage(
                        &lease,
                        &result,
                        context,
                        expires_at,
                        self.limits.recent_mfa_required,
                    )
                    .await
                {
                    Ok(wait) => wait,
                    Err(_) => {
                        return self
                            .finish_recovery(
                                &lease,
                                &guard,
                                AiAgentRecoveryPhase::ApplicationTool,
                                "supervised_approval_staging_uncertain",
                                result.provider_response_id(),
                            )
                            .await;
                    }
                };
                if wait.lease().state() != AiRunState::WaitingApproval {
                    return Err(AiError::Conflict);
                }
                Ok(AiSupervisedAgentRunOutcome::WaitingApproval {
                    approval_id: wait.approval_id(),
                    tool_call_id: wait.tool_call_id(),
                    provider_turns: guard.provider_turns(),
                    total_tool_calls: guard.total_tool_calls(),
                })
            }
        }
    }

    async fn execute_provider_with_heartbeats(
        &self,
        lease: &mut AiRunLease,
        plan: AiProviderCallPlan,
    ) -> Result<crate::AiProviderCallResult, SupervisedProviderTurnFailure> {
        let provider_lease = lease.clone();
        let provider = self.provider_executor.execute_turn(&provider_lease, plan);
        tokio::pin!(provider);
        let delay = self.limits.heartbeat_interval.unsigned_abs();
        loop {
            let heartbeat = tokio::time::sleep(delay);
            tokio::pin!(heartbeat);
            tokio::select! {
                result = &mut provider => {
                    return result.map_err(|_| SupervisedProviderTurnFailure::Provider);
                }
                () = &mut heartbeat => {
                    *lease = self
                        .run_control
                        .heartbeat(lease)
                        .await
                        .map_err(SupervisedProviderTurnFailure::LeaseLost)?;
                }
            }
        }
    }

    async fn finish_failed(
        &self,
        lease: &AiRunLease,
        guard: &AiAgentLoopGuard,
        code: &str,
    ) -> Result<AiSupervisedAgentRunOutcome, AiError> {
        let completion =
            AiRunCompletion::new(AiRunState::Failed, code, Some(code.to_owned()), None)?;
        self.run_control.finish(lease, completion).await?;
        Ok(AiSupervisedAgentRunOutcome::Failed {
            provider_turns: guard.provider_turns(),
            total_tool_calls: guard.total_tool_calls(),
        })
    }

    async fn finish_recovery(
        &self,
        lease: &AiRunLease,
        guard: &AiAgentLoopGuard,
        phase: AiAgentRecoveryPhase,
        code: &str,
        provider_response_id: Option<&str>,
    ) -> Result<AiSupervisedAgentRunOutcome, AiError> {
        self.finish_recovery_counts(
            lease,
            phase,
            code,
            provider_response_id,
            guard.provider_turns(),
            guard.total_tool_calls(),
        )
        .await
    }

    async fn finish_recovery_counts(
        &self,
        lease: &AiRunLease,
        phase: AiAgentRecoveryPhase,
        code: &str,
        provider_response_id: Option<&str>,
        provider_turns: u32,
        total_tool_calls: u32,
    ) -> Result<AiSupervisedAgentRunOutcome, AiError> {
        let completion = AiRunCompletion::new(
            AiRunState::RecoveryRequired,
            code,
            Some(code.to_owned()),
            provider_response_id.map(str::to_owned),
        )?;
        self.run_control.finish(lease, completion).await?;
        Ok(AiSupervisedAgentRunOutcome::RecoveryRequired {
            phase,
            provider_turns,
            total_tool_calls,
        })
    }
}

enum SupervisedProviderTurnFailure {
    Provider,
    LeaseLost(AiError),
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use agql_auth::{
        AccessTokenMetadata, AuthPrincipal, AuthUser, FixedClock, PrincipalReference,
        SessionContext,
    };
    use serde_json::json;

    use super::*;
    use crate::{
        AiAgentContinuation, AiAgentRuleResolution, AiDataSourceRef, AiDestinationTrust,
        AiEgressCapability, AiEgressManifest, AiPersistedApplicationToolCall,
        AiPersistedProviderOutput, AiRuleApprovalRequirement, AiRuleBudgetCeilings,
        AiRuleConstraints, AiSourceTrust, DataClassification, ModelContinuation, ToolMaturity,
    };

    struct TestRunControl {
        finishes: Mutex<Vec<AiRunState>>,
    }

    impl TestRunControl {
        fn new() -> Self {
            Self {
                finishes: Mutex::new(Vec::new()),
            }
        }

        fn final_states(&self) -> Vec<AiRunState> {
            self.finishes
                .lock()
                .expect("test finish lock should not be poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl AiAgentRunControl for TestRunControl {
        async fn start(&self, lease: &AiRunLease) -> Result<AiRunLease, AiError> {
            Ok(lease.clone())
        }

        async fn heartbeat(&self, lease: &AiRunLease) -> Result<AiRunLease, AiError> {
            Ok(lease.clone())
        }

        async fn finish(
            &self,
            _lease: &AiRunLease,
            completion: AiRunCompletion,
        ) -> Result<(), AiError> {
            self.finishes
                .lock()
                .expect("test finish lock should not be poisoned")
                .push(completion.final_state());
            Ok(())
        }
    }

    struct TestProviderExecutor {
        responses: Mutex<VecDeque<Result<crate::AiProviderCallResult, AiError>>>,
        require_checkpoint_cleared: bool,
        calls: AtomicUsize,
    }

    impl TestProviderExecutor {
        fn remaining_responses(&self) -> usize {
            self.responses
                .lock()
                .expect("test provider lock should not be poisoned")
                .len()
        }
    }

    #[async_trait]
    impl AiAgentProviderTurnExecutor for TestProviderExecutor {
        async fn execute_turn(
            &self,
            lease: &AiRunLease,
            _plan: AiProviderCallPlan,
        ) -> Result<crate::AiProviderCallResult, AiError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.require_checkpoint_cleared && lease.latest_checkpoint_id().is_some() {
                return Err(AiError::Conflict);
            }
            self.responses
                .lock()
                .expect("test provider lock should not be poisoned")
                .pop_front()
                .expect("test provider response should exist")
        }
    }

    struct TestPlanner {
        scope: AiScope,
        route: AiToolResultEgressRoute,
        continuation_count: AtomicUsize,
    }

    #[async_trait]
    impl AiSupervisedAgentTurnPlanner for TestPlanner {
        async fn initial_plan(
            &self,
            lease: &AiRunLease,
        ) -> Result<AiSupervisedAgentTurnPlan, AiError> {
            AiSupervisedAgentTurnPlan::new(
                AiProviderCallPlan::test_supervised_plan(lease, self.scope.clone(), false),
                self.route.clone(),
                test_rules(self.scope.clone()),
                false,
            )
        }

        async fn continuation_plan(
            &self,
            lease: &AiRunLease,
            _provider_turns: u32,
            _continuation: AiAgentContinuation,
        ) -> Result<AiSupervisedAgentTurnPlan, AiError> {
            self.continuation_count.fetch_add(1, Ordering::SeqCst);
            AiSupervisedAgentTurnPlan::new(
                AiProviderCallPlan::test_supervised_plan(lease, self.scope.clone(), true),
                self.route.clone(),
                test_rules(self.scope.clone()),
                false,
            )
        }
    }

    struct TestOutputWriter;

    #[async_trait]
    impl AiAgentProviderOutputWriter for TestOutputWriter {
        async fn persist_output(
            &self,
            lease: &AiRunLease,
            _result: &crate::AiProviderCallResult,
        ) -> Result<AiPersistedProviderOutput, AiError> {
            Ok(AiPersistedProviderOutput::test_output(lease.clone()))
        }
    }

    struct TestCheckpointWriter {
        provider_checkpoints: AtomicUsize,
    }

    #[async_trait]
    impl AiAgentCheckpointWriter for TestCheckpointWriter {
        async fn persist_provider_turn(
            &self,
            lease: &AiRunLease,
            result: &crate::AiProviderCallResult,
            _scope: &AiScope,
            _correlation_id: &str,
            _route: &AiToolResultEgressRoute,
            _rules: &AiResolvedRuleSet,
            _rule_usage: AiRuleRunUsage,
            provider_turns: u32,
            _total_tool_calls: u32,
        ) -> Result<AiRunLease, AiError> {
            if provider_turns == 0 || result.run_id() != lease.run_id() {
                return Err(AiError::Conflict);
            }
            self.provider_checkpoints.fetch_add(1, Ordering::SeqCst);
            Ok(lease.test_with_checkpoint(Uuid::new_v4()))
        }

        async fn persist_tool_batch(
            &self,
            _lease: &AiRunLease,
            _result: &crate::AiProviderCallResult,
            _completed_tools: &[AiPersistedApplicationToolCall],
            _continuation: &AiAgentContinuation,
            _scope: &AiScope,
            _correlation_id: &str,
            _route: &AiToolResultEgressRoute,
            _rules: &AiResolvedRuleSet,
            _rule_usage: AiRuleRunUsage,
            _provider_turns: u32,
            _total_tool_calls: u32,
        ) -> Result<AiRunLease, AiError> {
            Err(AiError::Conflict)
        }
    }

    struct TestCheckpointControl {
        adopted: Mutex<Option<AiAdoptedSupervisedToolBatch>>,
        consumed: AtomicBool,
    }

    #[async_trait]
    impl AiAgentSupervisedCheckpointControl for TestCheckpointControl {
        async fn adopt(
            &self,
            lease: &AiRunLease,
        ) -> Result<Option<AiAdoptedSupervisedToolBatch>, AiError> {
            let adopted = self
                .adopted
                .lock()
                .expect("test adoption lock should not be poisoned")
                .take();
            if adopted
                .as_ref()
                .map(AiAdoptedSupervisedToolBatch::checkpoint_id)
                != lease.latest_checkpoint_id()
            {
                return Err(AiError::Conflict);
            }
            Ok(adopted)
        }

        async fn consume(
            &self,
            lease: &AiRunLease,
            adopted: &AiAdoptedSupervisedToolBatch,
        ) -> Result<AiRunLease, AiError> {
            if lease.latest_checkpoint_id() != Some(adopted.checkpoint_id())
                || self.consumed.swap(true, Ordering::SeqCst)
            {
                return Err(AiError::Conflict);
            }
            Ok(lease.test_without_checkpoint())
        }
    }

    struct TestApprovalStager {
        calls: AtomicUsize,
        saw_checkpoint: AtomicBool,
    }

    #[async_trait]
    impl AiAgentSupervisedApprovalStager for TestApprovalStager {
        async fn stage(
            &self,
            lease: &AiRunLease,
            result: &crate::AiProviderCallResult,
            _context: AiApplicationToolCallContext,
            _expires_at: time::OffsetDateTime,
            _recent_mfa_required: bool,
        ) -> Result<AiSupervisedApprovalWait, AiError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if lease.latest_checkpoint_id().is_none() || result.tool_calls().len() != 1 {
                return Err(AiError::Conflict);
            }
            self.saw_checkpoint.store(true, Ordering::SeqCst);
            Ok(AiSupervisedApprovalWait {
                approval_id: AiApprovalId::new(),
                tool_call_id: AiToolCallId::new(),
                lease: lease.test_with_state(AiRunState::WaitingApproval),
            })
        }
    }

    struct TestResumeExecutor {
        outcome: Mutex<Option<AiSupervisedResumeOutcome>>,
    }

    #[async_trait]
    impl AiAgentSupervisedResumeExecutor for TestResumeExecutor {
        async fn resume(
            &self,
            _claim: &crate::AiApprovedRunClaim,
        ) -> Result<AiSupervisedResumeOutcome, AiError> {
            self.outcome
                .lock()
                .expect("test resume lock should not be poisoned")
                .take()
                .ok_or(AiError::Conflict)
        }
    }

    struct TestRuleResolver;

    #[async_trait]
    impl AiAgentRuleResolver for TestRuleResolver {
        async fn resolve_rules(
            &self,
            _lease: &AiRunLease,
            scope: &AiScope,
        ) -> Result<AiAgentRuleResolution, AiError> {
            AiAgentRuleResolution::new(test_rules(scope.clone()), time::OffsetDateTime::now_utc())
        }
    }

    struct ChangingRuleResolver(AtomicUsize);

    #[async_trait]
    impl AiAgentRuleResolver for ChangingRuleResolver {
        async fn resolve_rules(
            &self,
            _lease: &AiRunLease,
            scope: &AiScope,
        ) -> Result<AiAgentRuleResolution, AiError> {
            let call = self.0.fetch_add(1, Ordering::SeqCst);
            AiAgentRuleResolution::new(
                test_rules_with_fingerprint(scope.clone(), if call == 0 { '1' } else { '4' }),
                time::OffsetDateTime::now_utc(),
            )
        }
    }

    fn principal_reference() -> PrincipalReference {
        AuthPrincipal::User(AuthUser {
            user_id: "supervised-coordinator-user".to_owned(),
            session_id: Uuid::new_v4(),
            roles: Vec::new(),
            scopes: Vec::new(),
            session: SessionContext::default(),
            token_claims: AccessTokenMetadata {
                tenant_id: Some("supervised-coordinator-tenant".to_owned()),
                ..AccessTokenMetadata::default()
            },
        })
        .reference()
    }

    fn test_scope() -> AiScope {
        AiScope::new("test", "supervised-coordinator-scope")
            .with_tenant_id("supervised-coordinator-tenant")
    }

    fn test_rules(scope: AiScope) -> AiResolvedRuleSet {
        test_rules_with_fingerprint(scope, '1')
    }

    fn test_rules_with_fingerprint(scope: AiScope, fingerprint: char) -> AiResolvedRuleSet {
        AiResolvedRuleSet::new(
            scope,
            AiRuleConstraints {
                enabled: true,
                maximum_classification: DataClassification::Restricted,
                maximum_tool_maturity: ToolMaturity::SupervisedWrite,
                approval_requirement: AiRuleApprovalRequirement::DescriptorPolicy,
                allowed_tool_fingerprints: None,
                allowed_provider_kinds: None,
                allowed_provider_capabilities: None,
                allow_provider_retention: true,
                allow_byok: true,
                budget: AiRuleBudgetCeilings {
                    maximum_steps: Some(16),
                    maximum_duration_seconds: Some(3_600),
                    maximum_output_tokens: Some(16_000),
                    maximum_cost_microunits: Some(10_000_000),
                    maximum_provider_calls: Some(8),
                    maximum_tool_units: Some(100),
                    maximum_image_units: Some(100),
                },
            },
            Vec::new(),
            fingerprint.to_string().repeat(64),
        )
    }

    fn test_route() -> AiToolResultEgressRoute {
        AiToolResultEgressRoute::new(
            "test-provider-profile",
            "test-provider-boundary",
            AiDestinationTrust::ExternalProcessor,
            "supervised-agent-test",
            "none",
            "egress-v1",
        )
        .expect("test route should validate")
    }

    fn test_manifest(lease: &AiRunLease) -> AiEgressManifest {
        AiEgressManifest {
            provider_profile_id: "test-provider-profile".to_owned(),
            provider_kind: "openai".to_owned(),
            model: "coordinator-test-model".to_owned(),
            destination: "test-provider-boundary".to_owned(),
            destination_trust: AiDestinationTrust::ExternalProcessor,
            capability: AiEgressCapability::ToolResult,
            scope: test_scope(),
            session_id: Some(lease.session_id()),
            run_id: Some(lease.run_id()),
            sources: vec![AiDataSourceRef {
                kind: "application_tool_result".to_owned(),
                reference: Uuid::new_v4().to_string(),
                classification: DataClassification::Public,
                trust: AiSourceTrust::ResolverResult,
            }],
            estimated_bytes: 64,
            estimated_tokens: 0,
            attachment_count: 0,
            purpose: "supervised-agent-test".to_owned(),
            retention: "none".to_owned(),
            residency: None,
            policy_version: "egress-v1".to_owned(),
            consent_reference: None,
        }
    }

    fn adopted_checkpoint(lease: &AiRunLease, checkpoint_id: Uuid) -> AiAdoptedSupervisedToolBatch {
        let old_result = crate::AiProviderCallResult::test_result(
            lease,
            None,
            "test-previous-response",
            vec![("test-call", "test.write", json!({"value": "approved"}))],
        );
        let resolution =
            AiAgentRuleResolution::new(test_rules(test_scope()), time::OffsetDateTime::now_utc())
                .expect("test rules should resolve");
        let usage = AiRuleRunUsage::default()
            .accept_provider(old_result.usage(), &resolution)
            .and_then(|usage| usage.accept_tool_calls(1, &resolution))
            .expect("adopted usage should fit test rules");
        let completed = AiPersistedApplicationToolCall::test_completed(
            lease.clone(),
            "test-call",
            "test.write",
            Some(json!({"updated": true})),
            Some(test_manifest(lease)),
        );
        let continuation = AiAgentContinuation::from_persisted_results(
            ModelContinuation::ProviderResponse {
                response_id: "test-previous-response".to_owned(),
            },
            &[completed],
            Vec::new(),
        )
        .expect("test continuation should bind");
        AiAdoptedSupervisedToolBatch::test_adopted(
            checkpoint_id,
            1,
            1,
            test_scope(),
            continuation,
            resolution.rules().fingerprint().to_owned(),
            usage,
        )
    }

    fn limits() -> AiSupervisedAgentCoordinatorLimits {
        limits_with_provider_turns(4)
    }

    fn limits_with_provider_turns(
        maximum_provider_turns: u32,
    ) -> AiSupervisedAgentCoordinatorLimits {
        AiSupervisedAgentCoordinatorLimits::new(
            AiAgentLoopLimits::new(maximum_provider_turns, 8)
                .expect("test loop limits should validate"),
            Duration::milliseconds(50),
            Duration::minutes(10),
            true,
        )
        .expect("test supervised limits should validate")
    }

    fn unused_resume() -> Arc<TestResumeExecutor> {
        Arc::new(TestResumeExecutor {
            outcome: Mutex::new(Some(AiSupervisedResumeOutcome::RecoveryRequired {
                tool_call_id: AiToolCallId::new(),
                provider_turns: 0,
                total_tool_calls: 0,
            })),
        })
    }

    #[tokio::test]
    async fn provider_turn_is_checkpointed_before_one_approval_is_staged() {
        let lease = AiRunLease::test_running(principal_reference());
        let run = Arc::new(TestRunControl::new());
        let provider = Arc::new(TestProviderExecutor {
            responses: Mutex::new(VecDeque::from([Ok(
                crate::AiProviderCallResult::test_result(
                    &lease,
                    None,
                    "supervised-response",
                    vec![("write-call", "test.write", json!({"value": 7}))],
                ),
            )])),
            require_checkpoint_cleared: false,
            calls: AtomicUsize::new(0),
        });
        let checkpoints = Arc::new(TestCheckpointWriter {
            provider_checkpoints: AtomicUsize::new(0),
        });
        let stager = Arc::new(TestApprovalStager {
            calls: AtomicUsize::new(0),
            saw_checkpoint: AtomicBool::new(false),
        });
        let coordinator = AiSupervisedAgentCoordinator::new(
            run.clone(),
            provider,
            Arc::new(TestOutputWriter),
            checkpoints.clone(),
            Arc::new(TestCheckpointControl {
                adopted: Mutex::new(None),
                consumed: AtomicBool::new(false),
            }),
            stager.clone(),
            unused_resume(),
            Arc::new(TestRuleResolver),
            Arc::new(TestPlanner {
                scope: test_scope(),
                route: test_route(),
                continuation_count: AtomicUsize::new(0),
            }),
            Arc::new(FixedClock::new(time::OffsetDateTime::now_utc())),
            limits(),
        );

        let outcome = coordinator
            .execute_claimed(&lease)
            .await
            .expect("supervised turn should wait for approval");

        assert!(matches!(
            outcome,
            AiSupervisedAgentRunOutcome::WaitingApproval {
                provider_turns: 1,
                total_tool_calls: 1,
                ..
            }
        ));
        assert_eq!(checkpoints.provider_checkpoints.load(Ordering::SeqCst), 1);
        assert_eq!(stager.calls.load(Ordering::SeqCst), 1);
        assert!(stager.saw_checkpoint.load(Ordering::SeqCst));
        assert!(run.final_states().is_empty());
    }

    #[tokio::test]
    async fn parallel_supervised_calls_fail_without_staging_any_approval() {
        let lease = AiRunLease::test_running(principal_reference());
        let run = Arc::new(TestRunControl::new());
        let provider = Arc::new(TestProviderExecutor {
            responses: Mutex::new(VecDeque::from([Ok(
                crate::AiProviderCallResult::test_result(
                    &lease,
                    None,
                    "parallel-response",
                    vec![
                        ("write-call-1", "test.write", json!({"value": 1})),
                        ("write-call-2", "test.write", json!({"value": 2})),
                    ],
                ),
            )])),
            require_checkpoint_cleared: false,
            calls: AtomicUsize::new(0),
        });
        let stager = Arc::new(TestApprovalStager {
            calls: AtomicUsize::new(0),
            saw_checkpoint: AtomicBool::new(false),
        });
        let coordinator = AiSupervisedAgentCoordinator::new(
            run.clone(),
            provider,
            Arc::new(TestOutputWriter),
            Arc::new(TestCheckpointWriter {
                provider_checkpoints: AtomicUsize::new(0),
            }),
            Arc::new(TestCheckpointControl {
                adopted: Mutex::new(None),
                consumed: AtomicBool::new(false),
            }),
            stager.clone(),
            unused_resume(),
            Arc::new(TestRuleResolver),
            Arc::new(TestPlanner {
                scope: test_scope(),
                route: test_route(),
                continuation_count: AtomicUsize::new(0),
            }),
            Arc::new(FixedClock::new(time::OffsetDateTime::now_utc())),
            limits(),
        );

        let outcome = coordinator
            .execute_claimed(&lease)
            .await
            .expect("parallel mutation request should close safely");

        assert_eq!(
            outcome,
            AiSupervisedAgentRunOutcome::Failed {
                provider_turns: 1,
                total_tool_calls: 2,
            }
        );
        assert_eq!(stager.calls.load(Ordering::SeqCst), 0);
        assert_eq!(run.final_states(), vec![AiRunState::Failed]);
    }

    #[tokio::test]
    async fn final_allowed_provider_turn_cannot_stage_an_unfinishable_mutation() {
        let lease = AiRunLease::test_running(principal_reference());
        let run = Arc::new(TestRunControl::new());
        let provider = Arc::new(TestProviderExecutor {
            responses: Mutex::new(VecDeque::from([Ok(
                crate::AiProviderCallResult::test_result(
                    &lease,
                    None,
                    "last-allowed-response",
                    vec![("write-call", "test.write", json!({"value": 7}))],
                ),
            )])),
            require_checkpoint_cleared: false,
            calls: AtomicUsize::new(0),
        });
        let stager = Arc::new(TestApprovalStager {
            calls: AtomicUsize::new(0),
            saw_checkpoint: AtomicBool::new(false),
        });
        let coordinator = AiSupervisedAgentCoordinator::new(
            run.clone(),
            provider,
            Arc::new(TestOutputWriter),
            Arc::new(TestCheckpointWriter {
                provider_checkpoints: AtomicUsize::new(0),
            }),
            Arc::new(TestCheckpointControl {
                adopted: Mutex::new(None),
                consumed: AtomicBool::new(false),
            }),
            stager.clone(),
            unused_resume(),
            Arc::new(TestRuleResolver),
            Arc::new(TestPlanner {
                scope: test_scope(),
                route: test_route(),
                continuation_count: AtomicUsize::new(0),
            }),
            Arc::new(FixedClock::new(time::OffsetDateTime::now_utc())),
            limits_with_provider_turns(1),
        );

        let outcome = coordinator
            .execute_claimed(&lease)
            .await
            .expect("unfinishable mutation should close without human staging");

        assert_eq!(
            outcome,
            AiSupervisedAgentRunOutcome::Failed {
                provider_turns: 1,
                total_tool_calls: 1,
            }
        );
        assert_eq!(stager.calls.load(Ordering::SeqCst), 0);
        assert_eq!(run.final_states(), vec![AiRunState::Failed]);
    }

    #[tokio::test]
    async fn adopted_mutation_result_is_consumed_before_the_next_provider_turn() {
        let base = AiRunLease::test_running(principal_reference());
        let checkpoint_id = Uuid::new_v4();
        let claimed = base.test_with_checkpoint(checkpoint_id);
        let control = Arc::new(TestCheckpointControl {
            adopted: Mutex::new(Some(adopted_checkpoint(&base, checkpoint_id))),
            consumed: AtomicBool::new(false),
        });
        let provider = Arc::new(TestProviderExecutor {
            responses: Mutex::new(VecDeque::from([Ok(
                crate::AiProviderCallResult::test_result(
                    &claimed,
                    Some("test-previous-response".to_owned()),
                    "supervised-final-response",
                    Vec::new(),
                ),
            )])),
            require_checkpoint_cleared: true,
            calls: AtomicUsize::new(0),
        });
        let run = Arc::new(TestRunControl::new());
        let planner = Arc::new(TestPlanner {
            scope: test_scope(),
            route: test_route(),
            continuation_count: AtomicUsize::new(0),
        });
        let coordinator = AiSupervisedAgentCoordinator::new(
            run.clone(),
            provider.clone(),
            Arc::new(TestOutputWriter),
            Arc::new(TestCheckpointWriter {
                provider_checkpoints: AtomicUsize::new(0),
            }),
            control.clone(),
            Arc::new(TestApprovalStager {
                calls: AtomicUsize::new(0),
                saw_checkpoint: AtomicBool::new(false),
            }),
            unused_resume(),
            Arc::new(TestRuleResolver),
            planner.clone(),
            Arc::new(FixedClock::new(time::OffsetDateTime::now_utc())),
            limits(),
        );

        let outcome = coordinator
            .execute_claimed(&claimed)
            .await
            .expect("adopted supervised result should continue once");

        assert!(matches!(
            outcome,
            AiSupervisedAgentRunOutcome::Completed {
                provider_turns: 2,
                total_tool_calls: 1,
                ..
            }
        ));
        assert!(control.consumed.load(Ordering::SeqCst));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(planner.continuation_count.load(Ordering::SeqCst), 1);
        assert_eq!(run.final_states(), vec![AiRunState::Completed]);
    }

    #[tokio::test]
    async fn rule_change_after_checkpoint_consumption_blocks_provider_transport() {
        let base = AiRunLease::test_running(principal_reference());
        let checkpoint_id = Uuid::new_v4();
        let claimed = base.test_with_checkpoint(checkpoint_id);
        let control = Arc::new(TestCheckpointControl {
            adopted: Mutex::new(Some(adopted_checkpoint(&base, checkpoint_id))),
            consumed: AtomicBool::new(false),
        });
        let provider = Arc::new(TestProviderExecutor {
            responses: Mutex::new(VecDeque::from([Ok(
                crate::AiProviderCallResult::test_result(
                    &claimed,
                    Some("test-previous-response".to_owned()),
                    "must-not-run",
                    Vec::new(),
                ),
            )])),
            require_checkpoint_cleared: true,
            calls: AtomicUsize::new(0),
        });
        let run = Arc::new(TestRunControl::new());
        let coordinator = AiSupervisedAgentCoordinator::new(
            run.clone(),
            provider.clone(),
            Arc::new(TestOutputWriter),
            Arc::new(TestCheckpointWriter {
                provider_checkpoints: AtomicUsize::new(0),
            }),
            control.clone(),
            Arc::new(TestApprovalStager {
                calls: AtomicUsize::new(0),
                saw_checkpoint: AtomicBool::new(false),
            }),
            unused_resume(),
            Arc::new(ChangingRuleResolver(AtomicUsize::new(0))),
            Arc::new(TestPlanner {
                scope: test_scope(),
                route: test_route(),
                continuation_count: AtomicUsize::new(0),
            }),
            Arc::new(FixedClock::new(time::OffsetDateTime::now_utc())),
            limits(),
        );

        let outcome = coordinator
            .execute_claimed(&claimed)
            .await
            .expect("changed rule should close without provider transport");

        assert_eq!(
            outcome,
            AiSupervisedAgentRunOutcome::Failed {
                provider_turns: 1,
                total_tool_calls: 1,
            }
        );
        assert!(control.consumed.load(Ordering::SeqCst));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        assert_eq!(provider.remaining_responses(), 1);
        assert_eq!(run.final_states(), vec![AiRunState::Failed]);
    }

    #[tokio::test]
    async fn exhausted_turn_limit_preserves_the_unconsumed_checkpoint() {
        let base = AiRunLease::test_running(principal_reference());
        let checkpoint_id = Uuid::new_v4();
        let claimed = base.test_with_checkpoint(checkpoint_id);
        let control = Arc::new(TestCheckpointControl {
            adopted: Mutex::new(Some(adopted_checkpoint(&base, checkpoint_id))),
            consumed: AtomicBool::new(false),
        });
        let provider = Arc::new(TestProviderExecutor {
            responses: Mutex::new(VecDeque::from([Ok(
                crate::AiProviderCallResult::test_result(
                    &claimed,
                    Some("test-previous-response".to_owned()),
                    "must-not-run",
                    Vec::new(),
                ),
            )])),
            require_checkpoint_cleared: true,
            calls: AtomicUsize::new(0),
        });
        let run = Arc::new(TestRunControl::new());
        let coordinator = AiSupervisedAgentCoordinator::new(
            run.clone(),
            provider.clone(),
            Arc::new(TestOutputWriter),
            Arc::new(TestCheckpointWriter {
                provider_checkpoints: AtomicUsize::new(0),
            }),
            control.clone(),
            Arc::new(TestApprovalStager {
                calls: AtomicUsize::new(0),
                saw_checkpoint: AtomicBool::new(false),
            }),
            unused_resume(),
            Arc::new(TestRuleResolver),
            Arc::new(TestPlanner {
                scope: test_scope(),
                route: test_route(),
                continuation_count: AtomicUsize::new(0),
            }),
            Arc::new(FixedClock::new(time::OffsetDateTime::now_utc())),
            limits_with_provider_turns(1),
        );

        let outcome = coordinator
            .execute_claimed(&claimed)
            .await
            .expect("exhausted loop should close without consuming the checkpoint");

        assert_eq!(
            outcome,
            AiSupervisedAgentRunOutcome::Failed {
                provider_turns: 1,
                total_tool_calls: 1,
            }
        );
        assert!(!control.consumed.load(Ordering::SeqCst));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        assert_eq!(provider.remaining_responses(), 1);
        assert_eq!(run.final_states(), vec![AiRunState::Failed]);
    }

    #[tokio::test]
    async fn ambiguous_approved_mutation_never_reenters_the_provider() {
        let lease = AiRunLease::test_running(principal_reference());
        let claim =
            crate::AiApprovedRunClaim::test_claim(lease.test_with_state(AiRunState::WaitingTool));
        let provider = Arc::new(TestProviderExecutor {
            responses: Mutex::new(VecDeque::from([Ok(
                crate::AiProviderCallResult::test_result(&lease, None, "must-not-run", Vec::new()),
            )])),
            require_checkpoint_cleared: false,
            calls: AtomicUsize::new(0),
        });
        let run = Arc::new(TestRunControl::new());
        let coordinator = AiSupervisedAgentCoordinator::new(
            run.clone(),
            provider.clone(),
            Arc::new(TestOutputWriter),
            Arc::new(TestCheckpointWriter {
                provider_checkpoints: AtomicUsize::new(0),
            }),
            Arc::new(TestCheckpointControl {
                adopted: Mutex::new(None),
                consumed: AtomicBool::new(false),
            }),
            Arc::new(TestApprovalStager {
                calls: AtomicUsize::new(0),
                saw_checkpoint: AtomicBool::new(false),
            }),
            Arc::new(TestResumeExecutor {
                outcome: Mutex::new(Some(AiSupervisedResumeOutcome::RecoveryRequired {
                    tool_call_id: claim.tool_call_id(),
                    provider_turns: 3,
                    total_tool_calls: 2,
                })),
            }),
            Arc::new(TestRuleResolver),
            Arc::new(TestPlanner {
                scope: test_scope(),
                route: test_route(),
                continuation_count: AtomicUsize::new(0),
            }),
            Arc::new(FixedClock::new(time::OffsetDateTime::now_utc())),
            limits(),
        );

        let outcome = coordinator
            .execute_approved_claim(&claim)
            .await
            .expect("ambiguous mutation should already be durably closed");

        assert_eq!(
            outcome,
            AiSupervisedAgentRunOutcome::RecoveryRequired {
                phase: AiAgentRecoveryPhase::ApplicationTool,
                provider_turns: 3,
                total_tool_calls: 2,
            }
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        assert_eq!(provider.remaining_responses(), 1);
        assert!(run.final_states().is_empty());
    }

    #[test]
    fn supervised_turn_rejects_a_read_only_provider_plan() {
        let lease = AiRunLease::test_running(principal_reference());
        let result = AiSupervisedAgentTurnPlan::new(
            AiProviderCallPlan::test_plan(&lease, test_scope(), false),
            test_route(),
            test_rules(test_scope()),
            false,
        );

        assert!(matches!(result, Err(AiError::InvalidInput(_))));
    }

    #[test]
    fn limits_reject_unbounded_approval_waits() {
        let result = AiSupervisedAgentCoordinatorLimits::new(
            AiAgentLoopLimits::new(4, 8).expect("test loop limits should validate"),
            Duration::seconds(1),
            Duration::days(2),
            false,
        );

        assert!(matches!(result, Err(AiError::InvalidConfiguration(_))));
    }
}
