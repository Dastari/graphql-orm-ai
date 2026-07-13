//! Fenced top-level coordination for bounded read-only provider/tool loops.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::sync::Arc;

use async_trait::async_trait;
use time::Duration;
use uuid::Uuid;

use crate::{
    AiAgentContinuation, AiAgentLoopGuard, AiAgentLoopLimits, AiAgentLoopTurn,
    AiApplicationToolCallContext, AiError, AiPersistedApplicationToolCall,
    AiPersistedProviderOutput, AiProviderCallExecutor, AiProviderCallPlan, AiProviderCallResult,
    AiReadOnlyAgentRunOutcome::*, AiRunCompletion, AiRunLease, AiRunState, AiScope,
    AiToolResultEgressRoute, OrmAiApplicationToolCallService, OrmAiProviderOutputService,
    OrmAiRunService,
};

/// Deployment-owned bounds for a top-level read-only agent attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiReadOnlyAgentCoordinatorLimits {
    loop_limits: AiAgentLoopLimits,
    heartbeat_interval: Duration,
}

impl AiReadOnlyAgentCoordinatorLimits {
    /// Creates coordinator limits.
    ///
    /// The heartbeat interval must also be comfortably shorter than the lease
    /// TTL configured on the concrete run service. Keeping that relationship
    /// deployment-owned avoids silently changing durable lease policy here.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless the heartbeat interval
    /// is positive and no longer than five minutes.
    pub fn new(
        loop_limits: AiAgentLoopLimits,
        heartbeat_interval: Duration,
    ) -> Result<Self, AiError> {
        if !heartbeat_interval.is_positive() || heartbeat_interval > Duration::minutes(5) {
            return Err(AiError::InvalidConfiguration(
                "invalid agent coordinator heartbeat interval".to_owned(),
            ));
        }
        Ok(Self {
            loop_limits,
            heartbeat_interval,
        })
    }
}

/// One host-planned provider turn for the read-only coordinator.
///
/// Construction proves that custom application tools are present and that the
/// result-egress route is structurally valid. It does not authorize discovery,
/// resolver execution, or egress; those decisions remain fresh per turn/call.
pub struct AiReadOnlyAgentTurnPlan {
    provider_call: AiProviderCallPlan,
    result_egress_route: AiToolResultEgressRoute,
}

impl AiReadOnlyAgentTurnPlan {
    /// Binds a provider plan to the server-selected route used for every exact
    /// application-tool result in that turn.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidInput`] if the provider plan has no validated
    /// application tools or the route is malformed.
    pub fn new(
        provider_call: AiProviderCallPlan,
        result_egress_route: AiToolResultEgressRoute,
    ) -> Result<Self, AiError> {
        if !provider_call.has_application_tools() {
            return Err(AiError::InvalidInput(
                "agent turn plan has no application tools".to_owned(),
            ));
        }
        result_egress_route.validate()?;
        Ok(Self {
            provider_call,
            result_egress_route,
        })
    }

    fn into_parts(self) -> (AiProviderCallPlan, AiScope, String, AiToolResultEgressRoute) {
        let scope = self.provider_call.scope().clone();
        let correlation_id = self.provider_call.correlation_id().to_owned();
        (
            self.provider_call,
            scope,
            correlation_id,
            self.result_egress_route,
        )
    }

    fn is_continuation(&self) -> bool {
        self.provider_call.is_continuation()
    }
}

/// Host-owned construction of exact initial and continuation provider plans.
///
/// Implementations select configuration, provider profile, model, context,
/// enabled tool definitions, fresh atomic-budget estimates, and exact egress
/// manifests. Model output must never select these values. Continuations must
/// be consumed through [`AiProviderCallPlan::new_continuation_with_tools`].
#[async_trait]
pub trait AiReadOnlyAgentTurnPlanner: Send + Sync {
    /// Builds the first turn for a newly running fenced attempt.
    ///
    /// # Errors
    ///
    /// Returns a safe library error when current configuration, context,
    /// budget estimates, or egress manifests cannot produce an exact plan.
    async fn initial_plan(&self, lease: &AiRunLease) -> Result<AiReadOnlyAgentTurnPlan, AiError>;

    /// Builds the next provider turn from the exact completed tool batch.
    ///
    /// `provider_turns` is the number of provider results already accepted by
    /// the bounded guard. The opaque continuation binds every call ID, result,
    /// transfer manifest, and prior provider response as one unit.
    ///
    /// # Errors
    ///
    /// Returns a safe library error when fresh configuration or proofs cannot
    /// produce an exactly chained continuation plan.
    async fn continuation_plan(
        &self,
        lease: &AiRunLease,
        provider_turns: u32,
        continuation: AiAgentContinuation,
    ) -> Result<AiReadOnlyAgentTurnPlan, AiError>;
}

/// Minimal fenced run operations required by the coordinator.
///
/// Alternative implementations must preserve the same attempt, generation,
/// expiry, and row-version checks as [`OrmAiRunService`]. This seam is intended
/// for conformance testing and deployment wrapping, not weaker persistence.
#[async_trait]
pub trait AiAgentRunControl: Send + Sync {
    /// Moves a freshly claimed lease to `Running` and renews its proof.
    ///
    /// # Errors
    ///
    /// Fails closed when the claim is expired, stale, malformed, or cannot be
    /// persisted.
    async fn start(&self, lease: &AiRunLease) -> Result<AiRunLease, AiError>;

    /// Renews one current running proof.
    ///
    /// # Errors
    ///
    /// Fails closed when the fence is expired, stale, malformed, or cannot be
    /// persisted.
    async fn heartbeat(&self, lease: &AiRunLease) -> Result<AiRunLease, AiError>;

    /// Commits one exact terminal/recovery outcome.
    ///
    /// # Errors
    ///
    /// Fails closed when the fence/outcome is stale, invalid, duplicate, or
    /// cannot be persisted.
    async fn finish(&self, lease: &AiRunLease, completion: AiRunCompletion) -> Result<(), AiError>;
}

#[async_trait]
impl AiAgentRunControl for OrmAiRunService {
    async fn start(&self, lease: &AiRunLease) -> Result<AiRunLease, AiError> {
        OrmAiRunService::start(self, lease).await
    }

    async fn heartbeat(&self, lease: &AiRunLease) -> Result<AiRunLease, AiError> {
        OrmAiRunService::heartbeat(self, lease).await
    }

    async fn finish(&self, lease: &AiRunLease, completion: AiRunCompletion) -> Result<(), AiError> {
        OrmAiRunService::finish(self, lease, completion).await
    }
}

/// One-turn provider execution boundary used by the coordinator.
#[async_trait]
pub trait AiAgentProviderTurnExecutor: Send + Sync {
    /// Executes one exactly planned turn for the current attempt/generation.
    ///
    /// # Errors
    ///
    /// Returns a safe library error for any authorization, budget, egress,
    /// provider, normalization, usage, fence, or persistence failure.
    async fn execute_turn(
        &self,
        lease: &AiRunLease,
        plan: AiProviderCallPlan,
    ) -> Result<AiProviderCallResult, AiError>;
}

#[async_trait]
impl AiAgentProviderTurnExecutor for AiProviderCallExecutor {
    async fn execute_turn(
        &self,
        lease: &AiRunLease,
        plan: AiProviderCallPlan,
    ) -> Result<AiProviderCallResult, AiError> {
        self.execute(lease, plan).await
    }
}

/// Protected read-only application-tool execution boundary.
#[async_trait]
pub trait AiAgentReadOnlyToolExecutor: Send + Sync {
    /// Executes and durably resolves one exact provider-requested query.
    ///
    /// # Errors
    ///
    /// Returns a safe library error for stale binding/fence, authorization,
    /// disclosure, protection, execution, egress, or persistence failure.
    async fn execute_tool(
        &self,
        lease: &AiRunLease,
        provider_result: &AiProviderCallResult,
        context: AiApplicationToolCallContext,
        route: AiToolResultEgressRoute,
    ) -> Result<AiPersistedApplicationToolCall, AiError>;
}

#[async_trait]
impl AiAgentReadOnlyToolExecutor for OrmAiApplicationToolCallService {
    async fn execute_tool(
        &self,
        lease: &AiRunLease,
        provider_result: &AiProviderCallResult,
        context: AiApplicationToolCallContext,
        route: AiToolResultEgressRoute,
    ) -> Result<AiPersistedApplicationToolCall, AiError> {
        self.execute_read_only(lease, provider_result, context, route)
            .await
    }
}

/// Protected terminal assistant-output persistence boundary.
#[async_trait]
pub trait AiAgentProviderOutputWriter: Send + Sync {
    /// Persists one tool-free successful final provider result.
    ///
    /// # Errors
    ///
    /// Returns a safe library error for stale binding/fence, current-access or
    /// protection denial, malformed output, or persistence failure.
    async fn persist_output(
        &self,
        lease: &AiRunLease,
        result: &AiProviderCallResult,
    ) -> Result<AiPersistedProviderOutput, AiError>;
}

#[async_trait]
impl AiAgentProviderOutputWriter for OrmAiProviderOutputService {
    async fn persist_output(
        &self,
        lease: &AiRunLease,
        result: &AiProviderCallResult,
    ) -> Result<AiPersistedProviderOutput, AiError> {
        self.persist(lease, result).await
    }
}

/// External phase whose incomplete durable handoff requires reconciliation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AiAgentRecoveryPhase {
    /// A provider request or completed provider result could not be handed to
    /// the next exact durable phase.
    ProviderTurn,
    /// A protected application-tool call could have crossed resolver execution.
    ApplicationTool,
    /// Final provider output could not be proven persisted and finalized.
    ProviderOutput,
}

/// Durable result of one coordinator-owned claimed attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AiReadOnlyAgentRunOutcome {
    /// Final assistant output was persisted and the run committed `Completed`.
    Completed {
        /// Persisted assistant message identifier.
        message_id: Uuid,
        /// Accepted provider-turn count.
        provider_turns: u32,
        /// Durably resolved application-tool call count.
        total_tool_calls: u32,
    },
    /// A proof/configuration failure was durably classified as safe failure.
    Failed {
        /// Accepted provider-turn count.
        provider_turns: u32,
        /// Durably resolved application-tool call count.
        total_tool_calls: u32,
    },
    /// An external boundary remained ambiguous and the run was durably closed
    /// for privileged reconciliation instead of being replayed.
    RecoveryRequired {
        /// Phase whose completion could not be proven.
        phase: AiAgentRecoveryPhase,
        /// Accepted provider-turn count.
        provider_turns: u32,
        /// Durably resolved application-tool call count.
        total_tool_calls: u32,
    },
}

/// Security-ordered coordinator for one claimed read-only agent attempt.
///
/// The coordinator starts and heartbeats the exact lease, asks a trusted host
/// planner for each turn, executes provider calls, resolves every custom query
/// through the protected ORM tool service, constructs exact continuations,
/// persists final output, and commits a terminal outcome. Any ambiguous
/// provider/tool/output handoff becomes `RecoveryRequired`; the coordinator
/// never reconstructs or silently replays uncertain state.
pub struct AiReadOnlyAgentCoordinator {
    run_control: Arc<dyn AiAgentRunControl>,
    provider_executor: Arc<dyn AiAgentProviderTurnExecutor>,
    tool_executor: Arc<dyn AiAgentReadOnlyToolExecutor>,
    output_writer: Arc<dyn AiAgentProviderOutputWriter>,
    planner: Arc<dyn AiReadOnlyAgentTurnPlanner>,
    limits: AiReadOnlyAgentCoordinatorLimits,
}

impl AiReadOnlyAgentCoordinator {
    /// Creates a coordinator from proof-preserving service boundaries.
    pub fn new(
        run_control: Arc<dyn AiAgentRunControl>,
        provider_executor: Arc<dyn AiAgentProviderTurnExecutor>,
        tool_executor: Arc<dyn AiAgentReadOnlyToolExecutor>,
        output_writer: Arc<dyn AiAgentProviderOutputWriter>,
        planner: Arc<dyn AiReadOnlyAgentTurnPlanner>,
        limits: AiReadOnlyAgentCoordinatorLimits,
    ) -> Self {
        Self {
            run_control,
            provider_executor,
            tool_executor,
            output_writer,
            planner,
            limits,
        }
    }

    /// Executes one freshly claimed lease through a bounded terminal outcome.
    ///
    /// A successful return means the corresponding terminal or
    /// `RecoveryRequired` state was durably committed. An error means that even
    /// that final write could not be proven; normal expired-lease/restore
    /// reconciliation must remain authoritative.
    ///
    /// # Errors
    ///
    /// Returns an error for a stale/invalid claim, failed terminal write, lost
    /// heartbeat fence, or malformed service result. Provider/tool/output
    /// ambiguity is converted to a durable [`RecoveryRequired`] outcome when
    /// the current fence can still commit it.
    pub async fn execute_claimed(
        &self,
        claimed: &AiRunLease,
    ) -> Result<AiReadOnlyAgentRunOutcome, AiError> {
        let mut lease = self.run_control.start(claimed).await?;
        let mut guard = AiAgentLoopGuard::new(&lease, self.limits.loop_limits);
        let mut turn_plan = match self.planner.initial_plan(&lease).await {
            Ok(plan) if !plan.is_continuation() => plan,
            Err(_) => {
                return self
                    .finish_failed(&lease, &guard, "initial_plan_failed")
                    .await;
            }
            Ok(_) => {
                return self
                    .finish_failed(&lease, &guard, "initial_plan_phase_invalid")
                    .await;
            }
        };

        loop {
            let (provider_plan, scope, correlation_id, route) = turn_plan.into_parts();
            let result = match self
                .execute_provider_with_heartbeats(&mut lease, provider_plan)
                .await
            {
                Ok(result) => result,
                Err(ProviderTurnFailure::Provider) => {
                    return self
                        .finish_recovery(
                            &lease,
                            &guard,
                            AiAgentRecoveryPhase::ProviderTurn,
                            "provider_turn_uncertain",
                            None,
                        )
                        .await;
                }
                Err(ProviderTurnFailure::LeaseLost(error)) => return Err(error),
            };

            let observed = match guard.observe_provider_turn(&result) {
                Ok(observed) => observed,
                Err(_) => {
                    return self
                        .finish_recovery(
                            &lease,
                            &guard,
                            AiAgentRecoveryPhase::ProviderTurn,
                            "provider_turn_binding_failed",
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
                                    "provider_output_uncertain",
                                    result.provider_response_id(),
                                )
                                .await;
                        }
                    };
                    let message_id = persisted.message_id();
                    lease = persisted.into_lease();
                    let completion = AiRunCompletion::new(
                        AiRunState::Completed,
                        "agent_completed",
                        None,
                        result.provider_response_id().map(str::to_owned),
                    )?;
                    self.run_control.finish(&lease, completion).await?;
                    return Ok(Completed {
                        message_id,
                        provider_turns: guard.provider_turns(),
                        total_tool_calls: guard.total_tool_calls(),
                    });
                }
                AiAgentLoopTurn::ToolCalls {
                    provider_turn_index,
                    call_count,
                } => {
                    for tool_call_index in 0..call_count {
                        let context = AiApplicationToolCallContext::new(
                            provider_turn_index,
                            tool_call_index,
                            scope.clone(),
                            correlation_id.clone(),
                            result.budget_reservation_id().0.to_string(),
                        )?;
                        let persisted = match self
                            .tool_executor
                            .execute_tool(&lease, &result, context, route.clone())
                            .await
                        {
                            Ok(persisted) => persisted,
                            Err(_) => {
                                return self
                                    .finish_recovery(
                                        &lease,
                                        &guard,
                                        AiAgentRecoveryPhase::ApplicationTool,
                                        "application_tool_uncertain",
                                        result.provider_response_id(),
                                    )
                                    .await;
                            }
                        };
                        let renewed = persisted.lease().clone();
                        if guard.observe_tool_result(&persisted).is_err() {
                            return self
                                .finish_failed(
                                    &renewed,
                                    &guard,
                                    "application_tool_result_unavailable",
                                )
                                .await;
                        }
                        lease = renewed;
                    }
                    let continuation = match guard.continuation() {
                        Ok(continuation) => continuation,
                        Err(_) => {
                            return self
                                .finish_recovery(
                                    &lease,
                                    &guard,
                                    AiAgentRecoveryPhase::ApplicationTool,
                                    "application_tool_batch_invalid",
                                    result.provider_response_id(),
                                )
                                .await;
                        }
                    };
                    turn_plan = match self
                        .planner
                        .continuation_plan(&lease, guard.provider_turns(), continuation)
                        .await
                    {
                        Ok(plan) if plan.is_continuation() => plan,
                        Err(_) => {
                            return self
                                .finish_failed(&lease, &guard, "continuation_plan_failed")
                                .await;
                        }
                        Ok(_) => {
                            return self
                                .finish_failed(&lease, &guard, "continuation_plan_phase_invalid")
                                .await;
                        }
                    };
                }
            }
        }
    }

    async fn execute_provider_with_heartbeats(
        &self,
        lease: &mut AiRunLease,
        plan: AiProviderCallPlan,
    ) -> Result<AiProviderCallResult, ProviderTurnFailure> {
        let provider_lease = lease.clone();
        let provider = self.provider_executor.execute_turn(&provider_lease, plan);
        tokio::pin!(provider);
        let heartbeat_delay = self.limits.heartbeat_interval.unsigned_abs();
        loop {
            let heartbeat = tokio::time::sleep(heartbeat_delay);
            tokio::pin!(heartbeat);
            tokio::select! {
                result = &mut provider => return result.map_err(|_| ProviderTurnFailure::Provider),
                () = &mut heartbeat => {
                    *lease = self
                        .run_control
                        .heartbeat(lease)
                        .await
                        .map_err(ProviderTurnFailure::LeaseLost)?;
                }
            }
        }
    }

    async fn finish_failed(
        &self,
        lease: &AiRunLease,
        guard: &AiAgentLoopGuard,
        code: &str,
    ) -> Result<AiReadOnlyAgentRunOutcome, AiError> {
        let completion =
            AiRunCompletion::new(AiRunState::Failed, code, Some(code.to_owned()), None)?;
        self.run_control.finish(lease, completion).await?;
        Ok(Failed {
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
    ) -> Result<AiReadOnlyAgentRunOutcome, AiError> {
        let completion = AiRunCompletion::new(
            AiRunState::RecoveryRequired,
            code,
            Some(code.to_owned()),
            provider_response_id.map(str::to_owned),
        )?;
        self.run_control.finish(lease, completion).await?;
        Ok(RecoveryRequired {
            phase,
            provider_turns: guard.provider_turns(),
            total_tool_calls: guard.total_tool_calls(),
        })
    }
}

enum ProviderTurnFailure {
    Provider,
    LeaseLost(AiError),
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use agql_auth::{
        AccessTokenMetadata, AuthPrincipal, AuthUser, PrincipalReference, SessionContext,
    };
    use serde_json::json;

    use super::*;
    use crate::{
        AiDataSourceRef, AiDestinationTrust, AiEgressCapability, AiEgressManifest, AiSourceTrust,
        DataClassification,
    };

    struct TestRunControl {
        finishes: Mutex<Vec<AiRunState>>,
        heartbeat_count: AtomicUsize,
        fail_heartbeat: AtomicBool,
    }

    impl TestRunControl {
        fn new() -> Self {
            Self {
                finishes: Mutex::new(Vec::new()),
                heartbeat_count: AtomicUsize::new(0),
                fail_heartbeat: AtomicBool::new(false),
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
            self.heartbeat_count.fetch_add(1, Ordering::SeqCst);
            if self.fail_heartbeat.load(Ordering::SeqCst) {
                Err(AiError::Conflict)
            } else {
                Ok(lease.clone())
            }
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
        responses: Mutex<VecDeque<Result<AiProviderCallResult, AiError>>>,
        delay: Option<std::time::Duration>,
    }

    impl TestProviderExecutor {
        fn remaining_responses(&self) -> usize {
            self.responses
                .lock()
                .expect("test response lock should not be poisoned")
                .len()
        }
    }

    #[async_trait]
    impl AiAgentProviderTurnExecutor for TestProviderExecutor {
        async fn execute_turn(
            &self,
            _lease: &AiRunLease,
            _plan: AiProviderCallPlan,
        ) -> Result<AiProviderCallResult, AiError> {
            if let Some(delay) = self.delay {
                tokio::time::sleep(delay).await;
            }
            self.responses
                .lock()
                .expect("test response lock should not be poisoned")
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
    impl AiReadOnlyAgentTurnPlanner for TestPlanner {
        async fn initial_plan(
            &self,
            lease: &AiRunLease,
        ) -> Result<AiReadOnlyAgentTurnPlan, AiError> {
            AiReadOnlyAgentTurnPlan::new(
                AiProviderCallPlan::test_plan(lease, self.scope.clone(), false),
                self.route.clone(),
            )
        }

        async fn continuation_plan(
            &self,
            lease: &AiRunLease,
            _provider_turns: u32,
            _continuation: AiAgentContinuation,
        ) -> Result<AiReadOnlyAgentTurnPlan, AiError> {
            self.continuation_count.fetch_add(1, Ordering::SeqCst);
            AiReadOnlyAgentTurnPlan::new(
                AiProviderCallPlan::test_plan(lease, self.scope.clone(), true),
                self.route.clone(),
            )
        }
    }

    struct InvalidContinuationPlanner {
        scope: AiScope,
        route: AiToolResultEgressRoute,
    }

    #[async_trait]
    impl AiReadOnlyAgentTurnPlanner for InvalidContinuationPlanner {
        async fn initial_plan(
            &self,
            lease: &AiRunLease,
        ) -> Result<AiReadOnlyAgentTurnPlan, AiError> {
            AiReadOnlyAgentTurnPlan::new(
                AiProviderCallPlan::test_plan(lease, self.scope.clone(), false),
                self.route.clone(),
            )
        }

        async fn continuation_plan(
            &self,
            lease: &AiRunLease,
            _provider_turns: u32,
            _continuation: AiAgentContinuation,
        ) -> Result<AiReadOnlyAgentTurnPlan, AiError> {
            AiReadOnlyAgentTurnPlan::new(
                AiProviderCallPlan::test_plan(lease, self.scope.clone(), false),
                self.route.clone(),
            )
        }
    }

    struct TestToolExecutor {
        expose_result: bool,
    }

    #[async_trait]
    impl AiAgentReadOnlyToolExecutor for TestToolExecutor {
        async fn execute_tool(
            &self,
            lease: &AiRunLease,
            provider_result: &AiProviderCallResult,
            _context: AiApplicationToolCallContext,
            _route: AiToolResultEgressRoute,
        ) -> Result<AiPersistedApplicationToolCall, AiError> {
            let call = provider_result
                .tool_calls()
                .first()
                .expect("test tool call should exist");
            let manifest = self
                .expose_result
                .then(|| test_manifest(lease, AiEgressCapability::ToolResult));
            Ok(AiPersistedApplicationToolCall::test_completed(
                lease.clone(),
                call.call_id(),
                call.tool_id().as_str(),
                self.expose_result.then(|| json!({"record": "safe"})),
                manifest,
            ))
        }
    }

    struct TestOutputWriter;

    #[async_trait]
    impl AiAgentProviderOutputWriter for TestOutputWriter {
        async fn persist_output(
            &self,
            lease: &AiRunLease,
            _result: &AiProviderCallResult,
        ) -> Result<AiPersistedProviderOutput, AiError> {
            Ok(AiPersistedProviderOutput::test_output(lease.clone()))
        }
    }

    fn principal_reference() -> PrincipalReference {
        AuthPrincipal::User(AuthUser {
            user_id: "coordinator-user".to_owned(),
            session_id: Uuid::new_v4(),
            roles: Vec::new(),
            scopes: Vec::new(),
            session: SessionContext::default(),
            token_claims: AccessTokenMetadata {
                tenant_id: Some("coordinator-tenant".to_owned()),
                ..AccessTokenMetadata::default()
            },
        })
        .reference()
    }

    fn test_scope() -> AiScope {
        AiScope::new("test", "coordinator-scope").with_tenant_id("coordinator-tenant")
    }

    fn test_route() -> AiToolResultEgressRoute {
        AiToolResultEgressRoute::new(
            "test-provider-profile",
            "test-provider-boundary",
            AiDestinationTrust::ExternalProcessor,
            "agent-test",
            "none",
            "egress-v1",
        )
        .expect("test route should validate")
    }

    fn test_manifest(lease: &AiRunLease, capability: AiEgressCapability) -> AiEgressManifest {
        AiEgressManifest {
            provider_profile_id: "test-provider-profile".to_owned(),
            provider_kind: "openai".to_owned(),
            model: "coordinator-test-model".to_owned(),
            destination: "test-provider-boundary".to_owned(),
            destination_trust: AiDestinationTrust::ExternalProcessor,
            capability,
            scope: test_scope(),
            session_id: Some(lease.session_id()),
            run_id: Some(lease.run_id()),
            sources: vec![AiDataSourceRef {
                kind: "test".to_owned(),
                reference: "test-result".to_owned(),
                classification: DataClassification::Public,
                trust: AiSourceTrust::ResolverResult,
            }],
            estimated_bytes: 64,
            estimated_tokens: 0,
            attachment_count: 0,
            purpose: "agent-test".to_owned(),
            retention: "none".to_owned(),
            residency: None,
            policy_version: "egress-v1".to_owned(),
            consent_reference: None,
        }
    }

    fn limits(heartbeat_millis: i64) -> AiReadOnlyAgentCoordinatorLimits {
        AiReadOnlyAgentCoordinatorLimits::new(
            AiAgentLoopLimits::new(4, 8).expect("test loop limits should validate"),
            Duration::milliseconds(heartbeat_millis),
        )
        .expect("test coordinator limits should validate")
    }

    fn coordinator(
        run: Arc<TestRunControl>,
        provider: Arc<TestProviderExecutor>,
        planner: Arc<TestPlanner>,
        expose_tool_result: bool,
        coordinator_limits: AiReadOnlyAgentCoordinatorLimits,
    ) -> AiReadOnlyAgentCoordinator {
        AiReadOnlyAgentCoordinator::new(
            run,
            provider,
            Arc::new(TestToolExecutor {
                expose_result: expose_tool_result,
            }),
            Arc::new(TestOutputWriter),
            planner,
            coordinator_limits,
        )
    }

    #[tokio::test]
    async fn final_provider_turn_persists_output_before_completion() {
        let lease = AiRunLease::test_running(principal_reference());
        let run = Arc::new(TestRunControl::new());
        let provider = Arc::new(TestProviderExecutor {
            responses: Mutex::new(VecDeque::from([Ok(AiProviderCallResult::test_result(
                &lease,
                None,
                "response-final",
                Vec::new(),
            ))])),
            delay: None,
        });
        let planner = Arc::new(TestPlanner {
            scope: test_scope(),
            route: test_route(),
            continuation_count: AtomicUsize::new(0),
        });

        let outcome = coordinator(run.clone(), provider, planner, true, limits(50))
            .execute_claimed(&lease)
            .await
            .expect("coordinator should complete");

        assert!(matches!(
            outcome,
            Completed {
                provider_turns: 1,
                total_tool_calls: 0,
                ..
            }
        ));
        assert_eq!(run.final_states(), vec![AiRunState::Completed]);
    }

    #[tokio::test]
    async fn exact_tool_result_is_chained_into_the_next_turn() {
        let lease = AiRunLease::test_running(principal_reference());
        let run = Arc::new(TestRunControl::new());
        let provider = Arc::new(TestProviderExecutor {
            responses: Mutex::new(VecDeque::from([
                Ok(AiProviderCallResult::test_result(
                    &lease,
                    None,
                    "response-tool",
                    vec![("call-1", "test.read", json!({}))],
                )),
                Ok(AiProviderCallResult::test_result(
                    &lease,
                    Some("response-tool".to_owned()),
                    "response-final",
                    Vec::new(),
                )),
            ])),
            delay: None,
        });
        let planner = Arc::new(TestPlanner {
            scope: test_scope(),
            route: test_route(),
            continuation_count: AtomicUsize::new(0),
        });

        let outcome = coordinator(run.clone(), provider, planner.clone(), true, limits(50))
            .execute_claimed(&lease)
            .await
            .expect("coordinator should complete tool loop");

        assert!(matches!(
            outcome,
            Completed {
                provider_turns: 2,
                total_tool_calls: 1,
                ..
            }
        ));
        assert_eq!(planner.continuation_count.load(Ordering::SeqCst), 1);
        assert_eq!(run.final_states(), vec![AiRunState::Completed]);
    }

    #[tokio::test]
    async fn provider_failure_is_closed_as_recovery_required() {
        let lease = AiRunLease::test_running(principal_reference());
        let run = Arc::new(TestRunControl::new());
        let provider = Arc::new(TestProviderExecutor {
            responses: Mutex::new(VecDeque::from([Err(AiError::ProviderFailed)])),
            delay: None,
        });
        let planner = Arc::new(TestPlanner {
            scope: test_scope(),
            route: test_route(),
            continuation_count: AtomicUsize::new(0),
        });

        let outcome = coordinator(run.clone(), provider, planner, true, limits(50))
            .execute_claimed(&lease)
            .await
            .expect("provider ambiguity should be durably classified");

        assert_eq!(
            outcome,
            RecoveryRequired {
                phase: AiAgentRecoveryPhase::ProviderTurn,
                provider_turns: 0,
                total_tool_calls: 0,
            }
        );
        assert_eq!(run.final_states(), vec![AiRunState::RecoveryRequired]);
    }

    #[tokio::test]
    async fn unavailable_tool_egress_fails_without_a_second_provider_call() {
        let lease = AiRunLease::test_running(principal_reference());
        let run = Arc::new(TestRunControl::new());
        let provider = Arc::new(TestProviderExecutor {
            responses: Mutex::new(VecDeque::from([Ok(AiProviderCallResult::test_result(
                &lease,
                None,
                "response-tool",
                vec![("call-1", "test.read", json!({}))],
            ))])),
            delay: None,
        });
        let planner = Arc::new(TestPlanner {
            scope: test_scope(),
            route: test_route(),
            continuation_count: AtomicUsize::new(0),
        });

        let outcome = coordinator(run.clone(), provider, planner, false, limits(50))
            .execute_claimed(&lease)
            .await
            .expect("denied result should become a durable safe failure");

        assert!(matches!(
            outcome,
            Failed {
                provider_turns: 1,
                total_tool_calls: 1,
            }
        ));
        assert_eq!(run.final_states(), vec![AiRunState::Failed]);
    }

    #[tokio::test]
    async fn continuation_planner_cannot_drop_the_opaque_continuation() {
        let lease = AiRunLease::test_running(principal_reference());
        let run = Arc::new(TestRunControl::new());
        let provider = Arc::new(TestProviderExecutor {
            responses: Mutex::new(VecDeque::from([
                Ok(AiProviderCallResult::test_result(
                    &lease,
                    None,
                    "response-tool",
                    vec![("call-1", "test.read", json!({}))],
                )),
                Ok(AiProviderCallResult::test_result(
                    &lease,
                    None,
                    "must-not-run",
                    Vec::new(),
                )),
            ])),
            delay: None,
        });
        let invalid_planner = Arc::new(InvalidContinuationPlanner {
            scope: test_scope(),
            route: test_route(),
        });
        let coordinator = AiReadOnlyAgentCoordinator::new(
            run.clone(),
            provider.clone(),
            Arc::new(TestToolExecutor {
                expose_result: true,
            }),
            Arc::new(TestOutputWriter),
            invalid_planner,
            limits(50),
        );

        let outcome = coordinator
            .execute_claimed(&lease)
            .await
            .expect("invalid continuation phase should be a safe failure");

        assert!(matches!(
            outcome,
            Failed {
                provider_turns: 1,
                total_tool_calls: 1,
            }
        ));
        assert_eq!(provider.remaining_responses(), 1);
        assert_eq!(run.final_states(), vec![AiRunState::Failed]);
    }

    #[tokio::test]
    async fn lost_heartbeat_fence_stops_without_terminal_write() {
        let lease = AiRunLease::test_running(principal_reference());
        let run = Arc::new(TestRunControl::new());
        run.fail_heartbeat.store(true, Ordering::SeqCst);
        let provider = Arc::new(TestProviderExecutor {
            responses: Mutex::new(VecDeque::from([Ok(AiProviderCallResult::test_result(
                &lease,
                None,
                "late-response",
                Vec::new(),
            ))])),
            delay: Some(std::time::Duration::from_millis(50)),
        });
        let planner = Arc::new(TestPlanner {
            scope: test_scope(),
            route: test_route(),
            continuation_count: AtomicUsize::new(0),
        });

        let error = coordinator(run.clone(), provider, planner, true, limits(1))
            .execute_claimed(&lease)
            .await
            .expect_err("lost fence must stop the attempt");

        assert!(matches!(error, AiError::Conflict));
        assert!(run.heartbeat_count.load(Ordering::SeqCst) >= 1);
        assert!(run.final_states().is_empty());
    }
}
