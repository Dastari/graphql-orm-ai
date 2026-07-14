//! Protected same-attempt resumption of one approved supervised mutation.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::sync::Arc;

use crate::{
    AiApprovedRunClaim, AiConsequentialToolCallOutcome, AiError, AiProtectedSupervisedToolBatch,
    AiRunCompletion, AiRunState, AiToolCallId, OrmAiConsequentialToolCallService,
    OrmAiCoordinatorCheckpointService, OrmAiRunService,
};

/// Durable outcome of resuming one approved supervised mutation.
///
/// A checkpointed result is still not provider authority. A recovery-required
/// result means the consequential resolver or post-mutation continuation
/// handoff could not be proven safe and must never be replayed automatically.
#[derive(Clone, Debug)]
pub enum AiSupervisedResumeOutcome {
    /// The exact resolver result and next provider continuation were protected
    /// under the current fence.
    Checkpointed(Box<AiProtectedSupervisedToolBatch>),
    /// The mutation or post-mutation handoff is terminally ambiguous.
    RecoveryRequired {
        /// Durable consequential tool call left for privileged review.
        tool_call_id: AiToolCallId,
    },
}

impl AiSupervisedResumeOutcome {
    /// Returns the protected continuation checkpoint when the mutation and
    /// durable handoff both completed unambiguously.
    pub fn checkpointed(&self) -> Option<&AiProtectedSupervisedToolBatch> {
        match self {
            Self::Checkpointed(checkpoint) => Some(checkpoint.as_ref()),
            Self::RecoveryRequired { .. } => None,
        }
    }

    /// Returns the exact consequential tool call.
    pub const fn tool_call_id(&self) -> AiToolCallId {
        match self {
            Self::Checkpointed(checkpoint) => checkpoint.tool_call_id(),
            Self::RecoveryRequired { tool_call_id } => *tool_call_id,
        }
    }
}

/// Security-ordered service for one restart-safe approved-wait handoff.
///
/// The service first reopens the exact protected provider turn, then executes
/// the already staged mutation through fresh preview, approval consumption,
/// current principal/policy, and ordinary GraphQL resolver authorization. A
/// successful model-visible result is immediately protected in a dedicated
/// supervised continuation checkpoint. It never calls a provider.
pub struct OrmAiSupervisedResumeService {
    run_service: OrmAiRunService,
    checkpoints: Arc<OrmAiCoordinatorCheckpointService>,
    consequential_tools: Arc<OrmAiConsequentialToolCallService>,
}

impl OrmAiSupervisedResumeService {
    /// Creates the protected supervised-resume service.
    pub fn new(
        run_service: OrmAiRunService,
        checkpoints: Arc<OrmAiCoordinatorCheckpointService>,
        consequential_tools: Arc<OrmAiConsequentialToolCallService>,
    ) -> Self {
        Self {
            run_service,
            checkpoints,
            consequential_tools,
        }
    }

    /// Executes one exact approved-wait claim and protects its continuation.
    ///
    /// This first contract accepts one provider-retained supervised mutation.
    /// Multi-call and stateless provider turns remain closed until their
    /// complete durable ordering/history proofs are implemented.
    ///
    /// # Errors
    ///
    /// Returns a safe error before resolver ambiguity for stale fencing,
    /// invalid protected provider/approval/tool/rule evidence, fresh
    /// authorization denial, or persistence failure. Resolver or
    /// post-side-effect ambiguity is converted into a durable
    /// [`AiSupervisedResumeOutcome::RecoveryRequired`] whenever the current
    /// fence can still commit that fact.
    pub async fn execute_claimed(
        &self,
        claim: &AiApprovedRunClaim,
    ) -> Result<AiSupervisedResumeOutcome, AiError> {
        let adopted = self
            .checkpoints
            .adopt_supervised_provider_turn(claim)
            .await?;
        let provider_response_id = adopted.provider_response_id().map(str::to_owned);
        let tool_call_id = adopted.tool_call_id();
        let outcome = self
            .consequential_tools
            .execute_approved(
                claim.lease(),
                adopted.approval_id(),
                tool_call_id,
                adopted.result_egress_route().clone(),
            )
            .await?;
        let AiConsequentialToolCallOutcome::Persisted(persisted) = outcome else {
            return Ok(AiSupervisedResumeOutcome::RecoveryRequired { tool_call_id });
        };
        match self
            .checkpoints
            .persist_supervised_tool_batch(adopted, persisted.as_ref())
            .await
        {
            Ok(checkpoint) => Ok(AiSupervisedResumeOutcome::Checkpointed(Box::new(
                checkpoint,
            ))),
            Err(error) => {
                let completion = AiRunCompletion::new(
                    AiRunState::RecoveryRequired,
                    "supervised_continuation_checkpoint_uncertain",
                    Some("supervised_continuation_checkpoint_uncertain".to_owned()),
                    provider_response_id,
                )?;
                match self.run_service.finish(persisted.lease(), completion).await {
                    Ok(()) => Ok(AiSupervisedResumeOutcome::RecoveryRequired { tool_call_id }),
                    Err(_) => Err(error),
                }
            }
        }
    }
}
