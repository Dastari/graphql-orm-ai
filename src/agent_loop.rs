//! Bounded proof-oriented sequencing for multi-turn provider/tool loops.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    AiEgressManifest, AiError, AiPersistedApplicationToolCall, AiProviderCallResult, AiRunId,
    AiRunLease, AiSessionId, ModelContinuation, ModelInputBlock, ModelRequest,
};

/// Deployment-owned hard bounds for one multi-turn agent loop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiAgentLoopLimits {
    maximum_provider_turns: u32,
    maximum_total_tool_calls: u32,
}

impl AiAgentLoopLimits {
    /// Creates validated hard loop bounds.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless provider turns are in
    /// `1..=1_024` and total tool calls are in `1..=4_096`.
    pub fn new(
        maximum_provider_turns: u32,
        maximum_total_tool_calls: u32,
    ) -> Result<Self, AiError> {
        if !(1..=1_024).contains(&maximum_provider_turns)
            || !(1..=4_096).contains(&maximum_total_tool_calls)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid agent-loop limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_provider_turns,
            maximum_total_tool_calls,
        })
    }
}

/// Observation produced when one provider turn is accepted by the loop guard.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AiAgentLoopTurn {
    /// The provider completed without requesting another application tool.
    Completed,
    /// The provider requested a bounded set of application tools.
    ToolCalls {
        /// Zero-based provider turn index.
        provider_turn_index: u32,
        /// Number of exact calls that must be durably resolved.
        call_count: usize,
    },
}

/// Exact continuation material for the next provider plan.
///
/// Fields are private so call IDs and model-visible tool outputs cannot be
/// swapped after the guard matched every durable result to the preceding turn.
#[derive(Clone, Debug)]
pub struct AiAgentContinuation {
    continuation: ModelContinuation,
    input: Vec<ModelInputBlock>,
    transfers: Vec<AiEgressManifest>,
    replay_transfers: Vec<AiEgressManifest>,
}

pub(crate) struct AiStatelessToolEvidence {
    pub(crate) provider_turn_index: i64,
    pub(crate) tool_call_index: usize,
    pub(crate) call_id: String,
    pub(crate) tool_id: String,
    pub(crate) provider_name: String,
    pub(crate) tool_fingerprint: String,
    pub(crate) arguments: serde_json::Value,
    pub(crate) output: serde_json::Value,
}

pub(crate) struct AiStatelessConversationEvidence {
    pub(crate) provider_turns: u32,
    pub(crate) current_tool_count: usize,
    pub(crate) tools: Vec<AiStatelessToolEvidence>,
}

impl AiAgentContinuation {
    pub(crate) fn from_persisted_results(
        continuation: ModelContinuation,
        completed_tools: &[AiPersistedApplicationToolCall],
        replay_transfers: Vec<AiEgressManifest>,
    ) -> Result<Self, AiError> {
        if completed_tools.is_empty() || completed_tools.len() > 256 {
            return Err(AiError::Conflict);
        }
        let mut input = Vec::with_capacity(completed_tools.len());
        let mut transfers = Vec::with_capacity(completed_tools.len());
        let mut call_ids = BTreeSet::new();
        for completed in completed_tools {
            if !call_ids.insert(completed.provider_call_id()) {
                return Err(AiError::Conflict);
            }
            input.push(
                completed
                    .model_input()
                    .cloned()
                    .ok_or(AiError::EgressDenied)?,
            );
            transfers.push(
                completed
                    .egress_manifest()
                    .cloned()
                    .ok_or(AiError::EgressDenied)?,
            );
        }
        let candidate = Self {
            continuation,
            input,
            transfers,
            replay_transfers,
        };
        Self::from_checkpoint_value(candidate.checkpoint_value())
    }

    pub(crate) fn checkpoint_value(&self) -> serde_json::Value {
        serde_json::json!({
            "formatVersion": 2,
            "continuation": self.continuation,
            "input": self.input,
            "transfers": self.transfers,
            "replayTransfers": self.replay_transfers,
        })
    }

    pub(crate) fn from_checkpoint_value(value: serde_json::Value) -> Result<Self, AiError> {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct Snapshot {
            format_version: u32,
            continuation: ModelContinuation,
            input: Vec<ModelInputBlock>,
            transfers: Vec<AiEgressManifest>,
            #[serde(default)]
            replay_transfers: Vec<AiEgressManifest>,
        }

        let snapshot: Snapshot =
            serde_json::from_value(value).map_err(|_| AiError::PersistenceFailed)?;
        if !matches!(snapshot.format_version, 1 | 2)
            || snapshot.input.is_empty()
            || snapshot.input.len() > 256
            || snapshot.input.len() != snapshot.transfers.len()
        {
            return Err(AiError::Conflict);
        }
        let mut call_ids = BTreeSet::new();
        for (input, transfer) in snapshot.input.iter().zip(&snapshot.transfers) {
            let ModelInputBlock::ToolResult {
                call_id, tool_id, ..
            } = input
            else {
                return Err(AiError::Conflict);
            };
            if !valid_provider_reference(call_id)
                || tool_id.trim().is_empty()
                || tool_id.len() > 1_024
                || !call_ids.insert(call_id)
                || transfer.capability != crate::AiEgressCapability::ToolResult
            {
                return Err(AiError::Conflict);
            }
        }
        match &snapshot.continuation {
            ModelContinuation::ProviderResponse { response_id } => {
                if !valid_provider_reference(response_id) || !snapshot.replay_transfers.is_empty() {
                    return Err(AiError::Conflict);
                }
            }
            ModelContinuation::StatelessConversation { messages, .. } => {
                let historical_results = messages
                    .iter()
                    .filter(|message| {
                        matches!(message, crate::ModelConversationMessage::Tool { .. })
                    })
                    .count();
                if snapshot.format_version != 2
                    || historical_results != snapshot.replay_transfers.len()
                    || snapshot.replay_transfers.iter().any(|transfer| {
                        transfer.capability != crate::AiEgressCapability::ToolResult
                    })
                    || snapshot
                        .continuation
                        .chain_reference(&snapshot.input)
                        .is_none()
                {
                    return Err(AiError::Conflict);
                }
            }
        }
        let continuation = Self {
            continuation: snapshot.continuation,
            input: snapshot.input,
            transfers: snapshot.transfers,
            replay_transfers: snapshot.replay_transfers,
        };
        if let Some(evidence) = continuation.stateless_evidence()? {
            let historical_count = evidence
                .tools
                .len()
                .checked_sub(evidence.current_tool_count)
                .ok_or(AiError::Conflict)?;
            if historical_count != continuation.replay_transfers.len()
                || evidence.current_tool_count != continuation.transfers.len()
            {
                return Err(AiError::Conflict);
            }
        }
        Ok(continuation)
    }

    pub(crate) fn chain_reference(&self) -> Result<String, AiError> {
        self.continuation
            .chain_reference(&self.input)
            .ok_or(AiError::Conflict)
    }

    pub(crate) fn provider_response_id(&self) -> Option<&str> {
        match &self.continuation {
            ModelContinuation::ProviderResponse { response_id } => Some(response_id),
            ModelContinuation::StatelessConversation { .. } => None,
        }
    }

    pub(crate) fn input(&self) -> &[ModelInputBlock] {
        &self.input
    }

    pub(crate) fn transfers(&self) -> &[AiEgressManifest] {
        &self.transfers
    }

    pub(crate) fn replay_transfers(&self) -> &[AiEgressManifest] {
        &self.replay_transfers
    }

    pub(crate) fn stateless_evidence(
        &self,
    ) -> Result<Option<AiStatelessConversationEvidence>, AiError> {
        let ModelContinuation::StatelessConversation {
            instructions,
            messages,
        } = &self.continuation
        else {
            return Ok(None);
        };
        if instructions.len() > 32
            || instructions
                .iter()
                .any(|instruction| instruction.len() > 1024 * 1024)
            || messages.len() < 2
            || messages.len() > 256
        {
            return Err(AiError::Conflict);
        }
        let mut provider_turns = 0_u32;
        let mut pending: Vec<(usize, &crate::ModelConversationToolCall)> = Vec::new();
        let mut tools = Vec::new();
        let mut call_ids = BTreeSet::new();
        let mut expecting_assistant = false;
        for (message_index, message) in messages.iter().enumerate() {
            match message {
                crate::ModelConversationMessage::User { content }
                    if message_index == 0
                        && !content.is_empty()
                        && content.len() <= 256
                        && content.iter().all(valid_stateless_user_block) =>
                {
                    expecting_assistant = true;
                }
                crate::ModelConversationMessage::Assistant {
                    content,
                    tool_calls,
                } if expecting_assistant
                    && content.len() <= 16 * 1024 * 1024
                    && !tool_calls.is_empty()
                    && tool_calls.len() <= 64 =>
                {
                    provider_turns = provider_turns.checked_add(1).ok_or(AiError::Conflict)?;
                    for (call_index, call) in tool_calls.iter().enumerate() {
                        if !valid_provider_reference(&call.call_id)
                            || call.tool_id.trim().is_empty()
                            || call.tool_id.len() > 200
                            || call.provider_name.trim().is_empty()
                            || call.provider_name.len() > 200
                            || call.tool_fingerprint.trim().is_empty()
                            || call.tool_fingerprint.len() > 512
                            || !call.arguments.is_object()
                            || serde_json::to_vec(&call.arguments)
                                .map_or(true, |encoded| encoded.len() > 16 * 1024 * 1024)
                            || !call_ids.insert(call.call_id.as_str())
                        {
                            return Err(AiError::Conflict);
                        }
                        pending.push((call_index, call));
                    }
                    expecting_assistant = false;
                }
                crate::ModelConversationMessage::Tool {
                    call_id,
                    tool_id,
                    provider_name,
                    output,
                } if !expecting_assistant && !pending.is_empty() => {
                    let (tool_call_index, expected) = pending.remove(0);
                    if call_id != &expected.call_id
                        || tool_id != &expected.tool_id
                        || provider_name != &expected.provider_name
                        || serde_json::to_vec(output)
                            .map_or(true, |encoded| encoded.len() > 16 * 1024 * 1024)
                    {
                        return Err(AiError::Conflict);
                    }
                    tools.push(AiStatelessToolEvidence {
                        provider_turn_index: i64::from(
                            provider_turns.checked_sub(1).ok_or(AiError::Conflict)?,
                        ),
                        tool_call_index,
                        call_id: expected.call_id.clone(),
                        tool_id: expected.tool_id.clone(),
                        provider_name: expected.provider_name.clone(),
                        tool_fingerprint: expected.tool_fingerprint.clone(),
                        arguments: expected.arguments.clone(),
                        output: output.clone(),
                    });
                    if pending.is_empty() {
                        expecting_assistant = true;
                    }
                }
                _ => return Err(AiError::Conflict),
            }
        }
        if expecting_assistant || pending.is_empty() || self.input.len() != pending.len() {
            return Err(AiError::Conflict);
        }
        let current_tool_count = pending.len();
        for ((tool_call_index, expected), input) in pending.into_iter().zip(&self.input) {
            let ModelInputBlock::ToolResult {
                call_id,
                tool_id,
                output,
            } = input
            else {
                return Err(AiError::Conflict);
            };
            if call_id != &expected.call_id
                || tool_id != &expected.tool_id
                || serde_json::to_vec(output)
                    .map_or(true, |encoded| encoded.len() > 16 * 1024 * 1024)
            {
                return Err(AiError::Conflict);
            }
            tools.push(AiStatelessToolEvidence {
                provider_turn_index: i64::from(
                    provider_turns.checked_sub(1).ok_or(AiError::Conflict)?,
                ),
                tool_call_index,
                call_id: expected.call_id.clone(),
                tool_id: expected.tool_id.clone(),
                provider_name: expected.provider_name.clone(),
                tool_fingerprint: expected.tool_fingerprint.clone(),
                arguments: expected.arguments.clone(),
                output: output.clone(),
            });
        }
        if tools.len() > 256 {
            return Err(AiError::Conflict);
        }
        Ok(Some(AiStatelessConversationEvidence {
            provider_turns,
            current_tool_count,
            tools,
        }))
    }

    pub(crate) fn apply_with_transfers(
        self,
        request: &mut ModelRequest,
    ) -> Result<Vec<AiEgressManifest>, AiError> {
        if !request.input.is_empty() || request.continuation.is_some() {
            return Err(AiError::Conflict);
        }
        let expected_mode = match &self.continuation {
            ModelContinuation::ProviderResponse { .. } => {
                crate::ModelContinuationMode::ProviderRetained
            }
            ModelContinuation::StatelessConversation { .. } => {
                crate::ModelContinuationMode::StatelessReplay
            }
        };
        if request.continuation_mode != expected_mode
            || (expected_mode == crate::ModelContinuationMode::StatelessReplay
                && !request.instructions.is_empty())
        {
            return Err(AiError::Conflict);
        }
        request.continuation = Some(self.continuation);
        request.input = self.input;
        let mut transfers = self.replay_transfers;
        transfers.extend(self.transfers);
        Ok(transfers)
    }
}

/// In-memory exact-sequencing guard for a single claimed run attempt.
///
/// The guard complements, but never replaces, durable run/tool fencing. It
/// rejects swapped turns, missing/duplicate call results, response-chain
/// changes, work after terminal completion, and loops exceeding deployment
/// bounds. A new guard must not be reconstructed to resume an uncertain run;
/// restore reconciliation owns that case.
pub struct AiAgentLoopGuard {
    session_id: AiSessionId,
    run_id: AiRunId,
    attempt_id: uuid::Uuid,
    lease_generation: i64,
    limits: AiAgentLoopLimits,
    provider_turns: u32,
    total_tool_calls: u32,
    expected_previous_reference: Option<String>,
    pending_order: Vec<String>,
    pending_tools: BTreeMap<String, String>,
    outputs: BTreeMap<String, ModelInputBlock>,
    output_transfers: BTreeMap<String, AiEgressManifest>,
    pending_continuation: Option<ModelContinuation>,
    replay_transfers: Vec<AiEgressManifest>,
    terminal: bool,
}

impl AiAgentLoopGuard {
    /// Binds a fresh bounded loop guard to one current durable run claim.
    pub fn new(lease: &AiRunLease, limits: AiAgentLoopLimits) -> Self {
        Self {
            session_id: lease.session_id(),
            run_id: lease.run_id(),
            attempt_id: lease.attempt_id(),
            lease_generation: lease.lease_generation(),
            limits,
            provider_turns: 0,
            total_tool_calls: 0,
            expected_previous_reference: None,
            pending_order: Vec::new(),
            pending_tools: BTreeMap::new(),
            outputs: BTreeMap::new(),
            output_transfers: BTreeMap::new(),
            pending_continuation: None,
            replay_transfers: Vec::new(),
            terminal: false,
        }
    }

    pub(crate) fn resume_after_tool_batch(
        lease: &AiRunLease,
        limits: AiAgentLoopLimits,
        provider_turns: u32,
        total_tool_calls: u32,
        previous_reference: &str,
    ) -> Result<Self, AiError> {
        if provider_turns == 0
            || provider_turns > limits.maximum_provider_turns
            || total_tool_calls == 0
            || total_tool_calls > limits.maximum_total_tool_calls
            || !valid_provider_reference(previous_reference)
        {
            return Err(AiError::Conflict);
        }
        Ok(Self {
            session_id: lease.session_id(),
            run_id: lease.run_id(),
            attempt_id: lease.attempt_id(),
            lease_generation: lease.lease_generation(),
            limits,
            provider_turns,
            total_tool_calls,
            expected_previous_reference: Some(previous_reference.to_owned()),
            pending_order: Vec::new(),
            pending_tools: BTreeMap::new(),
            outputs: BTreeMap::new(),
            output_transfers: BTreeMap::new(),
            pending_continuation: None,
            replay_transfers: Vec::new(),
            terminal: false,
        })
    }

    pub(crate) fn can_begin_provider_turn(&self) -> bool {
        !self.terminal
            && self.pending_order.is_empty()
            && self.pending_tools.is_empty()
            && self.outputs.is_empty()
            && self.output_transfers.is_empty()
            && self.pending_continuation.is_none()
            && self.has_provider_turn_capacity()
    }

    pub(crate) fn has_provider_turn_capacity(&self) -> bool {
        self.provider_turns < self.limits.maximum_provider_turns
    }

    /// Accepts the next exactly chained provider result.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Conflict`] for a swapped fence/chain, pending calls,
    /// a terminal loop, duplicate call IDs, unavailable continuation material,
    /// or a hard-limit breach.
    pub fn observe_provider_turn(
        &mut self,
        result: &AiProviderCallResult,
    ) -> Result<AiAgentLoopTurn, AiError> {
        if self.terminal
            || !self.pending_order.is_empty()
            || !self.outputs.is_empty()
            || !self.output_transfers.is_empty()
            || result.session_id() != self.session_id
            || result.run_id() != self.run_id
            || result.attempt_id() != self.attempt_id
            || result.lease_generation() != self.lease_generation
            || result.previous_continuation_reference()
                != self.expected_previous_reference.as_deref()
            || self.provider_turns >= self.limits.maximum_provider_turns
        {
            return Err(AiError::Conflict);
        }
        let provider_turn_index = self.provider_turns;
        let next_provider_turns = self
            .provider_turns
            .checked_add(1)
            .ok_or(AiError::Conflict)?;
        if result.tool_calls().is_empty() {
            self.provider_turns = next_provider_turns;
            self.terminal = true;
            return Ok(AiAgentLoopTurn::Completed);
        }
        let call_count = u32::try_from(result.tool_calls().len()).map_err(|_| AiError::Conflict)?;
        let next_total_tool_calls = self
            .total_tool_calls
            .checked_add(call_count)
            .filter(|count| *count <= self.limits.maximum_total_tool_calls)
            .ok_or(AiError::Conflict)?;
        let continuation = result.next_continuation()?;
        let mut call_ids = BTreeSet::new();
        for call in result.tool_calls() {
            if !call_ids.insert(call.call_id()) {
                return Err(AiError::Conflict);
            }
        }
        self.provider_turns = next_provider_turns;
        self.total_tool_calls = next_total_tool_calls;
        for call in result.tool_calls() {
            self.pending_order.push(call.call_id().to_owned());
            self.pending_tools.insert(
                call.call_id().to_owned(),
                call.tool_id().as_str().to_owned(),
            );
        }
        self.replay_transfers = match &continuation {
            ModelContinuation::ProviderResponse { .. } => Vec::new(),
            ModelContinuation::StatelessConversation { .. } => {
                result.replay_tool_transfers().to_vec()
            }
        };
        self.pending_continuation = Some(continuation);
        Ok(AiAgentLoopTurn::ToolCalls {
            provider_turn_index,
            call_count: result.tool_calls().len(),
        })
    }

    /// Matches one durable, separately egress-authorized tool result to the
    /// current provider turn.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Conflict`] for an unknown/duplicate/swapped result or
    /// [`AiError::EgressDenied`] when no model-visible output was authorized.
    pub fn observe_tool_result(
        &mut self,
        result: &AiPersistedApplicationToolCall,
    ) -> Result<(), AiError> {
        let expected_tool = self
            .pending_tools
            .get(result.provider_call_id())
            .ok_or(AiError::Conflict)?;
        if self.outputs.contains_key(result.provider_call_id()) {
            return Err(AiError::Conflict);
        }
        let input = result.model_input().ok_or(AiError::EgressDenied)?;
        match input {
            ModelInputBlock::ToolResult {
                call_id, tool_id, ..
            } if call_id == result.provider_call_id() && tool_id == expected_tool => {}
            _ => return Err(AiError::Conflict),
        }
        self.outputs
            .insert(result.provider_call_id().to_owned(), input.clone());
        self.output_transfers.insert(
            result.provider_call_id().to_owned(),
            result
                .egress_manifest()
                .cloned()
                .ok_or(AiError::EgressDenied)?,
        );
        Ok(())
    }

    /// Completes the current tool batch and returns exact next-turn input.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Conflict`] until every expected call has exactly one
    /// matched result or when no response continuation is pending.
    pub fn continuation(&mut self) -> Result<AiAgentContinuation, AiError> {
        if self.pending_order.is_empty()
            || self.outputs.len() != self.pending_order.len()
            || self.output_transfers.len() != self.pending_order.len()
            || self.pending_continuation.is_none()
        {
            return Err(AiError::Conflict);
        }
        let mut input = Vec::with_capacity(self.pending_order.len());
        let mut transfers = Vec::with_capacity(self.pending_order.len());
        for call_id in &self.pending_order {
            input.push(self.outputs.remove(call_id).ok_or(AiError::Conflict)?);
            transfers.push(
                self.output_transfers
                    .remove(call_id)
                    .ok_or(AiError::Conflict)?,
            );
        }
        self.pending_order.clear();
        self.pending_tools.clear();
        let continuation = self.pending_continuation.take().ok_or(AiError::Conflict)?;
        let next = AiAgentContinuation {
            continuation,
            input,
            transfers,
            replay_transfers: std::mem::take(&mut self.replay_transfers),
        };
        self.expected_previous_reference = Some(next.chain_reference()?);
        Ok(next)
    }

    /// Number of accepted provider turns.
    pub const fn provider_turns(&self) -> u32 {
        self.provider_turns
    }

    /// Number of accepted custom application-tool requests.
    pub const fn total_tool_calls(&self) -> u32 {
        self.total_tool_calls
    }
}

fn valid_provider_reference(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= 1_024
        && value.bytes().all(|byte| !byte.is_ascii_control())
}

fn valid_stateless_user_block(block: &ModelInputBlock) -> bool {
    match block {
        ModelInputBlock::Text { text } => text.len() <= 16 * 1024 * 1024,
        ModelInputBlock::Json { value } => {
            serde_json::to_vec(value).is_ok_and(|encoded| encoded.len() <= 16 * 1024 * 1024)
        }
        ModelInputBlock::Attachment { .. } | ModelInputBlock::ToolResult { .. } => false,
    }
}
