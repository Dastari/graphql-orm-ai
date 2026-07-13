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
}

impl AiAgentContinuation {
    pub(crate) fn checkpoint_value(&self) -> serde_json::Value {
        serde_json::json!({
            "formatVersion": 1,
            "continuation": self.continuation,
            "input": self.input,
            "transfers": self.transfers,
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
        }

        let snapshot: Snapshot =
            serde_json::from_value(value).map_err(|_| AiError::PersistenceFailed)?;
        if snapshot.format_version != 1
            || snapshot.input.is_empty()
            || snapshot.input.len() > 4_096
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
        let ModelContinuation::ProviderResponse { response_id } = &snapshot.continuation;
        if !valid_provider_reference(response_id) {
            return Err(AiError::Conflict);
        }
        Ok(Self {
            continuation: snapshot.continuation,
            input: snapshot.input,
            transfers: snapshot.transfers,
        })
    }

    pub(crate) fn previous_response_id(&self) -> &str {
        match &self.continuation {
            ModelContinuation::ProviderResponse { response_id } => response_id,
        }
    }

    pub(crate) fn input(&self) -> &[ModelInputBlock] {
        &self.input
    }

    pub(crate) fn transfers(&self) -> &[AiEgressManifest] {
        &self.transfers
    }

    pub(crate) fn apply_with_transfers(
        self,
        request: &mut ModelRequest,
    ) -> Result<Vec<AiEgressManifest>, AiError> {
        if !request.input.is_empty() || request.continuation.is_some() {
            return Err(AiError::Conflict);
        }
        request.continuation = Some(self.continuation);
        request.input = self.input;
        Ok(self.transfers)
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
    expected_previous_response_id: Option<String>,
    pending_order: Vec<String>,
    pending_tools: BTreeMap<String, String>,
    outputs: BTreeMap<String, ModelInputBlock>,
    output_transfers: BTreeMap<String, AiEgressManifest>,
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
            expected_previous_response_id: None,
            pending_order: Vec::new(),
            pending_tools: BTreeMap::new(),
            outputs: BTreeMap::new(),
            output_transfers: BTreeMap::new(),
            terminal: false,
        }
    }

    pub(crate) fn resume_after_tool_batch(
        lease: &AiRunLease,
        limits: AiAgentLoopLimits,
        provider_turns: u32,
        total_tool_calls: u32,
        previous_response_id: &str,
    ) -> Result<Self, AiError> {
        if provider_turns == 0
            || provider_turns > limits.maximum_provider_turns
            || total_tool_calls == 0
            || total_tool_calls > limits.maximum_total_tool_calls
            || !valid_provider_reference(previous_response_id)
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
            expected_previous_response_id: Some(previous_response_id.to_owned()),
            pending_order: Vec::new(),
            pending_tools: BTreeMap::new(),
            outputs: BTreeMap::new(),
            output_transfers: BTreeMap::new(),
            terminal: false,
        })
    }

    /// Accepts the next exactly chained provider result.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Conflict`] for a swapped fence/chain, pending calls,
    /// a terminal loop, duplicate call IDs, absent response ID, or a hard-limit
    /// breach.
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
            || result.previous_response_id() != self.expected_previous_response_id.as_deref()
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
        let response_id = result
            .provider_response_id()
            .map(str::to_owned)
            .ok_or(AiError::Conflict)?;
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
        self.expected_previous_response_id = Some(response_id);
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
            || self.expected_previous_response_id.is_none()
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
        Ok(AiAgentContinuation {
            continuation: ModelContinuation::ProviderResponse {
                response_id: self
                    .expected_previous_response_id
                    .clone()
                    .ok_or(AiError::Conflict)?,
            },
            input,
            transfers,
        })
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
