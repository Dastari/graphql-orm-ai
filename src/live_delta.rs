//! Bounded, UTF-8-safe coalescing for trusted live provider text delivery.

use std::time::Instant;

#[cfg(any(feature = "sqlite", feature = "postgres"))]
use async_trait::async_trait;

#[cfg(any(feature = "sqlite", feature = "postgres"))]
use crate::{AiBudgetReservationId, AiRunId, AiRunLease, AiScope, AiSessionId, ProviderKind};
use crate::{AiError, ProviderEvent};

/// Model-visible live delta class.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum AiLiveDeltaKind {
    /// Visible assistant text.
    Text,
    /// Provider-supported visible reasoning summary, never hidden reasoning.
    ReasoningSummary,
}

/// Deployment hard bounds for live provider delta batching.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiLiveDeltaCoalescerLimits {
    maximum_delay: std::time::Duration,
    maximum_bytes: usize,
}

impl AiLiveDeltaCoalescerLimits {
    /// Creates bounds no weaker than the runtime's 50 ms / 4 KiB contract.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless the delay is in
    /// `1 ms..=50 ms` and the byte ceiling is in `4..=4096`. Four bytes ensures
    /// any one valid Unicode scalar can be emitted without splitting UTF-8.
    pub fn new(maximum_delay: std::time::Duration, maximum_bytes: usize) -> Result<Self, AiError> {
        if maximum_delay < std::time::Duration::from_millis(1)
            || maximum_delay > std::time::Duration::from_millis(50)
            || !(4..=4_096).contains(&maximum_bytes)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid live-delta coalescer limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_delay,
            maximum_bytes,
        })
    }

    /// Maximum time a non-empty batch may remain buffered.
    pub const fn maximum_delay(&self) -> std::time::Duration {
        self.maximum_delay
    }

    /// Maximum UTF-8 byte length of any emitted batch.
    pub const fn maximum_bytes(&self) -> usize {
        self.maximum_bytes
    }
}

impl Default for AiLiveDeltaCoalescerLimits {
    fn default() -> Self {
        Self {
            maximum_delay: std::time::Duration::from_millis(50),
            maximum_bytes: 4_096,
        }
    }
}

/// One bounded model-visible live batch.
///
/// Text remains sensitive application content. A trusted backend sink must
/// bind it to the current run fence, apply the scope content-protection policy,
/// and persist it before emitting a durable cursor event. This value by itself
/// is neither durable nor authorized for external disclosure.
#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use = "live delta batches must be persisted/delivered or explicitly discarded"]
pub struct AiLiveDeltaBatch {
    sequence: u64,
    kind: AiLiveDeltaKind,
    text: String,
}

impl AiLiveDeltaBatch {
    /// Monotonic batch order within this coalescer instance.
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Visible content class.
    pub const fn kind(&self) -> AiLiveDeltaKind {
        self.kind
    }

    /// Bounded UTF-8 content.
    pub fn text(&self) -> &str {
        &self.text
    }
}

/// Exact server-owned binding for one provisional provider-visible batch.
///
/// The context binds a batch to the current session/run attempt, settled
/// provider route selection, uncertain budget reservation, scope, and audit
/// correlation. It proves no current access, content protection, durable
/// persistence, or client disclosure by itself; an [`AiLiveDeltaSink`] must
/// recheck those properties for every batch.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub struct AiLiveDeltaPersistenceContext {
    session_id: AiSessionId,
    run_id: AiRunId,
    attempt_id: uuid::Uuid,
    lease_generation: i64,
    scope: AiScope,
    correlation_id: String,
    provider_kind: ProviderKind,
    provider_model: String,
    provider_response_id: Option<String>,
    budget_reservation_id: AiBudgetReservationId,
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
impl AiLiveDeltaPersistenceContext {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        lease: &AiRunLease,
        scope: AiScope,
        correlation_id: String,
        provider_kind: ProviderKind,
        provider_model: String,
        provider_response_id: Option<String>,
        budget_reservation_id: AiBudgetReservationId,
    ) -> Self {
        Self {
            session_id: lease.session_id(),
            run_id: lease.run_id(),
            attempt_id: lease.attempt_id(),
            lease_generation: lease.lease_generation(),
            scope,
            correlation_id,
            provider_kind,
            provider_model,
            provider_response_id,
            budget_reservation_id,
        }
    }

    /// Session receiving the protected durable event.
    pub const fn session_id(&self) -> AiSessionId {
        self.session_id
    }

    /// Run producing the provisional content.
    pub const fn run_id(&self) -> AiRunId {
        self.run_id
    }

    /// Exact provider-call attempt.
    pub const fn attempt_id(&self) -> uuid::Uuid {
        self.attempt_id
    }

    /// Exact durable fencing generation.
    pub const fn lease_generation(&self) -> i64 {
        self.lease_generation
    }

    /// Application-defined access and protection scope.
    pub fn scope(&self) -> &AiScope {
        &self.scope
    }

    /// Server-owned correlation identifier.
    pub fn correlation_id(&self) -> &str {
        &self.correlation_id
    }

    /// Provider family selected by the host plan.
    pub const fn provider_kind(&self) -> &ProviderKind {
        &self.provider_kind
    }

    /// Exact selected provider model.
    pub fn provider_model(&self) -> &str {
        &self.provider_model
    }

    /// Provider response reference observed before this batch, when available.
    pub fn provider_response_id(&self) -> Option<&str> {
        self.provider_response_id.as_deref()
    }

    /// Atomic budget reservation currently marked uncertain for this turn.
    pub const fn budget_reservation_id(&self) -> AiBudgetReservationId {
        self.budget_reservation_id
    }
}

/// Protected durable persistence boundary for provisional visible provider
/// batches.
///
/// Implementations must rehydrate current authority, verify the live run fence
/// and uncertain provider budget, protect the batch, and commit its durable
/// cursor event before returning. They must never persist tool arguments,
/// hidden reasoning, raw provider frames, or unbounded content.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
#[async_trait]
pub trait AiLiveDeltaSink: Send + Sync {
    /// Persists one exact ordered visible batch.
    ///
    /// # Errors
    ///
    /// Returns a safe error for stale fencing, current access/protection
    /// denial, mismatched provider/budget binding, malformed content, or a
    /// persistence failure. An error occurs after provider transport began and
    /// therefore must not cause automatic provider replay.
    async fn persist_batch(
        &self,
        lease: &AiRunLease,
        context: &AiLiveDeltaPersistenceContext,
        batch: &AiLiveDeltaBatch,
    ) -> Result<(), AiError>;
}

struct PendingDelta {
    text: String,
    started_at: Instant,
    first_fragment_order: u64,
}

/// In-memory batching state for one provider turn.
///
/// Call [`Self::flush_due`] from a timer no later than
/// [`AiLiveDeltaCoalescerLimits::maximum_delay`], even when the provider stream
/// is idle. Call [`Self::flush_all`] before final output persistence. The
/// coalescer is deliberately synchronous and performs no I/O, locking, task
/// spawning, authorization, persistence, or delivery.
pub struct AiLiveDeltaCoalescer {
    limits: AiLiveDeltaCoalescerLimits,
    text: Option<PendingDelta>,
    reasoning_summary: Option<PendingDelta>,
    next_fragment_order: u64,
    next_batch_sequence: u64,
}

impl AiLiveDeltaCoalescer {
    /// Creates empty batching state for one exact provider turn.
    pub const fn new(limits: AiLiveDeltaCoalescerLimits) -> Self {
        Self {
            limits,
            text: None,
            reasoning_summary: None,
            next_fragment_order: 0,
            next_batch_sequence: 0,
        }
    }

    /// Accepts one normalized provider event and returns every batch made ready
    /// by elapsed time, the byte ceiling, or a visible-content kind boundary.
    /// Kind changes flush the preceding batch so provider event order is never
    /// reversed.
    ///
    /// Non-visible and structured events are intentionally ignored. An empty
    /// text delta only triggers time-based flushing.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::ProviderFailed`] if monotonic sequencing overflows.
    pub fn push_event(
        &mut self,
        event: &ProviderEvent,
        now: Instant,
    ) -> Result<Vec<AiLiveDeltaBatch>, AiError> {
        let mut batches = self.flush_due(now)?;
        let (kind, fragment) = match event {
            ProviderEvent::TextDelta { text } => (AiLiveDeltaKind::Text, text.as_str()),
            ProviderEvent::ReasoningSummaryDelta { text } => {
                (AiLiveDeltaKind::ReasoningSummary, text.as_str())
            }
            _ => return Ok(batches),
        };
        if fragment.is_empty() {
            return Ok(batches);
        }
        let preceding = match kind {
            AiLiveDeltaKind::Text => self
                .reasoning_summary
                .take()
                .map(|pending| (AiLiveDeltaKind::ReasoningSummary, pending)),
            AiLiveDeltaKind::ReasoningSummary => self
                .text
                .take()
                .map(|pending| (AiLiveDeltaKind::Text, pending)),
        };
        if let Some((preceding_kind, pending)) = preceding {
            batches.push(self.batch(preceding_kind, pending)?);
        }
        let mut pending = self.take(kind);
        let mut remainder = fragment;
        while !remainder.is_empty() {
            let buffer = pending.get_or_insert_with(|| PendingDelta {
                text: String::new(),
                started_at: now,
                first_fragment_order: self.next_fragment_order,
            });
            if buffer.text.is_empty() {
                self.next_fragment_order = self
                    .next_fragment_order
                    .checked_add(1)
                    .ok_or(AiError::ProviderFailed)?;
            }
            let capacity = self
                .limits
                .maximum_bytes
                .checked_sub(buffer.text.len())
                .ok_or(AiError::ProviderFailed)?;
            let split = utf8_prefix_len(remainder, capacity);
            if split == 0 {
                let completed = pending.take().ok_or(AiError::ProviderFailed)?;
                batches.push(self.batch(kind, completed)?);
                continue;
            }
            buffer.text.push_str(&remainder[..split]);
            remainder = &remainder[split..];
            if buffer.text.len() == self.limits.maximum_bytes {
                let completed = pending.take().ok_or(AiError::ProviderFailed)?;
                batches.push(self.batch(kind, completed)?);
            }
        }
        self.put(kind, pending);
        Ok(batches)
    }

    /// Flushes batches whose maximum delay has elapsed.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::ProviderFailed`] if monotonic batch sequencing
    /// overflows.
    pub fn flush_due(&mut self, now: Instant) -> Result<Vec<AiLiveDeltaBatch>, AiError> {
        let mut due = Vec::new();
        if self.text.as_ref().is_some_and(|pending| {
            now.saturating_duration_since(pending.started_at) >= self.limits.maximum_delay
        }) {
            due.push((
                AiLiveDeltaKind::Text,
                self.text.take().expect("due text batch exists"),
            ));
        }
        if self.reasoning_summary.as_ref().is_some_and(|pending| {
            now.saturating_duration_since(pending.started_at) >= self.limits.maximum_delay
        }) {
            due.push((
                AiLiveDeltaKind::ReasoningSummary,
                self.reasoning_summary
                    .take()
                    .expect("due reasoning-summary batch exists"),
            ));
        }
        due.sort_by_key(|(_, pending)| pending.first_fragment_order);
        due.into_iter()
            .map(|(kind, pending)| self.batch(kind, pending))
            .collect()
    }

    /// Flushes every non-empty batch in first-fragment order.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::ProviderFailed`] if monotonic batch sequencing
    /// overflows.
    pub fn flush_all(&mut self) -> Result<Vec<AiLiveDeltaBatch>, AiError> {
        let mut pending = Vec::new();
        if let Some(text) = self.text.take() {
            pending.push((AiLiveDeltaKind::Text, text));
        }
        if let Some(reasoning) = self.reasoning_summary.take() {
            pending.push((AiLiveDeltaKind::ReasoningSummary, reasoning));
        }
        pending.sort_by_key(|(_, item)| item.first_fragment_order);
        pending
            .into_iter()
            .map(|(kind, item)| self.batch(kind, item))
            .collect()
    }

    fn take(&mut self, kind: AiLiveDeltaKind) -> Option<PendingDelta> {
        match kind {
            AiLiveDeltaKind::Text => self.text.take(),
            AiLiveDeltaKind::ReasoningSummary => self.reasoning_summary.take(),
        }
    }

    fn put(&mut self, kind: AiLiveDeltaKind, pending: Option<PendingDelta>) {
        match kind {
            AiLiveDeltaKind::Text => self.text = pending,
            AiLiveDeltaKind::ReasoningSummary => self.reasoning_summary = pending,
        }
    }

    fn batch(
        &mut self,
        kind: AiLiveDeltaKind,
        pending: PendingDelta,
    ) -> Result<AiLiveDeltaBatch, AiError> {
        if pending.text.is_empty() || pending.text.len() > self.limits.maximum_bytes {
            return Err(AiError::ProviderFailed);
        }
        let sequence = self.next_batch_sequence;
        self.next_batch_sequence = self
            .next_batch_sequence
            .checked_add(1)
            .ok_or(AiError::ProviderFailed)?;
        Ok(AiLiveDeltaBatch {
            sequence,
            kind,
            text: pending.text,
        })
    }
}

fn utf8_prefix_len(value: &str, maximum_bytes: usize) -> usize {
    if value.len() <= maximum_bytes {
        return value.len();
    }
    let mut end = maximum_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    end
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(maximum_bytes: usize) -> AiLiveDeltaCoalescerLimits {
        AiLiveDeltaCoalescerLimits::new(std::time::Duration::from_millis(50), maximum_bytes)
            .expect("test limits should validate")
    }

    #[test]
    fn rejects_bounds_weaker_than_the_runtime_contract() {
        assert!(
            AiLiveDeltaCoalescerLimits::new(std::time::Duration::from_millis(51), 4_096).is_err()
        );
        assert!(
            AiLiveDeltaCoalescerLimits::new(std::time::Duration::from_millis(50), 4_097).is_err()
        );
        assert!(AiLiveDeltaCoalescerLimits::new(std::time::Duration::ZERO, 4_096).is_err());
    }

    #[test]
    fn byte_ceiling_splits_only_on_utf8_boundaries() {
        let start = Instant::now();
        let mut coalescer = AiLiveDeltaCoalescer::new(limits(7));
        let batches = coalescer
            .push_event(
                &ProviderEvent::TextDelta {
                    text: "ab😀cd😀ef".to_owned(),
                },
                start,
            )
            .expect("valid text should coalesce");
        let mut batches = batches;
        batches.extend(coalescer.flush_all().expect("remainder should flush"));

        assert_eq!(
            batches
                .iter()
                .map(AiLiveDeltaBatch::text)
                .collect::<Vec<_>>(),
            vec!["ab😀c", "d😀ef"]
        );
        assert!(batches.iter().all(|batch| batch.text().len() <= 7));
        assert_eq!(batches[0].sequence(), 0);
        assert_eq!(batches[1].sequence(), 1);
    }

    #[test]
    fn timer_flushes_idle_text_at_fifty_milliseconds() {
        let start = Instant::now();
        let mut coalescer = AiLiveDeltaCoalescer::new(limits(4_096));
        assert!(
            coalescer
                .push_event(
                    &ProviderEvent::TextDelta {
                        text: "partial".to_owned(),
                    },
                    start,
                )
                .expect("text should buffer")
                .is_empty()
        );
        assert!(
            coalescer
                .flush_due(start + std::time::Duration::from_millis(49))
                .expect("early flush should be valid")
                .is_empty()
        );
        let batches = coalescer
            .flush_due(start + std::time::Duration::from_millis(50))
            .expect("due flush should succeed");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].text(), "partial");
    }

    #[test]
    fn interleaved_visible_kinds_flush_in_first_fragment_order() {
        let start = Instant::now();
        let mut coalescer = AiLiveDeltaCoalescer::new(limits(4_096));
        let mut batches = coalescer
            .push_event(
                &ProviderEvent::ReasoningSummaryDelta {
                    text: "summary".to_owned(),
                },
                start,
            )
            .expect("summary should buffer");
        batches.extend(
            coalescer
                .push_event(
                    &ProviderEvent::TextDelta {
                        text: "answer".to_owned(),
                    },
                    start,
                )
                .expect("text should buffer"),
        );
        batches.extend(coalescer.flush_all().expect("all batches should flush"));

        assert_eq!(batches[0].kind(), AiLiveDeltaKind::ReasoningSummary);
        assert_eq!(batches[1].kind(), AiLiveDeltaKind::Text);
    }

    #[test]
    fn structured_events_never_enter_visible_delta_batches() {
        let mut coalescer = AiLiveDeltaCoalescer::new(limits(4_096));
        let batches = coalescer
            .push_event(
                &ProviderEvent::ToolCallCompleted {
                    call_id: "call-1".to_owned(),
                    arguments: serde_json::json!({"secret": "not-a-live-delta"}),
                },
                Instant::now(),
            )
            .expect("structured event should be ignored");
        assert!(batches.is_empty());
        assert!(
            coalescer
                .flush_all()
                .expect("empty flush should succeed")
                .is_empty()
        );
    }
}
