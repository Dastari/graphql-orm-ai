//! Exporter-neutral, content-free operational telemetry contracts.
//!
//! The observations in this module deliberately contain no prompts, model
//! output, tool arguments/results, GraphQL documents, principal references,
//! provider response IDs, model/profile names, endpoint URLs, secret
//! references, durable resource IDs, or arbitrary error text. They are not an
//! audit log and do not grant authorization or prove durable state.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use crate::{
    AiRestorePlan, AiRestoredRunDisposition, AiRunState, AiRuntimeReadinessReport,
    AiSessionRetentionReport, AiToolOperationDomain, AiToolOperationKind, ProviderKind,
    ToolMaturity,
};

/// Stable schema version for the typed observation vocabulary.
pub const AI_OPERATIONAL_TELEMETRY_SCHEMA_VERSION: u16 = 1;

/// Random correlation for one telemetry operation.
///
/// This ID is created solely for telemetry and is not derived from a session,
/// run, attempt, principal, tool, or provider reference. It is still
/// high-cardinality and must be used only for trace/event correlation, never as
/// a metric attribute.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct AiTelemetryOperationId(Uuid);

impl AiTelemetryOperationId {
    /// Creates a fresh random telemetry-only operation ID.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Returns the random telemetry-only UUID for exporter correlation.
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for AiTelemetryOperationId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for AiTelemetryOperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AiTelemetryOperationId([REDACTED])")
    }
}

/// Content-free completion classification shared by operational observations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AiTelemetryOutcome {
    /// The operation completed and its authoritative work succeeded.
    Succeeded,
    /// The operation was rejected before an external side effect could occur.
    Rejected,
    /// The operation failed with known side-effect certainty.
    Failed,
    /// An external effect may have occurred and automated replay is unsafe.
    Uncertain,
    /// Work was safely retained because a proof, policy, or dependency blocked it.
    Blocked,
}

/// Provider-call observation phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AiProviderTelemetryPhase {
    /// Provider orchestration is about to begin.
    Started,
    /// Provider orchestration has finished with a classified outcome.
    Finished,
}

/// Content-free observation for one provider call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiProviderCallTelemetry {
    operation_id: AiTelemetryOperationId,
    provider_kind: ProviderKind,
    phase: AiProviderTelemetryPhase,
    outcome: Option<AiTelemetryOutcome>,
    duration: Option<Duration>,
    input_tokens: u64,
    output_tokens: u64,
    cached_input_tokens: u64,
}

impl AiProviderCallTelemetry {
    /// Creates a content-free provider-call start observation.
    pub fn started(operation_id: AiTelemetryOperationId, provider_kind: ProviderKind) -> Self {
        Self {
            operation_id,
            provider_kind,
            phase: AiProviderTelemetryPhase::Started,
            outcome: None,
            duration: None,
            input_tokens: 0,
            output_tokens: 0,
            cached_input_tokens: 0,
        }
    }

    /// Creates a content-free provider-call completion observation.
    ///
    /// Token counts must be zero unless the call succeeded. Cached input must
    /// be a subset of total input. Model name is intentionally excluded; a
    /// deployment that exports it must review its own bounded model registry.
    ///
    /// # Errors
    ///
    /// Returns [`crate::AiError::InvalidInput`] for inconsistent token counts.
    pub fn finished(
        operation_id: AiTelemetryOperationId,
        provider_kind: ProviderKind,
        outcome: AiTelemetryOutcome,
        duration: Duration,
        input_tokens: u64,
        output_tokens: u64,
        cached_input_tokens: u64,
    ) -> Result<Self, crate::AiError> {
        if cached_input_tokens > input_tokens
            || (outcome != AiTelemetryOutcome::Succeeded
                && (input_tokens != 0 || output_tokens != 0 || cached_input_tokens != 0))
        {
            return Err(crate::AiError::InvalidInput(
                "invalid provider telemetry counts".to_owned(),
            ));
        }
        Ok(Self {
            operation_id,
            provider_kind,
            phase: AiProviderTelemetryPhase::Finished,
            outcome: Some(outcome),
            duration: Some(duration),
            input_tokens,
            output_tokens,
            cached_input_tokens,
        })
    }

    /// Telemetry-only operation correlation.
    pub const fn operation_id(&self) -> AiTelemetryOperationId {
        self.operation_id
    }

    /// Provider family without endpoint, profile, credential, or model identity.
    pub fn provider_kind(&self) -> &ProviderKind {
        &self.provider_kind
    }

    /// Observation phase.
    pub const fn phase(&self) -> AiProviderTelemetryPhase {
        self.phase
    }

    /// Completion outcome, absent for a start observation.
    pub const fn outcome(&self) -> Option<AiTelemetryOutcome> {
        self.outcome
    }

    /// Completed operation duration, absent for a start observation.
    pub const fn duration(&self) -> Option<Duration> {
        self.duration
    }

    /// Authoritative total input tokens, present only after success.
    pub const fn input_tokens(&self) -> u64 {
        self.input_tokens
    }

    /// Authoritative output tokens, present only after success.
    pub const fn output_tokens(&self) -> u64 {
        self.output_tokens
    }

    /// Authoritative cached input-token subset, present only after success.
    pub const fn cached_input_tokens(&self) -> u64 {
        self.cached_input_tokens
    }

    /// OpenTelemetry GenAI operation value for the current inference contract.
    pub const fn otel_operation_name(&self) -> &'static str {
        "chat"
    }

    /// Standard OpenTelemetry provider value when this crate knows one.
    ///
    /// Profiled compatible endpoints and local harnesses deliberately return
    /// `None`; the host may map a reviewed immutable registration without
    /// exposing an endpoint or arbitrary profile name.
    pub const fn otel_provider_name(&self) -> Option<&'static str> {
        match self.provider_kind {
            ProviderKind::OpenAi => Some("openai"),
            ProviderKind::Anthropic => Some("anthropic"),
            ProviderKind::Xai => Some("x_ai"),
            ProviderKind::Ollama | ProviderKind::OpenAiCompatible | ProviderKind::LocalHarness => {
                None
            }
        }
    }
}

/// Content-free durable run-state transition observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiRunTransitionTelemetry {
    operation_id: AiTelemetryOperationId,
    from: AiRunState,
    to: AiRunState,
}

impl AiRunTransitionTelemetry {
    /// Records an authoritative durable transition without resource identity.
    ///
    /// Restore/recovery transitions may intentionally differ from ordinary
    /// worker state-machine edges, so this observation does not authorize or
    /// validate the transition.
    pub const fn new(
        operation_id: AiTelemetryOperationId,
        from: AiRunState,
        to: AiRunState,
    ) -> Self {
        Self {
            operation_id,
            from,
            to,
        }
    }

    /// Telemetry-only operation correlation.
    pub const fn operation_id(self) -> AiTelemetryOperationId {
        self.operation_id
    }

    /// Prior durable state.
    pub const fn from(self) -> AiRunState {
        self.from
    }

    /// Committed durable state.
    pub const fn to(self) -> AiRunState {
        self.to
    }
}

/// Tool-call observation phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AiToolTelemetryPhase {
    /// Fresh authorization is beginning for an exact registered call.
    Started,
    /// The exact registered call has finished with a classified outcome.
    Finished,
}

/// Content-free application/internal tool-call observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiToolCallTelemetry {
    operation_id: AiTelemetryOperationId,
    operation_kind: AiToolOperationKind,
    operation_domain: AiToolOperationDomain,
    maturity: ToolMaturity,
    phase: AiToolTelemetryPhase,
    outcome: Option<AiTelemetryOutcome>,
    duration: Option<Duration>,
}

impl AiToolCallTelemetry {
    /// Creates a tool-call start observation without tool identity or content.
    pub const fn started(
        operation_id: AiTelemetryOperationId,
        operation_kind: AiToolOperationKind,
        operation_domain: AiToolOperationDomain,
        maturity: ToolMaturity,
    ) -> Self {
        Self {
            operation_id,
            operation_kind,
            operation_domain,
            maturity,
            phase: AiToolTelemetryPhase::Started,
            outcome: None,
            duration: None,
        }
    }

    /// Creates a tool-call completion observation without arguments or result.
    pub const fn finished(
        operation_id: AiTelemetryOperationId,
        operation_kind: AiToolOperationKind,
        operation_domain: AiToolOperationDomain,
        maturity: ToolMaturity,
        outcome: AiTelemetryOutcome,
        duration: Duration,
    ) -> Self {
        Self {
            operation_id,
            operation_kind,
            operation_domain,
            maturity,
            phase: AiToolTelemetryPhase::Finished,
            outcome: Some(outcome),
            duration: Some(duration),
        }
    }

    /// Telemetry-only operation correlation.
    pub const fn operation_id(self) -> AiTelemetryOperationId {
        self.operation_id
    }

    /// Registered GraphQL/internal operation kind.
    pub const fn operation_kind(self) -> AiToolOperationKind {
        self.operation_kind
    }

    /// Registered ownership domain.
    pub const fn operation_domain(self) -> AiToolOperationDomain {
        self.operation_domain
    }

    /// Registered deployment maturity.
    pub const fn maturity(self) -> ToolMaturity {
        self.maturity
    }

    /// Observation phase.
    pub const fn phase(self) -> AiToolTelemetryPhase {
        self.phase
    }

    /// Completion outcome, absent for a start observation.
    pub const fn outcome(self) -> Option<AiTelemetryOutcome> {
        self.outcome
    }

    /// Completed operation duration, absent for a start observation.
    pub const fn duration(self) -> Option<Duration> {
        self.duration
    }
}

/// Content-free aggregate for one expired-run recovery pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiRunRecoveryTelemetry {
    operation_id: AiTelemetryOperationId,
    duration: Duration,
    requeued: u64,
    checkpoint_requeued: u64,
    recovery_required: u64,
    failed: u64,
    completed: u64,
}

impl AiRunRecoveryTelemetry {
    /// Builds an aggregate from one completed bounded recovery pass.
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub fn from_report(
        operation_id: AiTelemetryOperationId,
        duration: Duration,
        report: &crate::AiRunRecoveryReport,
    ) -> Self {
        Self {
            operation_id,
            duration,
            requeued: u64::from(report.requeued),
            checkpoint_requeued: u64::from(report.checkpoint_requeued),
            recovery_required: u64::from(report.recovery_required),
            failed: u64::from(report.failed),
            completed: u64::from(report.completed),
        }
    }

    /// Telemetry-only operation correlation.
    pub const fn operation_id(self) -> AiTelemetryOperationId {
        self.operation_id
    }

    /// Completed pass duration.
    pub const fn duration(self) -> Duration {
        self.duration
    }

    /// Safely requeued pre-provider claims.
    pub const fn requeued(self) -> u64 {
        self.requeued
    }

    /// Safely requeued complete checkpoint claims.
    pub const fn checkpoint_requeued(self) -> u64 {
        self.checkpoint_requeued
    }

    /// Claims moved to manual recovery.
    pub const fn recovery_required(self) -> u64 {
        self.recovery_required
    }

    /// Claims failed after exhausting safe retry.
    pub const fn failed(self) -> u64 {
        self.failed
    }

    /// Claims finalized from exact durable output evidence.
    pub const fn completed(self) -> u64 {
        self.completed
    }
}

/// Content-free aggregate for one bounded session-retention pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiRetentionTelemetry {
    operation_id: AiTelemetryOperationId,
    duration: Duration,
    sessions_scanned: u64,
    sessions_changed: u64,
    sessions_finalized: u64,
    rows_deleted: u64,
    payloads_tombstoned: u64,
    messages_scrubbed: u64,
    cleanups_requested: u64,
    blocked: u64,
    conflicted: u64,
    has_more: bool,
}

impl AiRetentionTelemetry {
    /// Projects one retention report without copying its opaque cursor.
    pub fn from_report(
        operation_id: AiTelemetryOperationId,
        duration: Duration,
        report: &AiSessionRetentionReport,
    ) -> Self {
        let rows_deleted = [
            report.live_delta_events_deleted,
            report.deleting_session_events_deleted,
            report.deleting_session_context_checkpoints_deleted,
            report.context_checkpoints_invalidated,
            report.expired_run_checkpoints_deleted,
            report.deleting_session_run_checkpoints_deleted,
            report.message_blocks_deleted,
            report.deleting_session_attachments_deleted,
            report.deleting_session_attachment_artifacts_deleted,
        ]
        .into_iter()
        .map(u64::from)
        .sum();
        let payloads_tombstoned = [
            report.deleting_session_inbox_payloads_purged,
            report.deleting_session_proposal_payloads_purged,
            report.deleting_session_tool_payloads_purged,
            report.deleting_session_approval_payloads_purged,
            report.expired_tool_payloads_purged,
            report.expired_approval_payloads_purged,
        ]
        .into_iter()
        .map(u64::from)
        .sum();
        let cleanups_requested = [
            report.deleting_session_attachment_cleanups_requested,
            report.deleting_session_attachment_artifact_cleanups_requested,
            report.deleting_session_run_checkpoint_references_cleared,
        ]
        .into_iter()
        .map(u64::from)
        .sum();
        let blocked = [
            report.sessions_not_ready,
            report.messages_blocked,
            report.attachment_cleanups_blocked,
            report.proposal_payload_purges_blocked,
            report.tool_payload_purges_blocked,
            report.raw_payload_purges_blocked,
            report.raw_checkpoint_purges_blocked,
            report.run_checkpoint_purges_blocked,
        ]
        .into_iter()
        .map(u64::from)
        .sum();
        Self {
            operation_id,
            duration,
            sessions_scanned: u64::from(report.sessions_scanned),
            sessions_changed: u64::from(report.sessions_changed),
            sessions_finalized: u64::from(report.deleting_sessions_finalized),
            rows_deleted,
            payloads_tombstoned,
            messages_scrubbed: u64::from(report.message_contents_purged),
            cleanups_requested,
            blocked,
            conflicted: u64::from(report.sessions_conflicted),
            has_more: report.next_session_cursor.is_some(),
        }
    }

    /// Telemetry-only operation correlation.
    pub const fn operation_id(self) -> AiTelemetryOperationId {
        self.operation_id
    }

    /// Completed pass duration.
    pub const fn duration(self) -> Duration {
        self.duration
    }

    /// Sessions scanned.
    pub const fn sessions_scanned(self) -> u64 {
        self.sessions_scanned
    }

    /// Sessions changed.
    pub const fn sessions_changed(self) -> u64 {
        self.sessions_changed
    }

    /// Sessions finalized as dependency-proved deleted shells.
    pub const fn sessions_finalized(self) -> u64 {
        self.sessions_finalized
    }

    /// Protected/metadata rows physically deleted or invalidated.
    pub const fn rows_deleted(self) -> u64 {
        self.rows_deleted
    }

    /// Protected payload fields tombstoned in retained rows.
    pub const fn payloads_tombstoned(self) -> u64 {
        self.payloads_tombstoned
    }

    /// Message previews scrubbed.
    pub const fn messages_scrubbed(self) -> u64 {
        self.messages_scrubbed
    }

    /// External cleanup or checkpoint-reference cleanup work requested.
    pub const fn cleanups_requested(self) -> u64 {
        self.cleanups_requested
    }

    /// Safe blockers observed across proof categories.
    pub const fn blocked(self) -> u64 {
        self.blocked
    }

    /// Sessions whose CAS transaction conflicted.
    pub const fn conflicted(self) -> u64 {
        self.conflicted
    }

    /// Whether the bounded scan returned an opaque next-page cursor.
    pub const fn has_more(self) -> bool {
        self.has_more
    }
}

/// Content-free aggregate for one dry-run restore plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiRestorePlanTelemetry {
    operation_id: AiTelemetryOperationId,
    duration: Duration,
    preserved_terminal: u64,
    requeued: u64,
    recovery_required: u64,
    approvals_to_revalidate: u64,
    consents_to_revalidate: u64,
    fatal_issues: u64,
    warning_issues: u64,
}

impl AiRestorePlanTelemetry {
    /// Projects a restore plan without fingerprints, issue text, or run IDs.
    pub fn from_plan(
        operation_id: AiTelemetryOperationId,
        duration: Duration,
        plan: &AiRestorePlan,
    ) -> Self {
        let mut preserved_terminal = 0_u64;
        let mut requeued = 0_u64;
        let mut recovery_required = 0_u64;
        for action in &plan.run_actions {
            match action.disposition {
                AiRestoredRunDisposition::PreserveTerminal => preserved_terminal += 1,
                AiRestoredRunDisposition::RequeueWithNewAttempt => requeued += 1,
                AiRestoredRunDisposition::RecoveryRequired => recovery_required += 1,
            }
        }
        let fatal_issues = plan.fatal_issue_count();
        Self {
            operation_id,
            duration,
            preserved_terminal,
            requeued,
            recovery_required,
            approvals_to_revalidate: plan.approvals_to_revalidate,
            consents_to_revalidate: plan.consents_to_revalidate,
            fatal_issues,
            warning_issues: u64::try_from(plan.issues.len())
                .unwrap_or(u64::MAX)
                .saturating_sub(fatal_issues),
        }
    }

    /// Telemetry-only operation correlation.
    pub const fn operation_id(self) -> AiTelemetryOperationId {
        self.operation_id
    }

    /// Completed planning duration.
    pub const fn duration(self) -> Duration {
        self.duration
    }

    /// Terminal runs preserved.
    pub const fn preserved_terminal(self) -> u64 {
        self.preserved_terminal
    }

    /// Runs safe to requeue under a new fence.
    pub const fn requeued(self) -> u64 {
        self.requeued
    }

    /// Runs requiring manual recovery.
    pub const fn recovery_required(self) -> u64 {
        self.recovery_required
    }

    /// Approval rows requiring current-policy revalidation.
    pub const fn approvals_to_revalidate(self) -> u64 {
        self.approvals_to_revalidate
    }

    /// Egress-consent rows requiring current-policy revalidation.
    pub const fn consents_to_revalidate(self) -> u64 {
        self.consents_to_revalidate
    }

    /// Fatal restore issues.
    pub const fn fatal_issues(self) -> u64 {
        self.fatal_issues
    }

    /// Nonfatal restore warnings.
    pub const fn warning_issues(self) -> u64 {
        self.warning_issues
    }
}

/// Content-free runtime start-gate evaluation after restore application.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiRestoreReadinessTelemetry {
    operation_id: AiTelemetryOperationId,
    executor_bound: bool,
    restore_reconciled: bool,
    fatal_issue_count: u64,
    non_fingerprint_requirements_ready: bool,
}

impl AiRestoreReadinessTelemetry {
    /// Projects readiness without copying the module fingerprint.
    pub fn from_report(
        operation_id: AiTelemetryOperationId,
        report: &AiRuntimeReadinessReport,
    ) -> Self {
        Self {
            operation_id,
            executor_bound: report.executor_bound,
            restore_reconciled: report.restore_reconciled,
            fatal_issue_count: report.fatal_issue_count,
            non_fingerprint_requirements_ready: report.executor_bound
                && report.restore_reconciled
                && report.fatal_issue_count == 0,
        }
    }

    /// Telemetry-only operation correlation.
    pub const fn operation_id(self) -> AiTelemetryOperationId {
        self.operation_id
    }

    /// Whether the finished host GraphQL executor was bound.
    pub const fn executor_bound(self) -> bool {
        self.executor_bound
    }

    /// Whether restore reconciliation completed.
    pub const fn restore_reconciled(self) -> bool {
        self.restore_reconciled
    }

    /// Fatal issue count.
    pub const fn fatal_issue_count(self) -> u64 {
        self.fatal_issue_count
    }

    /// Content-free readiness evaluation, excluding the fingerprint comparison.
    ///
    /// The authoritative start gate separately compares the exact private
    /// module fingerprint; this convenience value is not startup authority.
    pub const fn non_fingerprint_requirements_ready(self) -> bool {
        self.non_fingerprint_requirements_ready
    }
}

/// Typed content-free event accepted by an operational telemetry sink.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AiOperationalTelemetryEvent {
    /// Provider call lifecycle.
    ProviderCall(AiProviderCallTelemetry),
    /// Authoritative durable run transition.
    RunTransition(AiRunTransitionTelemetry),
    /// Application/internal tool call lifecycle.
    ToolCall(AiToolCallTelemetry),
    /// Completed expired-run recovery pass.
    RunRecovery(AiRunRecoveryTelemetry),
    /// Completed bounded retention pass.
    Retention(AiRetentionTelemetry),
    /// Completed dry-run restore plan.
    RestorePlan(AiRestorePlanTelemetry),
    /// Restore/start-gate readiness evaluation.
    RestoreReadiness(AiRestoreReadinessTelemetry),
}

impl AiOperationalTelemetryEvent {
    /// Stable low-cardinality event name for exporter adapters.
    pub const fn event_name(&self) -> &'static str {
        match self {
            Self::ProviderCall(_) => "graphql_orm_ai.provider.call",
            Self::RunTransition(_) => "graphql_orm_ai.run.transition",
            Self::ToolCall(_) => "graphql_orm_ai.tool.call",
            Self::RunRecovery(_) => "graphql_orm_ai.run.recovery",
            Self::Retention(_) => "graphql_orm_ai.retention.pass",
            Self::RestorePlan(_) => "graphql_orm_ai.restore.plan",
            Self::RestoreReadiness(_) => "graphql_orm_ai.restore.readiness",
        }
    }
}

/// Deployment-owned exporter boundary for content-free operational telemetry.
///
/// Implementations should enqueue and return promptly. The method is
/// intentionally synchronous and infallible so exporter availability cannot
/// change authoritative provider, tool, recovery, retention, or restore state.
/// A deployment may drop observations under bounded backpressure. It must not
/// enrich them with prompt/output/tool content, credentials, endpoint URLs,
/// principal or durable resource IDs, or arbitrary error messages. Operation
/// IDs are trace/event correlation only and must never become metric labels.
pub trait AiOperationalTelemetrySink: Send + Sync {
    /// Records one owned observation for asynchronous export.
    fn record(&self, event: AiOperationalTelemetryEvent);
}

/// Cloneable content-free telemetry emitter.
#[derive(Clone)]
pub struct AiOperationalTelemetry {
    sink: Arc<dyn AiOperationalTelemetrySink>,
}

impl AiOperationalTelemetry {
    /// Creates an emitter for one deployment-owned sink.
    pub fn new(sink: Arc<dyn AiOperationalTelemetrySink>) -> Self {
        Self { sink }
    }

    /// Records one event without affecting authoritative runtime outcomes.
    pub fn record(&self, event: AiOperationalTelemetryEvent) {
        self.sink.record(event);
    }
}

impl fmt::Debug for AiOperationalTelemetry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiOperationalTelemetry")
            .field("sink", &"[REDACTED]")
            .finish()
    }
}

/// Sink that intentionally discards every operational observation.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopAiOperationalTelemetrySink;

impl AiOperationalTelemetrySink for NoopAiOperationalTelemetrySink {
    fn record(&self, _event: AiOperationalTelemetryEvent) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AiRestoreIssue, AiRestoreIssueSeverity, AiRestoredRunAction, AiRunId};

    #[test]
    fn provider_observations_follow_content_free_otel_mapping() {
        let operation_id = AiTelemetryOperationId::new();
        let started = AiProviderCallTelemetry::started(operation_id, ProviderKind::OpenAi);
        assert_eq!(started.otel_operation_name(), "chat");
        assert_eq!(started.otel_provider_name(), Some("openai"));
        assert_eq!(started.outcome(), None);
        assert!(!format!("{started:?}").contains(&operation_id.as_uuid().to_string()));

        let finished = AiProviderCallTelemetry::finished(
            operation_id,
            ProviderKind::OpenAiCompatible,
            AiTelemetryOutcome::Succeeded,
            Duration::from_millis(125),
            20,
            5,
            4,
        )
        .expect("valid successful observation");
        assert_eq!(finished.otel_provider_name(), None);
        assert_eq!(finished.cached_input_tokens(), 4);
        assert!(
            AiProviderCallTelemetry::finished(
                operation_id,
                ProviderKind::OpenAi,
                AiTelemetryOutcome::Uncertain,
                Duration::ZERO,
                1,
                0,
                0,
            )
            .is_err()
        );
    }

    #[test]
    fn retention_projection_omits_cursor_and_preserves_categories() {
        let report = AiSessionRetentionReport {
            sessions_scanned: 5,
            sessions_changed: 2,
            deleting_session_inbox_payloads_purged: 3,
            message_blocks_deleted: 4,
            message_contents_purged: 1,
            deleting_session_attachment_cleanups_requested: 2,
            messages_blocked: 6,
            sessions_conflicted: 1,
            deleting_sessions_finalized: 1,
            next_session_cursor: Some("sensitive-opaque-cursor".to_owned()),
            ..AiSessionRetentionReport::default()
        };
        let projected = AiRetentionTelemetry::from_report(
            AiTelemetryOperationId::new(),
            Duration::from_secs(1),
            &report,
        );
        assert_eq!(projected.sessions_scanned(), 5);
        assert_eq!(projected.payloads_tombstoned(), 3);
        assert_eq!(projected.rows_deleted(), 4);
        assert_eq!(projected.messages_scrubbed(), 1);
        assert_eq!(projected.cleanups_requested(), 2);
        assert_eq!(projected.blocked(), 6);
        assert_eq!(projected.conflicted(), 1);
        assert!(projected.has_more());
        assert!(!format!("{projected:?}").contains("sensitive-opaque-cursor"));
    }

    #[test]
    fn restore_projection_omits_fingerprints_issue_text_and_run_ids() {
        let run_id = AiRunId::new();
        let plan = AiRestorePlan {
            expected_module_fingerprint: "private-fingerprint".to_owned(),
            run_actions: vec![AiRestoredRunAction {
                run_id,
                disposition: AiRestoredRunDisposition::RecoveryRequired,
                clear_lease: true,
                reverify_provider_continuation: true,
                reverify_provider_file: false,
            }],
            approvals_to_revalidate: 2,
            consents_to_revalidate: 3,
            issues: vec![AiRestoreIssue {
                code: "PRIVATE_ISSUE_TEXT".to_owned(),
                severity: AiRestoreIssueSeverity::Fatal,
                resource_ref: Some("private-resource".to_owned()),
            }],
        };
        let projected = AiRestorePlanTelemetry::from_plan(
            AiTelemetryOperationId::new(),
            Duration::from_millis(2),
            &plan,
        );
        assert_eq!(projected.recovery_required(), 1);
        assert_eq!(projected.fatal_issues(), 1);
        let debug = format!("{projected:?}");
        assert!(!debug.contains("private-fingerprint"));
        assert!(!debug.contains("PRIVATE_ISSUE_TEXT"));
        assert!(!debug.contains(&run_id.0.to_string()));
    }
}
