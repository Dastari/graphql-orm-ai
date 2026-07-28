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
    PredicateUpdateOutcome, TransactionError, TransactionMode,
};
use sha2::{Digest, Sha256};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::orm_inbox::{PreparedAiInboxEvent, append_inbox_event};
use crate::orm_provider_output::{
    AiProviderOutputLimits, BackgroundProviderOutputPreparation, prepare_background_provider_output,
};
use crate::orm_runs::{
    PreparedProviderOutput, append_attempt_outcome, canonical_second, exact_state,
    lease_from_record, load_and_validate_active_lease, validate_worker_id,
};
use crate::persistence::*;
use crate::{
    AI_EGRESS_RETENTION_PROVIDER_RESPONSE, AiBudgetAmounts, AiBudgetReconciliation,
    AiBudgetReconciliationOutcome, AiBudgetReservationId, AiBudgetService, AiDataSourceRef,
    AiDestinationTrust, AiEgressCapability, AiEgressDecisionAudit, AiEgressManifest, AiError,
    AiProviderCallPlan, AiProviderUsageAccounting, AiProviderUsageObservation, AiRunId, AiRunLease,
    AiRunState, AiRuntime, AiScope, AiSessionAction, AiSourceTrust, DataClassification,
    ModelContinuationMode, ModelInputBlock, OrmAiRunService, ProviderBackgroundBinding,
    ProviderBackgroundObservation, ProviderBackgroundRetrievalBinding,
    ProviderBackgroundRetrievalContext, ProviderBackgroundStatus, ProviderBackgroundSubmission,
    ProviderError, ProviderKind, ProviderRequestContext,
};

const MAXIMUM_TEMPORARY_RESPONSE_WINDOW: Duration = Duration::minutes(10);
const MAXIMUM_STORED_RESPONSE_WINDOW: Duration = Duration::days(30);
const MAXIMUM_RECONCILIATION_LEASE_TTL: Duration = Duration::minutes(5);
const MAXIMUM_RECONCILIATION_RETRY_DELAY: Duration = Duration::hours(1);
const MAXIMUM_BACKGROUND_RETRIEVAL_BYTES: usize = 64 * 1024 * 1024;
const MAXIMUM_BACKGROUND_RETRIEVAL_ITEMS: usize = 4_096;
const MAXIMUM_BACKGROUND_RETRIEVAL_TIMEOUT: Duration = Duration::minutes(5);
const MAXIMUM_BACKGROUND_PRINCIPAL_AGE: Duration = Duration::hours(1);

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

/// Fixed logical OpenAI route used for current background-response retrieval.
///
/// The route contains no URL or credential. The native adapter remains fixed
/// to OpenAI's official Responses endpoint and resolves its registered secret
/// just in time. A retrieval claim must match both the logical profile and the
/// original audited destination before a current egress decision can be bound.
#[derive(Clone, PartialEq, Eq)]
pub struct AiOpenAiBackgroundRetrievalRoute {
    provider_profile_id: String,
    destination: String,
    residency: Option<String>,
    policy_version: String,
    consent_reference: Option<String>,
}

impl std::fmt::Debug for AiOpenAiBackgroundRetrievalRoute {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AiOpenAiBackgroundRetrievalRoute")
            .field("provider_profile_id", &"[REDACTED]")
            .field("destination", &self.destination)
            .field("residency", &self.residency)
            .field("policy_version", &self.policy_version)
            .field("consent_reference", &"[REDACTED]")
            .finish()
    }
}

impl AiOpenAiBackgroundRetrievalRoute {
    /// Creates a fixed logical retrieval route.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] for empty, oversized, or
    /// control-character profile, destination, or policy values.
    pub fn new(
        provider_profile_id: impl Into<String>,
        destination: impl Into<String>,
        policy_version: impl Into<String>,
    ) -> Result<Self, AiError> {
        let route = Self {
            provider_profile_id: provider_profile_id.into(),
            destination: destination.into(),
            residency: None,
            policy_version: policy_version.into(),
            consent_reference: None,
        };
        route.validate()?;
        Ok(route)
    }

    /// Adds a reviewed processing residency/region class.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] for an empty, oversized, or
    /// control-character value.
    pub fn with_residency(mut self, residency: impl Into<String>) -> Result<Self, AiError> {
        self.residency = Some(residency.into());
        self.validate()?;
        Ok(self)
    }

    /// Adds a current purpose-bound consent/grant reference.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] for an empty, oversized, or
    /// control-character value.
    pub fn with_consent_reference(
        mut self,
        consent_reference: impl Into<String>,
    ) -> Result<Self, AiError> {
        self.consent_reference = Some(consent_reference.into());
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<(), AiError> {
        let valid = |value: &str, maximum: usize| {
            !value.trim().is_empty()
                && value.len() <= maximum
                && !value.chars().any(char::is_control)
        };
        if !valid(&self.provider_profile_id, 200)
            || !valid(&self.destination, 1_024)
            || !valid(&self.policy_version, 256)
            || self
                .residency
                .as_deref()
                .is_some_and(|value| !valid(value, 256))
            || self
                .consent_reference
                .as_deref()
                .is_some_and(|value| !valid(value, 1_024))
        {
            return Err(AiError::InvalidConfiguration(
                "invalid OpenAI background retrieval route".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Deployment-owned bounds for one exact OpenAI background response GET.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiOpenAiBackgroundRetrievalLimits {
    maximum_response_bytes: usize,
    maximum_visible_bytes: usize,
    maximum_output_items: usize,
    maximum_content_items: usize,
    maximum_request_timeout: Duration,
    maximum_principal_age: Duration,
}

impl AiOpenAiBackgroundRetrievalLimits {
    /// Creates fixed response, normalization, transport, and authority bounds.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless byte bounds are
    /// positive and at most 64 MiB, visible bytes do not exceed the full body,
    /// item counts are within `1..=4096`, request timeout is at most five
    /// minutes, and principal age is at most one hour.
    pub fn new(
        maximum_response_bytes: usize,
        maximum_visible_bytes: usize,
        maximum_output_items: usize,
        maximum_content_items: usize,
        maximum_request_timeout: Duration,
        maximum_principal_age: Duration,
    ) -> Result<Self, AiError> {
        if maximum_response_bytes == 0
            || maximum_response_bytes > MAXIMUM_BACKGROUND_RETRIEVAL_BYTES
            || maximum_visible_bytes == 0
            || maximum_visible_bytes > maximum_response_bytes
            || !(1..=MAXIMUM_BACKGROUND_RETRIEVAL_ITEMS).contains(&maximum_output_items)
            || !(1..=MAXIMUM_BACKGROUND_RETRIEVAL_ITEMS).contains(&maximum_content_items)
            || maximum_request_timeout <= Duration::ZERO
            || maximum_request_timeout > MAXIMUM_BACKGROUND_RETRIEVAL_TIMEOUT
            || maximum_principal_age <= Duration::ZERO
            || maximum_principal_age > MAXIMUM_BACKGROUND_PRINCIPAL_AGE
        {
            return Err(AiError::InvalidConfiguration(
                "invalid OpenAI background retrieval limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_response_bytes,
            maximum_visible_bytes,
            maximum_output_items,
            maximum_content_items,
            maximum_request_timeout,
            maximum_principal_age,
        })
    }
}

impl Default for AiOpenAiBackgroundRetrievalLimits {
    fn default() -> Self {
        Self {
            maximum_response_bytes: MAXIMUM_BACKGROUND_RETRIEVAL_BYTES,
            maximum_visible_bytes: MAXIMUM_BACKGROUND_RETRIEVAL_BYTES,
            maximum_output_items: MAXIMUM_BACKGROUND_RETRIEVAL_ITEMS,
            maximum_content_items: MAXIMUM_BACKGROUND_RETRIEVAL_ITEMS,
            maximum_request_timeout: Duration::seconds(30),
            maximum_principal_age: Duration::minutes(5),
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
    submission_key: String,
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
    original_egress_destination: String,
    original_maximum_classification: DataClassification,
    retrieval_egress_decision_id: Option<Uuid>,
    scope_kind: String,
    scope_id: String,
    tenant_id: Option<String>,
    matched_receipt: Option<MatchedBackgroundReceipt>,
}

#[derive(Clone, PartialEq, Eq)]
struct MatchedBackgroundReceipt {
    id: Uuid,
    provider_kind: String,
    provider_event_kind: String,
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
            .field("submission_key", &"[REDACTED]")
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
            .field("original_egress_destination", &"[REDACTED]")
            .field(
                "original_maximum_classification",
                &self.original_maximum_classification,
            )
            .field("retrieval_egress_decision_id", &"[REDACTED]")
            .field("scope_kind", &self.scope_kind)
            .field("scope_id", &"[REDACTED]")
            .field("tenant_id", &"[REDACTED]")
            .field(
                "matched_receipt",
                &self.matched_receipt.as_ref().map(|_| "[REDACTED]"),
            )
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

/// Current-authority, exact-egress OpenAI background retrieval service.
///
/// This service can bind and perform one bounded fixed-destination GET for an
/// exact reconciliation claim. It cannot select webhook content, settle
/// budget, protect or persist output, or mutate the parked run terminally.
pub struct OrmAiOpenAiBackgroundRetrievalService {
    database: Database<DefaultWriteBackend>,
    runtime: Arc<AiRuntime>,
    egress_audit: Arc<dyn AiEgressDecisionAudit>,
    clock: Arc<dyn Clock>,
    route: AiOpenAiBackgroundRetrievalRoute,
    limits: AiOpenAiBackgroundRetrievalLimits,
}

/// In-memory normalized result bound to one durable retrieval generation.
///
/// This value may contain protected visible provider output. It is not a
/// terminal persistence or run-completion proof. The claim remains
/// `reconciling`; if no future terminal service consumes it, its lease expires
/// and a higher generation may retrieve again.
#[derive(Clone)]
pub struct AiOpenAiBackgroundRetrievalObservation {
    claim: AiOpenAiBackgroundReconciliationClaim,
    observation: ProviderBackgroundObservation,
}

/// Durable result of consuming one terminal OpenAI background observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AiOpenAiBackgroundTerminalOutcome {
    /// Usage, protected assistant output, receipt, attempt, submission, and run
    /// completion committed atomically.
    Completed {
        /// Deterministic assistant message and output-checkpoint identifier.
        message_id: Uuid,
    },
    /// Authoritative provider failure and usage committed without output.
    Failed,
    /// Authoritative provider cancellation and usage committed without output.
    Cancelled,
    /// Conflicting terminal evidence was closed for operator recovery.
    RecoveryRequired,
    /// The exact terminal graph was already durable.
    AlreadyReconciled,
}

/// Classified result of one fixed-destination background retrieval attempt.
#[non_exhaustive]
pub enum AiOpenAiBackgroundRetrievalAttempt {
    /// A bounded reviewed provider observation is available.
    Observed(AiOpenAiBackgroundRetrievalObservation),
    /// Transport or provider availability failed and may be retried under
    /// bounded backoff.
    Retryable(AiOpenAiBackgroundRetrievalFailure),
    /// The provider response could not be interpreted safely and requires
    /// operator recovery without automatic retrieval replay.
    RecoveryRequired(AiOpenAiBackgroundRetrievalFailure),
}

impl std::fmt::Debug for AiOpenAiBackgroundRetrievalAttempt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Observed(observation) => formatter
                .debug_struct("Observed")
                .field(
                    "reconciliation_generation",
                    &observation.claim.reconciliation_generation,
                )
                .field("status", &observation.status())
                .field("provider_output", &"[REDACTED]")
                .finish(),
            Self::Retryable(failure) => formatter.debug_tuple("Retryable").field(failure).finish(),
            Self::RecoveryRequired(failure) => formatter
                .debug_tuple("RecoveryRequired")
                .field(failure)
                .finish(),
        }
    }
}

/// Redacted failure proof retaining the exact retrieval generation.
///
/// Fields are private so callers cannot manufacture a release or recovery
/// transition for another submission.
#[derive(Clone)]
pub struct AiOpenAiBackgroundRetrievalFailure {
    claim: AiOpenAiBackgroundReconciliationClaim,
    disposition: BackgroundRetrievalFailureDisposition,
    safe_error_code: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BackgroundRetrievalFailureDisposition {
    Retryable,
    RecoveryRequired,
}

impl AiOpenAiBackgroundRetrievalFailure {
    /// Stable redacted failure classification.
    pub const fn safe_error_code(&self) -> &'static str {
        self.safe_error_code
    }
}

impl std::fmt::Debug for AiOpenAiBackgroundRetrievalFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AiOpenAiBackgroundRetrievalFailure")
            .field(
                "reconciliation_generation",
                &self.claim.reconciliation_generation,
            )
            .field("disposition", &self.disposition)
            .field("safe_error_code", &self.safe_error_code)
            .field("provider_reference", &"[REDACTED]")
            .finish()
    }
}

/// Atomic terminal consumer for one exactly retrieved OpenAI background
/// response.
///
/// Provider retrieval remains a separate service so a host can heartbeat and
/// schedule workers explicitly. This service rehydrates current authority,
/// settles immutable pricing, protects completed output, rechecks authority
/// and protection immediately before mutation, and commits the complete local
/// terminal graph in one generated-ORM state-machine transaction.
pub struct OrmAiOpenAiBackgroundTerminalService {
    database: Database<DefaultWriteBackend>,
    runtime: Arc<AiRuntime>,
    usage_accounting: Arc<dyn AiProviderUsageAccounting>,
    clock: Arc<dyn Clock>,
    output_limits: AiProviderOutputLimits,
    maximum_principal_age: Duration,
    maximum_transaction_retries: usize,
}

impl AiOpenAiBackgroundRetrievalObservation {
    /// Exact reviewed provider status.
    pub const fn status(&self) -> crate::ProviderBackgroundStatus {
        self.observation.status()
    }

    /// Bounded visible completed-response events.
    ///
    /// This content remains protected backend data and has not passed the
    /// fenced terminal persistence boundary.
    pub fn events(&self) -> &[crate::ProviderEvent] {
        self.observation.events()
    }

    /// Authoritative terminal provider usage, when present.
    pub const fn usage(&self) -> Option<crate::ProviderBackgroundUsage> {
        self.observation.usage()
    }

    /// Reconciliation generation that authorized this retrieval attempt.
    pub const fn reconciliation_generation(&self) -> i64 {
        self.claim.reconciliation_generation
    }
}

impl std::fmt::Debug for AiOpenAiBackgroundRetrievalObservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AiOpenAiBackgroundRetrievalObservation")
            .field(
                "reconciliation_generation",
                &self.claim.reconciliation_generation,
            )
            .field("status", &self.observation.status())
            .field("event_count", &self.observation.events().len())
            .field("visible_content", &"[REDACTED]")
            .field("usage", &self.observation.usage())
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
            database: crate::orm_webhooks::with_provider_webhook_receipt_policy(database),
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
    /// reaching the retry ceiling or immutable deadline atomically closes the
    /// submission and parked run as recovery-required without releasing the
    /// uncertain budget.
    ///
    /// # Errors
    ///
    /// Returns an error when the delay is shorter than one second, exceeds the
    /// deployment maximum, the claim is stale or expired, or persistence
    /// fails.
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
        let now = canonical_second(self.clock.now());
        let eligible_at = now.checked_add(delay).ok_or_else(|| {
            AiError::InvalidConfiguration(
                "background reconciliation retry time overflow".to_owned(),
            )
        })?;
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

    /// Atomically closes a bounded batch whose provider-response deadline has
    /// expired.
    ///
    /// The submission and parked run move to `RecoveryRequired`, the original
    /// attempt receives one immutable outcome, and an already matched receipt
    /// is marked recovery-required in the same generated-ORM transaction. The
    /// uncertain budget remains reserved because expiry is not authoritative
    /// usage or provider-absence proof.
    ///
    /// # Errors
    ///
    /// Fails closed for malformed support graphs, conflicting receipt state,
    /// duplicate outcomes, or persistence failure.
    pub async fn close_expired(&self) -> Result<usize, AiError> {
        let now = canonical_second(self.clock.now());
        let filter_time =
            i32::try_from(now.unix_timestamp()).map_err(|_| AiError::PersistenceFailed)?;
        for retry in 0..=self.limits.maximum_transaction_retries {
            match self.close_expired_once(now, filter_time).await {
                Ok(closed) => return Ok(closed),
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

    /// Releases one authoritative nonterminal provider observation with
    /// deterministic bounded backoff.
    ///
    /// This path requires the retrieval-egress marker installed by
    /// [`OrmAiOpenAiBackgroundRetrievalService::retrieve`]. It clears that
    /// one-generation marker, retains any exact matched receipt as a hint, and
    /// never changes the parked run or uncertain budget. Retry or deadline
    /// exhaustion closes the complete graph as recovery-required instead.
    ///
    /// # Errors
    ///
    /// Fails for a terminal observation, stale claim, missing retrieval proof,
    /// malformed support graph, or persistence failure.
    pub async fn release_nonterminal(
        &self,
        observation: &AiOpenAiBackgroundRetrievalObservation,
    ) -> Result<(), AiError> {
        if observation.status().is_terminal() {
            return Err(AiError::InvalidInput(
                "terminal background observations cannot be released".to_owned(),
            ));
        }
        let now = canonical_second(self.clock.now());
        let delay = background_retry_delay(
            observation.claim.retry_count,
            self.limits.maximum_retry_delay,
        )?;
        let eligible_at = now.checked_add(delay).ok_or_else(|| {
            AiError::InvalidConfiguration(
                "background reconciliation retry time overflow".to_owned(),
            )
        })?;
        for retry in 0..=self.limits.maximum_transaction_retries {
            match self
                .release_nonterminal_once(observation.clone(), now, eligible_at)
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

    /// Consumes a classified provider retrieval failure for this exact
    /// reconciliation generation.
    ///
    /// Retryable transport failures clear the retrieval marker and release
    /// the submission with deterministic bounded backoff. Retry or deadline
    /// exhaustion, credential failure, malformed provider responses, and
    /// non-retryable provider rejection atomically close the parked run and
    /// submission as recovery-required. The uncertain budget is never
    /// released by this method.
    ///
    /// # Errors
    ///
    /// Fails for a stale or expired failure proof, a missing retrieval marker,
    /// malformed support graph, or persistence failure.
    pub async fn handle_retrieval_failure(
        &self,
        failure: &AiOpenAiBackgroundRetrievalFailure,
    ) -> Result<(), AiError> {
        let now = canonical_second(self.clock.now());
        let eligible_at = match failure.disposition {
            BackgroundRetrievalFailureDisposition::Retryable => {
                let delay = background_retry_delay(
                    failure.claim.retry_count,
                    self.limits.maximum_retry_delay,
                )?;
                Some(now.checked_add(delay).ok_or_else(|| {
                    AiError::InvalidConfiguration(
                        "background reconciliation retry time overflow".to_owned(),
                    )
                })?)
            }
            BackgroundRetrievalFailureDisposition::RecoveryRequired => None,
        };
        for retry in 0..=self.limits.maximum_transaction_retries {
            match self
                .handle_retrieval_failure_once(failure.clone(), now, eligible_at)
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
                                retrieval_egress_decision_id: Some(None),
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
                        &updated, &worker_id, generation, None, now,
                    )?;
                    let matched_receipt =
                        match_background_receipt(tx, &updated, limits.maximum_candidate_scan)
                            .await?;
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
                    claim_from_records(&updated, graph, matched_receipt)
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
                        None,
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

    async fn close_expired_once(
        &self,
        now: OffsetDateTime,
        filter_time: i32,
    ) -> Result<usize, TransactionError> {
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
                            reconciliation_deadline: Some(IntFilter {
                                lte: Some(filter_time),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .order_by(AiProviderBackgroundSubmissionRecordOrderByInput {
                            reconciliation_deadline: Some(OrderDirection::Asc),
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
                    let closed = candidates.len();
                    for current in candidates {
                        validate_expired_submission(&current, now, limits.maximum_retries)?;
                        let graph = load_background_claim_graph(tx, &current).await?;
                        close_background_recovery(
                            tx,
                            &current,
                            &graph,
                            "provider_response_deadline_expired",
                            now,
                        )
                        .await?;
                    }
                    Ok(closed)
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
                    let retry_exhausted =
                        u32::try_from(retry_count).map_or(true, |count| count > maximum_retries);
                    let deadline_exhausted = eligible_at.unix_timestamp()
                        >= claim.reconciliation_deadline.unix_timestamp();
                    if retry_exhausted || deadline_exhausted {
                        let graph = load_background_claim_graph(tx, &current).await?;
                        let safe_error_code = if retry_exhausted {
                            "provider_response_retry_exhausted"
                        } else {
                            "provider_response_deadline_exhausted"
                        };
                        close_background_recovery(tx, &current, &graph, safe_error_code, now)
                            .await?;
                        return Ok(());
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

    async fn release_nonterminal_once(
        &self,
        observation: AiOpenAiBackgroundRetrievalObservation,
        now: OffsetDateTime,
        eligible_at: OffsetDateTime,
    ) -> Result<(), TransactionError> {
        let database = self.database.clone();
        let maximum_retries = self.limits.maximum_retries;
        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let claim = &observation.claim;
                    let current = tx
                        .find_by_id::<AiProviderBackgroundSubmissionRecord>(&claim.submission_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    validate_current_reconciliation_claim(&current, claim, now)?;
                    if current.retrieval_egress_decision_id.is_none()
                        || !matches!(
                            observation.status(),
                            crate::ProviderBackgroundStatus::Queued
                                | crate::ProviderBackgroundStatus::InProgress
                        )
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let retry_count = current
                        .reconciliation_retry_count
                        .checked_add(1)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::Conflict))?;
                    let retry_exhausted =
                        u32::try_from(retry_count).map_or(true, |count| count > maximum_retries);
                    let deadline_exhausted = eligible_at.unix_timestamp()
                        >= claim.reconciliation_deadline.unix_timestamp();
                    if retry_exhausted || deadline_exhausted {
                        let graph = load_background_claim_graph(tx, &current).await?;
                        let safe_error_code = if retry_exhausted {
                            "provider_response_retry_exhausted"
                        } else {
                            "provider_response_deadline_exhausted"
                        };
                        close_background_recovery(tx, &current, &graph, safe_error_code, now)
                            .await?;
                        return Ok(());
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
                                provider_status: Some(Some(
                                    background_status_value(observation.status()).to_owned(),
                                )),
                                reconciliation_owner: Some(None),
                                reconciliation_lease_expires_at: Some(None),
                                reconciliation_next_attempt_at: Some(Some(
                                    eligible_at.unix_timestamp(),
                                )),
                                reconciliation_retry_count: Some(retry_count),
                                retrieval_egress_decision_id: Some(None),
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
                        actor_subject: claim.worker_id.clone(),
                        action: "release_provider_background_reconciliation".to_owned(),
                        resource_kind: "ai_provider_background_submission".to_owned(),
                        resource_reference: updated.id.to_string(),
                        outcome: "waiting_provider".to_owned(),
                        reason_code: "authoritative_provider_state_nonterminal".to_owned(),
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

    async fn handle_retrieval_failure_once(
        &self,
        failure: AiOpenAiBackgroundRetrievalFailure,
        now: OffsetDateTime,
        eligible_at: Option<OffsetDateTime>,
    ) -> Result<(), TransactionError> {
        let database = self.database.clone();
        let maximum_retries = self.limits.maximum_retries;
        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let claim = &failure.claim;
                    let current = tx
                        .find_by_id::<AiProviderBackgroundSubmissionRecord>(&claim.submission_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    validate_current_reconciliation_claim(&current, claim, now)?;
                    if current.retrieval_egress_decision_id.is_none() {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    match failure.disposition {
                        BackgroundRetrievalFailureDisposition::RecoveryRequired => {
                            let graph = load_background_claim_graph(tx, &current).await?;
                            close_background_recovery(
                                tx,
                                &current,
                                &graph,
                                failure.safe_error_code,
                                now,
                            )
                            .await?;
                        }
                        BackgroundRetrievalFailureDisposition::Retryable => {
                            let eligible_at = eligible_at
                                .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                            let retry_count = current
                                .reconciliation_retry_count
                                .checked_add(1)
                                .ok_or_else(|| OrmPublicError::new(OrmErrorCode::Conflict))?;
                            let retry_exhausted = u32::try_from(retry_count)
                                .map_or(true, |count| count > maximum_retries);
                            let deadline_exhausted = eligible_at.unix_timestamp()
                                >= claim.reconciliation_deadline.unix_timestamp();
                            if retry_exhausted || deadline_exhausted {
                                let graph = load_background_claim_graph(tx, &current).await?;
                                let safe_error_code = if retry_exhausted {
                                    "provider_response_retry_exhausted"
                                } else {
                                    "provider_response_deadline_exhausted"
                                };
                                close_background_recovery(
                                    tx,
                                    &current,
                                    &graph,
                                    safe_error_code,
                                    now,
                                )
                                .await?;
                                return Ok(());
                            }
                            let outcome = tx
                                .compare_and_swap::<AiProviderBackgroundSubmissionRecord>(
                                    &current.id,
                                    current.row_version,
                                    exact_background_state(
                                        BackgroundSubmissionState::Reconciling.as_str(),
                                    ),
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
                                        retrieval_egress_decision_id: Some(None),
                                        ..Default::default()
                                    },
                                )
                                .await
                                .map_err(OrmPublicError::from)?;
                            let updated = match outcome {
                                ConditionalUpdateOutcome::Updated(updated) => updated,
                                ConditionalUpdateOutcome::NotFound
                                | ConditionalUpdateOutcome::Conflict => {
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
                                actor_subject: claim.worker_id.clone(),
                                action: "release_provider_background_reconciliation".to_owned(),
                                resource_kind: "ai_provider_background_submission".to_owned(),
                                resource_reference: updated.id.to_string(),
                                outcome: "waiting_provider".to_owned(),
                                reason_code: failure.safe_error_code.to_owned(),
                                correlation_id: updated.id.to_string(),
                                causation_id: Some(updated.run_id.to_string()),
                                policy_version: Some(
                                    "provider-background-reconciliation-v1".to_owned(),
                                ),
                            })
                            .await
                            .map_err(OrmPublicError::from)?;
                        }
                    }
                    Ok(())
                })
            })
            .await
    }
}

impl OrmAiOpenAiBackgroundRetrievalService {
    /// Creates an exact-reference OpenAI background retrieval service.
    ///
    /// The egress audit must durably include the generated-ORM decision event
    /// in this same database before the service can bind transport authority.
    pub fn new(
        database: Database<DefaultWriteBackend>,
        runtime: Arc<AiRuntime>,
        egress_audit: Arc<dyn AiEgressDecisionAudit>,
        clock: Arc<dyn Clock>,
        route: AiOpenAiBackgroundRetrievalRoute,
        limits: AiOpenAiBackgroundRetrievalLimits,
    ) -> Self {
        Self {
            database,
            runtime,
            egress_audit,
            clock,
            route,
            limits,
        }
    }

    /// Freshly authorizes and retrieves one exact bound OpenAI response.
    ///
    /// The service revalidates the complete durable claim graph, rehydrates
    /// the current principal, proves scope/session write access and a ready
    /// content-protection policy, audits a new exact egress decision, and
    /// CAS-binds that allow before provider I/O. The adapter receives no URL
    /// and uses a request timeout strictly shorter than the remaining claim.
    ///
    /// A successful return remains in-memory only. No budget, receipt,
    /// transcript, attempt, submission terminal state, or run state is
    /// mutated by this method.
    ///
    /// # Errors
    ///
    /// Fails closed for runtime readiness, a stale or malformed claim,
    /// profile/destination mismatch, stale principal, current access,
    /// protection or egress denial, failed egress audit/binding, insufficient
    /// lease time, provider transport error, or malformed/oversized response.
    pub async fn retrieve(
        &self,
        claim: &AiOpenAiBackgroundReconciliationClaim,
    ) -> Result<AiOpenAiBackgroundRetrievalObservation, AiError> {
        match self.retrieve_classified(claim).await? {
            AiOpenAiBackgroundRetrievalAttempt::Observed(observation) => Ok(observation),
            AiOpenAiBackgroundRetrievalAttempt::Retryable(_)
            | AiOpenAiBackgroundRetrievalAttempt::RecoveryRequired(_) => {
                Err(AiError::ProviderFailed)
            }
        }
    }

    /// Freshly authorizes and classifies one exact bound OpenAI response
    /// retrieval.
    ///
    /// This is the lifecycle-aware form of [`Self::retrieve`]. Once exact
    /// egress is durably bound, provider timeout, rate limiting, and temporary
    /// unavailability return a private retryable proof. Credential,
    /// configuration, validation, capability, and rejection failures return a
    /// private recovery-required proof. Callers must pass either proof to
    /// [`OrmAiOpenAiBackgroundReconciliationService::handle_retrieval_failure`].
    ///
    /// No provider diagnostic or response content is retained in a failure
    /// proof.
    ///
    /// # Errors
    ///
    /// Returns an error before provider I/O for runtime readiness, a stale or
    /// malformed claim, profile/destination mismatch, stale principal,
    /// current access, protection or egress denial, failed egress
    /// audit/binding, or insufficient lease time.
    pub async fn retrieve_classified(
        &self,
        claim: &AiOpenAiBackgroundReconciliationClaim,
    ) -> Result<AiOpenAiBackgroundRetrievalAttempt, AiError> {
        if !self.runtime.start_gate().is_ready() {
            return Err(AiError::RuntimeNotReady);
        }
        self.validate_retrieval_claim(claim).await?;
        if claim.provider_profile_id != self.route.provider_profile_id
            || claim.original_egress_destination != self.route.destination
        {
            return Err(AiError::EgressDenied);
        }
        let scope = claim_scope(claim);
        self.require_current_authority(claim, &scope).await?;

        let manifest = self.retrieval_manifest(claim)?;
        let decision = self
            .runtime
            .authorize_egress(&claim.principal_reference, &manifest)
            .await?;
        self.egress_audit.record(&manifest, &decision).await?;
        let proof = decision.authorize(&manifest)?;
        self.require_current_authority(claim, &scope).await?;
        let bound = self
            .bind_retrieval_egress(claim.clone(), manifest.clone(), decision)
            .await?;

        let now = self.clock.now();
        let request_timeout = retrieval_timeout(
            now,
            bound.reconciliation_lease_expires_at,
            self.limits.maximum_request_timeout,
        )?;
        let binding = ProviderBackgroundRetrievalBinding::new(
            bound.submission_id,
            bound.submission_key.clone(),
            bound.provider_profile_id.clone(),
            bound.provider_response_id.clone(),
            bound.provider_created_at,
            bound.provider_model.clone(),
            bound.maximum_output_tokens,
            bound.provider_store,
            self.limits.maximum_response_bytes,
            self.limits.maximum_visible_bytes,
            self.limits.maximum_output_items,
            self.limits.maximum_content_items,
            request_timeout,
        )
        .map_err(|_| AiError::ProviderFailed)?;
        let context = ProviderBackgroundRetrievalContext::new(manifest, proof)
            .map_err(|_| AiError::EgressDenied)?;
        let observation = match tokio::time::timeout(
            request_timeout,
            self.runtime
                .retrieve_provider_background(&ProviderKind::OpenAi, binding, context),
        )
        .await
        {
            Err(_) => {
                return Ok(AiOpenAiBackgroundRetrievalAttempt::Retryable(
                    background_retrieval_failure(
                        bound,
                        BackgroundRetrievalFailureDisposition::Retryable,
                        "provider_response_retrieval_timeout",
                    ),
                ));
            }
            Ok(Err(error)) => return Ok(classify_background_retrieval_error(bound, &error)),
            Ok(Ok(observation)) => observation,
        };
        Ok(AiOpenAiBackgroundRetrievalAttempt::Observed(
            AiOpenAiBackgroundRetrievalObservation {
                claim: bound,
                observation,
            },
        ))
    }

    async fn require_current_authority(
        &self,
        claim: &AiOpenAiBackgroundReconciliationClaim,
        scope: &AiScope,
    ) -> Result<(), AiError> {
        let now = self.clock.now();
        let principal = self
            .runtime
            .resolve_current_principal(&claim.principal_reference)
            .await?;
        if principal.resolved_at() > now
            || now - principal.resolved_at() > self.limits.maximum_principal_age
            || principal
                .reference()
                .expires_at
                .is_some_and(|expires_at| expires_at <= now)
        {
            return Err(AiError::ReauthorizationFailed);
        }
        if !self
            .runtime
            .access_policy()
            .can_access_scope(principal.principal(), scope, AiSessionAction::Write)
            .await
            .is_allowed()
            || !self
                .runtime
                .access_policy()
                .can_access_session(
                    principal.principal(),
                    claim.session_id,
                    AiSessionAction::Write,
                )
                .await
                .is_allowed()
        {
            return Err(AiError::Forbidden);
        }
        let protection = self
            .runtime
            .content_protection_policy_resolver()
            .resolve(principal.principal(), scope)
            .await?;
        if !protection.ready || protection.scope != *scope {
            return Err(AiError::RuntimeNotReady);
        }
        Ok(())
    }

    fn retrieval_manifest(
        &self,
        claim: &AiOpenAiBackgroundReconciliationClaim,
    ) -> Result<AiEgressManifest, AiError> {
        let estimated_bytes = u64::try_from(claim.provider_response_id.len())
            .map_err(|_| AiError::InvalidConfiguration("response ID is too large".to_owned()))?;
        Ok(AiEgressManifest {
            provider_profile_id: self.route.provider_profile_id.clone(),
            provider_kind: ProviderKind::OpenAi.as_str().to_owned(),
            model: claim.provider_model.clone(),
            destination: self.route.destination.clone(),
            destination_trust: AiDestinationTrust::ManagedProvider,
            capability: AiEgressCapability::ModelInference,
            scope: claim_scope(claim),
            session_id: Some(claim.session_id),
            run_id: Some(claim.run_id),
            sources: vec![AiDataSourceRef {
                kind: "provider_response".to_owned(),
                reference: ProviderBackgroundRetrievalBinding::source_reference(
                    claim.submission_id,
                    &claim.provider_response_id,
                ),
                classification: claim.original_maximum_classification,
                trust: AiSourceTrust::ExternalUntrusted,
            }],
            estimated_bytes,
            estimated_tokens: 0,
            attachment_count: 0,
            purpose: "background_response_retrieval".to_owned(),
            retention: AI_EGRESS_RETENTION_PROVIDER_RESPONSE.to_owned(),
            residency: self.route.residency.clone(),
            policy_version: self.route.policy_version.clone(),
            consent_reference: self.route.consent_reference.clone(),
        })
    }

    async fn validate_retrieval_claim(
        &self,
        claim: &AiOpenAiBackgroundReconciliationClaim,
    ) -> Result<(), AiError> {
        let database = self.database.clone();
        let claim = claim.clone();
        let now = canonical_second(self.clock.now());
        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = tx
                        .find_by_id::<AiProviderBackgroundSubmissionRecord>(&claim.submission_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    validate_current_reconciliation_claim(&current, &claim, now)?;
                    if claim.retrieval_egress_decision_id.is_some() {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let graph = load_background_claim_graph(tx, &current).await?;
                    validate_claim_graph(&claim, &graph)
                })
            })
            .await
            .map_err(map_transaction)
    }

    async fn bind_retrieval_egress(
        &self,
        claim: AiOpenAiBackgroundReconciliationClaim,
        manifest: AiEgressManifest,
        decision: crate::AiEgressDecision,
    ) -> Result<AiOpenAiBackgroundReconciliationClaim, AiError> {
        let database = self.database.clone();
        let now = canonical_second(self.clock.now());
        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = tx
                        .find_by_id::<AiProviderBackgroundSubmissionRecord>(&claim.submission_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    validate_current_reconciliation_claim(&current, &claim, now)?;
                    if claim.retrieval_egress_decision_id.is_some() {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let graph = load_background_claim_graph(tx, &current).await?;
                    validate_claim_graph(&claim, &graph)?;
                    let egress = tx
                        .query::<AiEgressEventRecord>()
                        .filter(AiEgressEventRecordWhereInput {
                            id: Some(UuidFilter {
                                eq: Some(decision.id.0),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let [egress] = egress.as_slice() else {
                        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                    };
                    validate_retrieval_egress_record(
                        egress,
                        &manifest,
                        &decision,
                        &claim.principal_reference.subject,
                    )?;
                    let outcome = tx
                        .compare_and_swap::<AiProviderBackgroundSubmissionRecord>(
                            &current.id,
                            current.row_version,
                            exact_background_state(BackgroundSubmissionState::Reconciling.as_str()),
                            UpdateAiProviderBackgroundSubmissionRecordInput {
                                retrieval_egress_decision_id: Some(Some(decision.id.0)),
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
                        Some(decision.id.0),
                        now,
                    )?;
                    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                        actor_principal_kind: "system".to_owned(),
                        actor_subject: claim.worker_id.clone(),
                        action: "bind_provider_background_retrieval_egress".to_owned(),
                        resource_kind: "ai_provider_background_submission".to_owned(),
                        resource_reference: updated.id.to_string(),
                        outcome: "authorized".to_owned(),
                        reason_code: "current_egress_allow_bound".to_owned(),
                        correlation_id: updated.id.to_string(),
                        causation_id: Some(updated.run_id.to_string()),
                        policy_version: Some(decision.policy_version.clone()),
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    let mut bound = claim;
                    bound.retrieval_egress_decision_id = Some(decision.id.0);
                    bound.row_version = updated.row_version;
                    Ok(bound)
                })
            })
            .await
            .map_err(map_transaction)
    }
}

impl OrmAiOpenAiBackgroundTerminalService {
    /// Creates a terminal reconciliation service.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless the accepted
    /// current-principal age is positive and no greater than one hour.
    pub fn new(
        database: Database<DefaultWriteBackend>,
        runtime: Arc<AiRuntime>,
        usage_accounting: Arc<dyn AiProviderUsageAccounting>,
        clock: Arc<dyn Clock>,
        output_limits: AiProviderOutputLimits,
        maximum_principal_age: Duration,
    ) -> Result<Self, AiError> {
        if maximum_principal_age <= Duration::ZERO
            || maximum_principal_age > MAXIMUM_BACKGROUND_PRINCIPAL_AGE
        {
            return Err(AiError::InvalidConfiguration(
                "invalid OpenAI background terminal principal age".to_owned(),
            ));
        }
        Ok(Self {
            database: crate::orm_webhooks::with_provider_webhook_receipt_policy(database),
            runtime,
            usage_accounting,
            clock,
            output_limits,
            maximum_principal_age,
            maximum_transaction_retries: 8,
        })
    }

    /// Commits one authoritative terminal observation as a complete local
    /// graph.
    ///
    /// Completed responses become a protected assistant message. Failed,
    /// incomplete, and cancelled responses commit authoritative usage and a
    /// redacted terminal attempt without output. Exact retries validate and
    /// return the already-durable graph; conflicting matched receipt evidence
    /// closes the run as recovery-required without settling the uncertain
    /// budget.
    ///
    /// # Errors
    ///
    /// Fails closed for nonterminal or malformed observations, stale claims,
    /// current authority/protection changes, pricing mismatch, incomplete
    /// support graphs, receipt conflicts, output-protection failure, or
    /// persistence failure.
    pub async fn commit(
        &self,
        observation: &AiOpenAiBackgroundRetrievalObservation,
    ) -> Result<AiOpenAiBackgroundTerminalOutcome, AiError> {
        let status = observation.status();
        if !status.is_terminal() {
            return Err(AiError::InvalidInput(
                "nonterminal background observations cannot commit".to_owned(),
            ));
        }
        let usage = observation.usage().ok_or(AiError::ProviderFailed)?;
        let submission = AiProviderBackgroundSubmissionRecord::find_by_id(
            &self.database,
            &observation.claim.submission_id,
        )
        .await
        .map_err(|_| AiError::PersistenceFailed)?
        .ok_or(AiError::NotFound)?;
        validate_observation_submission_binding(&submission, &observation.claim)?;
        let budget = AiBudgetReservationRecord::find_by_id(
            &self.database,
            &observation.claim.budget_reservation_id.0,
        )
        .await
        .map_err(|_| AiError::PersistenceFailed)?
        .ok_or(AiError::NotFound)?;
        validate_terminal_budget_binding(&budget, &observation.claim)?;
        let usage_observation = AiProviderUsageObservation::for_background_response(
            claim_scope(&observation.claim),
            observation.claim.provider_model.clone(),
            budget.pricing_policy_version.clone(),
            usage,
        );
        let actual = self.usage_accounting.settle(&usage_observation).await?;
        if actual.input_tokens != usage.input_tokens()
            || actual.output_tokens != usage.output_tokens()
            || actual.tool_units != 0
            || actual.image_units != 0
            || actual.runs != 1
        {
            return Err(AiError::ProviderFailed);
        }
        if BackgroundSubmissionState::from_persisted(&submission.state).is_some_and(|state| {
            matches!(
                state,
                BackgroundSubmissionState::Completed
                    | BackgroundSubmissionState::Failed
                    | BackgroundSubmissionState::Cancelled
            )
        }) {
            self.validate_terminal_replay(observation, actual).await?;
            return Ok(AiOpenAiBackgroundTerminalOutcome::AlreadyReconciled);
        }

        let policy = self.current_policy(&observation.claim).await?;
        let message_id =
            background_terminal_identity(observation.claim.submission_id, "assistant-message");
        let prepared_output = if status == ProviderBackgroundStatus::Completed {
            Some(
                prepare_background_provider_output(
                    self.runtime.content_protector().as_ref(),
                    &policy,
                    BackgroundProviderOutputPreparation {
                        message_id,
                        event_id: background_terminal_identity(
                            observation.claim.submission_id,
                            "session-event",
                        ),
                        inbox_event_id: background_terminal_identity(
                            observation.claim.submission_id,
                            "inbox-event",
                        ),
                        run_id: observation.claim.run_id,
                        attempt_id: observation.claim.attempt_id,
                        lease_generation: observation.claim.original_lease_generation,
                        provider_model: observation.claim.provider_model.clone(),
                        provider_response_id: observation.claim.provider_response_id.clone(),
                        budget_reservation_id: observation.claim.budget_reservation_id.0,
                        correlation_id: observation.claim.submission_id.to_string(),
                        owner_principal_kind: principal_kind(
                            &observation.claim.principal_reference,
                        ),
                        owner_subject: observation.claim.principal_reference.subject.clone(),
                        scope: claim_scope(&observation.claim),
                    },
                    observation.events(),
                    self.output_limits,
                )
                .await?,
            )
        } else {
            None
        };
        let current_policy = self.current_policy(&observation.claim).await?;
        if current_policy != policy {
            return Err(AiError::ReauthorizationFailed);
        }
        let now = canonical_second(self.clock.now());
        for retry in 0..=self.maximum_transaction_retries {
            match self
                .commit_once(observation.clone(), prepared_output.clone(), actual, now)
                .await
            {
                Ok(outcome) => return Ok(outcome),
                Err(TransactionError::Retryable(_)) if retry < self.maximum_transaction_retries => {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(map_transaction(error)),
            }
        }
        Err(AiError::PersistenceFailed)
    }

    async fn current_policy(
        &self,
        claim: &AiOpenAiBackgroundReconciliationClaim,
    ) -> Result<crate::AiContentProtectionPolicy, AiError> {
        if !self.runtime.start_gate().is_ready() {
            return Err(AiError::RuntimeNotReady);
        }
        let now = self.clock.now();
        let principal = self
            .runtime
            .resolve_current_principal(&claim.principal_reference)
            .await?;
        if principal.resolved_at() > now
            || now - principal.resolved_at() > self.maximum_principal_age
            || principal
                .reference()
                .expires_at
                .is_some_and(|expires_at| expires_at <= now)
        {
            return Err(AiError::ReauthorizationFailed);
        }
        let scope = claim_scope(claim);
        if !self
            .runtime
            .access_policy()
            .can_access_scope(principal.principal(), &scope, AiSessionAction::Write)
            .await
            .is_allowed()
            || !self
                .runtime
                .access_policy()
                .can_access_session(
                    principal.principal(),
                    claim.session_id,
                    AiSessionAction::Write,
                )
                .await
                .is_allowed()
        {
            return Err(AiError::Forbidden);
        }
        let policy = self
            .runtime
            .content_protection_policy_resolver()
            .resolve(principal.principal(), &scope)
            .await?;
        if !policy.ready || policy.scope != scope {
            return Err(AiError::RuntimeNotReady);
        }
        Ok(policy)
    }

    async fn commit_once(
        &self,
        observation: AiOpenAiBackgroundRetrievalObservation,
        prepared_output: Option<PreparedProviderOutput>,
        actual: AiBudgetAmounts,
        now: OffsetDateTime,
    ) -> Result<AiOpenAiBackgroundTerminalOutcome, TransactionError> {
        let database = self.database.clone();
        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let claim = &observation.claim;
                    let current = tx
                        .find_by_id::<AiProviderBackgroundSubmissionRecord>(&claim.submission_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    validate_current_reconciliation_claim(&current, claim, now)?;
                    let graph = load_background_claim_graph(tx, &current).await?;
                    validate_claim_graph(claim, &graph)?;
                    let matched = match_background_receipt(tx, &current, 64).await?;
                    if matched.as_ref().is_some_and(|receipt| {
                        receipt.provider_event_kind != expected_receipt_kind(observation.status())
                    }) {
                        close_background_recovery(
                            tx,
                            &current,
                            &graph,
                            "provider_terminal_receipt_conflict",
                            now,
                        )
                        .await?;
                        return Ok(AiOpenAiBackgroundTerminalOutcome::RecoveryRequired);
                    }
                    let budget = tx
                        .find_by_id::<AiBudgetReservationRecord>(&claim.budget_reservation_id.0)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    validate_terminal_budget_binding(&budget, claim)
                        .map_err(|_| OrmPublicError::new(OrmErrorCode::Conflict))?;
                    crate::orm_budget::commit_uncertain_background_budget(
                        tx,
                        &budget,
                        actual,
                        observation
                            .usage()
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?
                            .cached_input_tokens(),
                        now,
                    )
                    .await?;
                    commit_background_terminal_graph(
                        tx,
                        &current,
                        &graph,
                        matched.as_ref(),
                        observation.status(),
                        prepared_output,
                        now,
                    )
                    .await
                })
            })
            .await
    }

    async fn validate_terminal_replay(
        &self,
        observation: &AiOpenAiBackgroundRetrievalObservation,
        actual: AiBudgetAmounts,
    ) -> Result<(), AiError> {
        validate_existing_background_terminal(&self.database, observation, actual)
            .await
            .map_err(map_transaction)
    }
}

fn claim_scope(claim: &AiOpenAiBackgroundReconciliationClaim) -> AiScope {
    AiScope {
        kind: claim.scope_kind.clone(),
        id: claim.scope_id.clone(),
        tenant_id: claim.tenant_id.clone(),
    }
}

fn principal_kind(reference: &PrincipalReference) -> String {
    match &reference.kind {
        PrincipalReferenceKind::UserSession => "user".to_owned(),
        PrincipalReferenceKind::ApiToken { principal_kind } => {
            format!("api_token:{principal_kind}")
        }
    }
}

fn background_terminal_identity(submission_id: Uuid, purpose: &str) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphql-orm-ai/background-terminal/v1\0");
    hasher.update(submission_id.as_bytes());
    hasher.update([0]);
    hasher.update(purpose.as_bytes());
    let digest = hasher.finalize();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(id)
}

fn expected_receipt_kind(status: ProviderBackgroundStatus) -> &'static str {
    match status {
        ProviderBackgroundStatus::Completed => "response_completed",
        ProviderBackgroundStatus::Failed => "response_failed",
        ProviderBackgroundStatus::Incomplete => "response_incomplete",
        ProviderBackgroundStatus::Cancelled => "response_cancelled",
        ProviderBackgroundStatus::Queued | ProviderBackgroundStatus::InProgress => "nonterminal",
    }
}

fn validate_observation_submission_binding(
    submission: &AiProviderBackgroundSubmissionRecord,
    claim: &AiOpenAiBackgroundReconciliationClaim,
) -> Result<(), AiError> {
    if submission.id != claim.submission_id
        || submission.submission_key != claim.submission_key
        || submission.session_id != claim.session_id.0
        || submission.run_id != claim.run_id.0
        || submission.attempt_id != claim.attempt_id
        || submission.lease_generation != claim.original_lease_generation
        || submission.provider_kind != "openai"
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
    {
        return Err(AiError::Conflict);
    }
    Ok(())
}

fn validate_terminal_budget_binding(
    budget: &AiBudgetReservationRecord,
    claim: &AiOpenAiBackgroundReconciliationClaim,
) -> Result<(), AiError> {
    if budget.id != claim.budget_reservation_id.0
        || budget.session_id != claim.session_id.0
        || budget.run_id != claim.run_id.0
        || budget.attempt_id != claim.attempt_id
        || budget.lease_generation != claim.original_lease_generation
        || budget.provider_kind != "openai"
        || budget.provider_model != claim.provider_model
        || budget.pricing_policy_version.trim().is_empty()
        || budget.pricing_policy_version.len() > 256
    {
        return Err(AiError::Conflict);
    }
    Ok(())
}

async fn commit_background_terminal_graph(
    tx: &mut MutationContext<'_, DefaultWriteBackend>,
    submission: &AiProviderBackgroundSubmissionRecord,
    graph: &BackgroundClaimGraph,
    matched_receipt: Option<&MatchedBackgroundReceipt>,
    status: ProviderBackgroundStatus,
    prepared_output: Option<PreparedProviderOutput>,
    now: OffsetDateTime,
) -> Result<AiOpenAiBackgroundTerminalOutcome, OrmPublicError> {
    let run = tx
        .find_by_id::<AiRunRecord>(&submission.run_id)
        .await
        .map_err(OrmPublicError::from)?
        .ok_or_else(OrmPublicError::not_found)?;
    if run.state != AiRunState::WaitingProvider.as_str()
        || run.attempt_id != Some(submission.attempt_id)
        || run.lease_generation != submission.lease_generation
        || run.lease_owner.is_some()
        || run.lease_expires_at.is_some()
        || run.lease_heartbeat_at.is_some()
        || run.next_attempt_at.is_some()
        || run.error_code.is_some()
        || run.latest_checkpoint_id.is_some()
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    let (run_state, submission_state, outcome_code, safe_error_code) = match status {
        ProviderBackgroundStatus::Completed => (
            AiRunState::Completed,
            BackgroundSubmissionState::Completed,
            "provider_response_completed",
            None,
        ),
        ProviderBackgroundStatus::Failed => (
            AiRunState::Failed,
            BackgroundSubmissionState::Failed,
            "provider_response_failed",
            Some("provider_response_failed"),
        ),
        ProviderBackgroundStatus::Incomplete => (
            AiRunState::Failed,
            BackgroundSubmissionState::Failed,
            "provider_response_incomplete",
            Some("provider_response_incomplete"),
        ),
        ProviderBackgroundStatus::Cancelled => (
            AiRunState::Cancelled,
            BackgroundSubmissionState::Cancelled,
            "provider_response_cancelled",
            Some("provider_response_cancelled"),
        ),
        ProviderBackgroundStatus::Queued | ProviderBackgroundStatus::InProgress => {
            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
        }
    };
    let message_id = prepared_output.as_ref().map(|output| output.message_id);
    if (status == ProviderBackgroundStatus::Completed) != message_id.is_some() {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    if let Some(output) = prepared_output {
        persist_background_provider_output(tx, submission, &run, output, now).await?;
    }
    let run_update = tx
        .compare_and_swap::<AiRunRecord>(
            &run.id,
            run.row_version,
            exact_state(AiRunState::WaitingProvider.as_str()),
            UpdateAiRunRecordInput {
                state: Some(run_state.as_str().to_owned()),
                lease_owner: Some(None),
                lease_expires_at: Some(None),
                lease_heartbeat_at: Some(None),
                next_attempt_at: Some(None),
                error_code: Some(safe_error_code.map(str::to_owned)),
                latest_checkpoint_id: Some(message_id),
                ..Default::default()
            },
        )
        .await
        .map_err(OrmPublicError::from)?;
    if !matches!(run_update, ConditionalUpdateOutcome::Updated(_)) {
        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
    }
    let submission_update = tx
        .compare_and_swap::<AiProviderBackgroundSubmissionRecord>(
            &submission.id,
            submission.row_version,
            exact_background_state(BackgroundSubmissionState::Reconciling.as_str()),
            UpdateAiProviderBackgroundSubmissionRecordInput {
                provider_status: Some(Some(background_status_value(status).to_owned())),
                state: Some(submission_state.as_str().to_owned()),
                safe_error_code: Some(safe_error_code.map(str::to_owned)),
                reconciliation_owner: Some(None),
                reconciliation_lease_expires_at: Some(None),
                reconciliation_next_attempt_at: Some(None),
                reconciled_at: Some(Some(now.unix_timestamp())),
                terminal_message_id: Some(message_id),
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
    if updated_submission.state != submission_state.as_str()
        || updated_submission.provider_status.as_deref() != Some(background_status_value(status))
        || updated_submission.safe_error_code.as_deref() != safe_error_code
        || updated_submission.reconciled_at != Some(now.unix_timestamp())
        || updated_submission.terminal_message_id != message_id
        || updated_submission.reconciliation_owner.is_some()
        || updated_submission.reconciliation_lease_expires_at.is_some()
        || updated_submission.reconciliation_next_attempt_at.is_some()
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    if let Some(receipt) = matched_receipt {
        commit_matched_terminal_receipt(tx, submission, receipt, status, now).await?;
    }
    close_duplicate_terminal_receipts(tx, submission, status, now, 64).await?;
    tx.insert::<AiRunAttemptOutcomeRecord>(CreateAiRunAttemptOutcomeRecordInput {
        attempt_id: submission.attempt_id,
        run_id: submission.run_id,
        lease_generation: submission.lease_generation,
        worker_id: graph.attempt_worker_id.clone(),
        final_state: run_state.as_str().to_owned(),
        outcome_code: outcome_code.to_owned(),
        provider_response_id: submission.provider_response_id.clone(),
        finished_at: now.unix_timestamp(),
    })
    .await
    .map_err(OrmPublicError::from)?;
    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
        actor_principal_kind: "system".to_owned(),
        actor_subject: submission
            .reconciliation_owner
            .clone()
            .unwrap_or_else(|| "provider-background".to_owned()),
        action: "commit_provider_background_terminal".to_owned(),
        resource_kind: "ai_provider_background_submission".to_owned(),
        resource_reference: submission.id.to_string(),
        outcome: submission_state.as_str().to_owned(),
        reason_code: outcome_code.to_owned(),
        correlation_id: submission.id.to_string(),
        causation_id: Some(submission.run_id.to_string()),
        policy_version: Some("provider-background-reconciliation-v1".to_owned()),
    })
    .await
    .map_err(OrmPublicError::from)?;
    Ok(match status {
        ProviderBackgroundStatus::Completed => AiOpenAiBackgroundTerminalOutcome::Completed {
            message_id: message_id
                .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?,
        },
        ProviderBackgroundStatus::Failed | ProviderBackgroundStatus::Incomplete => {
            AiOpenAiBackgroundTerminalOutcome::Failed
        }
        ProviderBackgroundStatus::Cancelled => AiOpenAiBackgroundTerminalOutcome::Cancelled,
        ProviderBackgroundStatus::Queued | ProviderBackgroundStatus::InProgress => {
            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
        }
    })
}

async fn persist_background_provider_output(
    tx: &mut MutationContext<'_, DefaultWriteBackend>,
    submission: &AiProviderBackgroundSubmissionRecord,
    run: &AiRunRecord,
    output: PreparedProviderOutput,
    now: OffsetDateTime,
) -> Result<(), OrmPublicError> {
    let session = tx
        .find_by_id::<AiSessionRecord>(&submission.session_id)
        .await
        .map_err(OrmPublicError::from)?
        .ok_or_else(OrmPublicError::not_found)?;
    if output.blocks.is_empty()
        || output.message_id != background_terminal_identity(submission.id, "assistant-message")
        || output.event_id != background_terminal_identity(submission.id, "session-event")
        || output.inbox_event_id != background_terminal_identity(submission.id, "inbox-event")
        || output.provider_kind != "openai"
        || output.provider_model != submission.provider_model
        || output.provider_response_id.as_deref() != submission.provider_response_id.as_deref()
        || output.budget_reservation_id != submission.budget_reservation_id
        || output.correlation_id != submission.id.to_string()
        || session.state != "active"
        || session.deleted_at.is_some()
        || session.owner_principal_kind != output.expected_owner_principal_kind
        || session.owner_subject != output.expected_owner_subject
        || session.scope_kind != output.expected_scope_kind
        || session.scope_id != output.expected_scope_id
        || session.tenant_id != output.expected_tenant_id
    {
        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
    }
    let expected_checkpoint_hash = crate::orm_runs::final_output_checkpoint_hash(
        AiRunId(submission.run_id),
        submission.attempt_id,
        submission.lease_generation,
        output.message_id,
        submission.provider_response_id.as_deref(),
        submission.budget_reservation_id,
    );
    if output.checkpoint_hash != expected_checkpoint_hash {
        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
    }
    let message_sequence = session
        .message_head
        .checked_add(1)
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let event_sequence = session
        .stream_head
        .checked_add(1)
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    if !matches!(
        tx.compare_and_swap::<AiSessionRecord>(
            &session.id,
            session.row_version,
            AiSessionRecordWhereInput::default(),
            UpdateAiSessionRecordInput {
                message_head: Some(message_sequence),
                stream_head: Some(event_sequence),
                last_activity_at: Some(now.unix_timestamp()),
                ..Default::default()
            },
        )
        .await
        .map_err(OrmPublicError::from)?,
        ConditionalUpdateOutcome::Updated(_)
    ) {
        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
    }
    tx.insert::<AiMessageRecord>(CreateAiMessageRecordInput {
        id: output.message_id,
        session_id: session.id,
        sequence: message_sequence,
        message_role: "assistant".to_owned(),
        author_principal_kind: None,
        author_subject: None,
        client_message_id: None,
        content_hash: None,
        run_id: Some(submission.run_id),
        provider_kind: Some(output.provider_kind),
        provider_model: Some(output.provider_model),
        protected_preview: Some(output.protected_preview),
        block_count: i64::try_from(output.blocks.len())
            .map_err(|_| OrmPublicError::new(OrmErrorCode::InvalidInput))?,
        completion_state: "complete".to_owned(),
        finalized_at: Some(now.unix_timestamp()),
        content_purged_at: None,
    })
    .await
    .map_err(OrmPublicError::from)?;
    tx.insert::<AiRunCheckpointRecord>(CreateAiRunCheckpointRecordInput {
        id: output.message_id,
        run_id: submission.run_id,
        attempt_id: submission.attempt_id,
        lease_generation: submission.lease_generation,
        checkpoint_kind: "assistant_output_persisted".to_owned(),
        provider_response_id: output.provider_response_id,
        budget_reservation_id: Some(submission.budget_reservation_id),
        assistant_message_id: Some(output.message_id),
        protected_state: None,
        checkpoint_hash: output.checkpoint_hash,
    })
    .await
    .map_err(OrmPublicError::from)?;
    for (block_index, block) in output.blocks.into_iter().enumerate() {
        tx.insert::<AiMessageBlockRecord>(CreateAiMessageBlockRecordInput {
            id: block.id,
            message_id: output.message_id,
            block_index: i64::try_from(block_index)
                .map_err(|_| OrmPublicError::new(OrmErrorCode::InvalidInput))?,
            block_kind: block.kind,
            protected_content: block.protected_content,
            byte_count: block.byte_count,
            line_count: block.line_count,
        })
        .await
        .map_err(OrmPublicError::from)?;
    }
    tx.insert::<AiSessionEventRecord>(CreateAiSessionEventRecordInput {
        id: output.event_id,
        session_id: session.id,
        sequence: event_sequence,
        event_type: "assistant_message_completed".to_owned(),
        run_id: Some(submission.run_id),
        causation_id: Some(run.input_message_id.to_string()),
        correlation_id: output.correlation_id,
        protected_payload: output.protected_event,
    })
    .await
    .map_err(OrmPublicError::from)?;
    tx.queue_event(crate::AiSessionWakeup {
        session_id: session.id,
        sequence: event_sequence,
    });
    append_inbox_event(
        tx,
        PreparedAiInboxEvent {
            id: output.inbox_event_id,
            principal_kind: output.expected_owner_principal_kind,
            principal_subject: output.expected_owner_subject,
            scope: AiScope {
                kind: output.expected_scope_kind,
                id: output.expected_scope_id,
                tenant_id: output.expected_tenant_id,
            },
            session_id: session.id,
            event_type: "assistant_message_completed".to_owned(),
            protected_payload: output.protected_inbox_event,
            created_at: now.unix_timestamp(),
        },
    )
    .await?;
    Ok(())
}

async fn commit_matched_terminal_receipt(
    tx: &mut MutationContext<'_, DefaultWriteBackend>,
    submission: &AiProviderBackgroundSubmissionRecord,
    matched: &MatchedBackgroundReceipt,
    status: ProviderBackgroundStatus,
    now: OffsetDateTime,
) -> Result<(), OrmPublicError> {
    let key = AiProviderWebhookReceiptRecordKey {
        id: matched.id,
        provider_kind: matched.provider_kind.clone(),
    };
    let receipt = tx
        .find_by_key::<AiProviderWebhookReceiptRecord>(&key)
        .await
        .map_err(OrmPublicError::from)?
        .ok_or_else(OrmPublicError::not_found)?;
    validate_background_receipt(&receipt, submission, "matched_pending", true)?;
    if receipt.provider_event_kind != expected_receipt_kind(status)
        || receipt.provider_event_kind != matched.provider_event_kind
    {
        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
    }
    let outcome = tx
        .update_if::<AiProviderWebhookReceiptRecord>(
            &key,
            AiProviderWebhookReceiptRecordWhereInput {
                state: Some(StringFilter {
                    eq: Some("matched_pending".to_owned()),
                    ..Default::default()
                }),
                run_id: Some(UuidFilter {
                    eq: Some(submission.run_id),
                    ..Default::default()
                }),
                attempt_id: Some(UuidFilter {
                    eq: Some(submission.attempt_id),
                    ..Default::default()
                }),
                ..Default::default()
            },
            UpdateAiProviderWebhookReceiptRecordInput {
                state: Some("processed".to_owned()),
                safe_error_code: Some(None),
                processed_at: Some(Some(now.unix_timestamp())),
                ..Default::default()
            },
        )
        .await
        .map_err(OrmPublicError::from)?;
    let updated = match outcome {
        PredicateUpdateOutcome::Updated(updated) => updated,
        PredicateUpdateOutcome::NotFound | PredicateUpdateOutcome::PredicateConflict => {
            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
        }
    };
    if updated.state != "processed"
        || updated.safe_error_code.is_some()
        || updated.processed_at != Some(now.unix_timestamp())
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    Ok(())
}

async fn close_duplicate_terminal_receipts(
    tx: &mut MutationContext<'_, DefaultWriteBackend>,
    submission: &AiProviderBackgroundSubmissionRecord,
    status: ProviderBackgroundStatus,
    now: OffsetDateTime,
    maximum: usize,
) -> Result<(), OrmPublicError> {
    let provider_response_id = submission
        .provider_response_id
        .as_deref()
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let limit = maximum
        .checked_add(1)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let receipts = tx
        .query::<AiProviderWebhookReceiptRecord>()
        .filter(background_receipt_filter(
            submission,
            provider_response_id,
            "pending_reconciliation",
            None,
            None,
        ))
        .order_by(background_receipt_order())
        .limit(limit)
        .fetch_all()
        .await
        .map_err(OrmPublicError::from)?;
    if receipts.len() > maximum {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    for receipt in receipts {
        validate_background_receipt(&receipt, submission, "pending_reconciliation", false)?;
        let agrees = receipt.provider_event_kind == expected_receipt_kind(status);
        let key = AiProviderWebhookReceiptRecordKey {
            id: receipt.id,
            provider_kind: receipt.provider_kind.clone(),
        };
        let outcome = tx
            .update_if::<AiProviderWebhookReceiptRecord>(
                &key,
                AiProviderWebhookReceiptRecordWhereInput {
                    state: Some(StringFilter {
                        eq: Some("pending_reconciliation".to_owned()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                UpdateAiProviderWebhookReceiptRecordInput {
                    run_id: Some(Some(submission.run_id)),
                    attempt_id: Some(Some(submission.attempt_id)),
                    state: Some(
                        if agrees {
                            "duplicate_terminal"
                        } else {
                            "recovery_required"
                        }
                        .to_owned(),
                    ),
                    safe_error_code: Some(
                        (!agrees).then(|| "provider_terminal_receipt_conflict".to_owned()),
                    ),
                    processed_at: Some(Some(now.unix_timestamp())),
                    ..Default::default()
                },
            )
            .await
            .map_err(OrmPublicError::from)?;
        if !matches!(outcome, PredicateUpdateOutcome::Updated(_)) {
            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
        }
    }
    Ok(())
}

async fn validate_existing_background_terminal(
    database: &Database<DefaultWriteBackend>,
    observation: &AiOpenAiBackgroundRetrievalObservation,
    actual: AiBudgetAmounts,
) -> Result<(), TransactionError> {
    let observation = observation.clone();
    let database = database.clone();
    database
        .transaction(TransactionMode::StateMachine, move |tx| {
            Box::pin(async move {
                let claim = &observation.claim;
                let submission = tx
                    .find_by_id::<AiProviderBackgroundSubmissionRecord>(&claim.submission_id)
                    .await
                    .map_err(OrmPublicError::from)?
                    .ok_or_else(OrmPublicError::not_found)?;
                validate_observation_submission_binding(&submission, claim)
                    .map_err(|_| OrmPublicError::new(OrmErrorCode::Conflict))?;
                let (run_state, submission_state, safe_error_code) = match observation.status() {
                    ProviderBackgroundStatus::Completed => (
                        AiRunState::Completed,
                        BackgroundSubmissionState::Completed,
                        None,
                    ),
                    ProviderBackgroundStatus::Failed => (
                        AiRunState::Failed,
                        BackgroundSubmissionState::Failed,
                        Some("provider_response_failed"),
                    ),
                    ProviderBackgroundStatus::Incomplete => (
                        AiRunState::Failed,
                        BackgroundSubmissionState::Failed,
                        Some("provider_response_incomplete"),
                    ),
                    ProviderBackgroundStatus::Cancelled => (
                        AiRunState::Cancelled,
                        BackgroundSubmissionState::Cancelled,
                        Some("provider_response_cancelled"),
                    ),
                    ProviderBackgroundStatus::Queued | ProviderBackgroundStatus::InProgress => {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                };
                let expected_message_id = (observation.status()
                    == ProviderBackgroundStatus::Completed)
                    .then(|| background_terminal_identity(submission.id, "assistant-message"));
                if submission.state != submission_state.as_str()
                    || submission.provider_status.as_deref()
                        != Some(background_status_value(observation.status()))
                    || submission.safe_error_code.as_deref() != safe_error_code
                    || submission.reconciled_at.is_none()
                    || submission.terminal_message_id != expected_message_id
                    || submission.reconciliation_owner.is_some()
                    || submission.reconciliation_lease_expires_at.is_some()
                    || submission.reconciliation_next_attempt_at.is_some()
                    || submission.retrieval_egress_decision_id.is_none()
                {
                    return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                }
                let budget = tx
                    .find_by_id::<AiBudgetReservationRecord>(&submission.budget_reservation_id)
                    .await
                    .map_err(OrmPublicError::from)?
                    .ok_or_else(OrmPublicError::not_found)?;
                validate_terminal_budget_binding(&budget, claim)
                    .map_err(|_| OrmPublicError::new(OrmErrorCode::Conflict))?;
                if budget.state != "committed"
                    || budget.actual_input_tokens != amount_i64(actual.input_tokens)
                    || budget.actual_cached_input_tokens
                        != amount_i64(
                            observation
                                .usage()
                                .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?
                                .cached_input_tokens(),
                        )
                    || budget.actual_output_tokens != amount_i64(actual.output_tokens)
                    || budget.actual_tool_units != amount_i64(actual.tool_units)
                    || budget.actual_image_units != amount_i64(actual.image_units)
                    || budget.actual_cost_microunits != amount_i64(actual.cost_microunits)
                    || budget.actual_runs != amount_i64(actual.runs)
                    || budget.reconciled_at.is_none()
                {
                    return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                }
                let usage = tx
                    .query::<AiUsageEntryRecord>()
                    .filter(AiUsageEntryRecordWhereInput {
                        budget_reservation_id: Some(UuidFilter {
                            eq: Some(budget.id),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })
                    .limit(2)
                    .fetch_all()
                    .await
                    .map_err(OrmPublicError::from)?;
                let [usage] = usage.as_slice() else {
                    return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                };
                if usage.run_id != Some(submission.run_id)
                    || usage.provider_kind != "openai"
                    || usage.provider_model != submission.provider_model
                    || Some(usage.input_tokens) != budget.actual_input_tokens
                    || Some(usage.cached_input_tokens) != budget.actual_cached_input_tokens
                    || Some(usage.output_tokens) != budget.actual_output_tokens
                    || Some(usage.tool_units) != budget.actual_tool_units
                    || Some(usage.image_units) != budget.actual_image_units
                    || usage.cost_microunits != budget.actual_cost_microunits
                {
                    return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                }
                let run = tx
                    .find_by_id::<AiRunRecord>(&submission.run_id)
                    .await
                    .map_err(OrmPublicError::from)?
                    .ok_or_else(OrmPublicError::not_found)?;
                if run.state != run_state.as_str()
                    || run.attempt_id != Some(submission.attempt_id)
                    || run.lease_generation != submission.lease_generation
                    || run.lease_owner.is_some()
                    || run.lease_expires_at.is_some()
                    || run.lease_heartbeat_at.is_some()
                    || run.next_attempt_at.is_some()
                    || run.error_code.as_deref() != safe_error_code
                    || run.latest_checkpoint_id != expected_message_id
                {
                    return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                }
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
                let [outcome] = outcomes.as_slice() else {
                    return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                };
                if outcome.run_id != submission.run_id
                    || outcome.lease_generation != submission.lease_generation
                    || outcome.final_state != run_state.as_str()
                    || outcome.provider_response_id != submission.provider_response_id
                {
                    return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                }
                if let Some(message_id) = expected_message_id {
                    validate_existing_background_output(tx, &submission, &run, message_id).await?;
                }
                let processed = tx
                    .query::<AiProviderWebhookReceiptRecord>()
                    .filter(background_receipt_filter(
                        &submission,
                        submission
                            .provider_response_id
                            .as_deref()
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?,
                        "processed",
                        Some(submission.run_id),
                        Some(submission.attempt_id),
                    ))
                    .limit(2)
                    .fetch_all()
                    .await
                    .map_err(OrmPublicError::from)?;
                if processed.len() > 1
                    || processed.first().is_some_and(|receipt| {
                        receipt.provider_event_kind != expected_receipt_kind(observation.status())
                            || receipt.processed_at.is_none()
                    })
                {
                    return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                }
                Ok(())
            })
        })
        .await
}

async fn validate_existing_background_output(
    tx: &mut MutationContext<'_, DefaultWriteBackend>,
    submission: &AiProviderBackgroundSubmissionRecord,
    run: &AiRunRecord,
    message_id: Uuid,
) -> Result<(), OrmPublicError> {
    let message = tx
        .find_by_id::<AiMessageRecord>(&message_id)
        .await
        .map_err(OrmPublicError::from)?
        .ok_or_else(OrmPublicError::not_found)?;
    let checkpoints = tx
        .query::<AiRunCheckpointRecord>()
        .filter(AiRunCheckpointRecordWhereInput {
            id: Some(UuidFilter {
                eq: Some(message_id),
                ..Default::default()
            }),
            ..Default::default()
        })
        .limit(2)
        .fetch_all()
        .await
        .map_err(OrmPublicError::from)?;
    let [checkpoint] = checkpoints.as_slice() else {
        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
    };
    let blocks = tx
        .query::<AiMessageBlockRecord>()
        .filter(AiMessageBlockRecordWhereInput {
            message_id: Some(UuidFilter {
                eq: Some(message_id),
                ..Default::default()
            }),
            ..Default::default()
        })
        .limit(
            message
                .block_count
                .checked_add(1)
                .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?,
        )
        .fetch_all()
        .await
        .map_err(OrmPublicError::from)?;
    let event = tx
        .find_by_id::<AiSessionEventRecord>(&background_terminal_identity(
            submission.id,
            "session-event",
        ))
        .await
        .map_err(OrmPublicError::from)?
        .ok_or_else(OrmPublicError::not_found)?;
    let inbox = tx
        .find_by_id::<AiInboxEventRecord>(&background_terminal_identity(
            submission.id,
            "inbox-event",
        ))
        .await
        .map_err(OrmPublicError::from)?
        .ok_or_else(OrmPublicError::not_found)?;
    if message.session_id != submission.session_id
        || message.run_id != Some(submission.run_id)
        || message.message_role != "assistant"
        || message.provider_kind.as_deref() != Some("openai")
        || message.provider_model.as_deref() != Some(submission.provider_model.as_str())
        || message.completion_state != "complete"
        || message.finalized_at.is_none()
        || message.content_purged_at.is_some()
        || message.block_count <= 0
        || usize::try_from(message.block_count).ok() != Some(blocks.len())
        || blocks.iter().enumerate().any(|(index, block)| {
            block.block_index != i64::try_from(index).unwrap_or(-1)
                || block.message_id != message_id
        })
        || checkpoint.run_id != submission.run_id
        || checkpoint.attempt_id != submission.attempt_id
        || checkpoint.lease_generation != submission.lease_generation
        || checkpoint.checkpoint_kind != "assistant_output_persisted"
        || checkpoint.provider_response_id != submission.provider_response_id
        || checkpoint.budget_reservation_id != Some(submission.budget_reservation_id)
        || checkpoint.assistant_message_id != Some(message_id)
        || checkpoint.protected_state.is_some()
        || checkpoint.checkpoint_hash
            != crate::orm_runs::final_output_checkpoint_hash(
                AiRunId(submission.run_id),
                submission.attempt_id,
                submission.lease_generation,
                message_id,
                submission.provider_response_id.as_deref(),
                submission.budget_reservation_id,
            )
        || event.session_id != submission.session_id
        || event.event_type != "assistant_message_completed"
        || event.run_id != Some(submission.run_id)
        || event.causation_id != Some(run.input_message_id.to_string())
        || inbox.session_id != Some(submission.session_id)
        || inbox.event_type != "assistant_message_completed"
    {
        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
    }
    Ok(())
}

fn amount_i64(value: u64) -> Option<i64> {
    i64::try_from(value).ok()
}

fn validate_claim_graph(
    claim: &AiOpenAiBackgroundReconciliationClaim,
    graph: &BackgroundClaimGraph,
) -> Result<(), OrmPublicError> {
    if graph.principal_reference != claim.principal_reference
        || graph.scope_kind != claim.scope_kind
        || graph.scope_id != claim.scope_id
        || graph.tenant_id != claim.tenant_id
        || graph.original_egress_destination != claim.original_egress_destination
        || graph.original_maximum_classification != claim.original_maximum_classification
    {
        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
    }
    Ok(())
}

fn validate_retrieval_egress_record(
    record: &AiEgressEventRecord,
    manifest: &AiEgressManifest,
    decision: &crate::AiEgressDecision,
    expected_principal_subject: &str,
) -> Result<(), OrmPublicError> {
    if record.id != decision.id.0
        || record.run_id != manifest.run_id.map(|run_id| run_id.0)
        || record.principal_subject != decision.principal_subject
        || record.principal_subject != expected_principal_subject
        || record.scope_kind != manifest.scope.kind
        || record.scope_id != manifest.scope.id
        || record.manifest_hash != decision.manifest_hash
        || record.manifest_hash != manifest.stable_hash()
        || record.destination != manifest.destination
        || record.capability != "model_inference"
        || record.classification != classification_value(manifest.maximum_classification())
        || record.outcome != "allow"
        || record.reason_code != "allowed"
        || record.policy_version != decision.policy_version
        || u64::try_from(record.estimated_bytes).ok() != Some(manifest.estimated_bytes)
        || record.estimated_tokens != 0
    {
        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
    }
    Ok(())
}

fn classification_value(value: DataClassification) -> &'static str {
    match value {
        DataClassification::Public => "public",
        DataClassification::Internal => "internal",
        DataClassification::Confidential => "confidential",
        DataClassification::Restricted => "restricted",
        DataClassification::Secret => "secret",
    }
}

fn retrieval_timeout(
    now: OffsetDateTime,
    claim_expiry: OffsetDateTime,
    configured_maximum: Duration,
) -> Result<std::time::Duration, AiError> {
    let remaining = claim_expiry - now;
    let safety_margin = Duration::milliseconds(1);
    if remaining <= safety_margin {
        return Err(AiError::Conflict);
    }
    let timeout = std::cmp::min(configured_maximum, remaining - safety_margin);
    if timeout <= Duration::ZERO {
        return Err(AiError::Conflict);
    }
    Ok(timeout.unsigned_abs())
}

fn classify_background_retrieval_error(
    claim: AiOpenAiBackgroundReconciliationClaim,
    error: &ProviderError,
) -> AiOpenAiBackgroundRetrievalAttempt {
    match error {
        ProviderError::RateLimited => {
            AiOpenAiBackgroundRetrievalAttempt::Retryable(background_retrieval_failure(
                claim,
                BackgroundRetrievalFailureDisposition::Retryable,
                "provider_response_retrieval_rate_limited",
            ))
        }
        ProviderError::Unavailable => {
            AiOpenAiBackgroundRetrievalAttempt::Retryable(background_retrieval_failure(
                claim,
                BackgroundRetrievalFailureDisposition::Retryable,
                "provider_response_retrieval_unavailable",
            ))
        }
        ProviderError::CredentialUnavailable => {
            AiOpenAiBackgroundRetrievalAttempt::RecoveryRequired(background_retrieval_failure(
                claim,
                BackgroundRetrievalFailureDisposition::RecoveryRequired,
                "provider_response_credential_unavailable",
            ))
        }
        ProviderError::InvalidConfiguration(_)
        | ProviderError::InvalidRequest
        | ProviderError::EgressDenied
        | ProviderError::BudgetDenied
        | ProviderError::Unsupported
        | ProviderError::Rejected
        | ProviderError::Cancelled => {
            AiOpenAiBackgroundRetrievalAttempt::RecoveryRequired(background_retrieval_failure(
                claim,
                BackgroundRetrievalFailureDisposition::RecoveryRequired,
                "provider_response_retrieval_rejected",
            ))
        }
    }
}

fn background_retrieval_failure(
    claim: AiOpenAiBackgroundReconciliationClaim,
    disposition: BackgroundRetrievalFailureDisposition,
    safe_error_code: &'static str,
) -> AiOpenAiBackgroundRetrievalFailure {
    AiOpenAiBackgroundRetrievalFailure {
        claim,
        disposition,
        safe_error_code,
    }
}

fn background_retry_delay(
    retry_count: u32,
    maximum_retry_delay: Duration,
) -> Result<Duration, AiError> {
    let shift = retry_count.min(30);
    let seconds = 1_i64
        .checked_shl(shift)
        .unwrap_or(i64::MAX)
        .min(maximum_retry_delay.whole_seconds());
    if seconds <= 0 {
        return Err(AiError::InvalidConfiguration(
            "invalid background retry delay".to_owned(),
        ));
    }
    Ok(Duration::seconds(seconds))
}

const fn background_status_value(status: crate::ProviderBackgroundStatus) -> &'static str {
    match status {
        crate::ProviderBackgroundStatus::Queued => "queued",
        crate::ProviderBackgroundStatus::InProgress => "in_progress",
        crate::ProviderBackgroundStatus::Completed => "completed",
        crate::ProviderBackgroundStatus::Failed => "failed",
        crate::ProviderBackgroundStatus::Incomplete => "incomplete",
        crate::ProviderBackgroundStatus::Cancelled => "cancelled",
    }
}

struct BackgroundClaimGraph {
    principal_reference: PrincipalReference,
    attempt_worker_id: String,
    scope_kind: String,
    scope_id: String,
    tenant_id: Option<String>,
    original_egress_destination: String,
    original_maximum_classification: DataClassification,
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
    let provider_response_id = submission
        .provider_response_id
        .as_deref()
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let retrieval_egress = match submission.retrieval_egress_decision_id {
        Some(decision_id) => {
            let events = tx
                .query::<AiEgressEventRecord>()
                .filter(AiEgressEventRecordWhereInput {
                    id: Some(UuidFilter {
                        eq: Some(decision_id),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .limit(2)
                .fetch_all()
                .await
                .map_err(OrmPublicError::from)?;
            let [event] = events.as_slice() else {
                return Err(OrmPublicError::new(OrmErrorCode::InternalError));
            };
            Some(event.clone())
        }
        None => None,
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
        || retrieval_egress.is_some_and(|retrieval| {
            Some(retrieval.id) != submission.retrieval_egress_decision_id
                || retrieval.run_id != Some(submission.run_id)
                || retrieval.principal_subject != principal_reference.subject
                || retrieval.scope_kind != session.scope_kind
                || retrieval.scope_id != session.scope_id
                || retrieval.destination != egress.destination
                || retrieval.capability != "model_inference"
                || retrieval.classification != egress.classification
                || retrieval.outcome != "allow"
                || retrieval.reason_code != "allowed"
                || !valid_sha256_hex(&retrieval.manifest_hash)
                || retrieval.policy_version.trim().is_empty()
                || u64::try_from(retrieval.estimated_bytes).ok()
                    != Some(provider_response_id.len() as u64)
                || retrieval.estimated_tokens != 0
        })
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    let original_maximum_classification = persisted_classification(&egress.classification)
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    Ok(BackgroundClaimGraph {
        principal_reference,
        attempt_worker_id: attempt.worker_id.clone(),
        scope_kind: session.scope_kind,
        scope_id: session.scope_id,
        tenant_id: session.tenant_id,
        original_egress_destination: egress.destination.clone(),
        original_maximum_classification,
    })
}

fn validate_expired_submission(
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
    let state_fields_valid = match state {
        BackgroundSubmissionState::WaitingProvider => {
            submission.reconciliation_owner.is_none()
                && submission.reconciliation_lease_expires_at.is_none()
                && submission.retrieval_egress_decision_id.is_none()
        }
        BackgroundSubmissionState::Reconciling => {
            submission.reconciliation_generation > 0
                && submission
                    .reconciliation_owner
                    .as_deref()
                    .is_some_and(|owner| validate_worker_id(owner).is_ok())
                && submission.reconciliation_lease_expires_at == Some(next_attempt_at)
                && next_attempt_at <= deadline
        }
        BackgroundSubmissionState::Prepared
        | BackgroundSubmissionState::Completed
        | BackgroundSubmissionState::Failed
        | BackgroundSubmissionState::Cancelled
        | BackgroundSubmissionState::RecoveryRequired => false,
    };
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
        || deadline > now.unix_timestamp()
        || deadline <= submitted_at
        || deadline > maximum_deadline
        || submission.safe_error_code.is_some()
        || submission.reconciliation_generation < 0
        || submission.reconciliation_retry_count < 0
        || submission.reconciliation_retry_count > submission.reconciliation_generation
        || u32::try_from(submission.reconciliation_retry_count)
            .map_or(true, |count| count > maximum_retries)
        || submission.reconciled_at.is_some()
        || submission.terminal_message_id.is_some()
        || submission.row_version < 1
        || !state_fields_valid
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    Ok(())
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
        || submission.terminal_message_id.is_some()
        || submission.row_version < 1
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    match state {
        BackgroundSubmissionState::WaitingProvider
            if submission.reconciliation_owner.is_none()
                && submission.reconciliation_lease_expires_at.is_none()
                && submission.retrieval_egress_decision_id.is_none()
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
    retrieval_egress_decision_id: Option<Uuid>,
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
        || submission.retrieval_egress_decision_id != retrieval_egress_decision_id
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
        || submission.submission_key != claim.submission_key
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
        || submission.retrieval_egress_decision_id != claim.retrieval_egress_decision_id
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
        claim.retrieval_egress_decision_id,
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

async fn close_background_recovery(
    tx: &mut MutationContext<'_, DefaultWriteBackend>,
    submission: &AiProviderBackgroundSubmissionRecord,
    graph: &BackgroundClaimGraph,
    safe_error_code: &'static str,
    now: OffsetDateTime,
) -> Result<(), OrmPublicError> {
    let run = tx
        .find_by_id::<AiRunRecord>(&submission.run_id)
        .await
        .map_err(OrmPublicError::from)?
        .ok_or_else(OrmPublicError::not_found)?;
    if run.state != AiRunState::WaitingProvider.as_str()
        || run.attempt_id != Some(submission.attempt_id)
        || run.lease_generation != submission.lease_generation
        || run.lease_owner.is_some()
        || run.lease_expires_at.is_some()
        || run.lease_heartbeat_at.is_some()
        || run.next_attempt_at.is_some()
        || run.error_code.is_some()
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    let submission_update = tx
        .compare_and_swap::<AiProviderBackgroundSubmissionRecord>(
            &submission.id,
            submission.row_version,
            exact_background_state(&submission.state),
            UpdateAiProviderBackgroundSubmissionRecordInput {
                state: Some(
                    BackgroundSubmissionState::RecoveryRequired
                        .as_str()
                        .to_owned(),
                ),
                safe_error_code: Some(Some(safe_error_code.to_owned())),
                reconciliation_owner: Some(None),
                reconciliation_lease_expires_at: Some(None),
                reconciliation_next_attempt_at: Some(None),
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
    if updated_submission.state != BackgroundSubmissionState::RecoveryRequired.as_str()
        || updated_submission.safe_error_code.as_deref() != Some(safe_error_code)
        || updated_submission.reconciliation_owner.is_some()
        || updated_submission.reconciliation_lease_expires_at.is_some()
        || updated_submission.reconciliation_next_attempt_at.is_some()
        || updated_submission.reconciled_at.is_some()
        || updated_submission.terminal_message_id.is_some()
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    let run_update = tx
        .compare_and_swap::<AiRunRecord>(
            &run.id,
            run.row_version,
            exact_state(AiRunState::WaitingProvider.as_str()),
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
    tx.insert::<AiRunAttemptOutcomeRecord>(CreateAiRunAttemptOutcomeRecordInput {
        attempt_id: submission.attempt_id,
        run_id: submission.run_id,
        lease_generation: submission.lease_generation,
        worker_id: graph.attempt_worker_id.clone(),
        final_state: AiRunState::RecoveryRequired.as_str().to_owned(),
        outcome_code: safe_error_code.to_owned(),
        provider_response_id: submission.provider_response_id.clone(),
        finished_at: now.unix_timestamp(),
    })
    .await
    .map_err(OrmPublicError::from)?;
    close_matched_receipt_recovery(tx, submission, safe_error_code, now).await?;
    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
        actor_principal_kind: "system".to_owned(),
        actor_subject: submission
            .reconciliation_owner
            .clone()
            .unwrap_or_else(|| "provider-background".to_owned()),
        action: "close_provider_background_reconciliation".to_owned(),
        resource_kind: "ai_provider_background_submission".to_owned(),
        resource_reference: submission.id.to_string(),
        outcome: "recovery_required".to_owned(),
        reason_code: safe_error_code.to_owned(),
        correlation_id: submission.id.to_string(),
        causation_id: Some(submission.run_id.to_string()),
        policy_version: Some("provider-background-reconciliation-v1".to_owned()),
    })
    .await
    .map_err(OrmPublicError::from)?;
    Ok(())
}

async fn close_matched_receipt_recovery(
    tx: &mut MutationContext<'_, DefaultWriteBackend>,
    submission: &AiProviderBackgroundSubmissionRecord,
    safe_error_code: &str,
    now: OffsetDateTime,
) -> Result<(), OrmPublicError> {
    let provider_response_id = submission
        .provider_response_id
        .as_deref()
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let matched = tx
        .query::<AiProviderWebhookReceiptRecord>()
        .filter(background_receipt_filter(
            submission,
            provider_response_id,
            "matched_pending",
            Some(submission.run_id),
            Some(submission.attempt_id),
        ))
        .order_by(background_receipt_order())
        .limit(2)
        .fetch_all()
        .await
        .map_err(OrmPublicError::from)?;
    let receipt = match matched.as_slice() {
        [] => return Ok(()),
        [receipt] => receipt,
        _ => return Err(OrmPublicError::new(OrmErrorCode::InternalError)),
    };
    validate_background_receipt(receipt, submission, "matched_pending", true)?;
    let key = AiProviderWebhookReceiptRecordKey {
        id: receipt.id,
        provider_kind: receipt.provider_kind.clone(),
    };
    let outcome = tx
        .update_if::<AiProviderWebhookReceiptRecord>(
            &key,
            AiProviderWebhookReceiptRecordWhereInput {
                state: Some(StringFilter {
                    eq: Some("matched_pending".to_owned()),
                    ..Default::default()
                }),
                run_id: Some(UuidFilter {
                    eq: Some(submission.run_id),
                    ..Default::default()
                }),
                attempt_id: Some(UuidFilter {
                    eq: Some(submission.attempt_id),
                    ..Default::default()
                }),
                ..Default::default()
            },
            UpdateAiProviderWebhookReceiptRecordInput {
                state: Some("recovery_required".to_owned()),
                safe_error_code: Some(Some(safe_error_code.to_owned())),
                processed_at: Some(Some(now.unix_timestamp())),
                ..Default::default()
            },
        )
        .await
        .map_err(OrmPublicError::from)?;
    let updated = match outcome {
        PredicateUpdateOutcome::Updated(updated) => updated,
        PredicateUpdateOutcome::NotFound | PredicateUpdateOutcome::PredicateConflict => {
            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
        }
    };
    if updated.state != "recovery_required"
        || updated.safe_error_code.as_deref() != Some(safe_error_code)
        || updated.processed_at != Some(now.unix_timestamp())
        || updated.run_id != Some(submission.run_id)
        || updated.attempt_id != Some(submission.attempt_id)
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    Ok(())
}

async fn match_background_receipt(
    tx: &mut MutationContext<'_, DefaultWriteBackend>,
    submission: &AiProviderBackgroundSubmissionRecord,
    maximum_scan: usize,
) -> Result<Option<MatchedBackgroundReceipt>, OrmPublicError> {
    let provider_response_id = submission
        .provider_response_id
        .as_deref()
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let matched = tx
        .query::<AiProviderWebhookReceiptRecord>()
        .filter(background_receipt_filter(
            submission,
            provider_response_id,
            "matched_pending",
            Some(submission.run_id),
            Some(submission.attempt_id),
        ))
        .order_by(background_receipt_order())
        .limit(2)
        .fetch_all()
        .await
        .map_err(OrmPublicError::from)?;
    match matched.as_slice() {
        [receipt] => {
            validate_background_receipt(receipt, submission, "matched_pending", true)?;
            return Ok(Some(matched_receipt(receipt)));
        }
        [] => {}
        _ => return Err(OrmPublicError::new(OrmErrorCode::InternalError)),
    }

    let pending = tx
        .query::<AiProviderWebhookReceiptRecord>()
        .filter(background_receipt_filter(
            submission,
            provider_response_id,
            "pending_reconciliation",
            None,
            None,
        ))
        .order_by(background_receipt_order())
        .limit(
            i64::try_from(maximum_scan)
                .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?,
        )
        .fetch_all()
        .await
        .map_err(OrmPublicError::from)?;
    let Some(receipt) = pending.first() else {
        return Ok(None);
    };
    validate_background_receipt(receipt, submission, "pending_reconciliation", false)?;
    let key = AiProviderWebhookReceiptRecordKey {
        id: receipt.id,
        provider_kind: receipt.provider_kind.clone(),
    };
    let outcome = tx
        .update_if::<AiProviderWebhookReceiptRecord>(
            &key,
            AiProviderWebhookReceiptRecordWhereInput {
                state: Some(StringFilter {
                    eq: Some("pending_reconciliation".to_owned()),
                    ..Default::default()
                }),
                run_id: Some(UuidFilter {
                    is_null: Some(true),
                    ..Default::default()
                }),
                attempt_id: Some(UuidFilter {
                    is_null: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            },
            UpdateAiProviderWebhookReceiptRecordInput {
                run_id: Some(Some(submission.run_id)),
                attempt_id: Some(Some(submission.attempt_id)),
                state: Some("matched_pending".to_owned()),
                ..Default::default()
            },
        )
        .await
        .map_err(OrmPublicError::from)?;
    let updated = match outcome {
        PredicateUpdateOutcome::Updated(updated) => updated,
        PredicateUpdateOutcome::NotFound | PredicateUpdateOutcome::PredicateConflict => {
            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
        }
    };
    validate_background_receipt(&updated, submission, "matched_pending", true)?;
    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
        actor_principal_kind: "system".to_owned(),
        actor_subject: submission
            .reconciliation_owner
            .clone()
            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?,
        action: "match_provider_webhook_receipt".to_owned(),
        resource_kind: "ai_provider_webhook_receipt".to_owned(),
        resource_reference: updated.id.to_string(),
        outcome: "matched_pending".to_owned(),
        reason_code: "exact_background_submission_bound".to_owned(),
        correlation_id: submission.id.to_string(),
        causation_id: Some(submission.run_id.to_string()),
        policy_version: Some("provider-background-reconciliation-v1".to_owned()),
    })
    .await
    .map_err(OrmPublicError::from)?;
    Ok(Some(matched_receipt(&updated)))
}

fn background_receipt_filter(
    submission: &AiProviderBackgroundSubmissionRecord,
    provider_response_id: &str,
    state: &str,
    run_id: Option<Uuid>,
    attempt_id: Option<Uuid>,
) -> AiProviderWebhookReceiptRecordWhereInput {
    AiProviderWebhookReceiptRecordWhereInput {
        provider_kind: Some(StringFilter {
            eq: Some("openai".to_owned()),
            ..Default::default()
        }),
        provider_profile_id: Some(StringFilter {
            eq: Some(submission.provider_profile_id.clone()),
            ..Default::default()
        }),
        provider_response_id: Some(StringFilter {
            eq: Some(provider_response_id.to_owned()),
            ..Default::default()
        }),
        run_id: run_id.map(|run_id| UuidFilter {
            eq: Some(run_id),
            ..Default::default()
        }),
        attempt_id: attempt_id.map(|attempt_id| UuidFilter {
            eq: Some(attempt_id),
            ..Default::default()
        }),
        state: Some(StringFilter {
            eq: Some(state.to_owned()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn background_receipt_order() -> AiProviderWebhookReceiptRecordOrderByInput {
    AiProviderWebhookReceiptRecordOrderByInput {
        received_at: Some(OrderDirection::Asc),
        provider_event_id: Some(OrderDirection::Asc),
        id: Some(OrderDirection::Asc),
    }
}

fn validate_background_receipt(
    receipt: &AiProviderWebhookReceiptRecord,
    submission: &AiProviderBackgroundSubmissionRecord,
    expected_state: &str,
    expected_match: bool,
) -> Result<(), OrmPublicError> {
    let provider_response_id = submission
        .provider_response_id
        .as_deref()
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let (receipt_key, receipt_id) = crate::providers::webhook_receipt_identity(
        &receipt.provider_profile_id,
        &receipt.provider_event_id,
    );
    let valid_kind = matches!(
        receipt.provider_event_kind.as_str(),
        "response_completed" | "response_failed" | "response_incomplete" | "response_cancelled"
    );
    if receipt.id != receipt_id
        || receipt.receipt_key != receipt_key
        || receipt.provider_kind != "openai"
        || receipt.provider_profile_id != submission.provider_profile_id
        || receipt.provider_event_id.trim().is_empty()
        || receipt.provider_event_id.len() > 200
        || !valid_kind
        || receipt.provider_created_at <= 0
        || receipt.provider_response_id.as_deref() != Some(provider_response_id)
        || !receipt.signature_verified
        || receipt.state != expected_state
        || receipt.safe_error_code.is_some()
        || receipt.received_at <= 0
        || receipt.processed_at.is_some()
        || receipt.row_version < 0
        || expected_match
            != (receipt.run_id == Some(submission.run_id)
                && receipt.attempt_id == Some(submission.attempt_id))
        || (!expected_match && (receipt.run_id.is_some() || receipt.attempt_id.is_some()))
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    Ok(())
}

fn matched_receipt(receipt: &AiProviderWebhookReceiptRecord) -> MatchedBackgroundReceipt {
    MatchedBackgroundReceipt {
        id: receipt.id,
        provider_kind: receipt.provider_kind.clone(),
        provider_event_kind: receipt.provider_event_kind.clone(),
    }
}

fn claim_from_records(
    submission: &AiProviderBackgroundSubmissionRecord,
    graph: BackgroundClaimGraph,
    matched_receipt: Option<MatchedBackgroundReceipt>,
) -> Result<AiOpenAiBackgroundReconciliationClaim, AiError> {
    Ok(AiOpenAiBackgroundReconciliationClaim {
        submission_id: submission.id,
        submission_key: submission.submission_key.clone(),
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
        original_egress_destination: graph.original_egress_destination,
        original_maximum_classification: graph.original_maximum_classification,
        retrieval_egress_decision_id: submission.retrieval_egress_decision_id,
        scope_kind: graph.scope_kind,
        scope_id: graph.scope_id,
        tenant_id: graph.tenant_id,
        matched_receipt,
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

fn persisted_classification(value: &str) -> Option<DataClassification> {
    match value {
        "public" => Some(DataClassification::Public),
        "internal" => Some(DataClassification::Internal),
        "confidential" => Some(DataClassification::Confidential),
        "restricted" => Some(DataClassification::Restricted),
        "secret" => Some(DataClassification::Secret),
        _ => None,
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
