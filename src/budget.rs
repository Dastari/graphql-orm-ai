//! Atomic budget reservation contracts for provider execution.

use agql_auth::ResolvedPrincipal;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    AiBudgetReservationId, AiError, AiRunId, AiScope, AiSessionId, ProviderError, ProviderKind,
};

/// Token, cost, and unit capacity reserved or consumed by one provider call.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiBudgetAmounts {
    /// Estimated or actual total input tokens.
    ///
    /// Provider-reported cached input is a subset recorded separately during
    /// reconciliation; it must not be added to this total.
    pub input_tokens: u64,
    /// Maximum or actual output tokens.
    pub output_tokens: u64,
    /// Provider/tool-specific billable units.
    pub tool_units: u64,
    /// Provider/image-specific billable units.
    pub image_units: u64,
    /// Cost in deployment-defined integer microunits.
    pub cost_microunits: u64,
    /// Run/call count consumed by the reservation.
    pub runs: u64,
}

impl AiBudgetAmounts {
    /// Returns whether this amount fits completely within the supplied ceiling.
    pub const fn fits_within(self, ceiling: Self) -> bool {
        self.input_tokens <= ceiling.input_tokens
            && self.output_tokens <= ceiling.output_tokens
            && self.tool_units <= ceiling.tool_units
            && self.image_units <= ceiling.image_units
            && self.cost_microunits <= ceiling.cost_microunits
            && self.runs <= ceiling.runs
    }
}

/// Durable state of an atomic provider budget reservation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiBudgetReservationState {
    /// Capacity is reserved and may authorize its exact provider call once.
    Reserved,
    /// Actual usage was committed and unused capacity released.
    Committed,
    /// No provider call occurred and all reserved capacity was released.
    Released,
    /// External execution is uncertain; capacity remains held for reconciliation.
    Uncertain,
    /// A provably unused reservation expired and was released.
    Expired,
}

/// Request passed to the transactional budget service before provider egress.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiBudgetReservationRequest {
    /// Session/application scope used to resolve applicable budget policies.
    pub scope: AiScope,
    /// Session owning the provider call.
    pub session_id: AiSessionId,
    /// Run owning the provider call.
    pub run_id: AiRunId,
    /// Current durable attempt identifier.
    pub attempt_id: Uuid,
    /// Current monotonically increasing run fencing generation.
    pub lease_generation: i64,
    /// Exact provider family.
    pub provider_kind: ProviderKind,
    /// Exact provider model.
    pub model: String,
    /// Immutable pricing-policy version used for the estimate.
    pub pricing_policy_version: String,
    /// Capacity to reserve before external execution.
    pub estimate: AiBudgetAmounts,
    /// Content-bound idempotency identifier for this provider start.
    pub idempotency_key: String,
    /// Latest time at which an unstarted reservation may be released.
    pub expires_at: OffsetDateTime,
}

/// Exact durable reservation returned by an atomic budget service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiBudgetReservation {
    id: AiBudgetReservationId,
    run_id: AiRunId,
    attempt_id: Uuid,
    lease_generation: i64,
    provider_kind: ProviderKind,
    model: String,
    pricing_policy_version: String,
    reserved: AiBudgetAmounts,
    actual: Option<AiBudgetAmounts>,
    state: AiBudgetReservationState,
    expires_at: OffsetDateTime,
}

impl AiBudgetReservation {
    /// Creates a reserved result after an implementation has atomically updated
    /// every applicable counter.
    ///
    /// This constructor performs no persistence. It exists for trusted custom
    /// [`AiBudgetService`] implementations; constructing the value alone does
    /// not prove that counters were reserved.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] for invalid identifiers,
    /// versions, models, generations, or a zero run reservation. Expiry is
    /// evaluated against the caller-provided trusted clock when authorizing a
    /// provider call.
    #[allow(clippy::too_many_arguments)]
    pub fn new_reserved(
        id: AiBudgetReservationId,
        run_id: AiRunId,
        attempt_id: Uuid,
        lease_generation: i64,
        provider_kind: ProviderKind,
        model: impl Into<String>,
        pricing_policy_version: impl Into<String>,
        reserved: AiBudgetAmounts,
        expires_at: OffsetDateTime,
    ) -> Result<Self, AiError> {
        let model = model.into();
        let pricing_policy_version = pricing_policy_version.into();
        if attempt_id.is_nil()
            || lease_generation < 0
            || model.trim().is_empty()
            || pricing_policy_version.trim().is_empty()
            || reserved.runs == 0
        {
            return Err(AiError::InvalidConfiguration(
                "invalid budget reservation binding".to_owned(),
            ));
        }
        Ok(Self {
            id,
            run_id,
            attempt_id,
            lease_generation,
            provider_kind,
            model,
            pricing_policy_version,
            reserved,
            actual: None,
            state: AiBudgetReservationState::Reserved,
            expires_at,
        })
    }

    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_persisted(
        id: AiBudgetReservationId,
        run_id: AiRunId,
        attempt_id: Uuid,
        lease_generation: i64,
        provider_kind: ProviderKind,
        model: impl Into<String>,
        pricing_policy_version: impl Into<String>,
        reserved: AiBudgetAmounts,
        actual: Option<AiBudgetAmounts>,
        state: AiBudgetReservationState,
        expires_at: OffsetDateTime,
    ) -> Result<Self, AiError> {
        let mut reservation = Self::new_reserved(
            id,
            run_id,
            attempt_id,
            lease_generation,
            provider_kind,
            model,
            pricing_policy_version,
            reserved,
            expires_at,
        )?;
        reservation.actual = actual;
        reservation.state = state;
        Ok(reservation)
    }

    /// Returns the durable reservation identifier.
    pub const fn id(&self) -> AiBudgetReservationId {
        self.id
    }

    /// Returns the exact reserved capacity.
    pub const fn reserved(&self) -> AiBudgetAmounts {
        self.reserved
    }

    /// Returns authoritative usage recorded during reconciliation, when known.
    pub const fn actual(&self) -> Option<AiBudgetAmounts> {
        self.actual
    }

    /// Returns the durable reservation state.
    pub const fn state(&self) -> AiBudgetReservationState {
        self.state
    }

    /// Returns the deadline for starting the bound provider call.
    pub const fn expires_at(&self) -> OffsetDateTime {
        self.expires_at
    }

    /// Returns the immutable pricing-policy version.
    pub fn pricing_policy_version(&self) -> &str {
        &self.pricing_policy_version
    }

    /// Converts a current exact reservation into the proof required by a
    /// provider call.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::BudgetDenied`] when the reservation is not
    /// active, has expired, or does not match the run, provider, model, output
    /// ceiling, attempt, or fencing generation.
    #[allow(clippy::too_many_arguments)]
    pub fn authorize_provider_call(
        &self,
        run_id: AiRunId,
        attempt_id: Uuid,
        lease_generation: i64,
        provider_kind: &ProviderKind,
        model: &str,
        requested_maximum_output_tokens: u64,
        now: OffsetDateTime,
    ) -> Result<AuthorizedBudgetReservation, ProviderError> {
        if self.state != AiBudgetReservationState::Reserved
            || now >= self.expires_at
            || self.run_id != run_id
            || self.attempt_id != attempt_id
            || self.lease_generation != lease_generation
            || &self.provider_kind != provider_kind
            || self.model != model
            || requested_maximum_output_tokens > self.reserved.output_tokens
        {
            return Err(ProviderError::BudgetDenied);
        }
        Ok(AuthorizedBudgetReservation {
            reservation_id: self.id,
            run_id: self.run_id,
            provider_kind: self.provider_kind.clone(),
            model: self.model.clone(),
            maximum_output_tokens: self.reserved.output_tokens,
            expires_at: self.expires_at,
        })
    }
}

/// Opaque proof that capacity was atomically reserved for one exact provider call.
#[derive(Clone, Debug)]
pub struct AuthorizedBudgetReservation {
    reservation_id: AiBudgetReservationId,
    run_id: AiRunId,
    provider_kind: ProviderKind,
    model: String,
    maximum_output_tokens: u64,
    expires_at: OffsetDateTime,
}

impl AuthorizedBudgetReservation {
    /// Returns the reservation identifier for usage/audit linkage.
    pub const fn reservation_id(&self) -> AiBudgetReservationId {
        self.reservation_id
    }

    pub(crate) fn matches(
        &self,
        run_id: AiRunId,
        provider_kind: &ProviderKind,
        model: &str,
        requested_maximum_output_tokens: u64,
        now: OffsetDateTime,
    ) -> bool {
        self.run_id == run_id
            && &self.provider_kind == provider_kind
            && self.model == model
            && requested_maximum_output_tokens <= self.maximum_output_tokens
            && now < self.expires_at
    }
}

/// Final provider-call classification used for transactional reconciliation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiBudgetReconciliationOutcome {
    /// Actual usage is authoritative and unused capacity may be released.
    Commit,
    /// The provider was provably not called and the full reservation may be released.
    ReleaseUnused,
    /// External execution may have occurred; reserved capacity must remain held.
    MarkUncertain,
}

/// Exact once-only budget reconciliation request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiBudgetReconciliation {
    /// Reservation being reconciled.
    pub reservation_id: AiBudgetReservationId,
    /// Current attempt identifier.
    pub attempt_id: Uuid,
    /// Current fencing generation.
    pub lease_generation: i64,
    /// Authoritative actual usage when known.
    pub actual: Option<AiBudgetAmounts>,
    /// Provider-reported cached input tokens. Required for committed usage,
    /// absent for an unused release, and optional while an outcome is uncertain.
    pub cached_input_tokens: Option<u64>,
    /// Safe final classification.
    pub outcome: AiBudgetReconciliationOutcome,
}

/// Durable result of an idempotent budget reconciliation.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct AiBudgetReconciliationResult {
    /// Reservation after the transaction committed.
    pub reservation: AiBudgetReservation,
    /// Capacity moved from reserved to committed counters.
    pub committed: AiBudgetAmounts,
    /// Proven unused capacity released from reserved counters.
    pub released: AiBudgetAmounts,
    /// Capacity still held because external execution is uncertain.
    pub held: AiBudgetAmounts,
}

/// Transactional budget boundary implemented with `graphql-orm` operations.
#[async_trait]
pub trait AiBudgetService: Send + Sync {
    /// Atomically checks and reserves every applicable budget counter.
    ///
    /// # Errors
    ///
    /// Returns an error when principal freshness, scope/fence binding,
    /// idempotency, configuration, persistence, or any applicable capacity
    /// check fails.
    async fn reserve(
        &self,
        principal: &ResolvedPrincipal,
        request: AiBudgetReservationRequest,
    ) -> Result<AiBudgetReservation, AiError>;

    /// Reconciles actual usage or retains capacity for uncertain recovery.
    ///
    /// Orchestration must durably mark a reservation uncertain immediately
    /// before handing its already-authorized proof to provider transport. This
    /// prevents an unobserved/ambiguous call from later taking the ordinary
    /// unused-release path.
    ///
    /// # Errors
    ///
    /// Returns an error for stale principal/fence state, a conflicting replay,
    /// malformed usage, missing counters, or persistence failure.
    async fn reconcile(
        &self,
        principal: &ResolvedPrincipal,
        reconciliation: AiBudgetReconciliation,
    ) -> Result<AiBudgetReconciliationResult, AiError>;
}
