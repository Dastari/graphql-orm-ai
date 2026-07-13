//! ORM-backed protected proposal staging and human review.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::sync::Arc;

use agql_auth::{AuthPrincipal, Clock, CurrentPrincipalResolver, PrincipalReference};
use async_trait::async_trait;
use graphql_orm::db::Database;
use graphql_orm::graphql::errors::{OrmErrorCode, OrmPublicError};
use graphql_orm::graphql::filters::UuidFilter;
use graphql_orm::graphql::orm::{
    ConditionalUpdateOutcome, DefaultWriteBackend, TransactionError, TransactionMode,
};
use graphql_orm::graphql::pagination::{
    KeysetConnectionInput, KeysetWindowDirection, ValidatedKeysetConnection,
};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::orm_runs::PreparedProposal;
use crate::persistence::*;
use crate::{
    AiContentProtectionPolicy, AiContentProtectionPolicyResolver, AiContentProtector, AiError,
    AiProposalAccessPolicy, AiProposalAction, AiProposalAppliedOutcome, AiProposalCatalog,
    AiProposalConnection, AiProposalDraft, AiProposalEdge, AiProposalId, AiProposalOutcomeRecorder,
    AiProposalReviewDecision, AiProposalService, AiProposalTypeId, AiProposalView, AiRunLease,
    AiScope, AiSessionId, AiSessionWakeup, ContentProtectionContext, OrmAiRunService,
    ProtectedContentEnvelope, ReviewAiProposalInput, ValidatedAiProposal,
};

/// Deployment-owned hard limits for proposal persistence and reauthorization.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiProposalServiceLimits {
    maximum_principal_age: Duration,
    maximum_lifetime: Duration,
    maximum_source_references: usize,
}

impl AiProposalServiceLimits {
    /// Creates validated proposal-service bounds.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless principal age and
    /// proposal lifetime are positive and source references are within
    /// `1..=10_000`.
    pub fn new(
        maximum_principal_age: Duration,
        maximum_lifetime: Duration,
        maximum_source_references: usize,
    ) -> Result<Self, AiError> {
        if !maximum_principal_age.is_positive()
            || !maximum_lifetime.is_positive()
            || !(1..=10_000).contains(&maximum_source_references)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid proposal-service limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_principal_age,
            maximum_lifetime,
            maximum_source_references,
        })
    }
}

impl Default for AiProposalServiceLimits {
    fn default() -> Self {
        Self {
            maximum_principal_age: Duration::seconds(60),
            maximum_lifetime: Duration::days(90),
            maximum_source_references: 1_000,
        }
    }
}

/// Result of a fenced proposal append.
#[derive(Clone, Debug)]
pub struct AiPersistedProposal {
    proposal_id: AiProposalId,
    lease: AiRunLease,
}

impl AiPersistedProposal {
    /// Persisted proposal identifier.
    pub const fn proposal_id(&self) -> AiProposalId {
        self.proposal_id
    }

    /// Renewed run lease. The caller must discard its previous lease.
    pub fn lease(&self) -> &AiRunLease {
        &self.lease
    }

    /// Consumes the result and returns the renewed lease.
    pub fn into_lease(self) -> AiRunLease {
        self.lease
    }
}

/// Protected proposal staging service using generated ORM APIs only.
#[derive(Clone)]
pub struct OrmAiProposalService {
    database: Database<DefaultWriteBackend>,
    run_service: OrmAiRunService,
    principal_resolver: Arc<dyn CurrentPrincipalResolver>,
    access_policy: Arc<dyn AiProposalAccessPolicy>,
    protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
    content_protector: Arc<dyn AiContentProtector>,
    catalog: Arc<AiProposalCatalog>,
    clock: Arc<dyn Clock>,
    limits: AiProposalServiceLimits,
}

impl OrmAiProposalService {
    /// Creates a protected proposal service.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database: Database<DefaultWriteBackend>,
        run_service: OrmAiRunService,
        principal_resolver: Arc<dyn CurrentPrincipalResolver>,
        access_policy: Arc<dyn AiProposalAccessPolicy>,
        protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
        content_protector: Arc<dyn AiContentProtector>,
        catalog: Arc<AiProposalCatalog>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            database,
            run_service,
            principal_resolver,
            access_policy,
            protection_policy,
            content_protector,
            catalog,
            clock,
            limits: AiProposalServiceLimits::default(),
        }
    }

    /// Overrides deployment-owned hard limits.
    #[must_use]
    pub fn with_limits(mut self, limits: AiProposalServiceLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Returns the ORM database handle for host schema wiring.
    pub fn database(&self) -> &Database<DefaultWriteBackend> {
        &self.database
    }

    /// Protects and durably appends one schema/provenance-validated proposal
    /// through the exact current run fence.
    ///
    /// This method stages AI-owned data only. It never invokes an application
    /// mutation or treats proposal acceptance as domain authorization.
    ///
    /// # Errors
    ///
    /// Fails closed for stale principals/fences, session/scope/run mismatch,
    /// invalid provenance or expiry, unavailable protection, denied access,
    /// or persistence ambiguity.
    pub async fn persist_validated(
        &self,
        lease: &AiRunLease,
        proposal: ValidatedAiProposal,
        expires_at: Option<OffsetDateTime>,
    ) -> Result<AiPersistedProposal, AiError> {
        if proposal.draft.session_id != lease.session_id()
            || proposal.draft.run_id != lease.run_id()
            || proposal.descriptor.id != proposal.draft.proposal_type
        {
            return Err(AiError::Conflict);
        }
        validate_sources(
            &proposal.draft.sources,
            self.limits.maximum_source_references,
        )?;
        let now = canonical_second(self.clock.now());
        let expires_at = validate_expiry(expires_at, now, self.limits.maximum_lifetime)?;
        let resolved = self
            .resolve_current(lease.principal_reference(), now)
            .await?;
        if !self
            .access_policy
            .can_access_proposal(
                resolved.principal(),
                &proposal.draft.scope,
                proposal.draft.session_id,
                AiProposalAction::Create,
            )
            .await
        {
            return Err(AiError::Forbidden);
        }
        let protection = self
            .protection_policy(resolved.principal(), &proposal.draft.scope)
            .await?;
        let protected_payload = self
            .protect_value(
                &protection,
                content_context(
                    "graphql_orm_ai_proposals",
                    proposal.id.0,
                    "protected_payload",
                    &proposal.draft.scope,
                ),
                proposal.draft.payload,
            )
            .await?;
        let sources = serde_json::to_value(&proposal.draft.sources)
            .map_err(|_| AiError::PersistenceFailed)?;
        let event_id = Uuid::new_v4();
        let protected_event = self
            .protect_value(
                &protection,
                content_context(
                    "graphql_orm_ai_session_events",
                    event_id,
                    "protected_payload",
                    &proposal.draft.scope,
                ),
                serde_json::json!({
                    "proposalId": proposal.id.0,
                    "proposalType": proposal.descriptor.id.as_str(),
                    "state": "pending_review"
                }),
            )
            .await?;
        let (owner_kind, owner_subject) = principal_identity(resolved.principal());
        let prepared = PreparedProposal {
            id: proposal.id.0,
            proposal_type: proposal.descriptor.id.as_str().to_owned(),
            schema_version: proposal.descriptor.schema_version,
            item_count: i64::from(proposal.draft.item_count),
            protected_payload,
            source_references: sources,
            created_by_subject: resolved.principal().subject().to_owned(),
            expires_at: expires_at.map(OffsetDateTime::unix_timestamp),
            event_id,
            protected_event,
            correlation_id: lease.run_id().0.to_string(),
            expected_owner_principal_kind: owner_kind,
            expected_owner_subject: owner_subject.to_owned(),
            expected_scope_kind: proposal.draft.scope.kind,
            expected_scope_id: proposal.draft.scope.id,
            expected_tenant_id: proposal.draft.scope.tenant_id,
        };
        let lease = self.run_service.append_proposal(lease, prepared).await?;
        Ok(AiPersistedProposal {
            proposal_id: proposal.id,
            lease,
        })
    }

    async fn resolve_current(
        &self,
        reference: &PrincipalReference,
        now: OffsetDateTime,
    ) -> Result<agql_auth::ResolvedPrincipal, AiError> {
        let resolved = self
            .principal_resolver
            .resolve(reference)
            .await
            .map_err(|_| AiError::ReauthorizationFailed)?;
        if resolved.resolved_at() > now
            || now - resolved.resolved_at() > self.limits.maximum_principal_age
            || resolved.reference() != reference
        {
            return Err(AiError::ReauthorizationFailed);
        }
        Ok(resolved)
    }

    async fn current_request_principal(
        &self,
        principal: &AuthPrincipal,
    ) -> Result<AuthPrincipal, AiError> {
        Ok(self
            .resolve_current(&principal.reference(), canonical_second(self.clock.now()))
            .await?
            .into_principal())
    }

    async fn protection_policy(
        &self,
        principal: &AuthPrincipal,
        scope: &AiScope,
    ) -> Result<AiContentProtectionPolicy, AiError> {
        let policy = self.protection_policy.resolve(principal, scope).await?;
        if !policy.ready || policy.scope != *scope {
            return Err(AiError::RuntimeNotReady);
        }
        Ok(policy)
    }

    async fn protect_value(
        &self,
        policy: &AiContentProtectionPolicy,
        context: ContentProtectionContext,
        value: serde_json::Value,
    ) -> Result<serde_json::Value, AiError> {
        let envelope = self
            .content_protector
            .protect(policy, &context, value)
            .await
            .map_err(map_protection)?;
        serde_json::to_value(envelope).map_err(|_| AiError::PersistenceFailed)
    }

    async fn open_value(
        &self,
        policy: &AiContentProtectionPolicy,
        context: ContentProtectionContext,
        value: &serde_json::Value,
    ) -> Result<serde_json::Value, AiError> {
        let envelope: ProtectedContentEnvelope =
            serde_json::from_value(value.clone()).map_err(|_| AiError::PersistenceFailed)?;
        self.content_protector
            .open(policy, &context, &envelope)
            .await
            .map_err(map_protection)
    }

    async fn visible_context(
        &self,
        principal: &AuthPrincipal,
        proposal: &AiProposalRecord,
        action: AiProposalAction,
    ) -> Result<(AiScope, AiContentProtectionPolicy), AiError> {
        let session = AiSessionRecord::find_by_id(&self.database, &proposal.session_id)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
            .ok_or(AiError::NotFound)?;
        if session.state == "deleting"
            || session.deleted_at.is_some()
            || session.scope_kind != proposal.scope_kind
            || session.scope_id != proposal.scope_id
        {
            return Err(AiError::NotFound);
        }
        let scope = record_scope(&session);
        if !self
            .access_policy
            .can_access_proposal(principal, &scope, AiSessionId(session.id), action)
            .await
        {
            return Err(AiError::NotFound);
        }
        let policy = self.protection_policy(principal, &scope).await?;
        Ok((scope, policy))
    }

    async fn view(
        &self,
        principal: &AuthPrincipal,
        record: &AiProposalRecord,
    ) -> Result<AiProposalView, AiError> {
        let (scope, policy) = self
            .visible_context(principal, record, AiProposalAction::Read)
            .await?;
        let payload = self
            .open_value(
                &policy,
                content_context(
                    "graphql_orm_ai_proposals",
                    record.id,
                    "protected_payload",
                    &scope,
                ),
                &record.protected_payload,
            )
            .await?;
        let state = if record.state == "pending_review"
            && record
                .expires_at
                .is_some_and(|expiry| expiry <= self.clock.now().unix_timestamp())
        {
            "expired".to_owned()
        } else {
            record.state.clone()
        };
        Ok(AiProposalView {
            id: record.id,
            session_id: record.session_id,
            run_id: record.run_id,
            scope_kind: record.scope_kind.clone(),
            scope_id: record.scope_id.clone(),
            proposal_type: record.proposal_type.clone(),
            schema_version: record.schema_version.clone(),
            payload: async_graphql::Json(payload),
            sources: async_graphql::Json(record.source_references.clone()),
            item_count: record.item_count,
            state,
            created_by_subject: record.created_by_subject.clone(),
            reviewed_by_subject: record.reviewed_by_subject.clone(),
            applied_resource_ref: record.applied_resource_ref.clone(),
            application_audit_ref: record.application_audit_ref.clone(),
            created_at: record.created_at,
            reviewed_at: record.reviewed_at,
            expires_at: record.expires_at,
            row_version: record.row_version,
        })
    }
}

#[async_trait]
impl AiProposalService for OrmAiProposalService {
    async fn proposals(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
        page: ValidatedKeysetConnection,
    ) -> Result<AiProposalConnection, AiError> {
        let principal = self.current_request_principal(principal).await?;
        let session = AiSessionRecord::find_by_id(&self.database, &session_id.0)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
            .ok_or(AiError::NotFound)?;
        if session.state == "deleting" || session.deleted_at.is_some() {
            return Err(AiError::NotFound);
        }
        let scope = record_scope(&session);
        if !self
            .access_policy
            .can_access_proposal(&principal, &scope, session_id, AiProposalAction::Read)
            .await
        {
            return Err(AiError::NotFound);
        }
        let connection = AiProposalRecord::keyset_connection_page(
            &self.database,
            AiProposalRecordWhereInput {
                session_id: Some(UuidFilter {
                    eq: Some(session_id.0),
                    ..Default::default()
                }),
                ..Default::default()
            },
            page_input(&page),
        )
        .await
        .map_err(map_orm)?;
        let mut edges = Vec::with_capacity(connection.edges.len());
        for edge in connection.edges {
            edges.push(AiProposalEdge {
                node: self.view(&principal, &edge.node).await?,
                cursor: edge.cursor,
            });
        }
        let mut page_info = connection.page_info;
        page_info.total_count = None;
        Ok(AiProposalConnection { edges, page_info })
    }

    async fn proposal(
        &self,
        principal: &AuthPrincipal,
        proposal_id: AiProposalId,
    ) -> Result<Option<AiProposalView>, AiError> {
        let principal = self.current_request_principal(principal).await?;
        let Some(record) = AiProposalRecord::find_by_id(&self.database, &proposal_id.0)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
        else {
            return Ok(None);
        };
        match self.view(&principal, &record).await {
            Ok(view) => Ok(Some(view)),
            Err(AiError::NotFound | AiError::Forbidden) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn review_proposal(
        &self,
        principal: &AuthPrincipal,
        input: ReviewAiProposalInput,
    ) -> Result<AiProposalView, AiError> {
        if input.expected_version < 0 {
            return Err(AiError::InvalidInput(
                "invalid proposal review version".to_owned(),
            ));
        }
        let principal = self.current_request_principal(principal).await?;
        let current = AiProposalRecord::find_by_id(&self.database, &input.id)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
            .ok_or(AiError::NotFound)?;
        let (scope, policy) = self
            .visible_context(&principal, &current, AiProposalAction::Review)
            .await?;
        let now = canonical_second(self.clock.now());
        if current.row_version != input.expected_version
            || current.state != "pending_review"
            || current
                .expires_at
                .is_some_and(|expiry| expiry <= now.unix_timestamp())
        {
            return Err(AiError::Conflict);
        }
        let (state, replacement_payload, replacement_item_count) = match input.decision {
            AiProposalReviewDecision::Accept => {
                if input.edited_payload.is_some() || input.edited_item_count.is_some() {
                    return Err(AiError::InvalidInput(
                        "unedited proposal acceptance cannot include edits".to_owned(),
                    ));
                }
                ("accepted", None, None)
            }
            AiProposalReviewDecision::AcceptEdited => {
                let payload = input.edited_payload.ok_or_else(|| {
                    AiError::InvalidInput("edited proposal payload is required".to_owned())
                })?;
                let item_count = input.edited_item_count.ok_or_else(|| {
                    AiError::InvalidInput("edited proposal item count is required".to_owned())
                })?;
                let item_count = u32::try_from(item_count).map_err(|_| {
                    AiError::InvalidInput("edited proposal item count is invalid".to_owned())
                })?;
                let proposal_type = AiProposalTypeId::parse(current.proposal_type.clone())?;
                let sources = serde_json::from_value(current.source_references.clone())
                    .map_err(|_| AiError::PersistenceFailed)?;
                let validated = self.catalog.validate(AiProposalDraft {
                    proposal_type,
                    session_id: AiSessionId(current.session_id),
                    run_id: crate::AiRunId(current.run_id),
                    scope: scope.clone(),
                    payload: payload.0.clone(),
                    sources,
                    item_count,
                })?;
                if validated.descriptor.schema_version != current.schema_version {
                    return Err(AiError::Conflict);
                }
                let protected = self
                    .protect_value(
                        &policy,
                        content_context(
                            "graphql_orm_ai_proposals",
                            current.id,
                            "protected_payload",
                            &scope,
                        ),
                        payload.0,
                    )
                    .await?;
                (
                    "accepted_edited",
                    Some(protected),
                    Some(i64::from(item_count)),
                )
            }
            AiProposalReviewDecision::Reject => {
                if input.edited_payload.is_some() || input.edited_item_count.is_some() {
                    return Err(AiError::InvalidInput(
                        "rejected proposal cannot include edits".to_owned(),
                    ));
                }
                ("rejected", None, None)
            }
        };
        let event_id = Uuid::new_v4();
        let protected_event = self
            .protect_value(
                &policy,
                content_context(
                    "graphql_orm_ai_session_events",
                    event_id,
                    "protected_payload",
                    &scope,
                ),
                serde_json::json!({"proposalId": current.id, "state": state}),
            )
            .await?;
        let proposal_id = current.id;
        let expected_version = input.expected_version;
        let reviewer = principal.subject().to_owned();
        let updated = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = tx
                        .find_by_id::<AiProposalRecord>(&proposal_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if current.row_version != expected_version
                        || current.state != "pending_review"
                        || current
                            .expires_at
                            .is_some_and(|expiry| expiry <= now.unix_timestamp())
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&current.session_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if session.state == "deleting" || session.deleted_at.is_some() {
                        return Err(OrmPublicError::not_found());
                    }
                    let event_sequence = session
                        .stream_head
                        .checked_add(1)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let session_update = tx
                        .compare_and_swap::<AiSessionRecord>(
                            &session.id,
                            session.row_version,
                            AiSessionRecordWhereInput::default(),
                            UpdateAiSessionRecordInput {
                                stream_head: Some(event_sequence),
                                last_activity_at: Some(now.unix_timestamp()),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(session_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let proposal_update = tx
                        .compare_and_swap::<AiProposalRecord>(
                            &current.id,
                            current.row_version,
                            AiProposalRecordWhereInput::default(),
                            UpdateAiProposalRecordInput {
                                protected_payload: replacement_payload,
                                item_count: replacement_item_count,
                                state: Some(state.to_owned()),
                                reviewed_by_subject: Some(Some(reviewer)),
                                reviewed_at: Some(Some(now.unix_timestamp())),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    let updated = match proposal_update {
                        ConditionalUpdateOutcome::Updated(updated) => updated,
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    tx.insert::<AiSessionEventRecord>(CreateAiSessionEventRecordInput {
                        id: event_id,
                        session_id: session.id,
                        sequence: event_sequence,
                        event_type: "proposal_reviewed".to_owned(),
                        run_id: Some(current.run_id),
                        causation_id: Some(proposal_id.to_string()),
                        correlation_id: proposal_id.to_string(),
                        protected_payload: protected_event,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.queue_event(AiSessionWakeup {
                        session_id: session.id,
                        sequence: event_sequence,
                    });
                    Ok(updated)
                })
            })
            .await
            .map_err(map_transaction)?;
        self.view(&principal, &updated).await
    }
}

#[async_trait]
impl AiProposalOutcomeRecorder for OrmAiProposalService {
    async fn record_applied_outcome(
        &self,
        principal: &AuthPrincipal,
        outcome: AiProposalAppliedOutcome,
    ) -> Result<(), AiError> {
        validate_outcome(&outcome)?;
        let principal = self.current_request_principal(principal).await?;
        if principal.subject() != outcome.applied_by_subject {
            return Err(AiError::Forbidden);
        }
        let current = AiProposalRecord::find_by_id(&self.database, &outcome.proposal_id.0)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
            .ok_or(AiError::NotFound)?;
        let (scope, policy) = self
            .visible_context(&principal, &current, AiProposalAction::RecordAppliedOutcome)
            .await?;
        let resource_ref = format!("{}:{}", outcome.resource_type, outcome.resource_id);
        if current.state == "applied" {
            if current.applied_resource_ref.as_deref() == Some(resource_ref.as_str())
                && current.application_audit_ref.as_deref()
                    == Some(outcome.application_audit_ref.as_str())
            {
                return Ok(());
            }
            return Err(AiError::Conflict);
        }
        if !matches!(current.state.as_str(), "accepted" | "accepted_edited") {
            return Err(AiError::Conflict);
        }
        let now = canonical_second(self.clock.now());
        let event_id = Uuid::new_v4();
        let protected_event = self
            .protect_value(
                &policy,
                content_context(
                    "graphql_orm_ai_session_events",
                    event_id,
                    "protected_payload",
                    &scope,
                ),
                serde_json::json!({
                    "proposalId": current.id,
                    "state": "applied",
                    "resourceType": outcome.resource_type,
                    "resourceId": outcome.resource_id,
                    "applicationAuditRef": outcome.application_audit_ref
                }),
            )
            .await?;
        let expected_version = current.row_version;
        let proposal_id = current.id;
        let audit_ref = outcome.application_audit_ref;
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = tx
                        .find_by_id::<AiProposalRecord>(&proposal_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if current.row_version != expected_version
                        || !matches!(current.state.as_str(), "accepted" | "accepted_edited")
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&current.session_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    let event_sequence = session
                        .stream_head
                        .checked_add(1)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let session_update = tx
                        .compare_and_swap::<AiSessionRecord>(
                            &session.id,
                            session.row_version,
                            AiSessionRecordWhereInput::default(),
                            UpdateAiSessionRecordInput {
                                stream_head: Some(event_sequence),
                                last_activity_at: Some(now.unix_timestamp()),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(session_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let update = tx
                        .compare_and_swap::<AiProposalRecord>(
                            &current.id,
                            current.row_version,
                            AiProposalRecordWhereInput::default(),
                            UpdateAiProposalRecordInput {
                                state: Some("applied".to_owned()),
                                applied_resource_ref: Some(Some(resource_ref)),
                                application_audit_ref: Some(Some(audit_ref)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    tx.insert::<AiSessionEventRecord>(CreateAiSessionEventRecordInput {
                        id: event_id,
                        session_id: session.id,
                        sequence: event_sequence,
                        event_type: "proposal_applied".to_owned(),
                        run_id: Some(current.run_id),
                        causation_id: Some(proposal_id.to_string()),
                        correlation_id: proposal_id.to_string(),
                        protected_payload: protected_event,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.queue_event(AiSessionWakeup {
                        session_id: session.id,
                        sequence: event_sequence,
                    });
                    Ok(())
                })
            })
            .await
            .map_err(map_transaction)
    }
}

fn validate_sources(
    sources: &[crate::AiDataSourceRef],
    maximum_source_references: usize,
) -> Result<(), AiError> {
    if sources.len() > maximum_source_references
        || sources.iter().any(|source| {
            source.kind.is_empty()
                || source.kind.len() > 200
                || source.reference.is_empty()
                || source.reference.len() > 1_024
                || source.classification == crate::DataClassification::Secret
        })
    {
        return Err(AiError::InvalidInput(
            "proposal provenance is invalid".to_owned(),
        ));
    }
    let mut unique = sources.to_vec();
    unique.sort();
    unique.dedup();
    if unique.len() != sources.len() {
        return Err(AiError::InvalidInput(
            "proposal provenance contains duplicates".to_owned(),
        ));
    }
    Ok(())
}

fn validate_expiry(
    expires_at: Option<OffsetDateTime>,
    now: OffsetDateTime,
    maximum_lifetime: Duration,
) -> Result<Option<OffsetDateTime>, AiError> {
    if let Some(expires_at) = expires_at
        && (expires_at <= now || expires_at - now > maximum_lifetime)
    {
        return Err(AiError::InvalidInput(
            "proposal expiry is invalid".to_owned(),
        ));
    }
    Ok(expires_at.map(canonical_second))
}

fn validate_outcome(outcome: &AiProposalAppliedOutcome) -> Result<(), AiError> {
    if outcome.resource_type.trim().is_empty()
        || outcome.resource_type.len() > 200
        || outcome.resource_id.trim().is_empty()
        || outcome.resource_id.len() > 1_024
        || outcome.application_audit_ref.trim().is_empty()
        || outcome.application_audit_ref.len() > 1_024
        || outcome.applied_by_subject.trim().is_empty()
        || outcome.applied_by_subject.len() > 512
        || outcome.application_audit_ref.chars().any(char::is_control)
    {
        return Err(AiError::InvalidInput(
            "applied proposal outcome is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn page_input(page: &ValidatedKeysetConnection) -> KeysetConnectionInput {
    match page.direction {
        KeysetWindowDirection::Forward => KeysetConnectionInput {
            after: page.cursor.clone(),
            first: Some(page.limit),
            ..Default::default()
        },
        KeysetWindowDirection::Backward => KeysetConnectionInput {
            before: page.cursor.clone(),
            last: Some(page.limit),
            ..Default::default()
        },
    }
}

fn principal_identity(principal: &AuthPrincipal) -> (String, &str) {
    let kind = match principal {
        AuthPrincipal::User(_) => "user".to_owned(),
        AuthPrincipal::ApiToken(token) => format!("api_token:{}", token.principal_kind.as_str()),
    };
    (kind, principal.subject())
}

fn record_scope(session: &AiSessionRecord) -> AiScope {
    AiScope {
        kind: session.scope_kind.clone(),
        id: session.scope_id.clone(),
        tenant_id: session.tenant_id.clone(),
    }
}

fn content_context(
    entity: &str,
    row_id: Uuid,
    field: &str,
    scope: &AiScope,
) -> ContentProtectionContext {
    ContentProtectionContext {
        entity: entity.to_owned(),
        row_id: row_id.to_string(),
        field: field.to_owned(),
        scope: scope.clone(),
    }
}

fn canonical_second(value: OffsetDateTime) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(value.unix_timestamp())
        .expect("an existing OffsetDateTime timestamp remains representable")
}

fn map_protection(error: crate::ContentProtectionError) -> AiError {
    match error {
        crate::ContentProtectionError::PolicyNotReady => AiError::RuntimeNotReady,
        _ => AiError::PersistenceFailed,
    }
}

fn map_transaction(error: TransactionError) -> AiError {
    map_orm(error.public_error().clone())
}

fn map_orm(error: OrmPublicError) -> AiError {
    match error.code {
        OrmErrorCode::InvalidInput
        | OrmErrorCode::CursorInvalid
        | OrmErrorCode::PageLimitExceeded => AiError::InvalidInput(error.message),
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
    use agql_auth::{AccessTokenMetadata, AuthUser, FixedClock, ResolvedPrincipal, SessionContext};
    use graphql_orm::graphql::orm::{ApplyOptions, OrmSchemaModule};
    use graphql_orm::graphql::pagination::KeysetConnectionInput;
    use graphql_orm::prelude::{Database, SqliteBackend};

    use crate::{
        AiDataSourceRef, AiProposalTypeDescriptor, AiRunId, AiRunServiceLimits, AiSourceTrust,
        DataClassification,
    };

    #[derive(Clone)]
    struct Resolver {
        principal: AuthPrincipal,
        now: OffsetDateTime,
    }

    #[async_trait]
    impl CurrentPrincipalResolver for Resolver {
        async fn resolve(
            &self,
            reference: &PrincipalReference,
        ) -> agql_auth::AuthResult<ResolvedPrincipal> {
            ResolvedPrincipal::new(reference.clone(), self.principal.clone(), self.now)
        }
    }

    struct AllowProposals;

    #[async_trait]
    impl AiProposalAccessPolicy for AllowProposals {
        async fn can_access_proposal(
            &self,
            _principal: &AuthPrincipal,
            _scope: &AiScope,
            _session_id: AiSessionId,
            _action: AiProposalAction,
        ) -> bool {
            true
        }
    }

    struct Protection(AiScope);

    #[async_trait]
    impl AiContentProtectionPolicyResolver for Protection {
        async fn resolve(
            &self,
            _principal: &AuthPrincipal,
            scope: &AiScope,
        ) -> Result<AiContentProtectionPolicy, AiError> {
            if scope != &self.0 {
                return Err(AiError::Forbidden);
            }
            Ok(AiContentProtectionPolicy {
                scope: scope.clone(),
                mode: crate::AiContentProtectionMode::DatabaseManaged,
                key_policy_reference: None,
                version: 1,
                ready: true,
            })
        }
    }

    struct Fixture {
        database: Database<SqliteBackend>,
        run_service: OrmAiRunService,
        proposal_service: OrmAiProposalService,
        catalog: Arc<AiProposalCatalog>,
        principal: AuthPrincipal,
        scope: AiScope,
        now: OffsetDateTime,
    }

    async fn fixture() -> Fixture {
        let database = Database::<SqliteBackend>::connect_sqlite("sqlite::memory:")
            .await
            .expect("in-memory SQLite should open");
        let module = crate::AiSchemaModule;
        let plan = database
            .schema()
            .plan_migration_to_entities(
                "ai-proposal-test-v1",
                "AI proposal lifecycle test",
                module.entities(),
            )
            .await
            .expect("AI proposal schema should plan");
        database
            .schema()
            .apply_migration(&plan, ApplyOptions::default())
            .await
            .expect("AI proposal schema should apply to in-memory SQLite");
        let now = OffsetDateTime::from_unix_timestamp(1_800_000_000)
            .expect("fixed test timestamp should be valid");
        let principal = AuthPrincipal::User(AuthUser {
            user_id: "proposal-user".to_owned(),
            session_id: Uuid::new_v4(),
            roles: vec![],
            scopes: vec![],
            session: SessionContext::default(),
            token_claims: AccessTokenMetadata {
                tenant_id: Some("tenant-proposal".to_owned()),
                ..AccessTokenMetadata::default()
            },
        });
        let scope = AiScope::new("tenant", "tenant-proposal").with_tenant_id("tenant-proposal");
        let run_limits = AiRunServiceLimits::new(Duration::hours(1), Duration::hours(1), 16, 2, 8)
            .expect("test run limits should validate");
        let clock = Arc::new(FixedClock::new(now));
        let run_service = OrmAiRunService::new(database.clone(), clock.clone(), run_limits);
        let schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {"summary": {"type": "string"}},
            "required": ["summary"],
            "additionalProperties": false
        });
        let descriptor = AiProposalTypeDescriptor::new("application.summary.v1", "1", schema)
            .expect("proposal descriptor should validate")
            .with_required_source_kinds(vec!["record".to_owned()]);
        let mut catalog = AiProposalCatalog::new();
        catalog
            .register(descriptor)
            .expect("proposal descriptor should register");
        let catalog = Arc::new(catalog);
        let proposal_service = OrmAiProposalService::new(
            database.clone(),
            run_service.clone(),
            Arc::new(Resolver {
                principal: principal.clone(),
                now,
            }),
            Arc::new(AllowProposals),
            Arc::new(Protection(scope.clone())),
            Arc::new(crate::DatabaseManagedContentProtector),
            catalog.clone(),
            clock,
        );
        Fixture {
            database,
            run_service,
            proposal_service,
            catalog,
            principal,
            scope,
            now,
        }
    }

    async fn seed_running(fixture: &Fixture) -> AiRunLease {
        let session_id = AiSessionId::new();
        let run_id = AiRunId::new();
        AiSessionRecord::insert(
            &fixture.database,
            CreateAiSessionRecordInput {
                id: session_id.0,
                owner_principal_kind: "user".to_owned(),
                owner_subject: fixture.principal.subject().to_owned(),
                tenant_id: fixture.scope.tenant_id.clone(),
                scope_kind: fixture.scope.kind.clone(),
                scope_id: fixture.scope.id.clone(),
                title: "Proposal test".to_owned(),
                state: "active".to_owned(),
                stream_head: 0,
                message_head: 0,
                last_activity_at: fixture.now.unix_timestamp(),
                archived_at: None,
                deleted_at: None,
            },
        )
        .await
        .expect("test session should seed");
        AiRunRecord::insert(
            &fixture.database,
            CreateAiRunRecordInput {
                id: run_id.0,
                session_id: session_id.0,
                input_message_id: Uuid::new_v4(),
                principal_reference: serde_json::to_value(fixture.principal.reference())
                    .expect("principal reference should serialize"),
                state: "queued".to_owned(),
                attempt_id: None,
                lease_owner: None,
                lease_generation: 0,
                lease_expires_at: None,
                lease_heartbeat_at: None,
                retry_count: 0,
                next_attempt_at: Some(fixture.now.unix_timestamp()),
                error_code: None,
                latest_checkpoint_id: None,
            },
        )
        .await
        .expect("test run should seed");
        let lease = fixture
            .run_service
            .claim_next("proposal-worker")
            .await
            .expect("claim should succeed")
            .expect("test run should be claimable");
        fixture
            .run_service
            .start(&lease)
            .await
            .expect("test run should start")
    }

    #[tokio::test]
    async fn proposal_is_fenced_protected_reviewed_and_linked_after_domain_commit() {
        let fixture = fixture().await;
        let running = seed_running(&fixture).await;
        let source = AiDataSourceRef {
            kind: "record".to_owned(),
            reference: "record-54".to_owned(),
            classification: DataClassification::Confidential,
            trust: AiSourceTrust::ResolverResult,
        };
        let proposal_type =
            AiProposalTypeId::parse("application.summary.v1").expect("proposal type should parse");
        let validated = fixture
            .catalog
            .validate(AiProposalDraft {
                proposal_type,
                session_id: running.session_id(),
                run_id: running.run_id(),
                scope: fixture.scope.clone(),
                payload: serde_json::json!({"summary": "Initial suggestion"}),
                sources: vec![source],
                item_count: 1,
            })
            .expect("proposal should validate");
        let persisted = fixture
            .proposal_service
            .persist_validated(&running, validated, Some(fixture.now + Duration::days(7)))
            .await
            .expect("proposal should persist through the current fence");
        assert!(matches!(
            fixture.run_service.heartbeat(&running).await,
            Err(AiError::Conflict)
        ));

        let view = fixture
            .proposal_service
            .proposal(&fixture.principal, persisted.proposal_id())
            .await
            .expect("proposal read should succeed")
            .expect("proposal should be visible");
        assert_eq!(view.state, "pending_review");
        assert_eq!(view.payload.0["summary"], "Initial suggestion");
        let reviewed = fixture
            .proposal_service
            .review_proposal(
                &fixture.principal,
                ReviewAiProposalInput {
                    id: view.id,
                    decision: AiProposalReviewDecision::AcceptEdited,
                    edited_payload: Some(async_graphql::Json(
                        serde_json::json!({"summary": "Human edited suggestion"}),
                    )),
                    edited_item_count: Some(1),
                    expected_version: view.row_version,
                },
            )
            .await
            .expect("schema-valid human edit should be accepted");
        assert_eq!(reviewed.state, "accepted_edited");
        assert_eq!(reviewed.payload.0["summary"], "Human edited suggestion");
        assert!(matches!(
            fixture
                .proposal_service
                .review_proposal(
                    &fixture.principal,
                    ReviewAiProposalInput {
                        id: view.id,
                        decision: AiProposalReviewDecision::Reject,
                        edited_payload: None,
                        edited_item_count: None,
                        expected_version: view.row_version,
                    },
                )
                .await,
            Err(AiError::Conflict)
        ));
        fixture
            .proposal_service
            .record_applied_outcome(
                &fixture.principal,
                AiProposalAppliedOutcome {
                    proposal_id: persisted.proposal_id(),
                    resource_type: "record".to_owned(),
                    resource_id: "resource-54".to_owned(),
                    application_audit_ref: "audit-ordinary-mutation-1".to_owned(),
                    applied_by_subject: fixture.principal.subject().to_owned(),
                },
            )
            .await
            .expect("already-committed ordinary mutation should link");
        let applied = fixture
            .proposal_service
            .proposal(&fixture.principal, persisted.proposal_id())
            .await
            .expect("applied proposal read should succeed")
            .expect("applied proposal should remain visible");
        assert_eq!(applied.state, "applied");
        assert_eq!(
            applied.application_audit_ref.as_deref(),
            Some("audit-ordinary-mutation-1")
        );

        let page = KeysetConnectionInput {
            first: Some(10),
            ..Default::default()
        }
        .validate(10, 100)
        .expect("proposal page should validate");
        let connection = fixture
            .proposal_service
            .proposals(&fixture.principal, running.session_id(), page)
            .await
            .expect("proposal connection should load");
        assert_eq!(connection.edges.len(), 1);
    }
}
