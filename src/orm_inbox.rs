//! ORM-backed durable per-principal inbox streams.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use agql_auth::{AuthPrincipal, Clock, CurrentPrincipalResolver};
use async_trait::async_trait;
use graphql_orm::db::Database;
use graphql_orm::graphql::errors::{OrmErrorCode, OrmPublicError};
use graphql_orm::graphql::filters::{IntFilter, StringFilter};
use graphql_orm::graphql::orm::{
    ConditionalUpdateOutcome, DefaultWriteBackend, MutationContext, TransactionError,
    TransactionMode,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::broadcast::error::RecvError;
use tokio::time::{Instant, MissedTickBehavior};
use uuid::Uuid;

use crate::orm_configuration::scope_key;
use crate::persistence::*;
use crate::{
    AiAccessPolicy, AiContentProtectionPolicy, AiContentProtectionPolicyResolver,
    AiContentProtector, AiError, AiInboxEventEnvelope, AiInboxEventPage, AiInboxEventStream,
    AiInboxEventView, AiInboxPruningReport, AiInboxPruningService, AiInboxService, AiInboxWakeup,
    AiScope, AiSessionAction, AiSessionId, ContentProtectionContext, ProtectedContentEnvelope,
};

/// Protected inbox event prepared before an atomic application-state commit.
pub(crate) struct PreparedAiInboxEvent {
    pub id: Uuid,
    pub principal_kind: String,
    pub principal_subject: String,
    pub scope: AiScope,
    pub session_id: Uuid,
    pub event_type: String,
    pub protected_payload: Value,
    pub created_at: i64,
}

/// Appends one event and advances its principal stream in the caller's exact
/// ORM transaction.
pub(crate) async fn append_inbox_event(
    tx: &mut MutationContext<'_, DefaultWriteBackend>,
    event: PreparedAiInboxEvent,
) -> Result<i64, OrmPublicError> {
    if !valid_event_type(&event.event_type)
        || event.principal_kind.trim().is_empty()
        || event.principal_kind.len() > 128
        || event.principal_subject.trim().is_empty()
        || event.principal_subject.len() > 512
    {
        return Err(OrmPublicError::new(OrmErrorCode::InvalidInput));
    }
    let stream_id = inbox_stream_id(&event.principal_kind, &event.principal_subject);
    let stream = match tx
        .find_by_id::<AiInboxStreamRecord>(&stream_id)
        .await
        .map_err(OrmPublicError::from)?
    {
        Some(stream) => stream,
        None => tx
            .insert::<AiInboxStreamRecord>(CreateAiInboxStreamRecordInput {
                id: stream_id,
                principal_kind: event.principal_kind.clone(),
                principal_subject: event.principal_subject.clone(),
                stream_head: 0,
                minimum_retained_sequence: 1,
                last_event_at: event.created_at,
            })
            .await
            .map_err(OrmPublicError::from)?,
    };
    if stream.principal_kind != event.principal_kind
        || stream.principal_subject != event.principal_subject
        || stream.stream_head < 0
        || stream.minimum_retained_sequence < 1
        || stream.minimum_retained_sequence > stream.stream_head.saturating_add(1)
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    let sequence = stream
        .stream_head
        .checked_add(1)
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
    let updated = tx
        .compare_and_swap::<AiInboxStreamRecord>(
            &stream.id,
            stream.row_version,
            AiInboxStreamRecordWhereInput {
                principal_kind: Some(StringFilter {
                    eq: Some(event.principal_kind.clone()),
                    ..Default::default()
                }),
                principal_subject: Some(StringFilter {
                    eq: Some(event.principal_subject.clone()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            UpdateAiInboxStreamRecordInput {
                stream_head: Some(sequence),
                last_event_at: Some(event.created_at),
                ..Default::default()
            },
        )
        .await
        .map_err(OrmPublicError::from)?;
    if !matches!(updated, ConditionalUpdateOutcome::Updated(_)) {
        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
    }
    tx.insert::<AiInboxEventRecord>(CreateAiInboxEventRecordInput {
        id: event.id,
        principal_kind: event.principal_kind.clone(),
        principal_subject: event.principal_subject.clone(),
        scope_key: scope_key(&event.scope),
        scope_kind: event.scope.kind,
        scope_id: event.scope.id,
        tenant_id: event.scope.tenant_id,
        sequence,
        session_id: Some(event.session_id),
        event_type: event.event_type,
        protected_payload: event.protected_payload,
    })
    .await
    .map_err(OrmPublicError::from)?;
    tx.queue_event(AiInboxWakeup {
        principal_kind: event.principal_kind,
        principal_subject: event.principal_subject,
        sequence,
    });
    Ok(sequence)
}

/// Deployment-owned hard limits for one inbox retention pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiInboxPruningLimits {
    maximum_streams: usize,
    maximum_events_per_stream: usize,
}

impl AiInboxPruningLimits {
    /// Creates validated scan and delete bounds.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless stream count is in
    /// `1..=256` and per-stream event count is in `1..=5_000`.
    pub fn new(maximum_streams: usize, maximum_events_per_stream: usize) -> Result<Self, AiError> {
        if !(1..=256).contains(&maximum_streams)
            || !(1..=5_000).contains(&maximum_events_per_stream)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid inbox pruning limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_streams,
            maximum_events_per_stream,
        })
    }

    /// Maximum principal streams considered per pass.
    pub const fn maximum_streams(&self) -> usize {
        self.maximum_streams
    }

    /// Maximum event rows inspected/deleted for one stream per pass.
    pub const fn maximum_events_per_stream(&self) -> usize {
        self.maximum_events_per_stream
    }
}

impl Default for AiInboxPruningLimits {
    fn default() -> Self {
        Self {
            maximum_streams: 50,
            maximum_events_per_stream: 500,
        }
    }
}

/// ORM-only host worker for GraphQL-managed principal-inbox retention.
///
/// The worker never opens protected event payloads. Each stream prefix,
/// applicable scope policy, CAS cursor advance, event deletion, and redacted
/// audit fact are evaluated in one state-machine transaction.
pub struct OrmAiInboxPruningService {
    database: Database<DefaultWriteBackend>,
    clock: Arc<dyn Clock>,
    limits: AiInboxPruningLimits,
}

impl OrmAiInboxPruningService {
    /// Creates a bounded pruning worker.
    pub fn new(
        database: Database<DefaultWriteBackend>,
        clock: Arc<dyn Clock>,
        limits: AiInboxPruningLimits,
    ) -> Self {
        Self {
            database,
            clock,
            limits,
        }
    }

    async fn candidates(&self) -> Result<Vec<AiInboxStreamRecord>, AiError> {
        let limit = self.limits.maximum_streams as i64;
        self.database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    tx.query::<AiInboxStreamRecord>()
                        .default_order()
                        .limit(limit)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .map_err(map_transaction)
    }

    async fn prune_stream(
        &self,
        candidate: AiInboxStreamRecord,
        now: i64,
    ) -> Result<PruneStreamOutcome, AiError> {
        let event_limit = self.limits.maximum_events_per_stream as i64;
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = tx
                        .find_by_id::<AiInboxStreamRecord>(&candidate.id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if current.row_version != candidate.row_version {
                        return Ok(PruneStreamOutcome::Conflict);
                    }
                    validate_stream(&current)?;
                    let rows = tx
                        .query::<AiInboxEventRecord>()
                        .filter(AiInboxEventRecordWhereInput {
                            principal_kind: Some(StringFilter {
                                eq: Some(current.principal_kind.clone()),
                                ..Default::default()
                            }),
                            principal_subject: Some(StringFilter {
                                eq: Some(current.principal_subject.clone()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .default_order()
                        .limit(event_limit)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if rows.is_empty() {
                        if current.minimum_retained_sequence
                            != current.stream_head.saturating_add(1)
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                        return Ok(PruneStreamOutcome::Noop);
                    }

                    let mut policies = BTreeMap::<String, AiRetentionPolicyRecord>::new();
                    let mut policy_versions = BTreeSet::new();
                    let mut delete_ids = Vec::new();
                    let mut expected_sequence = current.minimum_retained_sequence;
                    let mut not_ready = false;
                    for row in rows {
                        if row.principal_kind != current.principal_kind
                            || row.principal_subject != current.principal_subject
                            || row.sequence != expected_sequence
                            || row.sequence > current.stream_head
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                        expected_sequence = expected_sequence
                            .checked_add(1)
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                        let captured_scope = AiScope {
                            kind: row.scope_kind.clone(),
                            id: row.scope_id.clone(),
                            tenant_id: row.tenant_id.clone(),
                        };
                        if row.scope_key != scope_key(&captured_scope) {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                        let policy = if let Some(policy) = policies.get(&row.scope_key) {
                            policy.clone()
                        } else {
                            let found = tx
                                .query::<AiRetentionPolicyRecord>()
                                .filter(AiRetentionPolicyRecordWhereInput {
                                    scope_key: Some(StringFilter {
                                        eq: Some(row.scope_key.clone()),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                })
                                .limit(2)
                                .fetch_all()
                                .await
                                .map_err(OrmPublicError::from)?;
                            if found.len() > 1 {
                                return Err(OrmPublicError::new(
                                    OrmErrorCode::AuthorizationMisconfigured,
                                ));
                            }
                            let Some(policy) = found.into_iter().next() else {
                                not_ready = true;
                                break;
                            };
                            if !valid_retention_policy(&policy, &captured_scope, &row.scope_key) {
                                not_ready = true;
                                break;
                            }
                            policies.insert(row.scope_key.clone(), policy.clone());
                            policy
                        };
                        let retention_seconds = policy
                            .inbox_event_retention_seconds
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                        let minimum_events = policy
                            .inbox_minimum_events
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                        let newest_deletable = current.stream_head.saturating_sub(minimum_events);
                        let cutoff = now
                            .checked_sub(retention_seconds)
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                        if row.sequence > newest_deletable || row.created_at > cutoff {
                            break;
                        }
                        policy_versions.insert(format!("{}:{}", policy.id, policy.row_version));
                        delete_ids.push(row.id);
                    }
                    if delete_ids.is_empty() {
                        return Ok(if not_ready {
                            PruneStreamOutcome::NotReady
                        } else {
                            PruneStreamOutcome::Noop
                        });
                    }
                    let deleted = delete_ids.len();
                    let new_minimum = current
                        .minimum_retained_sequence
                        .checked_add(
                            i64::try_from(deleted)
                                .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?,
                        )
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let updated = tx
                        .compare_and_swap::<AiInboxStreamRecord>(
                            &current.id,
                            current.row_version,
                            AiInboxStreamRecordWhereInput {
                                principal_kind: Some(StringFilter {
                                    eq: Some(current.principal_kind.clone()),
                                    ..Default::default()
                                }),
                                principal_subject: Some(StringFilter {
                                    eq: Some(current.principal_subject.clone()),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            },
                            UpdateAiInboxStreamRecordInput {
                                minimum_retained_sequence: Some(new_minimum),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(updated, ConditionalUpdateOutcome::Updated(_)) {
                        return Ok(PruneStreamOutcome::Conflict);
                    }
                    for id in delete_ids {
                        if !tx
                            .delete_by_id::<AiInboxEventRecord>(&id)
                            .await
                            .map_err(OrmPublicError::from)?
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                    }
                    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                        actor_principal_kind: "system".to_owned(),
                        actor_subject: "inbox-retention".to_owned(),
                        action: "prune_inbox_events".to_owned(),
                        resource_kind: "ai_inbox_stream".to_owned(),
                        resource_reference: current.id.to_string(),
                        outcome: "allowed".to_owned(),
                        reason_code: "retention_prefix_expired".to_owned(),
                        correlation_id: Uuid::new_v4().to_string(),
                        causation_id: None,
                        policy_version: Some(policy_version_hash(&policy_versions)),
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    Ok(PruneStreamOutcome::Pruned(deleted))
                })
            })
            .await
            .map_err(map_transaction)
    }
}

#[async_trait]
impl AiInboxPruningService for OrmAiInboxPruningService {
    async fn prune_inbox_events(&self) -> Result<AiInboxPruningReport, AiError> {
        let candidates = self.candidates().await?;
        let mut report = AiInboxPruningReport {
            streams_scanned: u32::try_from(candidates.len())
                .map_err(|_| AiError::PersistenceFailed)?,
            ..AiInboxPruningReport::default()
        };
        let now = self.clock.now().unix_timestamp();
        for candidate in candidates {
            match self.prune_stream(candidate, now).await? {
                PruneStreamOutcome::Noop => {}
                PruneStreamOutcome::NotReady => report.streams_not_ready += 1,
                PruneStreamOutcome::Conflict => report.streams_conflicted += 1,
                PruneStreamOutcome::Pruned(count) => {
                    report.streams_pruned += 1;
                    report.events_deleted = report.events_deleted.saturating_add(
                        u32::try_from(count).map_err(|_| AiError::PersistenceFailed)?,
                    );
                }
            }
        }
        Ok(report)
    }
}

enum PruneStreamOutcome {
    Noop,
    NotReady,
    Conflict,
    Pruned(usize),
}

/// Current-principal durable inbox service.
pub struct OrmAiInboxService {
    database: Database<DefaultWriteBackend>,
    principal_resolver: Arc<dyn CurrentPrincipalResolver>,
    access_policy: Arc<dyn AiAccessPolicy>,
    protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
    content_protector: Arc<dyn AiContentProtector>,
    reauthorization_interval: Duration,
    replay_page_size: i64,
}

impl OrmAiInboxService {
    /// Creates a service with 30-second reauthorization and 100-event replay
    /// pages.
    pub fn new(
        database: Database<DefaultWriteBackend>,
        principal_resolver: Arc<dyn CurrentPrincipalResolver>,
        access_policy: Arc<dyn AiAccessPolicy>,
        protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
        content_protector: Arc<dyn AiContentProtector>,
    ) -> Self {
        Self {
            database,
            principal_resolver,
            access_policy,
            protection_policy,
            content_protector,
            reauthorization_interval: Duration::from_secs(30),
            replay_page_size: 100,
        }
    }

    /// Overrides the periodic principal/session/scope reauthorization
    /// interval. Zero is rejected when a stream opens.
    #[must_use]
    pub fn with_reauthorization_interval(mut self, interval: Duration) -> Self {
        self.reauthorization_interval = interval;
        self
    }

    /// Overrides durable replay page size, validated within `1..=500` when a
    /// stream opens.
    #[must_use]
    pub fn with_replay_page_size(mut self, page_size: i64) -> Self {
        self.replay_page_size = page_size;
        self
    }

    async fn event_page(
        &self,
        principal: &AuthPrincipal,
        after_sequence: i64,
        first: i64,
    ) -> Result<AiInboxEventPage, AiError> {
        if after_sequence < 0 || after_sequence > i64::from(i32::MAX) || !(1..=500).contains(&first)
        {
            return Err(AiError::InvalidInput(
                "invalid inbox event window".to_owned(),
            ));
        }
        let (principal_kind, principal_subject) = principal_identity(principal);
        let principal_subject = principal_subject.to_owned();
        let stream_id = inbox_stream_id(&principal_kind, &principal_subject);
        let (stream, rows) = self
            .database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    let stream = tx
                        .find_by_id::<AiInboxStreamRecord>(&stream_id)
                        .await
                        .map_err(OrmPublicError::from)?;
                    let Some(stream) = stream else {
                        return Ok((None, Vec::new()));
                    };
                    if stream.principal_kind != principal_kind
                        || stream.principal_subject != principal_subject
                        || stream.stream_head < 0
                        || stream.minimum_retained_sequence < 1
                        || stream.minimum_retained_sequence > stream.stream_head.saturating_add(1)
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                    }
                    let watermark = stream.stream_head;
                    let rows = tx
                        .query::<AiInboxEventRecord>()
                        .filter(AiInboxEventRecordWhereInput {
                            principal_kind: Some(StringFilter {
                                eq: Some(principal_kind),
                                ..Default::default()
                            }),
                            principal_subject: Some(StringFilter {
                                eq: Some(principal_subject),
                                ..Default::default()
                            }),
                            sequence: Some(IntFilter {
                                gt: Some(i32::try_from(after_sequence).map_err(|_| {
                                    OrmPublicError::new(OrmErrorCode::InvalidInput)
                                })?),
                                lte: Some(i32::try_from(watermark).map_err(|_| {
                                    OrmPublicError::new(OrmErrorCode::InvalidInput)
                                })?),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .default_order()
                        .limit(first.saturating_add(1))
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    Ok((Some(stream), rows))
                })
            })
            .await
            .map_err(map_transaction)?;
        let Some(stream) = stream else {
            return Ok(AiInboxEventPage {
                events: Vec::new(),
                watermark: 0,
                has_more: false,
                reset_required: after_sequence != 0,
            });
        };
        if after_sequence.saturating_add(1) < stream.minimum_retained_sequence {
            return Ok(AiInboxEventPage {
                events: Vec::new(),
                watermark: stream.stream_head,
                has_more: false,
                reset_required: true,
            });
        }
        let has_more = rows.len() > first as usize;
        let mut rows = rows;
        rows.truncate(first as usize);
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            if row.principal_kind != stream.principal_kind
                || row.principal_subject != stream.principal_subject
                || row.sequence <= after_sequence
                || row.sequence > stream.stream_head
            {
                return Err(AiError::PersistenceFailed);
            }
            let session_id = row.session_id.ok_or(AiError::PersistenceFailed)?;
            let session = AiSessionRecord::find_by_id(&self.database, &session_id)
                .await
                .map_err(|error| map_orm(OrmPublicError::from(error)))?
                .ok_or(AiError::Forbidden)?;
            if session.owner_principal_kind != stream.principal_kind
                || session.owner_subject != stream.principal_subject
            {
                return Err(AiError::Forbidden);
            }
            let scope = session_scope(&session);
            if !self
                .access_policy
                .can_access_session(principal, AiSessionId(session_id), AiSessionAction::Read)
                .await
                .is_allowed()
                || !self
                    .access_policy
                    .can_access_scope(principal, &scope, AiSessionAction::Read)
                    .await
                    .is_allowed()
            {
                return Err(AiError::Forbidden);
            }
            let policy = self.protection_policy.resolve(principal, &scope).await?;
            if !policy.ready || policy.scope != scope {
                return Err(AiError::RuntimeNotReady);
            }
            let payload = self
                .open_value(
                    &policy,
                    ContentProtectionContext {
                        entity: "graphql_orm_ai_inbox_events".to_owned(),
                        row_id: row.id.to_string(),
                        field: "protected_payload".to_owned(),
                        scope,
                    },
                    &row.protected_payload,
                )
                .await?;
            events.push(AiInboxEventView {
                id: row.id,
                sequence: row.sequence,
                session_id,
                event_type: row.event_type,
                payload: async_graphql::Json(payload),
                created_at: row.created_at,
            });
        }
        Ok(AiInboxEventPage {
            events,
            watermark: stream.stream_head,
            has_more,
            reset_required: false,
        })
    }

    async fn open_value(
        &self,
        policy: &AiContentProtectionPolicy,
        context: ContentProtectionContext,
        value: &Value,
    ) -> Result<Value, AiError> {
        let envelope: ProtectedContentEnvelope =
            serde_json::from_value(value.clone()).map_err(|_| AiError::PersistenceFailed)?;
        self.content_protector
            .open(policy, &context, &envelope)
            .await
            .map_err(|error| match error {
                crate::ContentProtectionError::PolicyNotReady => AiError::RuntimeNotReady,
                _ => AiError::PersistenceFailed,
            })
    }
}

#[async_trait]
impl AiInboxService for OrmAiInboxService {
    async fn inbox_event_page(
        &self,
        principal: &AuthPrincipal,
        after_sequence: i64,
        first: i64,
    ) -> Result<AiInboxEventPage, AiError> {
        self.event_page(principal, after_sequence, first).await
    }

    async fn inbox_events(
        &self,
        principal: AuthPrincipal,
        after_sequence: i64,
    ) -> Result<AiInboxEventStream, AiError> {
        if after_sequence < 0
            || after_sequence > i64::from(i32::MAX)
            || self.reauthorization_interval.is_zero()
            || !(1..=500).contains(&self.replay_page_size)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid AI inbox subscription bounds".to_owned(),
            ));
        }
        let principal_reference = principal.reference();
        let (principal_kind, principal_subject) = principal_identity(&principal);
        let principal_subject = principal_subject.to_owned();
        let mut wakeups = self
            .database
            .ensure_event_sender::<AiInboxWakeup>()
            .subscribe();
        let service = self.clone_for_stream();
        let resolver = self.principal_resolver.clone();
        let reauthorization_interval = self.reauthorization_interval;
        let replay_page_size = self.replay_page_size;

        Ok(Box::pin(async_stream::try_stream! {
            let mut current_principal = principal;
            let mut delivered_sequence = after_sequence;
            let mut replay_required = true;
            let mut reauthorize = tokio::time::interval_at(
                Instant::now() + reauthorization_interval,
                reauthorization_interval,
            );
            reauthorize.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                if replay_required {
                    let mut page = service
                        .event_page(&current_principal, delivered_sequence, replay_page_size)
                        .await?;
                    let target_watermark = page.watermark;
                    if target_watermark < delivered_sequence || page.reset_required {
                        yield AiInboxEventEnvelope {
                            event: None,
                            watermark: target_watermark,
                            reset_required: true,
                        };
                        return;
                    }
                    loop {
                        let mut crossed_watermark = false;
                        for event in page.events {
                            if event.sequence > target_watermark {
                                crossed_watermark = true;
                                break;
                            }
                            if event.sequence <= delivered_sequence {
                                continue;
                            }
                            delivered_sequence = event.sequence;
                            yield AiInboxEventEnvelope {
                                event: Some(event),
                                watermark: target_watermark,
                                reset_required: false,
                            };
                        }
                        if delivered_sequence >= target_watermark || crossed_watermark {
                            break;
                        }
                        if !page.has_more {
                            yield AiInboxEventEnvelope {
                                event: None,
                                watermark: target_watermark,
                                reset_required: true,
                            };
                            return;
                        }
                        page = service
                            .event_page(&current_principal, delivered_sequence, replay_page_size)
                            .await?;
                        if page.reset_required {
                            yield AiInboxEventEnvelope {
                                event: None,
                                watermark: target_watermark,
                                reset_required: true,
                            };
                            return;
                        }
                    }
                    replay_required = false;
                }

                let should_reauthorize = tokio::select! {
                    _ = reauthorize.tick() => true,
                    wakeup = wakeups.recv() => {
                        match wakeup {
                            Ok(wakeup)
                                if wakeup.principal_kind == principal_kind
                                    && wakeup.principal_subject == principal_subject
                                    && wakeup.sequence > delivered_sequence =>
                            {
                                replay_required = true;
                            }
                            Ok(_) => {}
                            Err(RecvError::Lagged(_)) => replay_required = true,
                            Err(RecvError::Closed) => return,
                        }
                        false
                    }
                };
                if should_reauthorize {
                    let resolved = resolver
                        .resolve(&principal_reference)
                        .await
                        .map_err(|_| AiError::ReauthorizationFailed)?;
                    current_principal = resolved.into_principal();
                    let (current_kind, current_subject) = principal_identity(&current_principal);
                    if current_kind != principal_kind || current_subject != principal_subject {
                        Err(AiError::ReauthorizationFailed)?;
                    }
                    replay_required = true;
                }
            }
        }))
    }
}

impl OrmAiInboxService {
    fn clone_for_stream(&self) -> Self {
        Self {
            database: self.database.clone(),
            principal_resolver: self.principal_resolver.clone(),
            access_policy: self.access_policy.clone(),
            protection_policy: self.protection_policy.clone(),
            content_protector: self.content_protector.clone(),
            reauthorization_interval: self.reauthorization_interval,
            replay_page_size: self.replay_page_size,
        }
    }
}

pub(crate) fn inbox_stream_id(principal_kind: &str, principal_subject: &str) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"graphql-orm-ai/inbox-stream/v1\0");
    hash.update(principal_kind.as_bytes());
    hash.update(b"\0");
    hash.update(principal_subject.as_bytes());
    let digest = hash.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn principal_identity(principal: &AuthPrincipal) -> (String, &str) {
    let kind = match principal {
        AuthPrincipal::User(_) => "user".to_owned(),
        AuthPrincipal::ApiToken(token) => format!("api_token:{}", token.principal_kind.as_str()),
    };
    (kind, principal.subject())
}

fn session_scope(session: &AiSessionRecord) -> AiScope {
    AiScope {
        kind: session.scope_kind.clone(),
        id: session.scope_id.clone(),
        tenant_id: session.tenant_id.clone(),
    }
}

fn valid_event_type(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn validate_stream(stream: &AiInboxStreamRecord) -> Result<(), OrmPublicError> {
    if stream.principal_kind.trim().is_empty()
        || stream.principal_subject.trim().is_empty()
        || stream.stream_head < 0
        || stream.minimum_retained_sequence < 1
        || stream.minimum_retained_sequence > stream.stream_head.saturating_add(1)
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    Ok(())
}

fn valid_retention_policy(
    policy: &AiRetentionPolicyRecord,
    scope: &AiScope,
    expected_scope_key: &str,
) -> bool {
    const MINIMUM_RETENTION_SECONDS: i64 = 60;
    const MAXIMUM_RETENTION_SECONDS: i64 = 315_576_000;
    policy.scope_key.as_deref() == Some(expected_scope_key)
        && policy.scope_kind == scope.kind
        && policy.scope_id == scope.id
        && policy.tenant_id == scope.tenant_id
        && policy.inbox_event_retention_seconds.is_some_and(|seconds| {
            (MINIMUM_RETENTION_SECONDS..=MAXIMUM_RETENTION_SECONDS).contains(&seconds)
        })
        && policy
            .inbox_minimum_events
            .is_some_and(|count| (1..=100_000).contains(&count))
}

fn policy_version_hash(versions: &BTreeSet<String>) -> String {
    let mut hash = Sha256::new();
    hash.update(b"graphql-orm-ai/inbox-retention-policy-set/v1\0");
    for value in versions {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hex::encode(hash.finalize())
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
