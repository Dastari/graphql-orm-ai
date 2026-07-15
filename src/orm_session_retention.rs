//! ORM-only bounded pruning of protected session, tool, checkpoint, and message content.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use agql_auth::Clock;
use async_trait::async_trait;
use graphql_orm::db::Database;
use graphql_orm::graphql::errors::{OrmErrorCode, OrmPublicError};
use graphql_orm::graphql::filters::{IntFilter, StringFilter, UuidFilter};
use graphql_orm::graphql::orm::{
    ConditionalUpdateOutcome, DefaultWriteBackend, EntityAccessKind, EntityAccessSurface,
    EntityPolicy, MutationLimit, OrderDirection, RetentionPurgeOutcome, TransactionError,
    TransactionMode,
};
use graphql_orm::graphql::pagination::KeysetConnectionInput;
use uuid::Uuid;

use crate::persistence::*;
use crate::{AiError, AiRunState, AiScope, AiSessionRetentionReport, AiSessionRetentionService};

pub(crate) const MAXIMUM_RETENTION_SECONDS: i64 = 315_576_000;
const RUN_CHECKPOINT_RETENTION_POLICY: &str = "graphql_orm_ai.run_checkpoint.retention_purge";
const PROTECTED_RUN_CHECKPOINT_KINDS: [&str; 3] = [
    "provider_turn_persisted",
    "tool_batch_persisted",
    "supervised_tool_batch_persisted",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ToolPayloadRetentionMode {
    DeletingSession,
    ExpiredRaw,
}

/// Deployment hard bounds for one session-retention scan page.
///
/// These limits constrain generated ORM reads and writes. They do not grant a
/// user capability and do not broaden the current GraphQL-managed policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiSessionRetentionLimits {
    maximum_sessions: usize,
    maximum_live_delta_events_per_session: usize,
    maximum_inbox_events_per_session: usize,
    maximum_context_checkpoints_per_session: usize,
    maximum_messages_per_session: usize,
    maximum_message_blocks_per_session: usize,
    maximum_proposals_per_session: usize,
    maximum_proposal_items_per_session: usize,
    maximum_tool_calls_per_session: usize,
    maximum_approvals_per_session: usize,
    maximum_attachments_per_session: usize,
    maximum_attachment_artifacts_per_session: usize,
    maximum_runs_per_session: usize,
    maximum_run_checkpoints_per_session: usize,
}

impl AiSessionRetentionLimits {
    /// Creates validated per-pass bounds, using the message bound for context
    /// checkpoints as well.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless sessions are in
    /// `1..=256`, protected events, context checkpoints, and messages are each
    /// in `1..=5_000`, and the total message-block bound is in `1..=20_000`.
    pub fn new(
        maximum_sessions: usize,
        maximum_live_delta_events_per_session: usize,
        maximum_messages_per_session: usize,
        maximum_message_blocks_per_session: usize,
    ) -> Result<Self, AiError> {
        Self::new_with_context_checkpoints(
            maximum_sessions,
            maximum_live_delta_events_per_session,
            maximum_messages_per_session,
            maximum_messages_per_session,
            maximum_message_blocks_per_session,
        )
    }

    /// Creates validated per-pass bounds with an independent context limit.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless sessions are in
    /// `1..=256`, protected events, context checkpoints, and messages are each
    /// in `1..=5_000`, and the total message-block bound is in `1..=20_000`.
    pub fn new_with_context_checkpoints(
        maximum_sessions: usize,
        maximum_live_delta_events_per_session: usize,
        maximum_context_checkpoints_per_session: usize,
        maximum_messages_per_session: usize,
        maximum_message_blocks_per_session: usize,
    ) -> Result<Self, AiError> {
        if !(1..=256).contains(&maximum_sessions)
            || !(1..=5_000).contains(&maximum_live_delta_events_per_session)
            || !(1..=5_000).contains(&maximum_context_checkpoints_per_session)
            || !(1..=5_000).contains(&maximum_messages_per_session)
            || !(1..=20_000).contains(&maximum_message_blocks_per_session)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid session-retention limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_sessions,
            maximum_live_delta_events_per_session,
            maximum_inbox_events_per_session: maximum_live_delta_events_per_session,
            maximum_context_checkpoints_per_session,
            maximum_messages_per_session,
            maximum_message_blocks_per_session,
            maximum_proposals_per_session: maximum_messages_per_session,
            maximum_proposal_items_per_session: maximum_message_blocks_per_session,
            maximum_tool_calls_per_session: maximum_messages_per_session,
            maximum_approvals_per_session: maximum_messages_per_session,
            maximum_attachments_per_session: maximum_messages_per_session,
            maximum_attachment_artifacts_per_session: maximum_messages_per_session,
            maximum_runs_per_session: maximum_messages_per_session,
            maximum_run_checkpoints_per_session: maximum_context_checkpoints_per_session,
        })
    }

    /// Sets independent bounds for proving terminal runs and purging their
    /// deleting-session or age-expired orphaned append-only coordinator
    /// checkpoints.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless both limits are in
    /// `1..=5_000`.
    pub fn with_run_checkpoint_limits(
        mut self,
        maximum_runs_per_session: usize,
        maximum_run_checkpoints_per_session: usize,
    ) -> Result<Self, AiError> {
        if !(1..=5_000).contains(&maximum_runs_per_session)
            || !(1..=5_000).contains(&maximum_run_checkpoints_per_session)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid session-retention run-checkpoint limits".to_owned(),
            ));
        }
        self.maximum_runs_per_session = maximum_runs_per_session;
        self.maximum_run_checkpoints_per_session = maximum_run_checkpoints_per_session;
        Ok(self)
    }

    /// Sets an independent bound for protected principal-inbox events tied to
    /// one deleting session.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless the limit is in
    /// `1..=5_000`.
    pub fn with_inbox_event_limit(
        mut self,
        maximum_inbox_events_per_session: usize,
    ) -> Result<Self, AiError> {
        if !(1..=5_000).contains(&maximum_inbox_events_per_session) {
            return Err(AiError::InvalidConfiguration(
                "invalid session-retention inbox-event limit".to_owned(),
            ));
        }
        self.maximum_inbox_events_per_session = maximum_inbox_events_per_session;
        Ok(self)
    }

    /// Sets an independent bound for proving and coordinating attachment
    /// cleanup for one deleting session.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless the limit is in
    /// `1..=5_000`.
    pub fn with_attachment_limit(
        mut self,
        maximum_attachments_per_session: usize,
    ) -> Result<Self, AiError> {
        if !(1..=5_000).contains(&maximum_attachments_per_session) {
            return Err(AiError::InvalidConfiguration(
                "invalid session-retention attachment limit".to_owned(),
            ));
        }
        self.maximum_attachments_per_session = maximum_attachments_per_session;
        Ok(self)
    }

    /// Sets an independent whole-session bound for proving and coordinating
    /// dependency-ordered attachment-artifact cleanup.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless the limit is in
    /// `1..=5_000`.
    pub fn with_attachment_artifact_limit(
        mut self,
        maximum_attachment_artifacts_per_session: usize,
    ) -> Result<Self, AiError> {
        if !(1..=5_000).contains(&maximum_attachment_artifacts_per_session) {
            return Err(AiError::InvalidConfiguration(
                "invalid session-retention attachment-artifact limit".to_owned(),
            ));
        }
        self.maximum_attachment_artifacts_per_session = maximum_attachment_artifacts_per_session;
        Ok(self)
    }

    /// Sets independent whole-session proposal and proposal-item proof bounds.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless proposals are in
    /// `1..=5_000` and proposal items are in `1..=20_000`.
    pub fn with_proposal_limits(
        mut self,
        maximum_proposals_per_session: usize,
        maximum_proposal_items_per_session: usize,
    ) -> Result<Self, AiError> {
        if !(1..=5_000).contains(&maximum_proposals_per_session)
            || !(1..=20_000).contains(&maximum_proposal_items_per_session)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid session-retention proposal limits".to_owned(),
            ));
        }
        self.maximum_proposals_per_session = maximum_proposals_per_session;
        self.maximum_proposal_items_per_session = maximum_proposal_items_per_session;
        Ok(self)
    }

    /// Sets independent whole-session tool-call and approval proof bounds.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless both limits are in
    /// `1..=5_000`.
    pub fn with_tool_payload_limits(
        mut self,
        maximum_tool_calls_per_session: usize,
        maximum_approvals_per_session: usize,
    ) -> Result<Self, AiError> {
        if !(1..=5_000).contains(&maximum_tool_calls_per_session)
            || !(1..=5_000).contains(&maximum_approvals_per_session)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid session-retention tool-payload limits".to_owned(),
            ));
        }
        self.maximum_tool_calls_per_session = maximum_tool_calls_per_session;
        self.maximum_approvals_per_session = maximum_approvals_per_session;
        Ok(self)
    }

    /// Maximum session metadata rows considered in one scan page.
    pub const fn maximum_sessions(self) -> usize {
        self.maximum_sessions
    }

    /// Maximum protected event rows deleted for one session.
    ///
    /// Outside the deleting-session cutoff this applies only to expired
    /// provisional live deltas. After that cutoff it bounds all protected
    /// session event rows.
    pub const fn maximum_live_delta_events_per_session(self) -> usize {
        self.maximum_live_delta_events_per_session
    }

    /// Maximum protected principal-inbox payloads tombstoned for one deleting
    /// session transaction.
    pub const fn maximum_inbox_events_per_session(self) -> usize {
        self.maximum_inbox_events_per_session
    }

    /// Maximum protected context-summary checkpoints deleted for one session.
    pub const fn maximum_context_checkpoints_per_session(self) -> usize {
        self.maximum_context_checkpoints_per_session
    }

    /// Maximum unpurged message rows inspected for one session.
    pub const fn maximum_messages_per_session(self) -> usize {
        self.maximum_messages_per_session
    }

    /// Maximum message blocks deleted for one session transaction.
    pub const fn maximum_message_blocks_per_session(self) -> usize {
        self.maximum_message_blocks_per_session
    }

    /// Maximum attachment rows proved for one deleting session.
    pub const fn maximum_attachments_per_session(self) -> usize {
        self.maximum_attachments_per_session
    }

    /// Maximum attachment-artifact rows proved for one deleting session.
    pub const fn maximum_attachment_artifacts_per_session(self) -> usize {
        self.maximum_attachment_artifacts_per_session
    }

    /// Maximum proposal rows proved for one deleting session.
    pub const fn maximum_proposals_per_session(self) -> usize {
        self.maximum_proposals_per_session
    }

    /// Maximum proposal-item rows proved for one deleting session.
    pub const fn maximum_proposal_items_per_session(self) -> usize {
        self.maximum_proposal_items_per_session
    }

    /// Maximum tool-call rows proved for one deleting session.
    pub const fn maximum_tool_calls_per_session(self) -> usize {
        self.maximum_tool_calls_per_session
    }

    /// Maximum approval rows proved for one deleting session.
    pub const fn maximum_approvals_per_session(self) -> usize {
        self.maximum_approvals_per_session
    }

    /// Maximum run rows used to prove one deleting session is terminal.
    pub const fn maximum_runs_per_session(self) -> usize {
        self.maximum_runs_per_session
    }

    /// Maximum append-only run checkpoints deleted for one session pass under
    /// either the deleting-session or age-expired orphan proof.
    pub const fn maximum_run_checkpoints_per_session(self) -> usize {
        self.maximum_run_checkpoints_per_session
    }
}

impl Default for AiSessionRetentionLimits {
    fn default() -> Self {
        Self {
            maximum_sessions: 50,
            maximum_live_delta_events_per_session: 500,
            maximum_inbox_events_per_session: 500,
            maximum_context_checkpoints_per_session: 100,
            maximum_messages_per_session: 100,
            maximum_message_blocks_per_session: 5_000,
            maximum_proposals_per_session: 100,
            maximum_proposal_items_per_session: 5_000,
            maximum_tool_calls_per_session: 100,
            maximum_approvals_per_session: 100,
            maximum_attachments_per_session: 100,
            maximum_attachment_artifacts_per_session: 100,
            maximum_runs_per_session: 100,
            maximum_run_checkpoints_per_session: 100,
        }
    }
}

#[derive(Clone)]
struct SessionRetentionEntityPolicy {
    delegate: Option<Arc<dyn EntityPolicy<DefaultWriteBackend>>>,
}

impl EntityPolicy<DefaultWriteBackend> for SessionRetentionEntityPolicy {
    fn can_access_entity<'a>(
        &'a self,
        context: Option<&'a async_graphql::Context<'_>>,
        database: &'a Database<DefaultWriteBackend>,
        entity_name: &'static str,
        policy_key: Option<&'static str>,
        kind: EntityAccessKind,
        surface: EntityAccessSurface,
    ) -> graphql_orm::futures::future::BoxFuture<'a, async_graphql::Result<bool>> {
        if surface == EntityAccessSurface::RetentionMaintenance {
            let allowed = entity_name == "AiRunCheckpointRecord"
                && policy_key == Some(RUN_CHECKPOINT_RETENTION_POLICY)
                && kind == EntityAccessKind::Write;
            return Box::pin(async move { Ok(allowed) });
        }
        if let Some(delegate) = &self.delegate {
            return delegate.can_access_entity(
                context,
                database,
                entity_name,
                policy_key,
                kind,
                surface,
            );
        }
        Box::pin(async { Ok(true) })
    }
}

/// Trusted ORM-only worker for GraphQL-managed session retention.
///
/// The worker never opens or copies protected payloads. It deletes expired
/// provisional delta rows and age-expired terminal tool/approval payloads. A
/// separate retention transaction may then delete age-expired orphaned
/// protected coordinator checkpoints only after re-proving terminal run,
/// attempt-outcome, budget, final-output or tombstoned-tool dependencies. After
/// the deleting-session cutoff it deletes all bounded protected session event
/// rows. Protected context-summary checkpoints are deleted in bounded pages
/// before terminal proposal/item payloads are tombstoned under whole-session
/// bounds. A later whole-session proof tombstones every remaining protected
/// tool/approval payload only for exact terminal call/step/approval graphs.
/// Attachment artifacts then enter independently scheduled generation-fenced
/// exact-reference local/provider cleanup before their parent attachments;
/// only confirmed tombstones are deleted before eligible finalized message
/// blocks are scrubbed. Once those
/// sources are exhausted and every bounded run is terminal, the worker clears
/// current checkpoint pointers before a separate retention transaction
/// physically deletes append-only coordinator checkpoints. Unresolved accepted
/// proposals, active or uncertain tool authority, nonterminal runs, and
/// ambiguous local/provider deletion remain blocked. Redacted audit,
/// usage, egress, attempt, pricing, and skill facts remain non-purgeable.
pub struct OrmAiSessionRetentionService {
    database: Database<DefaultWriteBackend>,
    clock: Arc<dyn Clock>,
    limits: AiSessionRetentionLimits,
}

impl OrmAiSessionRetentionService {
    /// Creates a bounded trusted retention worker.
    pub fn new(
        mut database: Database<DefaultWriteBackend>,
        clock: Arc<dyn Clock>,
        limits: AiSessionRetentionLimits,
    ) -> Self {
        let delegate = database.entity_policy().cloned();
        database.set_entity_policy(SessionRetentionEntityPolicy { delegate });
        Self {
            database,
            clock,
            limits,
        }
    }

    async fn candidates(&self, after: Option<String>) -> Result<SessionCandidatePage, AiError> {
        validate_cursor(after.as_deref())?;
        let connection =
            AiSessionRecord::keyset_connection_page(
                &self.database,
                AiSessionRecordWhereInput {
                    state: Some(StringFilter {
                        in_list: Some(vec![
                            "active".to_owned(),
                            "archived".to_owned(),
                            "deleting".to_owned(),
                        ]),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                KeysetConnectionInput {
                    after,
                    first: Some(i64::try_from(self.limits.maximum_sessions).map_err(|_| {
                        AiError::InvalidConfiguration("invalid scan limit".to_owned())
                    })?),
                    ..Default::default()
                },
            )
            .await
            .map_err(map_orm)?;
        let next_cursor = connection
            .page_info
            .has_next_page
            .then_some(connection.page_info.end_cursor)
            .flatten();
        Ok(SessionCandidatePage {
            sessions: connection.edges.into_iter().map(|edge| edge.node).collect(),
            next_cursor,
        })
    }

    async fn prune_session(
        &self,
        candidate: AiSessionRecord,
        now: i64,
    ) -> Result<SessionPruneOutcome, AiError> {
        let event_limit = i64::try_from(self.limits.maximum_live_delta_events_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid event limit".to_owned()))?;
        let inbox_event_limit = i64::try_from(self.limits.maximum_inbox_events_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid inbox event limit".to_owned()))?;
        let context_checkpoint_limit =
            i64::try_from(self.limits.maximum_context_checkpoints_per_session).map_err(|_| {
                AiError::InvalidConfiguration("invalid context checkpoint limit".to_owned())
            })?;
        let context_checkpoint_limit_with_lookahead =
            context_checkpoint_limit.checked_add(1).ok_or_else(|| {
                AiError::InvalidConfiguration("invalid context checkpoint limit".to_owned())
            })?;
        let message_limit = i64::try_from(self.limits.maximum_messages_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid message limit".to_owned()))?;
        let proposal_limit = i64::try_from(self.limits.maximum_proposals_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid proposal limit".to_owned()))?;
        let proposal_limit_with_lookahead = proposal_limit
            .checked_add(1)
            .ok_or_else(|| AiError::InvalidConfiguration("invalid proposal limit".to_owned()))?;
        let proposal_item_limit = i64::try_from(self.limits.maximum_proposal_items_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid proposal-item limit".to_owned()))?;
        let proposal_item_limit_with_lookahead =
            proposal_item_limit.checked_add(1).ok_or_else(|| {
                AiError::InvalidConfiguration("invalid proposal-item limit".to_owned())
            })?;
        let tool_call_limit = i64::try_from(self.limits.maximum_tool_calls_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid tool-call limit".to_owned()))?;
        let tool_call_limit_with_lookahead = tool_call_limit
            .checked_add(1)
            .ok_or_else(|| AiError::InvalidConfiguration("invalid tool-call limit".to_owned()))?;
        let approval_limit = i64::try_from(self.limits.maximum_approvals_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid approval limit".to_owned()))?;
        let approval_limit_with_lookahead = approval_limit
            .checked_add(1)
            .ok_or_else(|| AiError::InvalidConfiguration("invalid approval limit".to_owned()))?;
        let attachment_limit = i64::try_from(self.limits.maximum_attachments_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid attachment limit".to_owned()))?;
        let attachment_limit_with_lookahead = attachment_limit
            .checked_add(1)
            .ok_or_else(|| AiError::InvalidConfiguration("invalid attachment limit".to_owned()))?;
        let attachment_artifact_limit =
            i64::try_from(self.limits.maximum_attachment_artifacts_per_session).map_err(|_| {
                AiError::InvalidConfiguration("invalid attachment-artifact limit".to_owned())
            })?;
        let attachment_artifact_limit_with_lookahead =
            attachment_artifact_limit.checked_add(1).ok_or_else(|| {
                AiError::InvalidConfiguration("invalid attachment-artifact limit".to_owned())
            })?;
        let run_limit = i64::try_from(self.limits.maximum_runs_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid run limit".to_owned()))?;
        let run_limit_with_lookahead = run_limit
            .checked_add(1)
            .ok_or_else(|| AiError::InvalidConfiguration("invalid run limit".to_owned()))?;
        let maximum_blocks = self.limits.maximum_message_blocks_per_session;
        let maximum_context_checkpoints = self.limits.maximum_context_checkpoints_per_session;
        let result = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&candidate.id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if session.row_version != candidate.row_version {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    validate_session(&session)?;
                    let scope = session_scope(&session);
                    let exact_scope_key = crate::ai_scope_key(&scope);
                    let policies = tx
                        .query::<AiRetentionPolicyRecord>()
                        .filter(AiRetentionPolicyRecordWhereInput {
                            scope_key: Some(StringFilter {
                                eq: Some(exact_scope_key.clone()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if policies.len() > 1 {
                        return Err(OrmPublicError::new(
                            OrmErrorCode::AuthorizationMisconfigured,
                        ));
                    }
                    let Some(policy) = policies.into_iter().next() else {
                        return Ok(SessionPruneOutcome::NotReady);
                    };
                    if !valid_policy(&policy, &scope, &exact_scope_key) {
                        return Ok(SessionPruneOutcome::NotReady);
                    }

                    let deletion_cutoff_reached = if session.state == "deleting" {
                        let deleted_at = session
                            .deleted_at
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                        let cutoff = deleted_at
                            .checked_add(policy.deleted_content_purge_seconds)
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                        cutoff <= now
                    } else {
                        false
                    };
                    let delta_cutoff = now
                        .checked_sub(policy.delta_retention_seconds)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let event_rows = tx
                        .query::<AiSessionEventRecord>()
                        .filter(AiSessionEventRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session.id),
                                ..Default::default()
                            }),
                            event_type: (!deletion_cutoff_reached).then(|| StringFilter {
                                eq: Some("provider_live_delta".to_owned()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .default_order()
                        .limit(event_limit)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let mut previous_event_sequence = None;
                    let mut event_ids = Vec::new();
                    for row in event_rows {
                        if row.session_id != session.id
                            || (!deletion_cutoff_reached && row.event_type != "provider_live_delta")
                            || row.sequence <= 0
                            || row.sequence > session.stream_head
                            || previous_event_sequence.is_some_and(|value| row.sequence <= value)
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                        previous_event_sequence = Some(row.sequence);
                        if deletion_cutoff_reached || row.created_at <= delta_cutoff {
                            event_ids.push(row.id);
                        }
                    }
                    let deleting_session_events_deleted = if deletion_cutoff_reached {
                        event_ids.len()
                    } else {
                        0
                    };
                    let live_delta_events_deleted = if deletion_cutoff_reached {
                        0
                    } else {
                        event_ids.len()
                    };

                    let inbox_event_rows = if deletion_cutoff_reached {
                        tx.query::<AiInboxEventRecord>()
                            .filter(AiInboxEventRecordWhereInput {
                                session_id: Some(UuidFilter {
                                    eq: Some(session.id),
                                    ..Default::default()
                                }),
                                payload_purged_at: Some(IntFilter {
                                    is_null: Some(true),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .default_order()
                            .limit(inbox_event_limit)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?
                    } else {
                        Vec::new()
                    };
                    let mut inbox_events = Vec::with_capacity(inbox_event_rows.len());
                    let mut previous_inbox_sequence = None;
                    for event in inbox_event_rows {
                        if event.id.is_nil()
                            || event.session_id != Some(session.id)
                            || event.principal_kind != session.owner_principal_kind
                            || event.principal_subject != session.owner_subject
                            || event.scope_key != exact_scope_key
                            || event.scope_kind != session.scope_kind
                            || event.scope_id != session.scope_id
                            || event.tenant_id != session.tenant_id
                            || event.sequence <= 0
                            || event.protected_payload.is_none()
                            || event.payload_purged_at.is_some()
                            || previous_inbox_sequence
                                .is_some_and(|sequence| event.sequence <= sequence)
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                        previous_inbox_sequence = Some(event.sequence);
                        inbox_events.push(event);
                    }
                    let deleting_session_inbox_payloads_purged = inbox_events.len();

                    let context_checkpoint_rows = if deletion_cutoff_reached {
                        tx.query::<AiContextCheckpointRecord>()
                            .filter(AiContextCheckpointRecordWhereInput {
                                session_id: Some(UuidFilter {
                                    eq: Some(session.id),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .default_order()
                            .limit(context_checkpoint_limit)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?
                    } else {
                        Vec::new()
                    };
                    let mut context_checkpoint_ids = Vec::new();
                    for checkpoint in context_checkpoint_rows {
                        if checkpoint.id.is_nil()
                            || checkpoint.session_id != session.id
                            || checkpoint.through_sequence <= 0
                            || checkpoint.through_sequence > session.message_head
                            || checkpoint.source_hash.trim().is_empty()
                            || checkpoint.source_hash.len() > 512
                            || checkpoint.token_estimate < 0
                            || checkpoint.provider_kind.trim().is_empty()
                            || checkpoint.provider_model.trim().is_empty()
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                        context_checkpoint_ids.push(checkpoint.id);
                    }
                    let deleting_session_context_checkpoints_deleted = context_checkpoint_ids.len();

                    let mut proposal_payloads_purged = 0usize;
                    let mut proposal_payload_purge_blocked = false;
                    if deletion_cutoff_reached
                        && inbox_events.is_empty()
                        && context_checkpoint_ids.is_empty()
                    {
                        let proposals = tx
                            .query::<AiProposalRecord>()
                            .filter(AiProposalRecordWhereInput {
                                session_id: Some(UuidFilter {
                                    eq: Some(session.id),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .default_order()
                            .limit(proposal_limit_with_lookahead)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        if proposals.len()
                            > usize::try_from(proposal_limit)
                                .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?
                        {
                            proposal_payload_purge_blocked = true;
                        } else {
                            let maximum_items = usize::try_from(proposal_item_limit)
                                .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
                            let mut proposal_groups = Vec::with_capacity(proposals.len());
                            let mut item_count = 0usize;
                            for proposal in proposals {
                                validate_proposal(&proposal, &session)?;
                                let items = tx
                                    .query::<AiProposalItemRecord>()
                                    .filter(AiProposalItemRecordWhereInput {
                                        proposal_id: Some(UuidFilter {
                                            eq: Some(proposal.id),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    })
                                    .default_order()
                                    .limit(proposal_item_limit_with_lookahead)
                                    .fetch_all()
                                    .await
                                    .map_err(OrmPublicError::from)?;
                                item_count =
                                    item_count.checked_add(items.len()).ok_or_else(|| {
                                        OrmPublicError::new(OrmErrorCode::InternalError)
                                    })?;
                                if items.len() > maximum_items || item_count > maximum_items {
                                    proposal_payload_purge_blocked = true;
                                    break;
                                }
                                for (index, item) in items.iter().enumerate() {
                                    validate_proposal_item(item, proposal.id, index)?;
                                }
                                proposal_groups.push((proposal, items));
                            }
                            if !proposal_payload_purge_blocked {
                                for (proposal, items) in &proposal_groups {
                                    let Some(run) = tx
                                        .find_by_id::<AiRunRecord>(&proposal.run_id)
                                        .await
                                        .map_err(OrmPublicError::from)?
                                    else {
                                        return Err(OrmPublicError::new(
                                            OrmErrorCode::InternalError,
                                        ));
                                    };
                                    let run_state = AiRunState::from_persisted(&run.state)
                                        .ok_or_else(|| {
                                            OrmPublicError::new(OrmErrorCode::InternalError)
                                        })?;
                                    if run.session_id != session.id || !run_state.is_terminal() {
                                        proposal_payload_purge_blocked = true;
                                        break;
                                    }
                                    if proposal.payload_purged_at.is_some() {
                                        if proposal.protected_payload.is_some()
                                            || proposal.source_references.is_some()
                                            || !proposal_state_is_terminal(proposal, now)
                                            || items.iter().any(|item| {
                                                item.protected_suggested_value.is_some()
                                                    || item.protected_rationale.is_some()
                                                    || item.source_references.is_some()
                                                    || item.protected_review_value.is_some()
                                            })
                                        {
                                            return Err(OrmPublicError::new(
                                                OrmErrorCode::InternalError,
                                            ));
                                        }
                                        continue;
                                    }
                                    if proposal.protected_payload.is_none()
                                        || proposal.source_references.is_none()
                                        || !proposal_state_is_terminal(proposal, now)
                                    {
                                        proposal_payload_purge_blocked = true;
                                        break;
                                    }
                                }
                            }
                            if !proposal_payload_purge_blocked {
                                for (proposal, items) in proposal_groups {
                                    if proposal.payload_purged_at.is_some() {
                                        continue;
                                    }
                                    for item in items {
                                        let updated = tx
                                            .compare_and_swap::<AiProposalItemRecord>(
                                                &item.id,
                                                item.row_version,
                                                AiProposalItemRecordWhereInput {
                                                    proposal_id: Some(UuidFilter {
                                                        eq: Some(proposal.id),
                                                        ..Default::default()
                                                    }),
                                                    ..Default::default()
                                                },
                                                UpdateAiProposalItemRecordInput {
                                                    protected_suggested_value: Some(None),
                                                    protected_rationale: Some(None),
                                                    source_references: Some(None),
                                                    protected_review_value: Some(None),
                                                    ..Default::default()
                                                },
                                            )
                                            .await
                                            .map_err(OrmPublicError::from)?;
                                        if !matches!(updated, ConditionalUpdateOutcome::Updated(_))
                                        {
                                            return Err(OrmPublicError::new(
                                                OrmErrorCode::Conflict,
                                            ));
                                        }
                                    }
                                    let updated = tx
                                        .compare_and_swap::<AiProposalRecord>(
                                            &proposal.id,
                                            proposal.row_version,
                                            AiProposalRecordWhereInput {
                                                session_id: Some(UuidFilter {
                                                    eq: Some(session.id),
                                                    ..Default::default()
                                                }),
                                                ..Default::default()
                                            },
                                            UpdateAiProposalRecordInput {
                                                protected_payload: Some(None),
                                                source_references: Some(None),
                                                payload_purged_at: Some(Some(now)),
                                                state: (proposal.state == "pending_review")
                                                    .then(|| "expired".to_owned()),
                                                ..Default::default()
                                            },
                                        )
                                        .await
                                        .map_err(OrmPublicError::from)?;
                                    if !matches!(updated, ConditionalUpdateOutcome::Updated(_)) {
                                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                                    }
                                    proposal_payloads_purged += 1;
                                }
                            }
                        }
                    }

                    let mut deleting_tool_payloads_purged = 0usize;
                    let mut deleting_approval_payloads_purged = 0usize;
                    let mut expired_tool_payloads_purged = 0usize;
                    let mut expired_approval_payloads_purged = 0usize;
                    let mut tool_payload_purge_blocked = false;
                    let mut raw_payload_purge_blocked = false;
                    let tool_payload_retention_mode = if deletion_cutoff_reached
                        && context_checkpoint_ids.is_empty()
                        && proposal_payloads_purged == 0
                        && !proposal_payload_purge_blocked
                    {
                        Some(ToolPayloadRetentionMode::DeletingSession)
                    } else if !deletion_cutoff_reached {
                        Some(ToolPayloadRetentionMode::ExpiredRaw)
                    } else {
                        None
                    };
                    if let Some(tool_payload_retention_mode) = tool_payload_retention_mode {
                        let raw_payload_cutoff = match tool_payload_retention_mode {
                            ToolPayloadRetentionMode::DeletingSession => None,
                            ToolPayloadRetentionMode::ExpiredRaw => Some(
                                now.checked_sub(policy.raw_payload_retention_seconds)
                                    .ok_or_else(|| {
                                        OrmPublicError::new(OrmErrorCode::InternalError)
                                    })?,
                            ),
                        };
                        let mut phase_blocked = false;
                        let runs = tx
                            .query::<AiRunRecord>()
                            .filter(AiRunRecordWhereInput {
                                session_id: Some(UuidFilter {
                                    eq: Some(session.id),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .default_order()
                            .limit(run_limit_with_lookahead)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        let maximum_runs = usize::try_from(run_limit)
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
                        if runs.len() > maximum_runs {
                            phase_blocked = true;
                        }
                        let mut run_ids = Vec::with_capacity(runs.len());
                        let mut terminal_runs = HashSet::with_capacity(runs.len());
                        for run in &runs {
                            let state = AiRunState::from_persisted(&run.state)
                                .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                            if run.id.is_nil()
                                || run.session_id != session.id
                                || run.lease_generation < 0
                            {
                                return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                            }
                            if state.is_terminal() {
                                terminal_runs.insert(run.id);
                            } else if tool_payload_retention_mode
                                == ToolPayloadRetentionMode::DeletingSession
                            {
                                phase_blocked = true;
                            }
                            run_ids.push(run.id);
                        }
                        if !phase_blocked {
                            let calls = if run_ids.is_empty() {
                                Vec::new()
                            } else {
                                tx.query::<AiToolCallRecord>()
                                    .filter(AiToolCallRecordWhereInput {
                                        run_id: Some(UuidFilter {
                                            in_list: Some(run_ids.clone()),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    })
                                    .default_order()
                                    .limit(tool_call_limit_with_lookahead)
                                    .fetch_all()
                                    .await
                                    .map_err(OrmPublicError::from)?
                            };
                            let maximum_calls = usize::try_from(tool_call_limit)
                                .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
                            if calls.len() > maximum_calls {
                                phase_blocked = true;
                            }
                            let mut eligible_call_ids = HashSet::new();
                            if !phase_blocked {
                                for call in &calls {
                                    validate_tool_call(call, &run_ids)?;
                                    match tool_payload_retention_mode {
                                        ToolPayloadRetentionMode::DeletingSession => {
                                            if !tool_call_state_is_terminal(&call.state) {
                                                phase_blocked = true;
                                                break;
                                            }
                                            eligible_call_ids.insert(call.id);
                                        }
                                        ToolPayloadRetentionMode::ExpiredRaw => {
                                            if tool_call_state_is_terminal(&call.state)
                                                && terminal_runs.contains(&call.run_id)
                                            {
                                                let Some(completed_at) = call.completed_at else {
                                                    phase_blocked = true;
                                                    break;
                                                };
                                                if raw_payload_cutoff
                                                    .is_some_and(|cutoff| completed_at <= cutoff)
                                                {
                                                    eligible_call_ids.insert(call.id);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            let approvals = if !phase_blocked
                                && (tool_payload_retention_mode
                                    == ToolPayloadRetentionMode::DeletingSession
                                    || !eligible_call_ids.is_empty())
                            {
                                tx.query::<AiApprovalRecord>()
                                    .filter(AiApprovalRecordWhereInput {
                                        session_id: Some(UuidFilter {
                                            eq: Some(session.id),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    })
                                    .default_order()
                                    .limit(approval_limit_with_lookahead)
                                    .fetch_all()
                                    .await
                                    .map_err(OrmPublicError::from)?
                            } else {
                                Vec::new()
                            };
                            let maximum_approvals = usize::try_from(approval_limit)
                                .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
                            if approvals.len() > maximum_approvals {
                                phase_blocked = true;
                            }
                            if !phase_blocked {
                                let calls_by_id = calls
                                    .iter()
                                    .map(|call| (call.id, call))
                                    .collect::<HashMap<_, _>>();
                                let approvals_by_id = approvals
                                    .iter()
                                    .map(|approval| (approval.id, approval))
                                    .collect::<HashMap<_, _>>();
                                let mut eligible_approval_ids = HashSet::new();
                                for call in &calls {
                                    if !eligible_call_ids.contains(&call.id) {
                                        continue;
                                    }
                                    let Some(step) = tx
                                        .find_by_id::<AiRunStepRecord>(&call.id)
                                        .await
                                        .map_err(OrmPublicError::from)?
                                    else {
                                        return Err(OrmPublicError::new(
                                            OrmErrorCode::InternalError,
                                        ));
                                    };
                                    validate_tool_step(&step, call)?;
                                    let approval = call
                                        .approval_id
                                        .and_then(|approval_id| approvals_by_id.get(&approval_id))
                                        .copied();
                                    if call.approval_id.is_some() != approval.is_some()
                                        || approval.is_some_and(|approval| {
                                            approval.tool_call_id != call.id
                                        })
                                        || !tool_approval_states_match(call, approval)
                                    {
                                        phase_blocked = true;
                                        break;
                                    }
                                    if let Some(approval) = approval {
                                        validate_approval(approval, session.id)?;
                                        if !approval_state_is_terminal(&approval.state) {
                                            phase_blocked = true;
                                            break;
                                        }
                                        eligible_approval_ids.insert(approval.id);
                                    }
                                    if call.payload_purged_at.is_some() {
                                        if call.protected_arguments.is_some()
                                            || call.protected_result.is_some()
                                            || approval.is_some_and(|approval| {
                                                approval.payload_purged_at.is_none()
                                                    || approval
                                                        .protected_resource_bindings
                                                        .is_some()
                                                    || approval.protected_action_preview.is_some()
                                            })
                                        {
                                            return Err(OrmPublicError::new(
                                                OrmErrorCode::InternalError,
                                            ));
                                        }
                                        continue;
                                    }
                                    if call.protected_arguments.is_none()
                                        || tool_call_result_required(&call.state)
                                            != call.protected_result.is_some()
                                        || approval.is_some_and(|approval| {
                                            approval.payload_purged_at.is_some()
                                                || approval.protected_resource_bindings.is_none()
                                                || approval.protected_action_preview.is_none()
                                        })
                                    {
                                        return Err(OrmPublicError::new(
                                            OrmErrorCode::InternalError,
                                        ));
                                    }
                                }
                                if !phase_blocked {
                                    for approval in &approvals {
                                        let approval_is_in_scope = tool_payload_retention_mode
                                            == ToolPayloadRetentionMode::DeletingSession
                                            || eligible_call_ids.contains(&approval.tool_call_id)
                                            || eligible_approval_ids.contains(&approval.id);
                                        if !approval_is_in_scope {
                                            continue;
                                        }
                                        validate_approval(approval, session.id)?;
                                        let call = calls_by_id.get(&approval.tool_call_id).copied();
                                        if call.is_none_or(|call| {
                                            !eligible_call_ids.contains(&call.id)
                                                || call.approval_id != Some(approval.id)
                                        }) || !approval_state_is_terminal(&approval.state)
                                        {
                                            phase_blocked = true;
                                            break;
                                        }
                                    }
                                }
                                if !phase_blocked {
                                    for approval in approvals {
                                        if !eligible_approval_ids.contains(&approval.id) {
                                            continue;
                                        }
                                        if approval.payload_purged_at.is_some() {
                                            continue;
                                        }
                                        let updated = tx
                                            .compare_and_swap::<AiApprovalRecord>(
                                                &approval.id,
                                                approval.row_version,
                                                AiApprovalRecordWhereInput {
                                                    session_id: Some(UuidFilter {
                                                        eq: Some(session.id),
                                                        ..Default::default()
                                                    }),
                                                    ..Default::default()
                                                },
                                                UpdateAiApprovalRecordInput {
                                                    protected_resource_bindings: Some(None),
                                                    protected_action_preview: Some(None),
                                                    payload_purged_at: Some(Some(now)),
                                                    ..Default::default()
                                                },
                                            )
                                            .await
                                            .map_err(OrmPublicError::from)?;
                                        if !matches!(updated, ConditionalUpdateOutcome::Updated(_))
                                        {
                                            return Err(OrmPublicError::new(
                                                OrmErrorCode::Conflict,
                                            ));
                                        }
                                        match tool_payload_retention_mode {
                                            ToolPayloadRetentionMode::DeletingSession => {
                                                deleting_approval_payloads_purged += 1;
                                            }
                                            ToolPayloadRetentionMode::ExpiredRaw => {
                                                expired_approval_payloads_purged += 1;
                                            }
                                        }
                                    }
                                    for call in calls {
                                        if !eligible_call_ids.contains(&call.id) {
                                            continue;
                                        }
                                        if call.payload_purged_at.is_some() {
                                            continue;
                                        }
                                        let updated = tx
                                            .compare_and_swap::<AiToolCallRecord>(
                                                &call.id,
                                                call.row_version,
                                                AiToolCallRecordWhereInput {
                                                    run_id: Some(UuidFilter {
                                                        in_list: Some(run_ids.clone()),
                                                        ..Default::default()
                                                    }),
                                                    ..Default::default()
                                                },
                                                UpdateAiToolCallRecordInput {
                                                    protected_arguments: Some(None),
                                                    protected_result: Some(None),
                                                    payload_purged_at: Some(Some(now)),
                                                    ..Default::default()
                                                },
                                            )
                                            .await
                                            .map_err(OrmPublicError::from)?;
                                        if !matches!(updated, ConditionalUpdateOutcome::Updated(_))
                                        {
                                            return Err(OrmPublicError::new(
                                                OrmErrorCode::Conflict,
                                            ));
                                        }
                                        match tool_payload_retention_mode {
                                            ToolPayloadRetentionMode::DeletingSession => {
                                                deleting_tool_payloads_purged += 1;
                                            }
                                            ToolPayloadRetentionMode::ExpiredRaw => {
                                                expired_tool_payloads_purged += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if phase_blocked {
                            match tool_payload_retention_mode {
                                ToolPayloadRetentionMode::DeletingSession => {
                                    tool_payload_purge_blocked = true;
                                }
                                ToolPayloadRetentionMode::ExpiredRaw => {
                                    raw_payload_purge_blocked = true;
                                }
                            }
                        }
                    }

                    let mut attachment_cleanups_requested = 0usize;
                    let mut attachments_deleted = 0usize;
                    let mut attachment_artifact_cleanups_requested = 0usize;
                    let mut attachment_artifacts_deleted = 0usize;
                    let mut attachment_cleanup_blocked = false;
                    if deletion_cutoff_reached
                        && context_checkpoint_ids.is_empty()
                        && proposal_payloads_purged == 0
                        && !proposal_payload_purge_blocked
                        && deleting_tool_payloads_purged == 0
                        && deleting_approval_payloads_purged == 0
                        && !tool_payload_purge_blocked
                    {
                        let attachments = tx
                            .query::<AiAttachmentRecord>()
                            .filter(AiAttachmentRecordWhereInput {
                                session_id: Some(UuidFilter {
                                    eq: Some(session.id),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .default_order()
                            .limit(attachment_limit_with_lookahead)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        if attachments.len()
                            > usize::try_from(attachment_limit)
                                .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?
                        {
                            attachment_cleanup_blocked = true;
                        } else {
                            let mut remaining_artifact_capacity =
                                usize::try_from(attachment_artifact_limit).map_err(|_| {
                                    OrmPublicError::new(OrmErrorCode::InternalError)
                                })?;
                            let mut attachments_with_artifacts =
                                Vec::with_capacity(attachments.len());
                            for attachment in attachments {
                                validate_attachment(&attachment, session.id)?;
                                let artifact_query_limit =
                                    i64::try_from(remaining_artifact_capacity)
                                        .map_err(|_| {
                                            OrmPublicError::new(OrmErrorCode::InternalError)
                                        })?
                                        .checked_add(1)
                                        .ok_or_else(|| {
                                            OrmPublicError::new(OrmErrorCode::InternalError)
                                        })?
                                        .min(attachment_artifact_limit_with_lookahead);
                                let artifacts = tx
                                    .query::<AiAttachmentArtifactRecord>()
                                    .filter(AiAttachmentArtifactRecordWhereInput {
                                        attachment_id: Some(UuidFilter {
                                            eq: Some(attachment.id),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    })
                                    .default_order()
                                    .limit(artifact_query_limit)
                                    .fetch_all()
                                    .await
                                    .map_err(OrmPublicError::from)?;
                                if artifacts.len() > remaining_artifact_capacity {
                                    attachment_cleanup_blocked = true;
                                    break;
                                }
                                remaining_artifact_capacity -= artifacts.len();
                                for artifact in &artifacts {
                                    validate_attachment_artifact(artifact, attachment.id)?;
                                }
                                attachments_with_artifacts.push((attachment, artifacts));
                            }
                            if !attachment_cleanup_blocked {
                                for (attachment, artifacts) in attachments_with_artifacts {
                                    let mut artifact_dependency_remains = false;
                                    for artifact in artifacts {
                                        if attachment_artifact_ready_for_metadata_delete(&artifact)
                                        {
                                            if !tx
                                                .delete_by_id::<AiAttachmentArtifactRecord>(
                                                    &artifact.id,
                                                )
                                                .await
                                                .map_err(OrmPublicError::from)?
                                            {
                                                return Err(OrmPublicError::new(
                                                    OrmErrorCode::InternalError,
                                                ));
                                            }
                                            attachment_artifacts_deleted += 1;
                                            continue;
                                        }
                                        artifact_dependency_remains = true;
                                        if attachment_artifact_cleanup_pending(&artifact) {
                                            attachment_cleanup_blocked = true;
                                            continue;
                                        }
                                        let updated = tx
                                            .compare_and_swap::<AiAttachmentArtifactRecord>(
                                                &artifact.id,
                                                artifact.row_version,
                                                AiAttachmentArtifactRecordWhereInput {
                                                    attachment_id: Some(UuidFilter {
                                                        eq: Some(attachment.id),
                                                        ..Default::default()
                                                    }),
                                                    ..Default::default()
                                                },
                                                UpdateAiAttachmentArtifactRecordInput {
                                                    cleanup_state: Some(Some(
                                                        "cleanup_required".to_owned(),
                                                    )),
                                                    cleanup_lease_expires_at: Some(None),
                                                    cleanup_next_attempt_at: Some(None),
                                                    ..Default::default()
                                                },
                                            )
                                            .await
                                            .map_err(OrmPublicError::from)?;
                                        if !matches!(updated, ConditionalUpdateOutcome::Updated(_))
                                        {
                                            return Err(OrmPublicError::new(
                                                OrmErrorCode::Conflict,
                                            ));
                                        }
                                        attachment_artifact_cleanups_requested += 1;
                                        attachment_cleanup_blocked = true;
                                    }
                                    if artifact_dependency_remains {
                                        continue;
                                    }
                                    if attachment_ready_for_metadata_delete(&attachment) {
                                        if !tx
                                            .delete_by_id::<AiAttachmentRecord>(&attachment.id)
                                            .await
                                            .map_err(OrmPublicError::from)?
                                        {
                                            return Err(OrmPublicError::new(
                                                OrmErrorCode::InternalError,
                                            ));
                                        }
                                        attachments_deleted += 1;
                                        continue;
                                    }
                                    if attachment_cleanup_pending(&attachment) {
                                        attachment_cleanup_blocked = true;
                                        continue;
                                    }
                                    let updated = tx
                                        .compare_and_swap::<AiAttachmentRecord>(
                                            &attachment.id,
                                            attachment.row_version,
                                            AiAttachmentRecordWhereInput {
                                                session_id: Some(UuidFilter {
                                                    eq: Some(session.id),
                                                    ..Default::default()
                                                }),
                                                ..Default::default()
                                            },
                                            UpdateAiAttachmentRecordInput {
                                                upload_token_hash: Some(None),
                                                upload_expires_at: Some(None),
                                                quarantine_state: Some("deleting".to_owned()),
                                                processing_state: Some(
                                                    "retention_cleanup_required".to_owned(),
                                                ),
                                                processing_expires_at: Some(None),
                                                cleanup_lease_expires_at: Some(None),
                                                cleanup_next_attempt_at: Some(None),
                                                ..Default::default()
                                            },
                                        )
                                        .await
                                        .map_err(OrmPublicError::from)?;
                                    if !matches!(updated, ConditionalUpdateOutcome::Updated(_)) {
                                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                                    }
                                    attachment_cleanups_requested += 1;
                                    attachment_cleanup_blocked = true;
                                }
                            }
                        }
                    }

                    let mut messages_purged = 0usize;
                    let mut blocks_deleted = 0usize;
                    let mut messages_blocked = 0usize;
                    let mut context_checkpoints_invalidated = 0usize;
                    let message_cutoff = if deletion_cutoff_reached
                        && (!inbox_events.is_empty()
                            || !context_checkpoint_ids.is_empty()
                            || proposal_payloads_purged > 0
                            || proposal_payload_purge_blocked
                            || deleting_tool_payloads_purged > 0
                            || deleting_approval_payloads_purged > 0
                            || tool_payload_purge_blocked)
                    {
                        None
                    } else if deletion_cutoff_reached {
                        Some(now)
                    } else {
                        policy
                            .message_retention_seconds
                            .map(|retention_seconds| {
                                now.checked_sub(retention_seconds)
                                    .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))
                            })
                            .transpose()?
                    };
                    if let Some(message_cutoff) = message_cutoff {
                        let messages = tx
                            .query::<AiMessageRecord>()
                            .filter(AiMessageRecordWhereInput {
                                session_id: Some(UuidFilter {
                                    eq: Some(session.id),
                                    ..Default::default()
                                }),
                                content_purged_at: Some(IntFilter {
                                    is_null: Some(true),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .default_order()
                            .limit(message_limit)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        let mut previous_message_sequence = None;
                        for message in messages {
                            validate_message(&message, &session, previous_message_sequence)?;
                            previous_message_sequence = Some(message.sequence);
                            let Some(finalized_at) = message.finalized_at else {
                                messages_blocked += 1;
                                continue;
                            };
                            if !deletion_cutoff_reached && finalized_at > message_cutoff {
                                continue;
                            }
                            let Some(run_id) = message.run_id else {
                                messages_blocked += 1;
                                continue;
                            };
                            let Some(run) = tx
                                .find_by_id::<AiRunRecord>(&run_id)
                                .await
                                .map_err(OrmPublicError::from)?
                            else {
                                return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                            };
                            let run_state = AiRunState::from_persisted(&run.state)
                                .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                            if run.session_id != session.id || !run_state.is_terminal() {
                                messages_blocked += 1;
                                continue;
                            }
                            let attachments = tx
                                .query::<AiAttachmentRecord>()
                                .filter(AiAttachmentRecordWhereInput {
                                    message_id: Some(UuidFilter {
                                        eq: Some(message.id),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                })
                                .limit(1)
                                .fetch_all()
                                .await
                                .map_err(OrmPublicError::from)?;
                            if !attachments.is_empty() {
                                messages_blocked += 1;
                                continue;
                            }
                            let block_limit = message
                                .block_count
                                .checked_add(1)
                                .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                            if usize::try_from(message.block_count).map_or(true, |count| {
                                count > maximum_blocks.saturating_sub(blocks_deleted)
                            }) {
                                messages_blocked += 1;
                                continue;
                            }
                            let blocks = tx
                                .query::<AiMessageBlockRecord>()
                                .filter(AiMessageBlockRecordWhereInput {
                                    message_id: Some(UuidFilter {
                                        eq: Some(message.id),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                })
                                .default_order()
                                .limit(block_limit)
                                .fetch_all()
                                .await
                                .map_err(OrmPublicError::from)?;
                            if i64::try_from(blocks.len()).ok() != Some(message.block_count)
                                || blocks.iter().enumerate().any(|(index, block)| {
                                    block.message_id != message.id
                                        || i64::try_from(index).ok() != Some(block.block_index)
                                })
                            {
                                return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                            }
                            let checkpoints = tx
                                .query::<AiContextCheckpointRecord>()
                                .filter(AiContextCheckpointRecordWhereInput {
                                    session_id: Some(UuidFilter {
                                        eq: Some(session.id),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                })
                                .default_order()
                                .limit(context_checkpoint_limit_with_lookahead)
                                .fetch_all()
                                .await
                                .map_err(OrmPublicError::from)?;
                            if checkpoints.len() > maximum_context_checkpoints {
                                messages_blocked += 1;
                                continue;
                            }
                            let mut invalidated_ids = Vec::new();
                            for checkpoint in checkpoints {
                                if checkpoint.id.is_nil()
                                    || checkpoint.session_id != session.id
                                    || checkpoint.through_sequence <= 0
                                    || checkpoint.through_sequence > session.message_head
                                    || checkpoint.source_hash.trim().is_empty()
                                    || checkpoint.source_hash.len() > 512
                                    || checkpoint.token_estimate < 0
                                    || checkpoint.provider_kind.trim().is_empty()
                                    || checkpoint.provider_model.trim().is_empty()
                                {
                                    return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                                }
                                if checkpoint.through_sequence >= message.sequence {
                                    invalidated_ids.push(checkpoint.id);
                                }
                            }
                            for checkpoint_id in invalidated_ids {
                                if !tx
                                    .delete_by_id::<AiContextCheckpointRecord>(&checkpoint_id)
                                    .await
                                    .map_err(OrmPublicError::from)?
                                {
                                    return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                                }
                                context_checkpoints_invalidated += 1;
                            }
                            let updated = tx
                                .compare_and_swap::<AiMessageRecord>(
                                    &message.id,
                                    message.row_version,
                                    AiMessageRecordWhereInput {
                                        session_id: Some(UuidFilter {
                                            eq: Some(session.id),
                                            ..Default::default()
                                        }),
                                        content_purged_at: Some(IntFilter {
                                            is_null: Some(true),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    },
                                    UpdateAiMessageRecordInput {
                                        protected_preview: Some(None),
                                        block_count: Some(0),
                                        content_purged_at: Some(Some(now)),
                                        ..Default::default()
                                    },
                                )
                                .await
                                .map_err(OrmPublicError::from)?;
                            if !matches!(updated, ConditionalUpdateOutcome::Updated(_)) {
                                return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                            }
                            for block in blocks {
                                if !tx
                                    .delete_by_id::<AiMessageBlockRecord>(&block.id)
                                    .await
                                    .map_err(OrmPublicError::from)?
                                {
                                    return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                                }
                                blocks_deleted += 1;
                            }
                            messages_purged += 1;
                        }
                    }

                    for event_id in &event_ids {
                        if !tx
                            .delete_by_id::<AiSessionEventRecord>(event_id)
                            .await
                            .map_err(OrmPublicError::from)?
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                    }
                    for event in &inbox_events {
                        let outcome = tx
                            .compare_and_swap::<AiInboxEventRecord>(
                                &event.id,
                                event.row_version,
                                AiInboxEventRecordWhereInput {
                                    session_id: Some(UuidFilter {
                                        eq: Some(session.id),
                                        ..Default::default()
                                    }),
                                    payload_purged_at: Some(IntFilter {
                                        is_null: Some(true),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                UpdateAiInboxEventRecordInput {
                                    protected_payload: Some(None),
                                    payload_purged_at: Some(Some(now)),
                                    ..Default::default()
                                },
                            )
                            .await
                            .map_err(OrmPublicError::from)?;
                        if !matches!(outcome, ConditionalUpdateOutcome::Updated(_)) {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    }
                    for checkpoint_id in &context_checkpoint_ids {
                        if !tx
                            .delete_by_id::<AiContextCheckpointRecord>(checkpoint_id)
                            .await
                            .map_err(OrmPublicError::from)?
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                    }

                    let mut run_checkpoint_references_cleared = 0usize;
                    let mut run_checkpoint_purge_ready = false;
                    let mut run_checkpoint_purge_blocked = false;
                    if deletion_cutoff_reached
                        && event_ids.is_empty()
                        && inbox_events.is_empty()
                        && context_checkpoint_ids.is_empty()
                        && proposal_payloads_purged == 0
                        && !proposal_payload_purge_blocked
                        && deleting_tool_payloads_purged == 0
                        && deleting_approval_payloads_purged == 0
                        && !tool_payload_purge_blocked
                        && attachment_cleanups_requested == 0
                        && attachments_deleted == 0
                        && !attachment_cleanup_blocked
                        && messages_purged == 0
                        && messages_blocked == 0
                    {
                        let remaining_events = tx
                            .query::<AiSessionEventRecord>()
                            .filter(AiSessionEventRecordWhereInput {
                                session_id: Some(UuidFilter {
                                    eq: Some(session.id),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .limit(1)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        let remaining_context = tx
                            .query::<AiContextCheckpointRecord>()
                            .filter(AiContextCheckpointRecordWhereInput {
                                session_id: Some(UuidFilter {
                                    eq: Some(session.id),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .limit(1)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        let remaining_inbox = tx
                            .query::<AiInboxEventRecord>()
                            .filter(AiInboxEventRecordWhereInput {
                                session_id: Some(UuidFilter {
                                    eq: Some(session.id),
                                    ..Default::default()
                                }),
                                payload_purged_at: Some(IntFilter {
                                    is_null: Some(true),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .limit(1)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        let remaining_proposals = tx
                            .query::<AiProposalRecord>()
                            .filter(AiProposalRecordWhereInput {
                                session_id: Some(UuidFilter {
                                    eq: Some(session.id),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .default_order()
                            .limit(proposal_limit_with_lookahead)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        let proposals_are_exhausted = remaining_proposals.len()
                            <= usize::try_from(proposal_limit)
                                .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?
                            && remaining_proposals.iter().all(|proposal| {
                                proposal.session_id == session.id
                                    && proposal.payload_purged_at.is_some()
                                    && proposal.protected_payload.is_none()
                                    && proposal.source_references.is_none()
                                    && proposal_state_is_terminal(proposal, now)
                            });
                        let remaining_message_content = tx
                            .query::<AiMessageRecord>()
                            .filter(AiMessageRecordWhereInput {
                                session_id: Some(UuidFilter {
                                    eq: Some(session.id),
                                    ..Default::default()
                                }),
                                content_purged_at: Some(IntFilter {
                                    is_null: Some(true),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .limit(1)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        let remaining_attachments = tx
                            .query::<AiAttachmentRecord>()
                            .filter(AiAttachmentRecordWhereInput {
                                session_id: Some(UuidFilter {
                                    eq: Some(session.id),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .limit(1)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        if remaining_events.is_empty()
                            && remaining_inbox.is_empty()
                            && remaining_context.is_empty()
                            && proposals_are_exhausted
                            && remaining_message_content.is_empty()
                            && remaining_attachments.is_empty()
                        {
                            let runs = tx
                                .query::<AiRunRecord>()
                                .filter(AiRunRecordWhereInput {
                                    session_id: Some(UuidFilter {
                                        eq: Some(session.id),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                })
                                .default_order()
                                .limit(run_limit_with_lookahead)
                                .fetch_all()
                                .await
                                .map_err(OrmPublicError::from)?;
                            let runs_are_bounded = runs.len()
                                <= usize::try_from(run_limit).map_err(|_| {
                                    OrmPublicError::new(OrmErrorCode::InternalError)
                                })?;
                            let runs_are_terminal = runs.iter().all(|run| {
                                run.id != Uuid::nil()
                                    && run.session_id == session.id
                                    && run.lease_generation >= 0
                                    && AiRunState::from_persisted(&run.state)
                                        .is_some_and(AiRunState::is_terminal)
                            });
                            if runs_are_bounded && runs_are_terminal {
                                for run in runs {
                                    let Some(checkpoint_id) = run.latest_checkpoint_id else {
                                        continue;
                                    };
                                    let checkpoints = tx
                                        .project::<AiRunCheckpointRetentionProjection>()
                                        .filter(AiRunCheckpointRecordWhereInput {
                                            id: Some(UuidFilter {
                                                eq: Some(checkpoint_id),
                                                ..Default::default()
                                            }),
                                            ..Default::default()
                                        })
                                        .limit(2)
                                        .fetch_all()
                                        .await
                                        .map_err(OrmPublicError::from)?;
                                    if checkpoints.len() != 1 {
                                        return Err(OrmPublicError::new(
                                            OrmErrorCode::InternalError,
                                        ));
                                    }
                                    let checkpoint = &checkpoints[0];
                                    if checkpoint.id != checkpoint_id
                                        || checkpoint.run_id != run.id
                                        || checkpoint.attempt_id.is_nil()
                                        || checkpoint.lease_generation != run.lease_generation
                                        || checkpoint.checkpoint_kind.trim().is_empty()
                                        || checkpoint.checkpoint_kind.len() > 128
                                        || checkpoint.checkpoint_hash.trim().is_empty()
                                        || checkpoint.checkpoint_hash.len() > 512
                                    {
                                        return Err(OrmPublicError::new(
                                            OrmErrorCode::InternalError,
                                        ));
                                    }
                                    let updated = tx
                                        .compare_and_swap::<AiRunRecord>(
                                            &run.id,
                                            run.row_version,
                                            AiRunRecordWhereInput {
                                                session_id: Some(UuidFilter {
                                                    eq: Some(session.id),
                                                    ..Default::default()
                                                }),
                                                ..Default::default()
                                            },
                                            UpdateAiRunRecordInput {
                                                latest_checkpoint_id: Some(None),
                                                ..Default::default()
                                            },
                                        )
                                        .await
                                        .map_err(OrmPublicError::from)?;
                                    if !matches!(updated, ConditionalUpdateOutcome::Updated(_)) {
                                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                                    }
                                    run_checkpoint_references_cleared += 1;
                                }
                                run_checkpoint_purge_ready = true;
                            } else {
                                run_checkpoint_purge_blocked = true;
                            }
                        }
                    }
                    if event_ids.is_empty()
                        && inbox_events.is_empty()
                        && context_checkpoint_ids.is_empty()
                        && proposal_payloads_purged == 0
                        && deleting_tool_payloads_purged == 0
                        && deleting_approval_payloads_purged == 0
                        && expired_tool_payloads_purged == 0
                        && expired_approval_payloads_purged == 0
                        && attachment_cleanups_requested == 0
                        && attachments_deleted == 0
                        && attachment_artifact_cleanups_requested == 0
                        && attachment_artifacts_deleted == 0
                        && context_checkpoints_invalidated == 0
                        && messages_purged == 0
                        && run_checkpoint_references_cleared == 0
                    {
                        return Ok(SessionPruneOutcome::Noop {
                            messages_blocked,
                            proposal_payload_purge_blocked,
                            tool_payload_purge_blocked,
                            raw_payload_purge_blocked,
                            attachment_cleanup_blocked,
                            run_checkpoint_purge_ready,
                            run_checkpoint_purge_blocked,
                        });
                    }
                    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                        actor_principal_kind: "system".to_owned(),
                        actor_subject: "session-retention".to_owned(),
                        action: "prune_session_content".to_owned(),
                        resource_kind: "ai_session".to_owned(),
                        resource_reference: session.id.to_string(),
                        outcome: "allowed".to_owned(),
                        reason_code: if deletion_cutoff_reached {
                            "session_deletion_retention_expired".to_owned()
                        } else {
                            "scope_retention_expired".to_owned()
                        },
                        correlation_id: Uuid::new_v4().to_string(),
                        causation_id: None,
                        policy_version: Some(format!("{}:{}", policy.id, policy.row_version)),
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    Ok(SessionPruneOutcome::Changed {
                        live_delta_events_deleted,
                        deleting_session_events_deleted,
                        deleting_session_inbox_payloads_purged,
                        deleting_session_context_checkpoints_deleted,
                        context_checkpoints_invalidated,
                        proposal_payloads_purged,
                        proposal_payload_purge_blocked,
                        deleting_tool_payloads_purged,
                        deleting_approval_payloads_purged,
                        expired_tool_payloads_purged,
                        expired_approval_payloads_purged,
                        tool_payload_purge_blocked,
                        raw_payload_purge_blocked,
                        attachment_cleanups_requested,
                        attachments_deleted,
                        attachment_artifact_cleanups_requested,
                        attachment_artifacts_deleted,
                        attachment_cleanup_blocked,
                        messages_purged,
                        blocks_deleted,
                        messages_blocked,
                        run_checkpoint_references_cleared,
                        run_checkpoint_purge_ready,
                        run_checkpoint_purge_blocked,
                    })
                })
            })
            .await;
        match result {
            Ok(outcome) => Ok(outcome),
            Err(error) if error.public_error().code == OrmErrorCode::Conflict => {
                Ok(SessionPruneOutcome::Conflict)
            }
            Err(error) => Err(map_transaction(error)),
        }
    }

    async fn finalize_deleted_session(&self, session_id: Uuid, now: i64) -> Result<bool, AiError> {
        let run_limit = i64::try_from(self.limits.maximum_runs_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid run limit".to_owned()))?;
        let run_limit_with_lookahead = run_limit
            .checked_add(1)
            .ok_or_else(|| AiError::InvalidConfiguration("invalid run limit".to_owned()))?;
        let message_limit = i64::try_from(self.limits.maximum_messages_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid message limit".to_owned()))?;
        let message_limit_with_lookahead = message_limit
            .checked_add(1)
            .ok_or_else(|| AiError::InvalidConfiguration("invalid message limit".to_owned()))?;
        let result = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&session_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    validate_session(&session)?;
                    if session.state != "deleting" {
                        return Ok(false);
                    }
                    let deleted_at = session
                        .deleted_at
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let scope = session_scope(&session);
                    let exact_scope_key = crate::ai_scope_key(&scope);
                    let policies = tx
                        .query::<AiRetentionPolicyRecord>()
                        .filter(AiRetentionPolicyRecordWhereInput {
                            scope_key: Some(StringFilter {
                                eq: Some(exact_scope_key.clone()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if policies.len() != 1 {
                        return Ok(false);
                    }
                    let policy = &policies[0];
                    if !valid_policy(policy, &scope, &exact_scope_key)
                        || deleted_at
                            .checked_add(policy.deleted_content_purge_seconds)
                            .is_none_or(|cutoff| cutoff > now)
                    {
                        return Ok(false);
                    }

                    let remaining_events = tx
                        .query::<AiSessionEventRecord>()
                        .filter(AiSessionEventRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(1)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let remaining_inbox = tx
                        .query::<AiInboxEventRecord>()
                        .filter(AiInboxEventRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            payload_purged_at: Some(IntFilter {
                                is_null: Some(true),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(1)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let remaining_context = tx
                        .query::<AiContextCheckpointRecord>()
                        .filter(AiContextCheckpointRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(1)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let remaining_message_content = tx
                        .query::<AiMessageRecord>()
                        .filter(AiMessageRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            content_purged_at: Some(IntFilter {
                                is_null: Some(true),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(1)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let remaining_attachments = tx
                        .query::<AiAttachmentRecord>()
                        .filter(AiAttachmentRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(1)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !remaining_events.is_empty()
                        || !remaining_inbox.is_empty()
                        || !remaining_context.is_empty()
                        || !remaining_message_content.is_empty()
                        || !remaining_attachments.is_empty()
                    {
                        return Ok(false);
                    }

                    let messages = tx
                        .query::<AiMessageRecord>()
                        .filter(AiMessageRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .default_order()
                        .limit(message_limit_with_lookahead)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if messages.len()
                        > usize::try_from(message_limit)
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?
                        || i64::try_from(messages.len()).ok() != Some(session.message_head)
                    {
                        return Ok(false);
                    }
                    for (index, message) in messages.iter().enumerate() {
                        let expected_sequence = i64::try_from(index)
                            .ok()
                            .and_then(|index| index.checked_add(1));
                        if message.id.is_nil()
                            || message.session_id != session_id
                            || Some(message.sequence) != expected_sequence
                            || message.protected_preview.is_some()
                            || message.block_count != 0
                            || message.content_purged_at.is_none()
                            || message.completion_state != "complete"
                            || message.finalized_at.is_none()
                        {
                            return Ok(false);
                        }
                        let blocks = tx
                            .query::<AiMessageBlockRecord>()
                            .filter(AiMessageBlockRecordWhereInput {
                                message_id: Some(UuidFilter {
                                    eq: Some(message.id),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .limit(1)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        if !blocks.is_empty() {
                            return Ok(false);
                        }
                    }

                    let runs = tx
                        .query::<AiRunRecord>()
                        .filter(AiRunRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .default_order()
                        .limit(run_limit_with_lookahead)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if runs.len()
                        > usize::try_from(run_limit)
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?
                        || runs.iter().any(|run| {
                            run.id.is_nil()
                                || run.session_id != session_id
                                || run.lease_generation < 0
                                || run.latest_checkpoint_id.is_some()
                                || !AiRunState::from_persisted(&run.state)
                                    .is_some_and(AiRunState::is_terminal)
                        })
                    {
                        return Ok(false);
                    }
                    let run_ids = runs.iter().map(|run| run.id).collect::<Vec<_>>();
                    if !run_ids.is_empty() {
                        let remaining_checkpoints = tx
                            .project::<AiRunCheckpointRetentionProjection>()
                            .filter(AiRunCheckpointRecordWhereInput {
                                run_id: Some(UuidFilter {
                                    in_list: Some(run_ids),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .limit(1)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        if !remaining_checkpoints.is_empty() {
                            return Ok(false);
                        }
                    }
                    if session.title.trim().is_empty() {
                        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                    }
                    let outcome = tx
                        .compare_and_swap::<AiSessionRecord>(
                            &session.id,
                            session.row_version,
                            AiSessionRecordWhereInput {
                                state: Some(StringFilter {
                                    eq: Some("deleting".to_owned()),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            },
                            UpdateAiSessionRecordInput {
                                title: Some(String::new()),
                                state: Some("deleted".to_owned()),
                                last_activity_at: Some(now),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(outcome, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                        actor_principal_kind: "system".to_owned(),
                        actor_subject: "session-retention".to_owned(),
                        action: "finalize_session_deletion".to_owned(),
                        resource_kind: "ai_session".to_owned(),
                        resource_reference: session_id.to_string(),
                        outcome: "allowed".to_owned(),
                        reason_code: "session_content_dependencies_exhausted".to_owned(),
                        correlation_id: Uuid::new_v4().to_string(),
                        causation_id: None,
                        policy_version: Some(format!("{}:{}", policy.id, policy.row_version)),
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    Ok(true)
                })
            })
            .await;
        match result {
            Ok(finalized) => Ok(finalized),
            Err(error) if error.public_error().code == OrmErrorCode::Conflict => Ok(false),
            Err(error) => Err(map_transaction(error)),
        }
    }

    async fn purge_run_checkpoints(
        &self,
        session_id: Uuid,
        now: i64,
    ) -> Result<DeletingRunCheckpointPurgeOutcome, AiError> {
        let proposal_limit = i64::try_from(self.limits.maximum_proposals_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid proposal limit".to_owned()))?;
        let proposal_limit_with_lookahead = proposal_limit
            .checked_add(1)
            .ok_or_else(|| AiError::InvalidConfiguration("invalid proposal limit".to_owned()))?;
        let proposal_item_limit = i64::try_from(self.limits.maximum_proposal_items_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid proposal-item limit".to_owned()))?;
        let proposal_item_limit_with_lookahead =
            proposal_item_limit.checked_add(1).ok_or_else(|| {
                AiError::InvalidConfiguration("invalid proposal-item limit".to_owned())
            })?;
        let tool_call_limit = i64::try_from(self.limits.maximum_tool_calls_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid tool-call limit".to_owned()))?;
        let tool_call_limit_with_lookahead = tool_call_limit
            .checked_add(1)
            .ok_or_else(|| AiError::InvalidConfiguration("invalid tool-call limit".to_owned()))?;
        let approval_limit = i64::try_from(self.limits.maximum_approvals_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid approval limit".to_owned()))?;
        let approval_limit_with_lookahead = approval_limit
            .checked_add(1)
            .ok_or_else(|| AiError::InvalidConfiguration("invalid approval limit".to_owned()))?;
        let run_limit = i64::try_from(self.limits.maximum_runs_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid run limit".to_owned()))?;
        let run_limit_with_lookahead = run_limit
            .checked_add(1)
            .ok_or_else(|| AiError::InvalidConfiguration("invalid run limit".to_owned()))?;
        let checkpoint_limit = i64::try_from(self.limits.maximum_run_checkpoints_per_session)
            .map_err(|_| {
                AiError::InvalidConfiguration("invalid run checkpoint limit".to_owned())
            })?;
        self.database
            .retention_transaction(move |maintenance| {
                Box::pin(async move {
                    let session = maintenance
                        .query::<AiSessionRecord>()
                        .filter(AiSessionRecordWhereInput {
                            id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if session.len() != 1 {
                        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                    }
                    let session = &session[0];
                    validate_session(session)?;
                    if session.state != "deleting" {
                        return Ok(DeletingRunCheckpointPurgeOutcome::Blocked);
                    }
                    let scope = session_scope(session);
                    let exact_scope_key = crate::ai_scope_key(&scope);
                    let policies = maintenance
                        .query::<AiRetentionPolicyRecord>()
                        .filter(AiRetentionPolicyRecordWhereInput {
                            scope_key: Some(StringFilter {
                                eq: Some(exact_scope_key.clone()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if policies.len() > 1 {
                        return Err(OrmPublicError::new(
                            OrmErrorCode::AuthorizationMisconfigured,
                        ));
                    }
                    let Some(policy) = policies.into_iter().next() else {
                        return Ok(DeletingRunCheckpointPurgeOutcome::Blocked);
                    };
                    if !valid_policy(&policy, &scope, &exact_scope_key) {
                        return Ok(DeletingRunCheckpointPurgeOutcome::Blocked);
                    }
                    let deleted_at = session
                        .deleted_at
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let cutoff = deleted_at
                        .checked_add(policy.deleted_content_purge_seconds)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    if cutoff > now {
                        return Ok(DeletingRunCheckpointPurgeOutcome::Blocked);
                    }

                    let remaining_events = maintenance
                        .query::<AiSessionEventRecord>()
                        .filter(AiSessionEventRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(1)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let remaining_context = maintenance
                        .query::<AiContextCheckpointRecord>()
                        .filter(AiContextCheckpointRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(1)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let remaining_proposals = maintenance
                        .query::<AiProposalRecord>()
                        .filter(AiProposalRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .default_order()
                        .limit(proposal_limit_with_lookahead)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if remaining_proposals.len()
                        > usize::try_from(proposal_limit)
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?
                    {
                        return Ok(DeletingRunCheckpointPurgeOutcome::Blocked);
                    }
                    let maximum_proposal_items = usize::try_from(proposal_item_limit)
                        .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let mut proposal_item_count = 0usize;
                    for proposal in remaining_proposals {
                        validate_proposal(&proposal, session)?;
                        if proposal.payload_purged_at.is_none()
                            || proposal.protected_payload.is_some()
                            || proposal.source_references.is_some()
                            || !proposal_state_is_terminal(&proposal, now)
                        {
                            return Ok(DeletingRunCheckpointPurgeOutcome::Blocked);
                        }
                        let proposal_runs = maintenance
                            .query::<AiRunRecord>()
                            .filter(AiRunRecordWhereInput {
                                id: Some(UuidFilter {
                                    eq: Some(proposal.run_id),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .limit(2)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        if proposal_runs.len() != 1 {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                        let proposal_run = &proposal_runs[0];
                        if proposal_run.session_id != session_id
                            || !AiRunState::from_persisted(&proposal_run.state)
                                .is_some_and(AiRunState::is_terminal)
                        {
                            return Ok(DeletingRunCheckpointPurgeOutcome::Blocked);
                        }
                        let items = maintenance
                            .query::<AiProposalItemRecord>()
                            .filter(AiProposalItemRecordWhereInput {
                                proposal_id: Some(UuidFilter {
                                    eq: Some(proposal.id),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .default_order()
                            .limit(proposal_item_limit_with_lookahead)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        proposal_item_count = proposal_item_count
                            .checked_add(items.len())
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                        if items.len() > maximum_proposal_items
                            || proposal_item_count > maximum_proposal_items
                        {
                            return Ok(DeletingRunCheckpointPurgeOutcome::Blocked);
                        }
                        for (index, item) in items.iter().enumerate() {
                            validate_proposal_item(item, proposal.id, index)?;
                            if item.protected_suggested_value.is_some()
                                || item.protected_rationale.is_some()
                                || item.source_references.is_some()
                                || item.protected_review_value.is_some()
                            {
                                return Ok(DeletingRunCheckpointPurgeOutcome::Blocked);
                            }
                        }
                    }
                    let remaining_message_content = maintenance
                        .query::<AiMessageRecord>()
                        .filter(AiMessageRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            content_purged_at: Some(IntFilter {
                                is_null: Some(true),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(1)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let remaining_attachments = maintenance
                        .query::<AiAttachmentRecord>()
                        .filter(AiAttachmentRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(1)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !remaining_events.is_empty()
                        || !remaining_context.is_empty()
                        || !remaining_message_content.is_empty()
                        || !remaining_attachments.is_empty()
                    {
                        return Ok(DeletingRunCheckpointPurgeOutcome::Blocked);
                    }

                    let runs = maintenance
                        .query::<AiRunRecord>()
                        .filter(AiRunRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .default_order()
                        .limit(run_limit_with_lookahead)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if runs.len()
                        > usize::try_from(run_limit)
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?
                    {
                        return Ok(DeletingRunCheckpointPurgeOutcome::Blocked);
                    }
                    let mut run_fences = HashMap::with_capacity(runs.len());
                    for run in runs {
                        let state = AiRunState::from_persisted(&run.state)
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                        if run.id.is_nil()
                            || run.session_id != session_id
                            || run.lease_generation < 0
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                        if !state.is_terminal() || run.latest_checkpoint_id.is_some() {
                            return Ok(DeletingRunCheckpointPurgeOutcome::Blocked);
                        }
                        run_fences.insert(run.id, run.lease_generation);
                    }
                    let run_ids = run_fences.keys().copied().collect::<Vec<_>>();
                    let calls = if run_ids.is_empty() {
                        Vec::new()
                    } else {
                        maintenance
                            .query::<AiToolCallRecord>()
                            .filter(AiToolCallRecordWhereInput {
                                run_id: Some(UuidFilter {
                                    in_list: Some(run_ids.clone()),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .default_order()
                            .limit(tool_call_limit_with_lookahead)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?
                    };
                    let approvals = maintenance
                        .query::<AiApprovalRecord>()
                        .filter(AiApprovalRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .default_order()
                        .limit(approval_limit_with_lookahead)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if calls.len()
                        > usize::try_from(tool_call_limit)
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?
                        || approvals.len()
                            > usize::try_from(approval_limit)
                                .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?
                    {
                        return Ok(DeletingRunCheckpointPurgeOutcome::Blocked);
                    }
                    let calls_by_id = calls
                        .iter()
                        .map(|call| (call.id, call))
                        .collect::<HashMap<_, _>>();
                    let approvals_by_id = approvals
                        .iter()
                        .map(|approval| (approval.id, approval))
                        .collect::<HashMap<_, _>>();
                    for call in &calls {
                        validate_tool_call(call, &run_ids)?;
                        if !tool_call_state_is_terminal(&call.state)
                            || call.payload_purged_at.is_none()
                            || call.protected_arguments.is_some()
                            || call.protected_result.is_some()
                        {
                            return Ok(DeletingRunCheckpointPurgeOutcome::Blocked);
                        }
                        let steps = maintenance
                            .query::<AiRunStepRecord>()
                            .filter(AiRunStepRecordWhereInput {
                                id: Some(UuidFilter {
                                    eq: Some(call.id),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .limit(2)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        if steps.len() != 1 {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                        validate_tool_step(&steps[0], call)?;
                        let approval = call
                            .approval_id
                            .and_then(|approval_id| approvals_by_id.get(&approval_id))
                            .copied();
                        if call.approval_id.is_some() != approval.is_some()
                            || !tool_approval_states_match(call, approval)
                            || approval.is_some_and(|approval| {
                                approval.payload_purged_at.is_none()
                                    || approval.protected_resource_bindings.is_some()
                                    || approval.protected_action_preview.is_some()
                            })
                        {
                            return Ok(DeletingRunCheckpointPurgeOutcome::Blocked);
                        }
                    }
                    for approval in &approvals {
                        validate_approval(approval, session_id)?;
                        let Some(call) = calls_by_id.get(&approval.tool_call_id) else {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        };
                        if call.approval_id != Some(approval.id)
                            || !approval_state_is_terminal(&approval.state)
                            || approval.payload_purged_at.is_none()
                            || approval.protected_resource_bindings.is_some()
                            || approval.protected_action_preview.is_some()
                        {
                            return Ok(DeletingRunCheckpointPurgeOutcome::Blocked);
                        }
                    }
                    if run_fences.is_empty() {
                        return Ok(DeletingRunCheckpointPurgeOutcome::Verified { deleted: 0 });
                    }

                    let checkpoints = maintenance
                        .project::<AiRunCheckpointRetentionProjection>()
                        .filter(AiRunCheckpointRecordWhereInput {
                            run_id: Some(UuidFilter {
                                in_list: Some(run_fences.keys().copied().collect()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .order_by(AiRunCheckpointRecordOrderByInput {
                            created_at: Some(OrderDirection::Asc),
                            id: Some(OrderDirection::Asc),
                        })
                        .limit(checkpoint_limit)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let mut checkpoint_ids = Vec::with_capacity(checkpoints.len());
                    for checkpoint in checkpoints {
                        if checkpoint.id.is_nil()
                            || run_fences.get(&checkpoint.run_id)
                                != Some(&checkpoint.lease_generation)
                            || checkpoint.attempt_id.is_nil()
                            || checkpoint.checkpoint_kind.trim().is_empty()
                            || checkpoint.checkpoint_kind.len() > 128
                            || checkpoint.checkpoint_hash.trim().is_empty()
                            || checkpoint.checkpoint_hash.len() > 512
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                        checkpoint_ids.push(checkpoint.id);
                    }
                    if checkpoint_ids.is_empty() {
                        return Ok(DeletingRunCheckpointPurgeOutcome::Verified { deleted: 0 });
                    }
                    let maximum = u32::try_from(checkpoint_ids.len())
                        .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let outcome = maintenance
                        .purge::<AiRunCheckpointRecord>(
                            AiRunCheckpointRecordWhereInput {
                                id: Some(UuidFilter {
                                    in_list: Some(checkpoint_ids),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            },
                            MutationLimit::new(maximum)?,
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    let RetentionPurgeOutcome::Purged { affected } = outcome else {
                        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                    };
                    if affected != maximum {
                        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                    }
                    maintenance
                        .insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                            actor_principal_kind: "system".to_owned(),
                            actor_subject: "session-retention".to_owned(),
                            action: "purge_session_run_checkpoints".to_owned(),
                            resource_kind: "ai_session".to_owned(),
                            resource_reference: session_id.to_string(),
                            outcome: "allowed".to_owned(),
                            reason_code: "session_deletion_retention_expired".to_owned(),
                            correlation_id: Uuid::new_v4().to_string(),
                            causation_id: None,
                            policy_version: Some(format!("{}:{}", policy.id, policy.row_version)),
                        })
                        .await
                        .map_err(OrmPublicError::from)?;
                    Ok(DeletingRunCheckpointPurgeOutcome::Verified {
                        deleted: usize::try_from(affected)
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?,
                    })
                })
            })
            .await
            .map_err(map_transaction)
    }

    async fn purge_expired_run_checkpoints(
        &self,
        session_id: Uuid,
        now: i64,
    ) -> Result<RawCheckpointPurgeOutcome, AiError> {
        let run_limit = i64::try_from(self.limits.maximum_runs_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid run limit".to_owned()))?;
        let run_limit_with_lookahead = run_limit
            .checked_add(1)
            .ok_or_else(|| AiError::InvalidConfiguration("invalid run limit".to_owned()))?;
        let tool_call_limit = i64::try_from(self.limits.maximum_tool_calls_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid tool-call limit".to_owned()))?;
        let tool_call_limit_with_lookahead = tool_call_limit
            .checked_add(1)
            .ok_or_else(|| AiError::InvalidConfiguration("invalid tool-call limit".to_owned()))?;
        let approval_limit = i64::try_from(self.limits.maximum_approvals_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid approval limit".to_owned()))?;
        let approval_limit_with_lookahead = approval_limit
            .checked_add(1)
            .ok_or_else(|| AiError::InvalidConfiguration("invalid approval limit".to_owned()))?;
        let checkpoint_limit = i64::try_from(self.limits.maximum_run_checkpoints_per_session)
            .map_err(|_| {
                AiError::InvalidConfiguration("invalid run checkpoint limit".to_owned())
            })?;
        let checkpoint_scan_limit = checkpoint_limit.checked_add(run_limit).ok_or_else(|| {
            AiError::InvalidConfiguration("invalid run checkpoint scan limit".to_owned())
        })?;
        self.database
            .retention_transaction(move |maintenance| {
                Box::pin(async move {
                    let sessions = maintenance
                        .query::<AiSessionRecord>()
                        .filter(AiSessionRecordWhereInput {
                            id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if sessions.len() != 1 {
                        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                    }
                    let session = &sessions[0];
                    validate_session(session)?;
                    let scope = session_scope(session);
                    let exact_scope_key = crate::ai_scope_key(&scope);
                    let policies = maintenance
                        .query::<AiRetentionPolicyRecord>()
                        .filter(AiRetentionPolicyRecordWhereInput {
                            scope_key: Some(StringFilter {
                                eq: Some(exact_scope_key.clone()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if policies.len() > 1 {
                        return Err(OrmPublicError::new(
                            OrmErrorCode::AuthorizationMisconfigured,
                        ));
                    }
                    let Some(policy) = policies.into_iter().next() else {
                        return Ok(RawCheckpointPurgeOutcome::NotApplicable);
                    };
                    if !valid_policy(&policy, &scope, &exact_scope_key) {
                        return Ok(RawCheckpointPurgeOutcome::NotApplicable);
                    }
                    if session.state == "deleting" {
                        let deleted_at = session
                            .deleted_at
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                        let deletion_cutoff = deleted_at
                            .checked_add(policy.deleted_content_purge_seconds)
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                        if deletion_cutoff <= now {
                            return Ok(RawCheckpointPurgeOutcome::NotApplicable);
                        }
                    }
                    let raw_cutoff = now
                        .checked_sub(policy.raw_payload_retention_seconds)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;

                    let runs = maintenance
                        .query::<AiRunRecord>()
                        .filter(AiRunRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .default_order()
                        .limit(run_limit_with_lookahead)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if runs.len()
                        > usize::try_from(run_limit)
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?
                    {
                        return Ok(RawCheckpointPurgeOutcome::Blocked);
                    }
                    let mut terminal_runs = HashMap::new();
                    let mut current_checkpoint_ids = HashSet::new();
                    for run in runs {
                        let state = AiRunState::from_persisted(&run.state)
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                        if run.id.is_nil()
                            || run.session_id != session_id
                            || run.lease_generation < 0
                            || run.latest_checkpoint_id.is_some_and(|id| id.is_nil())
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                        if let Some(checkpoint_id) = run.latest_checkpoint_id {
                            current_checkpoint_ids.insert(checkpoint_id);
                            let current_checkpoints = maintenance
                                .project::<AiRunCheckpointRetentionProjection>()
                                .filter(AiRunCheckpointRecordWhereInput {
                                    id: Some(UuidFilter {
                                        eq: Some(checkpoint_id),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                })
                                .limit(2)
                                .fetch_all()
                                .await
                                .map_err(OrmPublicError::from)?;
                            if current_checkpoints.len() != 1 {
                                return Ok(RawCheckpointPurgeOutcome::Blocked);
                            }
                            let current = &current_checkpoints[0];
                            let current_kind_is_protected = PROTECTED_RUN_CHECKPOINT_KINDS
                                .contains(&current.checkpoint_kind.as_str());
                            let current_kind_is_final =
                                current.checkpoint_kind == "assistant_output_persisted";
                            let current_shape_is_valid = if current_kind_is_final {
                                current.assistant_message_id == Some(current.id)
                            } else {
                                current_kind_is_protected && current.assistant_message_id.is_none()
                            };
                            if current.id != checkpoint_id
                                || current.run_id != run.id
                                || current.attempt_id.is_nil()
                                || run.attempt_id != Some(current.attempt_id)
                                || current.lease_generation != run.lease_generation
                                || current.budget_reservation_id.is_none()
                                || current.checkpoint_hash.len() != 64
                                || !current_shape_is_valid
                            {
                                return Ok(RawCheckpointPurgeOutcome::Blocked);
                            }
                        }
                        if state.is_terminal() {
                            terminal_runs.insert(run.id, run);
                        }
                    }
                    if terminal_runs.is_empty() {
                        return Ok(RawCheckpointPurgeOutcome::Noop);
                    }

                    let checkpoints = maintenance
                        .project::<AiRunCheckpointRetentionProjection>()
                        .filter(AiRunCheckpointRecordWhereInput {
                            run_id: Some(UuidFilter {
                                in_list: Some(terminal_runs.keys().copied().collect()),
                                ..Default::default()
                            }),
                            checkpoint_kind: Some(StringFilter {
                                in_list: Some(
                                    PROTECTED_RUN_CHECKPOINT_KINDS
                                        .iter()
                                        .map(ToString::to_string)
                                        .collect(),
                                ),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .order_by(AiRunCheckpointRecordOrderByInput {
                            created_at: Some(OrderDirection::Asc),
                            id: Some(OrderDirection::Asc),
                        })
                        .limit(checkpoint_scan_limit)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let maximum_checkpoints = usize::try_from(checkpoint_limit)
                        .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let mut eligible = Vec::with_capacity(maximum_checkpoints);
                    for checkpoint in checkpoints {
                        if checkpoint.created_at > raw_cutoff {
                            break;
                        }
                        if current_checkpoint_ids.contains(&checkpoint.id) {
                            continue;
                        }
                        eligible.push(checkpoint);
                        if eligible.len() == maximum_checkpoints {
                            break;
                        }
                    }
                    if eligible.is_empty() {
                        return Ok(RawCheckpointPurgeOutcome::Noop);
                    }

                    let eligible_run_ids = eligible
                        .iter()
                        .map(|checkpoint| checkpoint.run_id)
                        .collect::<HashSet<_>>();
                    let calls = maintenance
                        .query::<AiToolCallRecord>()
                        .filter(AiToolCallRecordWhereInput {
                            run_id: Some(UuidFilter {
                                in_list: Some(eligible_run_ids.iter().copied().collect()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .default_order()
                        .limit(tool_call_limit_with_lookahead)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if calls.len()
                        > usize::try_from(tool_call_limit)
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?
                    {
                        return Ok(RawCheckpointPurgeOutcome::Blocked);
                    }
                    let approvals = if calls.is_empty() {
                        Vec::new()
                    } else {
                        maintenance
                            .query::<AiApprovalRecord>()
                            .filter(AiApprovalRecordWhereInput {
                                session_id: Some(UuidFilter {
                                    eq: Some(session_id),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .default_order()
                            .limit(approval_limit_with_lookahead)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?
                    };
                    if approvals.len()
                        > usize::try_from(approval_limit)
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?
                    {
                        return Ok(RawCheckpointPurgeOutcome::Blocked);
                    }
                    let calls_by_id = calls
                        .iter()
                        .map(|call| (call.id, call))
                        .collect::<HashMap<_, _>>();
                    let approvals_by_id = approvals
                        .iter()
                        .map(|approval| (approval.id, approval))
                        .collect::<HashMap<_, _>>();
                    let eligible_run_ids = eligible_run_ids.into_iter().collect::<Vec<_>>();
                    for call in &calls {
                        validate_tool_call(call, &eligible_run_ids)?;
                    }

                    for checkpoint in &eligible {
                        let Some(run) = terminal_runs.get(&checkpoint.run_id) else {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        };
                        if checkpoint.id.is_nil()
                            || checkpoint.attempt_id.is_nil()
                            || checkpoint.lease_generation <= 0
                            || checkpoint.lease_generation > run.lease_generation
                            || !PROTECTED_RUN_CHECKPOINT_KINDS
                                .contains(&checkpoint.checkpoint_kind.as_str())
                            || checkpoint
                                .provider_response_id
                                .as_ref()
                                .is_some_and(|value| {
                                    value.trim().is_empty()
                                        || value.len() > 1_024
                                        || value.chars().any(char::is_control)
                                })
                            || checkpoint.budget_reservation_id.is_none()
                            || checkpoint.assistant_message_id.is_some()
                            || checkpoint.checkpoint_hash.len() != 64
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                        let attempts = maintenance
                            .query::<AiRunAttemptRecord>()
                            .filter(AiRunAttemptRecordWhereInput {
                                id: Some(UuidFilter {
                                    eq: Some(checkpoint.attempt_id),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .limit(2)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        if attempts.len() != 1 {
                            return Ok(RawCheckpointPurgeOutcome::Blocked);
                        }
                        let attempt = &attempts[0];
                        if attempt.id != checkpoint.attempt_id
                            || attempt.run_id != checkpoint.run_id
                            || attempt.lease_generation != checkpoint.lease_generation
                            || attempt.worker_id.trim().is_empty()
                            || attempt.worker_id.len() > 256
                            || attempt.worker_id.chars().any(char::is_control)
                            || attempt.claimed_at > checkpoint.created_at
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                        let outcomes = maintenance
                            .query::<AiRunAttemptOutcomeRecord>()
                            .filter(AiRunAttemptOutcomeRecordWhereInput {
                                attempt_id: Some(UuidFilter {
                                    eq: Some(checkpoint.attempt_id),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .limit(2)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        if outcomes.len() != 1 {
                            return Ok(RawCheckpointPurgeOutcome::Blocked);
                        }
                        let outcome = &outcomes[0];
                        let outcome_state = AiRunState::from_persisted(&outcome.final_state)
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                        if outcome.attempt_id != attempt.id
                            || outcome.run_id != attempt.run_id
                            || outcome.lease_generation != attempt.lease_generation
                            || outcome.worker_id != attempt.worker_id
                            || outcome.outcome_code.trim().is_empty()
                            || outcome.outcome_code.len() > 200
                            || outcome.outcome_code.chars().any(char::is_control)
                            || outcome.finished_at < attempt.claimed_at
                            || outcome.finished_at < checkpoint.created_at
                            || !matches!(
                                outcome_state,
                                AiRunState::Queued
                                    | AiRunState::RetryScheduled
                                    | AiRunState::RecoveryRequired
                                    | AiRunState::Completed
                                    | AiRunState::Failed
                                    | AiRunState::Cancelled
                            )
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                        let budget_id = checkpoint
                            .budget_reservation_id
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                        let budgets = maintenance
                            .query::<AiBudgetReservationRecord>()
                            .filter(AiBudgetReservationRecordWhereInput {
                                id: Some(UuidFilter {
                                    eq: Some(budget_id),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .limit(2)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)?;
                        if budgets.len() != 1 {
                            return Ok(RawCheckpointPurgeOutcome::Blocked);
                        }
                        let budget = &budgets[0];
                        if budget.session_id != session_id
                            || budget.run_id != checkpoint.run_id
                            || budget.attempt_id != checkpoint.attempt_id
                            || budget.lease_generation != checkpoint.lease_generation
                            || budget.provider_kind.trim().is_empty()
                            || budget.provider_model.trim().is_empty()
                            || budget.state != "committed"
                            || budget.actual_runs != Some(1)
                            || budget
                                .reconciled_at
                                .is_none_or(|at| at > checkpoint.created_at)
                            || budget.created_at > checkpoint.created_at
                        {
                            return Ok(RawCheckpointPurgeOutcome::Blocked);
                        }

                        let relevant_calls = calls
                            .iter()
                            .filter(|call| {
                                call.run_id == checkpoint.run_id
                                    && call.lease_generation == checkpoint.lease_generation
                                    && call.provider_response_id == checkpoint.provider_response_id
                                    && call.budget_reservation_id == Some(budget_id)
                            })
                            .collect::<Vec<_>>();
                        if checkpoint.checkpoint_kind == "provider_turn_persisted"
                            && relevant_calls.is_empty()
                        {
                            let final_checkpoints = maintenance
                                .project::<AiRunCheckpointRetentionProjection>()
                                .filter(AiRunCheckpointRecordWhereInput {
                                    run_id: Some(UuidFilter {
                                        eq: Some(checkpoint.run_id),
                                        ..Default::default()
                                    }),
                                    checkpoint_kind: Some(StringFilter {
                                        eq: Some("assistant_output_persisted".to_owned()),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                })
                                .limit(2)
                                .fetch_all()
                                .await
                                .map_err(OrmPublicError::from)?;
                            if final_checkpoints.len() != 1 {
                                return Ok(RawCheckpointPurgeOutcome::Blocked);
                            }
                            let final_checkpoint = &final_checkpoints[0];
                            let Some(message_id) = final_checkpoint.assistant_message_id else {
                                return Ok(RawCheckpointPurgeOutcome::Blocked);
                            };
                            let expected_final_hash = crate::orm_runs::final_output_checkpoint_hash(
                                crate::AiRunId(final_checkpoint.run_id),
                                final_checkpoint.attempt_id,
                                final_checkpoint.lease_generation,
                                message_id,
                                final_checkpoint.provider_response_id.as_deref(),
                                budget_id,
                            );
                            if run.latest_checkpoint_id != Some(final_checkpoint.id)
                                || final_checkpoint.run_id != checkpoint.run_id
                                || final_checkpoint.attempt_id != checkpoint.attempt_id
                                || final_checkpoint.lease_generation != checkpoint.lease_generation
                                || final_checkpoint.provider_response_id
                                    != checkpoint.provider_response_id
                                || final_checkpoint.budget_reservation_id != Some(budget_id)
                                || final_checkpoint.created_at < checkpoint.created_at
                                || final_checkpoint.created_at > outcome.finished_at
                                || final_checkpoint.checkpoint_hash != expected_final_hash
                            {
                                return Ok(RawCheckpointPurgeOutcome::Blocked);
                            }
                            let messages = maintenance
                                .query::<AiMessageRecord>()
                                .filter(AiMessageRecordWhereInput {
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
                            if messages.len() != 1 {
                                return Ok(RawCheckpointPurgeOutcome::Blocked);
                            }
                            let message = &messages[0];
                            let message_content_shape_is_valid =
                                if message.content_purged_at.is_some() {
                                    message.protected_preview.is_none() && message.block_count == 0
                                } else {
                                    message.protected_preview.is_some()
                                        && (1..=4_096).contains(&message.block_count)
                                };
                            if message.id != message_id
                                || message.session_id != session_id
                                || message.run_id != Some(checkpoint.run_id)
                                || message.sequence <= 0
                                || message.sequence > session.message_head
                                || message.message_role != "assistant"
                                || message.provider_kind.as_deref()
                                    != Some(budget.provider_kind.as_str())
                                || message.provider_model.as_deref()
                                    != Some(budget.provider_model.as_str())
                                || message.completion_state != "complete"
                                || message
                                    .finalized_at
                                    .is_none_or(|at| at > final_checkpoint.created_at)
                                || message.created_at > final_checkpoint.created_at
                                || !message_content_shape_is_valid
                            {
                                return Ok(RawCheckpointPurgeOutcome::Blocked);
                            }
                        }
                        if (checkpoint.checkpoint_kind == "tool_batch_persisted"
                            && relevant_calls.is_empty())
                            || (checkpoint.checkpoint_kind == "supervised_tool_batch_persisted"
                                && relevant_calls.len() != 1)
                        {
                            return Ok(RawCheckpointPurgeOutcome::Blocked);
                        }
                        let mut relevant_call_ids = HashSet::new();
                        for call in relevant_calls {
                            if !tool_call_state_is_terminal(&call.state)
                                || call.completed_at.is_none_or(|at| at > raw_cutoff)
                                || call.payload_purged_at.is_none()
                                || call.protected_arguments.is_some()
                                || call.protected_result.is_some()
                            {
                                return Ok(RawCheckpointPurgeOutcome::Blocked);
                            }
                            let steps = maintenance
                                .query::<AiRunStepRecord>()
                                .filter(AiRunStepRecordWhereInput {
                                    id: Some(UuidFilter {
                                        eq: Some(call.id),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                })
                                .limit(2)
                                .fetch_all()
                                .await
                                .map_err(OrmPublicError::from)?;
                            if steps.len() != 1 {
                                return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                            }
                            validate_tool_step(&steps[0], call)?;
                            let approval = call
                                .approval_id
                                .and_then(|approval_id| approvals_by_id.get(&approval_id))
                                .copied();
                            if call.approval_id.is_some() != approval.is_some()
                                || !tool_approval_states_match(call, approval)
                                || approval.is_some_and(|approval| {
                                    approval.tool_call_id != call.id
                                        || !approval_state_is_terminal(&approval.state)
                                        || approval.payload_purged_at.is_none()
                                        || approval.protected_resource_bindings.is_some()
                                        || approval.protected_action_preview.is_some()
                                })
                            {
                                return Ok(RawCheckpointPurgeOutcome::Blocked);
                            }
                            if let Some(approval) = approval {
                                validate_approval(approval, session_id)?;
                            }
                            relevant_call_ids.insert(call.id);
                        }
                        for approval in &approvals {
                            if !relevant_call_ids.contains(&approval.tool_call_id) {
                                continue;
                            }
                            validate_approval(approval, session_id)?;
                            let call = calls_by_id.get(&approval.tool_call_id).copied();
                            if call.is_none_or(|call| call.approval_id != Some(approval.id))
                                || !approval_state_is_terminal(&approval.state)
                                || approval.payload_purged_at.is_none()
                                || approval.protected_resource_bindings.is_some()
                                || approval.protected_action_preview.is_some()
                            {
                                return Ok(RawCheckpointPurgeOutcome::Blocked);
                            }
                        }
                    }

                    let checkpoint_ids = eligible
                        .into_iter()
                        .map(|checkpoint| checkpoint.id)
                        .collect::<Vec<_>>();
                    let maximum = u32::try_from(checkpoint_ids.len())
                        .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let purge = maintenance
                        .purge::<AiRunCheckpointRecord>(
                            AiRunCheckpointRecordWhereInput {
                                id: Some(UuidFilter {
                                    in_list: Some(checkpoint_ids),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            },
                            MutationLimit::new(maximum)?,
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    let RetentionPurgeOutcome::Purged { affected } = purge else {
                        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                    };
                    if affected != maximum {
                        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                    }
                    maintenance
                        .insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                            actor_principal_kind: "system".to_owned(),
                            actor_subject: "session-retention".to_owned(),
                            action: "purge_expired_run_checkpoints".to_owned(),
                            resource_kind: "ai_session".to_owned(),
                            resource_reference: session_id.to_string(),
                            outcome: "allowed".to_owned(),
                            reason_code: "scope_retention_expired".to_owned(),
                            correlation_id: Uuid::new_v4().to_string(),
                            causation_id: None,
                            policy_version: Some(format!("{}:{}", policy.id, policy.row_version)),
                        })
                        .await
                        .map_err(OrmPublicError::from)?;
                    Ok(RawCheckpointPurgeOutcome::Deleted(
                        usize::try_from(affected)
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))?,
                    ))
                })
            })
            .await
            .map_err(map_transaction)
    }
}

#[async_trait]
impl AiSessionRetentionService for OrmAiSessionRetentionService {
    async fn prune_session_content(
        &self,
        after_session_cursor: Option<String>,
    ) -> Result<AiSessionRetentionReport, AiError> {
        let candidates = self.candidates(after_session_cursor).await?;
        let mut report = AiSessionRetentionReport {
            sessions_scanned: u32::try_from(candidates.sessions.len())
                .map_err(|_| AiError::PersistenceFailed)?,
            next_session_cursor: candidates.next_cursor,
            ..AiSessionRetentionReport::default()
        };
        let now = self.clock.now().unix_timestamp();
        for session in candidates.sessions {
            let session_id = session.id;
            let checkpoint_purge_ready;
            let mut session_changed = false;
            match self.prune_session(session, now).await? {
                SessionPruneOutcome::NotReady => {
                    report.sessions_not_ready += 1;
                    continue;
                }
                SessionPruneOutcome::Conflict => {
                    report.sessions_conflicted += 1;
                    continue;
                }
                SessionPruneOutcome::Noop {
                    messages_blocked,
                    proposal_payload_purge_blocked,
                    tool_payload_purge_blocked,
                    raw_payload_purge_blocked,
                    attachment_cleanup_blocked,
                    run_checkpoint_purge_ready,
                    run_checkpoint_purge_blocked,
                } => {
                    report.messages_blocked = add_count(report.messages_blocked, messages_blocked)?;
                    if proposal_payload_purge_blocked {
                        report.proposal_payload_purges_blocked = report
                            .proposal_payload_purges_blocked
                            .checked_add(1)
                            .ok_or(AiError::PersistenceFailed)?;
                    }
                    if tool_payload_purge_blocked {
                        report.tool_payload_purges_blocked = report
                            .tool_payload_purges_blocked
                            .checked_add(1)
                            .ok_or(AiError::PersistenceFailed)?;
                    }
                    if raw_payload_purge_blocked {
                        report.raw_payload_purges_blocked = report
                            .raw_payload_purges_blocked
                            .checked_add(1)
                            .ok_or(AiError::PersistenceFailed)?;
                    }
                    if attachment_cleanup_blocked {
                        report.attachment_cleanups_blocked = report
                            .attachment_cleanups_blocked
                            .checked_add(1)
                            .ok_or(AiError::PersistenceFailed)?;
                    }
                    checkpoint_purge_ready = run_checkpoint_purge_ready;
                    if run_checkpoint_purge_blocked {
                        report.run_checkpoint_purges_blocked = report
                            .run_checkpoint_purges_blocked
                            .checked_add(1)
                            .ok_or(AiError::PersistenceFailed)?;
                    }
                }
                SessionPruneOutcome::Changed {
                    live_delta_events_deleted,
                    deleting_session_events_deleted,
                    deleting_session_inbox_payloads_purged,
                    deleting_session_context_checkpoints_deleted,
                    context_checkpoints_invalidated,
                    proposal_payloads_purged,
                    proposal_payload_purge_blocked,
                    deleting_tool_payloads_purged,
                    deleting_approval_payloads_purged,
                    expired_tool_payloads_purged,
                    expired_approval_payloads_purged,
                    tool_payload_purge_blocked,
                    raw_payload_purge_blocked,
                    attachment_cleanups_requested,
                    attachments_deleted,
                    attachment_artifact_cleanups_requested,
                    attachment_artifacts_deleted,
                    attachment_cleanup_blocked,
                    messages_purged,
                    blocks_deleted,
                    messages_blocked,
                    run_checkpoint_references_cleared,
                    run_checkpoint_purge_ready,
                    run_checkpoint_purge_blocked,
                } => {
                    session_changed = true;
                    checkpoint_purge_ready = run_checkpoint_purge_ready;
                    report.live_delta_events_deleted =
                        add_count(report.live_delta_events_deleted, live_delta_events_deleted)?;
                    report.deleting_session_events_deleted = add_count(
                        report.deleting_session_events_deleted,
                        deleting_session_events_deleted,
                    )?;
                    report.deleting_session_inbox_payloads_purged = add_count(
                        report.deleting_session_inbox_payloads_purged,
                        deleting_session_inbox_payloads_purged,
                    )?;
                    report.deleting_session_context_checkpoints_deleted = add_count(
                        report.deleting_session_context_checkpoints_deleted,
                        deleting_session_context_checkpoints_deleted,
                    )?;
                    report.context_checkpoints_invalidated = add_count(
                        report.context_checkpoints_invalidated,
                        context_checkpoints_invalidated,
                    )?;
                    report.deleting_session_proposal_payloads_purged = add_count(
                        report.deleting_session_proposal_payloads_purged,
                        proposal_payloads_purged,
                    )?;
                    if proposal_payload_purge_blocked {
                        report.proposal_payload_purges_blocked = report
                            .proposal_payload_purges_blocked
                            .checked_add(1)
                            .ok_or(AiError::PersistenceFailed)?;
                    }
                    report.deleting_session_tool_payloads_purged = add_count(
                        report.deleting_session_tool_payloads_purged,
                        deleting_tool_payloads_purged,
                    )?;
                    report.deleting_session_approval_payloads_purged = add_count(
                        report.deleting_session_approval_payloads_purged,
                        deleting_approval_payloads_purged,
                    )?;
                    report.expired_tool_payloads_purged = add_count(
                        report.expired_tool_payloads_purged,
                        expired_tool_payloads_purged,
                    )?;
                    report.expired_approval_payloads_purged = add_count(
                        report.expired_approval_payloads_purged,
                        expired_approval_payloads_purged,
                    )?;
                    if tool_payload_purge_blocked {
                        report.tool_payload_purges_blocked = report
                            .tool_payload_purges_blocked
                            .checked_add(1)
                            .ok_or(AiError::PersistenceFailed)?;
                    }
                    if raw_payload_purge_blocked {
                        report.raw_payload_purges_blocked = report
                            .raw_payload_purges_blocked
                            .checked_add(1)
                            .ok_or(AiError::PersistenceFailed)?;
                    }
                    report.deleting_session_attachment_cleanups_requested = add_count(
                        report.deleting_session_attachment_cleanups_requested,
                        attachment_cleanups_requested,
                    )?;
                    report.deleting_session_attachments_deleted = add_count(
                        report.deleting_session_attachments_deleted,
                        attachments_deleted,
                    )?;
                    report.deleting_session_attachment_artifact_cleanups_requested = add_count(
                        report.deleting_session_attachment_artifact_cleanups_requested,
                        attachment_artifact_cleanups_requested,
                    )?;
                    report.deleting_session_attachment_artifacts_deleted = add_count(
                        report.deleting_session_attachment_artifacts_deleted,
                        attachment_artifacts_deleted,
                    )?;
                    report.message_contents_purged =
                        add_count(report.message_contents_purged, messages_purged)?;
                    report.message_blocks_deleted =
                        add_count(report.message_blocks_deleted, blocks_deleted)?;
                    report.messages_blocked = add_count(report.messages_blocked, messages_blocked)?;
                    if attachment_cleanup_blocked {
                        report.attachment_cleanups_blocked = report
                            .attachment_cleanups_blocked
                            .checked_add(1)
                            .ok_or(AiError::PersistenceFailed)?;
                    }
                    report.deleting_session_run_checkpoint_references_cleared = add_count(
                        report.deleting_session_run_checkpoint_references_cleared,
                        run_checkpoint_references_cleared,
                    )?;
                    if run_checkpoint_purge_blocked {
                        report.run_checkpoint_purges_blocked = report
                            .run_checkpoint_purges_blocked
                            .checked_add(1)
                            .ok_or(AiError::PersistenceFailed)?;
                    }
                }
            }
            if checkpoint_purge_ready {
                match self.purge_run_checkpoints(session_id, now).await? {
                    DeletingRunCheckpointPurgeOutcome::Verified { deleted } => {
                        if deleted > 0 {
                            session_changed = true;
                            report.deleting_session_run_checkpoints_deleted = add_count(
                                report.deleting_session_run_checkpoints_deleted,
                                deleted,
                            )?;
                        }
                        if self.finalize_deleted_session(session_id, now).await? {
                            session_changed = true;
                            report.deleting_sessions_finalized = report
                                .deleting_sessions_finalized
                                .checked_add(1)
                                .ok_or(AiError::PersistenceFailed)?;
                        }
                    }
                    DeletingRunCheckpointPurgeOutcome::Blocked => {
                        report.run_checkpoint_purges_blocked = report
                            .run_checkpoint_purges_blocked
                            .checked_add(1)
                            .ok_or(AiError::PersistenceFailed)?;
                    }
                }
            } else {
                match self.purge_expired_run_checkpoints(session_id, now).await? {
                    RawCheckpointPurgeOutcome::Deleted(deleted) => {
                        session_changed = true;
                        report.expired_run_checkpoints_deleted =
                            add_count(report.expired_run_checkpoints_deleted, deleted)?;
                    }
                    RawCheckpointPurgeOutcome::Blocked => {
                        report.raw_checkpoint_purges_blocked = report
                            .raw_checkpoint_purges_blocked
                            .checked_add(1)
                            .ok_or(AiError::PersistenceFailed)?;
                    }
                    RawCheckpointPurgeOutcome::NotApplicable | RawCheckpointPurgeOutcome::Noop => {}
                }
            }
            if session_changed {
                report.sessions_changed = report
                    .sessions_changed
                    .checked_add(1)
                    .ok_or(AiError::PersistenceFailed)?;
            }
        }
        Ok(report)
    }
}

struct SessionCandidatePage {
    sessions: Vec<AiSessionRecord>,
    next_cursor: Option<String>,
}

enum RawCheckpointPurgeOutcome {
    NotApplicable,
    Noop,
    Blocked,
    Deleted(usize),
}

enum DeletingRunCheckpointPurgeOutcome {
    Verified { deleted: usize },
    Blocked,
}

enum SessionPruneOutcome {
    NotReady,
    Conflict,
    Noop {
        messages_blocked: usize,
        proposal_payload_purge_blocked: bool,
        tool_payload_purge_blocked: bool,
        raw_payload_purge_blocked: bool,
        attachment_cleanup_blocked: bool,
        run_checkpoint_purge_ready: bool,
        run_checkpoint_purge_blocked: bool,
    },
    Changed {
        live_delta_events_deleted: usize,
        deleting_session_events_deleted: usize,
        deleting_session_inbox_payloads_purged: usize,
        deleting_session_context_checkpoints_deleted: usize,
        context_checkpoints_invalidated: usize,
        proposal_payloads_purged: usize,
        proposal_payload_purge_blocked: bool,
        deleting_tool_payloads_purged: usize,
        deleting_approval_payloads_purged: usize,
        expired_tool_payloads_purged: usize,
        expired_approval_payloads_purged: usize,
        tool_payload_purge_blocked: bool,
        raw_payload_purge_blocked: bool,
        attachment_cleanups_requested: usize,
        attachments_deleted: usize,
        attachment_artifact_cleanups_requested: usize,
        attachment_artifacts_deleted: usize,
        attachment_cleanup_blocked: bool,
        messages_purged: usize,
        blocks_deleted: usize,
        messages_blocked: usize,
        run_checkpoint_references_cleared: usize,
        run_checkpoint_purge_ready: bool,
        run_checkpoint_purge_blocked: bool,
    },
}

fn validate_cursor(cursor: Option<&str>) -> Result<(), AiError> {
    if cursor.is_some_and(|value| {
        value.is_empty() || value.len() > 4_096 || value.chars().any(char::is_control)
    }) {
        return Err(AiError::InvalidInput(
            "invalid session-retention cursor".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_session(session: &AiSessionRecord) -> Result<(), OrmPublicError> {
    let scope = session_scope(session);
    let lifecycle_is_valid = match session.state.as_str() {
        "active" | "archived" => session.deleted_at.is_none() && !session.title.trim().is_empty(),
        "deleting" => session.deleted_at.is_some() && !session.title.trim().is_empty(),
        "deleted" => session.deleted_at.is_some() && session.title.is_empty(),
        _ => false,
    };
    if session.id.is_nil()
        || session.owner_principal_kind.trim().is_empty()
        || session.owner_subject.trim().is_empty()
        || !lifecycle_is_valid
        || session.stream_head < 0
        || session.message_head < 0
        || scope.kind.trim().is_empty()
        || scope.id.trim().is_empty()
        || scope.kind.len() > 128
        || scope.id.len() > 512
        || scope
            .tenant_id
            .as_ref()
            .is_some_and(|tenant| tenant.trim().is_empty() || tenant.len() > 512)
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    Ok(())
}

fn validate_message(
    message: &AiMessageRecord,
    session: &AiSessionRecord,
    previous_sequence: Option<i64>,
) -> Result<(), OrmPublicError> {
    if message.session_id != session.id
        || message.sequence <= 0
        || message.sequence > session.message_head
        || previous_sequence.is_some_and(|value| message.sequence <= value)
        || message.content_purged_at.is_some()
        || message.protected_preview.is_none()
        || message.block_count <= 0
        || message.block_count > 4_096
        || message.completion_state != "complete"
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    Ok(())
}

fn validate_proposal(
    proposal: &AiProposalRecord,
    session: &AiSessionRecord,
) -> Result<(), OrmPublicError> {
    if proposal.id.is_nil()
        || proposal.session_id != session.id
        || proposal.run_id.is_nil()
        || proposal.scope_kind != session.scope_kind
        || proposal.scope_id != session.scope_id
        || proposal.scope_kind.trim().is_empty()
        || proposal.scope_kind.len() > 128
        || proposal.scope_id.trim().is_empty()
        || proposal.scope_id.len() > 512
        || proposal.proposal_type.trim().is_empty()
        || proposal.proposal_type.len() > 512
        || proposal.schema_version.trim().is_empty()
        || proposal.schema_version.len() > 512
        || !(0..=10_000).contains(&proposal.item_count)
        || !matches!(
            proposal.state.as_str(),
            "pending_review" | "accepted" | "accepted_edited" | "rejected" | "applied" | "expired"
        )
        || proposal.created_by_subject.trim().is_empty()
        || proposal.created_by_subject.len() > 512
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    Ok(())
}

fn validate_proposal_item(
    item: &AiProposalItemRecord,
    proposal_id: Uuid,
    expected_index: usize,
) -> Result<(), OrmPublicError> {
    if item.id.is_nil()
        || item.proposal_id != proposal_id
        || i64::try_from(expected_index).ok() != Some(item.item_index)
        || item.stable_path.trim().is_empty()
        || item.stable_path.len() > 4_096
        || item
            .review_decision
            .as_ref()
            .is_some_and(|decision| decision.trim().is_empty() || decision.len() > 128)
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    Ok(())
}

fn proposal_state_is_terminal(proposal: &AiProposalRecord, now: i64) -> bool {
    matches!(proposal.state.as_str(), "rejected" | "applied" | "expired")
        || (proposal.state == "pending_review"
            && proposal
                .expires_at
                .is_some_and(|expires_at| expires_at <= now))
}

fn validate_tool_call(call: &AiToolCallRecord, run_ids: &[Uuid]) -> Result<(), OrmPublicError> {
    if call.id.is_nil()
        || call.run_id.is_nil()
        || !run_ids.contains(&call.run_id)
        || call.provider_call_key.trim().is_empty()
        || call.provider_call_key.len() > 1_024
        || call.provider_call_id.trim().is_empty()
        || call.provider_call_id.len() > 1_024
        || call
            .provider_kind
            .as_ref()
            .is_none_or(|value| value.trim().is_empty() || value.len() > 128)
        || call
            .provider_model
            .as_ref()
            .is_none_or(|value| value.trim().is_empty() || value.len() > 512)
        || call.provider_turn_index < 0
        || call.tool_call_index < 0
        || call.tool_id.trim().is_empty()
        || call.tool_id.len() > 512
        || call.tool_fingerprint.trim().is_empty()
        || call.tool_fingerprint.len() > 512
        || call.argument_hash.trim().is_empty()
        || call.argument_hash.len() > 512
        || call.risk.trim().is_empty()
        || call.risk.len() > 128
        || call.lease_generation < 0
        || call.state.trim().is_empty()
        || call.state.len() > 128
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    Ok(())
}

fn validate_tool_step(
    step: &AiRunStepRecord,
    call: &AiToolCallRecord,
) -> Result<(), OrmPublicError> {
    if step.id != call.id
        || step.run_id != call.run_id
        || step.step_kind != "application_tool"
        || step.state != call.state
        || step.lease_generation != call.lease_generation
        || step.started_at.is_none()
        || step.finished_at.is_none()
        || call.completed_at.is_none()
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    Ok(())
}

fn validate_approval(approval: &AiApprovalRecord, session_id: Uuid) -> Result<(), OrmPublicError> {
    let consumption_shape_is_valid = if approval.state == "consumed" {
        approval.consumed_uses == 1 && approval.consumed_at.is_some()
    } else {
        approval.consumed_uses == 0 && approval.consumed_at.is_none()
    };
    if approval.id.is_nil()
        || approval.tool_call_id.is_nil()
        || approval.session_id != session_id
        || approval.principal_subject.trim().is_empty()
        || approval.principal_subject.len() > 512
        || approval.principal_reference_fingerprint.trim().is_empty()
        || approval.principal_reference_fingerprint.len() > 512
        || approval.argument_hash.trim().is_empty()
        || approval.argument_hash.len() > 512
        || approval.tool_fingerprint.trim().is_empty()
        || approval.tool_fingerprint.len() > 512
        || approval.binding_hash.trim().is_empty()
        || approval.binding_hash.len() > 512
        || approval.action_preview_hash.trim().is_empty()
        || approval.action_preview_hash.len() > 512
        || approval.maximum_uses != 1
        || !(0..=1).contains(&approval.consumed_uses)
        || !consumption_shape_is_valid
        || !matches!(
            approval.state.as_str(),
            "pending"
                | "approved"
                | "resume_claimed"
                | "denied"
                | "expired"
                | "revoked"
                | "consumed"
        )
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    Ok(())
}

fn tool_call_state_is_terminal(state: &str) -> bool {
    matches!(
        state,
        "completed"
            | "execution_failed"
            | "egress_denied"
            | "egress_audit_failed"
            | "approval_denied"
            | "approval_revoked"
            | "approval_expired"
    )
}

fn tool_call_result_required(state: &str) -> bool {
    matches!(
        state,
        "completed" | "execution_failed" | "egress_denied" | "egress_audit_failed"
    )
}

fn approval_state_is_terminal(state: &str) -> bool {
    matches!(state, "consumed" | "denied" | "expired" | "revoked")
}

fn tool_approval_states_match(
    call: &AiToolCallRecord,
    approval: Option<&AiApprovalRecord>,
) -> bool {
    match approval.map(|approval| approval.state.as_str()) {
        None => !matches!(
            call.state.as_str(),
            "approval_denied" | "approval_revoked" | "approval_expired"
        ),
        Some("consumed") => matches!(
            call.state.as_str(),
            "completed" | "execution_failed" | "egress_denied" | "egress_audit_failed"
        ),
        Some("denied") => call.state == "approval_denied",
        Some("revoked") => call.state == "approval_revoked",
        Some("expired") => call.state == "approval_expired",
        Some(_) => false,
    }
}

pub(crate) fn validate_attachment(
    attachment: &AiAttachmentRecord,
    session_id: Uuid,
) -> Result<(), OrmPublicError> {
    if attachment.id.is_nil()
        || attachment.session_id != session_id
        || attachment.message_id.is_some_and(|id| id.is_nil())
        || attachment.owner_principal_kind.trim().is_empty()
        || attachment.owner_subject.trim().is_empty()
        || attachment.safe_filename.trim().is_empty()
        || attachment.safe_filename.len() > 1_024
        || attachment.quarantine_state.trim().is_empty()
        || attachment.scan_state.trim().is_empty()
        || attachment.processing_state.trim().is_empty()
        || attachment
            .cleanup_generation
            .is_some_and(|value| value <= 0)
        || attachment
            .cleanup_retry_count
            .is_some_and(|value| value < 0)
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    Ok(())
}

pub(crate) fn validate_attachment_artifact(
    artifact: &AiAttachmentArtifactRecord,
    attachment_id: Uuid,
) -> Result<(), OrmPublicError> {
    if artifact.id.is_nil()
        || artifact.attachment_id != attachment_id
        || artifact.artifact_kind.trim().is_empty()
        || artifact.artifact_kind.len() > 128
        || artifact.artifact_kind.chars().any(char::is_control)
        || artifact.byte_count < 0
        || artifact.blob_reference.as_ref().is_some_and(|value| {
            value.trim().is_empty() || value.len() > 4_096 || value.chars().any(char::is_control)
        })
        || artifact.provider_reference.as_ref().is_some_and(|value| {
            value.trim().is_empty() || value.len() > 4_096 || value.chars().any(char::is_control)
        })
        || artifact
            .cleanup_generation
            .is_some_and(|generation| generation <= 0)
        || artifact
            .cleanup_retry_count
            .is_some_and(|retry_count| retry_count < 0)
        || (artifact.provider_expires_at.is_some() && artifact.provider_reference.is_none())
        || artifact.cleanup_state.as_deref().is_some_and(|state| {
            !matches!(
                state,
                "cleanup_required" | "cleanup_in_progress" | "cleanup_backoff" | "complete"
            )
        })
        || (artifact.cleanup_state.as_deref() == Some("cleanup_required")
            && (artifact.deleted_at.is_some()
                || artifact.cleanup_lease_expires_at.is_some()
                || artifact.cleanup_next_attempt_at.is_some()))
        || (artifact.cleanup_state.as_deref() == Some("cleanup_in_progress")
            && (artifact.deleted_at.is_some()
                || artifact.cleanup_generation.is_none()
                || artifact.cleanup_lease_expires_at.is_none()
                || artifact.cleanup_next_attempt_at.is_some()))
        || (artifact.cleanup_state.as_deref() == Some("cleanup_backoff")
            && (artifact.deleted_at.is_some()
                || artifact.cleanup_generation.is_none()
                || artifact.cleanup_lease_expires_at.is_some()
                || artifact
                    .cleanup_retry_count
                    .is_none_or(|retry_count| retry_count <= 0)
                || artifact.cleanup_next_attempt_at.is_none()))
        || (artifact.cleanup_state.as_deref() == Some("complete")
            && (artifact.blob_reference.is_some()
                || artifact.protected_content.is_some()
                || artifact.provider_reference.is_some()
                || artifact.provider_expires_at.is_some()
                || artifact.deleted_at.is_none()
                || artifact.cleanup_generation.is_none()
                || artifact.cleanup_lease_expires_at.is_some()
                || artifact.cleanup_next_attempt_at.is_some()))
    {
        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
    }
    Ok(())
}

fn attachment_artifact_ready_for_metadata_delete(artifact: &AiAttachmentArtifactRecord) -> bool {
    artifact.cleanup_state.as_deref() == Some("complete")
        && artifact
            .cleanup_generation
            .is_some_and(|generation| generation > 0)
        && artifact.blob_reference.is_none()
        && artifact.protected_content.is_none()
        && artifact.provider_reference.is_none()
        && artifact.provider_expires_at.is_none()
        && artifact.cleanup_lease_expires_at.is_none()
        && artifact.cleanup_next_attempt_at.is_none()
        && artifact.deleted_at.is_some()
}

fn attachment_artifact_cleanup_pending(artifact: &AiAttachmentArtifactRecord) -> bool {
    matches!(
        artifact.cleanup_state.as_deref(),
        Some("cleanup_required" | "cleanup_in_progress" | "cleanup_backoff")
    )
}

fn attachment_ready_for_metadata_delete(attachment: &AiAttachmentRecord) -> bool {
    attachment.deleted_at.is_some()
        && attachment.cleanup_generation.is_some_and(|value| value > 0)
        && attachment.blob_reference.is_none()
        && attachment.quarantine_blob_reference.is_none()
        && attachment.upload_token_hash.is_none()
        && attachment.processing_state == "complete"
        && attachment.processing_expires_at.is_none()
        && attachment.cleanup_lease_expires_at.is_none()
        && attachment.cleanup_next_attempt_at.is_none()
        && matches!(
            attachment.quarantine_state.as_str(),
            "deleted" | "expired" | "failed"
        )
}

pub(crate) fn attachment_retention_cleanup_pending(attachment: &AiAttachmentRecord) -> bool {
    attachment.quarantine_state == "deleting"
        && matches!(
            attachment.processing_state.as_str(),
            "retention_cleanup_required" | "cleanup_in_progress" | "cleanup_backoff"
        )
}

fn attachment_cleanup_pending(attachment: &AiAttachmentRecord) -> bool {
    attachment_retention_cleanup_pending(attachment)
        || attachment.quarantine_state == "deleting"
        || matches!(
            attachment.processing_state.as_str(),
            "cleanup_required" | "cleanup_in_progress" | "cleanup_backoff" | "deleting"
        )
}

pub(crate) fn valid_policy(
    policy: &AiRetentionPolicyRecord,
    scope: &AiScope,
    scope_key: &str,
) -> bool {
    let durations = [
        policy.message_retention_seconds,
        Some(policy.delta_retention_seconds),
        Some(policy.raw_payload_retention_seconds),
        Some(policy.audit_retention_seconds),
        Some(policy.deleted_content_purge_seconds),
        policy.inbox_event_retention_seconds,
    ];
    policy.scope_key.as_deref() == Some(scope_key)
        && policy.scope_kind == scope.kind
        && policy.scope_id == scope.id
        && policy.tenant_id == scope.tenant_id
        && durations
            .into_iter()
            .flatten()
            .all(|seconds| (60..=MAXIMUM_RETENTION_SECONDS).contains(&seconds))
        && policy
            .inbox_minimum_events
            .is_some_and(|value| (1..=100_000).contains(&value))
}

pub(crate) fn session_scope(session: &AiSessionRecord) -> AiScope {
    AiScope {
        kind: session.scope_kind.clone(),
        id: session.scope_id.clone(),
        tenant_id: session.tenant_id.clone(),
    }
}

fn add_count(current: u32, amount: usize) -> Result<u32, AiError> {
    current
        .checked_add(u32::try_from(amount).map_err(|_| AiError::PersistenceFailed)?)
        .ok_or(AiError::PersistenceFailed)
}

fn map_transaction(error: TransactionError) -> AiError {
    map_orm(error.public_error().clone())
}

fn map_orm(error: impl Into<OrmPublicError>) -> AiError {
    let error = error.into();
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
    use crate::{
        AiAttachmentAcceptancePolicy, AiAttachmentCandidate, AiAttachmentCleanupService,
        AiAttachmentScanReport, AiAttachmentScanRequest, AiAttachmentScanner,
        AiProviderFileDeletionRequest, AiProviderFileDeletionService, AiSessionService,
        DatabaseManagedContentProtector, OrmAiAttachmentService,
    };
    use agql_auth::{AccessTokenMetadata, AuthPrincipal, AuthUser, FixedClock, SessionContext};
    use graphql_orm::graphql::orm::{ApplyOptions, OrmSchemaModule};
    use graphql_orm::prelude::SqliteBackend;
    use graphql_orm_storage::{
        BlobBody, BlobListPage, BlobMetadata, BlobPutOptions, BlobStore, BlobWriteOutcome,
        StorageBackend, StorageByteStream, StorageError,
    };
    use std::collections::BTreeSet;
    use std::ops::Range;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use time::OffsetDateTime;

    struct AllowAll;

    #[async_trait]
    impl crate::AiAccessPolicy for AllowAll {
        async fn can_access_scope(
            &self,
            _principal: &AuthPrincipal,
            _scope: &AiScope,
            _action: crate::AiSessionAction,
        ) -> crate::AiAccessDecision {
            crate::AiAccessDecision::allow("retention-test", "v1")
        }

        async fn can_access_session(
            &self,
            _principal: &AuthPrincipal,
            _session_id: crate::AiSessionId,
            _action: crate::AiSessionAction,
        ) -> crate::AiAccessDecision {
            crate::AiAccessDecision::allow("retention-test", "v1")
        }
    }

    struct ProtectionPolicy;

    #[async_trait]
    impl crate::AiContentProtectionPolicyResolver for ProtectionPolicy {
        async fn resolve(
            &self,
            _principal: &AuthPrincipal,
            scope: &AiScope,
        ) -> Result<crate::AiContentProtectionPolicy, AiError> {
            Ok(crate::AiContentProtectionPolicy {
                scope: scope.clone(),
                mode: crate::AiContentProtectionMode::DatabaseManaged,
                key_policy_reference: None,
                version: 1,
                ready: true,
            })
        }
    }

    struct UnusedScanner;

    #[async_trait]
    impl AiAttachmentScanner for UnusedScanner {
        async fn scan(
            &self,
            _request: &AiAttachmentScanRequest,
            _body: StorageByteStream,
        ) -> Result<AiAttachmentScanReport, AiError> {
            Err(AiError::PersistenceFailed)
        }
    }

    struct DenyAttachmentAcceptance;

    #[async_trait]
    impl AiAttachmentAcceptancePolicy for DenyAttachmentAcceptance {
        async fn authorize(
            &self,
            _principal: &AuthPrincipal,
            _scope: &AiScope,
            _candidate: &AiAttachmentCandidate,
        ) -> crate::AiAccessDecision {
            crate::AiAccessDecision::deny("unused", "unused")
        }
    }

    #[derive(Default)]
    struct ArtifactBlobStore {
        references: Mutex<BTreeSet<String>>,
    }

    impl ArtifactBlobStore {
        fn insert(&self, reference: &str) {
            self.references
                .lock()
                .expect("artifact blob lock")
                .insert(reference.to_owned());
        }

        fn contains(&self, reference: &str) -> bool {
            self.references
                .lock()
                .expect("artifact blob lock")
                .contains(reference)
        }

        fn unsupported() -> StorageError {
            StorageError::Provider {
                backend: "artifact-test".to_owned(),
                message: "unused operation".to_owned(),
                retryable: false,
            }
        }
    }

    #[async_trait]
    impl BlobStore for ArtifactBlobStore {
        fn backend(&self) -> StorageBackend {
            StorageBackend::Local
        }

        async fn put_blob(
            &self,
            _key: &str,
            _body: StorageByteStream,
            _options: BlobPutOptions,
        ) -> Result<BlobWriteOutcome, StorageError> {
            Err(Self::unsupported())
        }

        async fn put_blob_if_not_exists(
            &self,
            _key: &str,
            _body: StorageByteStream,
            _options: BlobPutOptions,
        ) -> Result<Option<BlobWriteOutcome>, StorageError> {
            Err(Self::unsupported())
        }

        async fn get_blob(&self, _key: &str) -> Result<BlobBody, StorageError> {
            Err(Self::unsupported())
        }

        async fn get_blob_range(
            &self,
            _key: &str,
            _range: Range<u64>,
        ) -> Result<BlobBody, StorageError> {
            Err(Self::unsupported())
        }

        async fn blob_exists(&self, key: &str) -> Result<bool, StorageError> {
            Ok(self.contains(key))
        }

        async fn head_blob(&self, _key: &str) -> Result<Option<BlobMetadata>, StorageError> {
            Err(Self::unsupported())
        }

        async fn list_blobs_page(
            &self,
            _prefix: &str,
            _continuation: Option<String>,
            _limit: usize,
        ) -> Result<BlobListPage, StorageError> {
            Err(Self::unsupported())
        }

        async fn delete_blob(&self, key: &str) -> Result<(), StorageError> {
            self.references
                .lock()
                .expect("artifact blob lock")
                .remove(key);
            Ok(())
        }
    }

    #[derive(Default)]
    struct ProviderFileDeletion {
        fail: AtomicBool,
        deleted: Mutex<Vec<String>>,
    }

    impl ProviderFileDeletion {
        fn set_fail(&self, fail: bool) {
            self.fail.store(fail, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl AiProviderFileDeletionService for ProviderFileDeletion {
        async fn delete_and_confirm_absent(
            &self,
            request: &AiProviderFileDeletionRequest,
        ) -> Result<(), AiError> {
            let debug = format!("{request:?}");
            assert!(!debug.contains(request.provider_reference()));
            assert!(!request.artifact_id().is_nil());
            assert!(!request.attachment_id().is_nil());
            assert_eq!(request.artifact_kind(), "provider_file");
            if self.fail.load(Ordering::SeqCst) {
                return Err(AiError::PersistenceFailed);
            }
            self.deleted
                .lock()
                .expect("provider deletion lock")
                .push(request.provider_reference().to_owned());
            Ok(())
        }
    }

    fn principal() -> AuthPrincipal {
        AuthPrincipal::User(AuthUser {
            user_id: "retention-user".to_owned(),
            session_id: Uuid::new_v4(),
            roles: vec![],
            scopes: vec![],
            session: SessionContext::default(),
            token_claims: AccessTokenMetadata {
                tenant_id: Some("retention".to_owned()),
                ..AccessTokenMetadata::default()
            },
        })
    }

    async fn database() -> Database<SqliteBackend> {
        let database = Database::<SqliteBackend>::connect_sqlite("sqlite::memory:")
            .await
            .expect("in-memory SQLite should open");
        let module = crate::AiSchemaModule;
        let plan = database
            .schema()
            .plan_migration_to_entities(
                "ai-session-retention-test-v1",
                "AI session retention test",
                module.entities(),
            )
            .await
            .expect("retention schema should plan");
        database
            .schema()
            .apply_migration(&plan, ApplyOptions::default())
            .await
            .expect("retention schema should apply");
        database
    }

    fn now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(2_000_000_000)
            .expect("fixed retention time should validate")
    }

    async fn seed_policy(database: &Database<SqliteBackend>, scope: &AiScope) {
        seed_policy_with_message_retention(database, scope, Some(60)).await;
    }

    async fn seed_policy_with_message_retention(
        database: &Database<SqliteBackend>,
        scope: &AiScope,
        message_retention_seconds: Option<i64>,
    ) {
        AiRetentionPolicyRecord::insert(
            database,
            CreateAiRetentionPolicyRecordInput {
                scope_key: Some(crate::ai_scope_key(scope)),
                scope_kind: scope.kind.clone(),
                scope_id: scope.id.clone(),
                tenant_id: scope.tenant_id.clone(),
                message_retention_seconds,
                delta_retention_seconds: 60,
                raw_payload_retention_seconds: 60,
                audit_retention_seconds: 60,
                deleted_content_purge_seconds: 60,
                provider_file_delete_required: true,
                inbox_event_retention_seconds: Some(60),
                inbox_minimum_events: Some(1),
            },
        )
        .await
        .expect("retention policy should seed");
    }

    async fn set_provider_file_delete_required(
        database: &Database<SqliteBackend>,
        scope: &AiScope,
        required: bool,
    ) {
        let scope_key = crate::ai_scope_key(scope);
        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let policies = tx
                        .query::<AiRetentionPolicyRecord>()
                        .filter(AiRetentionPolicyRecordWhereInput {
                            scope_key: Some(StringFilter {
                                eq: Some(scope_key),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if policies.len() != 1 {
                        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                    }
                    let policy = &policies[0];
                    let outcome = tx
                        .compare_and_swap::<AiRetentionPolicyRecord>(
                            &policy.id,
                            policy.row_version,
                            AiRetentionPolicyRecordWhereInput::default(),
                            UpdateAiRetentionPolicyRecordInput {
                                provider_file_delete_required: Some(required),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if matches!(outcome, ConditionalUpdateOutcome::Updated(_)) {
                        Ok(())
                    } else {
                        Err(OrmPublicError::new(OrmErrorCode::Conflict))
                    }
                })
            })
            .await
            .expect("provider-file retention setting should update");
    }

    async fn mark_session_deleting(
        database: &Database<SqliteBackend>,
        session_id: Uuid,
        seconds_ago: i64,
    ) {
        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&session_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    let outcome = tx
                        .compare_and_swap::<AiSessionRecord>(
                            &session.id,
                            session.row_version,
                            AiSessionRecordWhereInput::default(),
                            UpdateAiSessionRecordInput {
                                state: Some("deleting".to_owned()),
                                deleted_at: Some(Some(now().unix_timestamp() - seconds_ago)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if matches!(outcome, ConditionalUpdateOutcome::Updated(_)) {
                        Ok(())
                    } else {
                        Err(OrmPublicError::new(OrmErrorCode::Conflict))
                    }
                })
            })
            .await
            .expect("test session should enter deleting state");
    }

    async fn seed_session(database: &Database<SqliteBackend>, scope: &AiScope) -> Uuid {
        let id = Uuid::new_v4();
        AiSessionRecord::insert(
            database,
            CreateAiSessionRecordInput {
                id,
                owner_principal_kind: "user".to_owned(),
                owner_subject: "retention-user".to_owned(),
                tenant_id: scope.tenant_id.clone(),
                scope_kind: scope.kind.clone(),
                scope_id: scope.id.clone(),
                title: "Retention test".to_owned(),
                state: "active".to_owned(),
                stream_head: 3,
                message_head: 1,
                last_activity_at: now().unix_timestamp() - 120,
                archived_at: None,
                deleted_at: None,
            },
        )
        .await
        .expect("session should seed");
        id
    }

    async fn seed_message(
        database: &Database<SqliteBackend>,
        session_id: Uuid,
        run_state: &str,
        with_attachment: bool,
    ) -> (Uuid, Uuid) {
        let message_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        AiRunRecord::insert(
            database,
            CreateAiRunRecordInput {
                id: run_id,
                session_id,
                input_message_id: message_id,
                principal_reference: serde_json::json!({"test": true}),
                state: run_state.to_owned(),
                attempt_id: None,
                lease_owner: None,
                lease_generation: 0,
                lease_expires_at: None,
                lease_heartbeat_at: None,
                retry_count: 0,
                next_attempt_at: None,
                error_code: None,
                latest_checkpoint_id: None,
            },
        )
        .await
        .expect("run should seed");
        AiMessageRecord::insert(
            database,
            CreateAiMessageRecordInput {
                id: message_id,
                session_id,
                sequence: 1,
                message_role: "user".to_owned(),
                author_principal_kind: Some("user".to_owned()),
                author_subject: Some("retention-user".to_owned()),
                client_message_id: Some(Uuid::new_v4()),
                content_hash: Some("test-content-hash".to_owned()),
                run_id: Some(run_id),
                provider_kind: None,
                provider_model: None,
                protected_preview: Some(serde_json::json!({"protected": "preview"})),
                block_count: 1,
                completion_state: "complete".to_owned(),
                finalized_at: Some(now().unix_timestamp() - 120),
                content_purged_at: None,
            },
        )
        .await
        .expect("message should seed");
        AiMessageBlockRecord::insert(
            database,
            CreateAiMessageBlockRecordInput {
                id: Uuid::new_v4(),
                message_id,
                block_index: 0,
                block_kind: "text".to_owned(),
                protected_content: serde_json::json!({"protected": "content"}),
                byte_count: 7,
                line_count: 1,
            },
        )
        .await
        .expect("message block should seed");
        if with_attachment {
            AiAttachmentRecord::insert(
                database,
                CreateAiAttachmentRecordInput {
                    id: Uuid::new_v4(),
                    owner_principal_kind: "user".to_owned(),
                    owner_subject: "retention-user".to_owned(),
                    session_id,
                    message_id: Some(message_id),
                    blob_reference: Some("opaque:test".to_owned()),
                    quarantine_blob_reference: None,
                    safe_filename: "retained.txt".to_owned(),
                    declared_mime: Some("text/plain".to_owned()),
                    detected_mime: Some("text/plain".to_owned()),
                    expected_byte_count: Some(7),
                    byte_count: Some(7),
                    sha256: Some("0".repeat(64)),
                    upload_token_hash: None,
                    upload_expires_at: None,
                    quarantine_state: "released".to_owned(),
                    scan_state: "clean".to_owned(),
                    processing_state: "ready".to_owned(),
                    processing_expires_at: None,
                    cleanup_generation: None,
                    cleanup_lease_expires_at: None,
                    cleanup_retry_count: None,
                    cleanup_next_attempt_at: None,
                    scanner_version: Some("test".to_owned()),
                    acceptance_policy_version: Some("test".to_owned()),
                    rejection_code: None,
                    finalized_at: Some(now().unix_timestamp() - 120),
                    deleted_at: None,
                },
            )
            .await
            .expect("attachment should seed");
        }
        (message_id, run_id)
    }

    async fn seed_proposal(
        database: &Database<SqliteBackend>,
        scope: &AiScope,
        session_id: Uuid,
        run_id: Uuid,
        state: &str,
        expires_at: Option<i64>,
        with_item: bool,
    ) -> (Uuid, Option<Uuid>) {
        let proposal_id = Uuid::new_v4();
        AiProposalRecord::insert(
            database,
            CreateAiProposalRecordInput {
                id: proposal_id,
                session_id,
                run_id,
                scope_kind: scope.kind.clone(),
                scope_id: scope.id.clone(),
                proposal_type: "test.retention".to_owned(),
                schema_version: "v1".to_owned(),
                item_count: i64::from(with_item),
                protected_payload: Some(serde_json::json!({"protected": "proposal"})),
                source_references: Some(serde_json::json!([{"kind": "message"}])),
                payload_purged_at: None,
                state: state.to_owned(),
                created_by_subject: "retention-user".to_owned(),
                reviewed_by_subject: (state != "pending_review")
                    .then(|| "retention-reviewer".to_owned()),
                applied_resource_ref: None,
                application_audit_ref: None,
                reviewed_at: (state != "pending_review").then_some(now().unix_timestamp() - 90),
                expires_at,
            },
        )
        .await
        .expect("proposal should seed");
        let item_id = with_item.then(Uuid::new_v4);
        if let Some(item_id) = item_id {
            AiProposalItemRecord::insert(
                database,
                CreateAiProposalItemRecordInput {
                    id: item_id,
                    proposal_id,
                    item_index: 0,
                    stable_path: "/title".to_owned(),
                    protected_suggested_value: Some(serde_json::json!("suggested")),
                    protected_rationale: Some(serde_json::json!("reason")),
                    source_references: Some(serde_json::json!([{"kind": "message"}])),
                    review_decision: Some("accepted".to_owned()),
                    protected_review_value: Some(serde_json::json!("reviewed")),
                },
            )
            .await
            .expect("proposal item should seed");
        }
        (proposal_id, item_id)
    }

    async fn seed_tool_call(
        database: &Database<SqliteBackend>,
        session_id: Uuid,
        run_id: Uuid,
        call_state: &str,
        approval_state: Option<&str>,
    ) -> (Uuid, Option<Uuid>) {
        seed_tool_call_completed_seconds_ago(
            database,
            session_id,
            run_id,
            call_state,
            approval_state,
            90,
        )
        .await
    }

    async fn seed_tool_call_completed_seconds_ago(
        database: &Database<SqliteBackend>,
        session_id: Uuid,
        run_id: Uuid,
        call_state: &str,
        approval_state: Option<&str>,
        completed_seconds_ago: i64,
    ) -> (Uuid, Option<Uuid>) {
        let call_id = Uuid::new_v4();
        let approval_id = approval_state.map(|_| Uuid::new_v4());
        let call_is_terminal = tool_call_state_is_terminal(call_state);
        AiRunStepRecord::insert(
            database,
            CreateAiRunStepRecordInput {
                id: call_id,
                run_id,
                step_index: 0,
                step_kind: "application_tool".to_owned(),
                state: call_state.to_owned(),
                lease_generation: 0,
                started_at: Some(now().unix_timestamp() - completed_seconds_ago - 10),
                finished_at: call_is_terminal
                    .then_some(now().unix_timestamp() - completed_seconds_ago),
                error_code: (call_state != "completed").then(|| "test_outcome".to_owned()),
            },
        )
        .await
        .expect("tool step should seed");
        AiToolCallRecord::insert(
            database,
            CreateAiToolCallRecordInput {
                id: call_id,
                run_id,
                provider_call_key: format!("retention:{call_id}"),
                provider_call_id: format!("provider-{call_id}"),
                provider_kind: Some("mock".to_owned()),
                provider_model: Some("retention-test".to_owned()),
                provider_response_id: Some("retention-response".to_owned()),
                budget_reservation_id: None,
                provider_turn_index: 0,
                tool_call_index: 0,
                tool_id: "test.read".to_owned(),
                tool_fingerprint: "tool-fingerprint".to_owned(),
                protected_arguments: Some(serde_json::json!({"protected": "arguments"})),
                argument_hash: "argument-hash".to_owned(),
                protected_result: tool_call_result_required(call_state)
                    .then(|| serde_json::json!({"protected": "result"})),
                payload_purged_at: None,
                risk: if approval_id.is_some() {
                    "high_impact".to_owned()
                } else {
                    "read_only".to_owned()
                },
                authorization_code: call_is_terminal.then(|| "allowed".to_owned()),
                authorization_policy_version: call_is_terminal.then(|| "policy-v1".to_owned()),
                authorization_state_digest: call_is_terminal.then(|| "auth-state".to_owned()),
                disclosure_schema_fingerprint: call_is_terminal
                    .then(|| "disclosure-fingerprint".to_owned()),
                result_classification: call_is_terminal.then(|| "internal".to_owned()),
                result_egress_decision_id: None,
                result_egress_manifest_hash: None,
                application_audit_ref: None,
                approval_id,
                idempotency_key: Some(call_id.to_string()),
                correlation_id: Some("tool-correlation".to_owned()),
                causation_id: Some("tool-causation".to_owned()),
                delegation_reference: None,
                lease_generation: 0,
                state: call_state.to_owned(),
                completed_at: call_is_terminal
                    .then_some(now().unix_timestamp() - completed_seconds_ago),
            },
        )
        .await
        .expect("tool call should seed");
        if let (Some(approval_id), Some(approval_state)) = (approval_id, approval_state) {
            let decided = approval_state != "pending";
            let consumed = approval_state == "consumed";
            AiApprovalRecord::insert(
                database,
                CreateAiApprovalRecordInput {
                    id: approval_id,
                    tool_call_id: call_id,
                    session_id,
                    principal_subject: "retention-user".to_owned(),
                    principal_reference_fingerprint: "principal-fingerprint".to_owned(),
                    delegated_actor_subject: None,
                    delegation_reference: None,
                    argument_hash: "argument-hash".to_owned(),
                    tool_fingerprint: "tool-fingerprint".to_owned(),
                    binding_hash: "binding-hash".to_owned(),
                    execution_target_id: "local".to_owned(),
                    target_schema_fingerprint: "schema-fingerprint".to_owned(),
                    operation_name: "RetentionTest".to_owned(),
                    operation_document_hash: "document-hash".to_owned(),
                    result_projection_fingerprint: "projection-fingerprint".to_owned(),
                    disclosure_schema_fingerprint: "disclosure-fingerprint".to_owned(),
                    policy_version: "policy-v1".to_owned(),
                    authorization_state_digest: "auth-state".to_owned(),
                    protected_resource_bindings: Some(
                        serde_json::json!({"protected": "resources"}),
                    ),
                    protected_action_preview: Some(serde_json::json!({"protected": "preview"})),
                    payload_purged_at: None,
                    action_preview_hash: "preview-hash".to_owned(),
                    state: approval_state.to_owned(),
                    recent_mfa_required: true,
                    approver_subject: decided.then(|| "retention-reviewer".to_owned()),
                    expires_at: now().unix_timestamp() + 3_600,
                    decided_at: decided.then_some(now().unix_timestamp() - 95),
                    maximum_uses: 1,
                    consumed_uses: i64::from(consumed),
                    consumed_at: consumed.then_some(now().unix_timestamp() - completed_seconds_ago),
                },
            )
            .await
            .expect("tool approval should seed");
        }
        (call_id, approval_id)
    }

    async fn seed_events(database: &Database<SqliteBackend>, session_id: Uuid) {
        for (sequence, event_type) in [
            (1, "provider_live_delta"),
            (2, "message_queued"),
            (3, "run_completed"),
        ] {
            AiSessionEventRecord::insert(
                database,
                CreateAiSessionEventRecordInput {
                    id: Uuid::new_v4(),
                    session_id,
                    sequence,
                    event_type: event_type.to_owned(),
                    run_id: None,
                    causation_id: None,
                    correlation_id: format!("event-{sequence}"),
                    protected_payload: serde_json::json!({"protected": true}),
                },
            )
            .await
            .expect("session event should seed");
        }
    }

    async fn seed_run_checkpoints(
        database: &Database<SqliteBackend>,
        run_id: Uuid,
        count: usize,
    ) -> Vec<Uuid> {
        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let run = tx
                        .find_by_id::<AiRunRecord>(&run_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    let attempt_id = Uuid::new_v4();
                    let mut checkpoint_ids = Vec::with_capacity(count);
                    for index in 0..count {
                        let checkpoint_id = Uuid::new_v4();
                        tx.insert::<AiRunCheckpointRecord>(CreateAiRunCheckpointRecordInput {
                            id: checkpoint_id,
                            run_id,
                            attempt_id,
                            lease_generation: run.lease_generation,
                            checkpoint_kind: format!("retention_test_{index}"),
                            provider_response_id: None,
                            budget_reservation_id: None,
                            assistant_message_id: None,
                            protected_state: Some(serde_json::json!({
                                "protected": index,
                            })),
                            checkpoint_hash: format!("retention-checkpoint-hash-{index}"),
                        })
                        .await
                        .map_err(OrmPublicError::from)?;
                        checkpoint_ids.push(checkpoint_id);
                    }
                    if let Some(latest_checkpoint_id) = checkpoint_ids.last().copied() {
                        let outcome = tx
                            .compare_and_swap::<AiRunRecord>(
                                &run.id,
                                run.row_version,
                                AiRunRecordWhereInput::default(),
                                UpdateAiRunRecordInput {
                                    latest_checkpoint_id: Some(Some(latest_checkpoint_id)),
                                    ..Default::default()
                                },
                            )
                            .await
                            .map_err(OrmPublicError::from)?;
                        if !matches!(outcome, ConditionalUpdateOutcome::Updated(_)) {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    }
                    Ok(checkpoint_ids)
                })
            })
            .await
            .expect("run checkpoints should seed")
    }

    async fn seed_protected_checkpoint_history(
        database: &Database<SqliteBackend>,
        session_id: Uuid,
        run_id: Uuid,
        make_current: bool,
        with_outcome: bool,
    ) -> AiRunCheckpointRecord {
        let wall_clock = OffsetDateTime::now_utc().unix_timestamp();
        let worker_id = format!("checkpoint-retention-{run_id}");
        let attempt = AiRunAttemptRecord::insert(
            database,
            CreateAiRunAttemptRecordInput {
                run_id,
                lease_generation: 1,
                worker_id: worker_id.clone(),
                claimed_at: wall_clock - 10,
                finished_at: None,
                provider_response_id: None,
                outcome_code: None,
            },
        )
        .await
        .expect("checkpoint attempt should seed");
        let attempt_id = attempt.id;
        let provider_response_id = format!("checkpoint-response-{run_id}");
        let budget = AiBudgetReservationRecord::insert(
            database,
            CreateAiBudgetReservationRecordInput {
                budget_counter_ids: serde_json::json!([]),
                scope_kind: "tenant".to_owned(),
                scope_id: "retention".to_owned(),
                tenant_id: Some("retention".to_owned()),
                principal_kind: "user".to_owned(),
                principal_subject: "retention-user".to_owned(),
                session_id,
                run_id,
                attempt_id,
                lease_generation: 1,
                provider_kind: "mock".to_owned(),
                provider_model: "retention-test".to_owned(),
                pricing_policy_version: "retention-pricing-v1".to_owned(),
                reserved_input_tokens: 1,
                reserved_output_tokens: 1,
                reserved_tool_units: 0,
                reserved_image_units: 0,
                reserved_cost_microunits: 1,
                reserved_runs: 1,
                actual_input_tokens: Some(1),
                actual_cached_input_tokens: Some(0),
                actual_output_tokens: Some(1),
                actual_tool_units: Some(0),
                actual_image_units: Some(0),
                actual_cost_microunits: Some(1),
                actual_runs: Some(1),
                idempotency_key: format!("checkpoint-retention-{run_id}"),
                state: "committed".to_owned(),
                expires_at: wall_clock + 3_600,
                reconciled_at: Some(wall_clock),
            },
        )
        .await
        .expect("checkpoint budget should seed");
        let checkpoint_id = Uuid::new_v4();
        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let run = tx
                        .find_by_id::<AiRunRecord>(&run_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    let updated = tx
                        .compare_and_swap::<AiRunRecord>(
                            &run.id,
                            run.row_version,
                            AiRunRecordWhereInput::default(),
                            UpdateAiRunRecordInput {
                                attempt_id: Some(Some(attempt_id)),
                                lease_generation: Some(1),
                                latest_checkpoint_id: make_current.then_some(Some(checkpoint_id)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(updated, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    tx.insert::<AiRunCheckpointRecord>(CreateAiRunCheckpointRecordInput {
                        id: checkpoint_id,
                        run_id,
                        attempt_id,
                        lease_generation: 1,
                        checkpoint_kind: "provider_turn_persisted".to_owned(),
                        provider_response_id: Some(provider_response_id),
                        budget_reservation_id: Some(budget.id),
                        assistant_message_id: None,
                        protected_state: Some(serde_json::json!({"protected": "provider"})),
                        checkpoint_hash: "a".repeat(64),
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    Ok(())
                })
            })
            .await
            .expect("protected checkpoint should seed");
        let checkpoint = AiRunCheckpointRecord::find_by_id(database, &checkpoint_id)
            .await
            .expect("protected checkpoint lookup should succeed")
            .expect("protected checkpoint should exist");
        if with_outcome {
            AiRunAttemptOutcomeRecord::insert(
                database,
                CreateAiRunAttemptOutcomeRecordInput {
                    attempt_id,
                    run_id,
                    lease_generation: 1,
                    worker_id,
                    final_state: "completed".to_owned(),
                    outcome_code: "retention_test_completed".to_owned(),
                    provider_response_id: checkpoint.provider_response_id.clone(),
                    finished_at: checkpoint.created_at + 60,
                },
            )
            .await
            .expect("checkpoint attempt outcome should seed");
        }
        checkpoint
    }

    async fn seed_final_output_checkpoint(
        database: &Database<SqliteBackend>,
        session_id: Uuid,
        run_id: Uuid,
        provider_checkpoint: &AiRunCheckpointRecord,
    ) -> Uuid {
        let provider_checkpoint = provider_checkpoint.clone();
        let message_id = Uuid::new_v4();
        let checkpoint_hash = crate::orm_runs::final_output_checkpoint_hash(
            crate::AiRunId(run_id),
            provider_checkpoint.attempt_id,
            provider_checkpoint.lease_generation,
            message_id,
            provider_checkpoint.provider_response_id.as_deref(),
            provider_checkpoint
                .budget_reservation_id
                .expect("test provider checkpoint should bind a budget"),
        );
        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&session_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    let run = tx
                        .find_by_id::<AiRunRecord>(&run_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    let session_update = tx
                        .compare_and_swap::<AiSessionRecord>(
                            &session.id,
                            session.row_version,
                            AiSessionRecordWhereInput::default(),
                            UpdateAiSessionRecordInput {
                                message_head: Some(2),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    let run_update = tx
                        .compare_and_swap::<AiRunRecord>(
                            &run.id,
                            run.row_version,
                            AiRunRecordWhereInput::default(),
                            UpdateAiRunRecordInput {
                                latest_checkpoint_id: Some(Some(message_id)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(session_update, ConditionalUpdateOutcome::Updated(_))
                        || !matches!(run_update, ConditionalUpdateOutcome::Updated(_))
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    tx.insert::<AiMessageRecord>(CreateAiMessageRecordInput {
                        id: message_id,
                        session_id,
                        sequence: 2,
                        message_role: "assistant".to_owned(),
                        author_principal_kind: None,
                        author_subject: None,
                        client_message_id: None,
                        content_hash: None,
                        run_id: Some(run_id),
                        provider_kind: Some("mock".to_owned()),
                        provider_model: Some("retention-test".to_owned()),
                        protected_preview: Some(serde_json::json!({"protected": "assistant"})),
                        block_count: 1,
                        completion_state: "complete".to_owned(),
                        finalized_at: Some(provider_checkpoint.created_at),
                        content_purged_at: None,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.insert::<AiMessageBlockRecord>(CreateAiMessageBlockRecordInput {
                        id: Uuid::new_v4(),
                        message_id,
                        block_index: 0,
                        block_kind: "text".to_owned(),
                        protected_content: serde_json::json!({"protected": "assistant-block"}),
                        byte_count: 9,
                        line_count: 1,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.insert::<AiRunCheckpointRecord>(CreateAiRunCheckpointRecordInput {
                        id: message_id,
                        run_id,
                        attempt_id: provider_checkpoint.attempt_id,
                        lease_generation: provider_checkpoint.lease_generation,
                        checkpoint_kind: "assistant_output_persisted".to_owned(),
                        provider_response_id: provider_checkpoint.provider_response_id,
                        budget_reservation_id: provider_checkpoint.budget_reservation_id,
                        assistant_message_id: Some(message_id),
                        protected_state: None,
                        checkpoint_hash,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    Ok(())
                })
            })
            .await
            .expect("final-output checkpoint should seed");
        message_id
    }

    async fn bind_tool_call_to_checkpoint(
        database: &Database<SqliteBackend>,
        call_id: Uuid,
        checkpoint: &AiRunCheckpointRecord,
    ) {
        let checkpoint = checkpoint.clone();
        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let call = tx
                        .find_by_id::<AiToolCallRecord>(&call_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    let step = tx
                        .find_by_id::<AiRunStepRecord>(&call_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    let call_update = tx
                        .compare_and_swap::<AiToolCallRecord>(
                            &call.id,
                            call.row_version,
                            AiToolCallRecordWhereInput::default(),
                            UpdateAiToolCallRecordInput {
                                provider_response_id: Some(checkpoint.provider_response_id.clone()),
                                budget_reservation_id: Some(checkpoint.budget_reservation_id),
                                lease_generation: Some(checkpoint.lease_generation),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    let step_update = tx
                        .compare_and_swap::<AiRunStepRecord>(
                            &step.id,
                            step.row_version,
                            AiRunStepRecordWhereInput::default(),
                            UpdateAiRunStepRecordInput {
                                lease_generation: Some(checkpoint.lease_generation),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(call_update, ConditionalUpdateOutcome::Updated(_))
                        || !matches!(step_update, ConditionalUpdateOutcome::Updated(_))
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    Ok(())
                })
            })
            .await
            .expect("checkpoint tool dependency should bind");
    }

    async fn seed_context_checkpoints(database: &Database<SqliteBackend>, session_id: Uuid) {
        for index in 0..2 {
            AiContextCheckpointRecord::insert(
                database,
                CreateAiContextCheckpointRecordInput {
                    id: Uuid::new_v4(),
                    session_id,
                    through_sequence: 1,
                    source_hash: format!("context-source-{index}"),
                    token_estimate: 10,
                    provider_kind: "mock".to_owned(),
                    provider_model: "retention-test".to_owned(),
                    protected_summary: serde_json::json!({"protected": true}),
                    invalidated_at: None,
                },
            )
            .await
            .expect("context checkpoint should seed");
        }
    }

    async fn seed_inbox_event(
        database: &Database<SqliteBackend>,
        scope: &AiScope,
        session_id: Uuid,
    ) {
        AiInboxEventRecord::insert(
            database,
            CreateAiInboxEventRecordInput {
                id: Uuid::new_v4(),
                principal_kind: "user".to_owned(),
                principal_subject: "retention-user".to_owned(),
                scope_key: crate::ai_scope_key(scope),
                scope_kind: scope.kind.clone(),
                scope_id: scope.id.clone(),
                tenant_id: scope.tenant_id.clone(),
                sequence: 1,
                session_id: Some(session_id),
                event_type: "session_deleting".to_owned(),
                protected_payload: Some(serde_json::json!({"protected": "inbox content"})),
                payload_purged_at: None,
            },
        )
        .await
        .expect("inbox event should seed");
    }

    #[tokio::test]
    async fn deleting_session_inbox_and_message_content_precede_shell_finalization() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy(&database, &scope).await;
        let session_id = seed_session(&database, &scope).await;
        let (message_id, _) = seed_message(&database, session_id, "completed", false).await;
        seed_inbox_event(&database, &scope, session_id).await;
        mark_session_deleting(&database, session_id, 120).await;
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            Arc::new(FixedClock::new(now())),
            AiSessionRetentionLimits::default(),
        );

        let inbox_pass = service
            .prune_session_content(None)
            .await
            .expect("inbox retention should succeed");
        assert_eq!(inbox_pass.deleting_session_inbox_payloads_purged, 1);
        assert_eq!(inbox_pass.message_contents_purged, 0);
        assert_eq!(inbox_pass.deleting_sessions_finalized, 0);

        let message_pass = service
            .prune_session_content(None)
            .await
            .expect("message retention should succeed");
        assert_eq!(message_pass.deleting_session_inbox_payloads_purged, 0);
        assert_eq!(message_pass.message_contents_purged, 1);
        assert_eq!(message_pass.deleting_sessions_finalized, 0);

        let final_pass = service
            .prune_session_content(None)
            .await
            .expect("session finalization should succeed");
        assert_eq!(final_pass.deleting_sessions_finalized, 1);
        assert_eq!(final_pass.sessions_changed, 1);
        let session = AiSessionRecord::find_by_id(&database, &session_id)
            .await
            .expect("session lookup should succeed")
            .expect("redacted session shell should remain");
        assert_eq!(session.state, "deleted");
        assert!(session.title.is_empty());
        assert!(session.deleted_at.is_some());
        let message = AiMessageRecord::find_by_id(&database, &message_id)
            .await
            .expect("message lookup should succeed")
            .expect("redacted message metadata should remain");
        assert!(message.protected_preview.is_none());
        assert_eq!(message.block_count, 0);
        assert!(message.content_purged_at.is_some());

        let replay = service
            .prune_session_content(None)
            .await
            .expect("finalized shell should leave the candidate set");
        assert_eq!(replay.sessions_scanned, 0);
        assert_eq!(replay.deleting_sessions_finalized, 0);
    }

    #[tokio::test]
    async fn expired_delta_and_terminal_message_content_are_pruned_atomically() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy(&database, &scope).await;
        let session_id = seed_session(&database, &scope).await;
        let (message_id, _) = seed_message(&database, session_id, "completed", false).await;
        seed_events(&database, session_id).await;
        seed_context_checkpoints(&database, session_id).await;
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            Arc::new(FixedClock::new(now())),
            AiSessionRetentionLimits::default(),
        );

        let report = service
            .prune_session_content(None)
            .await
            .expect("retention pass should succeed");
        assert_eq!(report.sessions_scanned, 1);
        assert_eq!(report.sessions_changed, 1);
        assert_eq!(report.live_delta_events_deleted, 1);
        assert_eq!(report.context_checkpoints_invalidated, 2);
        assert_eq!(report.message_contents_purged, 1);
        assert_eq!(report.message_blocks_deleted, 1);
        assert!(report.next_session_cursor.is_none());

        let message = AiMessageRecord::find_by_id(&database, &message_id)
            .await
            .expect("message lookup should succeed")
            .expect("message metadata should remain");
        assert!(message.protected_preview.is_none());
        assert_eq!(message.block_count, 0);
        assert_eq!(message.content_purged_at, Some(now().unix_timestamp()));
        let (events, contexts, blocks, audits) = database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    let events = tx
                        .query::<AiSessionEventRecord>()
                        .filter(AiSessionEventRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .default_order()
                        .limit(10)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let contexts = tx
                        .query::<AiContextCheckpointRecord>()
                        .filter(AiContextCheckpointRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(10)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let blocks = tx
                        .query::<AiMessageBlockRecord>()
                        .filter(AiMessageBlockRecordWhereInput {
                            message_id: Some(UuidFilter {
                                eq: Some(message_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(10)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let audits = tx
                        .query::<AiAuditEventRecord>()
                        .limit(10)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    Ok((events, contexts, blocks, audits))
                })
            })
            .await
            .expect("retention results should load");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "message_queued");
        assert_eq!(events[1].event_type, "run_completed");
        assert!(contexts.is_empty());
        assert!(blocks.is_empty());
        assert_eq!(audits.len(), 1);
        assert_eq!(audits[0].action, "prune_session_content");

        let session_service = crate::OrmAiSessionService::new(
            database.clone(),
            Arc::new(AllowAll),
            Arc::new(ProtectionPolicy),
            Arc::new(crate::DatabaseManagedContentProtector),
        );
        let gap = session_service
            .session_event_page(&principal(), crate::AiSessionId(session_id), 0, 10)
            .await
            .expect("retention gap should be represented safely");
        assert!(gap.reset_required);
        assert!(gap.events.is_empty());
        assert_eq!(gap.watermark, 3);

        let messages = session_service
            .messages(
                &principal(),
                crate::AiSessionId(session_id),
                KeysetConnectionInput {
                    first: Some(10),
                    ..Default::default()
                }
                .validate(10, 100)
                .expect("message page should validate"),
            )
            .await
            .expect("retained message metadata should remain readable");
        assert_eq!(messages.edges.len(), 1);
        assert!(messages.edges[0].node.content_purged);
        assert_eq!(
            messages.edges[0].node.preview,
            "Content removed by retention policy"
        );
        assert_eq!(messages.edges[0].node.block_count, 0);
        assert!(
            session_service
                .message_blocks(&principal(), message_id, None, 10)
                .await
                .expect("purged message block window should remain readable")
                .is_empty()
        );

        let replay = service
            .prune_session_content(None)
            .await
            .expect("retention replay should be idempotent");
        assert_eq!(replay.sessions_changed, 0);
    }

    #[tokio::test]
    async fn overbound_context_set_blocks_ordinary_message_purge_without_partial_invalidation() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy(&database, &scope).await;
        let session_id = seed_session(&database, &scope).await;
        let (message_id, _) = seed_message(&database, session_id, "completed", false).await;
        seed_context_checkpoints(&database, session_id).await;
        let limits = AiSessionRetentionLimits::new_with_context_checkpoints(10, 10, 1, 10, 100)
            .expect("retention limits should validate");
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            Arc::new(FixedClock::new(now())),
            limits,
        );

        let report = service
            .prune_session_content(None)
            .await
            .expect("overbound retention pass should remain safe");
        assert_eq!(report.context_checkpoints_invalidated, 0);
        assert_eq!(report.message_contents_purged, 0);
        assert_eq!(report.messages_blocked, 1);
        let message = AiMessageRecord::find_by_id(&database, &message_id)
            .await
            .expect("message lookup should succeed")
            .expect("message should remain");
        assert!(message.content_purged_at.is_none());
        assert!(message.protected_preview.is_some());
        let contexts = database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    tx.query::<AiContextCheckpointRecord>()
                        .filter(AiContextCheckpointRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(10)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("context rows should load");
        assert_eq!(contexts.len(), 2);
    }

    #[tokio::test]
    async fn deleting_session_cutoff_orders_context_before_events_and_message_content() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy_with_message_retention(&database, &scope, None).await;
        let session_id = seed_session(&database, &scope).await;
        let (message_id, run_id) = seed_message(&database, session_id, "completed", false).await;
        let checkpoint_ids = seed_run_checkpoints(&database, run_id, 2).await;
        seed_events(&database, session_id).await;
        seed_context_checkpoints(&database, session_id).await;
        mark_session_deleting(&database, session_id, 30).await;
        let clock = Arc::new(FixedClock::new(now()));
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            clock.clone(),
            AiSessionRetentionLimits::new_with_context_checkpoints(50, 1, 1, 100, 5_000)
                .expect("deleting-session retention limits should validate")
                .with_run_checkpoint_limits(10, 1)
                .expect("run-checkpoint retention limits should validate"),
        );

        let before_cutoff = service
            .prune_session_content(None)
            .await
            .expect("pre-cutoff deleting-session pass should succeed");
        assert_eq!(before_cutoff.deleting_session_events_deleted, 0);
        assert_eq!(
            before_cutoff.deleting_session_context_checkpoints_deleted,
            0
        );
        assert_eq!(before_cutoff.message_contents_purged, 0);
        let retained_message = AiMessageRecord::find_by_id(&database, &message_id)
            .await
            .expect("pre-cutoff message lookup should succeed")
            .expect("pre-cutoff message should remain durable");
        assert!(retained_message.protected_preview.is_some());

        clock.advance_seconds(31);
        let report = service
            .prune_session_content(None)
            .await
            .expect("expired deleting-session retention pass should succeed");
        assert_eq!(report.sessions_changed, 1);
        assert_eq!(report.live_delta_events_deleted, 0);
        assert_eq!(report.deleting_session_events_deleted, 1);
        assert_eq!(report.deleting_session_context_checkpoints_deleted, 1);
        assert_eq!(report.message_contents_purged, 0);
        assert_eq!(report.message_blocks_deleted, 0);

        let next_page = service
            .prune_session_content(None)
            .await
            .expect("next bounded deleting-session retention pass should succeed");
        assert_eq!(next_page.sessions_changed, 1);
        assert_eq!(next_page.deleting_session_events_deleted, 1);
        assert_eq!(next_page.deleting_session_context_checkpoints_deleted, 1);
        assert_eq!(next_page.message_contents_purged, 0);

        let content_page = service
            .prune_session_content(None)
            .await
            .expect("post-checkpoint deleting-session retention pass should succeed");
        assert_eq!(content_page.sessions_changed, 1);
        assert_eq!(content_page.deleting_session_events_deleted, 0);
        assert_eq!(content_page.deleting_session_context_checkpoints_deleted, 0);
        assert_eq!(content_page.message_contents_purged, 1);
        assert_eq!(content_page.message_blocks_deleted, 1);
        assert_eq!(
            content_page.deleting_session_run_checkpoint_references_cleared,
            0
        );
        assert_eq!(content_page.deleting_session_run_checkpoints_deleted, 0);

        let first_checkpoint_page = service
            .prune_session_content(None)
            .await
            .expect("first bounded run-checkpoint retention pass should succeed");
        assert_eq!(first_checkpoint_page.sessions_changed, 1);
        assert_eq!(
            first_checkpoint_page.deleting_session_run_checkpoint_references_cleared,
            1
        );
        assert_eq!(
            first_checkpoint_page.deleting_session_run_checkpoints_deleted,
            1
        );
        let run = AiRunRecord::find_by_id(&database, &run_id)
            .await
            .expect("run lookup should succeed")
            .expect("run metadata should remain");
        assert!(run.latest_checkpoint_id.is_none());

        let second_checkpoint_page = service
            .prune_session_content(None)
            .await
            .expect("second bounded run-checkpoint retention pass should succeed");
        assert_eq!(second_checkpoint_page.sessions_changed, 1);
        assert_eq!(
            second_checkpoint_page.deleting_session_run_checkpoint_references_cleared,
            0
        );
        assert_eq!(
            second_checkpoint_page.deleting_session_run_checkpoints_deleted,
            1
        );
        assert_eq!(second_checkpoint_page.deleting_sessions_finalized, 1);

        let message = AiMessageRecord::find_by_id(&database, &message_id)
            .await
            .expect("deleting-session message lookup should succeed")
            .expect("message metadata should remain");
        assert!(message.protected_preview.is_none());
        assert_eq!(message.block_count, 0);
        assert_eq!(
            message.content_purged_at,
            Some((now() + time::Duration::seconds(31)).unix_timestamp())
        );
        let (events, context_checkpoints, run_checkpoints, audits) = database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    let events = tx
                        .query::<AiSessionEventRecord>()
                        .filter(AiSessionEventRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(10)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let audits = tx
                        .query::<AiAuditEventRecord>()
                        .limit(10)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let context_checkpoints = tx
                        .query::<AiContextCheckpointRecord>()
                        .filter(AiContextCheckpointRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(10)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let run_checkpoints = tx
                        .query::<AiRunCheckpointRecord>()
                        .filter(AiRunCheckpointRecordWhereInput {
                            id: Some(UuidFilter {
                                in_list: Some(checkpoint_ids),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(10)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    Ok((events, context_checkpoints, run_checkpoints, audits))
                })
            })
            .await
            .expect("deleting-session retention facts should load");
        assert!(events.is_empty());
        assert!(context_checkpoints.is_empty());
        assert!(run_checkpoints.is_empty());
        assert_eq!(audits.len(), 8);
        assert_eq!(
            audits
                .iter()
                .filter(|audit| audit.reason_code == "scope_retention_expired")
                .count(),
            1
        );
        assert_eq!(
            audits
                .iter()
                .filter(|audit| audit.reason_code == "session_deletion_retention_expired")
                .count(),
            6
        );
        assert_eq!(
            audits
                .iter()
                .filter(|audit| audit.reason_code == "session_content_dependencies_exhausted")
                .count(),
            1
        );

        let replay = service
            .prune_session_content(None)
            .await
            .expect("deleting-session retention replay should be idempotent");
        assert_eq!(replay.sessions_changed, 0);
        assert_eq!(replay.sessions_scanned, 0);
        assert_eq!(replay.deleting_session_events_deleted, 0);
        assert_eq!(replay.deleting_session_run_checkpoints_deleted, 0);
    }

    #[tokio::test]
    async fn nonterminal_and_attachment_content_remain_blocked() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy(&database, &scope).await;
        let first_session = seed_session(&database, &scope).await;
        let (active_message, _) = seed_message(&database, first_session, "running", false).await;
        let second_session = seed_session(&database, &scope).await;
        let (attached_message, _) =
            seed_message(&database, second_session, "completed", true).await;
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            Arc::new(FixedClock::new(now())),
            AiSessionRetentionLimits::default(),
        );

        let report = service
            .prune_session_content(None)
            .await
            .expect("blocked retention pass should complete safely");
        assert_eq!(report.sessions_scanned, 2);
        assert_eq!(report.sessions_changed, 0);
        assert_eq!(report.messages_blocked, 2);
        for message_id in [active_message, attached_message] {
            let message = AiMessageRecord::find_by_id(&database, &message_id)
                .await
                .expect("message lookup should succeed")
                .expect("blocked message should remain");
            assert!(message.protected_preview.is_some());
            assert!(message.content_purged_at.is_none());
        }
    }

    #[tokio::test]
    async fn deleting_session_proposal_payloads_precede_message_scrubbing() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy(&database, &scope).await;
        let session_id = seed_session(&database, &scope).await;
        let (message_id, run_id) = seed_message(&database, session_id, "completed", false).await;
        let (proposal_id, item_id) = seed_proposal(
            &database,
            &scope,
            session_id,
            run_id,
            "pending_review",
            Some(now().unix_timestamp() - 1),
            true,
        )
        .await;
        mark_session_deleting(&database, session_id, 120).await;
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            Arc::new(FixedClock::new(now())),
            AiSessionRetentionLimits::default(),
        );

        let proposal_pass = service
            .prune_session_content(None)
            .await
            .expect("retention should scrub the expired proposal first");
        assert_eq!(proposal_pass.sessions_changed, 1);
        assert_eq!(proposal_pass.deleting_session_proposal_payloads_purged, 1);
        assert_eq!(proposal_pass.proposal_payload_purges_blocked, 0);
        assert_eq!(proposal_pass.message_contents_purged, 0);
        let proposal = AiProposalRecord::find_by_id(&database, &proposal_id)
            .await
            .expect("proposal lookup should succeed")
            .expect("proposal metadata should remain");
        assert_eq!(proposal.state, "expired");
        assert!(proposal.protected_payload.is_none());
        assert!(proposal.source_references.is_none());
        assert_eq!(proposal.payload_purged_at, Some(now().unix_timestamp()));
        let item = AiProposalItemRecord::find_by_id(
            &database,
            &item_id.expect("proposal item should exist"),
        )
        .await
        .expect("proposal-item lookup should succeed")
        .expect("proposal-item metadata should remain");
        assert!(item.protected_suggested_value.is_none());
        assert!(item.protected_rationale.is_none());
        assert!(item.source_references.is_none());
        assert!(item.protected_review_value.is_none());
        assert_eq!(item.review_decision.as_deref(), Some("accepted"));
        let message = AiMessageRecord::find_by_id(&database, &message_id)
            .await
            .expect("message lookup should succeed")
            .expect("message should remain");
        assert!(message.protected_preview.is_some());

        let message_pass = service
            .prune_session_content(None)
            .await
            .expect("later retention should scrub message content");
        assert_eq!(message_pass.deleting_session_proposal_payloads_purged, 0);
        assert_eq!(message_pass.message_contents_purged, 1);
        let message = AiMessageRecord::find_by_id(&database, &message_id)
            .await
            .expect("message lookup should succeed")
            .expect("message metadata should remain");
        assert!(message.protected_preview.is_none());
        assert!(message.content_purged_at.is_some());
    }

    #[tokio::test]
    async fn accepted_proposal_blocks_deleting_session_payload_retention() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy(&database, &scope).await;
        let session_id = seed_session(&database, &scope).await;
        let (message_id, run_id) = seed_message(&database, session_id, "completed", false).await;
        let (proposal_id, _) = seed_proposal(
            &database, &scope, session_id, run_id, "accepted", None, false,
        )
        .await;
        mark_session_deleting(&database, session_id, 120).await;
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            Arc::new(FixedClock::new(now())),
            AiSessionRetentionLimits::default(),
        );

        let report = service
            .prune_session_content(None)
            .await
            .expect("accepted proposal should fail closed without mutation");
        assert_eq!(report.sessions_changed, 0);
        assert_eq!(report.deleting_session_proposal_payloads_purged, 0);
        assert_eq!(report.proposal_payload_purges_blocked, 1);
        assert_eq!(report.message_contents_purged, 0);
        let proposal = AiProposalRecord::find_by_id(&database, &proposal_id)
            .await
            .expect("proposal lookup should succeed")
            .expect("accepted proposal should remain");
        assert!(proposal.protected_payload.is_some());
        assert!(proposal.source_references.is_some());
        assert!(proposal.payload_purged_at.is_none());
        let message = AiMessageRecord::find_by_id(&database, &message_id)
            .await
            .expect("message lookup should succeed")
            .expect("message should remain");
        assert!(message.protected_preview.is_some());
    }

    #[tokio::test]
    async fn proposal_lookahead_blocks_the_whole_deleting_session_set() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy(&database, &scope).await;
        let session_id = seed_session(&database, &scope).await;
        let (_, run_id) = seed_message(&database, session_id, "completed", false).await;
        let first = seed_proposal(
            &database, &scope, session_id, run_id, "rejected", None, false,
        )
        .await
        .0;
        let second = seed_proposal(
            &database, &scope, session_id, run_id, "rejected", None, false,
        )
        .await
        .0;
        mark_session_deleting(&database, session_id, 120).await;
        let limits = AiSessionRetentionLimits::default()
            .with_proposal_limits(1, 5_000)
            .expect("proposal lookahead limit should validate");
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            Arc::new(FixedClock::new(now())),
            limits,
        );

        let report = service
            .prune_session_content(None)
            .await
            .expect("over-bound proposals should remain closed");
        assert_eq!(report.sessions_changed, 0);
        assert_eq!(report.proposal_payload_purges_blocked, 1);
        assert_eq!(report.deleting_session_proposal_payloads_purged, 0);
        for proposal_id in [first, second] {
            let proposal = AiProposalRecord::find_by_id(&database, &proposal_id)
                .await
                .expect("proposal lookup should succeed")
                .expect("over-bound proposal should remain");
            assert!(proposal.protected_payload.is_some());
            assert!(proposal.payload_purged_at.is_none());
        }
    }

    #[tokio::test]
    async fn deleting_session_tool_and_approval_payloads_precede_message_scrubbing() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy(&database, &scope).await;
        let session_id = seed_session(&database, &scope).await;
        let (message_id, run_id) = seed_message(&database, session_id, "completed", false).await;
        let (call_id, approval_id) =
            seed_tool_call(&database, session_id, run_id, "completed", Some("consumed")).await;
        let checkpoint_ids = seed_run_checkpoints(&database, run_id, 1).await;
        mark_session_deleting(&database, session_id, 120).await;
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            Arc::new(FixedClock::new(now())),
            AiSessionRetentionLimits::default(),
        );

        let tool_pass = service
            .prune_session_content(None)
            .await
            .expect("retention should scrub terminal tool authority first");
        assert_eq!(tool_pass.sessions_changed, 1);
        assert_eq!(tool_pass.deleting_session_tool_payloads_purged, 1);
        assert_eq!(tool_pass.deleting_session_approval_payloads_purged, 1);
        assert_eq!(tool_pass.tool_payload_purges_blocked, 0);
        assert_eq!(tool_pass.message_contents_purged, 0);
        assert_eq!(
            tool_pass.deleting_session_run_checkpoint_references_cleared,
            0
        );
        assert_eq!(tool_pass.deleting_session_run_checkpoints_deleted, 0);
        let call = AiToolCallRecord::find_by_id(&database, &call_id)
            .await
            .expect("tool-call lookup should succeed")
            .expect("tool-call metadata should remain");
        assert_eq!(call.state, "completed");
        assert!(call.protected_arguments.is_none());
        assert!(call.protected_result.is_none());
        assert_eq!(call.payload_purged_at, Some(now().unix_timestamp()));
        assert_eq!(call.argument_hash, "argument-hash");
        let approval_id = approval_id.expect("approval should exist");
        let approval = AiApprovalRecord::find_by_id(&database, &approval_id)
            .await
            .expect("approval lookup should succeed")
            .expect("approval metadata should remain");
        assert_eq!(approval.state, "consumed");
        assert!(approval.protected_resource_bindings.is_none());
        assert!(approval.protected_action_preview.is_none());
        assert_eq!(approval.payload_purged_at, Some(now().unix_timestamp()));
        assert_eq!(approval.consumed_uses, 1);
        let message = AiMessageRecord::find_by_id(&database, &message_id)
            .await
            .expect("message lookup should succeed")
            .expect("message should remain");
        assert!(message.protected_preview.is_some());

        let message_pass = service
            .prune_session_content(None)
            .await
            .expect("later retention should scrub message content");
        assert_eq!(message_pass.deleting_session_tool_payloads_purged, 0);
        assert_eq!(message_pass.deleting_session_approval_payloads_purged, 0);
        assert_eq!(message_pass.message_contents_purged, 1);
        assert_eq!(
            message_pass.deleting_session_run_checkpoint_references_cleared,
            0
        );
        assert_eq!(message_pass.deleting_session_run_checkpoints_deleted, 0);

        let checkpoint_pass = service
            .prune_session_content(None)
            .await
            .expect("later retention should re-prove tombstones and purge checkpoints");
        assert_eq!(
            checkpoint_pass.deleting_session_run_checkpoint_references_cleared,
            1
        );
        assert_eq!(checkpoint_pass.deleting_session_run_checkpoints_deleted, 1);
        let checkpoints = database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    tx.query::<AiRunCheckpointRecord>()
                        .filter(AiRunCheckpointRecordWhereInput {
                            id: Some(UuidFilter {
                                in_list: Some(checkpoint_ids),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("checkpoint proof should load");
        assert!(checkpoints.is_empty());
    }

    #[tokio::test]
    async fn raw_payload_retention_purges_only_expired_orphaned_protected_checkpoints() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy_with_message_retention(&database, &scope, None).await;
        let session_id = seed_session(&database, &scope).await;
        let (message_id, orphan_run_id) =
            seed_message(&database, session_id, "completed", false).await;
        let current_run_id = Uuid::new_v4();
        AiRunRecord::insert(
            &database,
            CreateAiRunRecordInput {
                id: current_run_id,
                session_id,
                input_message_id: message_id,
                principal_reference: serde_json::json!({"test": true}),
                state: "completed".to_owned(),
                attempt_id: None,
                lease_owner: None,
                lease_generation: 0,
                lease_expires_at: None,
                lease_heartbeat_at: None,
                retry_count: 0,
                next_attempt_at: None,
                error_code: None,
                latest_checkpoint_id: None,
            },
        )
        .await
        .expect("current-checkpoint run should seed");
        let final_run_id = Uuid::new_v4();
        AiRunRecord::insert(
            &database,
            CreateAiRunRecordInput {
                id: final_run_id,
                session_id,
                input_message_id: message_id,
                principal_reference: serde_json::json!({"test": true}),
                state: "completed".to_owned(),
                attempt_id: None,
                lease_owner: None,
                lease_generation: 0,
                lease_expires_at: None,
                lease_heartbeat_at: None,
                retry_count: 0,
                next_attempt_at: None,
                error_code: None,
                latest_checkpoint_id: None,
            },
        )
        .await
        .expect("final-output run should seed");
        let orphan =
            seed_protected_checkpoint_history(&database, session_id, orphan_run_id, false, true)
                .await;
        let current =
            seed_protected_checkpoint_history(&database, session_id, current_run_id, true, true)
                .await;
        let final_provider =
            seed_protected_checkpoint_history(&database, session_id, final_run_id, false, true)
                .await;
        let final_checkpoint_id =
            seed_final_output_checkpoint(&database, session_id, final_run_id, &final_provider)
                .await;
        let expired_call = seed_tool_call(&database, session_id, orphan_run_id, "completed", None)
            .await
            .0;
        bind_tool_call_to_checkpoint(&database, expired_call, &orphan).await;
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            Arc::new(FixedClock::new(now())),
            AiSessionRetentionLimits::default(),
        );

        let report = service
            .prune_session_content(None)
            .await
            .expect("expired orphaned checkpoint retention should succeed");
        assert_eq!(report.sessions_changed, 1);
        assert_eq!(report.expired_tool_payloads_purged, 1);
        assert_eq!(report.expired_run_checkpoints_deleted, 2);
        assert_eq!(report.raw_checkpoint_purges_blocked, 0);
        assert!(
            AiRunCheckpointRecord::find_by_id(&database, &orphan.id)
                .await
                .expect("orphaned checkpoint lookup should succeed")
                .is_none()
        );
        assert!(
            AiRunCheckpointRecord::find_by_id(&database, &final_provider.id)
                .await
                .expect("final provider checkpoint lookup should succeed")
                .is_none()
        );
        assert!(
            AiRunCheckpointRecord::find_by_id(&database, &final_checkpoint_id)
                .await
                .expect("final output checkpoint lookup should succeed")
                .is_some()
        );
        let retained = AiRunCheckpointRecord::find_by_id(&database, &current.id)
            .await
            .expect("current checkpoint lookup should succeed")
            .expect("current protected checkpoint must remain");
        assert!(retained.protected_state.is_some());
        let current_run = AiRunRecord::find_by_id(&database, &current_run_id)
            .await
            .expect("current checkpoint run lookup should succeed")
            .expect("current checkpoint run should remain");
        assert_eq!(current_run.latest_checkpoint_id, Some(current.id));

        let replay = service
            .prune_session_content(None)
            .await
            .expect("expired checkpoint replay should be idempotent");
        assert_eq!(replay.sessions_changed, 0);
        assert_eq!(replay.expired_run_checkpoints_deleted, 0);
    }

    #[tokio::test]
    async fn raw_checkpoint_retention_requires_exact_attempt_outcome_history() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy_with_message_retention(&database, &scope, None).await;
        let session_id = seed_session(&database, &scope).await;
        let (_, run_id) = seed_message(&database, session_id, "completed", false).await;
        let checkpoint =
            seed_protected_checkpoint_history(&database, session_id, run_id, false, false).await;
        let retention_now = OffsetDateTime::from_unix_timestamp(checkpoint.created_at + 90)
            .expect("blocked checkpoint retention clock should validate");
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            Arc::new(FixedClock::new(retention_now)),
            AiSessionRetentionLimits::default(),
        );

        let report = service
            .prune_session_content(None)
            .await
            .expect("missing outcome history should fail closed");
        assert_eq!(report.sessions_changed, 0);
        assert_eq!(report.expired_run_checkpoints_deleted, 0);
        assert_eq!(report.raw_checkpoint_purges_blocked, 1);
        assert!(
            AiRunCheckpointRecord::find_by_id(&database, &checkpoint.id)
                .await
                .expect("blocked checkpoint lookup should succeed")
                .is_some()
        );
    }

    #[tokio::test]
    async fn raw_payload_retention_scrubs_only_age_expired_terminal_tool_authority() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy_with_message_retention(&database, &scope, None).await;
        let session_id = seed_session(&database, &scope).await;
        let (message_id, run_id) = seed_message(&database, session_id, "completed", false).await;
        let expired =
            seed_tool_call(&database, session_id, run_id, "completed", Some("consumed")).await;
        let fresh_terminal = seed_tool_call_completed_seconds_ago(
            &database,
            session_id,
            run_id,
            "completed",
            None,
            30,
        )
        .await
        .0;
        let active_run_id = Uuid::new_v4();
        AiRunRecord::insert(
            &database,
            CreateAiRunRecordInput {
                id: active_run_id,
                session_id,
                input_message_id: message_id,
                principal_reference: serde_json::json!({"test": true}),
                state: "running".to_owned(),
                attempt_id: Some(Uuid::new_v4()),
                lease_owner: Some("active-retention-test".to_owned()),
                lease_generation: 1,
                lease_expires_at: Some(now().unix_timestamp() + 300),
                lease_heartbeat_at: Some(now().unix_timestamp()),
                retry_count: 0,
                next_attempt_at: None,
                error_code: None,
                latest_checkpoint_id: None,
            },
        )
        .await
        .expect("active run should seed");
        let fresh = seed_tool_call(
            &database,
            session_id,
            active_run_id,
            "waiting_approval",
            Some("approved"),
        )
        .await;
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            Arc::new(FixedClock::new(now())),
            AiSessionRetentionLimits::default(),
        );

        let report = service
            .prune_session_content(None)
            .await
            .expect("raw payload retention should prune exact expired authority");
        assert_eq!(report.sessions_changed, 1);
        assert_eq!(report.expired_tool_payloads_purged, 1);
        assert_eq!(report.expired_approval_payloads_purged, 1);
        assert_eq!(report.raw_payload_purges_blocked, 0);
        assert_eq!(report.deleting_session_tool_payloads_purged, 0);
        assert_eq!(report.deleting_session_approval_payloads_purged, 0);

        let expired_call = AiToolCallRecord::find_by_id(&database, &expired.0)
            .await
            .expect("expired tool lookup should succeed")
            .expect("expired tool metadata should remain");
        assert!(expired_call.protected_arguments.is_none());
        assert!(expired_call.protected_result.is_none());
        assert_eq!(expired_call.payload_purged_at, Some(now().unix_timestamp()));
        let expired_approval = AiApprovalRecord::find_by_id(
            &database,
            &expired.1.expect("expired approval should exist"),
        )
        .await
        .expect("expired approval lookup should succeed")
        .expect("expired approval metadata should remain");
        assert!(expired_approval.protected_resource_bindings.is_none());
        assert!(expired_approval.protected_action_preview.is_none());
        assert_eq!(
            expired_approval.payload_purged_at,
            Some(now().unix_timestamp())
        );

        let fresh_terminal_call = AiToolCallRecord::find_by_id(&database, &fresh_terminal)
            .await
            .expect("fresh terminal tool lookup should succeed")
            .expect("fresh terminal tool should remain");
        assert!(fresh_terminal_call.protected_arguments.is_some());
        assert!(fresh_terminal_call.protected_result.is_some());
        assert!(fresh_terminal_call.payload_purged_at.is_none());

        let fresh_call = AiToolCallRecord::find_by_id(&database, &fresh.0)
            .await
            .expect("fresh tool lookup should succeed")
            .expect("fresh tool should remain");
        assert!(fresh_call.protected_arguments.is_some());
        assert!(fresh_call.protected_result.is_none());
        assert!(fresh_call.payload_purged_at.is_none());
        let fresh_approval =
            AiApprovalRecord::find_by_id(&database, &fresh.1.expect("fresh approval should exist"))
                .await
                .expect("fresh approval lookup should succeed")
                .expect("fresh approval should remain");
        assert!(fresh_approval.protected_resource_bindings.is_some());
        assert!(fresh_approval.protected_action_preview.is_some());
        assert!(fresh_approval.payload_purged_at.is_none());
    }

    #[tokio::test]
    async fn raw_payload_lookahead_blocks_the_complete_session_proof() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy_with_message_retention(&database, &scope, None).await;
        let session_id = seed_session(&database, &scope).await;
        let (_, run_id) = seed_message(&database, session_id, "completed", false).await;
        let first = seed_tool_call(&database, session_id, run_id, "completed", None)
            .await
            .0;
        let second = seed_tool_call(&database, session_id, run_id, "completed", None)
            .await
            .0;
        let limits = AiSessionRetentionLimits::default()
            .with_tool_payload_limits(1, 100)
            .expect("raw tool lookahead limit should validate");
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            Arc::new(FixedClock::new(now())),
            limits,
        );

        let report = service
            .prune_session_content(None)
            .await
            .expect("over-bound raw payloads should remain closed");
        assert_eq!(report.sessions_changed, 0);
        assert_eq!(report.raw_payload_purges_blocked, 1);
        assert_eq!(report.tool_payload_purges_blocked, 0);
        assert_eq!(report.expired_tool_payloads_purged, 0);
        for call_id in [first, second] {
            let call = AiToolCallRecord::find_by_id(&database, &call_id)
                .await
                .expect("tool-call lookup should succeed")
                .expect("over-bound raw tool call should remain");
            assert!(call.protected_arguments.is_some());
            assert!(call.protected_result.is_some());
            assert!(call.payload_purged_at.is_none());
        }
    }

    #[tokio::test]
    async fn active_tool_authority_blocks_deleting_session_payload_retention() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy(&database, &scope).await;
        let session_id = seed_session(&database, &scope).await;
        let (message_id, run_id) = seed_message(&database, session_id, "completed", false).await;
        let (call_id, approval_id) = seed_tool_call(
            &database,
            session_id,
            run_id,
            "waiting_approval",
            Some("approved"),
        )
        .await;
        mark_session_deleting(&database, session_id, 120).await;
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            Arc::new(FixedClock::new(now())),
            AiSessionRetentionLimits::default(),
        );

        let report = service
            .prune_session_content(None)
            .await
            .expect("active tool authority should remain closed");
        assert_eq!(report.sessions_changed, 0);
        assert_eq!(report.tool_payload_purges_blocked, 1);
        assert_eq!(report.deleting_session_tool_payloads_purged, 0);
        assert_eq!(report.message_contents_purged, 0);
        let call = AiToolCallRecord::find_by_id(&database, &call_id)
            .await
            .expect("tool-call lookup should succeed")
            .expect("active tool call should remain");
        assert!(call.protected_arguments.is_some());
        assert!(call.payload_purged_at.is_none());
        let approval =
            AiApprovalRecord::find_by_id(&database, &approval_id.expect("approval should exist"))
                .await
                .expect("approval lookup should succeed")
                .expect("active approval should remain");
        assert!(approval.protected_action_preview.is_some());
        assert!(approval.payload_purged_at.is_none());
        let message = AiMessageRecord::find_by_id(&database, &message_id)
            .await
            .expect("message lookup should succeed")
            .expect("message should remain");
        assert!(message.protected_preview.is_some());
    }

    #[tokio::test]
    async fn tool_lookahead_blocks_the_whole_deleting_session_set() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy(&database, &scope).await;
        let session_id = seed_session(&database, &scope).await;
        let (_, run_id) = seed_message(&database, session_id, "completed", false).await;
        let first = seed_tool_call(&database, session_id, run_id, "completed", None)
            .await
            .0;
        let second = seed_tool_call(&database, session_id, run_id, "completed", None)
            .await
            .0;
        mark_session_deleting(&database, session_id, 120).await;
        let limits = AiSessionRetentionLimits::default()
            .with_tool_payload_limits(1, 100)
            .expect("tool lookahead limit should validate");
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            Arc::new(FixedClock::new(now())),
            limits,
        );

        let report = service
            .prune_session_content(None)
            .await
            .expect("over-bound tools should remain closed");
        assert_eq!(report.sessions_changed, 0);
        assert_eq!(report.tool_payload_purges_blocked, 1);
        assert_eq!(report.deleting_session_tool_payloads_purged, 0);
        for call_id in [first, second] {
            let call = AiToolCallRecord::find_by_id(&database, &call_id)
                .await
                .expect("tool-call lookup should succeed")
                .expect("over-bound tool call should remain");
            assert!(call.protected_arguments.is_some());
            assert!(call.payload_purged_at.is_none());
        }
    }

    #[tokio::test]
    async fn approval_lookahead_blocks_the_whole_deleting_session_set() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy(&database, &scope).await;
        let session_id = seed_session(&database, &scope).await;
        let (_, run_id) = seed_message(&database, session_id, "completed", false).await;
        let first =
            seed_tool_call(&database, session_id, run_id, "completed", Some("consumed")).await;
        let second =
            seed_tool_call(&database, session_id, run_id, "completed", Some("consumed")).await;
        mark_session_deleting(&database, session_id, 120).await;
        let limits = AiSessionRetentionLimits::default()
            .with_tool_payload_limits(100, 1)
            .expect("approval lookahead limit should validate");
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            Arc::new(FixedClock::new(now())),
            limits,
        );

        let report = service
            .prune_session_content(None)
            .await
            .expect("over-bound approvals should remain closed");
        assert_eq!(report.sessions_changed, 0);
        assert_eq!(report.tool_payload_purges_blocked, 1);
        assert_eq!(report.deleting_session_tool_payloads_purged, 0);
        assert_eq!(report.deleting_session_approval_payloads_purged, 0);
        for (call_id, approval_id) in [first, second] {
            let call = AiToolCallRecord::find_by_id(&database, &call_id)
                .await
                .expect("tool-call lookup should succeed")
                .expect("over-bound tool call should remain");
            assert!(call.protected_arguments.is_some());
            assert!(call.payload_purged_at.is_none());
            let approval = AiApprovalRecord::find_by_id(
                &database,
                &approval_id.expect("approval should exist"),
            )
            .await
            .expect("approval lookup should succeed")
            .expect("over-bound approval should remain");
            assert!(approval.protected_resource_bindings.is_some());
            assert!(approval.protected_action_preview.is_some());
            assert!(approval.payload_purged_at.is_none());
        }
    }

    #[tokio::test]
    async fn deleting_session_attachment_cleanup_precedes_message_scrubbing() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy(&database, &scope).await;
        let session_id = seed_session(&database, &scope).await;
        let (message_id, _) = seed_message(&database, session_id, "completed", true).await;
        mark_session_deleting(&database, session_id, 120).await;
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            Arc::new(FixedClock::new(now())),
            AiSessionRetentionLimits::default(),
        );

        let requested = service
            .prune_session_content(None)
            .await
            .expect("retention should request verified attachment cleanup");
        assert_eq!(requested.deleting_session_attachment_cleanups_requested, 1);
        assert_eq!(requested.deleting_session_attachments_deleted, 0);
        assert_eq!(requested.attachment_cleanups_blocked, 1);
        assert_eq!(requested.message_contents_purged, 0);
        assert_eq!(requested.messages_blocked, 1);

        let attachment = database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    tx.query::<AiAttachmentRecord>()
                        .filter(AiAttachmentRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("attachment cleanup request should load")
            .into_iter()
            .next()
            .expect("attachment should remain for external cleanup");
        assert_eq!(attachment.quarantine_state, "deleting");
        assert_eq!(attachment.processing_state, "retention_cleanup_required");
        assert!(attachment.blob_reference.is_some());

        let attachment_id = attachment.id;
        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let outcome = tx
                        .compare_and_swap::<AiAttachmentRecord>(
                            &attachment_id,
                            attachment.row_version,
                            AiAttachmentRecordWhereInput::default(),
                            UpdateAiAttachmentRecordInput {
                                blob_reference: Some(None),
                                quarantine_blob_reference: Some(None),
                                quarantine_state: Some("deleted".to_owned()),
                                processing_state: Some("complete".to_owned()),
                                cleanup_generation: Some(Some(1)),
                                cleanup_lease_expires_at: Some(None),
                                cleanup_next_attempt_at: Some(None),
                                deleted_at: Some(Some(now().unix_timestamp())),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if matches!(outcome, ConditionalUpdateOutcome::Updated(_)) {
                        Ok(())
                    } else {
                        Err(OrmPublicError::new(OrmErrorCode::Conflict))
                    }
                })
            })
            .await
            .expect("confirmed external cleanup should finalize");

        let purged = service
            .prune_session_content(None)
            .await
            .expect("later retention should delete metadata then scrub content");
        assert_eq!(purged.deleting_session_attachment_cleanups_requested, 0);
        assert_eq!(purged.deleting_session_attachments_deleted, 1);
        assert_eq!(purged.attachment_cleanups_blocked, 0);
        assert_eq!(purged.message_contents_purged, 1);
        assert_eq!(purged.message_blocks_deleted, 1);
        assert!(
            AiAttachmentRecord::find_by_id(&database, &attachment_id)
                .await
                .expect("attachment lookup should succeed")
                .is_none()
        );
        let message = AiMessageRecord::find_by_id(&database, &message_id)
            .await
            .expect("message lookup should succeed")
            .expect("message tombstone should remain");
        assert!(message.protected_preview.is_none());
        assert!(message.content_purged_at.is_some());
    }

    #[tokio::test]
    async fn attachment_artifacts_are_cleaned_before_parent_attachments() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy(&database, &scope).await;
        let session_id = seed_session(&database, &scope).await;
        let (message_id, _) = seed_message(&database, session_id, "completed", true).await;
        let attachment = database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    tx.query::<AiAttachmentRecord>()
                        .filter(AiAttachmentRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(1)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("attachment should load")
            .into_iter()
            .next()
            .expect("attachment should exist");
        let artifact_id = Uuid::new_v4();
        AiAttachmentArtifactRecord::insert(
            &database,
            CreateAiAttachmentArtifactRecordInput {
                id: artifact_id,
                attachment_id: attachment.id,
                artifact_kind: "provider_file".to_owned(),
                blob_reference: None,
                protected_content: None,
                detected_mime: Some("text/plain".to_owned()),
                byte_count: 0,
                sha256: None,
                provider_reference: Some("provider-file-safe-reference".to_owned()),
                provider_expires_at: None,
                cleanup_state: None,
                cleanup_generation: None,
                cleanup_lease_expires_at: None,
                cleanup_retry_count: None,
                cleanup_next_attempt_at: None,
                deleted_at: None,
            },
        )
        .await
        .expect("provider artifact should seed");
        mark_session_deleting(&database, session_id, 120).await;
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            Arc::new(FixedClock::new(now())),
            AiSessionRetentionLimits::default(),
        );

        let report = service
            .prune_session_content(None)
            .await
            .expect("provider artifact should enter exact-reference cleanup");
        assert_eq!(report.deleting_session_attachment_cleanups_requested, 0);
        assert_eq!(report.deleting_session_attachments_deleted, 0);
        assert_eq!(
            report.deleting_session_attachment_artifact_cleanups_requested,
            1
        );
        assert_eq!(report.deleting_session_attachment_artifacts_deleted, 0);
        assert_eq!(report.attachment_cleanups_blocked, 1);
        assert_eq!(report.message_contents_purged, 0);
        let artifact = AiAttachmentArtifactRecord::find_by_id(&database, &artifact_id)
            .await
            .expect("artifact lookup should succeed")
            .expect("artifact should remain for exact-reference cleanup");
        assert_eq!(artifact.cleanup_state.as_deref(), Some("cleanup_required"));
        assert_eq!(
            artifact.provider_reference.as_deref(),
            Some("provider-file-safe-reference")
        );
        let retained = AiAttachmentRecord::find_by_id(&database, &attachment.id)
            .await
            .expect("attachment lookup should succeed")
            .expect("unsafe attachment should remain");
        assert_eq!(retained.processing_state, "ready");
        let message = AiMessageRecord::find_by_id(&database, &message_id)
            .await
            .expect("message lookup should succeed")
            .expect("blocked message should remain");
        assert!(message.protected_preview.is_some());

        let replay = service
            .prune_session_content(None)
            .await
            .expect("pending artifact cleanup should be idempotent");
        assert_eq!(
            replay.deleting_session_attachment_artifact_cleanups_requested,
            0
        );
        assert_eq!(replay.attachment_cleanups_blocked, 1);

        database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = tx
                        .find_by_id::<AiAttachmentArtifactRecord>(&artifact_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    let outcome = tx
                        .compare_and_swap::<AiAttachmentArtifactRecord>(
                            &artifact_id,
                            current.row_version,
                            AiAttachmentArtifactRecordWhereInput::default(),
                            UpdateAiAttachmentArtifactRecordInput {
                                provider_reference: Some(None),
                                provider_expires_at: Some(None),
                                cleanup_state: Some(Some("complete".to_owned())),
                                cleanup_generation: Some(Some(1)),
                                cleanup_lease_expires_at: Some(None),
                                cleanup_next_attempt_at: Some(None),
                                deleted_at: Some(Some(now().unix_timestamp())),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if matches!(outcome, ConditionalUpdateOutcome::Updated(_)) {
                        Ok(())
                    } else {
                        Err(OrmPublicError::new(OrmErrorCode::Conflict))
                    }
                })
            })
            .await
            .expect("confirmed provider absence should finalize the artifact");

        let artifact_deleted = service
            .prune_session_content(None)
            .await
            .expect("artifact metadata should delete before parent cleanup starts");
        assert_eq!(
            artifact_deleted.deleting_session_attachment_artifacts_deleted,
            1
        );
        assert_eq!(
            artifact_deleted.deleting_session_attachment_cleanups_requested,
            1
        );
        assert!(
            AiAttachmentArtifactRecord::find_by_id(&database, &artifact_id)
                .await
                .expect("artifact lookup should succeed")
                .is_none()
        );
        let parent = AiAttachmentRecord::find_by_id(&database, &attachment.id)
            .await
            .expect("parent lookup should succeed")
            .expect("parent should await its independent cleanup");
        assert_eq!(parent.quarantine_state, "deleting");
        assert_eq!(parent.processing_state, "retention_cleanup_required");
    }

    #[tokio::test]
    async fn artifact_worker_retries_ambiguous_provider_delete_and_fences_races() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy(&database, &scope).await;
        let session_id = seed_session(&database, &scope).await;
        let _ = seed_message(&database, session_id, "completed", true).await;
        let attachment = database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    tx.query::<AiAttachmentRecord>()
                        .filter(AiAttachmentRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("attachment should load")
            .into_iter()
            .next()
            .expect("attachment should exist");
        let artifact_id = Uuid::new_v4();
        let local_reference = "artifact:local:exact";
        let provider_reference = "provider:file:exact";
        AiAttachmentArtifactRecord::insert(
            &database,
            CreateAiAttachmentArtifactRecordInput {
                id: artifact_id,
                attachment_id: attachment.id,
                artifact_kind: "provider_file".to_owned(),
                blob_reference: Some(local_reference.to_owned()),
                protected_content: Some(serde_json::json!({"ciphertext": "protected"})),
                detected_mime: Some("text/plain".to_owned()),
                byte_count: 9,
                sha256: Some("0".repeat(64)),
                provider_reference: Some(provider_reference.to_owned()),
                provider_expires_at: Some(now().unix_timestamp() - 1),
                cleanup_state: None,
                cleanup_generation: None,
                cleanup_lease_expires_at: None,
                cleanup_retry_count: None,
                cleanup_next_attempt_at: None,
                deleted_at: None,
            },
        )
        .await
        .expect("artifact should seed");
        mark_session_deleting(&database, session_id, 120).await;
        let clock = Arc::new(FixedClock::new(now()));
        let retention = OrmAiSessionRetentionService::new(
            database.clone(),
            clock.clone(),
            AiSessionRetentionLimits::default(),
        );
        let requested = retention
            .prune_session_content(None)
            .await
            .expect("artifact cleanup should be requested");
        assert_eq!(
            requested.deleting_session_attachment_artifact_cleanups_requested,
            1
        );

        let blobs = Arc::new(ArtifactBlobStore::default());
        blobs.insert(local_reference);
        let provider = Arc::new(ProviderFileDeletion::default());
        let cleanup = OrmAiAttachmentService::new(
            database.clone(),
            Arc::new(AllowAll),
            Arc::new(ProtectionPolicy),
            Arc::new(DatabaseManagedContentProtector),
            blobs.clone(),
            Arc::new(UnusedScanner),
            Arc::new(DenyAttachmentAcceptance),
            clock.clone(),
        )
        .with_provider_file_deletion_service(provider.clone());

        set_provider_file_delete_required(&database, &scope, false).await;
        let policy_blocked = cleanup
            .cleanup_once()
            .await
            .expect("current policy should keep provider deletion closed");
        assert_eq!(policy_blocked.artifacts_examined, 1);
        assert_eq!(policy_blocked.artifacts_deferred, 1);
        assert_eq!(policy_blocked.artifacts_cleaned, 0);
        assert!(blobs.contains(local_reference));
        assert!(
            provider
                .deleted
                .lock()
                .expect("provider deletion lock")
                .is_empty()
        );

        set_provider_file_delete_required(&database, &scope, true).await;
        provider.set_fail(true);

        let ambiguous = cleanup
            .cleanup_once()
            .await
            .expect("ambiguous provider deletion should remain retryable");
        assert_eq!(ambiguous.artifacts_examined, 1);
        assert_eq!(ambiguous.artifacts_cleaned, 0);
        assert_eq!(ambiguous.artifacts_failed, 1);
        assert!(!blobs.contains(local_reference));
        let retained = AiAttachmentArtifactRecord::find_by_id(&database, &artifact_id)
            .await
            .expect("artifact lookup should succeed")
            .expect("ambiguous artifact should remain");
        assert_eq!(retained.cleanup_state.as_deref(), Some("cleanup_backoff"));
        assert_eq!(
            retained.blob_reference.as_deref(),
            Some(local_reference),
            "metadata remains until every external absence proof succeeds"
        );
        assert_eq!(
            retained.provider_reference.as_deref(),
            Some(provider_reference)
        );
        assert!(retained.protected_content.is_some());

        provider.set_fail(false);
        clock.advance_seconds(121);
        let (left, right) = tokio::join!(cleanup.cleanup_once(), cleanup.cleanup_once());
        let left = left.expect("left artifact worker should converge");
        let right = right.expect("right artifact worker should converge");
        assert_eq!(left.artifacts_cleaned + right.artifacts_cleaned, 1);
        assert_eq!(left.artifacts_failed + right.artifacts_failed, 0);
        assert!(left.artifacts_deferred + right.artifacts_deferred <= 1);
        assert_eq!(
            provider
                .deleted
                .lock()
                .expect("provider deletion lock")
                .as_slice(),
            &[provider_reference.to_owned()]
        );
        let cleaned = AiAttachmentArtifactRecord::find_by_id(&database, &artifact_id)
            .await
            .expect("artifact lookup should succeed")
            .expect("artifact tombstone should remain until retention");
        assert_eq!(cleaned.cleanup_state.as_deref(), Some("complete"));
        assert!(cleaned.blob_reference.is_none());
        assert!(cleaned.provider_reference.is_none());
        assert!(cleaned.provider_expires_at.is_none());
        assert!(cleaned.protected_content.is_none());
        assert!(cleaned.deleted_at.is_some());

        let finalized = retention
            .prune_session_content(None)
            .await
            .expect("retention should delete artifact metadata before parent cleanup");
        assert_eq!(finalized.deleting_session_attachment_artifacts_deleted, 1);
        assert_eq!(finalized.deleting_session_attachment_cleanups_requested, 1);
        assert!(
            AiAttachmentArtifactRecord::find_by_id(&database, &artifact_id)
                .await
                .expect("artifact lookup should succeed")
                .is_none()
        );
    }

    #[tokio::test]
    async fn overbound_artifact_set_makes_no_partial_cleanup_claims() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy(&database, &scope).await;
        let session_id = seed_session(&database, &scope).await;
        let _ = seed_message(&database, session_id, "completed", true).await;
        let attachment = database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    tx.query::<AiAttachmentRecord>()
                        .filter(AiAttachmentRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("attachment should load")
            .into_iter()
            .next()
            .expect("attachment should exist");
        let artifact_ids = [Uuid::new_v4(), Uuid::new_v4()];
        for (index, artifact_id) in artifact_ids.into_iter().enumerate() {
            AiAttachmentArtifactRecord::insert(
                &database,
                CreateAiAttachmentArtifactRecordInput {
                    id: artifact_id,
                    attachment_id: attachment.id,
                    artifact_kind: format!("derived_{index}"),
                    blob_reference: Some(format!("artifact:exact:{index}")),
                    protected_content: Some(serde_json::json!({"protected": index})),
                    detected_mime: Some("text/plain".to_owned()),
                    byte_count: 1,
                    sha256: Some("0".repeat(64)),
                    provider_reference: None,
                    provider_expires_at: None,
                    cleanup_state: None,
                    cleanup_generation: None,
                    cleanup_lease_expires_at: None,
                    cleanup_retry_count: None,
                    cleanup_next_attempt_at: None,
                    deleted_at: None,
                },
            )
            .await
            .expect("artifact should seed");
        }
        mark_session_deleting(&database, session_id, 120).await;
        let limits = AiSessionRetentionLimits::default()
            .with_attachment_artifact_limit(1)
            .expect("artifact lookahead bound should validate");
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            Arc::new(FixedClock::new(now())),
            limits,
        );
        let report = service
            .prune_session_content(None)
            .await
            .expect("overbound artifact proof should fail closed");
        assert_eq!(report.attachment_cleanups_blocked, 1);
        assert_eq!(
            report.deleting_session_attachment_artifact_cleanups_requested,
            0
        );
        for artifact_id in artifact_ids {
            let artifact = AiAttachmentArtifactRecord::find_by_id(&database, &artifact_id)
                .await
                .expect("artifact lookup should succeed")
                .expect("overbound artifact should remain");
            assert!(artifact.cleanup_state.is_none());
            assert!(artifact.blob_reference.is_some());
            assert!(artifact.protected_content.is_some());
        }
    }

    #[tokio::test]
    async fn nonterminal_run_blocks_checkpoint_detachment_and_purge() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy_with_message_retention(&database, &scope, None).await;
        let session_id = seed_session(&database, &scope).await;
        let run_id = Uuid::new_v4();
        AiRunRecord::insert(
            &database,
            CreateAiRunRecordInput {
                id: run_id,
                session_id,
                input_message_id: Uuid::new_v4(),
                principal_reference: serde_json::json!({"test": true}),
                state: "running".to_owned(),
                attempt_id: None,
                lease_owner: None,
                lease_generation: 0,
                lease_expires_at: None,
                lease_heartbeat_at: None,
                retry_count: 0,
                next_attempt_at: None,
                error_code: None,
                latest_checkpoint_id: None,
            },
        )
        .await
        .expect("nonterminal run should seed");
        let checkpoint_ids = seed_run_checkpoints(&database, run_id, 1).await;
        mark_session_deleting(&database, session_id, 120).await;
        let service = OrmAiSessionRetentionService::new(
            database.clone(),
            Arc::new(FixedClock::new(now())),
            AiSessionRetentionLimits::default(),
        );

        let report = service
            .prune_session_content(None)
            .await
            .expect("blocked run-checkpoint retention pass should be safe");
        assert_eq!(report.sessions_changed, 0);
        assert_eq!(report.tool_payload_purges_blocked, 1);
        assert_eq!(report.run_checkpoint_purges_blocked, 0);
        assert_eq!(report.deleting_session_run_checkpoint_references_cleared, 0);
        assert_eq!(report.deleting_session_run_checkpoints_deleted, 0);

        let run = AiRunRecord::find_by_id(&database, &run_id)
            .await
            .expect("run lookup should succeed")
            .expect("nonterminal run should remain");
        assert_eq!(run.latest_checkpoint_id, checkpoint_ids.first().copied());
        let checkpoints = database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    tx.query::<AiRunCheckpointRecord>()
                        .filter(AiRunCheckpointRecordWhereInput {
                            id: Some(UuidFilter {
                                in_list: Some(checkpoint_ids),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("checkpoint proof should load");
        assert_eq!(checkpoints.len(), 1);
    }

    #[tokio::test]
    async fn stale_session_candidate_is_reported_only_after_transaction_rollback() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy(&database, &scope).await;
        let session_id = seed_session(&database, &scope).await;
        let stale_candidate = AiSessionRecord::find_by_id(&database, &session_id)
            .await
            .expect("candidate lookup should succeed")
            .expect("candidate should exist");
        mark_session_deleting(&database, session_id, 120).await;
        let service = OrmAiSessionRetentionService::new(
            database,
            Arc::new(FixedClock::new(now())),
            AiSessionRetentionLimits::default(),
        );

        let outcome = service
            .prune_session(stale_candidate, now().unix_timestamp())
            .await
            .expect("stale candidate should be a bounded conflict");
        assert!(matches!(outcome, SessionPruneOutcome::Conflict));
    }

    #[tokio::test]
    async fn missing_policy_is_reported_and_keyset_scan_is_bounded() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_session(&database, &scope).await;
        seed_session(&database, &scope).await;
        let service = OrmAiSessionRetentionService::new(
            database,
            Arc::new(FixedClock::new(now())),
            AiSessionRetentionLimits::new(1, 10, 10, 10)
                .expect("bounded retention limits should validate"),
        );
        let first = service
            .prune_session_content(None)
            .await
            .expect("first scan page should succeed");
        assert_eq!(first.sessions_scanned, 1);
        assert_eq!(first.sessions_not_ready, 1);
        let cursor = first
            .next_session_cursor
            .expect("another bounded page should remain");
        let second = service
            .prune_session_content(Some(cursor))
            .await
            .expect("second scan page should succeed");
        assert_eq!(second.sessions_scanned, 1);
        assert_eq!(second.sessions_not_ready, 1);
        assert!(second.next_session_cursor.is_none());
        assert!(matches!(
            service.prune_session_content(Some("\n".to_owned())).await,
            Err(AiError::InvalidInput(_))
        ));
        let attachment_limits = AiSessionRetentionLimits::new(1, 10, 10, 10)
            .expect("base retention limits should validate")
            .with_attachment_limit(2)
            .expect("independent attachment limit should validate");
        assert_eq!(attachment_limits.maximum_attachments_per_session(), 2);
        assert!(matches!(
            attachment_limits.with_attachment_limit(0),
            Err(AiError::InvalidConfiguration(_))
        ));
        let artifact_limits = AiSessionRetentionLimits::new(1, 10, 10, 10)
            .expect("base retention limits should validate")
            .with_attachment_artifact_limit(3)
            .expect("independent attachment-artifact limit should validate");
        assert_eq!(
            artifact_limits.maximum_attachment_artifacts_per_session(),
            3
        );
        assert!(matches!(
            artifact_limits.with_attachment_artifact_limit(0),
            Err(AiError::InvalidConfiguration(_))
        ));
        let proposal_limits = AiSessionRetentionLimits::new(1, 10, 10, 10)
            .expect("base retention limits should validate")
            .with_proposal_limits(2, 3)
            .expect("independent proposal limits should validate");
        assert_eq!(proposal_limits.maximum_proposals_per_session(), 2);
        assert_eq!(proposal_limits.maximum_proposal_items_per_session(), 3);
        assert!(matches!(
            proposal_limits.with_proposal_limits(0, 3),
            Err(AiError::InvalidConfiguration(_))
        ));
        let tool_limits = AiSessionRetentionLimits::new(1, 10, 10, 10)
            .expect("base retention limits should validate")
            .with_tool_payload_limits(2, 3)
            .expect("independent tool limits should validate");
        assert_eq!(tool_limits.maximum_tool_calls_per_session(), 2);
        assert_eq!(tool_limits.maximum_approvals_per_session(), 3);
        assert!(matches!(
            tool_limits.with_tool_payload_limits(2, 0),
            Err(AiError::InvalidConfiguration(_))
        ));
    }
}
