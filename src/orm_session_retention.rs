//! ORM-only bounded pruning of protected session deltas and message content.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::sync::Arc;

use agql_auth::Clock;
use async_trait::async_trait;
use graphql_orm::db::Database;
use graphql_orm::graphql::errors::{OrmErrorCode, OrmPublicError};
use graphql_orm::graphql::filters::{IntFilter, StringFilter, UuidFilter};
use graphql_orm::graphql::orm::{
    ConditionalUpdateOutcome, DefaultWriteBackend, TransactionError, TransactionMode,
};
use graphql_orm::graphql::pagination::KeysetConnectionInput;
use uuid::Uuid;

use crate::persistence::*;
use crate::{AiError, AiRunState, AiScope, AiSessionRetentionReport, AiSessionRetentionService};

const MAXIMUM_RETENTION_SECONDS: i64 = 315_576_000;

/// Deployment hard bounds for one session-retention scan page.
///
/// These limits constrain generated ORM reads and writes. They do not grant a
/// user capability and do not broaden the current GraphQL-managed policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiSessionRetentionLimits {
    maximum_sessions: usize,
    maximum_live_delta_events_per_session: usize,
    maximum_messages_per_session: usize,
    maximum_message_blocks_per_session: usize,
}

impl AiSessionRetentionLimits {
    /// Creates validated per-pass bounds.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless sessions are in
    /// `1..=256`, live deltas and messages are each in `1..=5_000`, and the
    /// total message-block bound is in `1..=20_000`.
    pub fn new(
        maximum_sessions: usize,
        maximum_live_delta_events_per_session: usize,
        maximum_messages_per_session: usize,
        maximum_message_blocks_per_session: usize,
    ) -> Result<Self, AiError> {
        if !(1..=256).contains(&maximum_sessions)
            || !(1..=5_000).contains(&maximum_live_delta_events_per_session)
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
            maximum_messages_per_session,
            maximum_message_blocks_per_session,
        })
    }

    /// Maximum session metadata rows considered in one scan page.
    pub const fn maximum_sessions(self) -> usize {
        self.maximum_sessions
    }

    /// Maximum expired provisional events deleted for one session.
    pub const fn maximum_live_delta_events_per_session(self) -> usize {
        self.maximum_live_delta_events_per_session
    }

    /// Maximum unpurged message rows inspected for one session.
    pub const fn maximum_messages_per_session(self) -> usize {
        self.maximum_messages_per_session
    }

    /// Maximum message blocks deleted for one session transaction.
    pub const fn maximum_message_blocks_per_session(self) -> usize {
        self.maximum_message_blocks_per_session
    }
}

impl Default for AiSessionRetentionLimits {
    fn default() -> Self {
        Self {
            maximum_sessions: 50,
            maximum_live_delta_events_per_session: 500,
            maximum_messages_per_session: 100,
            maximum_message_blocks_per_session: 5_000,
        }
    }
}

/// Trusted ORM-only worker for GraphQL-managed session retention.
///
/// The worker never opens or copies protected payloads. It deletes only
/// expired provisional delta rows and finalized message blocks, replacing the
/// corresponding preview with an explicit metadata tombstone. Messages tied
/// to nonterminal runs or any attachment remain untouched and are reported as
/// blocked. Append-only audit/usage/fence facts are never deleted.
pub struct OrmAiSessionRetentionService {
    database: Database<DefaultWriteBackend>,
    clock: Arc<dyn Clock>,
    limits: AiSessionRetentionLimits,
}

impl OrmAiSessionRetentionService {
    /// Creates a bounded trusted retention worker.
    pub fn new(
        database: Database<DefaultWriteBackend>,
        clock: Arc<dyn Clock>,
        limits: AiSessionRetentionLimits,
    ) -> Self {
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
                AiSessionRecordWhereInput::default(),
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
        let message_limit = i64::try_from(self.limits.maximum_messages_per_session)
            .map_err(|_| AiError::InvalidConfiguration("invalid message limit".to_owned()))?;
        let maximum_blocks = self.limits.maximum_message_blocks_per_session;
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&candidate.id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if session.row_version != candidate.row_version {
                        return Ok(SessionPruneOutcome::Conflict);
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
                            event_type: Some(StringFilter {
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
                            || row.event_type != "provider_live_delta"
                            || row.sequence <= 0
                            || row.sequence > session.stream_head
                            || previous_event_sequence.is_some_and(|value| row.sequence <= value)
                        {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        }
                        previous_event_sequence = Some(row.sequence);
                        if row.created_at <= delta_cutoff {
                            event_ids.push(row.id);
                        }
                    }

                    let mut messages_purged = 0usize;
                    let mut blocks_deleted = 0usize;
                    let mut messages_blocked = 0usize;
                    if let Some(retention_seconds) = policy.message_retention_seconds {
                        let message_cutoff = now
                            .checked_sub(retention_seconds)
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
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
                            if finalized_at > message_cutoff {
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
                                return Ok(SessionPruneOutcome::Conflict);
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
                    if event_ids.is_empty() && messages_purged == 0 {
                        return Ok(SessionPruneOutcome::Noop { messages_blocked });
                    }
                    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                        actor_principal_kind: "system".to_owned(),
                        actor_subject: "session-retention".to_owned(),
                        action: "prune_session_content".to_owned(),
                        resource_kind: "ai_session".to_owned(),
                        resource_reference: session.id.to_string(),
                        outcome: "allowed".to_owned(),
                        reason_code: "scope_retention_expired".to_owned(),
                        correlation_id: Uuid::new_v4().to_string(),
                        causation_id: None,
                        policy_version: Some(format!("{}:{}", policy.id, policy.row_version)),
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    Ok(SessionPruneOutcome::Changed {
                        events_deleted: event_ids.len(),
                        messages_purged,
                        blocks_deleted,
                        messages_blocked,
                    })
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
            match self.prune_session(session, now).await? {
                SessionPruneOutcome::NotReady => report.sessions_not_ready += 1,
                SessionPruneOutcome::Conflict => report.sessions_conflicted += 1,
                SessionPruneOutcome::Noop { messages_blocked } => {
                    report.messages_blocked = add_count(report.messages_blocked, messages_blocked)?;
                }
                SessionPruneOutcome::Changed {
                    events_deleted,
                    messages_purged,
                    blocks_deleted,
                    messages_blocked,
                } => {
                    report.sessions_changed += 1;
                    report.live_delta_events_deleted =
                        add_count(report.live_delta_events_deleted, events_deleted)?;
                    report.message_contents_purged =
                        add_count(report.message_contents_purged, messages_purged)?;
                    report.message_blocks_deleted =
                        add_count(report.message_blocks_deleted, blocks_deleted)?;
                    report.messages_blocked = add_count(report.messages_blocked, messages_blocked)?;
                }
            }
        }
        Ok(report)
    }
}

struct SessionCandidatePage {
    sessions: Vec<AiSessionRecord>,
    next_cursor: Option<String>,
}

enum SessionPruneOutcome {
    NotReady,
    Conflict,
    Noop {
        messages_blocked: usize,
    },
    Changed {
        events_deleted: usize,
        messages_purged: usize,
        blocks_deleted: usize,
        messages_blocked: usize,
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

fn validate_session(session: &AiSessionRecord) -> Result<(), OrmPublicError> {
    let scope = session_scope(session);
    if session.id.is_nil()
        || session.owner_principal_kind.trim().is_empty()
        || session.owner_subject.trim().is_empty()
        || !matches!(session.state.as_str(), "active" | "archived" | "deleting")
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

fn valid_policy(policy: &AiRetentionPolicyRecord, scope: &AiScope, scope_key: &str) -> bool {
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

fn session_scope(session: &AiSessionRecord) -> AiScope {
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
    use crate::AiSessionService;
    use agql_auth::{AccessTokenMetadata, AuthPrincipal, AuthUser, FixedClock, SessionContext};
    use graphql_orm::graphql::orm::{ApplyOptions, OrmSchemaModule};
    use graphql_orm::prelude::SqliteBackend;
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
        AiRetentionPolicyRecord::insert(
            database,
            CreateAiRetentionPolicyRecordInput {
                scope_key: Some(crate::ai_scope_key(scope)),
                scope_kind: scope.kind.clone(),
                scope_id: scope.id.clone(),
                tenant_id: scope.tenant_id.clone(),
                message_retention_seconds: Some(60),
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
                stream_head: 2,
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

    async fn seed_events(database: &Database<SqliteBackend>, session_id: Uuid) {
        for (sequence, event_type) in [(1, "provider_live_delta"), (2, "message_queued")] {
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

    #[tokio::test]
    async fn expired_delta_and_terminal_message_content_are_pruned_atomically() {
        let database = database().await;
        let scope = AiScope::new("tenant", "retention").with_tenant_id("retention");
        seed_policy(&database, &scope).await;
        let session_id = seed_session(&database, &scope).await;
        let (message_id, _) = seed_message(&database, session_id, "completed", false).await;
        seed_events(&database, session_id).await;
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
        let (events, blocks, audits) = database
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
                    Ok((events, blocks, audits))
                })
            })
            .await
            .expect("retention results should load");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "message_queued");
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
        assert_eq!(gap.watermark, 2);

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
    }
}
