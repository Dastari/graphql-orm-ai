//! Durable ORM-backed conversational session service.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::sync::Arc;

use agql_auth::AuthPrincipal;
use async_trait::async_trait;
use graphql_orm::db::Database;
use graphql_orm::graphql::errors::{OrmErrorCode, OrmPublicError};
use graphql_orm::graphql::filters::{IntFilter, StringFilter, UuidFilter};
use graphql_orm::graphql::orm::{
    ConditionalUpdateOutcome, DefaultWriteBackend, TransactionError, TransactionMode,
};
use graphql_orm::graphql::pagination::{
    KeysetConnectionInput, KeysetWindowDirection, ValidatedKeysetConnection,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::orm_inbox::{PreparedAiInboxEvent, append_inbox_event};
use crate::persistence::*;
use crate::{
    AiAccessPolicy, AiContentProtectionPolicy, AiContentProtectionPolicyResolver,
    AiContentProtector, AiError, AiMessageBlockView, AiMessageConnection, AiMessageEdge,
    AiMessageView, AiScope, AiSessionAction, AiSessionConnection, AiSessionEdge,
    AiSessionEventPage, AiSessionEventView, AiSessionId, AiSessionService, AiSessionView,
    AiSessionWakeup, ContentProtectionContext, CreateAiSessionInput, ProtectedContentEnvelope,
    SendAiMessageInput, SendAiMessagePayload,
};

/// Service-side limits that are enforced even when callers bypass GraphQL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiSessionServiceLimits {
    /// Maximum UTF-8 bytes accepted for a title.
    pub maximum_title_bytes: usize,
    /// Maximum UTF-8 bytes accepted for one user message.
    pub maximum_message_bytes: usize,
    /// Maximum attachments accepted on one message.
    pub maximum_attachments: usize,
    /// Maximum protected preview size.
    pub maximum_preview_bytes: usize,
}

impl Default for AiSessionServiceLimits {
    fn default() -> Self {
        Self {
            maximum_title_bytes: 256,
            maximum_message_bytes: 256 * 1024,
            maximum_attachments: 10,
            maximum_preview_bytes: 4 * 1024,
        }
    }
}

/// Concrete owner-isolated session service using generated ORM repository and
/// transaction APIs only. It never executes backend-specific SQL.
pub struct OrmAiSessionService {
    database: Database<DefaultWriteBackend>,
    access_policy: Arc<dyn AiAccessPolicy>,
    protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
    content_protector: Arc<dyn AiContentProtector>,
    limits: AiSessionServiceLimits,
}

impl OrmAiSessionService {
    /// Creates a durable session service.
    pub fn new(
        database: Database<DefaultWriteBackend>,
        access_policy: Arc<dyn AiAccessPolicy>,
        protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
        content_protector: Arc<dyn AiContentProtector>,
    ) -> Self {
        Self {
            database,
            access_policy,
            protection_policy,
            content_protector,
            limits: AiSessionServiceLimits::default(),
        }
    }

    /// Overrides bounded service limits.
    #[must_use]
    pub fn with_limits(mut self, limits: AiSessionServiceLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Returns the underlying ORM database handle for schema composition and
    /// host wiring, without exposing a driver pool.
    pub fn database(&self) -> &Database<DefaultWriteBackend> {
        &self.database
    }

    async fn require_scope(
        &self,
        principal: &AuthPrincipal,
        scope: &AiScope,
        action: AiSessionAction,
    ) -> Result<(), AiError> {
        if self
            .access_policy
            .can_access_scope(principal, scope, action)
            .await
            .is_allowed()
        {
            Ok(())
        } else {
            Err(AiError::Forbidden)
        }
    }

    async fn require_session_policy(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
        action: AiSessionAction,
    ) -> Result<(), AiError> {
        if self
            .access_policy
            .can_access_session(principal, session_id, action)
            .await
            .is_allowed()
        {
            Ok(())
        } else {
            Err(AiError::Forbidden)
        }
    }

    async fn visible_session(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
        action: AiSessionAction,
    ) -> Result<Option<AiSessionRecord>, AiError> {
        self.require_session_policy(principal, session_id, action)
            .await?;
        let record = AiSessionRecord::find_by_id(&self.database, &session_id.0)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?;
        let Some(record) = record else {
            return Ok(None);
        };
        if !is_owner(principal, &record) || record.state == "deleting" {
            return Ok(None);
        }
        self.require_scope(principal, &record_scope(&record), action)
            .await?;
        Ok(Some(record))
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
}

#[async_trait]
impl AiSessionService for OrmAiSessionService {
    async fn sessions(
        &self,
        principal: &AuthPrincipal,
        page: ValidatedKeysetConnection,
    ) -> Result<AiSessionConnection, AiError> {
        let (kind, subject) = principal_identity(principal);
        let connection = AiSessionRecord::keyset_connection_page(
            &self.database,
            AiSessionRecordWhereInput {
                owner_principal_kind: Some(StringFilter {
                    eq: Some(kind),
                    ..Default::default()
                }),
                owner_subject: Some(StringFilter {
                    eq: Some(subject.to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            page_input(&page, false),
        )
        .await
        .map_err(map_orm)?;

        let mut edges = Vec::with_capacity(connection.edges.len());
        for edge in connection.edges {
            if edge.node.state == "deleting"
                || !self
                    .access_policy
                    .can_access_scope(principal, &record_scope(&edge.node), AiSessionAction::List)
                    .await
                    .is_allowed()
            {
                continue;
            }
            edges.push(AiSessionEdge {
                node: session_view(&edge.node),
                cursor: edge.cursor,
            });
        }
        let mut page_info = connection.page_info;
        page_info.total_count = None;
        Ok(AiSessionConnection { edges, page_info })
    }

    async fn session(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
    ) -> Result<Option<AiSessionView>, AiError> {
        Ok(self
            .visible_session(principal, session_id, AiSessionAction::Read)
            .await?
            .as_ref()
            .map(session_view))
    }

    async fn messages(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
        page: ValidatedKeysetConnection,
    ) -> Result<AiMessageConnection, AiError> {
        let session = self
            .visible_session(principal, session_id, AiSessionAction::Read)
            .await?
            .ok_or(AiError::NotFound)?;
        let scope = record_scope(&session);
        let policy = self.protection_policy(principal, &scope).await?;
        let connection = AiMessageRecord::keyset_connection_page(
            &self.database,
            AiMessageRecordWhereInput {
                session_id: Some(UuidFilter {
                    eq: Some(session_id.0),
                    ..Default::default()
                }),
                ..Default::default()
            },
            page_input(&page, false),
        )
        .await
        .map_err(map_orm)?;

        let mut edges = Vec::with_capacity(connection.edges.len());
        for edge in connection.edges {
            let content_purged = edge.node.content_purged_at.is_some();
            let preview = if content_purged {
                if edge.node.protected_preview.is_some() || edge.node.block_count != 0 {
                    return Err(AiError::PersistenceFailed);
                }
                "Content removed by retention policy".to_owned()
            } else {
                let protected_preview = edge
                    .node
                    .protected_preview
                    .as_ref()
                    .ok_or(AiError::PersistenceFailed)?;
                self.open_value(
                    &policy,
                    content_context(
                        "graphql_orm_ai_messages",
                        edge.node.id,
                        "protected_preview",
                        &scope,
                    ),
                    protected_preview,
                )
                .await?
                .as_str()
                .ok_or(AiError::PersistenceFailed)?
                .to_owned()
            };
            edges.push(AiMessageEdge {
                node: message_view(&edge.node, preview, content_purged),
                cursor: edge.cursor,
            });
        }
        Ok(AiMessageConnection {
            edges,
            page_info: connection.page_info,
        })
    }

    async fn message_blocks(
        &self,
        principal: &AuthPrincipal,
        message_id: Uuid,
        after_block_index: Option<i64>,
        first: i64,
    ) -> Result<Vec<AiMessageBlockView>, AiError> {
        if !(1..=100).contains(&first)
            || after_block_index.is_some_and(|value| value < 0 || value > i64::from(i32::MAX))
        {
            return Err(AiError::InvalidInput(
                "invalid message-block window".to_owned(),
            ));
        }
        let message = AiMessageRecord::find_by_id(&self.database, &message_id)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
            .ok_or(AiError::NotFound)?;
        let session = self
            .visible_session(
                principal,
                AiSessionId(message.session_id),
                AiSessionAction::Read,
            )
            .await?
            .ok_or(AiError::NotFound)?;
        if message.content_purged_at.is_some() {
            if message.protected_preview.is_some() || message.block_count != 0 {
                return Err(AiError::PersistenceFailed);
            }
            return Ok(Vec::new());
        }
        if message.protected_preview.is_none() {
            return Err(AiError::PersistenceFailed);
        }
        let scope = record_scope(&session);
        let policy = self.protection_policy(principal, &scope).await?;
        let block_after = after_block_index.map(|value| IntFilter {
            gt: Some(value as i32),
            ..Default::default()
        });
        let rows = self
            .database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    tx.query::<AiMessageBlockRecord>()
                        .filter(AiMessageBlockRecordWhereInput {
                            message_id: Some(UuidFilter {
                                eq: Some(message_id),
                                ..Default::default()
                            }),
                            block_index: block_after,
                            ..Default::default()
                        })
                        .default_order()
                        .limit(first)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .map_err(map_transaction)?;

        let mut views = Vec::with_capacity(rows.len());
        for row in rows {
            let content = self
                .open_value(
                    &policy,
                    content_context(
                        "graphql_orm_ai_message_blocks",
                        row.id,
                        "protected_content",
                        &scope,
                    ),
                    &row.protected_content,
                )
                .await?;
            views.push(AiMessageBlockView {
                id: row.id,
                message_id: row.message_id,
                block_index: row.block_index,
                kind: row.block_kind,
                content: async_graphql::Json(content),
                byte_count: row.byte_count,
                line_count: row.line_count,
            });
        }
        Ok(views)
    }

    async fn session_event_page(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
        after_sequence: i64,
        first: i64,
    ) -> Result<AiSessionEventPage, AiError> {
        if after_sequence < 0 || after_sequence > i64::from(i32::MAX) || !(1..=500).contains(&first)
        {
            return Err(AiError::InvalidInput("invalid event window".to_owned()));
        }
        let session = self
            .visible_session(principal, session_id, AiSessionAction::Read)
            .await?
            .ok_or(AiError::NotFound)?;
        let scope = record_scope(&session);
        let policy = self.protection_policy(principal, &scope).await?;
        let watermark = session.stream_head;
        let rows = self
            .database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    tx.query::<AiSessionEventRecord>()
                        .filter(AiSessionEventRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id.0),
                                ..Default::default()
                            }),
                            sequence: Some(IntFilter {
                                gt: Some(after_sequence as i32),
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
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .map_err(map_transaction)?;
        let mut expected_sequence = after_sequence.saturating_add(1);
        let reset_required = if after_sequence >= watermark {
            false
        } else if rows.is_empty() {
            true
        } else {
            rows.iter().any(|row| {
                let gap = row.session_id != session_id.0
                    || row.sequence != expected_sequence
                    || row.sequence > watermark;
                expected_sequence = row.sequence.saturating_add(1);
                gap
            })
        };
        if reset_required {
            return Ok(AiSessionEventPage {
                events: Vec::new(),
                watermark,
                has_more: false,
                reset_required: true,
            });
        }
        let has_more = rows.len() > first as usize;
        let mut rows = rows;
        rows.truncate(first as usize);
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let payload = self
                .open_value(
                    &policy,
                    content_context(
                        "graphql_orm_ai_session_events",
                        row.id,
                        "protected_payload",
                        &scope,
                    ),
                    &row.protected_payload,
                )
                .await?;
            events.push(AiSessionEventView {
                id: row.id,
                sequence: row.sequence,
                event_type: row.event_type,
                run_id: row.run_id,
                correlation_id: row.correlation_id,
                payload: async_graphql::Json(payload),
                created_at: row.created_at,
            });
        }
        Ok(AiSessionEventPage {
            events,
            watermark,
            has_more,
            reset_required: false,
        })
    }

    async fn create_session(
        &self,
        principal: &AuthPrincipal,
        input: CreateAiSessionInput,
    ) -> Result<AiSessionView, AiError> {
        let scope: AiScope = input.scope.into();
        validate_scope(&scope)?;
        self.require_scope(principal, &scope, AiSessionAction::Create)
            .await?;
        let title = input.title.unwrap_or_else(|| "New chat".to_owned());
        if title.trim().is_empty() || title.len() > self.limits.maximum_title_bytes {
            return Err(AiError::InvalidInput("invalid session title".to_owned()));
        }
        let session_id = Uuid::new_v4();
        let participant_id = Uuid::new_v4();
        let inbox_event_id = Uuid::new_v4();
        let (owner_principal_kind, owner_subject) = principal_identity(principal);
        let owner_subject = owner_subject.to_owned();
        let now = unix_seconds();
        let protection_policy = self.protection_policy(principal, &scope).await?;
        let protected_inbox_event = self
            .protect_value(
                &protection_policy,
                content_context(
                    "graphql_orm_ai_inbox_events",
                    inbox_event_id,
                    "protected_payload",
                    &scope,
                ),
                json!({"sessionId": session_id, "state": "active"}),
            )
            .await?;
        let scope_for_insert = scope.clone();
        let session = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let session = tx
                        .insert::<AiSessionRecord>(CreateAiSessionRecordInput {
                            id: session_id,
                            owner_principal_kind: owner_principal_kind.clone(),
                            owner_subject: owner_subject.clone(),
                            tenant_id: scope_for_insert.tenant_id.clone(),
                            scope_kind: scope_for_insert.kind.clone(),
                            scope_id: scope_for_insert.id.clone(),
                            title,
                            state: "active".to_owned(),
                            stream_head: 0,
                            message_head: 0,
                            last_activity_at: now,
                            archived_at: None,
                            deleted_at: None,
                        })
                        .await
                        .map_err(OrmPublicError::from)?;
                    tx.insert::<AiSessionParticipantRecord>(
                        CreateAiSessionParticipantRecordInput {
                            id: participant_id,
                            session_id,
                            principal_kind: owner_principal_kind.clone(),
                            principal_subject: owner_subject.clone(),
                            participant_role: "owner".to_owned(),
                        },
                    )
                    .await
                    .map_err(OrmPublicError::from)?;
                    append_inbox_event(
                        tx,
                        PreparedAiInboxEvent {
                            id: inbox_event_id,
                            principal_kind: owner_principal_kind,
                            principal_subject: owner_subject,
                            scope: scope_for_insert,
                            session_id,
                            event_type: "session_created".to_owned(),
                            protected_payload: protected_inbox_event,
                            created_at: now,
                        },
                    )
                    .await?;
                    Ok(session)
                })
            })
            .await
            .map_err(map_transaction)?;
        Ok(session_view(&session))
    }

    async fn archive_session(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
    ) -> Result<AiSessionView, AiError> {
        self.transition_session(principal, session_id, "active", "archived", true)
            .await
    }

    async fn restore_session(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
    ) -> Result<AiSessionView, AiError> {
        self.transition_session(principal, session_id, "archived", "active", false)
            .await
    }

    async fn delete_session(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
    ) -> Result<bool, AiError> {
        self.require_session_policy(principal, session_id, AiSessionAction::Delete)
            .await?;
        let existing = AiSessionRecord::find_by_id(&self.database, &session_id.0)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
            .ok_or(AiError::NotFound)?;
        if !is_owner(principal, &existing) {
            return Err(AiError::NotFound);
        }
        if existing.state == "deleting" {
            return Ok(true);
        }
        let scope = record_scope(&existing);
        self.require_scope(principal, &scope, AiSessionAction::Delete)
            .await?;
        let policy = self.protection_policy(principal, &scope).await?;
        let inbox_event_id = Uuid::new_v4();
        let protected_inbox_event = self
            .protect_value(
                &policy,
                content_context(
                    "graphql_orm_ai_inbox_events",
                    inbox_event_id,
                    "protected_payload",
                    &scope,
                ),
                json!({"sessionId": session_id.0, "state": "deleting"}),
            )
            .await?;
        let expected_kind = principal_identity(principal).0;
        let expected_subject = principal.subject().to_owned();
        let now = unix_seconds();
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let session = tx
                        .find_by_id::<AiSessionRecord>(&session_id.0)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if session.owner_principal_kind != expected_kind
                        || session.owner_subject != expected_subject
                    {
                        return Err(OrmPublicError::not_found());
                    }
                    if session.state == "deleting" {
                        return Ok(true);
                    }
                    let outcome = tx
                        .compare_and_swap::<AiSessionRecord>(
                            &session.id,
                            session.row_version,
                            AiSessionRecordWhereInput::default(),
                            UpdateAiSessionRecordInput {
                                state: Some("deleting".to_owned()),
                                deleted_at: Some(Some(now)),
                                last_activity_at: Some(now),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    match outcome {
                        ConditionalUpdateOutcome::Updated(_) => {
                            append_inbox_event(
                                tx,
                                PreparedAiInboxEvent {
                                    id: inbox_event_id,
                                    principal_kind: expected_kind,
                                    principal_subject: expected_subject,
                                    scope,
                                    session_id: session_id.0,
                                    event_type: "session_deleting".to_owned(),
                                    protected_payload: protected_inbox_event,
                                    created_at: now,
                                },
                            )
                            .await?;
                            Ok(true)
                        }
                        ConditionalUpdateOutcome::NotFound => Err(OrmPublicError::not_found()),
                        ConditionalUpdateOutcome::Conflict => {
                            Err(OrmPublicError::new(OrmErrorCode::Conflict))
                        }
                    }
                })
            })
            .await
            .map_err(map_transaction)
    }

    async fn send_message(
        &self,
        principal: &AuthPrincipal,
        input: SendAiMessageInput,
    ) -> Result<SendAiMessagePayload, AiError> {
        if input.text.trim().is_empty()
            || input.text.len() > self.limits.maximum_message_bytes
            || input.attachment_ids.len() > self.limits.maximum_attachments
        {
            return Err(AiError::InvalidInput(
                "message exceeds configured limits".to_owned(),
            ));
        }
        let mut deduplicated_attachments = input.attachment_ids.clone();
        deduplicated_attachments.sort_unstable();
        deduplicated_attachments.dedup();
        if deduplicated_attachments.len() != input.attachment_ids.len() {
            return Err(AiError::InvalidInput("duplicate attachment ID".to_owned()));
        }
        self.require_session_policy(
            principal,
            AiSessionId(input.session_id),
            AiSessionAction::Write,
        )
        .await?;
        let session = self
            .visible_session(
                principal,
                AiSessionId(input.session_id),
                AiSessionAction::Write,
            )
            .await?
            .ok_or(AiError::NotFound)?;
        if session.state != "active" {
            return Err(AiError::Conflict);
        }
        let scope = record_scope(&session);
        let policy = self.protection_policy(principal, &scope).await?;
        let message_id = Uuid::new_v4();
        let block_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        let event_id = Uuid::new_v4();
        let inbox_event_id = Uuid::new_v4();
        let content_hash = message_content_hash(&input.text, &deduplicated_attachments);
        let preview = bounded_prefix(&input.text, self.limits.maximum_preview_bytes);
        let protected_preview = self
            .protect_value(
                &policy,
                content_context(
                    "graphql_orm_ai_messages",
                    message_id,
                    "protected_preview",
                    &scope,
                ),
                json!(preview),
            )
            .await?;
        let protected_content = self
            .protect_value(
                &policy,
                content_context(
                    "graphql_orm_ai_message_blocks",
                    block_id,
                    "protected_content",
                    &scope,
                ),
                json!({"text": input.text}),
            )
            .await?;
        let protected_event = self
            .protect_value(
                &policy,
                content_context(
                    "graphql_orm_ai_session_events",
                    event_id,
                    "protected_payload",
                    &scope,
                ),
                json!({"messageId": message_id, "runId": run_id}),
            )
            .await?;
        let protected_inbox_event = self
            .protect_value(
                &policy,
                content_context(
                    "graphql_orm_ai_inbox_events",
                    inbox_event_id,
                    "protected_payload",
                    &scope,
                ),
                json!({"sessionId": input.session_id, "messageId": message_id, "runId": run_id}),
            )
            .await?;
        let principal_reference =
            serde_json::to_value(principal.reference()).map_err(|_| AiError::PersistenceFailed)?;
        let (principal_kind, principal_subject) = principal_identity(principal);
        let principal_subject = principal_subject.to_owned();
        let line_count = input.text.lines().count().max(1) as i64;
        let byte_count = input.text.len() as i64;
        let session_id = input.session_id;
        let client_message_id = input.client_message_id;
        let attachments = deduplicated_attachments;
        let now = unix_seconds();

        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let existing = tx
                        .query::<AiMessageRecord>()
                        .filter(AiMessageRecordWhereInput {
                            session_id: Some(UuidFilter {
                                eq: Some(session_id),
                                ..Default::default()
                            }),
                            client_message_id: Some(UuidFilter {
                                eq: Some(client_message_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(1)
                        .fetch_one()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if let Some(existing) = existing {
                        if existing.content_hash.as_deref() != Some(content_hash.as_str()) {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                        return existing
                            .run_id
                            .map(|run_id| SendAiMessagePayload {
                                message_id: existing.id,
                                run_id,
                            })
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::Conflict));
                    }

                    let current = tx
                        .find_by_id::<AiSessionRecord>(&session_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if current.owner_principal_kind != principal_kind
                        || current.owner_subject != principal_subject
                    {
                        return Err(OrmPublicError::not_found());
                    }
                    if current.state != "active" {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    for attachment_id in &attachments {
                        let attachment = tx
                            .find_by_id::<AiAttachmentRecord>(attachment_id)
                            .await
                            .map_err(OrmPublicError::from)?
                            .ok_or_else(OrmPublicError::not_found)?;
                        if attachment.owner_principal_kind != principal_kind
                            || attachment.owner_subject != principal_subject
                            || attachment.session_id != session_id
                            || attachment.deleted_at.is_some()
                            || attachment.quarantine_state != "released"
                            || attachment.scan_state != "clean"
                        {
                            return Err(OrmPublicError::not_found());
                        }
                    }

                    let message_sequence = current.message_head.saturating_add(1);
                    let event_sequence = current.stream_head.saturating_add(1);
                    let outcome = tx
                        .compare_and_swap::<AiSessionRecord>(
                            &current.id,
                            current.row_version,
                            AiSessionRecordWhereInput::default(),
                            UpdateAiSessionRecordInput {
                                message_head: Some(message_sequence),
                                stream_head: Some(event_sequence),
                                last_activity_at: Some(now),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(outcome, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }

                    tx.insert::<AiMessageRecord>(CreateAiMessageRecordInput {
                        id: message_id,
                        session_id,
                        sequence: message_sequence,
                        message_role: "user".to_owned(),
                        author_principal_kind: Some(principal_kind.clone()),
                        author_subject: Some(principal_subject.clone()),
                        client_message_id: Some(client_message_id),
                        content_hash: Some(content_hash),
                        run_id: Some(run_id),
                        provider_kind: None,
                        provider_model: None,
                        protected_preview: Some(protected_preview),
                        block_count: 1,
                        completion_state: "complete".to_owned(),
                        finalized_at: Some(now),
                        content_purged_at: None,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.insert::<AiMessageBlockRecord>(CreateAiMessageBlockRecordInput {
                        id: block_id,
                        message_id,
                        block_index: 0,
                        block_kind: "text".to_owned(),
                        protected_content,
                        byte_count,
                        line_count,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.insert::<AiRunRecord>(CreateAiRunRecordInput {
                        id: run_id,
                        session_id,
                        input_message_id: message_id,
                        principal_reference,
                        state: "queued".to_owned(),
                        attempt_id: None,
                        lease_owner: None,
                        lease_generation: 0,
                        lease_expires_at: None,
                        lease_heartbeat_at: None,
                        retry_count: 0,
                        next_attempt_at: Some(now),
                        error_code: None,
                        latest_checkpoint_id: None,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.insert::<AiSessionEventRecord>(CreateAiSessionEventRecordInput {
                        id: event_id,
                        session_id,
                        sequence: event_sequence,
                        event_type: "message_queued".to_owned(),
                        run_id: Some(run_id),
                        causation_id: Some(client_message_id.to_string()),
                        correlation_id: client_message_id.to_string(),
                        protected_payload: protected_event,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.queue_event(AiSessionWakeup {
                        session_id,
                        sequence: event_sequence,
                    });
                    append_inbox_event(
                        tx,
                        PreparedAiInboxEvent {
                            id: inbox_event_id,
                            principal_kind: principal_kind.clone(),
                            principal_subject: principal_subject.clone(),
                            scope,
                            session_id,
                            event_type: "message_queued".to_owned(),
                            protected_payload: protected_inbox_event,
                            created_at: now,
                        },
                    )
                    .await?;
                    for attachment_id in attachments {
                        let updated = tx
                            .update_by_id::<AiAttachmentRecord>(
                                &attachment_id,
                                UpdateAiAttachmentRecordInput {
                                    message_id: Some(Some(message_id)),
                                    ..Default::default()
                                },
                            )
                            .await
                            .map_err(OrmPublicError::from)?;
                        if updated.is_none() {
                            return Err(OrmPublicError::not_found());
                        }
                    }
                    Ok(SendAiMessagePayload { message_id, run_id })
                })
            })
            .await
            .map_err(map_transaction)
    }
}

impl OrmAiSessionService {
    async fn transition_session(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
        expected_state: &'static str,
        next_state: &'static str,
        archive: bool,
    ) -> Result<AiSessionView, AiError> {
        self.require_session_policy(principal, session_id, AiSessionAction::Archive)
            .await?;
        let visible = self
            .visible_session(principal, session_id, AiSessionAction::Archive)
            .await?
            .ok_or(AiError::NotFound)?;
        let scope = record_scope(&visible);
        let policy = self.protection_policy(principal, &scope).await?;
        let inbox_event_id = Uuid::new_v4();
        let protected_inbox_event = self
            .protect_value(
                &policy,
                content_context(
                    "graphql_orm_ai_inbox_events",
                    inbox_event_id,
                    "protected_payload",
                    &scope,
                ),
                json!({"sessionId": session_id.0, "state": next_state}),
            )
            .await?;
        let inbox_event_type = if archive {
            "session_archived"
        } else {
            "session_restored"
        };
        let (expected_kind, expected_subject) = principal_identity(principal);
        let expected_subject = expected_subject.to_owned();
        let now = unix_seconds();
        let record = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = tx
                        .find_by_id::<AiSessionRecord>(&session_id.0)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if current.owner_principal_kind != expected_kind
                        || current.owner_subject != expected_subject
                    {
                        return Err(OrmPublicError::not_found());
                    }
                    if current.state != expected_state {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let outcome = tx
                        .compare_and_swap::<AiSessionRecord>(
                            &current.id,
                            current.row_version,
                            AiSessionRecordWhereInput::default(),
                            UpdateAiSessionRecordInput {
                                state: Some(next_state.to_owned()),
                                archived_at: Some(archive.then_some(now)),
                                last_activity_at: Some(now),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    match outcome {
                        ConditionalUpdateOutcome::Updated(record) => {
                            append_inbox_event(
                                tx,
                                PreparedAiInboxEvent {
                                    id: inbox_event_id,
                                    principal_kind: expected_kind,
                                    principal_subject: expected_subject,
                                    scope,
                                    session_id: session_id.0,
                                    event_type: inbox_event_type.to_owned(),
                                    protected_payload: protected_inbox_event,
                                    created_at: now,
                                },
                            )
                            .await?;
                            Ok(record)
                        }
                        ConditionalUpdateOutcome::NotFound => Err(OrmPublicError::not_found()),
                        ConditionalUpdateOutcome::Conflict => {
                            Err(OrmPublicError::new(OrmErrorCode::Conflict))
                        }
                    }
                })
            })
            .await
            .map_err(map_transaction)?;
        Ok(session_view(&record))
    }
}

fn principal_identity(principal: &AuthPrincipal) -> (String, &str) {
    let kind = match principal {
        AuthPrincipal::User(_) => "user".to_owned(),
        AuthPrincipal::ApiToken(token) => {
            format!("api_token:{}", token.principal_kind.as_str())
        }
    };
    (kind, principal.subject())
}

fn is_owner(principal: &AuthPrincipal, session: &AiSessionRecord) -> bool {
    let (kind, subject) = principal_identity(principal);
    session.owner_principal_kind == kind && session.owner_subject == subject
}

fn record_scope(session: &AiSessionRecord) -> AiScope {
    AiScope {
        kind: session.scope_kind.clone(),
        id: session.scope_id.clone(),
        tenant_id: session.tenant_id.clone(),
    }
}

fn validate_scope(scope: &AiScope) -> Result<(), AiError> {
    if scope.kind.trim().is_empty()
        || scope.id.trim().is_empty()
        || scope.kind.len() > 128
        || scope.id.len() > 512
        || scope
            .tenant_id
            .as_ref()
            .is_some_and(|tenant| tenant.trim().is_empty() || tenant.len() > 512)
    {
        return Err(AiError::InvalidInput("invalid AI scope".to_owned()));
    }
    Ok(())
}

fn session_view(record: &AiSessionRecord) -> AiSessionView {
    AiSessionView {
        id: record.id,
        scope_kind: record.scope_kind.clone(),
        scope_id: record.scope_id.clone(),
        title: record.title.clone(),
        state: record.state.clone(),
        stream_head: record.stream_head,
        last_activity_at: record.last_activity_at,
        archived_at: record.archived_at,
    }
}

fn message_view(record: &AiMessageRecord, preview: String, content_purged: bool) -> AiMessageView {
    AiMessageView {
        id: record.id,
        session_id: record.session_id,
        sequence: record.sequence,
        role: record.message_role.clone(),
        author_subject: record.author_subject.clone(),
        run_id: record.run_id,
        preview,
        content_purged,
        block_count: record.block_count,
        completion_state: record.completion_state.clone(),
        created_at: record.created_at,
    }
}

fn page_input(
    page: &ValidatedKeysetConnection,
    include_total_count: bool,
) -> KeysetConnectionInput {
    match page.direction {
        KeysetWindowDirection::Forward => KeysetConnectionInput {
            after: page.cursor.clone(),
            first: Some(page.limit),
            include_total_count,
            ..Default::default()
        },
        KeysetWindowDirection::Backward => KeysetConnectionInput {
            before: page.cursor.clone(),
            last: Some(page.limit),
            include_total_count,
            ..Default::default()
        },
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

fn bounded_prefix(value: &str, maximum_bytes: usize) -> &str {
    if value.len() <= maximum_bytes {
        return value;
    }
    let mut end = maximum_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn message_content_hash(text: &str, attachment_ids: &[Uuid]) -> String {
    let mut hash = Sha256::new();
    hash.update(b"graphql-orm-ai/message/v1\0");
    hash.update((text.len() as u64).to_be_bytes());
    hash.update(text.as_bytes());
    for id in attachment_ids {
        hash.update(id.as_bytes());
    }
    hex::encode(hash.finalize())
}

fn unix_seconds() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
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
