//! Bounded per-user session GraphQL contract.

use std::sync::Arc;

use agql_auth::AuthPrincipal;
use async_graphql::{Context, ErrorExtensions, InputObject, Object, SimpleObject};
use async_trait::async_trait;
use graphql_orm::graphql::pagination::{
    KeysetConnectionInput, PageInfo, ValidatedKeysetConnection,
};
use uuid::Uuid;

use crate::{
    AiError, AiInboxEventPage, AiInboxService, AiScope, AiSessionId, AiUsageConnection,
    AiUsageFilterInput,
};

/// Scope input for session creation/configuration.
#[derive(Clone, Debug, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiScopeInput {
    /// Host-defined scope kind.
    pub kind: String,
    /// Host-defined scope ID.
    pub id: String,
    /// Optional tenant ID.
    pub tenant_id: Option<String>,
}

impl From<AiScopeInput> for AiScope {
    fn from(value: AiScopeInput) -> Self {
        Self {
            kind: value.kind,
            id: value.id,
            tenant_id: value.tenant_id,
        }
    }
}

/// Bounded session shell.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiSessionView {
    /// Session ID.
    pub id: Uuid,
    /// Scope kind.
    pub scope_kind: String,
    /// Scope ID.
    pub scope_id: String,
    /// User-visible title.
    pub title: String,
    /// Active/archived/deleting state.
    pub state: String,
    /// Durable event stream head.
    pub stream_head: i64,
    /// Last activity timestamp in Unix seconds.
    pub last_activity_at: i64,
    /// Archive timestamp.
    pub archived_at: Option<i64>,
}

/// Session connection edge.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiSessionEdge {
    /// Session node.
    pub node: AiSessionView,
    /// Opaque keyset cursor.
    pub cursor: String,
}

/// Bounded session connection.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiSessionConnection {
    /// Bounded edges.
    pub edges: Vec<AiSessionEdge>,
    /// Relay page metadata.
    pub page_info: PageInfo,
}

/// Message shell; large content remains in separately windowed blocks.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiMessageView {
    /// Message ID.
    pub id: Uuid,
    /// Session ID.
    pub session_id: Uuid,
    /// Stable session sequence.
    pub sequence: i64,
    /// User/assistant/tool/system role.
    pub role: String,
    /// Safe author reference.
    pub author_subject: Option<String>,
    /// Producing run.
    pub run_id: Option<Uuid>,
    /// Protected/decrypted bounded preview, maximum 4 KiB.
    pub preview: String,
    /// Whether retention removed the protected preview and content blocks.
    pub content_purged: bool,
    /// Number of separately fetched blocks.
    pub block_count: i64,
    /// Completion state.
    pub completion_state: String,
    /// Creation timestamp.
    pub created_at: i64,
}

/// Message connection edge.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiMessageEdge {
    /// Message shell.
    pub node: AiMessageView,
    /// Opaque keyset cursor.
    pub cursor: String,
}

/// Bounded bidirectional message connection.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiMessageConnection {
    /// Bounded edges.
    pub edges: Vec<AiMessageEdge>,
    /// Relay page metadata.
    pub page_info: PageInfo,
}

/// One bounded message content block.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiMessageBlockView {
    /// Block ID.
    pub id: Uuid,
    /// Parent message.
    pub message_id: Uuid,
    /// Stable block order.
    pub block_index: i64,
    /// Block kind.
    pub kind: String,
    /// Authorized/decrypted JSON content.
    pub content: async_graphql::Json<serde_json::Value>,
    /// Original byte count.
    pub byte_count: i64,
    /// Original line count.
    pub line_count: i64,
}

/// Durable event view.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiSessionEventView {
    /// Event ID.
    pub id: Uuid,
    /// Session sequence.
    pub sequence: i64,
    /// Stable event type.
    pub event_type: String,
    /// Optional run.
    pub run_id: Option<Uuid>,
    /// Correlation identifier.
    pub correlation_id: String,
    /// Authorized/decrypted event payload.
    pub payload: async_graphql::Json<serde_json::Value>,
    /// Creation timestamp.
    pub created_at: i64,
}

/// Bounded durable event catch-up page.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiSessionEventPage {
    /// Events after the requested sequence.
    pub events: Vec<AiSessionEventView>,
    /// Watermark captured for replay/live handoff.
    pub watermark: i64,
    /// Whether another bounded page remains before the watermark.
    pub has_more: bool,
    /// Whether retention removed the requested sequence.
    pub reset_required: bool,
}

/// Session creation input.
#[derive(Clone, Debug, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct CreateAiSessionInput {
    /// Application scope.
    pub scope: AiScopeInput,
    /// Optional initial title.
    pub title: Option<String>,
}

/// Message submission input.
#[derive(Clone, Debug, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct SendAiMessageInput {
    /// Session ID.
    pub session_id: Uuid,
    /// User text, bounded by service policy.
    pub text: String,
    /// Already-authorized AI attachment IDs.
    #[graphql(default)]
    pub attachment_ids: Vec<Uuid>,
    /// Client idempotency ID.
    pub client_message_id: Uuid,
}

/// Accepted message/run references.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct SendAiMessagePayload {
    /// Persisted user message.
    pub message_id: Uuid,
    /// Queued run.
    pub run_id: Uuid,
}

/// Owner/scope-aware session backend. Implementations must use keyset-bounded
/// queries and never return data owned by another principal.
#[async_trait]
pub trait AiSessionService: Send + Sync {
    /// Lists visible session shells.
    async fn sessions(
        &self,
        principal: &AuthPrincipal,
        page: ValidatedKeysetConnection,
    ) -> Result<AiSessionConnection, AiError>;

    /// Loads one visible session shell.
    async fn session(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
    ) -> Result<Option<AiSessionView>, AiError>;

    /// Loads a bounded bidirectional message-shell window.
    async fn messages(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
        page: ValidatedKeysetConnection,
    ) -> Result<AiMessageConnection, AiError>;

    /// Loads a bounded block window for one visible message.
    async fn message_blocks(
        &self,
        principal: &AuthPrincipal,
        message_id: Uuid,
        after_block_index: Option<i64>,
        first: i64,
    ) -> Result<Vec<AiMessageBlockView>, AiError>;

    /// Loads durable events for reconnect/catch-up.
    async fn session_event_page(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
        after_sequence: i64,
        first: i64,
    ) -> Result<AiSessionEventPage, AiError>;

    /// Creates an owner-only session.
    async fn create_session(
        &self,
        principal: &AuthPrincipal,
        input: CreateAiSessionInput,
    ) -> Result<AiSessionView, AiError>;

    /// Archives a visible owned session.
    async fn archive_session(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
    ) -> Result<AiSessionView, AiError>;

    /// Restores an archived owned session.
    async fn restore_session(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
    ) -> Result<AiSessionView, AiError>;

    /// Starts content/blob purge for an owned session.
    async fn delete_session(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
    ) -> Result<bool, AiError>;

    /// Persists a user message and queues its fenced run atomically.
    async fn send_message(
        &self,
        principal: &AuthPrincipal,
        input: SendAiMessageInput,
    ) -> Result<SendAiMessagePayload, AiError>;
}

/// Composable AI query root.
#[derive(Clone, Copy, Debug, Default)]
pub struct AiQueryRoot;

#[cfg_attr(
    feature = "graphql-case-pascal",
    Object(rename_fields = "PascalCase", rename_args = "PascalCase")
)]
#[cfg_attr(not(feature = "graphql-case-pascal"), Object)]
impl AiQueryRoot {
    /// Returns a bounded keyset window of immutable provider usage.
    async fn ai_usage(
        &self,
        context: &Context<'_>,
        scope: AiScopeInput,
        #[graphql(default)] filter: AiUsageFilterInput,
        #[graphql(default)] page: KeysetConnectionInput,
    ) -> async_graphql::Result<AiUsageConnection> {
        crate::usage::resolve_usage(context, scope, filter, page).await
    }

    /// Returns bounded session shells.
    async fn ai_sessions(
        &self,
        context: &Context<'_>,
        #[graphql(default)] page: KeysetConnectionInput,
    ) -> async_graphql::Result<AiSessionConnection> {
        let principal = agql_auth::principal_from_ctx(context)?;
        let page = page.validate(50, 200).map_err(|error| (&error).extend())?;
        service(context)?
            .sessions(&principal, page)
            .await
            .map_err(extend)
    }

    /// Returns one session shell, never full history.
    async fn ai_session(
        &self,
        context: &Context<'_>,
        id: Uuid,
    ) -> async_graphql::Result<Option<AiSessionView>> {
        let principal = agql_auth::principal_from_ctx(context)?;
        service(context)?
            .session(&principal, AiSessionId(id))
            .await
            .map_err(extend)
    }

    /// Returns a bounded bidirectional message window.
    async fn ai_messages(
        &self,
        context: &Context<'_>,
        session_id: Uuid,
        #[graphql(default)] page: KeysetConnectionInput,
    ) -> async_graphql::Result<AiMessageConnection> {
        let principal = agql_auth::principal_from_ctx(context)?;
        let page = if page == KeysetConnectionInput::default() {
            KeysetConnectionInput {
                last: Some(50),
                ..KeysetConnectionInput::default()
            }
        } else {
            page
        };
        let page = page.validate(50, 200).map_err(|error| (&error).extend())?;
        service(context)?
            .messages(&principal, AiSessionId(session_id), page)
            .await
            .map_err(extend)
    }

    /// Returns a bounded message-block window.
    async fn ai_message_blocks(
        &self,
        context: &Context<'_>,
        message_id: Uuid,
        after_block_index: Option<i64>,
        first: Option<i64>,
    ) -> async_graphql::Result<Vec<AiMessageBlockView>> {
        let principal = agql_auth::principal_from_ctx(context)?;
        let first = first.unwrap_or(20);
        if !(1..=100).contains(&first) || after_block_index.is_some_and(|value| value < 0) {
            return Err(AiError::InvalidInput("invalid message-block window".to_owned()).extend());
        }
        service(context)?
            .message_blocks(&principal, message_id, after_block_index, first)
            .await
            .map_err(extend)
    }

    /// Returns a bounded durable catch-up page.
    async fn ai_session_event_page(
        &self,
        context: &Context<'_>,
        session_id: Uuid,
        after_sequence: Option<i64>,
        first: Option<i64>,
    ) -> async_graphql::Result<AiSessionEventPage> {
        let principal = agql_auth::principal_from_ctx(context)?;
        let after_sequence = after_sequence.unwrap_or(0);
        let first = first.unwrap_or(100);
        if after_sequence < 0 || after_sequence > i64::from(i32::MAX) || !(1..=500).contains(&first)
        {
            return Err(AiError::InvalidInput("invalid event window".to_owned()).extend());
        }
        service(context)?
            .session_event_page(&principal, AiSessionId(session_id), after_sequence, first)
            .await
            .map_err(extend)
    }

    /// Returns a bounded cross-session inbox catch-up page for the current
    /// principal.
    async fn ai_inbox_event_page(
        &self,
        context: &Context<'_>,
        after_sequence: Option<i64>,
        first: Option<i64>,
    ) -> async_graphql::Result<AiInboxEventPage> {
        let principal = agql_auth::principal_from_ctx(context)?;
        let after_sequence = after_sequence.unwrap_or(0);
        let first = first.unwrap_or(100);
        if after_sequence < 0 || after_sequence > i64::from(i32::MAX) || !(1..=500).contains(&first)
        {
            return Err(AiError::InvalidInput("invalid inbox event window".to_owned()).extend());
        }
        inbox_service(context)?
            .inbox_event_page(&principal, after_sequence, first)
            .await
            .map_err(extend)
    }
}

/// Composable AI mutation root.
#[derive(Clone, Copy, Debug, Default)]
pub struct AiMutationRoot;

#[cfg_attr(
    feature = "graphql-case-pascal",
    Object(rename_fields = "PascalCase", rename_args = "PascalCase")
)]
#[cfg_attr(not(feature = "graphql-case-pascal"), Object)]
impl AiMutationRoot {
    /// Creates a private owner-only session.
    async fn create_ai_session(
        &self,
        context: &Context<'_>,
        input: CreateAiSessionInput,
    ) -> async_graphql::Result<AiSessionView> {
        let principal = agql_auth::principal_from_ctx(context)?;
        service(context)?
            .create_session(&principal, input)
            .await
            .map_err(extend)
    }

    /// Archives a session.
    async fn archive_ai_session(
        &self,
        context: &Context<'_>,
        id: Uuid,
    ) -> async_graphql::Result<AiSessionView> {
        let principal = agql_auth::principal_from_ctx(context)?;
        service(context)?
            .archive_session(&principal, AiSessionId(id))
            .await
            .map_err(extend)
    }

    /// Restores an archived session.
    async fn restore_ai_session(
        &self,
        context: &Context<'_>,
        id: Uuid,
    ) -> async_graphql::Result<AiSessionView> {
        let principal = agql_auth::principal_from_ctx(context)?;
        service(context)?
            .restore_session(&principal, AiSessionId(id))
            .await
            .map_err(extend)
    }

    /// Starts session content/blob purge.
    async fn delete_ai_session(
        &self,
        context: &Context<'_>,
        id: Uuid,
    ) -> async_graphql::Result<bool> {
        let principal = agql_auth::principal_from_ctx(context)?;
        service(context)?
            .delete_session(&principal, AiSessionId(id))
            .await
            .map_err(extend)
    }

    /// Persists a message and queues a run.
    async fn send_ai_message(
        &self,
        context: &Context<'_>,
        input: SendAiMessageInput,
    ) -> async_graphql::Result<SendAiMessagePayload> {
        let principal = agql_auth::principal_from_ctx(context)?;
        if input.text.is_empty() || input.text.len() > 256 * 1024 || input.attachment_ids.len() > 10
        {
            return Err(
                AiError::InvalidInput("message exceeds configured limits".to_owned()).extend(),
            );
        }
        service(context)?
            .send_message(&principal, input)
            .await
            .map_err(extend)
    }
}

fn service(context: &Context<'_>) -> async_graphql::Result<Arc<dyn AiSessionService>> {
    context
        .data::<Arc<dyn AiSessionService>>()
        .cloned()
        .map_err(|_| {
            AiError::InvalidConfiguration("AI session service is not installed".to_owned()).extend()
        })
}

fn inbox_service(context: &Context<'_>) -> async_graphql::Result<Arc<dyn AiInboxService>> {
    context
        .data_opt::<Arc<dyn AiInboxService>>()
        .cloned()
        .ok_or_else(|| {
            AiError::InvalidConfiguration("AI inbox service is not installed".to_owned()).extend()
        })
}

fn extend(error: AiError) -> async_graphql::Error {
    error.extend()
}
