//! Durable per-principal cross-session inbox contracts.

use std::pin::Pin;

use agql_auth::AuthPrincipal;
use async_graphql::SimpleObject;
use async_trait::async_trait;
use futures::Stream;
use uuid::Uuid;

use crate::AiError;

/// Commit-only wakeup hint for one principal inbox.
///
/// This value is never delivered directly to clients. Durable ORM rows remain
/// the source of truth after every wakeup or lag condition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiInboxWakeup {
    /// Principal class whose stream advanced.
    pub principal_kind: String,
    /// Principal subject whose stream advanced.
    pub principal_subject: String,
    /// Sequence observed in the committing transaction.
    pub sequence: i64,
}

/// One authorized durable cross-session notification.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiInboxEventView {
    /// Event identifier.
    pub id: Uuid,
    /// Per-principal monotonic sequence.
    pub sequence: i64,
    /// Related AI session.
    pub session_id: Uuid,
    /// Stable server-authored event type.
    pub event_type: String,
    /// Authorized and opened bounded payload.
    pub payload: async_graphql::Json<serde_json::Value>,
    /// Creation timestamp in Unix seconds.
    pub created_at: i64,
}

/// Bounded inbox catch-up page.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiInboxEventPage {
    /// Events after the requested exclusive sequence.
    pub events: Vec<AiInboxEventView>,
    /// Stream head captured for this page/replay handoff.
    pub watermark: i64,
    /// Whether another bounded page remains before the watermark.
    pub has_more: bool,
    /// Whether retention removed required history and the client must reload.
    pub reset_required: bool,
}

/// Inbox subscription item with explicit retention-gap signaling.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiInboxEventEnvelope {
    /// Durable event, absent only for a reset signal.
    pub event: Option<AiInboxEventView>,
    /// Replay watermark associated with this delivery.
    pub watermark: i64,
    /// Whether the client must discard its cursor and reload bounded session
    /// shells before reconnecting from the new stream head.
    pub reset_required: bool,
}

/// Type-erased bounded principal-inbox stream.
pub type AiInboxEventStream =
    Pin<Box<dyn Stream<Item = Result<AiInboxEventEnvelope, AiError>> + Send>>;

/// Current-principal inbox backend.
///
/// Implementations must bind every row to the exact principal kind and
/// subject, reauthorize each referenced session/scope, and reopen protected
/// payloads only after those checks. A wakeup is never authorization.
#[async_trait]
pub trait AiInboxService: Send + Sync {
    /// Reads one bounded catch-up page after an exclusive sequence.
    ///
    /// # Errors
    ///
    /// Returns a safe error for invalid bounds, principal/session/scope denial,
    /// malformed protected content, or persistence failure.
    async fn inbox_event_page(
        &self,
        principal: &AuthPrincipal,
        after_sequence: i64,
        first: i64,
    ) -> Result<AiInboxEventPage, AiError>;

    /// Replays durable events and follows commit-only wakeup hints.
    ///
    /// # Errors
    ///
    /// Returns a safe error when the initial bounds or principal stream cannot
    /// be authorized. Errors after opening are emitted by the returned stream.
    async fn inbox_events(
        &self,
        principal: AuthPrincipal,
        after_sequence: i64,
    ) -> Result<AiInboxEventStream, AiError>;
}

/// Bounded outcome of one host-scheduled inbox retention pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AiInboxPruningReport {
    /// Principal streams considered within the deployment-owned scan bound.
    pub streams_scanned: u32,
    /// Streams whose retained cursor advanced.
    pub streams_pruned: u32,
    /// Exact durable inbox events deleted.
    pub events_deleted: u32,
    /// Streams left unchanged because one required scope policy was absent or
    /// had not been migrated to the current contract.
    pub streams_not_ready: u32,
    /// Streams left unchanged because a concurrent append/prune won the CAS.
    pub streams_conflicted: u32,
}

/// Host-scheduled bounded inbox retention backend.
///
/// This is deliberately not a user GraphQL operation. Implementations must
/// read GraphQL-managed policies, delete only a contiguous expired prefix,
/// preserve a configured recent-event floor, and atomically advance the
/// minimum retained sequence without rewinding the stream head.
#[async_trait]
pub trait AiInboxPruningService: Send + Sync {
    /// Performs one bounded retention pass.
    ///
    /// # Errors
    ///
    /// Returns a safe error for malformed persistent state or persistence
    /// failure. Missing/migration-pending policies are reported without
    /// deleting the affected prefix.
    async fn prune_inbox_events(&self) -> Result<AiInboxPruningReport, AiError>;
}
