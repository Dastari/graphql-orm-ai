//! Bounded host-scheduled conversational retention contracts.

use async_trait::async_trait;

use crate::AiError;

/// Bounded result of one session-retention scan page.
///
/// This report proves only the exact ORM rows removed or scrubbed by one
/// completed pass. It does not prove that attachments, provider-persistent
/// files, tool/proposal content, or an entire deleting session were purged.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AiSessionRetentionReport {
    /// Session rows considered in this scan page.
    pub sessions_scanned: u32,
    /// Sessions whose live-delta or message content changed.
    pub sessions_changed: u32,
    /// Protected provisional `provider_live_delta` event rows deleted.
    pub live_delta_events_deleted: u32,
    /// Finalized message previews scrubbed after their retention deadline.
    pub message_contents_purged: u32,
    /// Protected message-block rows deleted with those previews.
    pub message_blocks_deleted: u32,
    /// Sessions skipped because their GraphQL-managed retention policy was
    /// absent or invalid.
    pub sessions_not_ready: u32,
    /// Sessions whose bounded transaction lost a CAS race.
    pub sessions_conflicted: u32,
    /// Expired messages retained because their run was nonterminal or a linked
    /// attachment still owned external/protected content.
    pub messages_blocked: u32,
    /// Opaque cursor for the next bounded session scan page. Absence means the
    /// current cycle reached the end; a later cycle starts again from `None`.
    pub next_session_cursor: Option<String>,
}

/// Host-only bounded retention backend for protected session content.
///
/// This is not a user GraphQL operation. Implementations read only the current
/// GraphQL-managed scope policy, never open protected content, never use raw
/// SQL, and must retain metadata needed for audit, usage, fencing, and restore.
#[async_trait]
pub trait AiSessionRetentionService: Send + Sync {
    /// Prunes one bounded keyset page of sessions.
    ///
    /// `after_session_cursor` must be absent at the start of a scan cycle and
    /// then set to the prior report's `next_session_cursor` until it becomes
    /// absent again.
    ///
    /// # Errors
    ///
    /// Returns a safe error for a malformed cursor, corrupt session/message/
    /// event state, unsafe dependency binding, arithmetic overflow, or
    /// persistence failure. Missing policies and safe per-message blockers are
    /// counted without deleting affected content.
    async fn prune_session_content(
        &self,
        after_session_cursor: Option<String>,
    ) -> Result<AiSessionRetentionReport, AiError>;
}
