//! Bounded host-scheduled conversational retention contracts.

use async_trait::async_trait;

use crate::AiError;

/// Bounded result of one session-retention scan page.
///
/// This report proves only the exact ORM rows removed or scrubbed by one
/// completed pass. Attachment counts prove only cleanup coordination or exact
/// metadata deletion after a separate worker confirmed object absence. They
/// do not prove that every provider-persistent file, artifact row, tool
/// dependency, unresolved accepted proposal, or an entire deleting session was
/// purged.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AiSessionRetentionReport {
    /// Session rows considered in this scan page.
    pub sessions_scanned: u32,
    /// Sessions whose protected content, attachment state, or checkpoint
    /// references changed.
    pub sessions_changed: u32,
    /// Protected provisional `provider_live_delta` event rows deleted.
    pub live_delta_events_deleted: u32,
    /// Protected event rows deleted after a session-deletion retention cutoff.
    pub deleting_session_events_deleted: u32,
    /// Protected principal-inbox payloads tombstoned after a session-deletion
    /// retention cutoff and before message content.
    pub deleting_session_inbox_payloads_purged: u32,
    /// Protected context-summary checkpoints deleted before message scrubbing.
    pub deleting_session_context_checkpoints_deleted: u32,
    /// Context-summary checkpoints invalidated by physical deletion before an
    /// ordinarily expired covered message is scrubbed.
    pub context_checkpoints_invalidated: u32,
    /// Terminal proposal payloads scrubbed after their context dependencies.
    pub deleting_session_proposal_payloads_purged: u32,
    /// Terminal tool-call payloads scrubbed after proposal dependencies.
    pub deleting_session_tool_payloads_purged: u32,
    /// Terminal approval payloads scrubbed with their exact tool calls.
    pub deleting_session_approval_payloads_purged: u32,
    /// Age-expired terminal tool-call payloads scrubbed under the current raw
    /// payload-retention policy.
    pub expired_tool_payloads_purged: u32,
    /// Age-expired terminal approval payloads scrubbed with their exact tool
    /// calls.
    pub expired_approval_payloads_purged: u32,
    /// Age-expired orphaned protected coordinator checkpoints physically
    /// deleted after their terminal history and content dependencies were
    /// re-proved.
    pub expired_run_checkpoints_deleted: u32,
    /// Terminal run pointers cleared before append-only checkpoint deletion.
    pub deleting_session_run_checkpoint_references_cleared: u32,
    /// Append-only coordinator checkpoints physically deleted after their
    /// terminal run pointers and ordinary protected sources were exhausted.
    pub deleting_session_run_checkpoints_deleted: u32,
    /// Finalized message previews scrubbed after a retention cutoff.
    pub message_contents_purged: u32,
    /// Protected message-block rows deleted with those previews.
    pub message_blocks_deleted: u32,
    /// Attachment rows moved into externally verified cleanup after a session
    /// deletion cutoff.
    pub deleting_session_attachment_cleanups_requested: u32,
    /// Fully cleaned attachment metadata rows physically deleted before their
    /// linked message content was scrubbed.
    pub deleting_session_attachments_deleted: u32,
    /// Attachment-artifact rows moved into exact-reference cleanup before
    /// their parent attachment can enter cleanup.
    pub deleting_session_attachment_artifact_cleanups_requested: u32,
    /// Fully tombstoned attachment-artifact metadata rows physically deleted
    /// before their parent attachment was cleaned.
    pub deleting_session_attachment_artifacts_deleted: u32,
    /// Session shells atomically finalized as `deleted` after every bounded
    /// protected/external dependency and coordinator checkpoint was proved
    /// exhausted.
    pub deleting_sessions_finalized: u32,
    /// Sessions skipped because their GraphQL-managed retention policy was
    /// absent or invalid.
    pub sessions_not_ready: u32,
    /// Sessions whose bounded transaction lost a CAS race.
    pub sessions_conflicted: u32,
    /// Eligible messages retained because their run was nonterminal or a
    /// linked attachment still owned external/protected content.
    pub messages_blocked: u32,
    /// Deleting sessions still waiting for bounded attachment/artifact proof,
    /// exact external absence, or a configured provider-file cleanup boundary.
    pub attachment_cleanups_blocked: u32,
    /// Deleting sessions whose proposal set was over-bound, nonterminal, or
    /// still accepted without an authoritative applied outcome.
    pub proposal_payload_purges_blocked: u32,
    /// Deleting sessions whose tool/approval set was over-bound, nonterminal,
    /// uncertain, or retained active authority.
    pub tool_payload_purges_blocked: u32,
    /// Sessions whose age-based raw tool/approval proof exceeded a deployment
    /// bound or could not prove an exact terminal payload graph.
    pub raw_payload_purges_blocked: u32,
    /// Sessions whose age-based protected-checkpoint purge exceeded a proof
    /// bound or could not prove exact terminal history and dependencies.
    pub raw_checkpoint_purges_blocked: u32,
    /// Deleting sessions whose run history exceeded the configured proof bound
    /// or still contained a nonterminal run.
    pub run_checkpoint_purges_blocked: u32,
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
    /// event/context state, unsafe dependency binding, arithmetic overflow, or
    /// persistence failure. Missing policies and safe per-message blockers are
    /// counted without deleting affected content.
    async fn prune_session_content(
        &self,
        after_session_cursor: Option<String>,
    ) -> Result<AiSessionRetentionReport, AiError>;
}
