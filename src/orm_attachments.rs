//! ORM-backed owner-isolated attachment intake and lifecycle.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::collections::BTreeMap;
use std::sync::Arc;

use agql_auth::{AuthPrincipal, Clock, ResolvedPrincipal};
use async_trait::async_trait;
use futures::StreamExt;
use graphql_orm::db::Database;
use graphql_orm::graphql::errors::{OrmErrorCode, OrmPublicError};
use graphql_orm::graphql::filters::{IntFilter, StringFilter, UuidFilter};
use graphql_orm::graphql::orm::{
    ConditionalUpdateOutcome, DefaultWriteBackend, TransactionError, TransactionMode,
};
use graphql_orm::graphql::pagination::{
    KeysetConnectionInput, KeysetWindowDirection, ValidatedKeysetConnection,
};
use graphql_orm_storage::{BlobPutOptions, BlobStore, StorageByteStream};
use secrecy::{ExposeSecret, SecretString};
use serde_json::json;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use time::Duration;
use uuid::Uuid;

use crate::orm_session_retention::{
    attachment_retention_cleanup_pending, session_scope as retention_session_scope,
    valid_policy as valid_session_retention_policy,
    validate_attachment as validate_retention_attachment,
    validate_attachment_artifact as validate_retention_artifact,
    validate_session as validate_retention_session,
};
use crate::persistence::*;
use crate::{
    AiAccessPolicy, AiAttachmentAcceptancePolicy, AiAttachmentCandidate, AiAttachmentCleanupReport,
    AiAttachmentCleanupService, AiAttachmentConnection, AiAttachmentEdge, AiAttachmentScanRequest,
    AiAttachmentScanVerdict, AiAttachmentScanner, AiAttachmentService, AiAttachmentUploadService,
    AiAttachmentUploadTicket, AiAttachmentView, AiContentProtectionPolicy,
    AiContentProtectionPolicyResolver, AiContentProtector, AiError, AiProviderAttachmentRequest,
    AiProviderAttachmentResolver, AiProviderFileDeletionRequest, AiProviderFileDeletionService,
    AiResolvedProviderAttachment, AiScope, AiSessionAction, AiSessionId, AiSessionWakeup,
    ContentProtectionContext, CreateAiAttachmentUploadInput, valid_mime, valid_safe_reference,
    valid_sha256,
};

/// Deployment hard limits for attachment intake.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiAttachmentServiceLimits {
    maximum_attachment_bytes: u64,
    maximum_filename_bytes: usize,
    upload_ticket_ttl: Duration,
    upload_processing_ttl: Duration,
}

impl AiAttachmentServiceLimits {
    /// Creates validated deployment hard limits.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless attachment size is in
    /// `1..=100 MiB`, filename size is in `1..=1024`, and ticket lifetime is
    /// positive and no more than one hour.
    pub fn new(
        maximum_attachment_bytes: u64,
        maximum_filename_bytes: usize,
        upload_ticket_ttl: Duration,
    ) -> Result<Self, AiError> {
        if !(1..=100 * 1024 * 1024).contains(&maximum_attachment_bytes)
            || !(1..=1_024).contains(&maximum_filename_bytes)
            || !upload_ticket_ttl.is_positive()
            || upload_ticket_ttl > Duration::hours(1)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid attachment service limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_attachment_bytes,
            maximum_filename_bytes,
            upload_ticket_ttl,
            upload_processing_ttl: Duration::hours(1),
        })
    }

    /// Overrides the maximum uninterrupted upload/scanner phase.
    ///
    /// Once this deadline passes, a cleanup worker may fence the row and
    /// delete its quarantine objects. Active deployments should choose a value
    /// longer than their maximum accepted upload and scanner latency.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless the duration is
    /// positive and no more than 24 hours.
    pub fn with_upload_processing_ttl(mut self, ttl: Duration) -> Result<Self, AiError> {
        if !ttl.is_positive() || ttl > Duration::hours(24) {
            return Err(AiError::InvalidConfiguration(
                "invalid attachment processing lifetime".to_owned(),
            ));
        }
        self.upload_processing_ttl = ttl;
        Ok(self)
    }

    /// Maximum exact bytes for one attachment.
    pub const fn maximum_attachment_bytes(self) -> u64 {
        self.maximum_attachment_bytes
    }

    /// Maximum UTF-8 bytes retained in the sanitized filename.
    pub const fn maximum_filename_bytes(self) -> usize {
        self.maximum_filename_bytes
    }

    /// One-time upload ticket lifetime.
    pub const fn upload_ticket_ttl(self) -> Duration {
        self.upload_ticket_ttl
    }

    /// Maximum uninterrupted upload/scanner lifetime.
    pub const fn upload_processing_ttl(self) -> Duration {
        self.upload_processing_ttl
    }
}

impl Default for AiAttachmentServiceLimits {
    fn default() -> Self {
        Self {
            maximum_attachment_bytes: 25 * 1024 * 1024,
            maximum_filename_bytes: 255,
            upload_ticket_ttl: Duration::minutes(10),
            upload_processing_ttl: Duration::hours(1),
        }
    }
}

/// Bounded host-scheduled cleanup worker limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiAttachmentCleanupLimits {
    maximum_batch_size: u32,
    cleanup_lease_ttl: Duration,
}

impl AiAttachmentCleanupLimits {
    /// Creates validated cleanup limits.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless the batch is in
    /// `1..=200` and the claim lifetime is positive and no more than one hour.
    pub fn new(maximum_batch_size: u32, cleanup_lease_ttl: Duration) -> Result<Self, AiError> {
        if !(1..=200).contains(&maximum_batch_size)
            || !cleanup_lease_ttl.is_positive()
            || cleanup_lease_ttl > Duration::hours(1)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid attachment cleanup limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_batch_size,
            cleanup_lease_ttl,
        })
    }

    /// Maximum candidate rows considered in one pass.
    pub const fn maximum_batch_size(self) -> u32 {
        self.maximum_batch_size
    }

    /// Reclaimable cleanup claim lifetime.
    pub const fn cleanup_lease_ttl(self) -> Duration {
        self.cleanup_lease_ttl
    }
}

impl Default for AiAttachmentCleanupLimits {
    fn default() -> Self {
        Self {
            maximum_batch_size: 50,
            cleanup_lease_ttl: Duration::minutes(5),
        }
    }
}

/// ORM-backed attachment service using a provider-neutral blob store.
///
/// Raw object keys never leave this service. Every phase verifies the current
/// authenticated owner and ordinary session/scope policy. External bytes move
/// from a random scope-bound quarantine key to a random final key only after
/// an exact full-object scan and a separate host acceptance decision.
pub struct OrmAiAttachmentService {
    database: Database<DefaultWriteBackend>,
    access_policy: Arc<dyn AiAccessPolicy>,
    protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
    content_protector: Arc<dyn AiContentProtector>,
    blob_store: Arc<dyn BlobStore>,
    scanner: Arc<dyn AiAttachmentScanner>,
    acceptance_policy: Arc<dyn AiAttachmentAcceptancePolicy>,
    provider_file_deletion: Option<Arc<dyn AiProviderFileDeletionService>>,
    clock: Arc<dyn Clock>,
    limits: AiAttachmentServiceLimits,
    cleanup_limits: AiAttachmentCleanupLimits,
}

struct RejectedUploadFacts {
    detected_mime: Option<String>,
    byte_count: Option<i64>,
    sha256: Option<String>,
    scanner_version: Option<String>,
    policy_version: Option<String>,
    reason_code: String,
}

impl OrmAiAttachmentService {
    /// Creates an attachment service with secure default hard limits.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database: Database<DefaultWriteBackend>,
        access_policy: Arc<dyn AiAccessPolicy>,
        protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
        content_protector: Arc<dyn AiContentProtector>,
        blob_store: Arc<dyn BlobStore>,
        scanner: Arc<dyn AiAttachmentScanner>,
        acceptance_policy: Arc<dyn AiAttachmentAcceptancePolicy>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            database,
            access_policy,
            protection_policy,
            content_protector,
            blob_store,
            scanner,
            acceptance_policy,
            provider_file_deletion: None,
            clock,
            limits: AiAttachmentServiceLimits::default(),
            cleanup_limits: AiAttachmentCleanupLimits::default(),
        }
    }

    /// Overrides deployment hard limits.
    #[must_use]
    pub fn with_limits(mut self, limits: AiAttachmentServiceLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Overrides bounded cleanup worker limits.
    #[must_use]
    pub fn with_cleanup_limits(mut self, limits: AiAttachmentCleanupLimits) -> Self {
        self.cleanup_limits = limits;
        self
    }

    /// Installs the trusted exact-reference provider-file deletion boundary.
    ///
    /// Without this boundary, artifacts carrying provider references remain
    /// fail-closed under bounded retry. Installing it grants authority only for
    /// references selected by a fenced deleting-session artifact claim.
    #[must_use]
    pub fn with_provider_file_deletion_service(
        mut self,
        service: Arc<dyn AiProviderFileDeletionService>,
    ) -> Self {
        self.provider_file_deletion = Some(service);
        self
    }

    /// Returns the ORM database handle for host composition.
    pub const fn database(&self) -> &Database<DefaultWriteBackend> {
        &self.database
    }

    async fn visible_session(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
        action: AiSessionAction,
    ) -> Result<AiSessionRecord, AiError> {
        if !self
            .access_policy
            .can_access_session(principal, session_id, action)
            .await
            .is_allowed()
        {
            return Err(AiError::Forbidden);
        }
        let session = AiSessionRecord::find_by_id(&self.database, &session_id.0)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
            .ok_or(AiError::NotFound)?;
        if !is_session_owner(principal, &session) || session.deleted_at.is_some() {
            return Err(AiError::NotFound);
        }
        let scope = session_scope(&session);
        if !self
            .access_policy
            .can_access_scope(principal, &scope, action)
            .await
            .is_allowed()
        {
            return Err(AiError::Forbidden);
        }
        Ok(session)
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

    async fn protect_event(
        &self,
        principal: &AuthPrincipal,
        scope: &AiScope,
        event_id: Uuid,
        value: serde_json::Value,
    ) -> Result<serde_json::Value, AiError> {
        let policy = self.protection_policy(principal, scope).await?;
        let context = ContentProtectionContext {
            entity: "graphql_orm_ai_session_events".to_owned(),
            row_id: event_id.to_string(),
            field: "protected_payload".to_owned(),
            scope: scope.clone(),
        };
        let envelope = self
            .content_protector
            .protect(&policy, &context, value)
            .await
            .map_err(map_protection)?;
        let current = self.protection_policy(principal, scope).await?;
        if current != policy {
            return Err(AiError::ReauthorizationFailed);
        }
        serde_json::to_value(envelope).map_err(|_| AiError::PersistenceFailed)
    }

    async fn claim_upload(
        &self,
        principal: &AuthPrincipal,
        attachment_id: Uuid,
        token: &SecretString,
    ) -> Result<(AiAttachmentRecord, AiScope), AiError> {
        let attachment = AiAttachmentRecord::find_by_id(&self.database, &attachment_id)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
            .ok_or(AiError::NotFound)?;
        let session = self
            .visible_session(
                principal,
                AiSessionId(attachment.session_id),
                AiSessionAction::Write,
            )
            .await?;
        let scope = session_scope(&session);
        let (kind, subject) = principal_identity(principal);
        let subject = subject.to_owned();
        let token_hash = token_hash(token.expose_secret());
        let now = self.clock.now().unix_timestamp();
        let processing_expires_at = now
            .checked_add(self.limits.upload_processing_ttl.whole_seconds())
            .ok_or_else(|| {
                AiError::InvalidConfiguration("attachment processing expiry overflow".to_owned())
            })?;
        let updated = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = tx
                        .find_by_id::<AiAttachmentRecord>(&attachment_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    let current_hash = current
                        .upload_token_hash
                        .as_deref()
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::Conflict))?;
                    if current.owner_principal_kind != kind
                        || current.owner_subject != subject
                        || current.session_id != session.id
                        || current.quarantine_state != "pending_upload"
                        || current.deleted_at.is_some()
                        || current
                            .upload_expires_at
                            .is_none_or(|expires_at| expires_at <= now)
                        || !constant_time_hash_eq(current_hash, &token_hash)
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let outcome = tx
                        .compare_and_swap::<AiAttachmentRecord>(
                            &attachment_id,
                            current.row_version,
                            AiAttachmentRecordWhereInput::default(),
                            UpdateAiAttachmentRecordInput {
                                upload_token_hash: Some(None),
                                quarantine_state: Some("uploading".to_owned()),
                                processing_state: Some("scanning".to_owned()),
                                processing_expires_at: Some(Some(processing_expires_at)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    match outcome {
                        ConditionalUpdateOutcome::Updated(record) => Ok(record),
                        ConditionalUpdateOutcome::NotFound => Err(OrmPublicError::not_found()),
                        ConditionalUpdateOutcome::Conflict => {
                            Err(OrmPublicError::new(OrmErrorCode::Conflict))
                        }
                    }
                })
            })
            .await
            .map_err(map_transaction)?;
        Ok((updated, scope))
    }

    async fn finish_external_phase(
        &self,
        claimed: &AiAttachmentRecord,
        update: UpdateAiAttachmentRecordInput,
    ) -> Result<AiAttachmentRecord, AiError> {
        let id = claimed.id;
        let expected_version = claimed.row_version;
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let outcome = tx
                        .compare_and_swap::<AiAttachmentRecord>(
                            &id,
                            expected_version,
                            AiAttachmentRecordWhereInput::default(),
                            update,
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    match outcome {
                        ConditionalUpdateOutcome::Updated(record) => Ok(record),
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

    async fn reject_upload(
        &self,
        claimed: &AiAttachmentRecord,
        facts: RejectedUploadFacts,
    ) -> Result<(), AiError> {
        let blob_reference = claimed
            .quarantine_blob_reference
            .as_deref()
            .ok_or(AiError::PersistenceFailed)?;
        let deleted = self.blob_store.delete_blob(blob_reference).await.is_ok();
        self.finish_external_phase(
            claimed,
            UpdateAiAttachmentRecordInput {
                quarantine_blob_reference: deleted.then_some(None),
                detected_mime: Some(facts.detected_mime),
                byte_count: Some(facts.byte_count),
                sha256: Some(facts.sha256),
                quarantine_state: Some("rejected".to_owned()),
                scan_state: Some("rejected".to_owned()),
                processing_state: Some(
                    if deleted {
                        "complete"
                    } else {
                        "cleanup_required"
                    }
                    .to_owned(),
                ),
                processing_expires_at: Some(None),
                scanner_version: Some(facts.scanner_version),
                acceptance_policy_version: Some(facts.policy_version),
                rejection_code: Some(Some(safe_reason(&facts.reason_code))),
                ..Default::default()
            },
        )
        .await?;
        Ok(())
    }

    async fn fail_upload(
        &self,
        claimed: &AiAttachmentRecord,
        cleanup_blob: bool,
        reason_code: &str,
    ) -> Result<(), AiError> {
        let quarantine_deleted = if cleanup_blob {
            match claimed.quarantine_blob_reference.as_deref() {
                Some(reference) => self.blob_store.delete_blob(reference).await.is_ok(),
                None => true,
            }
        } else {
            false
        };
        self.finish_external_phase(
            claimed,
            UpdateAiAttachmentRecordInput {
                quarantine_blob_reference: quarantine_deleted.then_some(None),
                quarantine_state: Some("failed".to_owned()),
                scan_state: Some("failed".to_owned()),
                processing_state: Some(
                    if quarantine_deleted {
                        "complete"
                    } else {
                        "cleanup_required"
                    }
                    .to_owned(),
                ),
                processing_expires_at: Some(None),
                rejection_code: Some(Some(safe_reason(reason_code))),
                ..Default::default()
            },
        )
        .await?;
        Ok(())
    }

    async fn artifact_cleanup_candidates(
        &self,
        now: i64,
    ) -> Result<Vec<AiAttachmentArtifactRecord>, AiError> {
        let limit = i64::from(self.cleanup_limits.maximum_batch_size);
        let queries = [
            AiAttachmentArtifactRecordWhereInput {
                cleanup_state: Some(StringFilter {
                    eq: Some("cleanup_required".to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            AiAttachmentArtifactRecordWhereInput {
                cleanup_state: Some(StringFilter {
                    eq: Some("cleanup_backoff".to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            AiAttachmentArtifactRecordWhereInput {
                cleanup_state: Some(StringFilter {
                    eq: Some("cleanup_in_progress".to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ];
        let mut candidates = BTreeMap::new();
        for query in queries {
            if candidates.len() >= self.cleanup_limits.maximum_batch_size as usize {
                break;
            }
            let connection = AiAttachmentArtifactRecord::keyset_connection_page(
                &self.database,
                query,
                KeysetConnectionInput {
                    first: Some(limit),
                    ..Default::default()
                },
            )
            .await
            .map_err(map_orm)?;
            for edge in connection.edges {
                if is_artifact_cleanup_eligible(&edge.node, now) {
                    candidates.entry(edge.node.id).or_insert(edge.node);
                }
            }
        }
        let mut candidates = candidates.into_values().collect::<Vec<_>>();
        candidates.sort_by_key(|candidate| (candidate.created_at, candidate.id));
        candidates.truncate(self.cleanup_limits.maximum_batch_size as usize);
        Ok(candidates)
    }

    async fn claim_artifact_cleanup(
        &self,
        candidate: &AiAttachmentArtifactRecord,
        now: i64,
    ) -> Result<Option<AiAttachmentArtifactRecord>, AiError> {
        let id = candidate.id;
        let cleanup_expires_at = now
            .checked_add(self.cleanup_limits.cleanup_lease_ttl.whole_seconds())
            .ok_or_else(|| {
                AiError::InvalidConfiguration("artifact cleanup expiry overflow".to_owned())
            })?;
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let Some(current) = tx
                        .find_by_id::<AiAttachmentArtifactRecord>(&id)
                        .await
                        .map_err(OrmPublicError::from)?
                    else {
                        return Ok(None);
                    };
                    validate_retention_artifact(&current, current.attachment_id)?;
                    if !is_artifact_cleanup_eligible(&current, now) {
                        return Ok(None);
                    }
                    let Some(attachment) = tx
                        .find_by_id::<AiAttachmentRecord>(&current.attachment_id)
                        .await
                        .map_err(OrmPublicError::from)?
                    else {
                        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                    };
                    validate_retention_attachment(&attachment, attachment.session_id)?;
                    let Some(session) = tx
                        .find_by_id::<AiSessionRecord>(&attachment.session_id)
                        .await
                        .map_err(OrmPublicError::from)?
                    else {
                        return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                    };
                    validate_retention_session(&session)?;
                    if session.state != "deleting" {
                        return Ok(None);
                    }
                    let scope = retention_session_scope(&session);
                    let scope_key = crate::ai_scope_key(&scope);
                    let policies = tx
                        .query::<AiRetentionPolicyRecord>()
                        .filter(AiRetentionPolicyRecordWhereInput {
                            scope_key: Some(StringFilter {
                                eq: Some(scope_key.clone()),
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
                        return Ok(None);
                    };
                    if !valid_session_retention_policy(&policy, &scope, &scope_key) {
                        return Ok(None);
                    }
                    if current.provider_reference.is_some() && !policy.provider_file_delete_required
                    {
                        return Ok(None);
                    }
                    let deleted_at = session
                        .deleted_at
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let cutoff = deleted_at
                        .checked_add(policy.deleted_content_purge_seconds)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    if cutoff > now {
                        return Ok(None);
                    }
                    let generation = current
                        .cleanup_generation
                        .unwrap_or(0)
                        .checked_add(1)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let outcome = tx
                        .compare_and_swap::<AiAttachmentArtifactRecord>(
                            &id,
                            current.row_version,
                            AiAttachmentArtifactRecordWhereInput {
                                attachment_id: Some(UuidFilter {
                                    eq: Some(attachment.id),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            },
                            UpdateAiAttachmentArtifactRecordInput {
                                cleanup_state: Some(Some("cleanup_in_progress".to_owned())),
                                cleanup_generation: Some(Some(generation)),
                                cleanup_lease_expires_at: Some(Some(cleanup_expires_at)),
                                cleanup_next_attempt_at: Some(None),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    match outcome {
                        ConditionalUpdateOutcome::Updated(record) => Ok(Some(record)),
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            Ok(None)
                        }
                    }
                })
            })
            .await
            .map_err(map_transaction)
    }

    async fn finish_artifact_cleanup(
        &self,
        claimed: &AiAttachmentArtifactRecord,
        external_objects_absent: bool,
        now: i64,
    ) -> Result<bool, AiError> {
        let id = claimed.id;
        let expected_version = claimed.row_version;
        let generation = claimed
            .cleanup_generation
            .ok_or(AiError::PersistenceFailed)?;
        let retry = if external_objects_absent {
            None
        } else {
            Some(cleanup_retry_facts(claimed.cleanup_retry_count, now)?)
        };
        let correlation_id = format!("attachment-artifact-cleanup:{id}:{generation}");
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let (update, outcome, reason_code) = if external_objects_absent {
                        (
                            UpdateAiAttachmentArtifactRecordInput {
                                blob_reference: Some(None),
                                protected_content: Some(None),
                                provider_reference: Some(None),
                                provider_expires_at: Some(None),
                                cleanup_state: Some(Some("complete".to_owned())),
                                cleanup_lease_expires_at: Some(None),
                                cleanup_next_attempt_at: Some(None),
                                deleted_at: Some(Some(now)),
                                ..Default::default()
                            },
                            "succeeded",
                            "artifact_objects_deleted",
                        )
                    } else {
                        let (next_retry_count, next_attempt_at) =
                            retry.expect("failed artifact deletion has retry facts");
                        (
                            UpdateAiAttachmentArtifactRecordInput {
                                cleanup_state: Some(Some("cleanup_backoff".to_owned())),
                                cleanup_lease_expires_at: Some(None),
                                cleanup_retry_count: Some(Some(next_retry_count)),
                                cleanup_next_attempt_at: Some(Some(next_attempt_at)),
                                ..Default::default()
                            },
                            "failed",
                            "artifact_cleanup_unconfirmed",
                        )
                    };
                    let result = tx
                        .compare_and_swap::<AiAttachmentArtifactRecord>(
                            &id,
                            expected_version,
                            AiAttachmentArtifactRecordWhereInput::default(),
                            update,
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(result, ConditionalUpdateOutcome::Updated(_)) {
                        return Ok(false);
                    }
                    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                        actor_principal_kind: "system".to_owned(),
                        actor_subject: "attachment-cleanup".to_owned(),
                        action: "cleanup_attachment_artifact_objects".to_owned(),
                        resource_kind: "ai_attachment_artifact".to_owned(),
                        resource_reference: id.to_string(),
                        outcome: outcome.to_owned(),
                        reason_code: reason_code.to_owned(),
                        correlation_id,
                        causation_id: Some(id.to_string()),
                        policy_version: Some("attachment-artifact-cleanup-v1".to_owned()),
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    Ok(true)
                })
            })
            .await
            .map_err(map_transaction)
    }

    async fn cleanup_artifact_external_objects(
        &self,
        claimed: &AiAttachmentArtifactRecord,
    ) -> bool {
        let local_absent = match claimed.blob_reference.as_deref() {
            Some(reference) => self.delete_blob_if_present(reference).await,
            None => true,
        };
        if !local_absent {
            return false;
        }
        match claimed.provider_reference.as_deref() {
            Some(reference) => {
                let Some(service) = &self.provider_file_deletion else {
                    return false;
                };
                let request = AiProviderFileDeletionRequest::new(
                    claimed.id,
                    claimed.attachment_id,
                    claimed.artifact_kind.clone(),
                    reference.to_owned(),
                );
                service.delete_and_confirm_absent(&request).await.is_ok()
            }
            None => true,
        }
    }

    async fn cleanup_candidates(
        &self,
        now: i64,
        maximum_batch_size: u32,
    ) -> Result<Vec<AiAttachmentRecord>, AiError> {
        let limit = i64::from(maximum_batch_size);
        let queries = [
            AiAttachmentRecordWhereInput {
                quarantine_state: Some(StringFilter {
                    eq: Some("pending_upload".to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            AiAttachmentRecordWhereInput {
                quarantine_state: Some(StringFilter {
                    eq: Some("uploading".to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            AiAttachmentRecordWhereInput {
                quarantine_state: Some(StringFilter {
                    eq: Some("deleting".to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            AiAttachmentRecordWhereInput {
                processing_state: Some(StringFilter {
                    in_list: Some(vec![
                        "cleanup_required".to_owned(),
                        "cleanup_backoff".to_owned(),
                        "cleanup_in_progress".to_owned(),
                    ]),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ];
        let mut candidates = BTreeMap::new();
        for query in queries {
            if candidates.len() >= maximum_batch_size as usize {
                break;
            }
            let connection = AiAttachmentRecord::keyset_connection_page(
                &self.database,
                query,
                KeysetConnectionInput {
                    last: Some(limit),
                    ..Default::default()
                },
            )
            .await
            .map_err(map_orm)?;
            for edge in connection.edges {
                if is_cleanup_eligible(&edge.node, now) {
                    candidates.entry(edge.node.id).or_insert(edge.node);
                }
            }
        }
        let mut candidates = candidates.into_values().collect::<Vec<_>>();
        candidates.sort_by_key(|candidate| (candidate.created_at, candidate.id));
        candidates.truncate(maximum_batch_size as usize);
        Ok(candidates)
    }

    async fn claim_cleanup(
        &self,
        candidate: &AiAttachmentRecord,
        now: i64,
    ) -> Result<Option<AiAttachmentRecord>, AiError> {
        let id = candidate.id;
        let cleanup_expires_at = now
            .checked_add(self.cleanup_limits.cleanup_lease_ttl.whole_seconds())
            .ok_or_else(|| {
                AiError::InvalidConfiguration("attachment cleanup expiry overflow".to_owned())
            })?;
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let Some(current) = tx
                        .find_by_id::<AiAttachmentRecord>(&id)
                        .await
                        .map_err(OrmPublicError::from)?
                    else {
                        return Ok(None);
                    };
                    if !is_cleanup_eligible(&current, now) {
                        return Ok(None);
                    }
                    if attachment_retention_cleanup_pending(&current) {
                        let Some(session) = tx
                            .find_by_id::<AiSessionRecord>(&current.session_id)
                            .await
                            .map_err(OrmPublicError::from)?
                        else {
                            return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                        };
                        validate_retention_session(&session)?;
                        if session.state != "deleting" {
                            return Ok(None);
                        }
                        let scope = retention_session_scope(&session);
                        let scope_key = crate::ai_scope_key(&scope);
                        let policies = tx
                            .query::<AiRetentionPolicyRecord>()
                            .filter(AiRetentionPolicyRecordWhereInput {
                                scope_key: Some(StringFilter {
                                    eq: Some(scope_key.clone()),
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
                            return Ok(None);
                        };
                        if !valid_session_retention_policy(&policy, &scope, &scope_key) {
                            return Ok(None);
                        }
                        let deleted_at = session
                            .deleted_at
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                        let cutoff = deleted_at
                            .checked_add(policy.deleted_content_purge_seconds)
                            .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                        if cutoff > now {
                            return Ok(None);
                        }
                    }
                    let generation = current
                        .cleanup_generation
                        .unwrap_or(0)
                        .checked_add(1)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let outcome = tx
                        .compare_and_swap::<AiAttachmentRecord>(
                            &id,
                            current.row_version,
                            AiAttachmentRecordWhereInput::default(),
                            UpdateAiAttachmentRecordInput {
                                upload_token_hash: Some(None),
                                processing_state: Some("cleanup_in_progress".to_owned()),
                                processing_expires_at: Some(None),
                                cleanup_generation: Some(Some(generation)),
                                cleanup_lease_expires_at: Some(Some(cleanup_expires_at)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    match outcome {
                        ConditionalUpdateOutcome::Updated(record) => Ok(Some(record)),
                        ConditionalUpdateOutcome::NotFound | ConditionalUpdateOutcome::Conflict => {
                            Ok(None)
                        }
                    }
                })
            })
            .await
            .map_err(map_transaction)
    }

    async fn delete_blob_if_present(&self, reference: &str) -> bool {
        match self.blob_store.blob_exists(reference).await {
            Ok(false) => true,
            Ok(true) => match self.blob_store.delete_blob(reference).await {
                Ok(()) => self
                    .blob_store
                    .blob_exists(reference)
                    .await
                    .is_ok_and(|exists| !exists),
                Err(_) => self
                    .blob_store
                    .blob_exists(reference)
                    .await
                    .is_ok_and(|exists| !exists),
            },
            Err(_) => false,
        }
    }

    async fn finish_cleanup(
        &self,
        claimed: &AiAttachmentRecord,
        storage_deleted: bool,
        now: i64,
    ) -> Result<bool, AiError> {
        let id = claimed.id;
        let expected_version = claimed.row_version;
        let generation = claimed
            .cleanup_generation
            .ok_or(AiError::PersistenceFailed)?;
        let retry = if storage_deleted {
            None
        } else {
            let next_retry_count = claimed
                .cleanup_retry_count
                .unwrap_or(0)
                .checked_add(1)
                .ok_or(AiError::PersistenceFailed)?;
            let retry_shift = u32::try_from(next_retry_count.min(6)).unwrap_or(6);
            let retry_delay_seconds = 60_i64.checked_shl(retry_shift).unwrap_or(3_600).min(3_600);
            let next_attempt_at = now
                .checked_add(retry_delay_seconds)
                .ok_or(AiError::PersistenceFailed)?;
            Some((next_retry_count, next_attempt_at))
        };
        let correlation_id = format!("attachment-cleanup:{id}:{generation}");
        let original_state = claimed.quarantine_state.clone();
        let existing_reason = claimed.rejection_code.clone();
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let (update, outcome, reason_code) = if storage_deleted {
                        let mut update = UpdateAiAttachmentRecordInput {
                            blob_reference: Some(None),
                            quarantine_blob_reference: Some(None),
                            upload_token_hash: Some(None),
                            processing_state: Some("complete".to_owned()),
                            processing_expires_at: Some(None),
                            cleanup_lease_expires_at: Some(None),
                            cleanup_next_attempt_at: Some(None),
                            ..Default::default()
                        };
                        let reason_code = match original_state.as_str() {
                            "pending_upload" => {
                                update.quarantine_state = Some("expired".to_owned());
                                update.scan_state = Some("failed".to_owned());
                                update.rejection_code =
                                    Some(Some("upload_ticket_expired".to_owned()));
                                update.deleted_at = Some(Some(now));
                                "upload_ticket_expired"
                            }
                            "uploading" => {
                                update.quarantine_state = Some("failed".to_owned());
                                update.scan_state = Some("failed".to_owned());
                                update.rejection_code =
                                    Some(Some("upload_processing_expired".to_owned()));
                                "upload_processing_expired"
                            }
                            "deleting" => {
                                update.quarantine_state = Some("deleted".to_owned());
                                update.deleted_at = Some(Some(now));
                                "attachment_deleted"
                            }
                            _ => "orphan_objects_deleted",
                        };
                        (update, "succeeded", reason_code.to_owned())
                    } else {
                        let (next_retry_count, next_attempt_at) =
                            retry.expect("failed deletion has retry facts");
                        (
                            UpdateAiAttachmentRecordInput {
                                processing_state: Some("cleanup_backoff".to_owned()),
                                cleanup_lease_expires_at: Some(None),
                                cleanup_retry_count: Some(Some(next_retry_count)),
                                cleanup_next_attempt_at: Some(Some(next_attempt_at)),
                                rejection_code: Some(Some(
                                    existing_reason
                                        .unwrap_or_else(|| "storage_cleanup_failed".to_owned()),
                                )),
                                ..Default::default()
                            },
                            "failed",
                            "storage_cleanup_unconfirmed".to_owned(),
                        )
                    };
                    let result = tx
                        .compare_and_swap::<AiAttachmentRecord>(
                            &id,
                            expected_version,
                            AiAttachmentRecordWhereInput::default(),
                            update,
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(result, ConditionalUpdateOutcome::Updated(_)) {
                        return Ok(false);
                    }
                    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                        actor_principal_kind: "system".to_owned(),
                        actor_subject: "attachment-cleanup".to_owned(),
                        action: "cleanup_attachment_objects".to_owned(),
                        resource_kind: "ai_attachment".to_owned(),
                        resource_reference: id.to_string(),
                        outcome: outcome.to_owned(),
                        reason_code,
                        correlation_id,
                        causation_id: Some(id.to_string()),
                        policy_version: Some("attachment-cleanup-v1".to_owned()),
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    Ok(true)
                })
            })
            .await
            .map_err(map_transaction)
    }
}

#[async_trait]
impl AiAttachmentService for OrmAiAttachmentService {
    async fn attachments(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
        page: ValidatedKeysetConnection,
    ) -> Result<AiAttachmentConnection, AiError> {
        self.visible_session(principal, session_id, AiSessionAction::Read)
            .await?;
        let (kind, subject) = principal_identity(principal);
        let connection = AiAttachmentRecord::keyset_connection_page(
            &self.database,
            AiAttachmentRecordWhereInput {
                owner_principal_kind: Some(StringFilter {
                    eq: Some(kind),
                    ..Default::default()
                }),
                owner_subject: Some(StringFilter {
                    eq: Some(subject.to_owned()),
                    ..Default::default()
                }),
                session_id: Some(UuidFilter {
                    eq: Some(session_id.0),
                    ..Default::default()
                }),
                deleted_at: Some(IntFilter {
                    is_null: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            },
            page_input(&page),
        )
        .await
        .map_err(map_orm)?;
        Ok(AiAttachmentConnection {
            edges: connection
                .edges
                .into_iter()
                .map(|edge| AiAttachmentEdge {
                    node: attachment_view(&edge.node),
                    cursor: edge.cursor,
                })
                .collect(),
            page_info: connection.page_info,
        })
    }

    async fn create_upload(
        &self,
        principal: &AuthPrincipal,
        input: CreateAiAttachmentUploadInput,
    ) -> Result<AiAttachmentUploadTicket, AiError> {
        let expected_byte_count = u64::try_from(input.expected_byte_count).map_err(|_| {
            AiError::InvalidInput("invalid attachment expected byte count".to_owned())
        })?;
        if expected_byte_count == 0 || expected_byte_count > self.limits.maximum_attachment_bytes {
            return Err(AiError::InvalidInput(
                "attachment exceeds configured byte limit".to_owned(),
            ));
        }
        if input
            .declared_mime
            .as_deref()
            .is_some_and(|value| !valid_mime(value))
        {
            return Err(AiError::InvalidInput(
                "invalid declared attachment MIME".to_owned(),
            ));
        }
        let safe_filename = sanitize_filename(&input.filename, self.limits.maximum_filename_bytes)?;
        let session = self
            .visible_session(
                principal,
                AiSessionId(input.session_id),
                AiSessionAction::Write,
            )
            .await?;
        if session.state != "active" {
            return Err(AiError::Conflict);
        }
        let scope = session_scope(&session);
        let attachment_id = Uuid::new_v4();
        let token_value = format!(
            "att.{}.{}.{}",
            attachment_id,
            Uuid::new_v4(),
            Uuid::new_v4()
        );
        let token = SecretString::from(token_value.clone());
        let token_hash = token_hash(&token_value);
        let scope_hash = scope_hash(&scope);
        let blob_reference = format!(
            "ai-attachments/quarantine/{scope_hash}/{attachment_id}/{}",
            Uuid::new_v4()
        );
        let now = self.clock.now().unix_timestamp();
        let expires_at = now
            .checked_add(self.limits.upload_ticket_ttl.whole_seconds())
            .ok_or(AiError::InvalidConfiguration(
                "attachment ticket expiry overflow".to_owned(),
            ))?;
        let event_id = Uuid::new_v4();
        let protected_event = self
            .protect_event(
                principal,
                &scope,
                event_id,
                json!({
                    "formatVersion": 1,
                    "attachmentId": attachment_id,
                    "safeFilename": safe_filename,
                    "declaredMime": input.declared_mime,
                    "expectedByteCount": input.expected_byte_count,
                    "uploadExpiresAt": expires_at,
                }),
            )
            .await?;
        let (owner_kind, owner_subject) = principal_identity(principal);
        let owner_subject = owner_subject.to_owned();
        let session_id = session.id;
        let declared_mime = input.declared_mime;
        let safe_filename_for_insert = safe_filename.clone();
        let attachment = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = tx
                        .find_by_id::<AiSessionRecord>(&session_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if current.owner_principal_kind != owner_kind
                        || current.owner_subject != owner_subject
                        || current.state != "active"
                        || current.deleted_at.is_some()
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let event_sequence = current
                        .stream_head
                        .checked_add(1)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let session_update = tx
                        .compare_and_swap::<AiSessionRecord>(
                            &session_id,
                            current.row_version,
                            AiSessionRecordWhereInput::default(),
                            UpdateAiSessionRecordInput {
                                stream_head: Some(event_sequence),
                                last_activity_at: Some(now),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(session_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let record = tx
                        .insert::<AiAttachmentRecord>(CreateAiAttachmentRecordInput {
                            id: attachment_id,
                            owner_principal_kind: owner_kind,
                            owner_subject,
                            session_id,
                            message_id: None,
                            blob_reference: None,
                            quarantine_blob_reference: Some(blob_reference),
                            safe_filename: safe_filename_for_insert,
                            declared_mime,
                            detected_mime: None,
                            expected_byte_count: Some(input.expected_byte_count),
                            byte_count: None,
                            sha256: None,
                            upload_token_hash: Some(token_hash),
                            upload_expires_at: Some(expires_at),
                            quarantine_state: "pending_upload".to_owned(),
                            scan_state: "pending".to_owned(),
                            processing_state: "pending".to_owned(),
                            processing_expires_at: None,
                            cleanup_generation: None,
                            cleanup_lease_expires_at: None,
                            cleanup_retry_count: None,
                            cleanup_next_attempt_at: None,
                            scanner_version: None,
                            acceptance_policy_version: None,
                            rejection_code: None,
                            finalized_at: None,
                            deleted_at: None,
                        })
                        .await
                        .map_err(OrmPublicError::from)?;
                    tx.insert::<AiSessionEventRecord>(CreateAiSessionEventRecordInput {
                        id: event_id,
                        session_id,
                        sequence: event_sequence,
                        event_type: "attachment_upload_created".to_owned(),
                        run_id: None,
                        causation_id: Some(attachment_id.to_string()),
                        correlation_id: attachment_id.to_string(),
                        protected_payload: protected_event,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.queue_event(AiSessionWakeup {
                        session_id,
                        sequence: event_sequence,
                    });
                    Ok(record)
                })
            })
            .await
            .map_err(map_transaction)?;
        Ok(AiAttachmentUploadTicket::new(
            attachment_view(&attachment),
            token,
            expires_at,
        ))
    }

    async fn finalize_upload(
        &self,
        principal: &AuthPrincipal,
        attachment_id: Uuid,
    ) -> Result<AiAttachmentView, AiError> {
        let attachment = AiAttachmentRecord::find_by_id(&self.database, &attachment_id)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
            .ok_or(AiError::NotFound)?;
        let session = self
            .visible_session(
                principal,
                AiSessionId(attachment.session_id),
                AiSessionAction::Write,
            )
            .await?;
        if session.state != "active" || !is_attachment_owner(principal, &attachment) {
            return Err(AiError::NotFound);
        }
        if attachment.quarantine_state != "ready"
            || attachment.scan_state != "clean"
            || attachment.blob_reference.is_none()
            || attachment.quarantine_blob_reference.is_some()
            || attachment.detected_mime.is_none()
            || attachment.byte_count.is_none()
            || attachment.sha256.is_none()
            || attachment.deleted_at.is_some()
        {
            return Err(AiError::Conflict);
        }
        let scope = session_scope(&session);
        let event_id = Uuid::new_v4();
        let protected_event = self
            .protect_event(
                principal,
                &scope,
                event_id,
                json!({
                    "formatVersion": 1,
                    "attachmentId": attachment.id,
                    "safeFilename": attachment.safe_filename,
                    "detectedMime": attachment.detected_mime,
                    "byteCount": attachment.byte_count,
                    "sha256": attachment.sha256,
                }),
            )
            .await?;
        let (owner_kind, owner_subject) = principal_identity(principal);
        let owner_subject = owner_subject.to_owned();
        let session_id = session.id;
        let expected_attachment_version = attachment.row_version;
        let now = self.clock.now().unix_timestamp();
        let updated = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current_session = tx
                        .find_by_id::<AiSessionRecord>(&session_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if current_session.owner_principal_kind != owner_kind
                        || current_session.owner_subject != owner_subject
                        || current_session.state != "active"
                        || current_session.deleted_at.is_some()
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let event_sequence = current_session
                        .stream_head
                        .checked_add(1)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let attachment_update = tx
                        .compare_and_swap::<AiAttachmentRecord>(
                            &attachment_id,
                            expected_attachment_version,
                            AiAttachmentRecordWhereInput::default(),
                            UpdateAiAttachmentRecordInput {
                                quarantine_state: Some("released".to_owned()),
                                processing_state: Some("complete".to_owned()),
                                processing_expires_at: Some(None),
                                finalized_at: Some(Some(now)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    let ConditionalUpdateOutcome::Updated(updated) = attachment_update else {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    };
                    let session_update = tx
                        .compare_and_swap::<AiSessionRecord>(
                            &session_id,
                            current_session.row_version,
                            AiSessionRecordWhereInput::default(),
                            UpdateAiSessionRecordInput {
                                stream_head: Some(event_sequence),
                                last_activity_at: Some(now),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(session_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    tx.insert::<AiSessionEventRecord>(CreateAiSessionEventRecordInput {
                        id: event_id,
                        session_id,
                        sequence: event_sequence,
                        event_type: "attachment_released".to_owned(),
                        run_id: None,
                        causation_id: Some(attachment_id.to_string()),
                        correlation_id: attachment_id.to_string(),
                        protected_payload: protected_event,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.queue_event(AiSessionWakeup {
                        session_id,
                        sequence: event_sequence,
                    });
                    Ok(updated)
                })
            })
            .await
            .map_err(map_transaction)?;
        Ok(attachment_view(&updated))
    }

    async fn remove_attachment(
        &self,
        principal: &AuthPrincipal,
        attachment_id: Uuid,
    ) -> Result<bool, AiError> {
        let attachment = AiAttachmentRecord::find_by_id(&self.database, &attachment_id)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
            .ok_or(AiError::NotFound)?;
        let session = self
            .visible_session(
                principal,
                AiSessionId(attachment.session_id),
                AiSessionAction::Delete,
            )
            .await?;
        if !is_attachment_owner(principal, &attachment) {
            return Err(AiError::NotFound);
        }
        if attachment.message_id.is_some()
            || attachment.quarantine_state == "uploading"
            || attachment.deleted_at.is_some()
        {
            return Err(AiError::Conflict);
        }
        let scope = session_scope(&session);
        let event_id = Uuid::new_v4();
        let protected_event = self
            .protect_event(
                principal,
                &scope,
                event_id,
                json!({"formatVersion": 1, "attachmentId": attachment_id}),
            )
            .await?;
        let expected_version = attachment.row_version;
        let now = self.clock.now().unix_timestamp();
        let deletion_expires_at = now
            .checked_add(self.limits.upload_processing_ttl.whole_seconds())
            .ok_or_else(|| {
                AiError::InvalidConfiguration("attachment deletion expiry overflow".to_owned())
            })?;
        let deleting = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let outcome = tx
                        .compare_and_swap::<AiAttachmentRecord>(
                            &attachment_id,
                            expected_version,
                            AiAttachmentRecordWhereInput::default(),
                            UpdateAiAttachmentRecordInput {
                                upload_token_hash: Some(None),
                                quarantine_state: Some("deleting".to_owned()),
                                processing_state: Some("deleting".to_owned()),
                                processing_expires_at: Some(Some(deletion_expires_at)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    match outcome {
                        ConditionalUpdateOutcome::Updated(record) => Ok(record),
                        ConditionalUpdateOutcome::NotFound => Err(OrmPublicError::not_found()),
                        ConditionalUpdateOutcome::Conflict => {
                            Err(OrmPublicError::new(OrmErrorCode::Conflict))
                        }
                    }
                })
            })
            .await
            .map_err(map_transaction)?;
        for reference in [
            deleting.blob_reference.as_deref(),
            deleting.quarantine_blob_reference.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if self
                .blob_store
                .blob_exists(reference)
                .await
                .map_err(|_| AiError::PersistenceFailed)?
            {
                self.blob_store
                    .delete_blob(reference)
                    .await
                    .map_err(|_| AiError::PersistenceFailed)?;
            }
        }
        let (owner_kind, owner_subject) = principal_identity(principal);
        let owner_subject = owner_subject.to_owned();
        let session_id = session.id;
        let deleting_version = deleting.row_version;
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current_session = tx
                        .find_by_id::<AiSessionRecord>(&session_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if current_session.owner_principal_kind != owner_kind
                        || current_session.owner_subject != owner_subject
                        || current_session.deleted_at.is_some()
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let event_sequence = current_session
                        .stream_head
                        .checked_add(1)
                        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::InternalError))?;
                    let attachment_update = tx
                        .compare_and_swap::<AiAttachmentRecord>(
                            &attachment_id,
                            deleting_version,
                            AiAttachmentRecordWhereInput::default(),
                            UpdateAiAttachmentRecordInput {
                                blob_reference: Some(None),
                                quarantine_blob_reference: Some(None),
                                quarantine_state: Some("deleted".to_owned()),
                                processing_state: Some("complete".to_owned()),
                                processing_expires_at: Some(None),
                                deleted_at: Some(Some(now)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(attachment_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let session_update = tx
                        .compare_and_swap::<AiSessionRecord>(
                            &session_id,
                            current_session.row_version,
                            AiSessionRecordWhereInput::default(),
                            UpdateAiSessionRecordInput {
                                stream_head: Some(event_sequence),
                                last_activity_at: Some(now),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    if !matches!(session_update, ConditionalUpdateOutcome::Updated(_)) {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    tx.insert::<AiSessionEventRecord>(CreateAiSessionEventRecordInput {
                        id: event_id,
                        session_id,
                        sequence: event_sequence,
                        event_type: "attachment_removed".to_owned(),
                        run_id: None,
                        causation_id: Some(attachment_id.to_string()),
                        correlation_id: attachment_id.to_string(),
                        protected_payload: protected_event,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    tx.queue_event(AiSessionWakeup {
                        session_id,
                        sequence: event_sequence,
                    });
                    Ok(())
                })
            })
            .await
            .map_err(map_transaction)?;
        Ok(true)
    }
}

#[async_trait]
impl AiAttachmentCleanupService for OrmAiAttachmentService {
    async fn cleanup_once(&self) -> Result<AiAttachmentCleanupReport, AiError> {
        let now = self.clock.now().unix_timestamp();
        let artifact_candidates = self.artifact_cleanup_candidates(now).await?;
        let mut report = AiAttachmentCleanupReport {
            artifacts_examined: u32::try_from(artifact_candidates.len()).unwrap_or(u32::MAX),
            ..Default::default()
        };
        for candidate in artifact_candidates {
            let Some(claimed) = self.claim_artifact_cleanup(&candidate, now).await? else {
                report.artifacts_deferred = report.artifacts_deferred.saturating_add(1);
                continue;
            };
            let external_objects_absent = self.cleanup_artifact_external_objects(&claimed).await;
            if !self
                .finish_artifact_cleanup(&claimed, external_objects_absent, now)
                .await?
            {
                report.artifacts_deferred = report.artifacts_deferred.saturating_add(1);
            } else if external_objects_absent {
                report.artifacts_cleaned = report.artifacts_cleaned.saturating_add(1);
            } else {
                report.artifacts_failed = report.artifacts_failed.saturating_add(1);
            }
        }
        let remaining = self
            .cleanup_limits
            .maximum_batch_size
            .saturating_sub(report.artifacts_examined);
        if remaining == 0 {
            return Ok(report);
        }
        let candidates = self.cleanup_candidates(now, remaining).await?;
        report.examined = u32::try_from(candidates.len()).unwrap_or(u32::MAX);
        for candidate in candidates {
            let Some(claimed) = self.claim_cleanup(&candidate, now).await? else {
                report.deferred = report.deferred.saturating_add(1);
                continue;
            };
            let mut storage_deleted = true;
            for reference in [
                claimed.blob_reference.as_deref(),
                claimed.quarantine_blob_reference.as_deref(),
            ]
            .into_iter()
            .flatten()
            {
                storage_deleted &= self.delete_blob_if_present(reference).await;
            }
            if !self.finish_cleanup(&claimed, storage_deleted, now).await? {
                report.deferred = report.deferred.saturating_add(1);
            } else if storage_deleted {
                report.cleaned = report.cleaned.saturating_add(1);
            } else {
                report.failed = report.failed.saturating_add(1);
            }
        }
        Ok(report)
    }
}

#[async_trait]
impl AiProviderAttachmentResolver for OrmAiAttachmentService {
    async fn resolve_for_provider(
        &self,
        principal: &ResolvedPrincipal,
        session_id: AiSessionId,
        scope: &AiScope,
        request: &AiProviderAttachmentRequest,
    ) -> Result<AiResolvedProviderAttachment, AiError> {
        let session = self
            .visible_session(principal.principal(), session_id, AiSessionAction::Read)
            .await?;
        if session_scope(&session) != *scope {
            return Err(AiError::Forbidden);
        }
        let attachment = AiAttachmentRecord::find_by_id(&self.database, &request.attachment_id())
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
            .ok_or(AiError::NotFound)?;
        if !is_attachment_owner(principal.principal(), &attachment)
            || attachment.session_id != session_id.0
            || attachment.message_id.is_none()
            || attachment.quarantine_state != "released"
            || attachment.scan_state != "clean"
            || attachment.processing_state != "complete"
            || attachment.deleted_at.is_some()
            || attachment.detected_mime.as_deref() != Some(request.mime())
            || attachment
                .byte_count
                .and_then(|value| u64::try_from(value).ok())
                != Some(request.byte_count())
            || attachment.sha256.as_deref() != Some(request.sha256())
            || attachment.quarantine_blob_reference.is_some()
        {
            return Err(AiError::ReauthorizationFailed);
        }
        let blob_reference = attachment
            .blob_reference
            .as_deref()
            .ok_or(AiError::ReauthorizationFailed)?;
        let blob = self
            .blob_store
            .get_blob(blob_reference)
            .await
            .map_err(|_| AiError::PersistenceFailed)?;
        if blob.key != blob_reference
            || blob.metadata.as_ref().is_some_and(|metadata| {
                metadata.key != blob_reference
                    || metadata
                        .size_bytes
                        .is_some_and(|size| size != request.byte_count())
                    || metadata
                        .sha256_hex
                        .as_deref()
                        .is_some_and(|sha256| sha256 != request.sha256())
            })
        {
            return Err(AiError::ReauthorizationFailed);
        }
        let bytes = collect_exact_provider_attachment(blob.body, request.byte_count()).await?;
        let current = AiAttachmentRecord::find_by_id(&self.database, &request.attachment_id())
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
            .ok_or(AiError::ReauthorizationFailed)?;
        if current.row_version != attachment.row_version
            || current.owner_principal_kind != attachment.owner_principal_kind
            || current.owner_subject != attachment.owner_subject
            || current.session_id != attachment.session_id
            || current.blob_reference != attachment.blob_reference
            || current.message_id != attachment.message_id
            || current.safe_filename != attachment.safe_filename
            || current.detected_mime != attachment.detected_mime
            || current.byte_count != attachment.byte_count
            || current.sha256 != attachment.sha256
            || current.quarantine_state != "released"
            || current.scan_state != "clean"
            || current.processing_state != "complete"
            || current.quarantine_blob_reference.is_some()
            || current.deleted_at.is_some()
        {
            return Err(AiError::ReauthorizationFailed);
        }
        AiResolvedProviderAttachment::new(request.clone(), attachment.safe_filename, bytes)
    }
}

#[async_trait]
impl AiAttachmentUploadService for OrmAiAttachmentService {
    async fn upload(
        &self,
        principal: &AuthPrincipal,
        attachment_id: Uuid,
        token: SecretString,
        body: StorageByteStream,
    ) -> Result<AiAttachmentView, AiError> {
        if body
            .size_hint()
            .is_some_and(|size| size == 0 || size > self.limits.maximum_attachment_bytes)
        {
            return Err(AiError::InvalidInput(
                "attachment exceeds configured byte limit".to_owned(),
            ));
        }
        let (claimed, scope) = self.claim_upload(principal, attachment_id, &token).await?;
        let expected_bytes = u64::try_from(
            claimed
                .expected_byte_count
                .ok_or(AiError::PersistenceFailed)?,
        )
        .map_err(|_| AiError::PersistenceFailed)?;
        if body.size_hint().is_some_and(|size| size != expected_bytes) {
            self.reject_upload(
                &claimed,
                RejectedUploadFacts {
                    detected_mime: None,
                    byte_count: None,
                    sha256: None,
                    scanner_version: None,
                    policy_version: None,
                    reason_code: "declared_size_mismatch".to_owned(),
                },
            )
            .await?;
            return Err(AiError::InvalidInput(
                "attachment byte count does not match ticket".to_owned(),
            ));
        }
        let quarantine_key = claimed
            .quarantine_blob_reference
            .as_deref()
            .ok_or(AiError::PersistenceFailed)?;
        let outcome = match self
            .blob_store
            .put_blob_if_not_exists(
                quarantine_key,
                body,
                BlobPutOptions {
                    content_type: claimed.declared_mime.clone(),
                },
            )
            .await
        {
            Ok(Some(outcome)) => outcome,
            Ok(None) => {
                self.fail_upload(&claimed, false, "quarantine_key_conflict")
                    .await?;
                return Err(AiError::Conflict);
            }
            Err(_) => {
                self.fail_upload(&claimed, true, "quarantine_write_failed")
                    .await?;
                return Err(AiError::PersistenceFailed);
            }
        };
        let byte_count =
            i64::try_from(outcome.size_bytes).map_err(|_| AiError::PersistenceFailed)?;
        if outcome.size_bytes != expected_bytes
            || outcome.size_bytes > self.limits.maximum_attachment_bytes
            || !valid_sha256(&outcome.sha256_hex)
        {
            self.reject_upload(
                &claimed,
                RejectedUploadFacts {
                    detected_mime: None,
                    byte_count: Some(byte_count),
                    sha256: Some(outcome.sha256_hex),
                    scanner_version: None,
                    policy_version: None,
                    reason_code: "stored_size_or_hash_mismatch".to_owned(),
                },
            )
            .await?;
            return Err(AiError::InvalidInput(
                "attachment storage result does not match ticket".to_owned(),
            ));
        }
        let blob = match self.blob_store.get_blob(quarantine_key).await {
            Ok(blob) => blob,
            Err(_) => {
                self.fail_upload(&claimed, true, "quarantine_read_failed")
                    .await?;
                return Err(AiError::PersistenceFailed);
            }
        };
        let scan_request = AiAttachmentScanRequest {
            attachment_id,
            safe_filename: claimed.safe_filename.clone(),
            declared_mime: claimed.declared_mime.clone(),
            byte_count: outcome.size_bytes,
            sha256: outcome.sha256_hex.clone(),
        };
        let report = match self.scanner.scan(&scan_request, blob.body).await {
            Ok(report) => report,
            Err(_) => {
                self.fail_upload(&claimed, true, "scan_unavailable").await?;
                return Err(AiError::PersistenceFailed);
            }
        };
        if report.observed_byte_count() != outcome.size_bytes
            || report.observed_sha256() != outcome.sha256_hex
        {
            self.reject_upload(
                &claimed,
                RejectedUploadFacts {
                    detected_mime: Some(report.detected_mime().to_owned()),
                    byte_count: Some(byte_count),
                    sha256: Some(outcome.sha256_hex),
                    scanner_version: Some(report.scanner_version().to_owned()),
                    policy_version: None,
                    reason_code: "scan_attestation_mismatch".to_owned(),
                },
            )
            .await?;
            return Err(AiError::PersistenceFailed);
        }
        if let AiAttachmentScanVerdict::Reject { reason_code } = report.verdict() {
            self.reject_upload(
                &claimed,
                RejectedUploadFacts {
                    detected_mime: Some(report.detected_mime().to_owned()),
                    byte_count: Some(byte_count),
                    sha256: Some(outcome.sha256_hex),
                    scanner_version: Some(report.scanner_version().to_owned()),
                    policy_version: None,
                    reason_code: reason_code.to_owned(),
                },
            )
            .await?;
            return Err(AiError::Forbidden);
        }
        let candidate = AiAttachmentCandidate {
            attachment_id,
            safe_filename: claimed.safe_filename.clone(),
            detected_mime: report.detected_mime().to_owned(),
            byte_count: outcome.size_bytes,
            sha256: outcome.sha256_hex.clone(),
            scanner_version: report.scanner_version().to_owned(),
        };
        let decision = self
            .acceptance_policy
            .authorize(principal, &scope, &candidate)
            .await;
        if !valid_safe_reference(&decision.policy_version, 128)
            || !valid_safe_reference(&decision.reason_code, 128)
        {
            self.fail_upload(&claimed, true, "acceptance_policy_invalid")
                .await?;
            return Err(AiError::PersistenceFailed);
        }
        if !decision.is_allowed() {
            self.reject_upload(
                &claimed,
                RejectedUploadFacts {
                    detected_mime: Some(candidate.detected_mime),
                    byte_count: Some(byte_count),
                    sha256: Some(outcome.sha256_hex),
                    scanner_version: Some(candidate.scanner_version),
                    policy_version: Some(decision.policy_version),
                    reason_code: decision.reason_code,
                },
            )
            .await?;
            return Err(AiError::Forbidden);
        }
        let final_key = format!(
            "ai-attachments/objects/{}/{attachment_id}/{}",
            scope_hash(&scope),
            Uuid::new_v4()
        );
        if self
            .blob_store
            .copy_blob(quarantine_key, &final_key)
            .await
            .is_err()
        {
            self.fail_upload(&claimed, true, "quarantine_promotion_failed")
                .await?;
            return Err(AiError::PersistenceFailed);
        }
        if self.blob_store.delete_blob(quarantine_key).await.is_err() {
            let final_deleted = self.blob_store.delete_blob(&final_key).await.is_ok();
            self.finish_external_phase(
                &claimed,
                UpdateAiAttachmentRecordInput {
                    blob_reference: (!final_deleted).then_some(Some(final_key)),
                    quarantine_blob_reference: Some(Some(quarantine_key.to_owned())),
                    quarantine_state: Some("failed".to_owned()),
                    scan_state: Some("failed".to_owned()),
                    processing_state: Some("cleanup_required".to_owned()),
                    processing_expires_at: Some(None),
                    rejection_code: Some(Some("quarantine_cleanup_failed".to_owned())),
                    ..Default::default()
                },
            )
            .await?;
            return Err(AiError::PersistenceFailed);
        }
        let updated = match self
            .finish_external_phase(
                &claimed,
                UpdateAiAttachmentRecordInput {
                    blob_reference: Some(Some(final_key.clone())),
                    quarantine_blob_reference: Some(None),
                    detected_mime: Some(Some(report.detected_mime().to_owned())),
                    byte_count: Some(Some(byte_count)),
                    sha256: Some(Some(outcome.sha256_hex)),
                    quarantine_state: Some("ready".to_owned()),
                    scan_state: Some("clean".to_owned()),
                    processing_state: Some("ready".to_owned()),
                    processing_expires_at: Some(None),
                    scanner_version: Some(Some(report.scanner_version().to_owned())),
                    acceptance_policy_version: Some(Some(decision.policy_version)),
                    rejection_code: Some(None),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(updated) => updated,
            Err(error) => {
                let _ = self.blob_store.delete_blob(&final_key).await;
                return Err(error);
            }
        };
        Ok(attachment_view(&updated))
    }
}

fn principal_identity(principal: &AuthPrincipal) -> (String, &str) {
    let kind = match principal {
        AuthPrincipal::User(_) => "user".to_owned(),
        AuthPrincipal::ApiToken(token) => format!("api_token:{}", token.principal_kind.as_str()),
    };
    (kind, principal.subject())
}

fn is_session_owner(principal: &AuthPrincipal, session: &AiSessionRecord) -> bool {
    let (kind, subject) = principal_identity(principal);
    session.owner_principal_kind == kind && session.owner_subject == subject
}

fn is_attachment_owner(principal: &AuthPrincipal, attachment: &AiAttachmentRecord) -> bool {
    let (kind, subject) = principal_identity(principal);
    attachment.owner_principal_kind == kind && attachment.owner_subject == subject
}

fn session_scope(session: &AiSessionRecord) -> AiScope {
    AiScope {
        kind: session.scope_kind.clone(),
        id: session.scope_id.clone(),
        tenant_id: session.tenant_id.clone(),
    }
}

fn attachment_view(record: &AiAttachmentRecord) -> AiAttachmentView {
    AiAttachmentView {
        id: record.id,
        session_id: record.session_id,
        message_id: record.message_id,
        safe_filename: record.safe_filename.clone(),
        declared_mime: record.declared_mime.clone(),
        detected_mime: record.detected_mime.clone(),
        expected_byte_count: record.expected_byte_count,
        byte_count: record.byte_count,
        quarantine_state: record.quarantine_state.clone(),
        scan_state: record.scan_state.clone(),
        rejection_code: record.rejection_code.clone(),
        created_at: record.created_at,
        finalized_at: record.finalized_at,
    }
}

fn is_cleanup_eligible(record: &AiAttachmentRecord, now: i64) -> bool {
    let retention_cleanup = attachment_retention_cleanup_pending(record);
    if (record.message_id.is_some() || record.deleted_at.is_some()) && !retention_cleanup {
        return false;
    }
    match record.processing_state.as_str() {
        "cleanup_required" | "retention_cleanup_required" => true,
        "cleanup_backoff" => record
            .cleanup_next_attempt_at
            .is_some_and(|next_attempt_at| next_attempt_at <= now),
        "cleanup_in_progress" => record
            .cleanup_lease_expires_at
            .is_some_and(|expires_at| expires_at <= now),
        _ => match record.quarantine_state.as_str() {
            "pending_upload" => record
                .upload_expires_at
                .is_some_and(|expires_at| expires_at <= now),
            "uploading" => record
                .processing_expires_at
                .or(record.upload_expires_at)
                .is_some_and(|expires_at| expires_at <= now),
            "deleting" => record
                .processing_expires_at
                .is_none_or(|expires_at| expires_at <= now),
            _ => false,
        },
    }
}

fn is_artifact_cleanup_eligible(record: &AiAttachmentArtifactRecord, now: i64) -> bool {
    match record.cleanup_state.as_deref() {
        Some("cleanup_required") => true,
        Some("cleanup_backoff") => record
            .cleanup_next_attempt_at
            .is_some_and(|next_attempt_at| next_attempt_at <= now),
        Some("cleanup_in_progress") => record
            .cleanup_lease_expires_at
            .is_some_and(|expires_at| expires_at <= now),
        _ => false,
    }
}

fn cleanup_retry_facts(retry_count: Option<i64>, now: i64) -> Result<(i64, i64), AiError> {
    let next_retry_count = retry_count
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(AiError::PersistenceFailed)?;
    let retry_shift = u32::try_from(next_retry_count.min(6)).unwrap_or(6);
    let retry_delay_seconds = 60_i64.checked_shl(retry_shift).unwrap_or(3_600).min(3_600);
    let next_attempt_at = now
        .checked_add(retry_delay_seconds)
        .ok_or(AiError::PersistenceFailed)?;
    Ok((next_retry_count, next_attempt_at))
}

async fn collect_exact_provider_attachment(
    body: StorageByteStream,
    expected_bytes: u64,
) -> Result<Arc<[u8]>, AiError> {
    if body
        .size_hint()
        .is_some_and(|size_hint| size_hint != expected_bytes)
    {
        return Err(AiError::ReauthorizationFailed);
    }
    let capacity = usize::try_from(expected_bytes).map_err(|_| AiError::PersistenceFailed)?;
    let mut collected = Vec::with_capacity(capacity);
    let mut stream = body.into_inner();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| AiError::PersistenceFailed)?;
        if collected.len().saturating_add(chunk.len()) > capacity {
            return Err(AiError::ReauthorizationFailed);
        }
        collected.extend_from_slice(&chunk);
    }
    if collected.len() != capacity {
        return Err(AiError::ReauthorizationFailed);
    }
    Ok(Arc::from(collected))
}

fn sanitize_filename(value: &str, maximum_bytes: usize) -> Result<String, AiError> {
    let basename = value.rsplit(['/', '\\']).next().unwrap_or_default().trim();
    if basename.is_empty() || basename == "." || basename == ".." {
        return Err(AiError::InvalidInput(
            "invalid attachment filename".to_owned(),
        ));
    }
    let mut safe = String::new();
    for character in basename.chars() {
        let character = if character.is_control() || matches!(character, '/' | '\\' | ':') {
            '_'
        } else {
            character
        };
        if safe.len().saturating_add(character.len_utf8()) > maximum_bytes {
            break;
        }
        safe.push(character);
    }
    let safe = safe.trim_matches([' ', '.']).to_owned();
    if safe.is_empty() {
        return Err(AiError::InvalidInput(
            "invalid attachment filename".to_owned(),
        ));
    }
    Ok(safe)
}

fn token_hash(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

fn constant_time_hash_eq(left: &str, right: &str) -> bool {
    left.as_bytes().ct_eq(right.as_bytes()).into()
}

fn scope_hash(scope: &AiScope) -> String {
    let mut hash = Sha256::new();
    hash.update(b"graphql-orm-ai/attachment-scope/v1\0");
    hash.update(scope.kind.as_bytes());
    hash.update(b"\0");
    hash.update(scope.id.as_bytes());
    hash.update(b"\0");
    if let Some(tenant_id) = &scope.tenant_id {
        hash.update(tenant_id.as_bytes());
    }
    hex::encode(hash.finalize())
}

fn safe_reason(value: &str) -> String {
    if valid_safe_reference(value, 128) {
        value.to_owned()
    } else {
        "attachment_rejected".to_owned()
    }
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
