//! Owner-isolated attachment intake, scanning, and GraphQL contracts.

use std::fmt;
use std::sync::Arc;

use agql_auth::{AuthPrincipal, ResolvedPrincipal};
use async_graphql::{Context, ErrorExtensions, InputObject, Object, SimpleObject};
use async_trait::async_trait;
use graphql_orm::graphql::pagination::{
    KeysetConnectionInput, PageInfo, ValidatedKeysetConnection,
};
use graphql_orm_storage::StorageByteStream;
use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{AiAccessDecision, AiError, AiScope, AiSessionId, ModelInputBlock};

/// Bounded client-visible attachment metadata.
///
/// Storage keys, upload-token hashes, scanner details, and provider file
/// references are deliberately absent.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiAttachmentView {
    /// AI-owned opaque attachment identifier.
    pub id: Uuid,
    /// Owning session.
    pub session_id: Uuid,
    /// Message linked after ordinary message creation, when present.
    pub message_id: Option<Uuid>,
    /// Sanitized display filename; never a storage path.
    pub safe_filename: String,
    /// Caller-declared MIME metadata, when supplied.
    pub declared_mime: Option<String>,
    /// Scanner-detected MIME, when scanning completed.
    pub detected_mime: Option<String>,
    /// Expected upload bytes bound into the ticket.
    pub expected_byte_count: Option<i64>,
    /// Verified stored bytes, when upload completed.
    pub byte_count: Option<i64>,
    /// Pending/uploading/ready/released/rejected/deleting/deleted state.
    pub quarantine_state: String,
    /// Pending/clean/rejected/failed state.
    pub scan_state: String,
    /// Stable redacted rejection code, when rejected.
    pub rejection_code: Option<String>,
    /// Creation time in Unix seconds.
    pub created_at: i64,
    /// Release time in Unix seconds.
    pub finalized_at: Option<i64>,
}

/// One attachment connection edge.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiAttachmentEdge {
    /// Attachment metadata.
    pub node: AiAttachmentView,
    /// Opaque keyset cursor.
    pub cursor: String,
}

/// Bounded attachment connection.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiAttachmentConnection {
    /// Bounded edges.
    pub edges: Vec<AiAttachmentEdge>,
    /// Relay page metadata.
    pub page_info: PageInfo,
}

/// Creates one exact pending upload.
#[derive(Clone, Debug, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct CreateAiAttachmentUploadInput {
    /// Active owning session.
    pub session_id: Uuid,
    /// User-supplied display filename. It is sanitized and never used as a key.
    pub filename: String,
    /// Optional declared MIME metadata. Content scanning remains authoritative.
    pub declared_mime: Option<String>,
    /// Exact expected byte count, bounded by deployment policy.
    pub expected_byte_count: i64,
}

/// One-time upload capability returned only by creation.
///
/// The token is intentionally available to the GraphQL response and upload
/// handler, but is redacted from `Debug`, is not serializable, and is never
/// persisted in plaintext. Possession alone is insufficient: the streaming
/// upload boundary also requires the current authenticated owner.
#[derive(Clone)]
pub struct AiAttachmentUploadTicket {
    attachment: AiAttachmentView,
    token: SecretString,
    expires_at: i64,
}

impl AiAttachmentUploadTicket {
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) fn new(attachment: AiAttachmentView, token: SecretString, expires_at: i64) -> Self {
        Self {
            attachment,
            token,
            expires_at,
        }
    }

    /// Pending attachment metadata.
    pub const fn attachment(&self) -> &AiAttachmentView {
        &self.attachment
    }

    /// One-time upload secret for a trusted streaming handler.
    ///
    /// Callers must not log, persist, reuse, or place this value in a URL.
    pub const fn secret(&self) -> &SecretString {
        &self.token
    }

    /// Ticket expiry in Unix seconds.
    pub const fn expires_at(&self) -> i64 {
        self.expires_at
    }
}

impl fmt::Debug for AiAttachmentUploadTicket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiAttachmentUploadTicket")
            .field("attachment", &self.attachment)
            .field("token", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

#[cfg_attr(
    feature = "graphql-case-pascal",
    Object(rename_fields = "PascalCase", rename_args = "PascalCase")
)]
#[cfg_attr(not(feature = "graphql-case-pascal"), Object)]
impl AiAttachmentUploadTicket {
    /// Pending attachment metadata.
    async fn attachment_metadata(&self) -> &AiAttachmentView {
        &self.attachment
    }

    /// One-time upload secret. Clients must send it in a protected request
    /// header to the host-owned streaming endpoint and then discard it.
    async fn upload_token(&self) -> String {
        self.token.expose_secret().to_owned()
    }

    /// Ticket expiry in Unix seconds.
    async fn upload_expires_at(&self) -> i64 {
        self.expires_at
    }
}

/// Exact immutable scan request for one quarantined object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiAttachmentScanRequest {
    /// Attachment identifier.
    pub attachment_id: Uuid,
    /// Sanitized display filename.
    pub safe_filename: String,
    /// Caller-declared MIME metadata.
    pub declared_mime: Option<String>,
    /// Exact stored byte count.
    pub byte_count: u64,
    /// Exact lowercase SHA-256 of stored bytes.
    pub sha256: String,
}

/// Scanner verdict over the entire exact quarantined object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AiAttachmentScanVerdict {
    /// No scanner threat or content-format rejection was found.
    Clean,
    /// The object must not be promoted.
    Reject {
        /// Stable redacted scanner reason code.
        reason_code: String,
    },
}

/// Scanner attestation for one exact object.
///
/// The service compares observed bytes and hash with the storage write before
/// accepting this report. The type proves no application acceptance policy;
/// that separate boundary runs after a clean scan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiAttachmentScanReport {
    detected_mime: String,
    observed_byte_count: u64,
    observed_sha256: String,
    scanner_version: String,
    verdict: AiAttachmentScanVerdict,
}

impl AiAttachmentScanReport {
    /// Creates a complete scanner attestation.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidInput`] for malformed MIME, hash, version, or
    /// rejection code values.
    pub fn new(
        detected_mime: impl Into<String>,
        observed_byte_count: u64,
        observed_sha256: impl Into<String>,
        scanner_version: impl Into<String>,
        verdict: AiAttachmentScanVerdict,
    ) -> Result<Self, AiError> {
        let detected_mime = detected_mime.into();
        let observed_sha256 = observed_sha256.into();
        let scanner_version = scanner_version.into();
        let rejected_invalid = matches!(
            &verdict,
            AiAttachmentScanVerdict::Reject { reason_code }
                if !valid_safe_reference(reason_code, 128)
        );
        if !valid_mime(&detected_mime)
            || !valid_sha256(&observed_sha256)
            || !valid_safe_reference(&scanner_version, 128)
            || rejected_invalid
        {
            return Err(AiError::InvalidInput(
                "invalid attachment scan report".to_owned(),
            ));
        }
        Ok(Self {
            detected_mime,
            observed_byte_count,
            observed_sha256,
            scanner_version,
            verdict,
        })
    }

    /// Scanner-detected MIME.
    pub fn detected_mime(&self) -> &str {
        &self.detected_mime
    }

    /// Bytes consumed and attested by the scanner.
    pub const fn observed_byte_count(&self) -> u64 {
        self.observed_byte_count
    }

    /// Lowercase SHA-256 consumed and attested by the scanner.
    pub fn observed_sha256(&self) -> &str {
        &self.observed_sha256
    }

    /// Stable scanner engine/signature version.
    pub fn scanner_version(&self) -> &str {
        &self.scanner_version
    }

    /// Clean or rejected verdict.
    pub const fn verdict(&self) -> &AiAttachmentScanVerdict {
        &self.verdict
    }
}

/// Clean scanner metadata presented to the separate host acceptance policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiAttachmentCandidate {
    /// Attachment identifier.
    pub attachment_id: Uuid,
    /// Sanitized display filename.
    pub safe_filename: String,
    /// Scanner-detected MIME.
    pub detected_mime: String,
    /// Exact bytes.
    pub byte_count: u64,
    /// Exact lowercase SHA-256.
    pub sha256: String,
    /// Stable scanner version.
    pub scanner_version: String,
}

/// Trusted full-object malware/content scanner.
///
/// Implementations must consume the complete stream and report the exact byte
/// count and SHA-256. They must fail closed on truncation, parser ambiguity,
/// timeout, unavailable signatures, or unsupported nested formats.
#[async_trait]
pub trait AiAttachmentScanner: Send + Sync {
    /// Scans one exact quarantined object stream.
    ///
    /// # Errors
    ///
    /// Returns a safe error when a complete authoritative scan cannot be
    /// produced. The service treats every error as a failed upload and never
    /// promotes the object.
    async fn scan(
        &self,
        request: &AiAttachmentScanRequest,
        body: StorageByteStream,
    ) -> Result<AiAttachmentScanReport, AiError>;
}

/// Host content-type/size policy applied after a clean malware scan.
///
/// Deployment hard limits still apply before upload. This policy may narrow
/// acceptance by scope and principal, but cannot promote a scanner rejection.
#[async_trait]
pub trait AiAttachmentAcceptancePolicy: Send + Sync {
    /// Evaluates one exact clean candidate.
    async fn authorize(
        &self,
        principal: &AuthPrincipal,
        scope: &AiScope,
        candidate: &AiAttachmentCandidate,
    ) -> AiAccessDecision;
}

/// Fail-closed attachment acceptance policy.
#[derive(Clone, Copy, Debug, Default)]
pub struct DenyAllAiAttachmentAcceptancePolicy;

#[async_trait]
impl AiAttachmentAcceptancePolicy for DenyAllAiAttachmentAcceptancePolicy {
    async fn authorize(
        &self,
        _principal: &AuthPrincipal,
        _scope: &AiScope,
        _candidate: &AiAttachmentCandidate,
    ) -> AiAccessDecision {
        AiAccessDecision::deny("default_deny", "deny-all")
    }
}

/// Owner/scope-aware attachment GraphQL backend.
#[async_trait]
pub trait AiAttachmentService: Send + Sync {
    /// Lists a bounded session attachment window.
    ///
    /// # Errors
    ///
    /// Returns a safe authorization, validation, or persistence error.
    async fn attachments(
        &self,
        principal: &AuthPrincipal,
        session_id: AiSessionId,
        page: ValidatedKeysetConnection,
    ) -> Result<AiAttachmentConnection, AiError>;

    /// Creates one expiring, one-use upload capability.
    ///
    /// # Errors
    ///
    /// Returns a safe authorization, validation, protection, or persistence
    /// error. The raw token is returned only on successful creation.
    async fn create_upload(
        &self,
        principal: &AuthPrincipal,
        input: CreateAiAttachmentUploadInput,
    ) -> Result<AiAttachmentUploadTicket, AiError>;

    /// Releases one clean ready object for message linkage.
    ///
    /// # Errors
    ///
    /// Returns a safe authorization, state, protection, or persistence error.
    async fn finalize_upload(
        &self,
        principal: &AuthPrincipal,
        attachment_id: Uuid,
    ) -> Result<AiAttachmentView, AiError>;

    /// Removes one unlinked attachment and its stored object.
    ///
    /// # Errors
    ///
    /// Returns a safe authorization, state, storage, or persistence error.
    async fn remove_attachment(
        &self,
        principal: &AuthPrincipal,
        attachment_id: Uuid,
    ) -> Result<bool, AiError>;
}

/// Authenticated streaming upload boundary.
///
/// Large bytes do not pass through ordinary GraphQL JSON. Implementations must
/// require both the current authenticated owner and the exact one-time token.
#[async_trait]
pub trait AiAttachmentUploadService: Send + Sync {
    /// Stores, scans, and promotes one exact ticketed body into ready state.
    ///
    /// # Errors
    ///
    /// Returns a safe error for invalid/expired/used tokens, owner or scope
    /// denial, stream/size/hash mismatch, scan/policy rejection, storage
    /// failure, or persistence conflict. Failed work is never released.
    async fn upload(
        &self,
        principal: &AuthPrincipal,
        attachment_id: Uuid,
        token: SecretString,
        body: StorageByteStream,
    ) -> Result<AiAttachmentView, AiError>;
}

/// Bounded outcome from one host-scheduled attachment cleanup pass.
///
/// Counts contain no owner, filename, storage reference, or content data.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AiAttachmentCleanupReport {
    /// Parent attachment rows examined after bounded state queries.
    pub examined: u32,
    /// Parent rows whose referenced objects were proven absent and durably
    /// finalized.
    pub cleaned: u32,
    /// Parent rows another worker claimed or changed before this worker could
    /// fence.
    pub deferred: u32,
    /// Parent rows retained for retry because storage deletion could not be
    /// proven.
    pub failed: u32,
    /// Artifact rows examined before their parent attachment rows.
    pub artifacts_examined: u32,
    /// Artifact rows whose local and provider objects were proven absent and
    /// whose protected derivatives were durably tombstoned.
    pub artifacts_cleaned: u32,
    /// Artifact rows another worker claimed or changed before fencing.
    pub artifacts_deferred: u32,
    /// Artifact rows retained because exact local or provider absence could
    /// not be proven.
    pub artifacts_failed: u32,
}

/// Exact provider-persistent file selected by durable artifact retention.
///
/// This host-only request is not general provider authority. It is constructed
/// only after a deleting session, its current scope retention policy, the
/// deletion cutoff, the parent attachment, and a fenced artifact cleanup claim
/// have been re-proved. The provider reference is deliberately redacted from
/// `Debug`, but remains sensitive deployment metadata and must not be logged,
/// persisted elsewhere, or exposed to a model or client.
#[derive(Clone, PartialEq, Eq)]
pub struct AiProviderFileDeletionRequest {
    artifact_id: Uuid,
    attachment_id: Uuid,
    artifact_kind: String,
    provider_reference: String,
}

impl AiProviderFileDeletionRequest {
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) fn new(
        artifact_id: Uuid,
        attachment_id: Uuid,
        artifact_kind: String,
        provider_reference: String,
    ) -> Self {
        Self {
            artifact_id,
            attachment_id,
            artifact_kind,
            provider_reference,
        }
    }

    /// AI-owned artifact identifier.
    pub const fn artifact_id(&self) -> Uuid {
        self.artifact_id
    }

    /// Parent attachment identifier.
    pub const fn attachment_id(&self) -> Uuid {
        self.attachment_id
    }

    /// Validated artifact kind used to route the trusted provider adapter.
    pub fn artifact_kind(&self) -> &str {
        &self.artifact_kind
    }

    /// Exact opaque provider reference selected by the fenced cleanup claim.
    pub fn provider_reference(&self) -> &str {
        &self.provider_reference
    }
}

impl fmt::Debug for AiProviderFileDeletionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiProviderFileDeletionRequest")
            .field("artifact_id", &self.artifact_id)
            .field("attachment_id", &self.attachment_id)
            .field("artifact_kind", &self.artifact_kind)
            .field("provider_reference", &"[REDACTED]")
            .finish()
    }
}

/// Trusted exact-reference deletion boundary for provider-persistent files.
///
/// Implementations must treat `Ok(())` as a strong assertion that the exact
/// referenced provider object is absent after the call, including a provider
/// "not found" response. A successful delete request without an authoritative
/// absence guarantee is not sufficient. Implementations must never list or
/// delete by prefix, infer absence from expiry, or expose the reference beyond
/// the configured provider adapter.
#[async_trait]
pub trait AiProviderFileDeletionService: Send + Sync {
    /// Deletes the exact provider object and confirms its absence.
    ///
    /// # Errors
    ///
    /// Returns a safe error whenever deletion or authoritative absence is
    /// unavailable, ambiguous, rate-limited, or rejected. The artifact worker
    /// then retains every reference and protected derivative for bounded retry.
    async fn delete_and_confirm_absent(
        &self,
        request: &AiProviderFileDeletionRequest,
    ) -> Result<(), AiError>;
}

/// Host-only maintenance boundary for expired or interrupted attachments and
/// dependency-ordered deleting-session artifacts.
///
/// This service is intentionally not exposed through GraphQL. Scheduling it
/// grants authority only to delete objects already selected by durable AI
/// lifecycle state; it grants no ability to read attachment bytes or inspect
/// user content. Artifact claims run before parent claims. A linked attachment
/// or artifact selected by deleting-session retention is accepted only after
/// this worker re-proves the exact parent, current scope policy, and deletion
/// cutoff in its cleanup claim transaction. Provider objects remain closed
/// unless a trusted [`AiProviderFileDeletionService`] is installed.
#[async_trait]
pub trait AiAttachmentCleanupService: Send + Sync {
    /// Runs one bounded, lease-fenced cleanup pass.
    ///
    /// Parent storage ambiguity is reported in
    /// [`AiAttachmentCleanupReport::failed`]; artifact local/provider ambiguity
    /// is reported in [`AiAttachmentCleanupReport::artifacts_failed`]. Both are
    /// retained for a later retry. A database or query failure aborts the pass
    /// with a safe error.
    ///
    /// # Errors
    ///
    /// Returns a safe persistence error when candidates cannot be loaded,
    /// claimed, or durably finalized.
    async fn cleanup_once(&self) -> Result<AiAttachmentCleanupReport, AiError>;
}

/// Exact released attachment metadata requested by a provider turn.
///
/// This value contains no storage reference. It is not authorization proof;
/// a trusted [`AiProviderAttachmentResolver`] must reopen the current durable
/// row and object under fresh authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiProviderAttachmentRequest {
    attachment_id: Uuid,
    mime: String,
    byte_count: u64,
    sha256: String,
}

impl AiProviderAttachmentRequest {
    /// AI-owned attachment identifier.
    pub const fn attachment_id(&self) -> Uuid {
        self.attachment_id
    }

    /// Exact scanner-detected MIME requested by the provider plan.
    pub fn mime(&self) -> &str {
        &self.mime
    }

    /// Exact verified raw bytes requested by the provider plan.
    pub const fn byte_count(&self) -> u64 {
        self.byte_count
    }

    /// Exact lowercase SHA-256 requested by the provider plan.
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    /// Canonical redacted source reference used by the egress manifest.
    pub fn egress_reference(&self) -> String {
        format!(
            "v1:{}:{}:{}:{}",
            self.attachment_id, self.byte_count, self.mime, self.sha256
        )
    }
}

impl TryFrom<&ModelInputBlock> for AiProviderAttachmentRequest {
    type Error = AiError;

    fn try_from(block: &ModelInputBlock) -> Result<Self, Self::Error> {
        let ModelInputBlock::Attachment {
            attachment_id,
            mime,
            byte_count,
            sha256,
        } = block
        else {
            return Err(AiError::InvalidInput(
                "provider attachment request requires an attachment block".to_owned(),
            ));
        };
        let attachment_id = Uuid::parse_str(attachment_id).map_err(|_| {
            AiError::InvalidInput("invalid provider attachment identifier".to_owned())
        })?;
        if !valid_mime(mime)
            || *byte_count == 0
            || *byte_count > 100 * 1024 * 1024
            || !valid_sha256(sha256)
        {
            return Err(AiError::InvalidInput(
                "invalid provider attachment metadata".to_owned(),
            ));
        }
        Ok(Self {
            attachment_id,
            mime: mime.clone(),
            byte_count: *byte_count,
            sha256: sha256.clone(),
        })
    }
}

/// Exact attachment bytes reopened for one provider turn.
///
/// Construction validates content length and SHA-256 against the request, but
/// does not itself prove owner/session access, released state, current policy,
/// or egress authorization. Those guarantees come from the trusted resolver,
/// provider executor, and exact manifest proof together.
#[derive(Clone)]
pub struct AiResolvedProviderAttachment {
    request: AiProviderAttachmentRequest,
    safe_filename: String,
    bytes: Arc<[u8]>,
}

impl AiResolvedProviderAttachment {
    /// Constructs an exact resolved payload after a trusted resolver reopens
    /// and verifies the durable object.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidInput`] for an unsafe filename or when bytes
    /// do not exactly match the requested byte count and SHA-256.
    pub fn new(
        request: AiProviderAttachmentRequest,
        safe_filename: impl Into<String>,
        bytes: impl Into<Arc<[u8]>>,
    ) -> Result<Self, AiError> {
        let safe_filename = safe_filename.into();
        let bytes = bytes.into();
        let filename_is_safe = valid_safe_reference(&safe_filename, 255)
            && safe_filename != "."
            && safe_filename != ".."
            && !safe_filename.chars().any(char::is_control)
            && !safe_filename
                .bytes()
                .any(|byte| matches!(byte, b'/' | b'\\' | b':'));
        let observed_count = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let observed_sha256 = hex::encode(Sha256::digest(bytes.as_ref()));
        if !filename_is_safe
            || observed_count != request.byte_count
            || observed_sha256 != request.sha256
        {
            return Err(AiError::InvalidInput(
                "resolved attachment does not match requested content".to_owned(),
            ));
        }
        Ok(Self {
            request,
            safe_filename,
            bytes,
        })
    }

    /// Exact request metadata bound to these bytes.
    pub const fn request(&self) -> &AiProviderAttachmentRequest {
        &self.request
    }

    /// Sanitized display filename used only as provider file metadata.
    pub fn safe_filename(&self) -> &str {
        &self.safe_filename
    }

    /// Sensitive attachment bytes for an authorized provider adapter.
    ///
    /// Callers must not log, persist, cache, or expose this slice outside the
    /// exact provider transport authorized by its request context.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl fmt::Debug for AiResolvedProviderAttachment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiResolvedProviderAttachment")
            .field("request", &self.request)
            .field("safe_filename", &self.safe_filename)
            .field("bytes", &"[REDACTED]")
            .finish()
    }
}

/// Fresh-authority boundary that reopens released attachment bytes.
///
/// Implementations must recheck owner, session, scope, released/clean/linked
/// state, exact metadata, and the exact current object. They must not return a
/// storage key, signed URL, or provider-persistent file reference.
#[async_trait]
pub trait AiProviderAttachmentResolver: Send + Sync {
    /// Resolves one exact attachment for an imminent provider call.
    ///
    /// # Errors
    ///
    /// Returns a safe error for stale principal, authorization/state mismatch,
    /// missing content, size/hash mismatch, or storage ambiguity.
    async fn resolve_for_provider(
        &self,
        principal: &ResolvedPrincipal,
        session_id: AiSessionId,
        scope: &AiScope,
        request: &AiProviderAttachmentRequest,
    ) -> Result<AiResolvedProviderAttachment, AiError>;
}

/// Composable attachment query root.
#[derive(Clone, Copy, Debug, Default)]
pub struct AiAttachmentQueryRoot;

#[cfg_attr(
    feature = "graphql-case-pascal",
    Object(rename_fields = "PascalCase", rename_args = "PascalCase")
)]
#[cfg_attr(not(feature = "graphql-case-pascal"), Object)]
impl AiAttachmentQueryRoot {
    /// Lists bounded metadata for one visible owned session.
    async fn ai_attachments(
        &self,
        context: &Context<'_>,
        session_id: Uuid,
        #[graphql(default)] page: KeysetConnectionInput,
    ) -> async_graphql::Result<AiAttachmentConnection> {
        let principal = agql_auth::principal_from_ctx(context)?;
        let page = page.validate(50, 200).map_err(|error| (&error).extend())?;
        attachment_service(context)?
            .attachments(&principal, AiSessionId(session_id), page)
            .await
            .map_err(extend)
    }
}

/// Composable attachment mutation root.
#[derive(Clone, Copy, Debug, Default)]
pub struct AiAttachmentMutationRoot;

#[cfg_attr(
    feature = "graphql-case-pascal",
    Object(rename_fields = "PascalCase", rename_args = "PascalCase")
)]
#[cfg_attr(not(feature = "graphql-case-pascal"), Object)]
impl AiAttachmentMutationRoot {
    /// Creates an expiring one-time upload ticket.
    async fn create_ai_attachment_upload(
        &self,
        context: &Context<'_>,
        input: CreateAiAttachmentUploadInput,
    ) -> async_graphql::Result<AiAttachmentUploadTicket> {
        let principal = agql_auth::principal_from_ctx(context)?;
        attachment_service(context)?
            .create_upload(&principal, input)
            .await
            .map_err(extend)
    }

    /// Releases a clean ready upload for message linkage.
    async fn finalize_ai_attachment_upload(
        &self,
        context: &Context<'_>,
        attachment_id: Uuid,
    ) -> async_graphql::Result<AiAttachmentView> {
        let principal = agql_auth::principal_from_ctx(context)?;
        attachment_service(context)?
            .finalize_upload(&principal, attachment_id)
            .await
            .map_err(extend)
    }

    /// Deletes one unlinked upload and its object.
    async fn remove_ai_attachment(
        &self,
        context: &Context<'_>,
        attachment_id: Uuid,
    ) -> async_graphql::Result<bool> {
        let principal = agql_auth::principal_from_ctx(context)?;
        attachment_service(context)?
            .remove_attachment(&principal, attachment_id)
            .await
            .map_err(extend)
    }
}

fn attachment_service(
    context: &Context<'_>,
) -> async_graphql::Result<Arc<dyn AiAttachmentService>> {
    context
        .data::<Arc<dyn AiAttachmentService>>()
        .cloned()
        .map_err(|_| {
            AiError::InvalidConfiguration("AI attachment service is not installed".to_owned())
                .extend()
        })
}

fn extend(error: AiError) -> async_graphql::Error {
    error.extend()
}

pub(crate) fn valid_mime(value: &str) -> bool {
    let Some((kind, subtype)) = value.split_once('/') else {
        return false;
    };
    !kind.is_empty()
        && !subtype.is_empty()
        && value.len() <= 127
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'/' | b'+' | b'-' | b'.')
        })
}

pub(crate) fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn valid_safe_reference(value: &str, maximum: usize) -> bool {
    !value.trim().is_empty()
        && value.len() <= maximum
        && value.bytes().all(|byte| !byte.is_ascii_control())
}
