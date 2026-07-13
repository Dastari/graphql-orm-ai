//! Owner-isolated attachment intake, scanning, and GraphQL contracts.

use std::fmt;
use std::sync::Arc;

use agql_auth::AuthPrincipal;
use async_graphql::{Context, ErrorExtensions, InputObject, Object, SimpleObject};
use async_trait::async_trait;
use graphql_orm::graphql::pagination::{
    KeysetConnectionInput, PageInfo, ValidatedKeysetConnection,
};
use graphql_orm_storage::StorageByteStream;
use secrecy::{ExposeSecret, SecretString};
use uuid::Uuid;

use crate::{AiAccessDecision, AiError, AiScope, AiSessionId};

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
