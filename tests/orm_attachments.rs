#![cfg(feature = "sqlite")]

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use agql_auth::{
    AccessTokenMetadata, AssuranceMatchMode, AuthPrincipal, AuthUser, Clock, FixedClock,
    MfaAcceptance, RecentMfaPolicy, ResolvedPrincipal, SessionAssurance, SessionContext,
    SystemClock,
};
use async_trait::async_trait;
use graphql_orm::graphql::orm::{ApplyOptions, OrmSchemaModule};
use graphql_orm::graphql::pagination::KeysetConnectionInput;
use graphql_orm::prelude::{Database, SqliteBackend};
use graphql_orm_ai::*;
use graphql_orm_storage::{
    BlobBody, BlobListPage, BlobMetadata, BlobPutOptions, BlobStore, BlobWriteOutcome,
    StorageBackend, StorageByteStream, StorageError, collect_storage_stream, sha256_hex,
};
use secrecy::{ExposeSecret, SecretString};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

struct AllowAll;

#[async_trait]
impl AiAccessPolicy for AllowAll {
    async fn can_access_scope(
        &self,
        _principal: &AuthPrincipal,
        _scope: &AiScope,
        _action: AiSessionAction,
    ) -> AiAccessDecision {
        AiAccessDecision::allow("test", "access-v1")
    }

    async fn can_access_session(
        &self,
        _principal: &AuthPrincipal,
        _session_id: AiSessionId,
        _action: AiSessionAction,
    ) -> AiAccessDecision {
        AiAccessDecision::allow("test", "access-v1")
    }
}

struct AllowConfiguration;

#[async_trait]
impl AiConfigurationAccessPolicy for AllowConfiguration {
    async fn can_configure(
        &self,
        _principal: &AuthPrincipal,
        _scope: &AiScope,
        _action: AiConfigurationAction,
    ) -> bool {
        true
    }
}

struct DenyEndpoints;

impl AiProviderEndpointPolicy for DenyEndpoints {
    fn authorize_endpoint(
        &self,
        _provider_kind: AiProviderKindInput,
        _normalized_url: &str,
    ) -> bool {
        false
    }
}

struct UnusedSecretStore;

#[async_trait]
impl AiSecretStore for UnusedSecretStore {
    async fn resolve(&self, _reference: &SecretRef) -> Result<SecretString, SecretError> {
        Err(SecretError::Unavailable)
    }

    async fn put(
        &self,
        _reference: Option<&SecretRef>,
        _value: SecretString,
    ) -> Result<SecretRef, SecretError> {
        Err(SecretError::Unavailable)
    }

    async fn delete(&self, _reference: &SecretRef) -> Result<(), SecretError> {
        Err(SecretError::Unavailable)
    }
}

struct ProtectionPolicy;

#[async_trait]
impl AiContentProtectionPolicyResolver for ProtectionPolicy {
    async fn resolve(
        &self,
        _principal: &AuthPrincipal,
        scope: &AiScope,
    ) -> Result<AiContentProtectionPolicy, AiError> {
        Ok(AiContentProtectionPolicy {
            scope: scope.clone(),
            mode: AiContentProtectionMode::DatabaseManaged,
            key_policy_reference: None,
            version: 1,
            ready: true,
        })
    }
}

struct AllowCleanImages;

#[async_trait]
impl AiAttachmentAcceptancePolicy for AllowCleanImages {
    async fn authorize(
        &self,
        _principal: &AuthPrincipal,
        _scope: &AiScope,
        candidate: &AiAttachmentCandidate,
    ) -> AiAccessDecision {
        if candidate.detected_mime == "image/png" {
            AiAccessDecision::allow("clean_image", "attachment-policy-v1")
        } else {
            AiAccessDecision::deny("unsupported_type", "attachment-policy-v1")
        }
    }
}

struct TestScanner;

#[async_trait]
impl AiAttachmentScanner for TestScanner {
    async fn scan(
        &self,
        _request: &AiAttachmentScanRequest,
        body: StorageByteStream,
    ) -> Result<AiAttachmentScanReport, AiError> {
        let bytes = collect_storage_stream(body)
            .await
            .map_err(|_| AiError::PersistenceFailed)?;
        let hash = sha256_hex(&bytes);
        let (mime, verdict) = if bytes.starts_with(b"MZ") {
            (
                "application/x-executable",
                AiAttachmentScanVerdict::Reject {
                    reason_code: "executable_denied".to_owned(),
                },
            )
        } else {
            ("image/png", AiAttachmentScanVerdict::Clean)
        };
        AiAttachmentScanReport::new(mime, bytes.len() as u64, hash, "test-scanner-v1", verdict)
    }
}

#[derive(Default)]
struct MemoryBlobStore {
    blobs: Mutex<BTreeMap<String, Vec<u8>>>,
    fail_deletes: Mutex<bool>,
}

impl MemoryBlobStore {
    fn count(&self) -> usize {
        self.blobs.lock().expect("blob lock").len()
    }

    fn set_fail_deletes(&self, fail: bool) {
        *self.fail_deletes.lock().expect("delete failure lock") = fail;
    }
}

#[async_trait]
impl BlobStore for MemoryBlobStore {
    fn backend(&self) -> StorageBackend {
        StorageBackend::Local
    }

    async fn put_blob(
        &self,
        key: &str,
        body: StorageByteStream,
        _options: BlobPutOptions,
    ) -> Result<BlobWriteOutcome, StorageError> {
        let bytes = collect_storage_stream(body).await?;
        let outcome = BlobWriteOutcome {
            size_bytes: bytes.len() as u64,
            sha256_hex: sha256_hex(&bytes),
        };
        self.blobs
            .lock()
            .expect("blob lock")
            .insert(key.to_owned(), bytes.to_vec());
        Ok(outcome)
    }

    async fn put_blob_if_not_exists(
        &self,
        key: &str,
        body: StorageByteStream,
        options: BlobPutOptions,
    ) -> Result<Option<BlobWriteOutcome>, StorageError> {
        if self.blobs.lock().expect("blob lock").contains_key(key) {
            return Ok(None);
        }
        self.put_blob(key, body, options).await.map(Some)
    }

    async fn get_blob(&self, key: &str) -> Result<BlobBody, StorageError> {
        let bytes = self
            .blobs
            .lock()
            .expect("blob lock")
            .get(key)
            .cloned()
            .ok_or_else(|| StorageError::MissingBlob {
                key: key.to_owned(),
            })?;
        Ok(BlobBody {
            key: key.to_owned(),
            metadata: Some(BlobMetadata {
                key: key.to_owned(),
                size_bytes: Some(bytes.len() as u64),
                sha256_hex: Some(sha256_hex(&bytes)),
                etag: None,
                last_modified: None,
            }),
            body: StorageByteStream::from_bytes(bytes),
        })
    }

    async fn get_blob_range(&self, key: &str, range: Range<u64>) -> Result<BlobBody, StorageError> {
        let blob = self.get_blob(key).await?;
        let bytes = collect_storage_stream(blob.body).await?;
        let start = usize::try_from(range.start).map_err(|_| StorageError::PreconditionFailed {
            key: key.to_owned(),
            condition: "invalid range".to_owned(),
        })?;
        let end = usize::try_from(range.end).map_err(|_| StorageError::PreconditionFailed {
            key: key.to_owned(),
            condition: "invalid range".to_owned(),
        })?;
        let selected = bytes
            .get(start..end)
            .ok_or_else(|| StorageError::PreconditionFailed {
                key: key.to_owned(),
                condition: "invalid range".to_owned(),
            })?
            .to_vec();
        Ok(BlobBody {
            key: key.to_owned(),
            metadata: None,
            body: StorageByteStream::from_bytes(selected),
        })
    }

    async fn blob_exists(&self, key: &str) -> Result<bool, StorageError> {
        Ok(self.blobs.lock().expect("blob lock").contains_key(key))
    }

    async fn head_blob(&self, key: &str) -> Result<Option<BlobMetadata>, StorageError> {
        Ok(self
            .blobs
            .lock()
            .expect("blob lock")
            .get(key)
            .map(|bytes| BlobMetadata {
                key: key.to_owned(),
                size_bytes: Some(bytes.len() as u64),
                sha256_hex: Some(sha256_hex(bytes)),
                etag: None,
                last_modified: None,
            }))
    }

    async fn list_blobs_page(
        &self,
        prefix: &str,
        _continuation: Option<String>,
        limit: usize,
    ) -> Result<BlobListPage, StorageError> {
        Ok(BlobListPage {
            keys: self
                .blobs
                .lock()
                .expect("blob lock")
                .keys()
                .filter(|key| key.starts_with(prefix))
                .take(limit)
                .cloned()
                .collect(),
            next_continuation: None,
        })
    }

    async fn delete_blob(&self, key: &str) -> Result<(), StorageError> {
        if *self.fail_deletes.lock().expect("delete failure lock") {
            return Err(StorageError::Provider {
                backend: "test".to_owned(),
                message: "injected delete failure".to_owned(),
                retryable: true,
            });
        }
        self.blobs.lock().expect("blob lock").remove(key);
        Ok(())
    }
}

fn principal(subject: &str) -> AuthPrincipal {
    AuthPrincipal::User(AuthUser {
        user_id: subject.to_owned(),
        session_id: Uuid::new_v4(),
        roles: vec![],
        scopes: vec![],
        session: SessionContext::default(),
        token_claims: AccessTokenMetadata {
            tenant_id: Some("tenant-1".to_owned()),
            ..AccessTokenMetadata::default()
        },
    })
}

fn recent_admin(now: OffsetDateTime) -> AuthPrincipal {
    let assurance = SessionAssurance::new(
        now,
        ["otp", "pwd"],
        Some("urn:test:loa:2".to_owned()),
        Some("test".to_owned()),
        MfaAcceptance::Satisfied,
    )
    .expect("test assurance should validate");
    AuthPrincipal::User(AuthUser {
        user_id: "attachment-admin".to_owned(),
        session_id: Uuid::new_v4(),
        roles: vec!["admin".to_owned()],
        scopes: vec![],
        session: SessionContext::default().with_assurance(assurance),
        token_claims: AccessTokenMetadata {
            auth_time: Some(now.unix_timestamp()),
            amr: Some(vec!["otp".to_owned(), "pwd".to_owned()]),
            acr: Some("urn:test:loa:2".to_owned()),
            tenant_id: Some("tenant-1".to_owned()),
            ..AccessTokenMetadata::default()
        },
    })
}

struct Fixture {
    database: Database<SqliteBackend>,
    session_service: OrmAiSessionService,
    attachment_service: OrmAiAttachmentService,
    blobs: Arc<MemoryBlobStore>,
}

async fn fixture() -> Fixture {
    fixture_with_clock(Arc::new(SystemClock)).await
}

async fn fixture_with_clock(clock: Arc<dyn Clock>) -> Fixture {
    let database = Database::<SqliteBackend>::connect_sqlite("sqlite::memory:")
        .await
        .expect("in-memory SQLite should open");
    let module = AiSchemaModule;
    let plan = database
        .schema()
        .plan_migration_to_entities(
            "ai-attachment-test-v1",
            "AI attachment service test",
            module.entities(),
        )
        .await
        .expect("AI schema migration should plan");
    database
        .schema()
        .apply_migration(&plan, ApplyOptions::default())
        .await
        .expect("AI schema migration should apply to in-memory SQLite");
    let configuration = OrmAiConfigurationService::new(
        database.clone(),
        Arc::new(AllowConfiguration),
        Arc::new(DenyEndpoints),
        RecentMfaPolicy {
            maximum_age: Duration::minutes(5),
            clock_skew: Duration::seconds(30),
            allowed_amr: vec!["otp".to_owned()],
            allowed_acr: vec!["urn:test:loa:2".to_owned()],
            match_mode: AssuranceMatchMode::All,
        },
        clock.clone(),
        Arc::new(UnusedSecretStore),
    );
    let configuration_now = OffsetDateTime::from_unix_timestamp(clock.now().unix_timestamp())
        .expect("configuration time should be representable");
    configuration
        .set_retention_policy(
            &recent_admin(configuration_now),
            SetAiRetentionPolicyInput {
                scope: AiScopeInput {
                    kind: "collection".to_owned(),
                    id: "54".to_owned(),
                    tenant_id: Some("tenant-1".to_owned()),
                },
                message_retention_seconds: None,
                delta_retention_seconds: 60,
                raw_payload_retention_seconds: 60,
                audit_retention_seconds: 60,
                deleted_content_purge_seconds: 60,
                provider_file_delete_required: true,
                inbox_event_retention_seconds: 60,
                inbox_minimum_events: 1,
                expected_version: None,
            },
        )
        .await
        .expect("attachment retention policy should configure");
    let blobs = Arc::new(MemoryBlobStore::default());
    let session_service = OrmAiSessionService::new(
        database.clone(),
        Arc::new(AllowAll),
        Arc::new(ProtectionPolicy),
        Arc::new(DatabaseManagedContentProtector),
    );
    let attachment_service = OrmAiAttachmentService::new(
        database.clone(),
        Arc::new(AllowAll),
        Arc::new(ProtectionPolicy),
        Arc::new(DatabaseManagedContentProtector),
        blobs.clone(),
        Arc::new(TestScanner),
        Arc::new(AllowCleanImages),
        clock,
    );
    Fixture {
        database,
        session_service,
        attachment_service,
        blobs,
    }
}

async fn create_session(fixture: &Fixture, owner: &AuthPrincipal) -> AiSessionView {
    fixture
        .session_service
        .create_session(
            owner,
            CreateAiSessionInput {
                scope: AiScopeInput {
                    kind: "collection".to_owned(),
                    id: "54".to_owned(),
                    tenant_id: Some("tenant-1".to_owned()),
                },
                title: Some("Attachment test".to_owned()),
            },
        )
        .await
        .expect("session should create")
}

fn page() -> graphql_orm::graphql::pagination::ValidatedKeysetConnection {
    KeysetConnectionInput {
        first: Some(20),
        ..Default::default()
    }
    .validate(20, 100)
    .expect("valid attachment page")
}

#[tokio::test]
async fn ticket_is_owner_bound_scanned_promoted_released_and_message_linked() {
    let fixture = fixture().await;
    let owner = principal("owner");
    let stranger = principal("stranger");
    let session = create_session(&fixture, &owner).await;
    let bytes = b"\x89PNG\r\n\x1a\nclean-test-image".to_vec();
    let ticket = fixture
        .attachment_service
        .create_upload(
            &owner,
            CreateAiAttachmentUploadInput {
                session_id: session.id,
                filename: "../unsafe/path/example.png".to_owned(),
                declared_mime: Some("image/png".to_owned()),
                expected_byte_count: bytes.len() as i64,
            },
        )
        .await
        .expect("ticket should create");
    assert_eq!(ticket.attachment().safe_filename, "example.png");
    assert!(!format!("{ticket:?}").contains(ticket.secret().expose_secret()));
    assert!(matches!(
        fixture
            .attachment_service
            .upload(
                &stranger,
                ticket.attachment().id,
                SecretString::from(ticket.secret().expose_secret().to_owned()),
                StorageByteStream::from_bytes(bytes.clone()),
            )
            .await,
        Err(AiError::NotFound)
    ));
    assert!(matches!(
        fixture
            .attachment_service
            .upload(
                &owner,
                ticket.attachment().id,
                SecretString::from("wrong-upload-token".to_owned()),
                StorageByteStream::from_bytes(bytes.clone()),
            )
            .await,
        Err(AiError::Conflict)
    ));
    let ready = fixture
        .attachment_service
        .upload(
            &owner,
            ticket.attachment().id,
            SecretString::from(ticket.secret().expose_secret().to_owned()),
            StorageByteStream::from_bytes(bytes.clone()),
        )
        .await
        .expect("clean exact body should become ready");
    assert_eq!(ready.quarantine_state, "ready");
    assert_eq!(ready.scan_state, "clean");
    assert_eq!(fixture.blobs.count(), 1);
    assert!(matches!(
        fixture
            .attachment_service
            .upload(
                &owner,
                ready.id,
                SecretString::from(ticket.secret().expose_secret().to_owned()),
                StorageByteStream::from_bytes(b"replay".to_vec()),
            )
            .await,
        Err(AiError::Conflict)
    ));
    let released = fixture
        .attachment_service
        .finalize_upload(&owner, ready.id)
        .await
        .expect("clean ready object should release");
    assert_eq!(released.quarantine_state, "released");
    let sent = fixture
        .session_service
        .send_message(
            &owner,
            SendAiMessageInput {
                session_id: session.id,
                text: "Please inspect the attached image".to_owned(),
                attachment_ids: vec![released.id],
                client_message_id: Uuid::new_v4(),
            },
        )
        .await
        .expect("released owned attachment should link through normal message mutation");
    assert!(!sent.message_id.is_nil());
    let request_block = ModelInputBlock::Attachment {
        attachment_id: released.id.to_string(),
        mime: "image/png".to_owned(),
        byte_count: bytes.len() as u64,
        sha256: sha256_hex(&bytes),
    };
    let request = AiProviderAttachmentRequest::try_from(&request_block)
        .expect("released attachment request should validate");
    let scope = AiScope::new("collection", "54").with_tenant_id("tenant-1");
    let resolved_owner =
        ResolvedPrincipal::new(owner.reference(), owner.clone(), OffsetDateTime::now_utc())
            .expect("owner principal should resolve");
    let resolved = fixture
        .attachment_service
        .resolve_for_provider(&resolved_owner, AiSessionId(session.id), &scope, &request)
        .await
        .expect("linked released bytes should reopen exactly for their owner");
    assert_eq!(resolved.request(), &request);
    assert_eq!(resolved.safe_filename(), "example.png");
    assert_eq!(resolved.bytes(), bytes);
    let resolved_stranger = ResolvedPrincipal::new(
        stranger.reference(),
        stranger.clone(),
        OffsetDateTime::now_utc(),
    )
    .expect("stranger principal should resolve");
    assert!(matches!(
        fixture
            .attachment_service
            .resolve_for_provider(
                &resolved_stranger,
                AiSessionId(session.id),
                &scope,
                &request,
            )
            .await,
        Err(AiError::NotFound | AiError::ReauthorizationFailed)
    ));
    {
        let mut blobs = fixture.blobs.blobs.lock().expect("blob lock");
        let stored = blobs
            .values_mut()
            .next()
            .expect("released object should still exist");
        stored[0] ^= 0xff;
    }
    assert!(matches!(
        fixture
            .attachment_service
            .resolve_for_provider(&resolved_owner, AiSessionId(session.id), &scope, &request)
            .await,
        Err(AiError::ReauthorizationFailed)
    ));
    assert!(matches!(
        fixture
            .attachment_service
            .remove_attachment(&owner, released.id)
            .await,
        Err(AiError::Conflict)
    ));
    let attachments = fixture
        .attachment_service
        .attachments(&owner, AiSessionId(session.id), page())
        .await
        .expect("owner metadata should list");
    assert_eq!(attachments.edges.len(), 1);
    assert_eq!(attachments.edges[0].node.message_id, Some(sent.message_id));
    assert!(matches!(
        fixture
            .attachment_service
            .attachments(&stranger, AiSessionId(session.id), page())
            .await,
        Err(AiError::NotFound)
    ));
    let events = fixture
        .session_service
        .session_event_page(&owner, AiSessionId(session.id), 0, 20)
        .await
        .expect("attachment events should be cursor-addressable");
    assert_eq!(events.events[0].event_type, "attachment_upload_created");
    assert_eq!(events.events[1].event_type, "attachment_released");
    assert_eq!(events.events[2].event_type, "message_queued");
}

#[tokio::test]
async fn scanner_rejection_never_promotes_or_releases_bytes() {
    let fixture = fixture().await;
    let owner = principal("owner");
    let session = create_session(&fixture, &owner).await;
    let bytes = b"MZmalware-or-executable".to_vec();
    let ticket = fixture
        .attachment_service
        .create_upload(
            &owner,
            CreateAiAttachmentUploadInput {
                session_id: session.id,
                filename: "payload.exe".to_owned(),
                declared_mime: Some("application/octet-stream".to_owned()),
                expected_byte_count: bytes.len() as i64,
            },
        )
        .await
        .expect("ticket should create");
    assert!(matches!(
        fixture
            .attachment_service
            .upload(
                &owner,
                ticket.attachment().id,
                SecretString::from(ticket.secret().expose_secret().to_owned()),
                StorageByteStream::from_bytes(bytes),
            )
            .await,
        Err(AiError::Forbidden)
    ));
    assert_eq!(fixture.blobs.count(), 0);
    let attachments = fixture
        .attachment_service
        .attachments(&owner, AiSessionId(session.id), page())
        .await
        .expect("rejected metadata remains inspectable");
    assert_eq!(attachments.edges[0].node.quarantine_state, "rejected");
    assert_eq!(attachments.edges[0].node.scan_state, "rejected");
    assert_eq!(
        attachments.edges[0].node.rejection_code.as_deref(),
        Some("executable_denied")
    );
    assert!(matches!(
        fixture
            .attachment_service
            .finalize_upload(&owner, ticket.attachment().id)
            .await,
        Err(AiError::Conflict)
    ));
}

#[tokio::test]
async fn cleanup_expires_tickets_once_and_hides_deleted_metadata() {
    let clock = FixedClock::new(
        time::OffsetDateTime::from_unix_timestamp(2_000_000_000)
            .expect("fixed timestamp should be valid"),
    );
    let fixture = fixture_with_clock(Arc::new(clock.clone())).await;
    let owner = principal("owner");
    let session = create_session(&fixture, &owner).await;
    fixture
        .attachment_service
        .create_upload(
            &owner,
            CreateAiAttachmentUploadInput {
                session_id: session.id,
                filename: "eventually-expired.png".to_owned(),
                declared_mime: Some("image/png".to_owned()),
                expected_byte_count: 16,
            },
        )
        .await
        .expect("ticket should create");
    clock.advance_seconds(601);

    let first = fixture
        .attachment_service
        .cleanup_once()
        .await
        .expect("expired ticket cleanup should complete");
    assert_eq!(first.examined, 1);
    assert_eq!(first.cleaned, 1);
    assert_eq!(first.failed, 0);
    let second = fixture
        .attachment_service
        .cleanup_once()
        .await
        .expect("cleanup retry should be idempotent");
    assert_eq!(second.cleaned, 0);
    let visible = fixture
        .attachment_service
        .attachments(&owner, AiSessionId(session.id), page())
        .await
        .expect("owner list should remain available");
    assert!(visible.edges.is_empty());
}

#[tokio::test]
async fn concurrent_cleanup_workers_finalize_one_expired_ticket_once() {
    let clock = FixedClock::new(
        time::OffsetDateTime::from_unix_timestamp(2_000_000_000)
            .expect("fixed timestamp should be valid"),
    );
    let fixture = fixture_with_clock(Arc::new(clock.clone())).await;
    let owner = principal("owner");
    let session = create_session(&fixture, &owner).await;
    fixture
        .attachment_service
        .create_upload(
            &owner,
            CreateAiAttachmentUploadInput {
                session_id: session.id,
                filename: "raced-expiry.png".to_owned(),
                declared_mime: Some("image/png".to_owned()),
                expected_byte_count: 16,
            },
        )
        .await
        .expect("ticket should create");
    clock.advance_seconds(601);

    let (left, right) = tokio::join!(
        fixture.attachment_service.cleanup_once(),
        fixture.attachment_service.cleanup_once()
    );
    let left = left.expect("left cleanup worker should converge");
    let right = right.expect("right cleanup worker should converge");
    assert_eq!(left.cleaned + right.cleaned, 1);
    assert_eq!(left.failed + right.failed, 0);
}

#[tokio::test]
async fn cleanup_retains_ambiguous_storage_deletes_and_retries_safely() {
    let clock = FixedClock::new(
        time::OffsetDateTime::from_unix_timestamp(2_000_000_000)
            .expect("fixed timestamp should be valid"),
    );
    let fixture = fixture_with_clock(Arc::new(clock.clone())).await;
    let owner = principal("owner");
    let session = create_session(&fixture, &owner).await;
    let bytes = b"MZcleanup-retry".to_vec();
    let ticket = fixture
        .attachment_service
        .create_upload(
            &owner,
            CreateAiAttachmentUploadInput {
                session_id: session.id,
                filename: "rejected.exe".to_owned(),
                declared_mime: Some("application/octet-stream".to_owned()),
                expected_byte_count: bytes.len() as i64,
            },
        )
        .await
        .expect("ticket should create");
    fixture.blobs.set_fail_deletes(true);
    assert!(matches!(
        fixture
            .attachment_service
            .upload(
                &owner,
                ticket.attachment().id,
                SecretString::from(ticket.secret().expose_secret().to_owned()),
                StorageByteStream::from_bytes(bytes),
            )
            .await,
        Err(AiError::Forbidden)
    ));
    assert_eq!(fixture.blobs.count(), 1);

    let failed = fixture
        .attachment_service
        .cleanup_once()
        .await
        .expect("ambiguous storage deletion should remain a reportable retry");
    assert_eq!(failed.failed, 1);
    assert_eq!(fixture.blobs.count(), 1);
    fixture.blobs.set_fail_deletes(false);
    clock.advance_seconds(121);
    let recovered = fixture
        .attachment_service
        .cleanup_once()
        .await
        .expect("later cleanup retry should converge");
    assert_eq!(recovered.cleaned, 1);
    assert_eq!(fixture.blobs.count(), 0);
}

#[tokio::test]
async fn deleting_session_retention_waits_for_confirmed_attachment_cleanup() {
    let clock = FixedClock::new(
        OffsetDateTime::from_unix_timestamp(2_000_000_000)
            .expect("fixed timestamp should be valid"),
    );
    let fixture = fixture_with_clock(Arc::new(clock.clone())).await;
    let owner = principal("owner");
    let session = create_session(&fixture, &owner).await;
    let bytes = b"\x89PNG\r\n\x1a\nretention-image".to_vec();
    let ticket = fixture
        .attachment_service
        .create_upload(
            &owner,
            CreateAiAttachmentUploadInput {
                session_id: session.id,
                filename: "retention.png".to_owned(),
                declared_mime: Some("image/png".to_owned()),
                expected_byte_count: bytes.len() as i64,
            },
        )
        .await
        .expect("retention attachment ticket should create");
    let ready = fixture
        .attachment_service
        .upload(
            &owner,
            ticket.attachment().id,
            SecretString::from(ticket.secret().expose_secret().to_owned()),
            StorageByteStream::from_bytes(bytes),
        )
        .await
        .expect("retention attachment should upload");
    let released = fixture
        .attachment_service
        .finalize_upload(&owner, ready.id)
        .await
        .expect("retention attachment should release");
    let sent = fixture
        .session_service
        .send_message(
            &owner,
            SendAiMessageInput {
                session_id: session.id,
                text: "Retain metadata until object deletion is proven".to_owned(),
                attachment_ids: vec![released.id],
                client_message_id: Uuid::new_v4(),
            },
        )
        .await
        .expect("released attachment should link to a message");
    let run_service = OrmAiRunService::new(
        fixture.database.clone(),
        Arc::new(clock.clone()),
        AiRunServiceLimits::new(Duration::minutes(5), Duration::hours(1), 16, 2, 8)
            .expect("run limits should validate"),
    );
    let lease = run_service
        .claim_next("attachment-retention-test")
        .await
        .expect("queued attachment run should be claimable")
        .expect("attachment run should exist");
    assert_eq!(lease.run_id(), AiRunId(sent.run_id));
    run_service
        .finish(
            &lease,
            AiRunCompletion::new(
                AiRunState::Cancelled,
                "session_deleted_before_execution",
                Some("session_deleted_before_execution".to_owned()),
                None,
            )
            .expect("redacted cancellation should validate"),
        )
        .await
        .expect("attachment run should close before deletion retention");
    fixture
        .session_service
        .delete_session(&owner, AiSessionId(session.id))
        .await
        .expect("session should enter deletion");
    let retention = OrmAiSessionRetentionService::new(
        fixture.database.clone(),
        Arc::new(clock.clone()),
        AiSessionRetentionLimits::default(),
    );

    let requested = retention
        .prune_session_content(None)
        .await
        .expect("retention should request external attachment cleanup");
    assert_eq!(requested.deleting_session_attachment_cleanups_requested, 1);
    assert_eq!(requested.deleting_session_attachments_deleted, 0);
    assert_eq!(fixture.blobs.count(), 1);

    fixture.blobs.set_fail_deletes(true);
    let ambiguous = fixture
        .attachment_service
        .cleanup_once()
        .await
        .expect("ambiguous deletion should remain retryable");
    assert_eq!(ambiguous.failed, 1);
    assert_eq!(fixture.blobs.count(), 1);
    let retained = retention
        .prune_session_content(None)
        .await
        .expect("retention should preserve ambiguous attachment metadata");
    assert_eq!(retained.deleting_session_attachments_deleted, 0);
    assert_eq!(retained.attachment_cleanups_blocked, 1);

    fixture.blobs.set_fail_deletes(false);
    clock.advance_seconds(121);
    let cleaned = fixture
        .attachment_service
        .cleanup_once()
        .await
        .expect("later exact-reference cleanup should converge");
    assert_eq!(cleaned.cleaned, 1);
    assert_eq!(fixture.blobs.count(), 0);
    let deleted = retention
        .prune_session_content(None)
        .await
        .expect("retention should delete only fully cleaned metadata");
    assert_eq!(deleted.deleting_session_attachments_deleted, 1);
    assert_eq!(deleted.deleting_session_attachment_cleanups_requested, 0);
}
