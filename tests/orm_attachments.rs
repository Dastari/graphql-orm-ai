#![cfg(feature = "sqlite")]

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use agql_auth::{AccessTokenMetadata, AuthPrincipal, AuthUser, SessionContext, SystemClock};
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
}

impl MemoryBlobStore {
    fn count(&self) -> usize {
        self.blobs.lock().expect("blob lock").len()
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

struct Fixture {
    session_service: OrmAiSessionService,
    attachment_service: OrmAiAttachmentService,
    blobs: Arc<MemoryBlobStore>,
}

async fn fixture() -> Fixture {
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
    let blobs = Arc::new(MemoryBlobStore::default());
    let session_service = OrmAiSessionService::new(
        database.clone(),
        Arc::new(AllowAll),
        Arc::new(ProtectionPolicy),
        Arc::new(DatabaseManagedContentProtector),
    );
    let attachment_service = OrmAiAttachmentService::new(
        database,
        Arc::new(AllowAll),
        Arc::new(ProtectionPolicy),
        Arc::new(DatabaseManagedContentProtector),
        blobs.clone(),
        Arc::new(TestScanner),
        Arc::new(AllowCleanImages),
        Arc::new(SystemClock),
    );
    Fixture {
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
            StorageByteStream::from_bytes(bytes),
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
