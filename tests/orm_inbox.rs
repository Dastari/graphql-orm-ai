#![cfg(feature = "sqlite")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use agql_auth::{
    AccessTokenMetadata, AssuranceMatchMode, AuthPrincipal, AuthUser, CurrentPrincipalResolver,
    FixedClock, MfaAcceptance, PrincipalReference, RecentMfaPolicy, ResolvedPrincipal,
    SessionAssurance, SessionContext,
};
use async_trait::async_trait;
use futures::StreamExt;
use graphql_orm::graphql::orm::{ApplyOptions, OrmSchemaModule};
use graphql_orm::prelude::{Database, SqliteBackend};
use graphql_orm_ai::*;
use secrecy::SecretString;
use time::Duration as TimeDuration;
use time::OffsetDateTime;
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
        AiAccessDecision::allow("test", "test-v1")
    }

    async fn can_access_session(
        &self,
        _principal: &AuthPrincipal,
        _session_id: AiSessionId,
        _action: AiSessionAction,
    ) -> AiAccessDecision {
        AiAccessDecision::allow("test", "test-v1")
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
        Err(SecretError::ReadOnly)
    }

    async fn delete(&self, _reference: &SecretRef) -> Result<(), SecretError> {
        Err(SecretError::ReadOnly)
    }
}

struct ToggleResolver {
    principal: AuthPrincipal,
    active: Arc<AtomicBool>,
}

#[async_trait]
impl CurrentPrincipalResolver for ToggleResolver {
    async fn resolve(
        &self,
        reference: &PrincipalReference,
    ) -> agql_auth::AuthResult<ResolvedPrincipal> {
        if !self.active.load(Ordering::SeqCst) {
            return Err(agql_auth::AuthError::Forbidden);
        }
        ResolvedPrincipal::new(
            reference.clone(),
            self.principal.clone(),
            OffsetDateTime::now_utc(),
        )
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
    .expect("test assurance is valid");
    AuthPrincipal::User(AuthUser {
        user_id: "retention-admin".to_owned(),
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

async fn services(
    reauthorization_interval: Duration,
) -> (
    OrmAiSessionService,
    OrmAiInboxService,
    OrmAiInboxPruningService,
    OrmAiSessionRetentionService,
    AuthPrincipal,
    Arc<AtomicBool>,
) {
    let database = Database::<SqliteBackend>::connect_sqlite("sqlite::memory:")
        .await
        .expect("in-memory SQLite opens");
    let module = AiSchemaModule;
    let plan = database
        .schema()
        .plan_migration_to_entities(
            "ai-inbox-test-v1",
            "AI principal inbox test",
            module.entities(),
        )
        .await
        .expect("schema plans");
    database
        .schema()
        .apply_migration(&plan, ApplyOptions::default())
        .await
        .expect("schema applies");
    let now = OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp())
        .expect("current second is representable");
    let configuration = OrmAiConfigurationService::new(
        database.clone(),
        Arc::new(AllowConfiguration),
        Arc::new(DenyEndpoints),
        RecentMfaPolicy {
            maximum_age: TimeDuration::minutes(5),
            clock_skew: TimeDuration::seconds(30),
            allowed_amr: vec!["otp".to_owned()],
            allowed_acr: vec!["urn:test:loa:2".to_owned()],
            match_mode: AssuranceMatchMode::All,
        },
        Arc::new(FixedClock::new(now)),
        Arc::new(UnusedSecretStore),
    );
    configuration
        .set_retention_policy(
            &recent_admin(now),
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
        .expect("retention policy is GraphQL-service managed");
    let access_policy = Arc::new(AllowAll);
    let protection_policy = Arc::new(ProtectionPolicy);
    let content_protector = Arc::new(DatabaseManagedContentProtector);
    let sessions = OrmAiSessionService::new(
        database.clone(),
        access_policy.clone(),
        protection_policy.clone(),
        content_protector.clone(),
    );
    let owner = principal("owner");
    let active = Arc::new(AtomicBool::new(true));
    let inbox = OrmAiInboxService::new(
        database.clone(),
        Arc::new(ToggleResolver {
            principal: owner.clone(),
            active: active.clone(),
        }),
        access_policy,
        protection_policy,
        content_protector,
    )
    .with_reauthorization_interval(reauthorization_interval)
    .with_replay_page_size(1);
    let pruning = OrmAiInboxPruningService::new(
        database.clone(),
        Arc::new(FixedClock::new(now + TimeDuration::seconds(61))),
        AiInboxPruningLimits::new(10, 100).expect("valid pruning limits"),
    );
    let session_retention = OrmAiSessionRetentionService::new(
        database,
        Arc::new(FixedClock::new(now + TimeDuration::seconds(61))),
        AiSessionRetentionLimits::default()
            .with_inbox_event_limit(1)
            .expect("valid session inbox-retention limit"),
    );
    (sessions, inbox, pruning, session_retention, owner, active)
}

async fn create_session(
    sessions: &OrmAiSessionService,
    principal: &AuthPrincipal,
) -> AiSessionView {
    sessions
        .create_session(
            principal,
            CreateAiSessionInput {
                scope: AiScopeInput {
                    kind: "collection".to_owned(),
                    id: "54".to_owned(),
                    tenant_id: Some("tenant-1".to_owned()),
                },
                title: Some("Inbox".to_owned()),
            },
        )
        .await
        .expect("session is created")
}

#[tokio::test]
async fn owner_pages_atomic_cross_session_events_without_cross_principal_leakage() {
    let (sessions, inbox, _pruning, _retention, owner, _active) =
        services(Duration::from_secs(60)).await;
    let first_session = create_session(&sessions, &owner).await;
    let second_session = create_session(&sessions, &owner).await;
    let sent = sessions
        .send_message(
            &owner,
            SendAiMessageInput {
                session_id: first_session.id,
                text: "bounded message".to_owned(),
                attachment_ids: vec![],
                client_message_id: Uuid::new_v4(),
            },
        )
        .await
        .expect("message commits with its inbox event");

    let first_page = inbox
        .inbox_event_page(&owner, 0, 2)
        .await
        .expect("owner inbox loads");
    assert_eq!(first_page.watermark, 3);
    assert!(first_page.has_more);
    assert_eq!(first_page.events.len(), 2);
    assert_eq!(first_page.events[0].event_type, "session_created");
    assert_eq!(first_page.events[0].session_id, first_session.id);
    assert_eq!(first_page.events[1].session_id, second_session.id);

    let second_page = inbox
        .inbox_event_page(&owner, first_page.events[1].sequence, 2)
        .await
        .expect("owner inbox advances");
    assert_eq!(second_page.events.len(), 1);
    assert_eq!(second_page.events[0].event_type, "message_queued");
    assert_eq!(
        second_page.events[0].payload.0["runId"],
        sent.run_id.to_string()
    );

    let stranger_page = inbox
        .inbox_event_page(&principal("stranger"), 0, 100)
        .await
        .expect("an unrelated empty inbox does not reveal owner activity");
    assert!(stranger_page.events.is_empty());
    assert_eq!(stranger_page.watermark, 0);
}

#[tokio::test]
async fn subscription_replays_to_a_watermark_follows_commits_and_closes_on_revocation() {
    let (sessions, inbox, _pruning, _retention, owner, active) =
        services(Duration::from_millis(20)).await;
    let session = create_session(&sessions, &owner).await;
    let mut stream = inbox
        .inbox_events(owner.clone(), 0)
        .await
        .expect("inbox subscription opens");

    let created = stream
        .next()
        .await
        .expect("created item")
        .expect("created event");
    assert_eq!(
        created.event.expect("durable event").event_type,
        "session_created"
    );

    sessions
        .archive_session(&owner, AiSessionId(session.id))
        .await
        .expect("archive commits with a wakeup");
    let archived = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("subscription wakes")
        .expect("archive item")
        .expect("archive event");
    assert_eq!(
        archived.event.expect("durable event").event_type,
        "session_archived"
    );

    active.store(false, Ordering::SeqCst);
    let revoked = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("periodic reauthorization runs")
        .expect("terminal error item");
    assert!(matches!(revoked, Err(AiError::ReauthorizationFailed)));
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn pruning_deletes_only_an_expired_prefix_and_requires_cursor_reset() {
    let (sessions, inbox, pruning, _retention, owner, _active) =
        services(Duration::from_secs(60)).await;
    let session = create_session(&sessions, &owner).await;
    for text in ["first", "second"] {
        sessions
            .send_message(
                &owner,
                SendAiMessageInput {
                    session_id: session.id,
                    text: text.to_owned(),
                    attachment_ids: vec![],
                    client_message_id: Uuid::new_v4(),
                },
            )
            .await
            .expect("message commits");
    }

    let report = pruning
        .prune_inbox_events()
        .await
        .expect("bounded pruning succeeds");
    assert_eq!(report.streams_pruned, 1);
    assert_eq!(report.events_deleted, 2);
    assert_eq!(report.streams_not_ready, 0);

    let stale = inbox
        .inbox_event_page(&owner, 0, 100)
        .await
        .expect("stale cursor receives an explicit reset");
    assert!(stale.reset_required);
    assert_eq!(stale.watermark, 3);
    assert!(stale.events.is_empty());

    let retained = inbox
        .inbox_event_page(&owner, 2, 100)
        .await
        .expect("retained tail remains readable");
    assert!(!retained.reset_required);
    assert_eq!(retained.events.len(), 1);
    assert_eq!(retained.events[0].sequence, 3);
    assert_eq!(retained.events[0].event_type, "message_queued");

    let repeated = pruning
        .prune_inbox_events()
        .await
        .expect("recent-event floor makes repeated pruning a no-op");
    assert_eq!(repeated.events_deleted, 0);
}

#[tokio::test]
async fn deleting_session_tombstones_inbox_payloads_without_punching_stream_holes() {
    let (sessions, inbox, pruning, retention, owner, _active) =
        services(Duration::from_secs(60)).await;
    let session = create_session(&sessions, &owner).await;
    sessions
        .delete_session(&owner, AiSessionId(session.id))
        .await
        .expect("session deletion should start");

    let payload_pass = retention
        .prune_session_content(None)
        .await
        .expect("session-bound inbox payloads should tombstone");
    assert_eq!(payload_pass.deleting_session_inbox_payloads_purged, 1);
    assert_eq!(payload_pass.deleting_sessions_finalized, 0);
    let reset = inbox
        .inbox_event_page(&owner, 0, 100)
        .await
        .expect("a tombstone should request explicit reset");
    assert!(reset.reset_required);
    assert!(reset.events.is_empty());
    assert_eq!(reset.watermark, 2);

    let second_payload_pass = retention
        .prune_session_content(None)
        .await
        .expect("the next bounded inbox payload should tombstone");
    assert_eq!(
        second_payload_pass.deleting_session_inbox_payloads_purged,
        1
    );
    assert_eq!(second_payload_pass.deleting_sessions_finalized, 0);

    let final_pass = retention
        .prune_session_content(None)
        .await
        .expect("dependency-free session should finalize");
    assert_eq!(final_pass.deleting_sessions_finalized, 1);
    assert!(
        sessions
            .delete_session(&owner, AiSessionId(session.id))
            .await
            .expect("delete replay after finalization should remain idempotent")
    );

    let prefix = pruning
        .prune_inbox_events()
        .await
        .expect("ordinary pruning should retain prefix semantics");
    assert_eq!(prefix.events_deleted, 1);
    let stale = inbox
        .inbox_event_page(&owner, 0, 100)
        .await
        .expect("the advanced prefix still requests reset");
    assert!(stale.reset_required);
    assert_eq!(stale.watermark, 2);
}

#[tokio::test]
async fn pruning_fails_closed_for_an_unconfigured_scope() {
    let (sessions, inbox, pruning, _retention, owner, _active) =
        services(Duration::from_secs(60)).await;
    sessions
        .create_session(
            &owner,
            CreateAiSessionInput {
                scope: AiScopeInput {
                    kind: "collection".to_owned(),
                    id: "unconfigured".to_owned(),
                    tenant_id: Some("tenant-1".to_owned()),
                },
                title: Some("No retention policy".to_owned()),
            },
        )
        .await
        .expect("session creation is independent from retention configuration");

    let report = pruning
        .prune_inbox_events()
        .await
        .expect("missing policy is a reported fail-closed outcome");
    assert_eq!(report.streams_not_ready, 1);
    assert_eq!(report.events_deleted, 0);
    let page = inbox
        .inbox_event_page(&owner, 0, 100)
        .await
        .expect("unconfigured event remains durable");
    assert_eq!(page.events.len(), 1);
    assert!(!page.reset_required);
}
