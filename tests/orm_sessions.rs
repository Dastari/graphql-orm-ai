#![cfg(feature = "sqlite")]

use std::sync::Arc;

use agql_auth::{AccessTokenMetadata, AuthPrincipal, AuthUser, SessionContext};
use async_trait::async_trait;
use graphql_orm::graphql::orm::{ApplyOptions, OrmSchemaModule};
use graphql_orm::graphql::pagination::KeysetConnectionInput;
use graphql_orm::prelude::{Database, SqliteBackend};
use graphql_orm_ai::*;
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

async fn service() -> OrmAiSessionService {
    let database = Database::<SqliteBackend>::connect_sqlite("sqlite::memory:")
        .await
        .expect("in-memory SQLite should open");
    let module = AiSchemaModule;
    let plan = database
        .schema()
        .plan_migration_to_entities(
            "ai-session-test-v1",
            "AI session service test",
            module.entities(),
        )
        .await
        .expect("AI schema migration should plan");
    database
        .schema()
        .apply_migration(&plan, ApplyOptions::default())
        .await
        .expect("AI schema migration should apply to in-memory SQLite");
    OrmAiSessionService::new(
        database,
        Arc::new(AllowAll),
        Arc::new(ProtectionPolicy),
        Arc::new(DatabaseManagedContentProtector),
    )
}

fn scope_input() -> AiScopeInput {
    AiScopeInput {
        kind: "collection".to_owned(),
        id: "54".to_owned(),
        tenant_id: Some("tenant-1".to_owned()),
    }
}

#[tokio::test]
async fn owner_isolation_atomic_send_idempotency_and_windowed_reads() {
    let service = service().await;
    let owner = principal("owner");
    let stranger = principal("stranger");
    let session = service
        .create_session(
            &owner,
            CreateAiSessionInput {
                scope: scope_input(),
                title: Some("Research".to_owned()),
            },
        )
        .await
        .expect("owner creates a session");
    assert_eq!(session.stream_head, 0);
    assert!(
        service
            .session(&stranger, AiSessionId(session.id))
            .await
            .expect("cross-owner lookup is safely handled")
            .is_none()
    );

    let client_message_id = Uuid::new_v4();
    let first = service
        .send_message(
            &owner,
            SendAiMessageInput {
                session_id: session.id,
                text: "Find records containing the example term".to_owned(),
                attachment_ids: vec![],
                client_message_id,
            },
        )
        .await
        .expect("message and run are committed atomically");
    let replay = service
        .send_message(
            &owner,
            SendAiMessageInput {
                session_id: session.id,
                text: "Find records containing the example term".to_owned(),
                attachment_ids: vec![],
                client_message_id,
            },
        )
        .await
        .expect("same idempotency input returns the committed result");
    assert_eq!(first.message_id, replay.message_id);
    assert_eq!(first.run_id, replay.run_id);
    assert!(matches!(
        service
            .send_message(
                &owner,
                SendAiMessageInput {
                    session_id: session.id,
                    text: "Different content".to_owned(),
                    attachment_ids: vec![],
                    client_message_id,
                },
            )
            .await,
        Err(AiError::Conflict)
    ));

    let messages = service
        .messages(
            &owner,
            AiSessionId(session.id),
            KeysetConnectionInput {
                last: Some(20),
                ..Default::default()
            }
            .validate(20, 100)
            .expect("valid keyset request"),
        )
        .await
        .expect("bounded message window loads");
    assert_eq!(messages.edges.len(), 1);
    assert_eq!(
        messages.edges[0].node.preview,
        "Find records containing the example term"
    );

    let blocks = service
        .message_blocks(&owner, first.message_id, None, 20)
        .await
        .expect("bounded block window loads");
    assert_eq!(blocks.len(), 1);
    assert_eq!(
        blocks[0].content.0["text"],
        "Find records containing the example term"
    );

    let events = service
        .session_event_page(&owner, AiSessionId(session.id), 0, 100)
        .await
        .expect("durable event catch-up loads");
    assert_eq!(events.watermark, 1);
    assert_eq!(events.events.len(), 1);
    assert_eq!(events.events[0].event_type, "message_queued");
    assert_eq!(
        events.events[0].payload.0["runId"],
        first.run_id.to_string()
    );
}

#[tokio::test]
async fn archive_restore_and_session_keyset_are_bounded() {
    let service = service().await;
    let owner = principal("owner");
    let first = service
        .create_session(
            &owner,
            CreateAiSessionInput {
                scope: scope_input(),
                title: Some("First".to_owned()),
            },
        )
        .await
        .expect("first session");
    let second = service
        .create_session(
            &owner,
            CreateAiSessionInput {
                scope: scope_input(),
                title: Some("Second".to_owned()),
            },
        )
        .await
        .expect("second session");

    let archived = service
        .archive_session(&owner, AiSessionId(first.id))
        .await
        .expect("archive uses CAS");
    assert_eq!(archived.state, "archived");
    assert!(matches!(
        service
            .send_message(
                &owner,
                SendAiMessageInput {
                    session_id: first.id,
                    text: "cannot send while archived".to_owned(),
                    attachment_ids: vec![],
                    client_message_id: Uuid::new_v4(),
                },
            )
            .await,
        Err(AiError::Conflict)
    ));
    let restored = service
        .restore_session(&owner, AiSessionId(first.id))
        .await
        .expect("restore uses CAS");
    assert_eq!(restored.state, "active");

    let page = service
        .sessions(
            &owner,
            KeysetConnectionInput {
                first: Some(1),
                include_total_count: true,
                ..Default::default()
            }
            .validate(10, 50)
            .expect("valid page"),
        )
        .await
        .expect("session page loads");
    assert_eq!(page.edges.len(), 1);
    assert!(page.page_info.has_next_page);
    assert!(page.page_info.total_count.is_none());

    let full_page = service
        .sessions(
            &owner,
            KeysetConnectionInput {
                first: Some(2),
                ..Default::default()
            }
            .validate(10, 50)
            .expect("valid full page"),
        )
        .await
        .expect("full session page loads");
    let hidden_id = full_page.edges[0].node.id;
    assert!(hidden_id == first.id || hidden_id == second.id);
    assert!(
        service
            .delete_session(&owner, AiSessionId(hidden_id))
            .await
            .expect("delete begins")
    );
    assert!(
        service
            .delete_session(&owner, AiSessionId(hidden_id))
            .await
            .expect("delete replay is idempotent")
    );
    assert!(
        service
            .session(&owner, AiSessionId(hidden_id))
            .await
            .expect("hidden lookup succeeds")
            .is_none()
    );
    let visible_page = service
        .sessions(
            &owner,
            KeysetConnectionInput {
                first: Some(1),
                ..Default::default()
            }
            .validate(10, 50)
            .expect("valid visible page"),
        )
        .await
        .expect("visible session page loads");
    assert_eq!(visible_page.edges.len(), 1);
    assert_ne!(visible_page.edges[0].node.id, hidden_id);
    assert!(!visible_page.page_info.has_next_page);
}
