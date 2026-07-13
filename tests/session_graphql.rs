use std::sync::{Arc, Mutex};

use agql_auth::{AccessTokenMetadata, AuthPrincipal, AuthUser, SessionContext};
use async_graphql::{EmptySubscription, Request, Schema};
use async_trait::async_trait;
use graphql_orm::graphql::pagination::{
    KeysetWindowDirection, PageInfo, ValidatedKeysetConnection,
};
use graphql_orm_ai::*;
use serde_json::json;
use uuid::Uuid;

fn principal(subject: &str) -> AuthPrincipal {
    AuthPrincipal::User(AuthUser {
        user_id: subject.to_owned(),
        session_id: Uuid::new_v4(),
        roles: vec![],
        scopes: vec!["ai:chat".to_owned()],
        session: SessionContext::default(),
        token_claims: AccessTokenMetadata::default(),
    })
}

fn page_info() -> PageInfo {
    PageInfo {
        has_next_page: false,
        has_previous_page: false,
        start_cursor: None,
        end_cursor: None,
        total_count: None,
    }
}

#[derive(Default)]
struct RecordingSessionService {
    message_page: Mutex<Option<ValidatedKeysetConnection>>,
    principal_subject: Mutex<Option<String>>,
}

#[async_trait]
impl AiSessionService for RecordingSessionService {
    async fn sessions(
        &self,
        principal: &AuthPrincipal,
        _page: ValidatedKeysetConnection,
    ) -> Result<AiSessionConnection, AiError> {
        *self
            .principal_subject
            .lock()
            .expect("test mutex should not be poisoned") = Some(principal.subject().to_owned());
        Ok(AiSessionConnection {
            edges: vec![],
            page_info: page_info(),
        })
    }

    async fn session(
        &self,
        _principal: &AuthPrincipal,
        _session_id: AiSessionId,
    ) -> Result<Option<AiSessionView>, AiError> {
        Ok(None)
    }

    async fn messages(
        &self,
        principal: &AuthPrincipal,
        _session_id: AiSessionId,
        page: ValidatedKeysetConnection,
    ) -> Result<AiMessageConnection, AiError> {
        *self
            .message_page
            .lock()
            .expect("test mutex should not be poisoned") = Some(page);
        *self
            .principal_subject
            .lock()
            .expect("test mutex should not be poisoned") = Some(principal.subject().to_owned());
        Ok(AiMessageConnection {
            edges: vec![],
            page_info: page_info(),
        })
    }

    async fn message_blocks(
        &self,
        _principal: &AuthPrincipal,
        _message_id: Uuid,
        _after_block_index: Option<i64>,
        _first: i64,
    ) -> Result<Vec<AiMessageBlockView>, AiError> {
        Ok(vec![])
    }

    async fn session_event_page(
        &self,
        _principal: &AuthPrincipal,
        _session_id: AiSessionId,
        _after_sequence: i64,
        _first: i64,
    ) -> Result<AiSessionEventPage, AiError> {
        Ok(AiSessionEventPage {
            events: vec![],
            watermark: 0,
            has_more: false,
            reset_required: false,
        })
    }

    async fn create_session(
        &self,
        principal: &AuthPrincipal,
        input: CreateAiSessionInput,
    ) -> Result<AiSessionView, AiError> {
        Ok(AiSessionView {
            id: Uuid::new_v4(),
            scope_kind: input.scope.kind,
            scope_id: input.scope.id,
            title: input.title.unwrap_or_else(|| "New chat".to_owned()),
            state: "active".to_owned(),
            stream_head: 0,
            last_activity_at: 0,
            archived_at: None,
        })
        .inspect(|_| {
            *self
                .principal_subject
                .lock()
                .expect("test mutex should not be poisoned") = Some(principal.subject().to_owned());
        })
    }

    async fn archive_session(
        &self,
        _principal: &AuthPrincipal,
        _session_id: AiSessionId,
    ) -> Result<AiSessionView, AiError> {
        Err(AiError::NotFound)
    }

    async fn restore_session(
        &self,
        _principal: &AuthPrincipal,
        _session_id: AiSessionId,
    ) -> Result<AiSessionView, AiError> {
        Err(AiError::NotFound)
    }

    async fn delete_session(
        &self,
        _principal: &AuthPrincipal,
        _session_id: AiSessionId,
    ) -> Result<bool, AiError> {
        Ok(false)
    }

    async fn send_message(
        &self,
        _principal: &AuthPrincipal,
        _input: SendAiMessageInput,
    ) -> Result<SendAiMessagePayload, AiError> {
        Ok(SendAiMessagePayload {
            message_id: Uuid::new_v4(),
            run_id: Uuid::new_v4(),
        })
    }
}

fn schema(
    service: Arc<RecordingSessionService>,
) -> Schema<AiQueryRoot, AiMutationRoot, EmptySubscription> {
    Schema::build(AiQueryRoot, AiMutationRoot, EmptySubscription)
        .data(service as Arc<dyn AiSessionService>)
        .finish()
}

#[tokio::test]
async fn message_query_defaults_to_bounded_tail_and_passes_current_principal() {
    let service = Arc::new(RecordingSessionService::default());
    let schema = schema(service.clone());
    let session_id = Uuid::new_v4();
    let response = schema
        .execute(
            Request::new(format!(
                "{{ aiMessages(sessionId: \"{session_id}\") {{ edges {{ cursor }} pageInfo {{ hasNextPage }} }} }}"
            ))
            .data(principal("user-a")),
        )
        .await;

    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(
        response.data.into_json().expect("response JSON"),
        json!({
            "aiMessages": {
                "edges": [],
                "pageInfo": {"hasNextPage": false}
            }
        })
    );
    let page = service
        .message_page
        .lock()
        .expect("test mutex should not be poisoned")
        .clone()
        .expect("service should receive a page");
    assert_eq!(page.direction, KeysetWindowDirection::Backward);
    assert_eq!(page.limit, 50);
    assert_eq!(
        service
            .principal_subject
            .lock()
            .expect("test mutex should not be poisoned")
            .as_deref(),
        Some("user-a")
    );
}

#[tokio::test]
async fn session_roots_fail_closed_without_authentication() {
    let schema = schema(Arc::new(RecordingSessionService::default()));
    let response = schema.execute("{ aiSessions { edges { cursor } } }").await;

    assert_eq!(response.errors.len(), 1);
}

#[tokio::test]
async fn event_page_rejects_unbounded_requests_before_service_execution() {
    let service = Arc::new(RecordingSessionService::default());
    let schema = schema(service.clone());
    let response = schema
        .execute(
            Request::new(format!(
                "{{ aiSessionEventPage(sessionId: \"{}\", first: 501) {{ watermark }} }}",
                Uuid::new_v4()
            ))
            .data(principal("user-a")),
        )
        .await;

    assert_eq!(response.errors.len(), 1);
    assert_eq!(
        response.errors[0]
            .extensions
            .as_ref()
            .and_then(|extensions| extensions.get("code")),
        Some(&async_graphql::Value::from("AI_INVALID_INPUT"))
    );
}
