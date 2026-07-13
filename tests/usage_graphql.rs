use std::sync::{Arc, Mutex};

use agql_auth::{AccessTokenMetadata, AuthPrincipal, AuthUser, SessionContext};
use async_graphql::{EmptySubscription, Request, Schema};
use async_trait::async_trait;
use graphql_orm::graphql::pagination::{PageInfo, ValidatedKeysetConnection};
use graphql_orm_ai::{
    AiError, AiMutationRoot, AiQueryRoot, AiScope, AiUsageConnection, AiUsageFilter, AiUsageService,
};
use uuid::Uuid;

#[derive(Default)]
struct RecordingUsageService {
    request: Mutex<Option<(String, AiScope, AiUsageFilter, ValidatedKeysetConnection)>>,
}

#[async_trait]
impl AiUsageService for RecordingUsageService {
    async fn usage(
        &self,
        principal: &AuthPrincipal,
        scope: AiScope,
        filter: AiUsageFilter,
        page: ValidatedKeysetConnection,
    ) -> Result<AiUsageConnection, AiError> {
        *self.request.lock().expect("request lock should be healthy") =
            Some((principal.subject().to_owned(), scope, filter, page));
        Ok(AiUsageConnection {
            edges: vec![],
            page_info: PageInfo::default(),
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
        token_claims: AccessTokenMetadata::default(),
    })
}

fn schema(
    service: Arc<RecordingUsageService>,
) -> Schema<AiQueryRoot, AiMutationRoot, EmptySubscription> {
    Schema::build(AiQueryRoot, AiMutationRoot, EmptySubscription)
        .data(service as Arc<dyn AiUsageService>)
        .finish()
}

#[tokio::test]
async fn usage_query_is_authenticated_bounded_and_passes_exact_scope_and_filters() {
    let service = Arc::new(RecordingUsageService::default());
    let schema = schema(service.clone());
    #[cfg(not(feature = "graphql-case-pascal"))]
    let query = r#"{
        aiUsage(
            scope: { kind: "project", id: "project-7", tenantId: "tenant-3" }
            filter: { providerModel: "gpt-test" }
        ) { edges { cursor } pageInfo { hasNextPage } }
    }"#;
    #[cfg(feature = "graphql-case-pascal")]
    let query = r#"{
        AiUsage(
            Scope: { Kind: "project", Id: "project-7", TenantId: "tenant-3" }
            Filter: { ProviderModel: "gpt-test" }
        ) { Edges { Cursor } PageInfo { HasNextPage } }
    }"#;
    let response = schema
        .execute(Request::new(query).data(principal("usage-user")))
        .await;

    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let request = service
        .request
        .lock()
        .expect("request lock should be healthy")
        .clone()
        .expect("usage service should be called");
    assert_eq!(request.0, "usage-user");
    assert_eq!(request.1.kind, "project");
    assert_eq!(request.1.id, "project-7");
    assert_eq!(request.1.tenant_id.as_deref(), Some("tenant-3"));
    assert_eq!(request.2.provider_model(), Some("gpt-test"));
    assert_eq!(request.3.limit, 50);
}

#[tokio::test]
async fn usage_query_fails_closed_without_auth_service_or_valid_filter() {
    let service = Arc::new(RecordingUsageService::default());
    let schema = schema(service.clone());
    #[cfg(not(feature = "graphql-case-pascal"))]
    let basic_query = "{ aiUsage(scope: { kind: \"project\", id: \"one\" }) { edges { cursor } } }";
    #[cfg(feature = "graphql-case-pascal")]
    let basic_query = "{ AiUsage(Scope: { Kind: \"project\", Id: \"one\" }) { Edges { Cursor } } }";
    #[cfg(not(feature = "graphql-case-pascal"))]
    let invalid_query = "{ aiUsage(scope: { kind: \"project\", id: \"one\" }, filter: { providerModel: \" \" }) { edges { cursor } } }";
    #[cfg(feature = "graphql-case-pascal")]
    let invalid_query = "{ AiUsage(Scope: { Kind: \"project\", Id: \"one\" }, Filter: { ProviderModel: \" \" }) { Edges { Cursor } } }";
    let unauthenticated = schema.execute(basic_query).await;
    assert_eq!(unauthenticated.errors.len(), 1);

    let invalid = schema
        .execute(Request::new(invalid_query).data(principal("usage-user")))
        .await;
    assert_eq!(invalid.errors.len(), 1);
    assert!(
        service
            .request
            .lock()
            .expect("request lock should be healthy")
            .is_none()
    );

    let missing_service = Schema::build(AiQueryRoot, AiMutationRoot, EmptySubscription)
        .finish()
        .execute(Request::new(basic_query).data(principal("usage-user")))
        .await;
    assert_eq!(missing_service.errors.len(), 1);
}
