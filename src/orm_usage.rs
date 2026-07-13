//! ORM-backed authenticated usage reporting.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::sync::Arc;

use agql_auth::AuthPrincipal;
use async_trait::async_trait;
use graphql_orm::db::Database;
use graphql_orm::graphql::errors::OrmPublicError;
use graphql_orm::graphql::filters::{IntFilter, StringFilter};
use graphql_orm::graphql::orm::DefaultWriteBackend;
use graphql_orm::graphql::pagination::{
    KeysetConnectionInput, KeysetWindowDirection, ValidatedKeysetConnection,
};

use crate::persistence::*;
use crate::{
    AiError, AiScope, AiUsageAccessPolicy, AiUsageConnection, AiUsageEdge, AiUsageFilter,
    AiUsageReadAccess, AiUsageService, AiUsageView,
};

/// ORM-backed bounded provider-usage reporting.
///
/// The service exposes only append-oriented usage facts and never raw provider
/// payloads, transcript content, tool arguments/results, pricing secrets, or
/// reservation counter internals. Host policy chooses current-principal-only
/// or exact-scope visibility for each request.
pub struct OrmAiUsageService {
    database: Database<DefaultWriteBackend>,
    access_policy: Arc<dyn AiUsageAccessPolicy>,
}

impl OrmAiUsageService {
    /// Creates an authenticated usage service over the AI ORM database.
    pub fn new(
        database: Database<DefaultWriteBackend>,
        access_policy: Arc<dyn AiUsageAccessPolicy>,
    ) -> Self {
        Self {
            database,
            access_policy,
        }
    }
}

#[async_trait]
impl AiUsageService for OrmAiUsageService {
    async fn usage(
        &self,
        principal: &AuthPrincipal,
        scope: AiScope,
        filter: AiUsageFilter,
        page: ValidatedKeysetConnection,
    ) -> Result<AiUsageConnection, AiError> {
        validate_scope(&scope)?;
        if let Some(tenant_id) = principal.tenant_id()
            && scope.tenant_id.as_deref() != Some(tenant_id)
        {
            return Err(AiError::Forbidden);
        }
        let access = self.access_policy.read_access(principal, &scope).await;
        if access == AiUsageReadAccess::Denied {
            return Err(AiError::Forbidden);
        }
        let (principal_kind, principal_subject) = principal_identity(principal);
        let created_at =
            match (filter.created_from(), filter.created_to()) {
                (None, None) => None,
                (from, to) => Some(IntFilter {
                    gte: from.map(i32::try_from).transpose().map_err(|_| {
                        AiError::InvalidInput("invalid AI usage time bound".to_owned())
                    })?,
                    lt: to.map(i32::try_from).transpose().map_err(|_| {
                        AiError::InvalidInput("invalid AI usage time bound".to_owned())
                    })?,
                    ..Default::default()
                }),
            };
        let connection = AiUsageEntryRecord::keyset_connection_page(
            &self.database,
            AiUsageEntryRecordWhereInput {
                scope_kind: Some(StringFilter {
                    eq: Some(scope.kind),
                    ..Default::default()
                }),
                scope_id: Some(StringFilter {
                    eq: Some(scope.id),
                    ..Default::default()
                }),
                tenant_id: Some(match scope.tenant_id {
                    Some(tenant_id) => StringFilter {
                        eq: Some(tenant_id),
                        ..Default::default()
                    },
                    None => StringFilter {
                        is_null: Some(true),
                        ..Default::default()
                    },
                }),
                principal_kind: (access == AiUsageReadAccess::OwnPrincipal).then(|| StringFilter {
                    eq: Some(principal_kind),
                    ..Default::default()
                }),
                principal_subject: (access == AiUsageReadAccess::OwnPrincipal).then(|| {
                    StringFilter {
                        eq: Some(principal_subject.to_owned()),
                        ..Default::default()
                    }
                }),
                provider_kind: filter.provider_kind().map(|value| StringFilter {
                    eq: Some(value.to_owned()),
                    ..Default::default()
                }),
                provider_model: filter.provider_model().map(|value| StringFilter {
                    eq: Some(value.to_owned()),
                    ..Default::default()
                }),
                created_at,
                ..Default::default()
            },
            page_input(&page),
        )
        .await
        .map_err(map_orm)?;
        Ok(AiUsageConnection {
            edges: connection
                .edges
                .into_iter()
                .map(|edge| AiUsageEdge {
                    node: usage_view(edge.node),
                    cursor: edge.cursor,
                })
                .collect(),
            page_info: connection.page_info,
        })
    }
}

fn usage_view(record: AiUsageEntryRecord) -> AiUsageView {
    AiUsageView {
        id: record.id,
        budget_reservation_id: record.budget_reservation_id,
        scope_kind: record.scope_kind,
        scope_id: record.scope_id,
        tenant_id: record.tenant_id,
        principal_kind: record.principal_kind,
        principal_subject: record.principal_subject,
        session_id: record.session_id,
        run_id: record.run_id,
        provider_kind: record.provider_kind,
        provider_model: record.provider_model,
        input_tokens: record.input_tokens,
        cached_input_tokens: record.cached_input_tokens,
        output_tokens: record.output_tokens,
        tool_units: record.tool_units,
        image_units: record.image_units,
        cost_microunits: record.cost_microunits,
        created_at: record.created_at,
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

fn validate_scope(scope: &AiScope) -> Result<(), AiError> {
    if scope.kind.trim().is_empty()
        || scope.kind.len() > 128
        || scope.id.trim().is_empty()
        || scope.id.len() > 512
        || scope
            .tenant_id
            .as_ref()
            .is_some_and(|tenant| tenant.trim().is_empty() || tenant.len() > 512)
    {
        return Err(AiError::InvalidInput("invalid AI usage scope".to_owned()));
    }
    Ok(())
}

fn principal_identity(principal: &AuthPrincipal) -> (String, &str) {
    match principal {
        AuthPrincipal::User(user) => ("user".to_owned(), &user.user_id),
        AuthPrincipal::ApiToken(token) => (
            format!("api_token:{}", token.principal_kind.as_str()),
            &token.subject,
        ),
    }
}

fn map_orm(error: impl Into<OrmPublicError>) -> AiError {
    let error = error.into();
    match error.code {
        graphql_orm::graphql::errors::OrmErrorCode::Forbidden => AiError::Forbidden,
        graphql_orm::graphql::errors::OrmErrorCode::NotFound => AiError::NotFound,
        graphql_orm::graphql::errors::OrmErrorCode::Conflict => AiError::Conflict,
        _ => AiError::PersistenceFailed,
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use agql_auth::{AccessTokenMetadata, AuthUser, SessionContext};
    use graphql_orm::graphql::orm::{ApplyOptions, OrmSchemaModule};
    use graphql_orm::prelude::SqliteBackend;
    use uuid::Uuid;

    #[derive(Clone, Copy)]
    struct FixedAccess(AiUsageReadAccess);

    #[async_trait]
    impl AiUsageAccessPolicy for FixedAccess {
        async fn read_access(
            &self,
            _principal: &AuthPrincipal,
            _scope: &AiScope,
        ) -> AiUsageReadAccess {
            self.0
        }
    }

    async fn database() -> Database<SqliteBackend> {
        let database = Database::<SqliteBackend>::connect_sqlite("sqlite::memory:")
            .await
            .expect("in-memory SQLite should open");
        let module = crate::AiSchemaModule;
        let plan = database
            .schema()
            .plan_migration_to_entities(
                "ai-usage-test-v1",
                "AI usage service test",
                module.entities(),
            )
            .await
            .expect("AI usage schema should plan");
        database
            .schema()
            .apply_migration(&plan, ApplyOptions::default())
            .await
            .expect("AI usage schema should apply");
        database
    }

    fn principal(subject: &str) -> AuthPrincipal {
        AuthPrincipal::User(AuthUser {
            user_id: subject.to_owned(),
            session_id: Uuid::new_v4(),
            roles: vec![],
            scopes: vec![],
            session: SessionContext::default(),
            token_claims: AccessTokenMetadata {
                tenant_id: Some("tenant-a".to_owned()),
                ..AccessTokenMetadata::default()
            },
        })
    }

    async fn seed(
        database: &Database<SqliteBackend>,
        subject: &str,
        scope_id: &str,
        tenant_id: &str,
        model: &str,
    ) -> Uuid {
        let id = Uuid::new_v4();
        AiUsageEntryRecord::insert(
            database,
            CreateAiUsageEntryRecordInput {
                id,
                budget_reservation_id: Uuid::new_v4(),
                scope_kind: "tenant".to_owned(),
                scope_id: scope_id.to_owned(),
                tenant_id: Some(tenant_id.to_owned()),
                principal_kind: "user".to_owned(),
                principal_subject: subject.to_owned(),
                session_id: None,
                run_id: None,
                provider_kind: "openai".to_owned(),
                provider_model: model.to_owned(),
                input_tokens: 10,
                cached_input_tokens: 2,
                output_tokens: 3,
                tool_units: 0,
                image_units: 0,
                cost_microunits: Some(7),
            },
        )
        .await
        .expect("usage fact should seed through generated ORM operations");
        id
    }

    fn page(first: i64, after: Option<String>) -> ValidatedKeysetConnection {
        KeysetConnectionInput {
            first: Some(first),
            after,
            ..Default::default()
        }
        .validate(50, 200)
        .expect("test page should validate")
    }

    #[tokio::test]
    async fn current_principal_and_scope_filters_are_enforced_before_pagination() {
        let database = database().await;
        let own_id = seed(&database, "alice", "tenant-a", "tenant-a", "model-a").await;
        seed(&database, "bob", "tenant-a", "tenant-a", "model-b").await;
        seed(&database, "alice", "tenant-b", "tenant-b", "model-a").await;

        let own = OrmAiUsageService::new(
            database.clone(),
            Arc::new(FixedAccess(AiUsageReadAccess::OwnPrincipal)),
        );
        let connection = own
            .usage(
                &principal("alice"),
                AiScope::new("tenant", "tenant-a").with_tenant_id("tenant-a"),
                AiUsageFilter::default(),
                page(10, None),
            )
            .await
            .expect("own usage should be readable");
        assert_eq!(connection.edges.len(), 1);
        assert_eq!(connection.edges[0].node.id, own_id);
        assert_eq!(connection.edges[0].node.principal_subject, "alice");

        let all = OrmAiUsageService::new(
            database,
            Arc::new(FixedAccess(AiUsageReadAccess::WholeScope)),
        );
        let connection = all
            .usage(
                &principal("alice"),
                AiScope::new("tenant", "tenant-a").with_tenant_id("tenant-a"),
                AiUsageFilter::default(),
                page(10, None),
            )
            .await
            .expect("whole-scope usage should be readable");
        assert_eq!(connection.edges.len(), 2);
        assert!(matches!(
            all.usage(
                &principal("alice"),
                AiScope::new("tenant", "tenant-b").with_tenant_id("tenant-b"),
                AiUsageFilter::default(),
                page(10, None),
            )
            .await,
            Err(AiError::Forbidden)
        ));
    }

    #[tokio::test]
    async fn usage_filters_keysets_and_denials_are_bounded() {
        let database = database().await;
        seed(&database, "alice", "tenant-a", "tenant-a", "model-a").await;
        seed(&database, "alice", "tenant-a", "tenant-a", "model-b").await;
        let scope = AiScope::new("tenant", "tenant-a").with_tenant_id("tenant-a");
        let principal = principal("alice");
        let service = OrmAiUsageService::new(
            database.clone(),
            Arc::new(FixedAccess(AiUsageReadAccess::WholeScope)),
        );

        let first = service
            .usage(
                &principal,
                scope.clone(),
                AiUsageFilter::default(),
                page(1, None),
            )
            .await
            .expect("first usage page should load");
        assert_eq!(first.edges.len(), 1);
        let second = service
            .usage(
                &principal,
                scope.clone(),
                AiUsageFilter::default(),
                page(1, Some(first.edges[0].cursor.clone())),
            )
            .await
            .expect("second usage page should load");
        assert_eq!(second.edges.len(), 1);
        assert_ne!(first.edges[0].node.id, second.edges[0].node.id);

        let filtered = service
            .usage(
                &principal,
                scope.clone(),
                AiUsageFilter::try_from(crate::AiUsageFilterInput {
                    provider_model: Some("model-b".to_owned()),
                    ..Default::default()
                })
                .expect("model filter should validate"),
                page(10, None),
            )
            .await
            .expect("filtered usage should load");
        assert_eq!(filtered.edges.len(), 1);
        assert_eq!(filtered.edges[0].node.provider_model, "model-b");

        let denied =
            OrmAiUsageService::new(database, Arc::new(FixedAccess(AiUsageReadAccess::Denied)));
        assert!(matches!(
            denied
                .usage(&principal, scope, AiUsageFilter::default(), page(10, None))
                .await,
            Err(AiError::Forbidden)
        ));
    }

    #[test]
    fn invalid_or_unbounded_usage_filters_fail_closed() {
        assert!(
            AiUsageFilter::try_from(crate::AiUsageFilterInput {
                provider_model: Some("  ".to_owned()),
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            AiUsageFilter::try_from(crate::AiUsageFilterInput {
                created_from: Some(1),
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            AiUsageFilter::try_from(crate::AiUsageFilterInput {
                created_from: Some(1),
                created_to: Some(367 * 24 * 60 * 60),
                ..Default::default()
            })
            .is_err()
        );
    }
}
