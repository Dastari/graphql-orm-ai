//! Authenticated bounded provider-usage reporting contracts.

use std::sync::Arc;

use agql_auth::AuthPrincipal;
use async_graphql::{Context, ErrorExtensions, InputObject, Object, SimpleObject};
use async_trait::async_trait;
use graphql_orm::graphql::pagination::{
    KeysetConnectionInput, PageInfo, ValidatedKeysetConnection,
};
use uuid::Uuid;

use crate::{AiError, AiProviderKindInput, AiScope, AiScopeInput};

/// Maximum usage time interval accepted by one query.
const MAXIMUM_USAGE_INTERVAL_SECONDS: i64 = 366 * 24 * 60 * 60;

/// Host authorization result for one exact usage scope.
///
/// This is read authorization only. It does not grant provider execution,
/// budget configuration, or access to transcript/tool content.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AiUsageReadAccess {
    /// No usage rows may be returned.
    Denied,
    /// Only rows for the current exact principal kind/subject may be returned.
    OwnPrincipal,
    /// All rows in the exact authorized application scope may be returned.
    WholeScope,
}

/// Host-owned usage reporting authorization.
#[async_trait]
pub trait AiUsageAccessPolicy: Send + Sync {
    /// Resolves the caller's maximum read authority for one exact scope.
    async fn read_access(&self, principal: &AuthPrincipal, scope: &AiScope) -> AiUsageReadAccess;
}

/// Optional bounded usage filters.
#[derive(Clone, Debug, Default, InputObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiUsageFilterInput {
    /// Exact provider family.
    pub provider_kind: Option<AiProviderKindInput>,
    /// Exact provider model/logical model.
    pub provider_model: Option<String>,
    /// Inclusive Unix-second lower bound. Supply both time bounds together.
    pub created_from: Option<i64>,
    /// Exclusive Unix-second upper bound. Supply both time bounds together.
    pub created_to: Option<i64>,
}

/// Validated usage filter passed to a service implementation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AiUsageFilter {
    provider_kind: Option<String>,
    provider_model: Option<String>,
    created_from: Option<i64>,
    created_to: Option<i64>,
}

impl AiUsageFilter {
    fn validate(input: AiUsageFilterInput) -> Result<Self, AiError> {
        let raw_provider_model = input.provider_model;
        let provider_model = raw_provider_model
            .as_ref()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());
        if provider_model
            .as_ref()
            .is_some_and(|value| value.len() > 200)
            || raw_provider_model
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
            || input.created_from.is_some_and(|value| value < 0)
            || input.created_to.is_some_and(|value| value < 0)
            || input.created_from.is_some() != input.created_to.is_some()
            || matches!((input.created_from, input.created_to), (Some(from), Some(to))
                if from >= to || to.saturating_sub(from) > MAXIMUM_USAGE_INTERVAL_SECONDS)
            || input
                .created_from
                .is_some_and(|value| i32::try_from(value).is_err())
            || input
                .created_to
                .is_some_and(|value| i32::try_from(value).is_err())
        {
            return Err(AiError::InvalidInput("invalid AI usage filter".to_owned()));
        }
        Ok(Self {
            provider_kind: input.provider_kind.map(|kind| kind.as_str().to_owned()),
            provider_model,
            created_from: input.created_from,
            created_to: input.created_to,
        })
    }

    /// Stable provider kind filter.
    pub fn provider_kind(&self) -> Option<&str> {
        self.provider_kind.as_deref()
    }

    /// Exact provider model filter.
    pub fn provider_model(&self) -> Option<&str> {
        self.provider_model.as_deref()
    }

    /// Inclusive Unix-second lower bound. Both time bounds are required when
    /// either is supplied.
    pub const fn created_from(&self) -> Option<i64> {
        self.created_from
    }

    /// Exclusive Unix-second upper bound. Both time bounds are required when
    /// either is supplied.
    pub const fn created_to(&self) -> Option<i64> {
        self.created_to
    }
}

impl TryFrom<AiUsageFilterInput> for AiUsageFilter {
    type Error = AiError;

    fn try_from(value: AiUsageFilterInput) -> Result<Self, Self::Error> {
        Self::validate(value)
    }
}

/// One immutable, redacted provider-usage fact.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiUsageView {
    /// Usage fact ID.
    pub id: Uuid,
    /// Exact budget reservation settled by this fact.
    pub budget_reservation_id: Uuid,
    /// Scope kind captured at provider execution.
    pub scope_kind: String,
    /// Scope ID captured at provider execution.
    pub scope_id: String,
    /// Optional tenant boundary.
    pub tenant_id: Option<String>,
    /// Exact principal kind responsible for the call.
    pub principal_kind: String,
    /// Exact principal subject responsible for the call.
    pub principal_subject: String,
    /// Owning session, when applicable.
    pub session_id: Option<Uuid>,
    /// Owning run, when applicable.
    pub run_id: Option<Uuid>,
    /// Stable provider family.
    pub provider_kind: String,
    /// Exact provider or logical model.
    pub provider_model: String,
    /// Provider-reported total input tokens.
    pub input_tokens: i64,
    /// Provider-reported cached subset of input tokens.
    pub cached_input_tokens: i64,
    /// Provider-reported output tokens.
    pub output_tokens: i64,
    /// Authoritatively settled tool units.
    pub tool_units: i64,
    /// Authoritatively settled image units.
    pub image_units: i64,
    /// Authoritatively settled deployment-defined cost microunits.
    pub cost_microunits: Option<i64>,
    /// Commit time in Unix seconds.
    pub created_at: i64,
}

/// One usage keyset edge.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiUsageEdge {
    /// Immutable usage fact.
    pub node: AiUsageView,
    /// Opaque keyset cursor.
    pub cursor: String,
}

/// Bounded usage connection.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiUsageConnection {
    /// Bounded result edges.
    pub edges: Vec<AiUsageEdge>,
    /// Relay page metadata.
    pub page_info: PageInfo,
}

/// Authenticated usage reporting service.
#[async_trait]
pub trait AiUsageService: Send + Sync {
    /// Returns one bounded, stable usage window under current request authority.
    ///
    /// # Errors
    ///
    /// Returns an error for denied scope access, invalid/stale persistence,
    /// unsupported filters, or database failure.
    async fn usage(
        &self,
        principal: &AuthPrincipal,
        scope: AiScope,
        filter: AiUsageFilter,
        page: ValidatedKeysetConnection,
    ) -> Result<AiUsageConnection, AiError>;
}

/// Composable authenticated usage query root.
#[derive(Clone, Copy, Debug, Default)]
pub struct AiUsageQueryRoot;

#[cfg_attr(
    feature = "graphql-case-pascal",
    Object(rename_fields = "PascalCase", rename_args = "PascalCase")
)]
#[cfg_attr(not(feature = "graphql-case-pascal"), Object)]
impl AiUsageQueryRoot {
    /// Returns a bounded keyset window of immutable provider usage.
    async fn ai_usage(
        &self,
        context: &Context<'_>,
        scope: AiScopeInput,
        #[graphql(default)] filter: AiUsageFilterInput,
        #[graphql(default)] page: KeysetConnectionInput,
    ) -> async_graphql::Result<AiUsageConnection> {
        resolve_usage(context, scope, filter, page).await
    }
}

pub(crate) async fn resolve_usage(
    context: &Context<'_>,
    scope: AiScopeInput,
    filter: AiUsageFilterInput,
    page: KeysetConnectionInput,
) -> async_graphql::Result<AiUsageConnection> {
    let principal = agql_auth::principal_from_ctx(context)?;
    let filter = AiUsageFilter::try_from(filter).map_err(extend)?;
    let page = if page == KeysetConnectionInput::default() {
        KeysetConnectionInput {
            first: Some(50),
            ..Default::default()
        }
    } else {
        page
    };
    let page = page.validate(50, 200).map_err(|error| (&error).extend())?;
    usage_service(context)?
        .usage(&principal, scope.into(), filter, page)
        .await
        .map_err(extend)
}

fn usage_service(context: &Context<'_>) -> async_graphql::Result<Arc<dyn AiUsageService>> {
    context
        .data::<Arc<dyn AiUsageService>>()
        .cloned()
        .map_err(|_| {
            AiError::InvalidConfiguration("AI usage service is not installed".to_owned()).extend()
        })
}

fn extend(error: AiError) -> async_graphql::Error {
    error.extend()
}
