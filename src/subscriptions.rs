//! Resumable durable-session subscription GraphQL contract.

use std::pin::Pin;
use std::sync::Arc;

use agql_auth::AuthPrincipal;
use async_graphql::{Context, ErrorExtensions, SimpleObject, Subscription};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use uuid::Uuid;

use crate::{AiError, AiSessionEventView, AiSessionId};

/// Commit-only wakeup hint. The durable event table remains the source of
/// truth; consumers never deliver this value directly to clients.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiSessionWakeup {
    /// Session whose durable stream advanced.
    pub session_id: Uuid,
    /// Sequence observed in the committing transaction.
    pub sequence: i64,
}

/// Subscription item supporting explicit retention-gap reset signaling.
#[derive(Clone, Debug, SimpleObject)]
#[cfg_attr(feature = "graphql-case-pascal", graphql(rename_fields = "PascalCase"))]
pub struct AiSessionEventEnvelope {
    /// Durable event, absent only for a reset signal.
    pub event: Option<AiSessionEventView>,
    /// Replay watermark associated with this delivery.
    pub watermark: i64,
    /// Whether retention removed required history and the client must reload.
    pub reset_required: bool,
}

/// Type-erased bounded event stream.
pub type AiSessionEventStream =
    Pin<Box<dyn Stream<Item = Result<AiSessionEventEnvelope, AiError>> + Send>>;

/// Backend for catch-up-to-watermark plus live durable subscriptions.
#[async_trait]
pub trait AiSubscriptionService: Send + Sync {
    /// Starts after an exclusive durable sequence.
    async fn session_events(
        &self,
        principal: AuthPrincipal,
        session_id: AiSessionId,
        after_sequence: i64,
    ) -> Result<AiSessionEventStream, AiError>;
}

/// Composable AI subscription root.
#[derive(Clone, Copy, Debug, Default)]
pub struct AiSubscriptionRoot;

#[cfg_attr(
    feature = "graphql-case-pascal",
    Subscription(rename_fields = "PascalCase", rename_args = "PascalCase")
)]
#[cfg_attr(not(feature = "graphql-case-pascal"), Subscription)]
impl AiSubscriptionRoot {
    /// Replays durable events, then follows commit-only wakeup hints while
    /// periodically reauthorizing the principal.
    async fn ai_session_events(
        &self,
        context: &Context<'_>,
        session_id: Uuid,
        after_sequence: Option<i64>,
    ) -> async_graphql::Result<
        Pin<Box<dyn Stream<Item = async_graphql::Result<AiSessionEventEnvelope>> + Send>>,
    > {
        let after_sequence = after_sequence.unwrap_or(0);
        if after_sequence < 0 {
            return Err(AiError::InvalidInput("invalid event sequence".to_owned()).extend());
        }
        let principal = agql_auth::principal_from_ctx(context)?;
        let stream = subscription_service(context)?
            .session_events(principal, AiSessionId(session_id), after_sequence)
            .await
            .map_err(|error| error.extend())?;
        Ok(Box::pin(
            stream.map(|item| item.map_err(|error| error.extend())),
        ))
    }
}

fn subscription_service(
    context: &Context<'_>,
) -> async_graphql::Result<Arc<dyn AiSubscriptionService>> {
    context
        .data_opt::<Arc<dyn AiSubscriptionService>>()
        .cloned()
        .ok_or_else(|| {
            AiError::InvalidConfiguration("AI subscription service is missing".to_owned()).extend()
        })
}
