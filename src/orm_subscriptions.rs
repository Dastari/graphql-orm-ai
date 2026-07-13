//! ORM-backed catch-up-to-watermark durable subscriptions.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::sync::Arc;
use std::time::Duration;

use agql_auth::CurrentPrincipalResolver;
use async_trait::async_trait;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::{Instant, MissedTickBehavior};

use crate::{
    AiError, AiSessionEventEnvelope, AiSessionEventStream, AiSessionId, AiSessionService,
    AiSessionWakeup, AiSubscriptionService, OrmAiSessionService,
};

/// Durable subscription service. Broadcast events are commit-only wakeup hints;
/// every client item is re-read from protected durable storage.
pub struct OrmAiSubscriptionService {
    sessions: Arc<OrmAiSessionService>,
    principal_resolver: Arc<dyn CurrentPrincipalResolver>,
    reauthorization_interval: Duration,
    replay_page_size: i64,
}

impl OrmAiSubscriptionService {
    /// Creates a service with a 30-second reauthorization interval and bounded
    /// 100-event replay pages.
    pub fn new(
        sessions: Arc<OrmAiSessionService>,
        principal_resolver: Arc<dyn CurrentPrincipalResolver>,
    ) -> Self {
        Self {
            sessions,
            principal_resolver,
            reauthorization_interval: Duration::from_secs(30),
            replay_page_size: 100,
        }
    }

    /// Overrides the reauthorization interval. Zero is rejected when opening
    /// a stream.
    #[must_use]
    pub fn with_reauthorization_interval(mut self, interval: Duration) -> Self {
        self.reauthorization_interval = interval;
        self
    }

    /// Overrides the durable replay page size, bounded to 1..=500 when a stream
    /// opens.
    #[must_use]
    pub fn with_replay_page_size(mut self, page_size: i64) -> Self {
        self.replay_page_size = page_size;
        self
    }
}

#[async_trait]
impl AiSubscriptionService for OrmAiSubscriptionService {
    async fn session_events(
        &self,
        principal: agql_auth::AuthPrincipal,
        session_id: AiSessionId,
        after_sequence: i64,
    ) -> Result<AiSessionEventStream, AiError> {
        if after_sequence < 0
            || self.reauthorization_interval.is_zero()
            || !(1..=500).contains(&self.replay_page_size)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid AI subscription bounds".to_owned(),
            ));
        }
        let principal_reference = principal.reference();
        let mut wakeups = self
            .sessions
            .database()
            .ensure_event_sender::<AiSessionWakeup>()
            .subscribe();
        let sessions = self.sessions.clone();
        let resolver = self.principal_resolver.clone();
        let reauthorization_interval = self.reauthorization_interval;
        let replay_page_size = self.replay_page_size;

        Ok(Box::pin(async_stream::try_stream! {
            let mut current_principal = principal;
            let mut delivered_sequence = after_sequence;
            let mut replay_required = true;
            let mut reauthorize = tokio::time::interval_at(
                Instant::now() + reauthorization_interval,
                reauthorization_interval,
            );
            reauthorize.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                if replay_required {
                    let mut page = sessions
                        .session_event_page(
                            &current_principal,
                            session_id,
                            delivered_sequence,
                            replay_page_size,
                        )
                        .await?;
                    let target_watermark = page.watermark;
                    if target_watermark < delivered_sequence || page.reset_required {
                        yield AiSessionEventEnvelope {
                            event: None,
                            watermark: target_watermark,
                            reset_required: true,
                        };
                        return;
                    }

                    loop {
                        let mut crossed_watermark = false;
                        for event in page.events {
                            if event.sequence > target_watermark {
                                crossed_watermark = true;
                                break;
                            }
                            if event.sequence <= delivered_sequence {
                                continue;
                            }
                            delivered_sequence = event.sequence;
                            yield AiSessionEventEnvelope {
                                event: Some(event),
                                watermark: target_watermark,
                                reset_required: false,
                            };
                        }
                        if delivered_sequence >= target_watermark || crossed_watermark {
                            break;
                        }
                        if !page.has_more {
                            yield AiSessionEventEnvelope {
                                event: None,
                                watermark: target_watermark,
                                reset_required: true,
                            };
                            return;
                        }
                        page = sessions
                            .session_event_page(
                                &current_principal,
                                session_id,
                                delivered_sequence,
                                replay_page_size,
                            )
                            .await?;
                        if page.reset_required {
                            yield AiSessionEventEnvelope {
                                event: None,
                                watermark: target_watermark,
                                reset_required: true,
                            };
                            return;
                        }
                    }
                    replay_required = false;
                }

                let should_reauthorize = tokio::select! {
                    _ = reauthorize.tick() => Some(true),
                    wakeup = wakeups.recv() => {
                        match wakeup {
                            Ok(wakeup)
                                if wakeup.session_id == session_id.0
                                    && wakeup.sequence > delivered_sequence =>
                            {
                                replay_required = true;
                            }
                            Ok(_) => {}
                            Err(RecvError::Lagged(_)) => replay_required = true,
                            Err(RecvError::Closed) => return,
                        }
                        Some(false)
                    }
                };
                if should_reauthorize == Some(true) {
                    let resolved = resolver
                        .resolve(&principal_reference)
                        .await
                        .map_err(|_| AiError::ReauthorizationFailed)?;
                    current_principal = resolved.into_principal();
                    if sessions
                        .session(&current_principal, session_id)
                        .await?
                        .is_none()
                    {
                        Err(AiError::Forbidden)?;
                    }
                }
            }
        }))
    }
}
