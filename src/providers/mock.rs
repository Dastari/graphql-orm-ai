//! Deterministic provider used for orchestration and security tests.

#[cfg(test)]
use std::collections::VecDeque;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use futures::stream;

use crate::{
    AiProvider, ModelRequest, ProviderBackgroundBinding, ProviderBackgroundSubmission,
    ProviderCapabilities, ProviderError, ProviderEvent, ProviderEventStream, ProviderKind,
    ProviderRequestContext,
};

#[cfg(test)]
type MockEventBatches = Arc<Mutex<VecDeque<Arc<[ProviderEvent]>>>>;

/// Deterministic, network-free provider fixture.
#[derive(Clone, Debug)]
pub struct MockProvider {
    kind: ProviderKind,
    capabilities: ProviderCapabilities,
    events: Arc<[ProviderEvent]>,
    #[cfg(test)]
    event_batches: Option<MockEventBatches>,
    #[cfg(test)]
    background_submission: Option<(String, String, i64)>,
    #[cfg(test)]
    background_delay: Option<std::time::Duration>,
    #[cfg(test)]
    background_binding: Option<(String, u64, bool)>,
    request_count: Arc<AtomicU64>,
}

impl MockProvider {
    /// Creates a local mock provider yielding the supplied events in order.
    pub fn new(events: impl Into<Vec<ProviderEvent>>) -> Self {
        Self {
            kind: ProviderKind::OpenAiCompatible,
            capabilities: ProviderCapabilities {
                streaming: true,
                custom_tools: true,
                structured_output: true,
                provider_retained_continuation: true,
                local: true,
                ..ProviderCapabilities::default()
            },
            events: events.into().into(),
            #[cfg(test)]
            event_batches: None,
            #[cfg(test)]
            background_submission: None,
            #[cfg(test)]
            background_delay: None,
            #[cfg(test)]
            background_binding: None,
            request_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Changes the provider family exposed by the fixture.
    pub fn with_kind(mut self, kind: ProviderKind) -> Self {
        self.kind = kind;
        self
    }

    /// Changes the declared capabilities exposed by the fixture.
    pub fn with_capabilities(mut self, capabilities: ProviderCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Returns the number of accepted requests without retaining prompt data.
    pub fn request_count(&self) -> u64 {
        self.request_count.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn with_event_batches(
        mut self,
        batches: impl IntoIterator<Item = Vec<ProviderEvent>>,
    ) -> Self {
        self.event_batches = Some(Arc::new(Mutex::new(
            batches
                .into_iter()
                .map(Arc::<[ProviderEvent]>::from)
                .collect(),
        )));
        self
    }

    #[cfg(test)]
    pub(crate) fn with_background_submission(
        mut self,
        response_id: impl Into<String>,
        status: impl Into<String>,
        created_at: i64,
    ) -> Self {
        self.background_submission = Some((response_id.into(), status.into(), created_at));
        self
    }

    #[cfg(test)]
    pub(crate) fn with_background_delay(mut self, delay: std::time::Duration) -> Self {
        self.background_delay = Some(delay);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_background_binding(
        mut self,
        provider_model: impl Into<String>,
        maximum_output_tokens: u64,
        provider_store: bool,
    ) -> Self {
        self.background_binding =
            Some((provider_model.into(), maximum_output_tokens, provider_store));
        self
    }
}

#[async_trait]
impl AiProvider for MockProvider {
    fn provider_kind(&self) -> ProviderKind {
        self.kind.clone()
    }

    fn capabilities(&self) -> ProviderCapabilities {
        self.capabilities.clone()
    }

    async fn stream(
        &self,
        request: ModelRequest,
        context: ProviderRequestContext,
    ) -> Result<ProviderEventStream, ProviderError> {
        context.validate_request(&self.kind, &request)?;
        self.request_count.fetch_add(1, Ordering::AcqRel);
        #[cfg(test)]
        let events = match &self.event_batches {
            Some(batches) => batches
                .lock()
                .map_err(|_| ProviderError::Unavailable)?
                .pop_front()
                .ok_or(ProviderError::Unavailable)?,
            None => self.events.clone(),
        };
        #[cfg(not(test))]
        let events = self.events.clone();
        Ok(Box::pin(stream::iter(
            events.iter().cloned().map(Ok).collect::<Vec<_>>(),
        )))
    }

    async fn submit_background(
        &self,
        request: ModelRequest,
        context: ProviderRequestContext,
        binding: ProviderBackgroundBinding,
    ) -> Result<ProviderBackgroundSubmission, ProviderError> {
        context.validate_request(&self.kind, &request)?;
        if binding.submission_key().is_empty() || binding.provider_profile_id().is_empty() {
            return Err(ProviderError::Rejected);
        }
        #[cfg(test)]
        {
            if let Some(delay) = self.background_delay {
                tokio::time::sleep(delay).await;
            }
            let (response_id, status, created_at) = self
                .background_submission
                .clone()
                .ok_or(ProviderError::Unsupported)?;
            let maximum_output_tokens = request
                .maximum_output_tokens
                .ok_or(ProviderError::InvalidRequest)?;
            let (provider_model, maximum_output_tokens, provider_store) = self
                .background_binding
                .clone()
                .unwrap_or((request.model, maximum_output_tokens, false));
            self.request_count.fetch_add(1, Ordering::AcqRel);
            Ok(ProviderBackgroundSubmission::new(
                response_id,
                status,
                created_at,
                provider_model,
                maximum_output_tokens,
                provider_store,
            ))
        }
        #[cfg(not(test))]
        {
            let _ = binding;
            Err(ProviderError::Unsupported)
        }
    }
}
