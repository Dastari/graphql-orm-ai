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
    AiProvider, ModelRequest, ProviderCapabilities, ProviderError, ProviderEvent,
    ProviderEventStream, ProviderKind, ProviderRequestContext,
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
}
