//! Deterministic provider used for orchestration and security tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use futures::stream;

use crate::{
    AiProvider, ModelRequest, ProviderCapabilities, ProviderError, ProviderEvent,
    ProviderEventStream, ProviderKind, ProviderRequestContext,
};

/// Deterministic, network-free provider fixture.
#[derive(Clone, Debug)]
pub struct MockProvider {
    kind: ProviderKind,
    capabilities: ProviderCapabilities,
    events: Arc<[ProviderEvent]>,
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
        let events = self.events.clone();
        Ok(Box::pin(stream::iter(
            events.iter().cloned().map(Ok).collect::<Vec<_>>(),
        )))
    }
}
