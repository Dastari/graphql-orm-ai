//! Built-in provider adapter implementations.

mod mock;

#[cfg(feature = "provider-anthropic")]
mod anthropic;

#[cfg(any(
    feature = "provider-openai",
    feature = "provider-xai",
    feature = "provider-openai-compatible"
))]
mod openai;

#[cfg(feature = "provider-openai-compatible")]
mod openai_compatible;

#[cfg(feature = "provider-xai")]
mod xai;

#[cfg(feature = "provider-ollama")]
mod ollama;

pub use mock::MockProvider;

#[cfg(feature = "provider-anthropic")]
pub use anthropic::{AnthropicProvider, AnthropicProviderConfig};

#[cfg(feature = "provider-openai")]
pub use openai::{OpenAiFileDeletionService, OpenAiProvider, OpenAiProviderConfig};

#[cfg(feature = "provider-openai-compatible")]
pub use openai_compatible::{
    OpenAiCompatibleCapabilities, OpenAiCompatibleProvider, OpenAiCompatibleProviderConfig,
};

#[cfg(feature = "provider-xai")]
pub use xai::{XAiProvider, XAiProviderConfig};

#[cfg(feature = "provider-ollama")]
pub use ollama::{OllamaProvider, OllamaProviderConfig};
