//! Built-in provider adapter implementations.

mod mock;

#[cfg(feature = "provider-anthropic")]
mod anthropic;

#[cfg(feature = "provider-openai")]
mod openai;

#[cfg(feature = "provider-ollama")]
mod ollama;

pub use mock::MockProvider;

#[cfg(feature = "provider-anthropic")]
pub use anthropic::{AnthropicProvider, AnthropicProviderConfig};

#[cfg(feature = "provider-openai")]
pub use openai::{OpenAiProvider, OpenAiProviderConfig};

#[cfg(feature = "provider-ollama")]
pub use ollama::{OllamaProvider, OllamaProviderConfig};
