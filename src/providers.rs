//! Built-in provider adapter implementations.

mod mock;

#[cfg(feature = "provider-openai")]
mod openai;

pub use mock::MockProvider;

#[cfg(feature = "provider-openai")]
pub use openai::{OpenAiProvider, OpenAiProviderConfig};
