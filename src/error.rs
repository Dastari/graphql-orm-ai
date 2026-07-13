//! Public error contract.

use async_graphql::ErrorExtensions;
use thiserror::Error;

/// Stable library error.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AiError {
    /// Configuration is invalid.
    #[error("invalid AI configuration: {0}")]
    InvalidConfiguration(String),
    /// Requested item already exists.
    #[error("AI resource already exists: {0}")]
    AlreadyExists(String),
    /// Requested item was not found or is not visible.
    #[error("AI resource not found")]
    NotFound,
    /// Current state changed or an idempotency/CAS precondition failed.
    #[error("AI operation conflicts with current state")]
    Conflict,
    /// Current principal is not authorized.
    #[error("AI operation forbidden")]
    Forbidden,
    /// A configuration operation requires current host-accepted MFA.
    #[error("additional authentication is required")]
    RecentMfaRequired,
    /// External disclosure was denied.
    #[error("AI data egress denied")]
    EgressDenied,
    /// Input failed a public schema contract.
    #[error("invalid AI input: {0}")]
    InvalidInput(String),
    /// Authentication dependency failed closed.
    #[error("AI principal reauthorization failed")]
    ReauthorizationFailed,
    /// Host GraphQL execution failed safely.
    #[error("AI tool execution failed")]
    ToolExecutionFailed,
    /// Provider operation failed safely.
    #[error("AI provider operation failed")]
    ProviderFailed,
    /// Runtime has not passed startup/restore readiness checks.
    #[error("AI runtime is not ready")]
    RuntimeNotReady,
    /// Durable persistence is temporarily unavailable or failed safely.
    #[error("AI persistence operation failed")]
    PersistenceFailed,
}

impl AiError {
    /// Stable public error code.
    pub const fn public_code(&self) -> &'static str {
        match self {
            Self::InvalidConfiguration(_) => "AI_INVALID_CONFIGURATION",
            Self::AlreadyExists(_) => "AI_ALREADY_EXISTS",
            Self::NotFound => "AI_NOT_FOUND",
            Self::Conflict => "AI_CONFLICT",
            Self::Forbidden => "AI_FORBIDDEN",
            Self::RecentMfaRequired => "AI_RECENT_MFA_REQUIRED",
            Self::EgressDenied => "AI_EGRESS_DENIED",
            Self::InvalidInput(_) => "AI_INVALID_INPUT",
            Self::ReauthorizationFailed => "AI_REAUTHORIZATION_FAILED",
            Self::ToolExecutionFailed => "AI_TOOL_EXECUTION_FAILED",
            Self::ProviderFailed => "AI_PROVIDER_FAILED",
            Self::RuntimeNotReady => "AI_RUNTIME_NOT_READY",
            Self::PersistenceFailed => "AI_PERSISTENCE_FAILED",
        }
    }
}

impl ErrorExtensions for AiError {
    fn extend(&self) -> async_graphql::Error {
        async_graphql::Error::new(self.to_string()).extend_with(|_, extensions| {
            extensions.set("code", self.public_code());
        })
    }
}
