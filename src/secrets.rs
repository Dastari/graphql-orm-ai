//! Provider credential indirection and secret-store contracts.
//!
//! Durable records contain only [`SecretRef`] values. Secret plaintext is
//! deliberately non-serializable and is resolved immediately before a remote
//! request so credential rotation does not require rewriting provider rows.

use std::collections::BTreeMap;

use async_trait::async_trait;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Opaque, non-secret reference to provider credentials or key material.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretRef(String);

impl SecretRef {
    /// Parses a bounded reference suitable for persistence and audit metadata.
    ///
    /// # Errors
    ///
    /// Returns [`SecretError::InvalidReference`] when the value is empty,
    /// unreasonably long, or contains characters outside the stable reference
    /// alphabet.
    pub fn parse(value: impl Into<String>) -> Result<Self, SecretError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 200
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
            });
        if !valid {
            return Err(SecretError::InvalidReference);
        }
        Ok(Self(value))
    }

    /// Returns the non-secret opaque reference.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretRef {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_tuple("SecretRef").field(&self.0).finish()
    }
}

/// Secret-store failure with no plaintext or backend diagnostic exposure.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum SecretError {
    /// A reference is malformed.
    #[error("invalid secret reference")]
    InvalidReference,
    /// The referenced secret does not exist or is not visible.
    #[error("secret unavailable")]
    Unavailable,
    /// This store is read-only.
    #[error("secret store is read-only")]
    ReadOnly,
    /// The external secret backend failed closed.
    #[error("secret store temporarily unavailable")]
    BackendUnavailable,
}

/// Secret storage abstraction for encrypted ORM stores, KMS/Vault adapters,
/// and read-only deployment bootstrap sources.
#[async_trait]
pub trait AiSecretStore: Send + Sync {
    /// Resolves current secret plaintext. Implementations must not log values.
    async fn resolve(&self, reference: &SecretRef) -> Result<SecretString, SecretError>;

    /// Stores or rotates a value and returns its durable non-secret reference.
    ///
    /// When `reference` is `None`, mutable stores must allocate a fresh,
    /// unguessable reference rather than overwrite an existing secret. This
    /// enables configuration services to compensate safely if their database
    /// transaction fails. Stores should expire unreferenced fresh values so a
    /// failed compensating delete cannot leave an indefinite orphan.
    async fn put(
        &self,
        reference: Option<&SecretRef>,
        value: SecretString,
    ) -> Result<SecretRef, SecretError>;

    /// Deletes or revokes a referenced value.
    async fn delete(&self, reference: &SecretRef) -> Result<(), SecretError>;
}

/// Read-only bootstrap store mapping explicit secret references to explicit
/// environment variable names.
///
/// This store never interprets a caller-provided reference as an environment
/// variable name. The host must register every mapping at construction time,
/// preventing model/configuration input from probing the process environment.
#[derive(Clone, Debug, Default)]
pub struct EnvironmentSecretStore {
    variables: BTreeMap<SecretRef, String>,
}

impl EnvironmentSecretStore {
    /// Creates an empty default-deny mapping.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one deployment-owned reference-to-variable mapping.
    ///
    /// # Errors
    ///
    /// Returns [`SecretError::InvalidReference`] for an invalid environment
    /// variable name.
    pub fn register(
        mut self,
        reference: SecretRef,
        variable_name: impl Into<String>,
    ) -> Result<Self, SecretError> {
        let variable_name = variable_name.into();
        let valid = !variable_name.is_empty()
            && variable_name.len() <= 200
            && variable_name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
        if !valid {
            return Err(SecretError::InvalidReference);
        }
        self.variables.insert(reference, variable_name);
        Ok(self)
    }
}

#[async_trait]
impl AiSecretStore for EnvironmentSecretStore {
    async fn resolve(&self, reference: &SecretRef) -> Result<SecretString, SecretError> {
        let variable_name = self
            .variables
            .get(reference)
            .ok_or(SecretError::Unavailable)?;
        let value = std::env::var(variable_name).map_err(|_| SecretError::Unavailable)?;
        if value.is_empty() {
            return Err(SecretError::Unavailable);
        }
        Ok(SecretString::from(value))
    }

    async fn put(
        &self,
        _reference: Option<&SecretRef>,
        _value: SecretString,
    ) -> Result<SecretRef, SecretError> {
        Err(SecretError::ReadOnly)
    }

    async fn delete(&self, _reference: &SecretRef) -> Result<(), SecretError> {
        Err(SecretError::ReadOnly)
    }
}
