//! Data classification and provenance.

use serde::{Deserialize, Serialize};

/// Ordered confidentiality classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataClassification {
    /// Safe for public disclosure.
    Public,
    /// Internal application data.
    Internal,
    /// Confidential user/tenant data.
    Confidential,
    /// Highly restricted regulated or sensitive data.
    Restricted,
    /// Credentials, keys, or other material that must never be model-facing.
    Secret,
}

/// Provenance trust applied to model-facing input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiSourceTrust {
    /// Runtime-authored instructions or metadata.
    TrustedRuntime,
    /// Authenticated user-provided content.
    UserProvided,
    /// Application resolver output.
    ResolverResult,
    /// Web, MCP, provider, or other untrusted external content.
    ExternalUntrusted,
}

/// Redacted source reference used for provenance and egress manifests.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AiDataSourceRef {
    /// Source kind such as `message_block`, `attachment`, or `tool_artifact`.
    pub kind: String,
    /// Opaque stable identifier; never source plaintext.
    pub reference: String,
    /// Confidentiality classification.
    pub classification: DataClassification,
    /// Trust/provenance classification.
    pub trust: AiSourceTrust,
}
