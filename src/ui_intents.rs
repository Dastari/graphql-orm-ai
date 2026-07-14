//! Project-neutral, schema-validated frontend intent suggestions.

use std::collections::BTreeMap;

#[cfg(any(feature = "sqlite", feature = "postgres"))]
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::AiError;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use crate::{AiProviderCallResult, AiRunLease};

const JSON_SCHEMA_2020_12: &str = "https://json-schema.org/draft/2020-12/schema";
const MAXIMUM_INTENT_SCHEMA_BYTES: usize = 1024 * 1024;
const MAXIMUM_DISPLAY_METADATA_BYTES: usize = 64 * 1024;

/// Stable lower-case identifier for a logical UI intent type.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AiUiIntentTypeId(String);

impl AiUiIntentTypeId {
    /// Parses a bounded lower-case namespaced intent type.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is empty, longer than 200 bytes, or
    /// contains characters outside lower-case ASCII letters, digits, dots,
    /// dashes, and underscores.
    pub fn parse(value: impl Into<String>) -> Result<Self, AiError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 200
            || !value.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'-' | b'_')
            })
        {
            return Err(AiError::InvalidConfiguration(
                "UI intent type IDs must be bounded lower-case ASCII names".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    /// Returns the stable type identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Exact immutable binding to one registered UI-intent descriptor.
///
/// A binding identifies a schema but grants no permission to emit, deliver,
/// render, or execute the intent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiUiIntentTypeBinding {
    /// Registered logical type.
    pub intent_type: AiUiIntentTypeId,
    /// SHA-256 fingerprint of the descriptor's security-relevant contract.
    pub descriptor_fingerprint: String,
}

/// Host-registered logical UI-intent payload contract.
///
/// Descriptors contain no route, URL, component, callback, or executable
/// frontend code. The fingerprint binds the type, schema version, schema, and
/// maximum payload size. Display metadata is safe presentation-only data and
/// is deliberately outside the security fingerprint.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AiUiIntentTypeDescriptor {
    /// Stable logical intent type.
    pub id: AiUiIntentTypeId,
    /// Immutable application-defined schema version.
    pub schema_version: String,
    /// JSON Schema 2020-12 payload contract.
    pub schema: serde_json::Value,
    /// Safe presentation hints; never route or executable code.
    pub display_metadata: serde_json::Value,
    /// Maximum serialized payload bytes.
    pub maximum_payload_bytes: u64,
    /// Exact descriptor fingerprint.
    pub fingerprint: String,
}

impl AiUiIntentTypeDescriptor {
    /// Creates and validates an intent descriptor.
    ///
    /// # Errors
    ///
    /// Returns an error unless the ID and version are valid, the schema
    /// explicitly declares JSON Schema 2020-12, the schema compiles, and its
    /// serialized representation is bounded.
    pub fn new(
        id: impl Into<String>,
        schema_version: impl Into<String>,
        schema: serde_json::Value,
    ) -> Result<Self, AiError> {
        let id = AiUiIntentTypeId::parse(id)?;
        let schema_version = schema_version.into();
        validate_schema_version(&schema_version)?;
        validate_schema(&schema)?;
        let mut descriptor = Self {
            id,
            schema_version,
            schema,
            display_metadata: json!({}),
            maximum_payload_bytes: 64 * 1024,
            fingerprint: String::new(),
        };
        descriptor.refresh_fingerprint()?;
        Ok(descriptor)
    }

    /// Sets bounded safe presentation metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when metadata is not an object or exceeds 64 KiB.
    pub fn with_display_metadata(
        mut self,
        display_metadata: serde_json::Value,
    ) -> Result<Self, AiError> {
        if !display_metadata.is_object()
            || serialized_len(&display_metadata)? > MAXIMUM_DISPLAY_METADATA_BYTES
        {
            return Err(AiError::InvalidConfiguration(
                "UI intent display metadata is invalid".to_owned(),
            ));
        }
        self.display_metadata = display_metadata;
        Ok(self)
    }

    /// Sets a hard payload limit and refreshes the exact fingerprint.
    ///
    /// # Errors
    ///
    /// Returns an error unless the limit is between 1 byte and 1 MiB.
    pub fn with_maximum_payload_bytes(mut self, maximum: u64) -> Result<Self, AiError> {
        if !(1..=1024 * 1024).contains(&maximum) {
            return Err(AiError::InvalidConfiguration(
                "UI intent payload limit is invalid".to_owned(),
            ));
        }
        self.maximum_payload_bytes = maximum;
        self.refresh_fingerprint()?;
        Ok(self)
    }

    /// Returns an exact binding suitable for an immutable skill version.
    pub fn binding(&self) -> AiUiIntentTypeBinding {
        AiUiIntentTypeBinding {
            intent_type: self.id.clone(),
            descriptor_fingerprint: self.fingerprint.clone(),
        }
    }

    fn validate_integrity(&self) -> Result<(), AiError> {
        validate_schema_version(&self.schema_version)?;
        validate_schema(&self.schema)?;
        if !(1..=1024 * 1024).contains(&self.maximum_payload_bytes)
            || !self.display_metadata.is_object()
            || serialized_len(&self.display_metadata)? > MAXIMUM_DISPLAY_METADATA_BYTES
            || self.fingerprint != descriptor_fingerprint(self)?
        {
            return Err(AiError::InvalidConfiguration(
                "UI intent descriptor integrity check failed".to_owned(),
            ));
        }
        Ok(())
    }

    fn refresh_fingerprint(&mut self) -> Result<(), AiError> {
        self.fingerprint = descriptor_fingerprint(self)?;
        Ok(())
    }
}

/// Model-produced logical intent before registry and schema validation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AiUiIntentDraft {
    /// Registered logical type.
    pub intent_type: AiUiIntentTypeId,
    /// Project-neutral structured payload.
    pub payload: serde_json::Value,
}

/// Schema- and fingerprint-validated logical frontend suggestion.
///
/// This value proves conformance to one exact registered descriptor. It does
/// not prove the user may access a referenced resource and never authorizes or
/// performs navigation. A frontend must treat it as a suggestion, revalidate
/// current application state, and map the logical type through its own code.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ValidatedAiUiIntent {
    /// Server-assigned suggestion ID.
    pub id: Uuid,
    /// Exact descriptor binding.
    pub binding: AiUiIntentTypeBinding,
    /// Validated structured payload.
    pub payload: serde_json::Value,
}

/// Durable result of one fenced UI-intent suggestion event.
///
/// The returned value proves schema validation and durable event persistence,
/// not resource authorization or frontend navigation permission.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
#[derive(Clone, Debug)]
pub struct AiPersistedUiIntent {
    intent: ValidatedAiUiIntent,
    event_sequence: i64,
    lease: AiRunLease,
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
impl AiPersistedUiIntent {
    /// Returns the schema-validated logical suggestion.
    pub fn intent(&self) -> &ValidatedAiUiIntent {
        &self.intent
    }

    /// Returns its durable per-session event sequence.
    pub const fn event_sequence(&self) -> i64 {
        self.event_sequence
    }

    /// Returns the renewed run lease required for the next fenced write.
    pub fn lease(&self) -> &AiRunLease {
        &self.lease
    }

    /// Consumes the result and returns the renewed lease.
    pub fn into_lease(self) -> AiRunLease {
        self.lease
    }

    pub(crate) fn new(intent: ValidatedAiUiIntent, event_sequence: i64, lease: AiRunLease) -> Self {
        Self {
            intent,
            event_sequence,
            lease,
        }
    }
}

/// Fenced backend delivery for an exact provider-produced UI-intent envelope.
///
/// Implementations must parse only the exact visible provider output, validate
/// it against the registered descriptor and binding, reauthorize the current
/// principal, protect the payload, and commit through the current run fence.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
#[async_trait]
pub trait AiUiIntentDeliveryService: Send + Sync {
    /// Persists one exact provider-produced logical suggestion.
    ///
    /// # Errors
    ///
    /// Returns an error for a stale/swapped provider result or lease,
    /// unregistered/stale descriptor binding, malformed or schema-invalid
    /// output, denied current authority, unready protection, missing committed
    /// budget proof, stale fence, or persistence failure.
    async fn persist_provider_suggestion(
        &self,
        lease: &AiRunLease,
        result: &AiProviderCallResult,
        binding: &AiUiIntentTypeBinding,
    ) -> Result<AiPersistedUiIntent, AiError>;
}

/// Immutable host registry for logical UI-intent schemas.
///
/// Registering a descriptor is discovery only. It does not enable a skill,
/// grant model egress, authorize a resource, or permit frontend navigation.
#[derive(Clone, Debug, Default)]
pub struct AiUiIntentCatalog {
    descriptors: BTreeMap<AiUiIntentTypeId, AiUiIntentTypeDescriptor>,
}

impl AiUiIntentCatalog {
    /// Creates an empty default-deny catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one immutable descriptor.
    ///
    /// # Errors
    ///
    /// Returns an error for a duplicate type or an invalid/tampered descriptor.
    pub fn register(&mut self, descriptor: AiUiIntentTypeDescriptor) -> Result<(), AiError> {
        descriptor.validate_integrity()?;
        if self.descriptors.contains_key(&descriptor.id) {
            return Err(AiError::AlreadyExists(descriptor.id.as_str().to_owned()));
        }
        self.descriptors.insert(descriptor.id.clone(), descriptor);
        Ok(())
    }

    /// Returns one registered descriptor.
    pub fn descriptor(&self, id: &AiUiIntentTypeId) -> Option<&AiUiIntentTypeDescriptor> {
        self.descriptors.get(id)
    }

    /// Validates one draft against an exact skill/policy binding.
    ///
    /// # Errors
    ///
    /// Returns an error when the type is unregistered, the binding is stale or
    /// swapped, the payload exceeds its limit, or JSON Schema validation fails.
    pub fn validate_bound(
        &self,
        binding: &AiUiIntentTypeBinding,
        draft: AiUiIntentDraft,
    ) -> Result<ValidatedAiUiIntent, AiError> {
        if draft.intent_type != binding.intent_type {
            return Err(AiError::InvalidInput(
                "UI intent type does not match its binding".to_owned(),
            ));
        }
        let descriptor = self
            .descriptors
            .get(&draft.intent_type)
            .ok_or(AiError::NotFound)?;
        descriptor.validate_integrity()?;
        if descriptor.fingerprint != binding.descriptor_fingerprint {
            return Err(AiError::Conflict);
        }
        if serialized_len(&draft.payload)? as u64 > descriptor.maximum_payload_bytes {
            return Err(AiError::InvalidInput(
                "UI intent payload exceeds the configured limit".to_owned(),
            ));
        }
        let validator = jsonschema::validator_for(&descriptor.schema).map_err(|_| {
            AiError::InvalidConfiguration("registered UI intent schema is invalid".to_owned())
        })?;
        if !validator.is_valid(&draft.payload) {
            return Err(AiError::InvalidInput(
                "UI intent payload does not match the registered schema".to_owned(),
            ));
        }
        Ok(ValidatedAiUiIntent {
            id: Uuid::new_v4(),
            binding: binding.clone(),
            payload: draft.payload,
        })
    }
}

fn validate_schema_version(value: &str) -> Result<(), AiError> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(AiError::InvalidConfiguration(
            "UI intent schema version is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_schema(schema: &serde_json::Value) -> Result<(), AiError> {
    if schema.get("$schema").and_then(serde_json::Value::as_str) != Some(JSON_SCHEMA_2020_12)
        || serialized_len(schema)? > MAXIMUM_INTENT_SCHEMA_BYTES
        || jsonschema::validator_for(schema).is_err()
    {
        return Err(AiError::InvalidConfiguration(
            "UI intent schemas must be bounded valid JSON Schema 2020-12".to_owned(),
        ));
    }
    Ok(())
}

fn descriptor_fingerprint(descriptor: &AiUiIntentTypeDescriptor) -> Result<String, AiError> {
    let value = json!({
        "format": "graphql-orm-ai-ui-intent-v1",
        "type": descriptor.id.as_str(),
        "schema_version": descriptor.schema_version,
        "schema": descriptor.schema,
        "maximum_payload_bytes": descriptor.maximum_payload_bytes,
    });
    let bytes = serde_json::to_vec(&canonical_json(&value)).map_err(|_| {
        AiError::InvalidConfiguration("UI intent descriptor is not serializable".to_owned())
    })?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn serialized_len(value: &serde_json::Value) -> Result<usize, AiError> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(|_| AiError::InvalidConfiguration("UI intent JSON is invalid".to_owned()))
}

fn canonical_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(canonical_json).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), canonical_json(value)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        value => value.clone(),
    }
}
