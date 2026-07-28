//! Verified, content-free OpenAI webhook envelope handling.

use std::fmt;
use std::sync::Arc;

use agql_auth::Clock;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, Mac};
use secrecy::ExposeSecret;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use time::Duration;

use crate::{AiError, AiSecretStore, SecretRef};

const DEFAULT_MAXIMUM_BODY_BYTES: usize = 64 * 1024;
const HARD_MAXIMUM_BODY_BYTES: usize = 1024 * 1024;
const DEFAULT_TIMESTAMP_TOLERANCE_SECONDS: i64 = 300;
const MAXIMUM_TIMESTAMP_TOLERANCE_SECONDS: i64 = 900;
const MAXIMUM_WEBHOOK_ID_BYTES: usize = 200;
const MAXIMUM_SIGNATURE_HEADER_BYTES: usize = 4_096;
const MAXIMUM_SIGNATURES: usize = 8;
const MAXIMUM_PROFILE_ID_BYTES: usize = 200;
const MAXIMUM_PROVIDER_REFERENCE_BYTES: usize = 200;
const MAXIMUM_WEBHOOK_SECRET_BYTES: usize = 4_096;

type HmacSha256 = Hmac<Sha256>;

/// Bounded raw OpenAI Standard Webhooks headers.
///
/// Construction validates syntax and size only. Authenticity is established
/// later by [`OpenAiWebhookVerifier::verify`], using these exact values and the
/// unparsed request body.
#[derive(Clone, PartialEq, Eq)]
pub struct OpenAiWebhookHeaders {
    webhook_id: String,
    timestamp: String,
    signature: String,
}

impl OpenAiWebhookHeaders {
    /// Creates a bounded header set from the exact HTTP header values.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidInput`] when a header is empty, oversized,
    /// contains controls, or the timestamp is not an unsigned decimal integer.
    pub fn new(
        webhook_id: impl Into<String>,
        timestamp: impl Into<String>,
        signature: impl Into<String>,
    ) -> Result<Self, AiError> {
        let webhook_id = webhook_id.into();
        let timestamp = timestamp.into();
        let signature = signature.into();
        if !valid_header_value(&webhook_id, MAXIMUM_WEBHOOK_ID_BYTES)
            || !valid_header_value(&timestamp, 20)
            || !timestamp.bytes().all(|byte| byte.is_ascii_digit())
            || !valid_header_value(&signature, MAXIMUM_SIGNATURE_HEADER_BYTES)
        {
            return Err(AiError::InvalidInput(
                "invalid OpenAI webhook headers".to_owned(),
            ));
        }
        Ok(Self {
            webhook_id,
            timestamp,
            signature,
        })
    }
}

impl fmt::Debug for OpenAiWebhookHeaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiWebhookHeaders")
            .field("webhook_id", &"[REDACTED]")
            .field("timestamp", &self.timestamp)
            .field("signature", &"[REDACTED]")
            .finish()
    }
}

/// Deployment bounds for OpenAI webhook verification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenAiWebhookVerifierLimits {
    maximum_body_bytes: usize,
    timestamp_tolerance: Duration,
}

impl OpenAiWebhookVerifierLimits {
    /// Creates bounded body and replay-window limits.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless the body limit is in
    /// `64..=1 MiB` and timestamp tolerance is positive and no more than
    /// fifteen minutes.
    pub fn new(maximum_body_bytes: usize, timestamp_tolerance: Duration) -> Result<Self, AiError> {
        if !(64..=HARD_MAXIMUM_BODY_BYTES).contains(&maximum_body_bytes)
            || !timestamp_tolerance.is_positive()
            || timestamp_tolerance > Duration::seconds(MAXIMUM_TIMESTAMP_TOLERANCE_SECONDS)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid OpenAI webhook verification limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_body_bytes,
            timestamp_tolerance,
        })
    }

    /// Maximum exact raw request-body bytes accepted for verification.
    pub const fn maximum_body_bytes(self) -> usize {
        self.maximum_body_bytes
    }

    /// Maximum accepted delivery timestamp skew in either direction.
    pub const fn timestamp_tolerance(self) -> Duration {
        self.timestamp_tolerance
    }
}

impl Default for OpenAiWebhookVerifierLimits {
    fn default() -> Self {
        Self {
            maximum_body_bytes: DEFAULT_MAXIMUM_BODY_BYTES,
            timestamp_tolerance: Duration::seconds(DEFAULT_TIMESTAMP_TOLERANCE_SECONDS),
        }
    }
}

/// Reviewed OpenAI webhook event classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OpenAiWebhookEventKind {
    /// A background response completed and may be retrieved by a later worker.
    ResponseCompleted,
    /// A background response failed.
    ResponseFailed,
    /// A background response became incomplete.
    ResponseIncomplete,
    /// A background response was cancelled.
    ResponseCancelled,
    /// A validly signed event outside the supported background response set.
    Unsupported,
}

impl OpenAiWebhookEventKind {
    /// Stable content-free persistence value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ResponseCompleted => "response_completed",
            Self::ResponseFailed => "response_failed",
            Self::ResponseIncomplete => "response_incomplete",
            Self::ResponseCancelled => "response_cancelled",
            Self::Unsupported => "unsupported",
        }
    }

    /// Whether this event names a terminal background response.
    pub const fn is_terminal_response(self) -> bool {
        !matches!(self, Self::Unsupported)
    }
}

/// Signature-verified, content-free OpenAI webhook envelope.
///
/// This value proves only that the exact bounded raw body passed OpenAI's
/// Standard Webhooks signature and replay-window checks for one configured
/// logical profile. It grants no run, attempt, budget, egress, response
/// retrieval, persistence, or completion authority. Event, response, and
/// profile identifiers remain sensitive operational metadata and are redacted
/// from `Debug`.
#[derive(Clone, PartialEq, Eq)]
pub struct OpenAiVerifiedWebhookEvent {
    provider_profile_id: String,
    provider_event_id: String,
    event_kind: OpenAiWebhookEventKind,
    provider_response_id: Option<String>,
    provider_created_at: i64,
    received_at: i64,
    receipt_key: String,
    receipt_id: uuid::Uuid,
}

impl OpenAiVerifiedWebhookEvent {
    /// Exact logical OpenAI profile whose signing secret verified the event.
    pub fn provider_profile_id(&self) -> &str {
        &self.provider_profile_id
    }

    /// Exact provider event identifier from the signed body.
    pub fn provider_event_id(&self) -> &str {
        &self.provider_event_id
    }

    /// Reviewed content-free event classification.
    pub const fn event_kind(&self) -> OpenAiWebhookEventKind {
        self.event_kind
    }

    /// Exact background response identifier for supported terminal events.
    pub fn provider_response_id(&self) -> Option<&str> {
        self.provider_response_id.as_deref()
    }

    /// Provider-declared event creation time as Unix seconds.
    pub const fn provider_created_at(&self) -> i64 {
        self.provider_created_at
    }

    /// Trusted local verification time as Unix seconds.
    pub const fn received_at(&self) -> i64 {
        self.received_at
    }

    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) fn receipt_key(&self) -> &str {
        &self.receipt_key
    }

    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) const fn receipt_id(&self) -> uuid::Uuid {
        self.receipt_id
    }
}

impl fmt::Debug for OpenAiVerifiedWebhookEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiVerifiedWebhookEvent")
            .field("provider_profile_id", &"[REDACTED]")
            .field("provider_event_id", &"[REDACTED]")
            .field("event_kind", &self.event_kind)
            .field("provider_response_id", &"[REDACTED]")
            .field("provider_created_at", &self.provider_created_at)
            .field("received_at", &self.received_at)
            .finish()
    }
}

/// OpenAI Standard Webhooks verifier using a just-in-time signing secret.
pub struct OpenAiWebhookVerifier {
    provider_profile_id: String,
    webhook_secret: SecretRef,
    secrets: Arc<dyn AiSecretStore>,
    clock: Arc<dyn Clock>,
    limits: OpenAiWebhookVerifierLimits,
}

impl OpenAiWebhookVerifier {
    /// Builds a verifier for one exact logical OpenAI profile.
    ///
    /// The secret is resolved for every verification and is never persisted in
    /// a receipt or returned event.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] for an invalid profile ID.
    pub fn new(
        provider_profile_id: impl Into<String>,
        webhook_secret: SecretRef,
        secrets: Arc<dyn AiSecretStore>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, AiError> {
        Self::with_limits(
            provider_profile_id,
            webhook_secret,
            secrets,
            clock,
            OpenAiWebhookVerifierLimits::default(),
        )
    }

    /// Builds a verifier with deployment-narrowed verification bounds.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] for an invalid profile ID.
    pub fn with_limits(
        provider_profile_id: impl Into<String>,
        webhook_secret: SecretRef,
        secrets: Arc<dyn AiSecretStore>,
        clock: Arc<dyn Clock>,
        limits: OpenAiWebhookVerifierLimits,
    ) -> Result<Self, AiError> {
        let provider_profile_id = provider_profile_id.into();
        if !valid_safe_value(&provider_profile_id, MAXIMUM_PROFILE_ID_BYTES) {
            return Err(AiError::InvalidConfiguration(
                "invalid OpenAI webhook profile".to_owned(),
            ));
        }
        Ok(Self {
            provider_profile_id,
            webhook_secret,
            secrets,
            clock,
            limits,
        })
    }

    /// Verifies and minimally parses one exact raw webhook delivery.
    ///
    /// Verification follows OpenAI's Standard Webhooks contract over
    /// `webhook-id.webhook-timestamp.raw-body`, accepts bounded rotating `v1`
    /// signatures, and rejects stale or future delivery timestamps. The raw
    /// body is neither retained nor returned.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Forbidden`] when signature, secret, or timestamp
    /// verification fails. Returns [`AiError::InvalidInput`] for an oversized,
    /// non-UTF-8, malformed, or inconsistent signed event envelope.
    pub async fn verify(
        &self,
        headers: &OpenAiWebhookHeaders,
        raw_body: &[u8],
    ) -> Result<OpenAiVerifiedWebhookEvent, AiError> {
        if raw_body.is_empty() || raw_body.len() > self.limits.maximum_body_bytes {
            return Err(AiError::InvalidInput(
                "invalid OpenAI webhook body".to_owned(),
            ));
        }
        let body = std::str::from_utf8(raw_body)
            .map_err(|_| AiError::InvalidInput("invalid OpenAI webhook body".to_owned()))?;
        let received_at = self.clock.now().unix_timestamp();
        let delivery_timestamp = headers
            .timestamp
            .parse::<i64>()
            .map_err(|_| AiError::Forbidden)?;
        let tolerance = self.limits.timestamp_tolerance.whole_seconds();
        if received_at.saturating_sub(delivery_timestamp) > tolerance
            || delivery_timestamp.saturating_sub(received_at) > tolerance
        {
            return Err(AiError::Forbidden);
        }

        let secret = self
            .secrets
            .resolve(&self.webhook_secret)
            .await
            .map_err(|_| AiError::Forbidden)?;
        verify_signature(headers, raw_body, secret.expose_secret().as_bytes())?;

        let payload: WireWebhookEvent = serde_json::from_str(body)
            .map_err(|_| AiError::InvalidInput("invalid OpenAI webhook body".to_owned()))?;
        if !valid_provider_id(&payload.id, "evt_")
            || payload.created_at <= 0
            || payload.created_at > received_at.saturating_add(tolerance)
        {
            return Err(AiError::InvalidInput(
                "invalid OpenAI webhook body".to_owned(),
            ));
        }
        let event_kind = match payload.event_type.as_str() {
            "response.completed" => OpenAiWebhookEventKind::ResponseCompleted,
            "response.failed" => OpenAiWebhookEventKind::ResponseFailed,
            "response.incomplete" => OpenAiWebhookEventKind::ResponseIncomplete,
            "response.cancelled" => OpenAiWebhookEventKind::ResponseCancelled,
            _ => OpenAiWebhookEventKind::Unsupported,
        };
        let provider_response_id = if event_kind.is_terminal_response() {
            let response_id = payload
                .data
                .and_then(|data| data.id)
                .filter(|value| valid_provider_id(value, "resp_"))
                .ok_or_else(|| AiError::InvalidInput("invalid OpenAI webhook body".to_owned()))?;
            Some(response_id)
        } else {
            None
        };
        let (receipt_key, receipt_id) =
            webhook_receipt_identity(&self.provider_profile_id, &payload.id);
        Ok(OpenAiVerifiedWebhookEvent {
            provider_profile_id: self.provider_profile_id.clone(),
            provider_event_id: payload.id,
            event_kind,
            provider_response_id,
            provider_created_at: payload.created_at,
            received_at,
            receipt_key,
            receipt_id,
        })
    }
}

impl fmt::Debug for OpenAiWebhookVerifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiWebhookVerifier")
            .field("provider_profile_id", &"[REDACTED]")
            .field("webhook_secret", &"[REDACTED]")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct WireWebhookEvent {
    id: String,
    #[serde(rename = "type")]
    event_type: String,
    created_at: i64,
    data: Option<WireWebhookData>,
}

#[derive(Deserialize)]
struct WireWebhookData {
    id: Option<String>,
}

fn verify_signature(
    headers: &OpenAiWebhookHeaders,
    raw_body: &[u8],
    exposed_secret: &[u8],
) -> Result<(), AiError> {
    if exposed_secret.is_empty() || exposed_secret.len() > MAXIMUM_WEBHOOK_SECRET_BYTES {
        return Err(AiError::Forbidden);
    }
    let decoded_secret;
    let secret = if let Some(encoded) = exposed_secret.strip_prefix(b"whsec_") {
        decoded_secret = STANDARD.decode(encoded).map_err(|_| AiError::Forbidden)?;
        if decoded_secret.is_empty() || decoded_secret.len() > MAXIMUM_WEBHOOK_SECRET_BYTES {
            return Err(AiError::Forbidden);
        }
        decoded_secret.as_slice()
    } else {
        exposed_secret
    };

    let mut signed = Vec::with_capacity(
        headers
            .webhook_id
            .len()
            .saturating_add(headers.timestamp.len())
            .saturating_add(raw_body.len())
            .saturating_add(2),
    );
    signed.extend_from_slice(headers.webhook_id.as_bytes());
    signed.push(b'.');
    signed.extend_from_slice(headers.timestamp.as_bytes());
    signed.push(b'.');
    signed.extend_from_slice(raw_body);
    let mut mac = HmacSha256::new_from_slice(secret).map_err(|_| AiError::Forbidden)?;
    mac.update(&signed);

    let mut signature_count = 0_usize;
    let mut verified = false;
    for candidate in headers.signature.split_ascii_whitespace() {
        signature_count = signature_count.saturating_add(1);
        if signature_count > MAXIMUM_SIGNATURES {
            return Err(AiError::Forbidden);
        }
        let encoded = candidate.strip_prefix("v1,").unwrap_or(candidate);
        verified |= STANDARD
            .decode(encoded)
            .ok()
            .is_some_and(|signature| mac.clone().verify_slice(&signature).is_ok());
    }
    if signature_count != 0 && verified {
        Ok(())
    } else {
        Err(AiError::Forbidden)
    }
}

pub(crate) fn webhook_receipt_identity(profile_id: &str, event_id: &str) -> (String, uuid::Uuid) {
    let mut hasher = Sha256::new();
    hasher.update(b"graphql-orm-ai/openai-webhook-receipt/v1\0");
    hasher.update(profile_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(event_id.as_bytes());
    let digest = hasher.finalize();
    let mut id_bytes = [0_u8; 16];
    id_bytes.copy_from_slice(&digest[..16]);
    (hex::encode(digest), uuid::Uuid::from_bytes(id_bytes))
}

fn valid_provider_id(value: &str, prefix: &str) -> bool {
    value.starts_with(prefix)
        && (prefix.len() + 1..=MAXIMUM_PROVIDER_REFERENCE_BYTES).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn valid_safe_value(value: &str, maximum: usize) -> bool {
    !value.trim().is_empty()
        && value.len() <= maximum
        && value.chars().all(|character| !character.is_control())
}

fn valid_header_value(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii() && !byte.is_ascii_control())
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use secrecy::SecretString;
    use time::OffsetDateTime;

    use super::*;
    use crate::SecretError;

    struct TestSecrets(SecretRef, String);

    #[async_trait]
    impl AiSecretStore for TestSecrets {
        async fn resolve(&self, reference: &SecretRef) -> Result<SecretString, SecretError> {
            if reference == &self.0 {
                Ok(SecretString::from(self.1.clone()))
            } else {
                Err(SecretError::Unavailable)
            }
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

    fn fixture() -> (OpenAiWebhookVerifier, String) {
        let reference =
            SecretRef::parse("openai/webhook-test").expect("test secret reference should parse");
        let secret = "synthetic-webhook-secret".to_owned();
        let now = OffsetDateTime::from_unix_timestamp(2_000_000_000)
            .expect("test timestamp should be valid");
        let verifier = OpenAiWebhookVerifier::new(
            "profile-openai",
            reference.clone(),
            Arc::new(TestSecrets(reference, secret.clone())),
            Arc::new(agql_auth::FixedClock::new(now)),
        )
        .expect("verifier should build");
        (verifier, secret)
    }

    fn signed_headers(
        secret: &str,
        event_id: &str,
        timestamp: i64,
        body: &[u8],
    ) -> OpenAiWebhookHeaders {
        let mut signed = format!("{event_id}.{timestamp}.").into_bytes();
        signed.extend_from_slice(body);
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .expect("synthetic secret should be accepted");
        mac.update(&signed);
        let signature = STANDARD.encode(mac.finalize().into_bytes());
        OpenAiWebhookHeaders::new(event_id, timestamp.to_string(), format!("v1,{signature}"))
            .expect("synthetic headers should validate")
    }

    #[tokio::test]
    async fn exact_signed_terminal_event_is_content_free_and_redacted() {
        let (verifier, secret) = fixture();
        let body = br#"{"id":"evt_background_1","type":"response.completed","created_at":1999999999,"data":{"id":"resp_background_1"},"ignored":"never persisted"}"#;
        let headers = signed_headers(&secret, "delivery_background_1", 2_000_000_000, body);
        let event = verifier
            .verify(&headers, body)
            .await
            .expect("exact signature should verify");
        assert_eq!(
            event.event_kind(),
            OpenAiWebhookEventKind::ResponseCompleted
        );
        assert_eq!(event.provider_response_id(), Some("resp_background_1"));
        assert_eq!(event.received_at(), 2_000_000_000);
        let debug = format!("{event:?} {headers:?} {verifier:?}");
        assert!(!debug.contains("profile-openai"));
        assert!(!debug.contains("evt_background_1"));
        assert!(!debug.contains("resp_background_1"));
        assert!(!debug.contains("synthetic-webhook-secret"));
        assert!(!debug.contains("never persisted"));
    }

    #[tokio::test]
    async fn tampering_stale_delivery_and_missing_response_fail_closed() {
        let (verifier, secret) = fixture();
        let body = br#"{"id":"evt_background_2","type":"response.failed","created_at":1999999999,"data":{"id":"resp_background_2"}}"#;
        let headers = signed_headers(&secret, "delivery_background_2", 2_000_000_000, body);
        let mut tampered = body.to_vec();
        tampered.push(b' ');
        assert!(matches!(
            verifier.verify(&headers, &tampered).await,
            Err(AiError::Forbidden)
        ));

        let stale = signed_headers(&secret, "delivery_background_2", 1_999_999_000, body);
        assert!(matches!(
            verifier.verify(&stale, body).await,
            Err(AiError::Forbidden)
        ));

        let missing = br#"{"id":"evt_background_2","type":"response.failed","created_at":1999999999,"data":{}}"#;
        let headers = signed_headers(&secret, "delivery_background_2", 2_000_000_000, missing);
        assert!(matches!(
            verifier.verify(&headers, missing).await,
            Err(AiError::InvalidInput(_))
        ));
    }

    #[tokio::test]
    async fn rotating_signature_and_unsupported_event_are_bounded() {
        let (verifier, secret) = fixture();
        let body = br#"{"id":"evt_batch_1","type":"batch.completed","created_at":1999999999,"data":{"id":"batch_1"}}"#;
        let valid = signed_headers(&secret, "delivery_batch_1", 2_000_000_000, body);
        let headers = OpenAiWebhookHeaders::new(
            "delivery_batch_1",
            "2000000000",
            format!("v1,{} {}", STANDARD.encode([0_u8; 32]), valid.signature),
        )
        .expect("rotating signature header should validate");
        let event = verifier
            .verify(&headers, body)
            .await
            .expect("one rotating signature should verify");
        assert_eq!(event.event_kind(), OpenAiWebhookEventKind::Unsupported);
        assert_eq!(event.provider_response_id(), None);

        let reference = SecretRef::parse("openai/webhook-prefixed-test")
            .expect("test secret reference should parse");
        let prefixed_verifier = OpenAiWebhookVerifier::new(
            "profile-openai",
            reference.clone(),
            Arc::new(TestSecrets(
                reference,
                format!("whsec_{}", STANDARD.encode(secret.as_bytes())),
            )),
            Arc::new(agql_auth::FixedClock::new(
                OffsetDateTime::from_unix_timestamp(2_000_000_000)
                    .expect("test timestamp should be valid"),
            )),
        )
        .expect("prefixed-secret verifier should build");
        let prefixed_headers = signed_headers(&secret, "delivery_batch_2", 2_000_000_000, body);
        prefixed_verifier
            .verify(&prefixed_headers, body)
            .await
            .expect("base64-prefixed OpenAI secret should verify");
    }
}
