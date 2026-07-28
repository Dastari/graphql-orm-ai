//! Durable idempotent intake for verified provider webhook envelopes.

#![cfg(all(
    any(feature = "sqlite", feature = "postgres"),
    feature = "provider-openai"
))]

use graphql_orm::db::Database;
use graphql_orm::graphql::errors::{OrmErrorCode, OrmPublicError};
use std::sync::Arc;

use crate::persistence::*;
use crate::{AiError, OpenAiVerifiedWebhookEvent, OpenAiWebhookEventKind};
use graphql_orm::graphql::orm::{
    DefaultWriteBackend, EntityAccessKind, EntityAccessSurface, EntityPolicy,
    InsertIfAbsentOutcome, TransactionError, TransactionMode,
};

const MAXIMUM_TRANSACTION_RETRIES: u8 = 4;

/// Result of durably accepting one signature-verified provider webhook.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AiProviderWebhookReceiptOutcome {
    /// This call durably inserted the first receipt and its redacted audit.
    Recorded,
    /// The exact same provider/profile/event receipt was already durable.
    AlreadyRecorded,
}

/// ORM-backed content-free intake for verified OpenAI webhook envelopes.
///
/// Intake is deliberately separate from background response reconciliation.
/// It persists no raw webhook body or signature and does not look up, mutate,
/// heartbeat, complete, or recover a run. Supported response events enter
/// `pending_reconciliation`; unsupported but validly signed events are marked
/// ignored. A later worker must independently prove the exact original
/// run/attempt/fence/provider/profile/response/budget/egress bindings before
/// retrieving provider state or changing a run.
#[derive(Clone)]
pub struct OrmAiProviderWebhookReceiptService {
    database: Database<DefaultWriteBackend>,
}

#[derive(Clone)]
struct ProviderWebhookReceiptEntityPolicy {
    delegate: Option<Arc<dyn EntityPolicy<DefaultWriteBackend>>>,
}

impl EntityPolicy<DefaultWriteBackend> for ProviderWebhookReceiptEntityPolicy {
    fn can_access_entity<'a>(
        &'a self,
        context: Option<&'a async_graphql::Context<'_>>,
        database: &'a Database<DefaultWriteBackend>,
        entity_name: &'static str,
        policy_key: Option<&'static str>,
        kind: EntityAccessKind,
        surface: EntityAccessSurface,
    ) -> graphql_orm::futures::future::BoxFuture<'a, async_graphql::Result<bool>> {
        if entity_name == "AiProviderWebhookReceiptRecord"
            && policy_key.is_none()
            && surface == EntityAccessSurface::Repository
        {
            return Box::pin(async { Ok(true) });
        }
        if let Some(delegate) = &self.delegate {
            return delegate.can_access_entity(
                context,
                database,
                entity_name,
                policy_key,
                kind,
                surface,
            );
        }
        Box::pin(async { Ok(true) })
    }
}

impl OrmAiProviderWebhookReceiptService {
    /// Creates a receipt service over the AI schema database.
    pub fn new(database: Database<DefaultWriteBackend>) -> Self {
        Self {
            database: with_provider_webhook_receipt_policy(database),
        }
    }

    /// Returns the ORM database handle for host composition.
    pub const fn database(&self) -> &Database<DefaultWriteBackend> {
        &self.database
    }

    /// Durably records one signature-verified content-free event envelope.
    ///
    /// The database atomically inserts on a SHA-256 key bound to provider
    /// family, logical profile, and provider event ID. Exact replays return
    /// [`AiProviderWebhookReceiptOutcome::AlreadyRecorded`]; a conflicting
    /// event under the same key fails closed. A first insert appends one
    /// redacted audit in the same transaction.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Conflict`] for a mismatched replay, or a redacted
    /// persistence error when the receipt and audit cannot commit atomically
    /// after the bounded serialization-retry limit.
    pub async fn record(
        &self,
        event: &OpenAiVerifiedWebhookEvent,
    ) -> Result<AiProviderWebhookReceiptOutcome, AiError> {
        for retry in 0..=MAXIMUM_TRANSACTION_RETRIES {
            match self.record_once(event).await {
                Ok(outcome) => return Ok(outcome),
                Err(TransactionError::Retryable(_)) if retry < MAXIMUM_TRANSACTION_RETRIES => {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(map_transaction(error)),
            }
        }
        Err(AiError::PersistenceFailed)
    }

    async fn record_once(
        &self,
        event: &OpenAiVerifiedWebhookEvent,
    ) -> Result<AiProviderWebhookReceiptOutcome, TransactionError> {
        let id = event.receipt_id();
        let receipt_key = event.receipt_key().to_owned();
        let provider_profile_id = event.provider_profile_id().to_owned();
        let provider_event_id = event.provider_event_id().to_owned();
        let provider_event_kind = event.event_kind().as_str().to_owned();
        let provider_response_id = event.provider_response_id().map(str::to_owned);
        let provider_created_at = event.provider_created_at();
        let received_at = event.received_at();
        let (state, safe_error_code, processed_at) = match event.event_kind() {
            OpenAiWebhookEventKind::Unsupported => (
                "ignored".to_owned(),
                Some("unsupported_event_kind".to_owned()),
                Some(received_at),
            ),
            _ => ("pending_reconciliation".to_owned(), None, None),
        };
        self.database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let outcome = tx
                        .insert_if_absent::<AiProviderWebhookReceiptRecord>(
                            CreateAiProviderWebhookReceiptRecordInput {
                                id,
                                receipt_key: receipt_key.clone(),
                                provider_kind: "openai".to_owned(),
                                provider_profile_id: provider_profile_id.clone(),
                                provider_event_id: provider_event_id.clone(),
                                provider_event_kind: provider_event_kind.clone(),
                                provider_created_at,
                                provider_response_id: provider_response_id.clone(),
                                run_id: None,
                                attempt_id: None,
                                signature_verified: true,
                                state: state.clone(),
                                safe_error_code: safe_error_code.clone(),
                                received_at,
                                processed_at,
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?;
                    match outcome {
                        InsertIfAbsentOutcome::Inserted(receipt) => {
                            if !valid_receipt_facts(
                                &receipt,
                                id,
                                &receipt_key,
                                &provider_profile_id,
                                &provider_event_id,
                                &provider_event_kind,
                                provider_created_at,
                                provider_response_id.as_deref(),
                            ) || receipt.run_id.is_some()
                                || receipt.attempt_id.is_some()
                                || receipt.state != state
                                || receipt.safe_error_code.as_deref() != safe_error_code.as_deref()
                                || receipt.received_at != received_at
                                || receipt.processed_at != processed_at
                                || receipt.row_version != 0
                            {
                                return Err(OrmPublicError::new(OrmErrorCode::InternalError));
                            }
                            tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                                actor_principal_kind: "system".to_owned(),
                                actor_subject: "provider-webhook".to_owned(),
                                action: "record_provider_webhook".to_owned(),
                                resource_kind: "ai_provider_webhook_receipt".to_owned(),
                                resource_reference: receipt.id.to_string(),
                                outcome: if state == "ignored" {
                                    "ignored".to_owned()
                                } else {
                                    "pending".to_owned()
                                },
                                reason_code: if state == "ignored" {
                                    "unsupported_event_kind".to_owned()
                                } else {
                                    "verified_event_recorded".to_owned()
                                },
                                correlation_id: receipt.id.to_string(),
                                causation_id: None,
                                policy_version: Some("provider-webhook-v1".to_owned()),
                            })
                            .await
                            .map_err(OrmPublicError::from)?;
                            Ok(AiProviderWebhookReceiptOutcome::Recorded)
                        }
                        InsertIfAbsentOutcome::AlreadyPresent(receipt) => {
                            if !valid_receipt_facts(
                                &receipt,
                                id,
                                &receipt_key,
                                &provider_profile_id,
                                &provider_event_id,
                                &provider_event_kind,
                                provider_created_at,
                                provider_response_id.as_deref(),
                            ) {
                                return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                            }
                            Ok(AiProviderWebhookReceiptOutcome::AlreadyRecorded)
                        }
                    }
                })
            })
            .await
    }
}

pub(crate) fn with_provider_webhook_receipt_policy(
    mut database: Database<DefaultWriteBackend>,
) -> Database<DefaultWriteBackend> {
    let delegate = database.entity_policy().cloned();
    database.set_entity_policy(ProviderWebhookReceiptEntityPolicy { delegate });
    database
}

#[allow(clippy::too_many_arguments)]
fn valid_receipt_facts(
    receipt: &AiProviderWebhookReceiptRecord,
    receipt_id: graphql_orm::uuid::Uuid,
    receipt_key: &str,
    provider_profile_id: &str,
    provider_event_id: &str,
    provider_event_kind: &str,
    provider_created_at: i64,
    provider_response_id: Option<&str>,
) -> bool {
    receipt.id == receipt_id
        && receipt.receipt_key == receipt_key
        && receipt.provider_kind == "openai"
        && receipt.provider_profile_id == provider_profile_id
        && receipt.provider_event_id == provider_event_id
        && receipt.provider_event_kind == provider_event_kind
        && receipt.provider_created_at == provider_created_at
        && receipt.provider_response_id.as_deref() == provider_response_id
        && receipt.signature_verified
}

fn map_transaction(error: TransactionError) -> AiError {
    map_orm(error.public_error().clone())
}

fn map_orm(error: OrmPublicError) -> AiError {
    match error.code {
        OrmErrorCode::InvalidInput
        | OrmErrorCode::CursorInvalid
        | OrmErrorCode::PageLimitExceeded => AiError::InvalidInput(error.message),
        OrmErrorCode::Unauthenticated | OrmErrorCode::Forbidden => AiError::Forbidden,
        OrmErrorCode::NotFound => AiError::NotFound,
        OrmErrorCode::Conflict | OrmErrorCode::ConstraintViolation => AiError::Conflict,
        OrmErrorCode::ServiceUnavailable
        | OrmErrorCode::InternalError
        | OrmErrorCode::AuthorizationMisconfigured => AiError::PersistenceFailed,
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use std::sync::Arc;

    use agql_auth::FixedClock;
    use async_trait::async_trait;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use graphql_orm::graphql::orm::{ApplyOptions, Entity, OrmSchemaModule};
    use graphql_orm::prelude::{GraphQLEntity, GraphQLOperations, SqliteBackend};
    use hmac::{Hmac, Mac};
    use secrecy::SecretString;
    use sha2::Sha256;
    use time::OffsetDateTime;

    use super::*;
    use crate::{
        AiSchemaModule, AiSecretStore, OpenAiWebhookHeaders, OpenAiWebhookVerifier, SecretError,
        SecretRef,
    };

    #[derive(
        GraphQLEntity,
        GraphQLOperations,
        serde::Serialize,
        serde::Deserialize,
        Clone,
        Debug,
        PartialEq,
    )]
    #[graphql_entity(
        table = "graphql_orm_ai_provider_webhook_receipts",
        plural = "LegacyGraphqlOrmAiProviderWebhookReceipts",
        default_sort = "received_at DESC"
    )]
    struct LegacyWebhookReceiptRecord {
        #[primary_key]
        #[filterable(type = "uuid")]
        id: graphql_orm::uuid::Uuid,
        #[filterable(type = "string")]
        provider_kind: String,
        #[filterable(type = "string")]
        provider_event_id: String,
        provider_response_id: Option<String>,
        run_id: Option<graphql_orm::uuid::Uuid>,
        attempt_id: Option<graphql_orm::uuid::Uuid>,
        signature_verified: bool,
        state: String,
        safe_error_code: Option<String>,
        #[sortable]
        received_at: i64,
        processed_at: Option<i64>,
        #[graphql_orm(version, default = "0")]
        row_version: i64,
    }

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

    async fn database() -> Database<SqliteBackend> {
        let database = Database::<SqliteBackend>::connect_sqlite("sqlite::memory:")
            .await
            .expect("in-memory SQLite should open");
        let module = AiSchemaModule;
        let plan = database
            .schema()
            .plan_migration_to_entities(
                "ai-webhook-receipt-test-v1",
                "AI webhook receipt test",
                module.entities(),
            )
            .await
            .expect("webhook schema should plan");
        database
            .schema()
            .apply_migration(&plan, ApplyOptions::default())
            .await
            .expect("webhook schema should apply");
        database
    }

    #[tokio::test]
    async fn empty_legacy_webhook_placeholder_migrates_to_current_schema() {
        let database = Database::<SqliteBackend>::connect_sqlite("sqlite::memory:")
            .await
            .expect("in-memory SQLite should open");
        let legacy = [LegacyWebhookReceiptRecord::metadata()];
        let legacy_plan = database
            .schema()
            .plan_migration_to_entities(
                "ai-webhook-receipt-legacy-v1",
                "legacy AI webhook receipt test",
                &legacy,
            )
            .await
            .expect("legacy webhook schema should plan");
        database
            .schema()
            .apply_migration(&legacy_plan, ApplyOptions::default())
            .await
            .expect("legacy webhook schema should apply");

        let module = AiSchemaModule;
        let current_plan = database
            .schema()
            .plan_migration_to_entities(
                "ai-webhook-receipt-current-v1",
                "current AI webhook receipt test",
                module.entities(),
            )
            .await
            .expect("current webhook schema should plan");
        database
            .schema()
            .apply_migration(&current_plan, ApplyOptions::default())
            .await
            .expect("empty legacy webhook schema should migrate");
    }

    async fn verified_event_at(
        event_type: &str,
        event_id: &str,
        received_at: i64,
    ) -> OpenAiVerifiedWebhookEvent {
        let reference = SecretRef::parse("openai/webhook-receipt-test")
            .expect("test secret reference should parse");
        let secret = "synthetic-webhook-secret";
        let now = OffsetDateTime::from_unix_timestamp(received_at)
            .expect("test timestamp should be valid");
        let verifier = OpenAiWebhookVerifier::new(
            "profile-openai",
            reference.clone(),
            Arc::new(TestSecrets(reference, secret.to_owned())),
            Arc::new(FixedClock::new(now)),
        )
        .expect("verifier should build");
        let response_data = if event_type.starts_with("response.") {
            r#"{"id":"resp_background_1"}"#
        } else {
            r#"{"id":"batch_1"}"#
        };
        let body = format!(
            r#"{{"id":"{event_id}","type":"{event_type}","created_at":1999999999,"data":{response_data}}}"#
        );
        let delivery_id = format!("delivery-{event_id}");
        let timestamp = received_at.to_string();
        let mut signed = format!("{delivery_id}.{timestamp}.").into_bytes();
        signed.extend_from_slice(body.as_bytes());
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
            .expect("synthetic secret should be accepted");
        mac.update(&signed);
        let signature = STANDARD.encode(mac.finalize().into_bytes());
        let headers = OpenAiWebhookHeaders::new(delivery_id, timestamp, format!("v1,{signature}"))
            .expect("headers should validate");
        verifier
            .verify(&headers, body.as_bytes())
            .await
            .expect("event should verify")
    }

    async fn verified_event(event_type: &str, event_id: &str) -> OpenAiVerifiedWebhookEvent {
        verified_event_at(event_type, event_id, 2_000_000_000).await
    }

    #[tokio::test]
    async fn concurrent_exact_replay_inserts_one_receipt_and_audit() {
        let database = database().await;
        let service = OrmAiProviderWebhookReceiptService::new(database.clone());
        let event = verified_event("response.completed", "evt_receipt_1").await;
        let first_service = service.clone();
        let first_event = event.clone();
        let second_service = service.clone();
        let second_event = event.clone();
        let (first, second) = tokio::join!(
            first_service.record(&first_event),
            second_service.record(&second_event)
        );
        let outcomes = [
            first.expect("first intake should succeed"),
            second.expect("second intake should succeed"),
        ];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, AiProviderWebhookReceiptOutcome::Recorded))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(
                    outcome,
                    AiProviderWebhookReceiptOutcome::AlreadyRecorded
                ))
                .count(),
            1
        );
        let (receipts, audits) = database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    let receipts = tx
                        .query::<AiProviderWebhookReceiptRecord>()
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    let audits = tx
                        .query::<AiAuditEventRecord>()
                        .limit(2)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    Ok((receipts, audits))
                })
            })
            .await
            .expect("receipt facts should load");
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].state, "pending_reconciliation");
        assert_eq!(
            receipts[0].provider_response_id.as_deref(),
            Some("resp_background_1")
        );
        assert_eq!(audits.len(), 1);
        assert_eq!(audits[0].reason_code, "verified_event_recorded");
    }

    #[tokio::test]
    async fn unsupported_signed_event_is_durably_ignored_without_provider_reference() {
        let database = database().await;
        let service = OrmAiProviderWebhookReceiptService::new(database.clone());
        let event = verified_event("batch.completed", "evt_receipt_ignored").await;
        assert_eq!(
            service.record(&event).await.expect("intake should succeed"),
            AiProviderWebhookReceiptOutcome::Recorded
        );
        let receipt = database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    tx.query::<AiProviderWebhookReceiptRecord>()
                        .limit(1)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("receipt should load")
            .into_iter()
            .next()
            .expect("receipt should exist");
        assert_eq!(receipt.state, "ignored");
        assert_eq!(
            receipt.safe_error_code.as_deref(),
            Some("unsupported_event_kind")
        );
        assert!(receipt.provider_response_id.is_none());
        assert_eq!(receipt.processed_at, Some(2_000_000_000));
    }

    #[tokio::test]
    async fn later_exact_redelivery_preserves_first_receipt_and_conflicts_on_changed_facts() {
        let database = database().await;
        let service = OrmAiProviderWebhookReceiptService::new(database.clone());
        let first =
            verified_event_at("response.completed", "evt_receipt_replay", 2_000_000_000).await;
        let later =
            verified_event_at("response.completed", "evt_receipt_replay", 2_000_000_060).await;
        assert_eq!(
            service
                .record(&first)
                .await
                .expect("first intake should succeed"),
            AiProviderWebhookReceiptOutcome::Recorded
        );
        assert_eq!(
            service
                .record(&later)
                .await
                .expect("later exact redelivery should succeed"),
            AiProviderWebhookReceiptOutcome::AlreadyRecorded
        );

        let changed =
            verified_event_at("response.failed", "evt_receipt_replay", 2_000_000_060).await;
        assert!(matches!(
            service.record(&changed).await,
            Err(AiError::Conflict)
        ));

        let receipt = database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    tx.query::<AiProviderWebhookReceiptRecord>()
                        .limit(1)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("receipt should load")
            .into_iter()
            .next()
            .expect("receipt should exist");
        assert_eq!(receipt.received_at, 2_000_000_000);
        assert_eq!(receipt.provider_event_kind, "response_completed");
    }
}
