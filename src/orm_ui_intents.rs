//! Fenced persistence of exact schema-validated UI-intent suggestions.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::collections::BTreeMap;
use std::sync::Arc;

use agql_auth::{Clock, CurrentPrincipalResolver, PrincipalReferenceKind, ResolvedPrincipal};
use async_trait::async_trait;
use graphql_orm::graphql::errors::OrmPublicError;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use time::Duration;
use uuid::Uuid;

use crate::orm_runs::PreparedUiIntentEvent;
use crate::persistence::AiSessionRecord;
use crate::{
    AiAccessPolicy, AiContentProtectionPolicy, AiContentProtectionPolicyResolver,
    AiContentProtector, AiError, AiPersistedUiIntent, AiProviderCallResult, AiRunLease, AiScope,
    AiSessionAction, AiUiIntentCatalog, AiUiIntentDeliveryService, AiUiIntentDraft,
    AiUiIntentTypeBinding, AiUiIntentTypeId, ContentProtectionContext, OrmAiRunService,
    ProviderEvent,
};

/// Deployment bounds for durable UI-intent extraction and persistence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiUiIntentDeliveryLimits {
    maximum_envelope_bytes: usize,
    maximum_principal_age: Duration,
}

impl AiUiIntentDeliveryLimits {
    /// Creates validated delivery bounds.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless the provider envelope
    /// limit is in `128..=1 MiB` and principal freshness is positive and no
    /// more than one hour.
    pub fn new(
        maximum_envelope_bytes: usize,
        maximum_principal_age: Duration,
    ) -> Result<Self, AiError> {
        if !(128..=1024 * 1024).contains(&maximum_envelope_bytes)
            || !maximum_principal_age.is_positive()
            || maximum_principal_age > Duration::hours(1)
        {
            return Err(AiError::InvalidConfiguration(
                "invalid UI intent delivery limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_envelope_bytes,
            maximum_principal_age,
        })
    }

    /// Maximum complete visible JSON envelope bytes.
    pub const fn maximum_envelope_bytes(self) -> usize {
        self.maximum_envelope_bytes
    }

    /// Maximum accepted age of each freshly resolved principal.
    pub const fn maximum_principal_age(self) -> Duration {
        self.maximum_principal_age
    }
}

impl Default for AiUiIntentDeliveryLimits {
    fn default() -> Self {
        Self {
            maximum_envelope_bytes: 256 * 1024,
            maximum_principal_age: Duration::minutes(5),
        }
    }
}

/// ORM-backed fenced delivery of provider-produced UI-intent suggestions.
///
/// The service accepts only one strict JSON envelope assembled from exact
/// visible text deltas of a completed, tool-free provider result. It validates
/// the exact registered type/fingerprint/schema, rehydrates current authority
/// before and after protection, requires committed matching provider usage,
/// and atomically appends protected session/inbox events plus redacted audit.
/// It never constructs or executes a route.
pub struct OrmAiUiIntentDeliveryService {
    run_service: OrmAiRunService,
    principal_resolver: Arc<dyn CurrentPrincipalResolver>,
    access_policy: Arc<dyn AiAccessPolicy>,
    protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
    content_protector: Arc<dyn AiContentProtector>,
    catalog: Arc<AiUiIntentCatalog>,
    clock: Arc<dyn Clock>,
    limits: AiUiIntentDeliveryLimits,
}

impl OrmAiUiIntentDeliveryService {
    /// Creates a fail-closed UI-intent event service.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        run_service: OrmAiRunService,
        principal_resolver: Arc<dyn CurrentPrincipalResolver>,
        access_policy: Arc<dyn AiAccessPolicy>,
        protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
        content_protector: Arc<dyn AiContentProtector>,
        catalog: Arc<AiUiIntentCatalog>,
        clock: Arc<dyn Clock>,
        limits: AiUiIntentDeliveryLimits,
    ) -> Self {
        Self {
            run_service,
            principal_resolver,
            access_policy,
            protection_policy,
            content_protector,
            catalog,
            clock,
            limits,
        }
    }

    async fn current_policy(
        &self,
        lease: &AiRunLease,
        scope: &AiScope,
    ) -> Result<(ResolvedPrincipal, AiContentProtectionPolicy), AiError> {
        let principal = self
            .principal_resolver
            .resolve(lease.principal_reference())
            .await
            .map_err(|_| AiError::ReauthorizationFailed)?;
        let now = self.clock.now();
        if principal.reference() != lease.principal_reference()
            || principal.resolved_at() > now
            || now - principal.resolved_at() > self.limits.maximum_principal_age
            || principal
                .reference()
                .expires_at
                .is_some_and(|expires_at| expires_at <= now)
        {
            return Err(AiError::ReauthorizationFailed);
        }
        if !self
            .access_policy
            .can_access_scope(principal.principal(), scope, AiSessionAction::Write)
            .await
            .is_allowed()
            || !self
                .access_policy
                .can_access_session(
                    principal.principal(),
                    lease.session_id(),
                    AiSessionAction::Write,
                )
                .await
                .is_allowed()
        {
            return Err(AiError::Forbidden);
        }
        let policy = self
            .protection_policy
            .resolve(principal.principal(), scope)
            .await?;
        if !policy.ready || policy.scope != *scope {
            return Err(AiError::RuntimeNotReady);
        }
        Ok((principal, policy))
    }

    async fn protect(
        &self,
        policy: &AiContentProtectionPolicy,
        context: ContentProtectionContext,
        value: serde_json::Value,
    ) -> Result<serde_json::Value, AiError> {
        let envelope = self
            .content_protector
            .protect(policy, &context, value)
            .await
            .map_err(|error| match error {
                crate::ContentProtectionError::PolicyNotReady => AiError::RuntimeNotReady,
                _ => AiError::PersistenceFailed,
            })?;
        serde_json::to_value(envelope).map_err(|_| AiError::PersistenceFailed)
    }
}

#[async_trait]
impl AiUiIntentDeliveryService for OrmAiUiIntentDeliveryService {
    async fn persist_provider_suggestion(
        &self,
        lease: &AiRunLease,
        result: &AiProviderCallResult,
        binding: &AiUiIntentTypeBinding,
    ) -> Result<AiPersistedUiIntent, AiError> {
        if result.session_id() != lease.session_id()
            || result.run_id() != lease.run_id()
            || result.attempt_id() != lease.attempt_id()
            || result.lease_generation() != lease.lease_generation()
            || !result.tool_calls().is_empty()
            || result.usage().runs != 1
        {
            return Err(AiError::Conflict);
        }
        let envelope = extract_envelope(result, self.limits.maximum_envelope_bytes)?;
        if envelope.format_version != 1 {
            return Err(AiError::InvalidInput(
                "unsupported UI intent envelope version".to_owned(),
            ));
        }
        let intent_type = AiUiIntentTypeId::parse(envelope.intent_type)
            .map_err(|_| AiError::InvalidInput("invalid UI intent type".to_owned()))?;
        let mut intent = self.catalog.validate_bound(
            binding,
            AiUiIntentDraft {
                intent_type,
                payload: envelope.payload,
            },
        )?;

        let session =
            AiSessionRecord::find_by_id(self.run_service.database(), &lease.session_id().0)
                .await
                .map_err(|error| map_orm(OrmPublicError::from(error)))?
                .ok_or(AiError::NotFound)?;
        let scope = AiScope {
            kind: session.scope_kind.clone(),
            id: session.scope_id.clone(),
            tenant_id: session.tenant_id.clone(),
        };
        validate_session_binding(&session, lease, &scope)?;
        let (principal, policy) = self.current_policy(lease, &scope).await?;
        let source_hash = source_hash(lease, result, binding, &intent.payload)?;
        let intent_id = derived_uuid(b"graphql-orm-ai/ui-intent/session/v1\0", &source_hash);
        let inbox_event_id = derived_uuid(b"graphql-orm-ai/ui-intent/inbox/v1\0", &source_hash);
        intent.id = intent_id;
        let correlation_id = format!("ui-intent:{}", hex::encode(source_hash));
        let payload = json!({
            "formatVersion": 1,
            "intentId": intent_id,
            "runId": lease.run_id().0,
            "providerKind": result.provider_kind().as_str(),
            "providerModel": result.provider_model(),
            "providerResponseId": result.provider_response_id(),
            "budgetReservationId": result.budget_reservation_id().0,
            "intentType": binding.intent_type.as_str(),
            "descriptorFingerprint": binding.descriptor_fingerprint,
            "payload": intent.payload,
            "sourceHash": hex::encode(source_hash),
        });
        let protected_payload = self
            .protect(
                &policy,
                content_context(
                    "graphql_orm_ai_session_events",
                    intent_id,
                    "protected_payload",
                    &scope,
                ),
                payload,
            )
            .await?;
        let protected_inbox_payload = self
            .protect(
                &policy,
                content_context(
                    "graphql_orm_ai_inbox_events",
                    inbox_event_id,
                    "protected_payload",
                    &scope,
                ),
                json!({
                    "formatVersion": 1,
                    "sessionId": lease.session_id().0,
                    "runId": lease.run_id().0,
                    "intentId": intent_id,
                    "intentType": binding.intent_type.as_str(),
                    "descriptorFingerprint": binding.descriptor_fingerprint,
                }),
            )
            .await?;
        let (current, current_policy) = self.current_policy(lease, &scope).await?;
        if current.reference() != principal.reference() || current_policy != policy {
            return Err(AiError::ReauthorizationFailed);
        }
        let (lease, event_sequence) = self
            .run_service
            .append_ui_intent_event(
                lease,
                PreparedUiIntentEvent {
                    id: intent_id,
                    inbox_event_id,
                    protected_payload,
                    protected_inbox_payload,
                    correlation_id,
                    provider_kind: result.provider_kind().as_str().to_owned(),
                    provider_model: result.provider_model().to_owned(),
                    provider_response_id: result.provider_response_id().map(str::to_owned),
                    budget_reservation_id: result.budget_reservation_id().0,
                    usage: result.usage(),
                    cached_input_tokens: result.cached_input_tokens(),
                    expected_owner_principal_kind: session.owner_principal_kind,
                    expected_owner_subject: session.owner_subject,
                    expected_scope_kind: scope.kind,
                    expected_scope_id: scope.id,
                    expected_tenant_id: scope.tenant_id,
                },
            )
            .await?;
        Ok(AiPersistedUiIntent::new(intent, event_sequence, lease))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProviderUiIntentEnvelope {
    format_version: u32,
    intent_type: String,
    payload: serde_json::Value,
}

fn extract_envelope(
    result: &AiProviderCallResult,
    maximum_bytes: usize,
) -> Result<ProviderUiIntentEnvelope, AiError> {
    let mut text = String::new();
    let mut started = false;
    let mut usage_seen = false;
    let mut completed = false;
    for event in result.events() {
        match event {
            ProviderEvent::ResponseStarted { response_id } => {
                if started || usage_seen || completed || !text.is_empty() {
                    return Err(AiError::InvalidInput(
                        "UI intent provider event sequence is invalid".to_owned(),
                    ));
                }
                validate_response_id(response_id.as_deref(), result.provider_response_id())?;
                started = true;
            }
            ProviderEvent::TextDelta { text: delta } => {
                if !started || usage_seen || completed {
                    return Err(AiError::InvalidInput(
                        "UI intent provider event sequence is invalid".to_owned(),
                    ));
                }
                if text
                    .len()
                    .checked_add(delta.len())
                    .is_none_or(|size| size > maximum_bytes)
                {
                    return Err(AiError::InvalidInput(
                        "UI intent envelope exceeds delivery limits".to_owned(),
                    ));
                }
                text.push_str(delta);
            }
            ProviderEvent::Usage {
                input_tokens,
                output_tokens,
                cached_input_tokens,
            } => {
                if !started
                    || usage_seen
                    || completed
                    || *input_tokens != result.usage().input_tokens
                    || *output_tokens != result.usage().output_tokens
                    || *cached_input_tokens != result.cached_input_tokens()
                {
                    return Err(AiError::InvalidInput(
                        "UI intent provider usage evidence is invalid".to_owned(),
                    ));
                }
                usage_seen = true;
            }
            ProviderEvent::ResponseCompleted { response_id } => {
                if !started || !usage_seen || completed {
                    return Err(AiError::InvalidInput(
                        "UI intent provider event sequence is invalid".to_owned(),
                    ));
                }
                validate_response_id(response_id.as_deref(), result.provider_response_id())?;
                completed = true;
            }
            ProviderEvent::ReasoningSummaryDelta { .. }
            | ProviderEvent::ToolCallStarted { .. }
            | ProviderEvent::ToolArgumentsDelta { .. }
            | ProviderEvent::ToolCallCompleted { .. }
            | ProviderEvent::BuiltinToolStarted { .. }
            | ProviderEvent::BuiltinToolCompleted { .. }
            | ProviderEvent::Citation { .. }
            | ProviderEvent::Unknown { .. } => {
                return Err(AiError::InvalidInput(
                    "UI intent provider output is not a strict envelope".to_owned(),
                ));
            }
        }
    }
    if text.is_empty() || !started || !usage_seen || !completed {
        return Err(AiError::InvalidInput(
            "UI intent provider output is incomplete".to_owned(),
        ));
    }
    serde_json::from_str(&text)
        .map_err(|_| AiError::InvalidInput("invalid UI intent provider envelope".to_owned()))
}

fn validate_response_id(observed: Option<&str>, expected: Option<&str>) -> Result<(), AiError> {
    if observed.is_some_and(|observed| Some(observed) != expected) {
        return Err(AiError::Conflict);
    }
    Ok(())
}

fn validate_session_binding(
    session: &AiSessionRecord,
    lease: &AiRunLease,
    scope: &AiScope,
) -> Result<(), AiError> {
    let expected_kind = match &lease.principal_reference().kind {
        PrincipalReferenceKind::UserSession => "user".to_owned(),
        PrincipalReferenceKind::ApiToken { principal_kind } => {
            format!("api_token:{principal_kind}")
        }
    };
    if session.id != lease.session_id().0
        || session.state != "active"
        || session.deleted_at.is_some()
        || session.owner_principal_kind != expected_kind
        || session.owner_subject != lease.principal_reference().subject
        || session.scope_kind != scope.kind
        || session.scope_id != scope.id
        || session.tenant_id != scope.tenant_id
        || lease
            .principal_reference()
            .tenant_id
            .as_ref()
            .is_some_and(|tenant_id| scope.tenant_id.as_ref() != Some(tenant_id))
    {
        return Err(AiError::Forbidden);
    }
    Ok(())
}

fn source_hash(
    lease: &AiRunLease,
    result: &AiProviderCallResult,
    binding: &AiUiIntentTypeBinding,
    payload: &serde_json::Value,
) -> Result<[u8; 32], AiError> {
    let value = json!({
        "format": "graphql-orm-ai-ui-intent-source-v1",
        "session_id": lease.session_id().0,
        "run_id": lease.run_id().0,
        "attempt_id": lease.attempt_id(),
        "lease_generation": lease.lease_generation(),
        "provider_kind": result.provider_kind().as_str(),
        "provider_model": result.provider_model(),
        "provider_response_id": result.provider_response_id(),
        "budget_reservation_id": result.budget_reservation_id().0,
        "intent_type": binding.intent_type.as_str(),
        "descriptor_fingerprint": binding.descriptor_fingerprint,
        "payload": payload,
    });
    let bytes =
        serde_json::to_vec(&canonical_json(&value)).map_err(|_| AiError::PersistenceFailed)?;
    Ok(Sha256::digest(bytes).into())
}

fn derived_uuid(domain: &[u8], source_hash: &[u8; 32]) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(source_hash);
    let digest = hash.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn content_context(
    entity: &str,
    row_id: Uuid,
    field: &str,
    scope: &AiScope,
) -> ContentProtectionContext {
    ContentProtectionContext {
        entity: entity.to_owned(),
        row_id: row_id.to_string(),
        field: field.to_owned(),
        scope: scope.clone(),
    }
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

fn map_orm(error: OrmPublicError) -> AiError {
    use graphql_orm::graphql::errors::OrmErrorCode;
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
    use super::*;
    use agql_auth::{
        AccessTokenMetadata, AuthPrincipal, AuthUser, FixedClock, PrincipalReference,
        SessionContext,
    };
    use graphql_orm::graphql::orm::{ApplyOptions, OrmSchemaModule, TransactionMode};
    use graphql_orm::prelude::{Database, SqliteBackend};
    use time::OffsetDateTime;

    use crate::orm_runs::{
        PreparedProviderBlock, PreparedProviderOutput, final_output_checkpoint_hash,
    };
    use crate::persistence::{
        AiAuditEventRecord, AiBudgetReservationRecord, AiInboxEventRecord, AiRunRecord,
        AiSessionEventRecord, CreateAiBudgetReservationRecordInput, CreateAiRunRecordInput,
        CreateAiSessionRecordInput,
    };
    use crate::{
        AiAccessDecision, AiBudgetReservationId, AiContentProtectionMode, AiUiIntentTypeDescriptor,
        DatabaseManagedContentProtector, ProtectedContentEnvelope,
    };

    #[derive(Clone)]
    struct Resolver {
        principal: AuthPrincipal,
        now: OffsetDateTime,
    }

    #[async_trait]
    impl CurrentPrincipalResolver for Resolver {
        async fn resolve(
            &self,
            reference: &PrincipalReference,
        ) -> agql_auth::AuthResult<ResolvedPrincipal> {
            ResolvedPrincipal::new(reference.clone(), self.principal.clone(), self.now)
        }
    }

    struct Allow;

    #[async_trait]
    impl AiAccessPolicy for Allow {
        async fn can_access_scope(
            &self,
            _principal: &AuthPrincipal,
            _scope: &AiScope,
            _action: AiSessionAction,
        ) -> AiAccessDecision {
            AiAccessDecision::allow("ui_intent_test", "ui-intent-test-v1")
        }

        async fn can_access_session(
            &self,
            _principal: &AuthPrincipal,
            _session_id: crate::AiSessionId,
            _action: AiSessionAction,
        ) -> AiAccessDecision {
            AiAccessDecision::allow("ui_intent_test", "ui-intent-test-v1")
        }
    }

    struct Protection(AiScope);

    #[async_trait]
    impl AiContentProtectionPolicyResolver for Protection {
        async fn resolve(
            &self,
            _principal: &AuthPrincipal,
            scope: &AiScope,
        ) -> Result<AiContentProtectionPolicy, AiError> {
            if scope != &self.0 {
                return Err(AiError::Forbidden);
            }
            Ok(AiContentProtectionPolicy {
                scope: scope.clone(),
                mode: AiContentProtectionMode::DatabaseManaged,
                key_policy_reference: None,
                version: 1,
                ready: true,
            })
        }
    }

    struct Fixture {
        database: Database<SqliteBackend>,
        run_service: OrmAiRunService,
        delivery: OrmAiUiIntentDeliveryService,
        binding: AiUiIntentTypeBinding,
        principal: AuthPrincipal,
        scope: AiScope,
        now: OffsetDateTime,
    }

    async fn fixture() -> Fixture {
        let database = Database::<SqliteBackend>::connect_sqlite("sqlite::memory:")
            .await
            .expect("in-memory SQLite should open");
        let module = crate::AiSchemaModule;
        let plan = database
            .schema()
            .plan_migration_to_entities(
                "ai-ui-intent-delivery-test-v1",
                "AI UI intent delivery test",
                module.entities(),
            )
            .await
            .expect("AI schema should plan");
        database
            .schema()
            .apply_migration(&plan, ApplyOptions::default())
            .await
            .expect("AI schema should apply");
        let now = OffsetDateTime::from_unix_timestamp(1_800_000_000)
            .expect("fixed timestamp should be valid");
        let principal = AuthPrincipal::User(AuthUser {
            user_id: "ui-intent-user".to_owned(),
            session_id: Uuid::new_v4(),
            roles: Vec::new(),
            scopes: Vec::new(),
            session: SessionContext::default(),
            token_claims: AccessTokenMetadata {
                tenant_id: Some("tenant-ui-intent".to_owned()),
                ..AccessTokenMetadata::default()
            },
        });
        let scope = AiScope::new("tenant", "tenant-ui-intent").with_tenant_id("tenant-ui-intent");
        let clock = Arc::new(FixedClock::new(now));
        let run_service = OrmAiRunService::new(
            database.clone(),
            clock.clone(),
            crate::AiRunServiceLimits::new(Duration::minutes(5), Duration::hours(1), 16, 2, 8)
                .expect("run limits should validate"),
        );
        let descriptor = AiUiIntentTypeDescriptor::new(
            "application.record.focus.v1",
            "1",
            json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"recordId": {"type": "string", "maxLength": 128}},
                "required": ["recordId"],
                "additionalProperties": false
            }),
        )
        .expect("UI intent descriptor should validate");
        let binding = descriptor.binding();
        let mut catalog = AiUiIntentCatalog::new();
        catalog
            .register(descriptor)
            .expect("UI intent descriptor should register");
        let delivery = OrmAiUiIntentDeliveryService::new(
            run_service.clone(),
            Arc::new(Resolver {
                principal: principal.clone(),
                now,
            }),
            Arc::new(Allow),
            Arc::new(Protection(scope.clone())),
            Arc::new(DatabaseManagedContentProtector),
            Arc::new(catalog),
            clock,
            AiUiIntentDeliveryLimits::default(),
        );
        Fixture {
            database,
            run_service,
            delivery,
            binding,
            principal,
            scope,
            now,
        }
    }

    async fn seed_running(fixture: &Fixture) -> (AiRunLease, AiBudgetReservationId) {
        let session_id = crate::AiSessionId::new();
        let run_id = crate::AiRunId::new();
        AiSessionRecord::insert(
            &fixture.database,
            CreateAiSessionRecordInput {
                id: session_id.0,
                owner_principal_kind: "user".to_owned(),
                owner_subject: fixture.principal.subject().to_owned(),
                tenant_id: fixture.scope.tenant_id.clone(),
                scope_kind: fixture.scope.kind.clone(),
                scope_id: fixture.scope.id.clone(),
                title: "UI intent delivery test".to_owned(),
                state: "active".to_owned(),
                stream_head: 0,
                message_head: 0,
                last_activity_at: fixture.now.unix_timestamp(),
                archived_at: None,
                deleted_at: None,
            },
        )
        .await
        .expect("test session should insert");
        AiRunRecord::insert(
            &fixture.database,
            CreateAiRunRecordInput {
                id: run_id.0,
                session_id: session_id.0,
                input_message_id: Uuid::new_v4(),
                principal_reference: serde_json::to_value(fixture.principal.reference())
                    .expect("principal reference should serialize"),
                state: "queued".to_owned(),
                attempt_id: None,
                lease_owner: None,
                lease_generation: 0,
                lease_expires_at: None,
                lease_heartbeat_at: None,
                retry_count: 0,
                next_attempt_at: Some(fixture.now.unix_timestamp()),
                error_code: None,
                latest_checkpoint_id: None,
            },
        )
        .await
        .expect("test run should insert");
        let claimed = fixture
            .run_service
            .claim_next("ui-intent-worker")
            .await
            .expect("claim should succeed")
            .expect("test run should be claimable");
        let lease = fixture
            .run_service
            .start(&claimed)
            .await
            .expect("test run should start");
        let reservation = AiBudgetReservationRecord::insert(
            &fixture.database,
            CreateAiBudgetReservationRecordInput {
                budget_counter_ids: json!([]),
                scope_kind: fixture.scope.kind.clone(),
                scope_id: fixture.scope.id.clone(),
                tenant_id: fixture.scope.tenant_id.clone(),
                principal_kind: "user".to_owned(),
                principal_subject: fixture.principal.subject().to_owned(),
                session_id: lease.session_id().0,
                run_id: lease.run_id().0,
                attempt_id: lease.attempt_id(),
                lease_generation: lease.lease_generation(),
                provider_kind: "openai".to_owned(),
                provider_model: "ui-intent-test-model".to_owned(),
                pricing_policy_version: "ui-intent-test-pricing-v1".to_owned(),
                reserved_input_tokens: 1,
                reserved_output_tokens: 1,
                reserved_tool_units: 0,
                reserved_image_units: 0,
                reserved_cost_microunits: 0,
                reserved_runs: 1,
                actual_input_tokens: Some(1),
                actual_cached_input_tokens: Some(0),
                actual_output_tokens: Some(1),
                actual_tool_units: Some(0),
                actual_image_units: Some(0),
                actual_cost_microunits: Some(0),
                actual_runs: Some(1),
                idempotency_key: format!("ui-intent:{}", lease.attempt_id()),
                state: "committed".to_owned(),
                expires_at: (fixture.now + Duration::minutes(5)).unix_timestamp(),
                reconciled_at: Some(fixture.now.unix_timestamp()),
            },
        )
        .await
        .expect("committed budget proof should insert");
        (lease, AiBudgetReservationId(reservation.id))
    }

    fn envelope(binding: &AiUiIntentTypeBinding) -> serde_json::Value {
        json!({
            "formatVersion": 1,
            "intentType": binding.intent_type.as_str(),
            "payload": {"recordId": "record-54"}
        })
    }

    async fn persist_assistant_output(
        fixture: &Fixture,
        lease: &AiRunLease,
        reservation_id: AiBudgetReservationId,
    ) -> AiRunLease {
        let message_id = Uuid::new_v4();
        fixture
            .run_service
            .append_provider_output(
                lease,
                PreparedProviderOutput {
                    message_id,
                    event_id: Uuid::new_v4(),
                    inbox_event_id: Uuid::new_v4(),
                    provider_kind: "openai".to_owned(),
                    provider_model: "ui-intent-test-model".to_owned(),
                    protected_preview: json!({"protection": "database_managed", "value": {}}),
                    protected_event: json!({"protection": "database_managed", "value": {}}),
                    protected_inbox_event: json!({
                        "protection": "database_managed",
                        "value": {}
                    }),
                    blocks: vec![PreparedProviderBlock {
                        id: Uuid::new_v4(),
                        kind: "text".to_owned(),
                        protected_content: json!({
                            "protection": "database_managed",
                            "value": "provider envelope"
                        }),
                        byte_count: 17,
                        line_count: 1,
                    }],
                    correlation_id: reservation_id.0.to_string(),
                    provider_response_id: Some("ui-intent-response".to_owned()),
                    budget_reservation_id: reservation_id.0,
                    checkpoint_hash: final_output_checkpoint_hash(
                        lease.run_id(),
                        lease.attempt_id(),
                        lease.lease_generation(),
                        message_id,
                        Some("ui-intent-response"),
                        reservation_id.0,
                    ),
                    expected_owner_principal_kind: "user".to_owned(),
                    expected_owner_subject: fixture.principal.subject().to_owned(),
                    expected_scope_kind: fixture.scope.kind.clone(),
                    expected_scope_id: fixture.scope.id.clone(),
                    expected_tenant_id: fixture.scope.tenant_id.clone(),
                },
            )
            .await
            .expect("assistant output checkpoint should persist")
    }

    #[tokio::test]
    async fn exact_provider_suggestion_is_protected_fenced_and_idempotent() {
        let fixture = fixture().await;
        let (started, reservation_id) = seed_running(&fixture).await;
        let lease = persist_assistant_output(&fixture, &started, reservation_id).await;
        let result = AiProviderCallResult::test_ui_intent_result(
            &lease,
            reservation_id,
            envelope(&fixture.binding),
        );
        let persisted = fixture
            .delivery
            .persist_provider_suggestion(&lease, &result, &fixture.binding)
            .await
            .expect("exact validated suggestion should persist");
        assert_eq!(persisted.event_sequence(), 2);
        assert_eq!(persisted.intent().payload, json!({"recordId": "record-54"}));
        let event_id = persisted.intent().id;
        let renewed_row_version = AiRunRecord::find_by_id(&fixture.database, &lease.run_id().0)
            .await
            .expect("run lookup should succeed")
            .expect("run should exist")
            .row_version;

        let replayed = fixture
            .delivery
            .persist_provider_suggestion(&lease, &result, &fixture.binding)
            .await
            .expect("exact replay should resolve idempotently");
        assert_eq!(replayed.intent().id, event_id);
        assert_eq!(replayed.event_sequence(), 2);
        assert_eq!(
            AiRunRecord::find_by_id(&fixture.database, &lease.run_id().0)
                .await
                .expect("run lookup should succeed")
                .expect("run should exist")
                .row_version,
            renewed_row_version,
            "idempotent replay must not rotate the fence again"
        );

        let event = AiSessionEventRecord::find_by_id(&fixture.database, &event_id)
            .await
            .expect("event lookup should succeed")
            .expect("session event should exist");
        assert_eq!(event.event_type, "ui_intent_suggested");
        let protected: ProtectedContentEnvelope = serde_json::from_value(event.protected_payload)
            .expect("protected event envelope should decode");
        let opened = DatabaseManagedContentProtector
            .open(
                &AiContentProtectionPolicy {
                    scope: fixture.scope.clone(),
                    mode: AiContentProtectionMode::DatabaseManaged,
                    key_policy_reference: None,
                    version: 1,
                    ready: true,
                },
                &content_context(
                    "graphql_orm_ai_session_events",
                    event_id,
                    "protected_payload",
                    &fixture.scope,
                ),
                &protected,
            )
            .await
            .expect("exact protected event should open");
        assert_eq!(opened["intentType"], fixture.binding.intent_type.as_str());
        assert_eq!(opened["payload"], json!({"recordId": "record-54"}));
        assert!(opened.get("route").is_none());
        assert!(opened.get("url").is_none());

        let source = source_hash(
            &lease,
            &result,
            &fixture.binding,
            &json!({"recordId": "record-54"}),
        )
        .expect("source hash should compute");
        let inbox_id = derived_uuid(b"graphql-orm-ai/ui-intent/inbox/v1\0", &source);
        let inbox = AiInboxEventRecord::find_by_id(&fixture.database, &inbox_id)
            .await
            .expect("inbox lookup should succeed")
            .expect("inbox event should exist");
        assert_eq!(inbox.event_type, "ui_intent_suggested");
        assert_eq!(inbox.session_id, Some(lease.session_id().0));
        let audits = fixture
            .database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    tx.query::<AiAuditEventRecord>()
                        .limit(8)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("audit query should succeed");
        assert_eq!(
            audits
                .iter()
                .filter(|event| event.action == "ai.ui_intent.persist")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn stale_binding_invalid_shape_and_missing_budget_fail_closed() {
        let fixture = fixture().await;
        let (started, reservation_id) = seed_running(&fixture).await;
        let before_output = AiProviderCallResult::test_ui_intent_result(
            &started,
            reservation_id,
            envelope(&fixture.binding),
        );
        assert!(matches!(
            fixture
                .delivery
                .persist_provider_suggestion(&started, &before_output, &fixture.binding)
                .await,
            Err(AiError::Conflict)
        ));
        let lease = persist_assistant_output(&fixture, &started, reservation_id).await;
        let mut stale_binding = fixture.binding.clone();
        stale_binding.descriptor_fingerprint = "0".repeat(64);
        let exact_result = AiProviderCallResult::test_ui_intent_result(
            &lease,
            reservation_id,
            envelope(&fixture.binding),
        );
        assert!(matches!(
            fixture
                .delivery
                .persist_provider_suggestion(&lease, &exact_result, &stale_binding)
                .await,
            Err(AiError::Conflict)
        ));

        let invalid_result = AiProviderCallResult::test_ui_intent_result(
            &lease,
            reservation_id,
            json!({
                "formatVersion": 1,
                "intentType": fixture.binding.intent_type.as_str(),
                "payload": {"recordId": "record-54"},
                "route": "/records/54"
            }),
        );
        assert!(matches!(
            fixture
                .delivery
                .persist_provider_suggestion(&lease, &invalid_result, &fixture.binding)
                .await,
            Err(AiError::InvalidInput(_))
        ));

        let missing_budget = AiProviderCallResult::test_ui_intent_result(
            &lease,
            AiBudgetReservationId::new(),
            envelope(&fixture.binding),
        );
        assert!(matches!(
            fixture
                .delivery
                .persist_provider_suggestion(&lease, &missing_budget, &fixture.binding)
                .await,
            Err(AiError::Conflict)
        ));
        let session = AiSessionRecord::find_by_id(&fixture.database, &lease.session_id().0)
            .await
            .expect("session lookup should succeed")
            .expect("session should exist");
        assert_eq!(session.stream_head, 1);
    }
}
