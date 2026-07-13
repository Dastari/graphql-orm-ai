//! Fenced, content-protected persistence for completed provider turns.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::sync::Arc;

use agql_auth::{Clock, CurrentPrincipalResolver, PrincipalReferenceKind};
use graphql_orm::graphql::errors::OrmPublicError;
use serde_json::json;
use time::Duration;
use uuid::Uuid;

use crate::orm_runs::{
    PreparedProviderBlock, PreparedProviderOutput, final_output_checkpoint_hash,
};
use crate::persistence::*;
use crate::{
    AiAccessPolicy, AiContentProtectionPolicyResolver, AiContentProtector, AiError,
    AiProviderCallResult, AiRunLease, AiScope, AiSessionAction, AiSessionId,
    ContentProtectionContext, OrmAiRunService, ProviderEvent,
};

/// Deployment hard limits for completed assistant-message persistence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiProviderOutputLimits {
    maximum_block_bytes: usize,
    maximum_blocks: usize,
    maximum_preview_bytes: usize,
    maximum_total_bytes: usize,
    maximum_principal_age: Duration,
}

impl AiProviderOutputLimits {
    /// Creates validated transcript persistence limits.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless block/preview/total
    /// bounds are positive, block count is within `1..=4096`, total capacity is
    /// at least one block and at most 64 MiB, and principal freshness is
    /// positive.
    pub fn new(
        maximum_block_bytes: usize,
        maximum_blocks: usize,
        maximum_preview_bytes: usize,
        maximum_total_bytes: usize,
        maximum_principal_age: Duration,
    ) -> Result<Self, AiError> {
        if maximum_block_bytes == 0
            || !(1..=4_096).contains(&maximum_blocks)
            || maximum_preview_bytes == 0
            || maximum_preview_bytes > maximum_block_bytes
            || maximum_total_bytes < maximum_block_bytes
            || maximum_total_bytes > 64 * 1024 * 1024
            || !maximum_principal_age.is_positive()
        {
            return Err(AiError::InvalidConfiguration(
                "invalid provider-output limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_block_bytes,
            maximum_blocks,
            maximum_preview_bytes,
            maximum_total_bytes,
            maximum_principal_age,
        })
    }
}

/// Durable result of a fenced assistant-message append.
#[derive(Clone, Debug)]
pub struct AiPersistedProviderOutput {
    message_id: Uuid,
    block_count: usize,
    lease: AiRunLease,
}

impl AiPersistedProviderOutput {
    /// Assistant message identifier.
    pub const fn message_id(&self) -> Uuid {
        self.message_id
    }

    /// Number of separately windowable content blocks.
    pub const fn block_count(&self) -> usize {
        self.block_count
    }

    /// Renewed lease proof required for the next fenced write or completion.
    pub fn lease(&self) -> &AiRunLease {
        &self.lease
    }

    /// Consumes the result and returns its renewed lease proof.
    pub fn into_lease(self) -> AiRunLease {
        self.lease
    }

    #[cfg(test)]
    pub(crate) fn test_output(lease: AiRunLease) -> Self {
        Self {
            message_id: Uuid::new_v4(),
            block_count: 1,
            lease,
        }
    }
}

/// Persists a successful provider result as a protected assistant message,
/// windowable blocks, and one durable session event.
pub struct OrmAiProviderOutputService {
    run_service: OrmAiRunService,
    principal_resolver: Arc<dyn CurrentPrincipalResolver>,
    access_policy: Arc<dyn AiAccessPolicy>,
    protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
    content_protector: Arc<dyn AiContentProtector>,
    clock: Arc<dyn Clock>,
    limits: AiProviderOutputLimits,
}

impl OrmAiProviderOutputService {
    /// Creates a fenced provider-output service.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        run_service: OrmAiRunService,
        principal_resolver: Arc<dyn CurrentPrincipalResolver>,
        access_policy: Arc<dyn AiAccessPolicy>,
        protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
        content_protector: Arc<dyn AiContentProtector>,
        clock: Arc<dyn Clock>,
        limits: AiProviderOutputLimits,
    ) -> Self {
        Self {
            run_service,
            principal_resolver,
            access_policy,
            protection_policy,
            content_protector,
            clock,
            limits,
        }
    }

    /// Persists one exactly bound successful provider result.
    ///
    /// Current principal authority, owner/tenant/session state, scope access,
    /// protection readiness, and the complete run fence are rechecked. The
    /// message shell remains small while potentially large output is split into
    /// independently fetched blocks.
    ///
    /// # Errors
    ///
    /// Fails closed for a swapped result, stale principal/fence, owner/scope or
    /// policy denial, unready content protection, oversized/malformed output,
    /// CAS conflict, or persistence failure.
    pub async fn persist(
        &self,
        lease: &AiRunLease,
        result: &AiProviderCallResult,
    ) -> Result<AiPersistedProviderOutput, AiError> {
        if result.session_id() != lease.session_id()
            || result.run_id() != lease.run_id()
            || result.attempt_id() != lease.attempt_id()
            || result.lease_generation() != lease.lease_generation()
            || !result.tool_calls().is_empty()
        {
            return Err(AiError::Conflict);
        }
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

        let principal = self
            .principal_resolver
            .resolve(lease.principal_reference())
            .await
            .map_err(|_| AiError::ReauthorizationFailed)?;
        let now = self.clock.now();
        if principal.resolved_at() > now
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
            .can_access_scope(principal.principal(), &scope, AiSessionAction::Write)
            .await
            .is_allowed()
            || !self
                .access_policy
                .can_access_session(
                    principal.principal(),
                    AiSessionId(session.id),
                    AiSessionAction::Write,
                )
                .await
                .is_allowed()
        {
            return Err(AiError::Forbidden);
        }
        let policy = self
            .protection_policy
            .resolve(principal.principal(), &scope)
            .await?;
        if !policy.ready || policy.scope != scope {
            return Err(AiError::RuntimeNotReady);
        }

        let message_id = Uuid::new_v4();
        let event_id = Uuid::new_v4();
        let inbox_event_id = Uuid::new_v4();
        let raw_blocks = normalize_blocks(result.events(), self.limits)?;
        let preview_text = raw_blocks
            .iter()
            .find_map(|block| (block.kind == "text").then_some(block.preview_text.as_str()))
            .unwrap_or("[structured assistant response]");
        let preview_text = bounded_prefix(preview_text, self.limits.maximum_preview_bytes);
        let protected_preview = self
            .protect(
                &policy,
                context(
                    "graphql_orm_ai_messages",
                    message_id,
                    "protected_preview",
                    &scope,
                ),
                json!({"text": preview_text}),
            )
            .await?;

        let mut blocks = Vec::with_capacity(raw_blocks.len());
        for raw in raw_blocks {
            let block_id = Uuid::new_v4();
            let protected_content = self
                .protect(
                    &policy,
                    context(
                        "graphql_orm_ai_message_blocks",
                        block_id,
                        "protected_content",
                        &scope,
                    ),
                    raw.value,
                )
                .await?;
            blocks.push(PreparedProviderBlock {
                id: block_id,
                kind: raw.kind,
                protected_content,
                byte_count: i64::try_from(raw.byte_count)
                    .map_err(|_| AiError::InvalidInput("provider output too large".to_owned()))?,
                line_count: i64::try_from(raw.line_count)
                    .map_err(|_| AiError::InvalidInput("provider output too large".to_owned()))?,
            });
        }
        let protected_event = self
            .protect(
                &policy,
                context(
                    "graphql_orm_ai_session_events",
                    event_id,
                    "protected_payload",
                    &scope,
                ),
                json!({
                    "messageId": message_id,
                    "runId": lease.run_id().0,
                    "blockCount": blocks.len(),
                    "budgetReservationId": result.budget_reservation_id().0,
                }),
            )
            .await?;
        let protected_inbox_event = self
            .protect(
                &policy,
                context(
                    "graphql_orm_ai_inbox_events",
                    inbox_event_id,
                    "protected_payload",
                    &scope,
                ),
                json!({
                    "sessionId": lease.session_id().0,
                    "messageId": message_id,
                    "runId": lease.run_id().0,
                    "blockCount": blocks.len(),
                }),
            )
            .await?;
        let block_count = blocks.len();
        let checkpoint_hash = final_output_checkpoint_hash(
            lease.run_id(),
            lease.attempt_id(),
            lease.lease_generation(),
            message_id,
            result.provider_response_id(),
            result.budget_reservation_id().0,
        );
        let renewed = self
            .run_service
            .append_provider_output(
                lease,
                PreparedProviderOutput {
                    message_id,
                    event_id,
                    inbox_event_id,
                    provider_kind: result.provider_kind().as_str().to_owned(),
                    provider_model: result.provider_model().to_owned(),
                    protected_preview,
                    protected_event,
                    protected_inbox_event,
                    blocks,
                    correlation_id: result.budget_reservation_id().0.to_string(),
                    provider_response_id: result.provider_response_id().map(str::to_owned),
                    budget_reservation_id: result.budget_reservation_id().0,
                    checkpoint_hash,
                    expected_owner_principal_kind: session.owner_principal_kind,
                    expected_owner_subject: session.owner_subject,
                    expected_scope_kind: scope.kind,
                    expected_scope_id: scope.id,
                    expected_tenant_id: scope.tenant_id,
                },
            )
            .await?;
        Ok(AiPersistedProviderOutput {
            message_id,
            block_count,
            lease: renewed,
        })
    }

    async fn protect(
        &self,
        policy: &crate::AiContentProtectionPolicy,
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

struct RawBlock {
    kind: String,
    value: serde_json::Value,
    preview_text: String,
    byte_count: usize,
    line_count: usize,
}

fn normalize_blocks(
    events: &[ProviderEvent],
    limits: AiProviderOutputLimits,
) -> Result<Vec<RawBlock>, AiError> {
    let mut text = String::new();
    let mut reasoning = String::new();
    let mut structured = Vec::new();
    for event in events {
        match event {
            ProviderEvent::TextDelta { text: delta } => text.push_str(delta),
            ProviderEvent::ReasoningSummaryDelta { text: delta } => reasoning.push_str(delta),
            ProviderEvent::Citation { source, title } => {
                structured.push(("citation", json!({"source": source, "title": title})))
            }
            ProviderEvent::BuiltinToolCompleted { call_id, result } => structured.push((
                "builtin_tool_result",
                json!({"callId": call_id, "result": result}),
            )),
            _ => {}
        }
    }
    let total_bytes = text
        .len()
        .checked_add(reasoning.len())
        .and_then(|value| {
            structured.iter().try_fold(value, |total, (_, value)| {
                total.checked_add(value.to_string().len())
            })
        })
        .ok_or_else(|| AiError::InvalidInput("provider output too large".to_owned()))?;
    if total_bytes > limits.maximum_total_bytes {
        return Err(AiError::InvalidInput(
            "provider output exceeds persistence limits".to_owned(),
        ));
    }
    let mut blocks = Vec::new();
    append_text_blocks(&mut blocks, "text", &text, limits.maximum_block_bytes);
    append_text_blocks(
        &mut blocks,
        "reasoning_summary",
        &reasoning,
        limits.maximum_block_bytes,
    );
    for (kind, value) in structured {
        let encoded = serde_json::to_vec(&value).map_err(|_| AiError::ProviderFailed)?;
        if encoded.len() > limits.maximum_block_bytes {
            return Err(AiError::InvalidInput(
                "structured provider output exceeds block limit".to_owned(),
            ));
        }
        blocks.push(RawBlock {
            kind: kind.to_owned(),
            preview_text: String::new(),
            value,
            byte_count: encoded.len(),
            line_count: 1,
        });
    }
    if blocks.is_empty() {
        blocks.push(RawBlock {
            kind: "metadata".to_owned(),
            value: json!({"status": "completed"}),
            preview_text: String::new(),
            byte_count: 22,
            line_count: 1,
        });
    }
    if blocks.len() > limits.maximum_blocks {
        return Err(AiError::InvalidInput(
            "provider output exceeds block-count limit".to_owned(),
        ));
    }
    Ok(blocks)
}

fn append_text_blocks(blocks: &mut Vec<RawBlock>, kind: &str, value: &str, maximum_bytes: usize) {
    let mut remainder = value;
    while !remainder.is_empty() {
        let chunk = bounded_prefix(remainder, maximum_bytes);
        blocks.push(RawBlock {
            kind: kind.to_owned(),
            value: json!({"text": chunk}),
            preview_text: chunk.to_owned(),
            byte_count: chunk.len(),
            line_count: chunk.lines().count().max(1),
        });
        remainder = &remainder[chunk.len()..];
    }
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

fn context(entity: &str, row_id: Uuid, field: &str, scope: &AiScope) -> ContentProtectionContext {
    ContentProtectionContext {
        entity: entity.to_owned(),
        row_id: row_id.to_string(),
        field: field.to_owned(),
        scope: scope.clone(),
    }
}

fn bounded_prefix(value: &str, maximum_bytes: usize) -> &str {
    if value.len() <= maximum_bytes {
        return value;
    }
    let mut end = maximum_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
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
