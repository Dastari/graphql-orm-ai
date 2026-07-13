//! ORM-backed immutable pricing catalog and authoritative token accounting.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::sync::Arc;

use agql_auth::{AuthPrincipal, Clock, RecentMfaPolicy};
use async_trait::async_trait;
use graphql_orm::db::Database;
use graphql_orm::graphql::errors::{OrmErrorCode, OrmPublicError};
use graphql_orm::graphql::filters::StringFilter;
use graphql_orm::graphql::orm::{DefaultWriteBackend, TransactionError, TransactionMode};
use uuid::Uuid;

use crate::persistence::*;
use crate::{
    AiBudgetAmounts, AiConfigurationAccessPolicy, AiConfigurationAction, AiError,
    AiPricingCatalogService, AiPricingPolicyView, AiPricingQuoteRequest, AiPricingQuoteService,
    AiProviderKindInput, AiProviderUsageAccounting, AiProviderUsageObservation, AiScope,
    CreateAiPricingPolicyInput, ProviderKind,
};

const RATE_DENOMINATOR: u64 = 1_000_000;
const MAXIMUM_MODEL_LENGTH: usize = 200;

/// Deployment hard bounds for immutable pricing-catalog administration.
///
/// These limits constrain what GraphQL administrators may append. They do not
/// authorize access, select a model, prove provider billing, or grant budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiPricingCatalogManagementLimits {
    maximum_fixed_call_microunits: u64,
    maximum_token_rate_microunits_per_million: u64,
    maximum_versions_per_route: usize,
}

impl AiPricingCatalogManagementLimits {
    /// Creates validated deployment pricing bounds.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::InvalidConfiguration`] unless the version cap is in
    /// `1..=100`. Zero-valued rate ceilings are permitted for deployments that
    /// intentionally allow only explicit free-pricing entries.
    pub fn new(
        maximum_fixed_call_microunits: u64,
        maximum_token_rate_microunits_per_million: u64,
        maximum_versions_per_route: usize,
    ) -> Result<Self, AiError> {
        if !(1..=100).contains(&maximum_versions_per_route) {
            return Err(AiError::InvalidConfiguration(
                "invalid pricing-catalog management limits".to_owned(),
            ));
        }
        Ok(Self {
            maximum_fixed_call_microunits,
            maximum_token_rate_microunits_per_million,
            maximum_versions_per_route,
        })
    }

    /// Greatest fixed-call microunit rate an administrator may append.
    pub const fn maximum_fixed_call_microunits(self) -> u64 {
        self.maximum_fixed_call_microunits
    }

    /// Greatest per-million-token microunit rate an administrator may append.
    pub const fn maximum_token_rate_microunits_per_million(self) -> u64 {
        self.maximum_token_rate_microunits_per_million
    }

    /// Maximum immutable versions retained for one exact route.
    pub const fn maximum_versions_per_route(self) -> usize {
        self.maximum_versions_per_route
    }
}

/// ORM-backed immutable pricing catalog.
///
/// Every version is immediately effective but is never selected implicitly:
/// callers bind its globally unique reference into a budget reservation. The
/// same service provides conservative preflight quotes and authoritative
/// token-only provider settlement for that exact version.
#[derive(Clone)]
pub struct OrmAiPricingService {
    database: Database<DefaultWriteBackend>,
    access_policy: Arc<dyn AiConfigurationAccessPolicy>,
    recent_mfa_policy: RecentMfaPolicy,
    clock: Arc<dyn Clock>,
    limits: AiPricingCatalogManagementLimits,
}

impl OrmAiPricingService {
    /// Creates an immutable pricing service under deployment hard bounds.
    pub fn new(
        database: Database<DefaultWriteBackend>,
        access_policy: Arc<dyn AiConfigurationAccessPolicy>,
        recent_mfa_policy: RecentMfaPolicy,
        clock: Arc<dyn Clock>,
        limits: AiPricingCatalogManagementLimits,
    ) -> Self {
        Self {
            database,
            access_policy,
            recent_mfa_policy,
            clock,
            limits,
        }
    }

    async fn require_access(
        &self,
        principal: &AuthPrincipal,
        scope: &AiScope,
        action: AiConfigurationAction,
    ) -> Result<(), AiError> {
        validate_scope(scope)?;
        if let Some(tenant_id) = principal.tenant_id()
            && scope.tenant_id.as_deref() != Some(tenant_id)
        {
            return Err(AiError::Forbidden);
        }
        if self
            .access_policy
            .can_configure(principal, scope, action)
            .await
        {
            Ok(())
        } else {
            Err(AiError::Forbidden)
        }
    }

    fn require_recent_mfa(&self, principal: &AuthPrincipal) -> Result<(), AiError> {
        let user = principal.as_user().ok_or(AiError::RecentMfaRequired)?;
        self.recent_mfa_policy
            .evaluate(user, self.clock.as_ref())
            .map_err(|_| AiError::RecentMfaRequired)
    }

    async fn exact_version(
        &self,
        version_reference: &str,
    ) -> Result<AiPricingPolicyRecord, AiError> {
        validate_reference(version_reference)?;
        let rows = self
            .database
            .transaction(TransactionMode::Default, {
                let version_reference = version_reference.to_owned();
                move |tx| {
                    Box::pin(async move {
                        tx.query::<AiPricingPolicyRecord>()
                            .filter(AiPricingPolicyRecordWhereInput {
                                version_reference: Some(StringFilter {
                                    eq: Some(version_reference),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })
                            .limit(2)
                            .fetch_all()
                            .await
                            .map_err(OrmPublicError::from)
                    })
                }
            })
            .await
            .map_err(map_transaction)?;
        if rows.len() != 1 {
            return if rows.is_empty() {
                Err(AiError::NotFound)
            } else {
                Err(AiError::InvalidConfiguration(
                    "pricing version uniqueness is corrupt".to_owned(),
                ))
            };
        }
        let record = rows.into_iter().next().ok_or(AiError::NotFound)?;
        validate_record(&record)?;
        Ok(record)
    }
}

#[async_trait]
impl AiPricingCatalogService for OrmAiPricingService {
    async fn pricing_policies(
        &self,
        principal: &AuthPrincipal,
        scope: AiScope,
        provider_kind: AiProviderKindInput,
        provider_model: String,
    ) -> Result<Vec<AiPricingPolicyView>, AiError> {
        self.require_access(principal, &scope, AiConfigurationAction::ReadPricingCatalog)
            .await?;
        let provider_model = normalize_model(provider_model)?;
        let expected_model = provider_model.clone();
        let scope_key = crate::ai_scope_key(&scope);
        let rows = self
            .database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    tx.query::<AiPricingPolicyRecord>()
                        .filter(AiPricingPolicyRecordWhereInput {
                            scope_key: Some(StringFilter {
                                eq: Some(scope_key),
                                ..Default::default()
                            }),
                            provider_kind: Some(StringFilter {
                                eq: Some(provider_kind.as_str().to_owned()),
                                ..Default::default()
                            }),
                            provider_model: Some(StringFilter {
                                eq: Some(provider_model),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .default_order()
                        .limit(101)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .map_err(map_transaction)?;
        if rows.len() > 100 {
            return Err(AiError::InvalidConfiguration(
                "pricing route exceeds the bounded version limit".to_owned(),
            ));
        }
        rows.iter()
            .map(|record| {
                validate_record(record)?;
                if record_scope(record) != scope
                    || record.provider_kind != provider_kind.as_str()
                    || record.provider_model != expected_model
                {
                    return Err(AiError::InvalidConfiguration(
                        "pricing route binding is corrupt".to_owned(),
                    ));
                }
                Ok(pricing_view(record))
            })
            .collect()
    }

    async fn create_pricing_policy(
        &self,
        principal: &AuthPrincipal,
        mut input: CreateAiPricingPolicyInput,
    ) -> Result<AiPricingPolicyView, AiError> {
        self.require_recent_mfa(principal)?;
        let scope: AiScope = input.scope.clone().into();
        self.require_access(
            principal,
            &scope,
            AiConfigurationAction::ManagePricingCatalog,
        )
        .await?;
        input.provider_model = normalize_model(input.provider_model)?;
        validate_rates(&input, self.limits)?;

        let id = Uuid::new_v4();
        let version_reference = format!("pricing:{id}");
        let scope_key = crate::ai_scope_key(&scope);
        let provider_kind = input.provider_kind.as_str().to_owned();
        let provider_model = input.provider_model;
        let actor_kind = principal_kind(principal);
        let actor_subject = principal.subject().to_owned();
        let maximum_versions =
            i64::try_from(self.limits.maximum_versions_per_route()).map_err(|_| {
                AiError::InvalidConfiguration("invalid pricing version limit".to_owned())
            })?;
        let audit_reference = version_reference.clone();
        let record = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let count = tx
                        .query::<AiPricingPolicyRecord>()
                        .filter(AiPricingPolicyRecordWhereInput {
                            scope_key: Some(StringFilter {
                                eq: Some(scope_key.clone()),
                                ..Default::default()
                            }),
                            provider_kind: Some(StringFilter {
                                eq: Some(provider_kind.clone()),
                                ..Default::default()
                            }),
                            provider_model: Some(StringFilter {
                                eq: Some(provider_model.clone()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .count()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if count >= maximum_versions {
                        return Err(OrmPublicError::new(OrmErrorCode::InvalidInput));
                    }
                    let record = tx
                        .insert::<AiPricingPolicyRecord>(CreateAiPricingPolicyRecordInput {
                            id,
                            version_reference,
                            scope_key,
                            scope_kind: scope.kind,
                            scope_id: scope.id,
                            tenant_id: scope.tenant_id,
                            provider_kind,
                            provider_model,
                            fixed_call_microunits: input.fixed_call_microunits,
                            input_microunits_per_million: input.input_microunits_per_million,
                            cached_input_microunits_per_million: input
                                .cached_input_microunits_per_million,
                            output_microunits_per_million: input.output_microunits_per_million,
                            created_by_principal_kind: actor_kind.clone(),
                            created_by_subject: actor_subject.clone(),
                        })
                        .await
                        .map_err(OrmPublicError::from)?;
                    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
                        actor_principal_kind: actor_kind,
                        actor_subject,
                        action: "ai.pricing_policy.create".to_owned(),
                        resource_kind: "pricing_policy".to_owned(),
                        resource_reference: audit_reference,
                        outcome: "allowed".to_owned(),
                        reason_code: "immutable_pricing_version_created".to_owned(),
                        correlation_id: Uuid::new_v4().to_string(),
                        causation_id: None,
                        policy_version: None,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    Ok(record)
                })
            })
            .await
            .map_err(map_transaction)?;
        validate_record(&record)?;
        Ok(pricing_view(&record))
    }
}

#[async_trait]
impl AiPricingQuoteService for OrmAiPricingService {
    async fn quote(&self, mut request: AiPricingQuoteRequest) -> Result<AiBudgetAmounts, AiError> {
        validate_scope(&request.scope)?;
        request.provider_model = normalize_model(request.provider_model)?;
        let record = self.exact_version(&request.version_reference).await?;
        require_route(
            &record,
            &request.scope,
            &request.provider_kind,
            &request.provider_model,
        )?;
        Ok(AiBudgetAmounts {
            input_tokens: request.input_tokens,
            output_tokens: request.output_tokens,
            tool_units: 0,
            image_units: 0,
            cost_microunits: price_tokens(&record, request.input_tokens, 0, request.output_tokens)?,
            runs: 1,
        })
    }
}

#[async_trait]
impl AiProviderUsageAccounting for OrmAiPricingService {
    async fn settle(
        &self,
        observation: &AiProviderUsageObservation,
    ) -> Result<AiBudgetAmounts, AiError> {
        if !observation.builtin_tools().is_empty() {
            return Err(AiError::InvalidConfiguration(
                "token pricing cannot settle provider built-in billable units".to_owned(),
            ));
        }
        if observation.cached_input_tokens() > observation.input_tokens() {
            return Err(AiError::InvalidConfiguration(
                "provider cached input exceeds total input".to_owned(),
            ));
        }
        let model = normalize_model(observation.model().to_owned())?;
        let record = self
            .exact_version(observation.pricing_policy_version())
            .await?;
        require_route(
            &record,
            observation.scope(),
            observation.provider_kind(),
            &model,
        )?;
        Ok(AiBudgetAmounts {
            input_tokens: observation.input_tokens(),
            output_tokens: observation.output_tokens(),
            tool_units: 0,
            image_units: 0,
            cost_microunits: price_tokens(
                &record,
                observation.input_tokens(),
                observation.cached_input_tokens(),
                observation.output_tokens(),
            )?,
            runs: 1,
        })
    }
}

fn require_route(
    record: &AiPricingPolicyRecord,
    scope: &AiScope,
    provider_kind: &ProviderKind,
    provider_model: &str,
) -> Result<(), AiError> {
    if record_scope(record) != *scope
        || record.provider_kind != provider_kind.as_str()
        || record.provider_model != provider_model
    {
        return Err(AiError::NotFound);
    }
    Ok(())
}

fn validate_record(record: &AiPricingPolicyRecord) -> Result<(), AiError> {
    let scope = record_scope(record);
    validate_scope(&scope)?;
    validate_reference(&record.version_reference)?;
    let normalized_model = normalize_model(record.provider_model.clone());
    if record.scope_key != crate::ai_scope_key(&scope)
        || record.id.is_nil()
        || !is_provider_kind(&record.provider_kind)
        || !matches!(normalized_model, Ok(model) if model == record.provider_model)
        || record.fixed_call_microunits < 0
        || record.input_microunits_per_million < 0
        || record.cached_input_microunits_per_million < 0
        || record.output_microunits_per_million < 0
        || record.cached_input_microunits_per_million > record.input_microunits_per_million
        || record.created_by_principal_kind.trim().is_empty()
        || record.created_by_subject.trim().is_empty()
    {
        return Err(AiError::InvalidConfiguration(
            "immutable pricing record is corrupt".to_owned(),
        ));
    }
    Ok(())
}

fn validate_rates(
    input: &CreateAiPricingPolicyInput,
    limits: AiPricingCatalogManagementLimits,
) -> Result<(), AiError> {
    let fixed = u64::try_from(input.fixed_call_microunits);
    let input_rate = u64::try_from(input.input_microunits_per_million);
    let cached_rate = u64::try_from(input.cached_input_microunits_per_million);
    let output_rate = u64::try_from(input.output_microunits_per_million);
    if fixed.map_or(true, |value| value > limits.maximum_fixed_call_microunits())
        || input_rate.map_or(true, |value| {
            value > limits.maximum_token_rate_microunits_per_million()
        })
        || cached_rate.map_or(true, |value| {
            value > limits.maximum_token_rate_microunits_per_million()
        })
        || output_rate.map_or(true, |value| {
            value > limits.maximum_token_rate_microunits_per_million()
        })
        || input.cached_input_microunits_per_million > input.input_microunits_per_million
    {
        return Err(AiError::InvalidInput(
            "invalid immutable pricing rates".to_owned(),
        ));
    }
    Ok(())
}

fn price_tokens(
    record: &AiPricingPolicyRecord,
    input_tokens: u64,
    cached_input_tokens: u64,
    output_tokens: u64,
) -> Result<u64, AiError> {
    let uncached_input_tokens = input_tokens
        .checked_sub(cached_input_tokens)
        .ok_or_else(|| {
            AiError::InvalidConfiguration("cached input exceeds total input".to_owned())
        })?;
    let fixed = stored_rate(record.fixed_call_microunits)?;
    let input = price_dimension(
        uncached_input_tokens,
        stored_rate(record.input_microunits_per_million)?,
    )?;
    let cached = price_dimension(
        cached_input_tokens,
        stored_rate(record.cached_input_microunits_per_million)?,
    )?;
    let output = price_dimension(
        output_tokens,
        stored_rate(record.output_microunits_per_million)?,
    )?;
    fixed
        .checked_add(input)
        .and_then(|value| value.checked_add(cached))
        .and_then(|value| value.checked_add(output))
        .ok_or_else(|| AiError::InvalidConfiguration("pricing arithmetic overflow".to_owned()))
}

fn price_dimension(units: u64, rate: u64) -> Result<u64, AiError> {
    if units == 0 || rate == 0 {
        return Ok(0);
    }
    units
        .checked_mul(rate)
        .and_then(|value| value.checked_add(RATE_DENOMINATOR - 1))
        .map(|value| value / RATE_DENOMINATOR)
        .ok_or_else(|| AiError::InvalidConfiguration("pricing arithmetic overflow".to_owned()))
}

fn stored_rate(value: i64) -> Result<u64, AiError> {
    u64::try_from(value)
        .map_err(|_| AiError::InvalidConfiguration("negative stored pricing rate".to_owned()))
}

fn normalize_model(model: String) -> Result<String, AiError> {
    let model = model.trim().to_owned();
    if model.is_empty() || model.len() > MAXIMUM_MODEL_LENGTH || model.chars().any(char::is_control)
    {
        return Err(AiError::InvalidInput(
            "invalid pricing provider model".to_owned(),
        ));
    }
    Ok(model)
}

fn validate_reference(reference: &str) -> Result<(), AiError> {
    if reference.trim().is_empty()
        || reference.len() > 200
        || reference.chars().any(char::is_control)
    {
        return Err(AiError::InvalidInput(
            "invalid pricing version reference".to_owned(),
        ));
    }
    Ok(())
}

fn validate_scope(scope: &AiScope) -> Result<(), AiError> {
    if scope.kind.trim().is_empty()
        || scope.kind.len() > 128
        || scope.id.trim().is_empty()
        || scope.id.len() > 512
        || scope
            .tenant_id
            .as_ref()
            .is_some_and(|tenant| tenant.trim().is_empty() || tenant.len() > 512)
    {
        return Err(AiError::InvalidInput("invalid AI pricing scope".to_owned()));
    }
    Ok(())
}

fn is_provider_kind(kind: &str) -> bool {
    matches!(
        kind,
        "openai" | "anthropic" | "xai" | "ollama" | "openai_compatible" | "local_harness"
    )
}

fn record_scope(record: &AiPricingPolicyRecord) -> AiScope {
    AiScope {
        kind: record.scope_kind.clone(),
        id: record.scope_id.clone(),
        tenant_id: record.tenant_id.clone(),
    }
}

fn pricing_view(record: &AiPricingPolicyRecord) -> AiPricingPolicyView {
    AiPricingPolicyView {
        id: record.id,
        version_reference: record.version_reference.clone(),
        scope_kind: record.scope_kind.clone(),
        scope_id: record.scope_id.clone(),
        tenant_id: record.tenant_id.clone(),
        provider_kind: record.provider_kind.clone(),
        provider_model: record.provider_model.clone(),
        fixed_call_microunits: record.fixed_call_microunits,
        input_microunits_per_million: record.input_microunits_per_million,
        cached_input_microunits_per_million: record.cached_input_microunits_per_million,
        output_microunits_per_million: record.output_microunits_per_million,
        created_at: record.created_at,
    }
}

fn principal_kind(principal: &AuthPrincipal) -> String {
    match principal {
        AuthPrincipal::User(_) => "user".to_owned(),
        AuthPrincipal::ApiToken(token) => {
            format!("api_token:{}", token.principal_kind.as_str())
        }
    }
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
    use super::*;
    use agql_auth::{
        AccessTokenMetadata, AssuranceMatchMode, AuthUser, FixedClock, MfaAcceptance,
        SessionAssurance, SessionContext,
    };
    use graphql_orm::graphql::orm::{ApplyOptions, OrmSchemaModule};
    use graphql_orm::prelude::SqliteBackend;
    use time::{Duration, OffsetDateTime};

    struct AllowConfiguration;

    #[async_trait]
    impl AiConfigurationAccessPolicy for AllowConfiguration {
        async fn can_configure(
            &self,
            _principal: &AuthPrincipal,
            _scope: &AiScope,
            _action: AiConfigurationAction,
        ) -> bool {
            true
        }
    }

    fn now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_800_000_000).expect("fixed time should validate")
    }

    fn principal(recent_mfa: bool) -> AuthPrincipal {
        let now = now();
        let session = if recent_mfa {
            let assurance = SessionAssurance::new(
                now,
                ["otp", "pwd"],
                Some("urn:test:loa:2".to_owned()),
                Some("test".to_owned()),
                MfaAcceptance::Satisfied,
            )
            .expect("test assurance should validate");
            SessionContext::default().with_assurance(assurance)
        } else {
            SessionContext::default()
        };
        AuthPrincipal::User(AuthUser {
            user_id: "pricing-admin".to_owned(),
            session_id: Uuid::new_v4(),
            roles: vec!["admin".to_owned()],
            scopes: vec![],
            session,
            token_claims: AccessTokenMetadata {
                auth_time: recent_mfa.then_some(now.unix_timestamp()),
                amr: recent_mfa.then(|| vec!["otp".to_owned(), "pwd".to_owned()]),
                acr: recent_mfa.then(|| "urn:test:loa:2".to_owned()),
                tenant_id: Some("tenant-a".to_owned()),
                ..AccessTokenMetadata::default()
            },
        })
    }

    fn scope() -> AiScope {
        AiScope::new("tenant", "tenant-a").with_tenant_id("tenant-a")
    }

    fn input() -> CreateAiPricingPolicyInput {
        CreateAiPricingPolicyInput {
            scope: crate::AiScopeInput {
                kind: "tenant".to_owned(),
                id: "tenant-a".to_owned(),
                tenant_id: Some("tenant-a".to_owned()),
            },
            provider_kind: AiProviderKindInput::OpenAi,
            provider_model: "model-a".to_owned(),
            fixed_call_microunits: 5,
            input_microunits_per_million: 2_000_000,
            cached_input_microunits_per_million: 1_000_000,
            output_microunits_per_million: 3_000_000,
        }
    }

    async fn service(maximum_versions: usize) -> OrmAiPricingService {
        let database = Database::<SqliteBackend>::connect_sqlite("sqlite::memory:")
            .await
            .expect("in-memory SQLite should open");
        let module = crate::AiSchemaModule;
        let plan = database
            .schema()
            .plan_migration_to_entities(
                "ai-pricing-test-v1",
                "AI immutable pricing test",
                module.entities(),
            )
            .await
            .expect("pricing schema should plan");
        database
            .schema()
            .apply_migration(&plan, ApplyOptions::default())
            .await
            .expect("pricing schema should apply");
        OrmAiPricingService::new(
            database,
            Arc::new(AllowConfiguration),
            RecentMfaPolicy {
                maximum_age: Duration::minutes(5),
                clock_skew: Duration::seconds(30),
                allowed_amr: vec!["otp".to_owned()],
                allowed_acr: vec!["urn:test:loa:2".to_owned()],
                match_mode: AssuranceMatchMode::All,
            },
            Arc::new(FixedClock::new(now())),
            AiPricingCatalogManagementLimits::new(10_000_000, 10_000_000, maximum_versions)
                .expect("test pricing limits should validate"),
        )
    }

    #[tokio::test]
    async fn immutable_version_is_mfa_scope_audit_and_cardinality_bound() {
        let service = service(1).await;
        assert!(matches!(
            service
                .create_pricing_policy(&principal(false), input())
                .await,
            Err(AiError::RecentMfaRequired)
        ));
        let created = service
            .create_pricing_policy(&principal(true), input())
            .await
            .expect("recent MFA should permit one authorized pricing version");
        assert!(created.version_reference.starts_with("pricing:"));
        let listed = service
            .pricing_policies(
                &principal(true),
                scope(),
                AiProviderKindInput::OpenAi,
                "model-a".to_owned(),
            )
            .await
            .expect("exact route should be readable");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].version_reference, created.version_reference);
        assert!(matches!(
            service
                .pricing_policies(
                    &principal(true),
                    AiScope::new("tenant", "tenant-b").with_tenant_id("tenant-b"),
                    AiProviderKindInput::OpenAi,
                    "model-a".to_owned(),
                )
                .await,
            Err(AiError::Forbidden)
        ));
        assert!(matches!(
            service
                .create_pricing_policy(&principal(true), input())
                .await,
            Err(AiError::InvalidInput(_))
        ));

        let audits = service
            .database
            .transaction(TransactionMode::Default, |tx| {
                Box::pin(async move {
                    tx.query::<AiAuditEventRecord>()
                        .limit(10)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)
                })
            })
            .await
            .expect("pricing audit should load");
        assert_eq!(audits.len(), 1);
        assert_eq!(audits[0].action, "ai.pricing_policy.create");
        assert_eq!(audits[0].resource_reference, created.version_reference);
    }

    #[tokio::test]
    async fn quote_and_authoritative_accounting_use_only_the_exact_version() {
        let service = service(2).await;
        let created = service
            .create_pricing_policy(&principal(true), input())
            .await
            .expect("pricing version should be created");
        let quote = service
            .quote(AiPricingQuoteRequest {
                scope: scope(),
                provider_kind: ProviderKind::OpenAi,
                provider_model: "model-a".to_owned(),
                version_reference: created.version_reference.clone(),
                input_tokens: 10,
                output_tokens: 4,
            })
            .await
            .expect("exact pricing quote should succeed");
        assert_eq!(quote.cost_microunits, 37);
        assert_eq!(quote.runs, 1);

        let observation = AiProviderUsageObservation::test_observation(
            scope(),
            ProviderKind::OpenAi,
            "model-a",
            created.version_reference.clone(),
            10,
            4,
            4,
            vec![],
        );
        let actual = service
            .settle(&observation)
            .await
            .expect("exact authoritative token accounting should succeed");
        assert_eq!(actual.cost_microunits, 33);
        assert_eq!(actual.input_tokens, 10);
        assert_eq!(actual.output_tokens, 4);

        let wrong_scope = AiPricingQuoteRequest {
            scope: AiScope::new("tenant", "tenant-b").with_tenant_id("tenant-b"),
            provider_kind: ProviderKind::OpenAi,
            provider_model: "model-a".to_owned(),
            version_reference: created.version_reference.clone(),
            input_tokens: 1,
            output_tokens: 1,
        };
        assert!(matches!(
            service.quote(wrong_scope).await,
            Err(AiError::NotFound)
        ));
        let wrong_scope_observation = AiProviderUsageObservation::test_observation(
            AiScope::new("tenant", "tenant-b").with_tenant_id("tenant-b"),
            ProviderKind::OpenAi,
            "model-a",
            created.version_reference.clone(),
            1,
            1,
            0,
            vec![],
        );
        assert!(matches!(
            service.settle(&wrong_scope_observation).await,
            Err(AiError::NotFound)
        ));
        let builtin = AiProviderUsageObservation::test_observation(
            scope(),
            ProviderKind::OpenAi,
            "model-a",
            created.version_reference,
            1,
            1,
            0,
            vec![crate::ModelBuiltinTool::WebSearch {
                allowed_domains: vec![],
            }],
        );
        assert!(matches!(
            service.settle(&builtin).await,
            Err(AiError::InvalidConfiguration(_))
        ));
    }
}
