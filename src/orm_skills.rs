//! ORM-backed protected skill publication and resolution.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use agql_auth::{AuthPrincipal, Clock, RecentMfaPolicy};
use async_trait::async_trait;
use graphql_orm::db::Database;
use graphql_orm::graphql::errors::{OrmErrorCode, OrmPublicError};
use graphql_orm::graphql::filters::StringFilter;
use graphql_orm::graphql::orm::{
    ConditionalUpdateOutcome, DefaultWriteBackend, MutationContext, TransactionError,
    TransactionMode,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::persistence::*;
use crate::{
    AiContentProtectionPolicy, AiContentProtectionPolicyResolver, AiContentProtector, AiError,
    AiResolvedSkill, AiScope, AiSkillAccessPolicy, AiSkillAction, AiSkillCatalogService,
    AiSkillUiIntentBindingInput, AiSkillUiIntentBindingView, AiSkillVersionView, AiSkillView,
    AiUiIntentTypeId, ContentProtectionContext, ProtectedContentEnvelope,
    PublishAiSkillVersionInput, SetAiSkillEnabledInput, UpsertAiSkillInput,
};

const SKILL_POLICY_FORMAT_VERSION: u32 = 1;
const MAXIMUM_SKILLS_PER_SCOPE: usize = 100;
const MAXIMUM_INSTRUCTION_BYTES: usize = 1024 * 1024;
const MAXIMUM_SCHEMA_BYTES: usize = 1024 * 1024;
const MAXIMUM_TOOL_FINGERPRINTS: usize = 256;
const MAXIMUM_LOGICAL_TYPES: usize = 128;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredAllowedTools {
    version: u32,
    fingerprints: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredDataPolicy {
    version: u32,
    maximum_classification: String,
    maximum_tool_maturity: String,
    required_provider_capabilities: StoredProviderCapabilities,
    allowed_proposal_types: Vec<String>,
    allowed_ui_intents: Vec<StoredUiIntentBinding>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct StoredUiIntentBinding {
    intent_type: String,
    descriptor_fingerprint: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredProviderCapabilities {
    image_input: bool,
    file_input: bool,
    structured_output: bool,
    custom_tools: bool,
    web_search: bool,
    image_generation: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredActivationRule {
    version: u32,
    kind: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredSchemas {
    version: u32,
    input: serde_json::Value,
    output: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredBudgets {
    version: u32,
    maximum_steps: i64,
    maximum_duration_seconds: i64,
    maximum_output_tokens: i64,
    maximum_cost_microunits: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredProvenance {
    version: u32,
    source: String,
    author_subject: String,
}

struct PreparedSkillVersion {
    id: Uuid,
    version: String,
    protected_instructions: serde_json::Value,
    allowed_tools: StoredAllowedTools,
    data_policy: StoredDataPolicy,
    activation_rule: StoredActivationRule,
    schemas: StoredSchemas,
    budgets: StoredBudgets,
    provenance: StoredProvenance,
    checksum: String,
    enable: bool,
}

struct SkillChecksumContent<'a> {
    version: &'a str,
    instructions: &'a str,
    allowed_tools: &'a StoredAllowedTools,
    data_policy: &'a StoredDataPolicy,
    activation_rule: &'a StoredActivationRule,
    schemas: &'a StoredSchemas,
    budgets: &'a StoredBudgets,
    provenance: &'a StoredProvenance,
}

/// Concrete protected skill catalog using generated ORM APIs only.
///
/// Published instructions are immutable and protected. Enabling a skill only
/// makes it eligible for resolution; it never enables a tool or widens current
/// resolver, egress, provider, proposal, approval, or budget policy.
pub struct OrmAiSkillCatalogService {
    database: Database<DefaultWriteBackend>,
    access_policy: Arc<dyn AiSkillAccessPolicy>,
    protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
    content_protector: Arc<dyn AiContentProtector>,
    recent_mfa_policy: RecentMfaPolicy,
    clock: Arc<dyn Clock>,
}

impl OrmAiSkillCatalogService {
    /// Creates a protected, fail-closed skill catalog.
    pub fn new(
        database: Database<DefaultWriteBackend>,
        access_policy: Arc<dyn AiSkillAccessPolicy>,
        protection_policy: Arc<dyn AiContentProtectionPolicyResolver>,
        content_protector: Arc<dyn AiContentProtector>,
        recent_mfa_policy: RecentMfaPolicy,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            database,
            access_policy,
            protection_policy,
            content_protector,
            recent_mfa_policy,
            clock,
        }
    }

    /// Returns the generated ORM database handle for host wiring.
    pub fn database(&self) -> &Database<DefaultWriteBackend> {
        &self.database
    }

    async fn require_access(
        &self,
        principal: &AuthPrincipal,
        scope: &AiScope,
        action: AiSkillAction,
    ) -> Result<(), AiError> {
        validate_scope(scope)?;
        if self
            .access_policy
            .can_access_skill(principal, scope, action)
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

    async fn ready_protection_policy(
        &self,
        principal: &AuthPrincipal,
        scope: &AiScope,
    ) -> Result<AiContentProtectionPolicy, AiError> {
        let policy = self.protection_policy.resolve(principal, scope).await?;
        if !policy.ready || policy.scope != *scope {
            return Err(AiError::RuntimeNotReady);
        }
        Ok(policy)
    }

    async fn load_skill(&self, id: Uuid) -> Result<AiSkillRecord, AiError> {
        AiSkillRecord::find_by_id(&self.database, &id)
            .await
            .map_err(|error| map_orm(OrmPublicError::from(error)))?
            .ok_or(AiError::NotFound)
    }

    async fn prepare_version(
        &self,
        principal: &AuthPrincipal,
        scope: &AiScope,
        input: PublishAiSkillVersionInput,
    ) -> Result<PreparedSkillVersion, AiError> {
        validate_version_label(&input.version)?;
        validate_instruction_text(&input.instructions)?;
        let allowed_tools = normalized_fingerprints(input.allowed_tool_fingerprints)?;
        let allowed_proposal_types =
            normalized_logical_types(input.allowed_proposal_types, "proposal type")?;
        let allowed_ui_intents = normalized_ui_intent_bindings(input.allowed_ui_intents)?;
        validate_schema(&input.input_schema.0, "skill input")?;
        validate_schema(&input.output_schema.0, "skill output")?;
        validate_budget(
            input.budget.maximum_steps,
            input.budget.maximum_duration_seconds,
            input.budget.maximum_output_tokens,
            input.budget.maximum_cost_microunits,
        )?;

        let id = Uuid::new_v4();
        let policy = self.ready_protection_policy(principal, scope).await?;
        let envelope = self
            .content_protector
            .protect(
                &policy,
                &content_context(id, scope),
                serde_json::Value::String(input.instructions.clone()),
            )
            .await
            .map_err(map_protection)?;
        let protected_instructions =
            serde_json::to_value(envelope).map_err(|_| AiError::PersistenceFailed)?;
        let allowed_tools = StoredAllowedTools {
            version: SKILL_POLICY_FORMAT_VERSION,
            fingerprints: allowed_tools,
        };
        let data_policy = StoredDataPolicy {
            version: SKILL_POLICY_FORMAT_VERSION,
            maximum_classification: input.maximum_classification.as_str().to_owned(),
            maximum_tool_maturity: input.maximum_tool_maturity.as_str().to_owned(),
            required_provider_capabilities: StoredProviderCapabilities {
                image_input: input.required_provider_capabilities.image_input,
                file_input: input.required_provider_capabilities.file_input,
                structured_output: input.required_provider_capabilities.structured_output,
                custom_tools: input.required_provider_capabilities.custom_tools,
                web_search: input.required_provider_capabilities.web_search,
                image_generation: input.required_provider_capabilities.image_generation,
            },
            allowed_proposal_types,
            allowed_ui_intents,
        };
        let activation_rule = StoredActivationRule {
            version: SKILL_POLICY_FORMAT_VERSION,
            kind: input.activation.as_str().to_owned(),
        };
        let schemas = StoredSchemas {
            version: SKILL_POLICY_FORMAT_VERSION,
            input: input.input_schema.0,
            output: input.output_schema.0,
        };
        let budgets = StoredBudgets {
            version: SKILL_POLICY_FORMAT_VERSION,
            maximum_steps: input.budget.maximum_steps,
            maximum_duration_seconds: input.budget.maximum_duration_seconds,
            maximum_output_tokens: input.budget.maximum_output_tokens,
            maximum_cost_microunits: input.budget.maximum_cost_microunits,
        };
        let provenance = StoredProvenance {
            version: SKILL_POLICY_FORMAT_VERSION,
            source: "authenticated_graphql".to_owned(),
            author_subject: principal.subject().to_owned(),
        };
        let checksum = skill_checksum(SkillChecksumContent {
            version: &input.version,
            instructions: &input.instructions,
            allowed_tools: &allowed_tools,
            data_policy: &data_policy,
            activation_rule: &activation_rule,
            schemas: &schemas,
            budgets: &budgets,
            provenance: &provenance,
        })?;
        Ok(PreparedSkillVersion {
            id,
            version: input.version,
            protected_instructions,
            allowed_tools,
            data_policy,
            activation_rule,
            schemas,
            budgets,
            provenance,
            checksum,
            enable: input.enable,
        })
    }
}

#[async_trait]
impl AiSkillCatalogService for OrmAiSkillCatalogService {
    async fn skills(
        &self,
        principal: &AuthPrincipal,
        scope: AiScope,
    ) -> Result<Vec<AiSkillView>, AiError> {
        self.require_access(principal, &scope, AiSkillAction::Read)
            .await?;
        load_scope_skills(&self.database, &scope)
            .await?
            .into_iter()
            .map(|(skill, version)| skill_view(&skill, version.as_ref()))
            .collect()
    }

    async fn upsert_skill(
        &self,
        principal: &AuthPrincipal,
        input: UpsertAiSkillInput,
    ) -> Result<AiSkillView, AiError> {
        self.require_recent_mfa(principal)?;
        let scope: AiScope = input.scope.into();
        self.require_access(principal, &scope, AiSkillAction::ManageSkill)
            .await?;
        let name = input.name.trim().to_owned();
        let description = input.description.trim().to_owned();
        validate_safe_text(&name, 200, false, "skill name")?;
        validate_safe_text(&description, 4_000, true, "skill description")?;
        if input.id.is_some() != input.expected_version.is_some()
            || input.expected_version.is_some_and(|version| version < 0)
        {
            return Err(AiError::InvalidInput(
                "invalid skill CAS identity".to_owned(),
            ));
        }
        let actor = principal.subject().to_owned();
        let skill_id = input.id;
        let expected_version = input.expected_version;
        let scope_for_tx = scope.clone();
        let (skill, version) = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let skill = match (skill_id, expected_version) {
                        (None, None) => tx
                            .insert::<AiSkillRecord>(CreateAiSkillRecordInput {
                                scope_kind: scope_for_tx.kind.clone(),
                                scope_id: scope_for_tx.id.clone(),
                                tenant_id: scope_for_tx.tenant_id.clone(),
                                name,
                                description,
                                enabled: false,
                                current_version_id: None,
                                created_by_subject: actor.clone(),
                            })
                            .await
                            .map_err(OrmPublicError::from)?,
                        (Some(id), Some(expected)) => {
                            let current = tx
                                .find_by_id::<AiSkillRecord>(&id)
                                .await
                                .map_err(OrmPublicError::from)?
                                .ok_or_else(OrmPublicError::not_found)?;
                            if skill_scope(&current) != scope_for_tx {
                                return Err(OrmPublicError::not_found());
                            }
                            match tx
                                .compare_and_swap::<AiSkillRecord>(
                                    &id,
                                    expected,
                                    AiSkillRecordWhereInput::default(),
                                    UpdateAiSkillRecordInput {
                                        name: Some(name),
                                        description: Some(description),
                                        ..Default::default()
                                    },
                                )
                                .await
                                .map_err(OrmPublicError::from)?
                            {
                                ConditionalUpdateOutcome::Updated(skill) => skill,
                                ConditionalUpdateOutcome::NotFound => {
                                    return Err(OrmPublicError::not_found());
                                }
                                ConditionalUpdateOutcome::Conflict => {
                                    return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                                }
                            }
                        }
                        _ => return Err(OrmPublicError::new(OrmErrorCode::InvalidInput)),
                    };
                    insert_skill_audit(
                        tx,
                        &actor,
                        "ai.skill.upsert",
                        skill.id,
                        "skill_metadata_updated",
                    )
                    .await?;
                    let version = load_current_version(tx, &skill).await?;
                    Ok((skill, version))
                })
            })
            .await
            .map_err(map_transaction)?;
        skill_view(&skill, version.as_ref())
    }

    async fn publish_version(
        &self,
        principal: &AuthPrincipal,
        input: PublishAiSkillVersionInput,
    ) -> Result<AiSkillView, AiError> {
        self.require_recent_mfa(principal)?;
        if input.expected_skill_version < 0 {
            return Err(AiError::InvalidInput(
                "invalid skill CAS version".to_owned(),
            ));
        }
        let skill = self.load_skill(input.skill_id).await?;
        let scope = skill_scope(&skill);
        self.require_access(principal, &scope, AiSkillAction::PublishVersion)
            .await?;
        let expected_skill_version = input.expected_skill_version;
        let skill_id = input.skill_id;
        let prepared = self.prepare_version(principal, &scope, input).await?;
        let actor = principal.subject().to_owned();
        let (skill, version) = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = tx
                        .find_by_id::<AiSkillRecord>(&skill_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if current.row_version != expected_skill_version
                        || skill_scope(&current) != scope
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let duplicate = tx
                        .query::<AiSkillVersionRecord>()
                        .filter(AiSkillVersionRecordWhereInput {
                            skill_id: Some(graphql_orm::graphql::filters::UuidFilter {
                                eq: Some(skill_id),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(1_001)
                        .fetch_all()
                        .await
                        .map_err(OrmPublicError::from)?;
                    if duplicate.len() > 1_000 {
                        return Err(OrmPublicError::new(
                            OrmErrorCode::AuthorizationMisconfigured,
                        ));
                    }
                    if duplicate
                        .iter()
                        .any(|version| version.version == prepared.version)
                    {
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let version = tx
                        .insert::<AiSkillVersionRecord>(CreateAiSkillVersionRecordInput {
                            id: prepared.id,
                            skill_id,
                            version: prepared.version,
                            protected_instructions: prepared.protected_instructions,
                            allowed_tools: to_json(&prepared.allowed_tools)?,
                            data_policy: to_json(&prepared.data_policy)?,
                            activation_rule: to_json(&prepared.activation_rule)?,
                            schemas: to_json(&prepared.schemas)?,
                            budgets: to_json(&prepared.budgets)?,
                            provenance: to_json(&prepared.provenance)?,
                            checksum: prepared.checksum,
                            published: true,
                            author_subject: actor.clone(),
                        })
                        .await
                        .map_err(OrmPublicError::from)?;
                    let updated = match tx
                        .compare_and_swap::<AiSkillRecord>(
                            &skill_id,
                            expected_skill_version,
                            AiSkillRecordWhereInput::default(),
                            UpdateAiSkillRecordInput {
                                current_version_id: Some(Some(version.id)),
                                enabled: Some(prepared.enable),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?
                    {
                        ConditionalUpdateOutcome::Updated(skill) => skill,
                        ConditionalUpdateOutcome::NotFound => {
                            return Err(OrmPublicError::not_found());
                        }
                        ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    insert_skill_audit(
                        tx,
                        &actor,
                        "ai.skill_version.publish",
                        skill_id,
                        "immutable_version_published",
                    )
                    .await?;
                    Ok((updated, version))
                })
            })
            .await
            .map_err(map_transaction)?;
        skill_view(&skill, Some(&version))
    }

    async fn set_enabled(
        &self,
        principal: &AuthPrincipal,
        input: SetAiSkillEnabledInput,
    ) -> Result<AiSkillView, AiError> {
        self.require_recent_mfa(principal)?;
        if input.expected_version < 0 {
            return Err(AiError::InvalidInput(
                "invalid skill CAS version".to_owned(),
            ));
        }
        let existing = self.load_skill(input.skill_id).await?;
        let scope = skill_scope(&existing);
        self.require_access(principal, &scope, AiSkillAction::SetEnabled)
            .await?;
        let actor = principal.subject().to_owned();
        let (skill, version) = self
            .database
            .transaction(TransactionMode::StateMachine, move |tx| {
                Box::pin(async move {
                    let current = tx
                        .find_by_id::<AiSkillRecord>(&input.skill_id)
                        .await
                        .map_err(OrmPublicError::from)?
                        .ok_or_else(OrmPublicError::not_found)?;
                    if skill_scope(&current) != scope {
                        return Err(OrmPublicError::not_found());
                    }
                    let version = load_current_version(tx, &current).await?;
                    if input.enabled && version.is_none() {
                        return Err(OrmPublicError::new(OrmErrorCode::InvalidInput));
                    }
                    let updated = match tx
                        .compare_and_swap::<AiSkillRecord>(
                            &input.skill_id,
                            input.expected_version,
                            AiSkillRecordWhereInput::default(),
                            UpdateAiSkillRecordInput {
                                enabled: Some(input.enabled),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(OrmPublicError::from)?
                    {
                        ConditionalUpdateOutcome::Updated(skill) => skill,
                        ConditionalUpdateOutcome::NotFound => {
                            return Err(OrmPublicError::not_found());
                        }
                        ConditionalUpdateOutcome::Conflict => {
                            return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                        }
                    };
                    insert_skill_audit(
                        tx,
                        &actor,
                        "ai.skill.set_enabled",
                        updated.id,
                        if input.enabled {
                            "skill_enabled"
                        } else {
                            "skill_disabled"
                        },
                    )
                    .await?;
                    Ok((updated, version))
                })
            })
            .await
            .map_err(map_transaction)?;
        skill_view(&skill, version.as_ref())
    }

    async fn resolve_enabled_skills(
        &self,
        principal: &AuthPrincipal,
        scope: AiScope,
    ) -> Result<Vec<AiResolvedSkill>, AiError> {
        self.require_access(principal, &scope, AiSkillAction::ResolveForRun)
            .await?;
        let policy = self.ready_protection_policy(principal, &scope).await?;
        let rows = load_scope_skills(&self.database, &scope).await?;
        let mut resolved = Vec::new();
        for (skill, version) in rows {
            if !skill.enabled {
                continue;
            }
            let version = version.ok_or_else(|| {
                AiError::InvalidConfiguration(
                    "enabled skill has no current published version".to_owned(),
                )
            })?;
            let parsed = parse_version(&version)?;
            let envelope: ProtectedContentEnvelope =
                serde_json::from_value(version.protected_instructions.clone()).map_err(|_| {
                    AiError::InvalidConfiguration("invalid protected skill instructions".to_owned())
                })?;
            let opened = self
                .content_protector
                .open(&policy, &content_context(version.id, &scope), &envelope)
                .await
                .map_err(map_protection)?;
            let instructions = opened.as_str().map(str::to_owned).ok_or_else(|| {
                AiError::InvalidConfiguration("invalid protected skill instruction type".to_owned())
            })?;
            validate_instruction_text(&instructions).map_err(|_| {
                AiError::InvalidConfiguration("invalid stored skill instructions".to_owned())
            })?;
            let checksum = skill_checksum(SkillChecksumContent {
                version: &version.version,
                instructions: &instructions,
                allowed_tools: &parsed.allowed_tools,
                data_policy: &parsed.data_policy,
                activation_rule: &parsed.activation_rule,
                schemas: &parsed.schemas,
                budgets: &parsed.budgets,
                provenance: &parsed.provenance,
            })?;
            if checksum != version.checksum
                || parsed.provenance.author_subject != version.author_subject
            {
                return Err(AiError::InvalidConfiguration(
                    "skill version checksum or provenance mismatch".to_owned(),
                ));
            }
            resolved.push(AiResolvedSkill {
                skill: skill_view(&skill, Some(&version))?,
                instructions,
                input_schema: parsed.schemas.input,
                output_schema: parsed.schemas.output,
                required_provider_capabilities: to_value(
                    &parsed.data_policy.required_provider_capabilities,
                )?,
            });
        }
        Ok(resolved)
    }
}

struct ParsedVersion {
    allowed_tools: StoredAllowedTools,
    data_policy: StoredDataPolicy,
    activation_rule: StoredActivationRule,
    schemas: StoredSchemas,
    budgets: StoredBudgets,
    provenance: StoredProvenance,
}

async fn load_scope_skills(
    database: &Database<DefaultWriteBackend>,
    scope: &AiScope,
) -> Result<Vec<(AiSkillRecord, Option<AiSkillVersionRecord>)>, AiError> {
    let scope = scope.clone();
    database
        .transaction(TransactionMode::Default, move |tx| {
            Box::pin(async move {
                let skills = tx
                    .query::<AiSkillRecord>()
                    .filter(skill_scope_filter(&scope))
                    .default_order()
                    .limit((MAXIMUM_SKILLS_PER_SCOPE + 1) as i64)
                    .fetch_all()
                    .await
                    .map_err(OrmPublicError::from)?;
                if skills.len() > MAXIMUM_SKILLS_PER_SCOPE
                    || skills.iter().any(|skill| skill_scope(skill) != scope)
                {
                    return Err(OrmPublicError::new(
                        OrmErrorCode::AuthorizationMisconfigured,
                    ));
                }
                let mut rows = Vec::with_capacity(skills.len());
                for skill in skills {
                    let version = load_current_version(tx, &skill).await?;
                    rows.push((skill, version));
                }
                Ok(rows)
            })
        })
        .await
        .map_err(map_transaction)
}

async fn load_current_version(
    tx: &mut MutationContext<'_, DefaultWriteBackend>,
    skill: &AiSkillRecord,
) -> Result<Option<AiSkillVersionRecord>, OrmPublicError> {
    let Some(version_id) = skill.current_version_id else {
        if skill.enabled {
            return Err(OrmPublicError::new(
                OrmErrorCode::AuthorizationMisconfigured,
            ));
        }
        return Ok(None);
    };
    let mut versions = tx
        .query::<AiSkillVersionRecord>()
        .filter(AiSkillVersionRecordWhereInput {
            id: Some(graphql_orm::graphql::filters::UuidFilter {
                eq: Some(version_id),
                ..Default::default()
            }),
            ..Default::default()
        })
        .limit(2)
        .fetch_all()
        .await
        .map_err(OrmPublicError::from)?;
    if versions.len() != 1 {
        return Err(OrmPublicError::new(
            OrmErrorCode::AuthorizationMisconfigured,
        ));
    }
    let version = versions
        .pop()
        .ok_or_else(|| OrmPublicError::new(OrmErrorCode::AuthorizationMisconfigured))?;
    if version.skill_id != skill.id || !version.published {
        return Err(OrmPublicError::new(
            OrmErrorCode::AuthorizationMisconfigured,
        ));
    }
    Ok(Some(version))
}

fn skill_view(
    skill: &AiSkillRecord,
    version: Option<&AiSkillVersionRecord>,
) -> Result<AiSkillView, AiError> {
    let current_version = version.map(version_view).transpose()?;
    if skill.enabled && current_version.is_none() {
        return Err(AiError::InvalidConfiguration(
            "enabled skill has no current version".to_owned(),
        ));
    }
    Ok(AiSkillView {
        id: skill.id,
        scope_kind: skill.scope_kind.clone(),
        scope_id: skill.scope_id.clone(),
        tenant_id: skill.tenant_id.clone(),
        name: skill.name.clone(),
        description: skill.description.clone(),
        enabled: skill.enabled,
        current_version,
        row_version: skill.row_version,
        updated_at: skill.updated_at,
    })
}

fn version_view(version: &AiSkillVersionRecord) -> Result<AiSkillVersionView, AiError> {
    let parsed = parse_version(version)?;
    Ok(AiSkillVersionView {
        id: version.id,
        version: version.version.clone(),
        checksum: version.checksum.clone(),
        allowed_tool_fingerprints: parsed.allowed_tools.fingerprints,
        maximum_classification: parsed.data_policy.maximum_classification,
        maximum_tool_maturity: parsed.data_policy.maximum_tool_maturity,
        activation: parsed.activation_rule.kind,
        allowed_proposal_types: parsed.data_policy.allowed_proposal_types,
        allowed_ui_intents: parsed
            .data_policy
            .allowed_ui_intents
            .into_iter()
            .map(|binding| AiSkillUiIntentBindingView {
                intent_type: binding.intent_type,
                descriptor_fingerprint: binding.descriptor_fingerprint,
            })
            .collect(),
        maximum_steps: parsed.budgets.maximum_steps,
        maximum_duration_seconds: parsed.budgets.maximum_duration_seconds,
        maximum_output_tokens: parsed.budgets.maximum_output_tokens,
        maximum_cost_microunits: parsed.budgets.maximum_cost_microunits,
        author_subject: version.author_subject.clone(),
        created_at: version.created_at,
    })
}

fn parse_version(version: &AiSkillVersionRecord) -> Result<ParsedVersion, AiError> {
    if !version.published
        || version.checksum.len() != 64
        || !version
            .checksum
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || version.author_subject.trim().is_empty()
        || validate_version_label(&version.version).is_err()
    {
        return Err(AiError::InvalidConfiguration(
            "invalid published skill version".to_owned(),
        ));
    }
    let parsed = ParsedVersion {
        allowed_tools: from_value(version.allowed_tools.clone(), "allowed tools")?,
        data_policy: from_value(version.data_policy.clone(), "data policy")?,
        activation_rule: from_value(version.activation_rule.clone(), "activation rule")?,
        schemas: from_value(version.schemas.clone(), "schemas")?,
        budgets: from_value(version.budgets.clone(), "budgets")?,
        provenance: from_value(version.provenance.clone(), "provenance")?,
    };
    if [
        parsed.allowed_tools.version,
        parsed.data_policy.version,
        parsed.activation_rule.version,
        parsed.schemas.version,
        parsed.budgets.version,
        parsed.provenance.version,
    ]
    .into_iter()
    .any(|value| value != SKILL_POLICY_FORMAT_VERSION)
        || !matches!(
            parsed.data_policy.maximum_classification.as_str(),
            "public" | "internal" | "confidential" | "restricted"
        )
        || !matches!(
            parsed.data_policy.maximum_tool_maturity.as_str(),
            "read_only" | "proposal_only" | "supervised_write"
        )
        || !matches!(
            parsed.activation_rule.kind.as_str(),
            "manual" | "always_for_scope"
        )
        || parsed.provenance.source != "authenticated_graphql"
    {
        return Err(AiError::InvalidConfiguration(
            "unsupported skill policy format".to_owned(),
        ));
    }
    validate_schema(&parsed.schemas.input, "stored skill input")
        .map_err(|_| AiError::InvalidConfiguration("invalid stored skill schema".to_owned()))?;
    validate_schema(&parsed.schemas.output, "stored skill output")
        .map_err(|_| AiError::InvalidConfiguration("invalid stored skill schema".to_owned()))?;
    validate_budget(
        parsed.budgets.maximum_steps,
        parsed.budgets.maximum_duration_seconds,
        parsed.budgets.maximum_output_tokens,
        parsed.budgets.maximum_cost_microunits,
    )
    .map_err(|_| AiError::InvalidConfiguration("invalid stored skill budget".to_owned()))?;
    let canonical_fingerprints = normalized_fingerprints(parsed.allowed_tools.fingerprints.clone())
        .map_err(|_| {
            AiError::InvalidConfiguration("invalid stored skill tool policy".to_owned())
        })?;
    let canonical_proposal_types = normalized_logical_types(
        parsed.data_policy.allowed_proposal_types.clone(),
        "proposal type",
    )
    .map_err(|_| AiError::InvalidConfiguration("invalid stored skill policy".to_owned()))?;
    let canonical_ui_intents =
        normalized_stored_ui_intent_bindings(parsed.data_policy.allowed_ui_intents.clone())
            .map_err(|_| AiError::InvalidConfiguration("invalid stored skill policy".to_owned()))?;
    if canonical_fingerprints != parsed.allowed_tools.fingerprints
        || canonical_proposal_types != parsed.data_policy.allowed_proposal_types
        || canonical_ui_intents != parsed.data_policy.allowed_ui_intents
    {
        return Err(AiError::InvalidConfiguration(
            "noncanonical stored skill policy".to_owned(),
        ));
    }
    Ok(parsed)
}

fn skill_scope_filter(scope: &AiScope) -> AiSkillRecordWhereInput {
    AiSkillRecordWhereInput {
        scope_kind: Some(StringFilter {
            eq: Some(scope.kind.clone()),
            ..Default::default()
        }),
        scope_id: Some(StringFilter {
            eq: Some(scope.id.clone()),
            ..Default::default()
        }),
        tenant_id: Some(optional_string_filter(scope.tenant_id.as_deref())),
        ..Default::default()
    }
}

fn optional_string_filter(value: Option<&str>) -> StringFilter {
    match value {
        Some(value) => StringFilter {
            eq: Some(value.to_owned()),
            ..Default::default()
        },
        None => StringFilter {
            is_null: Some(true),
            ..Default::default()
        },
    }
}

fn skill_scope(skill: &AiSkillRecord) -> AiScope {
    AiScope {
        kind: skill.scope_kind.clone(),
        id: skill.scope_id.clone(),
        tenant_id: skill.tenant_id.clone(),
    }
}

fn validate_scope(scope: &AiScope) -> Result<(), AiError> {
    if scope.kind.trim().is_empty()
        || scope.id.trim().is_empty()
        || scope.kind.len() > 128
        || scope.id.len() > 512
        || scope
            .tenant_id
            .as_ref()
            .is_some_and(|value| value.trim().is_empty() || value.len() > 512)
    {
        return Err(AiError::InvalidInput("invalid AI skill scope".to_owned()));
    }
    Ok(())
}

fn validate_safe_text(
    value: &str,
    maximum_bytes: usize,
    allow_newlines: bool,
    field: &str,
) -> Result<(), AiError> {
    if value.is_empty()
        || value.len() > maximum_bytes
        || value.chars().any(|character| {
            character.is_control() && !(allow_newlines && matches!(character, '\n' | '\r' | '\t'))
        })
    {
        return Err(AiError::InvalidInput(format!("invalid {field}")));
    }
    Ok(())
}

fn validate_version_label(value: &str) -> Result<(), AiError> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(AiError::InvalidInput(
            "invalid skill version label".to_owned(),
        ));
    }
    Ok(())
}

fn validate_instruction_text(value: &str) -> Result<(), AiError> {
    if value.trim().is_empty() || value.len() > MAXIMUM_INSTRUCTION_BYTES || value.contains('\0') {
        return Err(AiError::InvalidInput(
            "invalid skill instruction text".to_owned(),
        ));
    }
    Ok(())
}

fn normalized_fingerprints(values: Vec<String>) -> Result<Vec<String>, AiError> {
    if values.len() > MAXIMUM_TOOL_FINGERPRINTS
        || values.iter().any(|value| {
            value.len() != 64
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
    {
        return Err(AiError::InvalidInput(
            "invalid skill tool fingerprint list".to_owned(),
        ));
    }
    let original_len = values.len();
    let set = values.into_iter().collect::<BTreeSet<_>>();
    if set.len() != original_len {
        return Err(AiError::InvalidInput(
            "invalid skill tool fingerprint list".to_owned(),
        ));
    }
    Ok(set.into_iter().collect())
}

fn normalized_logical_types(values: Vec<String>, field: &str) -> Result<Vec<String>, AiError> {
    if values.len() > MAXIMUM_LOGICAL_TYPES
        || values.iter().any(|value| {
            value.is_empty()
                || value.len() > 200
                || !value.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'.' | b'-' | b'_')
                })
        })
    {
        return Err(AiError::InvalidInput(format!("invalid skill {field} list")));
    }
    let original_len = values.len();
    let values = values.into_iter().collect::<BTreeSet<_>>();
    if values.len() != original_len {
        return Err(AiError::InvalidInput(format!("invalid skill {field} list")));
    }
    Ok(values.into_iter().collect())
}

fn normalized_ui_intent_bindings(
    values: Vec<AiSkillUiIntentBindingInput>,
) -> Result<Vec<StoredUiIntentBinding>, AiError> {
    normalized_stored_ui_intent_bindings(
        values
            .into_iter()
            .map(|binding| StoredUiIntentBinding {
                intent_type: binding.intent_type,
                descriptor_fingerprint: binding.descriptor_fingerprint,
            })
            .collect(),
    )
}

fn normalized_stored_ui_intent_bindings(
    values: Vec<StoredUiIntentBinding>,
) -> Result<Vec<StoredUiIntentBinding>, AiError> {
    if values.len() > MAXIMUM_LOGICAL_TYPES {
        return Err(AiError::InvalidInput(
            "invalid skill UI intent binding list".to_owned(),
        ));
    }
    let original_len = values.len();
    let mut bindings = BTreeMap::new();
    for binding in values {
        AiUiIntentTypeId::parse(binding.intent_type.clone()).map_err(|_| {
            AiError::InvalidInput("invalid skill UI intent binding list".to_owned())
        })?;
        if binding.descriptor_fingerprint.len() != 64
            || !binding
                .descriptor_fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || bindings
                .insert(binding.intent_type.clone(), binding)
                .is_some()
        {
            return Err(AiError::InvalidInput(
                "invalid skill UI intent binding list".to_owned(),
            ));
        }
    }
    if bindings.len() != original_len {
        return Err(AiError::InvalidInput(
            "invalid skill UI intent binding list".to_owned(),
        ));
    }
    Ok(bindings.into_values().collect())
}

fn validate_schema(value: &serde_json::Value, field: &str) -> Result<(), AiError> {
    let encoded = serde_json::to_vec(value)
        .map_err(|_| AiError::InvalidInput(format!("invalid {field} schema")))?;
    if encoded.len() > MAXIMUM_SCHEMA_BYTES || jsonschema::validator_for(value).is_err() {
        return Err(AiError::InvalidInput(format!("invalid {field} schema")));
    }
    Ok(())
}

fn validate_budget(
    steps: i64,
    duration_seconds: i64,
    output_tokens: i64,
    cost_microunits: Option<i64>,
) -> Result<(), AiError> {
    if !(1..=10_000).contains(&steps)
        || !(1..=604_800).contains(&duration_seconds)
        || !(1..=100_000_000).contains(&output_tokens)
        || cost_microunits.is_some_and(|value| !(0..=1_000_000_000_000_000).contains(&value))
    {
        return Err(AiError::InvalidInput(
            "invalid skill budget ceilings".to_owned(),
        ));
    }
    Ok(())
}

fn skill_checksum(content: SkillChecksumContent<'_>) -> Result<String, AiError> {
    let value = json!({
        "format": "graphql-orm-ai-skill-v1",
        "version": content.version,
        "instructions": content.instructions,
        "allowed_tools": content.allowed_tools,
        "data_policy": content.data_policy,
        "activation_rule": content.activation_rule,
        "schemas": content.schemas,
        "budgets": content.budgets,
        "provenance": content.provenance,
    });
    let encoded =
        serde_json::to_vec(&canonical_json(&value)).map_err(|_| AiError::PersistenceFailed)?;
    Ok(hex::encode(Sha256::digest(encoded)))
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
                .collect::<std::collections::BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        value => value.clone(),
    }
}

fn content_context(id: Uuid, scope: &AiScope) -> ContentProtectionContext {
    ContentProtectionContext {
        entity: "graphql_orm_ai_skill_versions".to_owned(),
        row_id: id.to_string(),
        field: "protected_instructions".to_owned(),
        scope: scope.clone(),
    }
}

async fn insert_skill_audit(
    tx: &mut MutationContext<'_, DefaultWriteBackend>,
    actor: &str,
    action: &str,
    skill_id: Uuid,
    reason_code: &str,
) -> Result<(), OrmPublicError> {
    tx.insert::<AiAuditEventRecord>(CreateAiAuditEventRecordInput {
        actor_principal_kind: "user".to_owned(),
        actor_subject: actor.to_owned(),
        action: action.to_owned(),
        resource_kind: "ai_skill".to_owned(),
        resource_reference: skill_id.to_string(),
        outcome: "allowed".to_owned(),
        reason_code: reason_code.to_owned(),
        correlation_id: Uuid::new_v4().to_string(),
        causation_id: None,
        policy_version: None,
    })
    .await
    .map_err(OrmPublicError::from)?;
    Ok(())
}

fn to_json<T: Serialize>(value: &T) -> Result<serde_json::Value, OrmPublicError> {
    serde_json::to_value(value).map_err(|_| OrmPublicError::new(OrmErrorCode::InternalError))
}

fn to_value<T: Serialize>(value: &T) -> Result<serde_json::Value, AiError> {
    serde_json::to_value(value).map_err(|_| AiError::PersistenceFailed)
}

fn from_value<T: for<'de> Deserialize<'de>>(
    value: serde_json::Value,
    field: &str,
) -> Result<T, AiError> {
    serde_json::from_value(value)
        .map_err(|_| AiError::InvalidConfiguration(format!("invalid stored skill {field}")))
}

fn map_protection(error: crate::ContentProtectionError) -> AiError {
    match error {
        crate::ContentProtectionError::PolicyNotReady => AiError::RuntimeNotReady,
        _ => AiError::PersistenceFailed,
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
