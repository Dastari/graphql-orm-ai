//! ORM-backed append-only egress decision audit.

#![cfg(any(feature = "sqlite", feature = "postgres"))]

use async_trait::async_trait;
use graphql_orm::db::Database;
use graphql_orm::graphql::errors::{OrmErrorCode, OrmPublicError};
use graphql_orm::graphql::filters::UuidFilter;
use graphql_orm::graphql::orm::{DefaultWriteBackend, TransactionError, TransactionMode};

use crate::persistence::*;
use crate::{
    AiEgressCapability, AiEgressDecision, AiEgressDecisionAudit, AiEgressManifest, AiEgressOutcome,
    AiEgressReason, AiError, DataClassification,
};

/// Immutable egress audit implemented only through generated `graphql-orm`
/// repository operations.
#[derive(Clone)]
pub struct OrmAiEgressDecisionAudit {
    database: Database<DefaultWriteBackend>,
}

impl OrmAiEgressDecisionAudit {
    /// Creates an ORM-backed egress decision audit.
    pub fn new(database: Database<DefaultWriteBackend>) -> Self {
        Self { database }
    }

    /// Returns the ORM database handle for host schema composition.
    pub fn database(&self) -> &Database<DefaultWriteBackend> {
        &self.database
    }
}

#[async_trait]
impl AiEgressDecisionAudit for OrmAiEgressDecisionAudit {
    async fn record(
        &self,
        manifest: &AiEgressManifest,
        decision: &AiEgressDecision,
    ) -> Result<(), AiError> {
        validate_record(manifest, decision)?;
        let manifest = manifest.clone();
        let decision = decision.clone();
        self.database
            .transaction(TransactionMode::Default, move |tx| {
                Box::pin(async move {
                    if let Some(existing) = tx
                        .query::<AiEgressEventRecord>()
                        .filter(AiEgressEventRecordWhereInput {
                            id: Some(UuidFilter {
                                eq: Some(decision.id.0),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .limit(1)
                        .fetch_one()
                        .await
                        .map_err(OrmPublicError::from)?
                    {
                        if record_matches(&existing, &manifest, &decision) {
                            return Ok(());
                        }
                        return Err(OrmPublicError::new(OrmErrorCode::Conflict));
                    }
                    let classification =
                        classification_value(manifest.maximum_classification()).to_owned();
                    tx.insert::<AiEgressEventRecord>(CreateAiEgressEventRecordInput {
                        id: decision.id.0,
                        run_id: manifest.run_id.map(|run_id| run_id.0),
                        principal_subject: decision.principal_subject,
                        scope_kind: manifest.scope.kind,
                        scope_id: manifest.scope.id,
                        manifest_hash: decision.manifest_hash,
                        destination: manifest.destination,
                        capability: capability_value(manifest.capability).to_owned(),
                        classification,
                        outcome: outcome_value(decision.outcome).to_owned(),
                        reason_code: reason_value(decision.reason).to_owned(),
                        policy_version: decision.policy_version,
                        estimated_bytes: i64::try_from(manifest.estimated_bytes)
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::InvalidInput))?,
                        estimated_tokens: i64::try_from(manifest.estimated_tokens)
                            .map_err(|_| OrmPublicError::new(OrmErrorCode::InvalidInput))?,
                    })
                    .await
                    .map_err(OrmPublicError::from)?;
                    Ok(())
                })
            })
            .await
            .map_err(map_transaction)
    }
}

fn validate_record(
    manifest: &AiEgressManifest,
    decision: &AiEgressDecision,
) -> Result<(), AiError> {
    if decision.manifest_hash != manifest.stable_hash()
        || decision.principal_subject.trim().is_empty()
        || decision.principal_subject.len() > 1_024
        || manifest.scope.kind.trim().is_empty()
        || manifest.scope.kind.len() > 128
        || manifest.scope.id.trim().is_empty()
        || manifest.scope.id.len() > 1_024
        || manifest.destination.trim().is_empty()
        || manifest.destination.len() > 1_024
        || decision.policy_version.trim().is_empty()
        || decision.policy_version.len() > 256
        || i64::try_from(manifest.estimated_bytes).is_err()
        || i64::try_from(manifest.estimated_tokens).is_err()
    {
        return Err(AiError::InvalidInput(
            "invalid redacted egress audit event".to_owned(),
        ));
    }
    Ok(())
}

fn record_matches(
    record: &AiEgressEventRecord,
    manifest: &AiEgressManifest,
    decision: &AiEgressDecision,
) -> bool {
    record.id == decision.id.0
        && record.run_id == manifest.run_id.map(|run_id| run_id.0)
        && record.principal_subject == decision.principal_subject
        && record.scope_kind == manifest.scope.kind
        && record.scope_id == manifest.scope.id
        && record.manifest_hash == decision.manifest_hash
        && record.destination == manifest.destination
        && record.capability == capability_value(manifest.capability)
        && record.classification == classification_value(manifest.maximum_classification())
        && record.outcome == outcome_value(decision.outcome)
        && record.reason_code == reason_value(decision.reason)
        && record.policy_version == decision.policy_version
        && u64::try_from(record.estimated_bytes).ok() == Some(manifest.estimated_bytes)
        && u64::try_from(record.estimated_tokens).ok() == Some(manifest.estimated_tokens)
}

const fn capability_value(value: AiEgressCapability) -> &'static str {
    match value {
        AiEgressCapability::ModelInference => "model_inference",
        AiEgressCapability::WebSearch => "web_search",
        AiEgressCapability::ImageAnalysis => "image_analysis",
        AiEgressCapability::ImageGeneration => "image_generation",
        AiEgressCapability::ProviderFile => "provider_file",
        AiEgressCapability::CodeExecution => "code_execution",
        AiEgressCapability::RemoteMcp => "remote_mcp",
        AiEgressCapability::ToolResult => "tool_result",
    }
}

const fn classification_value(value: DataClassification) -> &'static str {
    match value {
        DataClassification::Public => "public",
        DataClassification::Internal => "internal",
        DataClassification::Confidential => "confidential",
        DataClassification::Restricted => "restricted",
        DataClassification::Secret => "secret",
    }
}

const fn outcome_value(value: AiEgressOutcome) -> &'static str {
    match value {
        AiEgressOutcome::Allow => "allow",
        AiEgressOutcome::Deny => "deny",
    }
}

const fn reason_value(value: AiEgressReason) -> &'static str {
    match value {
        AiEgressReason::Allowed => "allowed",
        AiEgressReason::DeploymentDenied => "deployment_denied",
        AiEgressReason::PolicyDenied => "policy_denied",
        AiEgressReason::PrincipalDenied => "principal_denied",
        AiEgressReason::ClassificationDenied => "classification_denied",
        AiEgressReason::SecretDataDenied => "secret_data_denied",
        AiEgressReason::ConsentRequired => "consent_required",
        AiEgressReason::LimitExceeded => "limit_exceeded",
    }
}

fn map_transaction(error: TransactionError) -> AiError {
    let public = error.public_error();
    match public.code {
        OrmErrorCode::InvalidInput
        | OrmErrorCode::CursorInvalid
        | OrmErrorCode::PageLimitExceeded => AiError::InvalidInput(public.message.clone()),
        OrmErrorCode::Unauthenticated | OrmErrorCode::Forbidden => AiError::Forbidden,
        OrmErrorCode::NotFound => AiError::NotFound,
        OrmErrorCode::Conflict | OrmErrorCode::ConstraintViolation => AiError::Conflict,
        OrmErrorCode::ServiceUnavailable
        | OrmErrorCode::InternalError
        | OrmErrorCode::AuthorizationMisconfigured => AiError::PersistenceFailed,
    }
}
