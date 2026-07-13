use std::collections::BTreeSet;
use std::sync::Arc;

use agql_auth::{
    AccessTokenMetadata, AuthPrincipal, AuthUser, CurrentPrincipalResolver, PrincipalReference,
    ResolvedPrincipal, SessionContext,
};
use async_trait::async_trait;
use graphql_orm_ai::*;
use serde_json::json;
use time::OffsetDateTime;
use uuid::Uuid;

fn principal(scopes: &[&str]) -> AuthPrincipal {
    AuthPrincipal::User(AuthUser {
        user_id: "user-1".to_owned(),
        session_id: Uuid::from_u128(1),
        roles: vec![],
        scopes: scopes.iter().map(|scope| (*scope).to_owned()).collect(),
        session: SessionContext::default(),
        token_claims: AccessTokenMetadata {
            tenant_id: Some("tenant-1".to_owned()),
            resource_type: Some("project".to_owned()),
            resource_id: Some("project-1".to_owned()),
            ..AccessTokenMetadata::default()
        },
    })
}

struct Resolver(AuthPrincipal);

#[async_trait]
impl CurrentPrincipalResolver for Resolver {
    async fn resolve(
        &self,
        reference: &PrincipalReference,
    ) -> agql_auth::AuthResult<ResolvedPrincipal> {
        ResolvedPrincipal::new(
            reference.clone(),
            self.0.clone(),
            OffsetDateTime::UNIX_EPOCH,
        )
    }
}

struct ContextFactory;

#[async_trait]
impl GraphqlRequestContextFactory for ContextFactory {
    async fn build(
        &self,
        principal: &ResolvedPrincipal,
        _target: &GraphqlExecutionTarget,
        _request: &ToolGraphqlRequest,
    ) -> Result<GraphqlRequestContext, ToolExecutionError> {
        Ok(GraphqlRequestContext::new(
            principal.principal().scopes().to_vec(),
        ))
    }
}

struct Executor;

#[async_trait]
impl AuthenticatedGraphqlExecutor for Executor {
    async fn execute(
        &self,
        context: GraphqlRequestContext,
        request: ToolGraphqlRequest,
    ) -> Result<ToolGraphqlResponse, ToolExecutionError> {
        let scopes = context
            .downcast_ref::<Vec<String>>()
            .ok_or(ToolExecutionError::RequestContext)?;
        let data = if request
            .variables
            .get("emitUnknown")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            json!({"scopes": scopes, "credential": "must-not-escape"})
        } else {
            json!({"scopes": scopes})
        };
        Ok(ToolGraphqlResponse {
            data,
            error_codes: vec![],
            application_audit_ref: Some("audit-1".to_owned()),
        })
    }
}

struct AllowEgress;

struct AllowAccess;

struct AllowTools;

struct ProtectionPolicy;

#[async_trait]
impl AiContentProtectionPolicyResolver for ProtectionPolicy {
    async fn resolve(
        &self,
        _principal: &AuthPrincipal,
        scope: &AiScope,
    ) -> Result<AiContentProtectionPolicy, AiError> {
        Ok(AiContentProtectionPolicy {
            scope: scope.clone(),
            mode: AiContentProtectionMode::DatabaseManaged,
            key_policy_reference: None,
            version: 1,
            ready: true,
        })
    }
}

#[async_trait]
impl AiAccessPolicy for AllowAccess {
    async fn can_access_scope(
        &self,
        _principal: &AuthPrincipal,
        _scope: &AiScope,
        _action: AiSessionAction,
    ) -> AiAccessDecision {
        AiAccessDecision::allow("test", "test-1")
    }

    async fn can_access_session(
        &self,
        _principal: &AuthPrincipal,
        _session_id: AiSessionId,
        _action: AiSessionAction,
    ) -> AiAccessDecision {
        AiAccessDecision::allow("test", "test-1")
    }
}

#[async_trait]
impl AiToolAuthorizationPolicy for AllowTools {
    async fn authorize(
        &self,
        principal: &ResolvedPrincipal,
        _scope: &AiScope,
        _descriptor: &AiToolDescriptor,
        variables: &serde_json::Value,
    ) -> AiToolAuthorizationDecision {
        if variables
            .get("incompleteAuthorization")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            return AiToolAuthorizationDecision::allow("", "", "");
        }
        AiToolAuthorizationDecision::allow(
            "test",
            "tool-policy-v1",
            format!("auth-state:{}", principal.principal().subject()),
        )
    }
}

#[async_trait]
impl AiEgressPolicy for AllowEgress {
    async fn authorize(
        &self,
        principal: &ResolvedPrincipal,
        manifest: &AiEgressManifest,
    ) -> AiEgressDecision {
        AiEgressDecision::allow(manifest, "scope-policy-1", principal.principal().subject())
    }
}

fn runtime() -> AiRuntime {
    let document = "query Current { current { scopes } }";
    let disclosure = AiDisclosureSchema::new(
        "current-v1",
        AiDisclosureShape::object(
            AiDisclosureRule::exportable(DataClassification::Internal),
            [(
                "scopes".to_owned(),
                AiDisclosureShape::list(
                    AiDisclosureRule::exportable(DataClassification::Internal),
                    32,
                    AiDisclosureShape::scalar(AiDisclosureRule::exportable(
                        DataClassification::Internal,
                    )),
                ),
            )],
        ),
    )
    .expect("disclosure schema should validate");
    let contract = GraphqlOperationContract::new(
        GraphqlExecutionTargetId::parse("local-app").expect("target ID"),
        "schema-v1",
        "Current",
        document,
        "projection-v1",
        disclosure.fingerprint.clone(),
    )
    .expect("contract should validate");
    let descriptor = AiToolDescriptor::new(
        "records.current",
        "Read the current principal's visible records",
        AiToolOperationKind::Query,
        document,
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "emitUnknown": { "type": "boolean" },
                "incompleteAuthorization": { "type": "boolean" }
            },
            "additionalProperties": false
        }),
    )
    .expect("descriptor should validate")
    .with_result_projection("projection-v1")
    .with_graphql_contract(contract);
    let mut tool_catalog = AiToolCatalog::new();
    tool_catalog
        .register_with_disclosure(descriptor, disclosure)
        .expect("tool should register");
    let mut targets = GraphqlExecutionTargetRegistry::new();
    targets
        .register(GraphqlExecutionTarget {
            id: GraphqlExecutionTargetId::parse("local-app").expect("target ID"),
            class: GraphqlExecutionTargetClass::Local,
            audience: None,
            resource_type: None,
            resource_id: None,
            schema_fingerprint: "schema-v1".to_owned(),
        })
        .expect("target should register");
    AiRuntime::builder()
        .principal_resolver(Arc::new(Resolver(principal(&["records:read"]))))
        .access_policy(Arc::new(AllowAccess))
        .tool_authorization_policy(Arc::new(AllowTools))
        .request_context_factory(Arc::new(ContextFactory))
        .graphql_executor(Arc::new(Executor))
        .graphql_targets(targets)
        .egress_policy(Arc::new(AllowEgress))
        .deployment_egress(AiDeploymentEgressBoundary {
            allowed_destination_trust: BTreeSet::from([AiDestinationTrust::ManagedProvider]),
            allowed_capabilities: BTreeSet::from([AiEgressCapability::ModelInference]),
            maximum_classification: DataClassification::Internal,
            maximum_bytes: 1_000,
            maximum_attachments: 0,
        })
        .maximum_tool_maturity(ToolMaturity::ProposalOnly)
        .tool_catalog(tool_catalog)
        .secret_store(Arc::new(EnvironmentSecretStore::new()))
        .content_protection_policy_resolver(Arc::new(ProtectionPolicy))
        .content_protector(Arc::new(DatabaseManagedContentProtector))
        .build()
        .expect("runtime configuration should validate")
}

fn current_request(runtime: &AiRuntime, variables: serde_json::Value) -> ToolGraphqlRequest {
    let document = "query Current { current { scopes } }";
    ToolGraphqlRequest {
        document: document.to_owned(),
        operation_name: "Current".to_owned(),
        contract: GraphqlOperationContract::new(
            GraphqlExecutionTargetId::parse("local-app").expect("target ID"),
            "schema-v1",
            "Current",
            document,
            "projection-v1",
            runtime
                .tool_catalog()
                .disclosure_schema(
                    &AiToolId::parse("records.current").expect("tool ID should validate"),
                )
                .expect("disclosure schema should be registered")
                .fingerprint
                .clone(),
        )
        .expect("contract should validate"),
        variables,
        invocation: GraphqlInvocationContext {
            run_id: AiRunId::new(),
            tool_call_id: AiToolCallId::new(),
            scope: AiScope::new("project", "project-1"),
            correlation_id: "correlation-1".to_owned(),
            causation_id: "command-1".to_owned(),
            delegation_reference: None,
            idempotency_key: None,
        },
    }
}

fn open_runtime(runtime: &AiRuntime) {
    runtime
        .start_gate()
        .open(&AiRuntimeReadinessReport {
            module_fingerprint: runtime
                .start_gate()
                .expected_module_fingerprint()
                .to_owned(),
            executor_bound: true,
            restore_reconciled: true,
            fatal_issue_count: 0,
        })
        .expect("matching readiness should open runtime");
}

#[tokio::test]
async fn runtime_is_closed_until_matching_readiness_evidence() {
    let runtime = runtime();
    let principal_reference = principal(&["stale:scope"]).reference();
    let request = current_request(&runtime, json!({}));

    assert!(matches!(
        runtime
            .execute_tool(
                &principal_reference,
                &AiToolId::parse("records.current").expect("tool ID"),
                request.clone(),
            )
            .await,
        Err(AiError::RuntimeNotReady)
    ));
    assert!(
        runtime
            .start_gate()
            .open(&AiRuntimeReadinessReport {
                module_fingerprint: "wrong".to_owned(),
                executor_bound: true,
                restore_reconciled: true,
                fatal_issue_count: 0,
            })
            .is_err()
    );

    open_runtime(&runtime);
    let response = runtime
        .execute_tool(
            &principal_reference,
            &AiToolId::parse("records.current").expect("tool ID"),
            request,
        )
        .await
        .expect("ready runtime should execute through the bridge");

    assert_eq!(
        response.response().data,
        json!({"scopes": ["records:read"]})
    );
    assert_eq!(
        response.response().application_audit_ref.as_deref(),
        Some("audit-1")
    );
    assert_eq!(response.policy_version(), "tool-policy-v1");
    assert_eq!(
        response.disclosure().maximum_classification,
        DataClassification::Internal
    );
}

#[tokio::test]
async fn runtime_rejects_invalid_arguments_and_non_disclosed_resolver_fields() {
    let runtime = runtime();
    open_runtime(&runtime);
    let principal_reference = principal(&["records:read"]).reference();
    let tool_id = AiToolId::parse("records.current").expect("tool ID");

    assert!(matches!(
        runtime
            .execute_tool(
                &principal_reference,
                &tool_id,
                current_request(&runtime, json!({"unknown": true})),
            )
            .await,
        Err(AiError::InvalidInput(_))
    ));
    assert!(matches!(
        runtime
            .execute_tool(
                &principal_reference,
                &tool_id,
                current_request(&runtime, json!({"emitUnknown": true})),
            )
            .await,
        Err(AiError::ToolExecutionFailed)
    ));
    assert!(matches!(
        runtime
            .execute_tool(
                &principal_reference,
                &tool_id,
                current_request(&runtime, json!({"incompleteAuthorization": true})),
            )
            .await,
        Err(AiError::ToolExecutionFailed)
    ));

    let mut stale_schema = current_request(&runtime, json!({}));
    stale_schema.contract.schema_fingerprint = "schema-v2".to_owned();
    assert!(matches!(
        runtime
            .execute_tool(&principal_reference, &tool_id, stale_schema)
            .await,
        Err(AiError::Forbidden)
    ));
}

#[test]
fn runtime_builder_requires_every_security_boundary() {
    let result = AiRuntime::builder().build();
    assert!(matches!(result, Err(AiError::InvalidConfiguration(_))));
}
