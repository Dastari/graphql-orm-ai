#![cfg(feature = "postgres")]

use std::process::{Command, Output};
use std::sync::Arc;
use std::thread;
use std::time::{Duration as StdDuration, Instant};

use agql_auth::{
    AccessTokenMetadata, AssuranceMatchMode, AuthPrincipal, AuthUser, FixedClock, MfaAcceptance,
    RecentMfaPolicy, SessionAssurance, SessionContext, SystemClock,
};
use async_trait::async_trait;
use graphql_orm::graphql::orm::{ApplyOptions, Entity, OrmSchemaModule};
use graphql_orm::graphql::pagination::KeysetConnectionInput;
use graphql_orm::prelude::{Database, GraphQLEntity, GraphQLOperations, PostgresBackend};
use graphql_orm_ai::*;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

#[cfg(feature = "provider-openai")]
use base64::Engine;
#[cfg(feature = "provider-openai")]
use base64::engine::general_purpose::STANDARD;
#[cfg(feature = "provider-openai")]
use hmac::{Hmac, Mac};
#[cfg(feature = "provider-openai")]
use secrecy::SecretString;
#[cfg(feature = "provider-openai")]
use sha2::Sha256;

const LOCAL_DOCKER_SOCKET: &str = "unix:///var/run/docker.sock";
const POSTGRES_IMAGE: &str = "postgres:17-alpine";

#[derive(
    GraphQLEntity, GraphQLOperations, serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq,
)]
#[graphql_entity(
    table = "graphql_orm_ai_provider_background_submissions",
    plural = "LegacyGraphqlOrmAiProviderBackgroundSubmissions",
    default_sort = "created_at ASC"
)]
struct LegacyProviderBackgroundSubmissionRecord {
    #[primary_key]
    #[graphql_orm(auto_generated = false)]
    #[filterable(type = "uuid")]
    id: graphql_orm::uuid::Uuid,
    #[unique]
    submission_key: String,
    #[filterable(type = "uuid")]
    session_id: graphql_orm::uuid::Uuid,
    #[filterable(type = "uuid")]
    run_id: graphql_orm::uuid::Uuid,
    #[unique]
    #[filterable(type = "uuid")]
    attempt_id: graphql_orm::uuid::Uuid,
    lease_generation: i64,
    provider_kind: String,
    provider_profile_id: String,
    provider_model: String,
    maximum_output_tokens: i64,
    provider_store: Option<bool>,
    request_hash: String,
    #[unique]
    budget_reservation_id: graphql_orm::uuid::Uuid,
    egress_decision_id: graphql_orm::uuid::Uuid,
    egress_manifest_hash: String,
    #[unique]
    provider_response_id: Option<String>,
    provider_status: Option<String>,
    #[filterable(type = "string")]
    state: String,
    safe_error_code: Option<String>,
    #[sortable]
    created_at: i64,
    provider_created_at: Option<i64>,
    submitted_at: Option<i64>,
    #[graphql_orm(version, default = "0")]
    row_version: i64,
}

struct OwnedPostgres {
    container_id: String,
    owner_token: String,
    user: String,
    password: String,
    database: String,
    port: u16,
}

impl OwnedPostgres {
    fn start() -> Result<Option<Self>, String> {
        if !local_docker_available() {
            if std::env::var_os("CI").is_some() {
                return Err(
                    "the CI PostgreSQL parity job requires the local Docker socket".to_owned(),
                );
            }
            eprintln!("skipping PostgreSQL parity: local Docker socket is unavailable");
            return Ok(None);
        }

        let owner_token = Uuid::new_v4().simple().to_string();
        let user = format!("ai_user_{owner_token}");
        let password = format!("ai_password_{owner_token}");
        let database = format!("ai_database_{owner_token}");
        let name = format!("graphql-orm-ai-pg-{owner_token}");
        let output = local_docker()
            .args([
                "run",
                "--detach",
                "--rm",
                "--pull=missing",
                "--name",
                &name,
                "--label",
                &format!("com.dastari.graphql-orm-ai.test-owner={owner_token}"),
                "--publish",
                "127.0.0.1::5432/tcp",
                "--tmpfs",
                "/var/lib/postgresql/data:rw,nosuid,nodev,size=1g",
                "--health-cmd",
                "pg_isready -h 127.0.0.1 -U $POSTGRES_USER -d $POSTGRES_DB",
                "--health-interval",
                "250ms",
                "--health-timeout",
                "2s",
                "--health-retries",
                "120",
                "--env",
                &format!("POSTGRES_USER={user}"),
                "--env",
                &format!("POSTGRES_PASSWORD={password}"),
                "--env",
                &format!("POSTGRES_DB={database}"),
                POSTGRES_IMAGE,
            ])
            .output()
            .map_err(|_| "failed to invoke the local Docker CLI".to_owned())?;
        if !output.status.success() {
            return Err("failed to create the owned disposable PostgreSQL container".to_owned());
        }
        let container_id = String::from_utf8(output.stdout)
            .map_err(|_| "Docker returned a non-UTF-8 container ID".to_owned())?
            .trim()
            .to_owned();
        if container_id.len() < 12 || !container_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err("Docker returned an invalid container ID".to_owned());
        }

        let mut owned = Self {
            container_id,
            owner_token,
            user,
            password,
            database,
            port: 0,
        };
        owned.port = owned.published_port()?;
        owned.wait_until_healthy()?;
        Ok(Some(owned))
    }

    fn connection_url(&self) -> String {
        format!(
            "postgres://{}:{}@127.0.0.1:{}/{}?sslmode=disable",
            self.user, self.password, self.port, self.database
        )
    }

    fn published_port(&self) -> Result<u16, String> {
        let output = self.docker_output(&["port", &self.container_id, "5432/tcp"])?;
        if !output.status.success() {
            return Err("failed to read the owned PostgreSQL port".to_owned());
        }
        let value = String::from_utf8(output.stdout)
            .map_err(|_| "Docker returned a non-UTF-8 port".to_owned())?;
        let port = value
            .trim()
            .strip_prefix("127.0.0.1:")
            .ok_or_else(|| "Docker did not bind PostgreSQL to IPv4 loopback".to_owned())?
            .parse::<u16>()
            .map_err(|_| "Docker returned an invalid PostgreSQL port".to_owned())?;
        if port == 0 {
            return Err("Docker returned an invalid PostgreSQL port".to_owned());
        }
        Ok(port)
    }

    fn wait_until_healthy(&self) -> Result<(), String> {
        let deadline = Instant::now() + StdDuration::from_secs(30);
        while Instant::now() < deadline {
            let output = self.docker_output(&[
                "inspect",
                "--format={{.State.Health.Status}}",
                &self.container_id,
            ])?;
            if output.status.success() {
                let health = String::from_utf8(output.stdout)
                    .map_err(|_| "Docker returned non-UTF-8 health state".to_owned())?;
                match health.trim() {
                    "healthy" => return Ok(()),
                    "unhealthy" => {
                        return Err("the owned PostgreSQL container became unhealthy".to_owned());
                    }
                    _ => {}
                }
            }
            thread::sleep(StdDuration::from_millis(250));
        }
        Err("the owned PostgreSQL container did not become healthy in time".to_owned())
    }

    fn docker_output(&self, args: &[&str]) -> Result<Output, String> {
        local_docker()
            .args(args)
            .output()
            .map_err(|_| "failed to invoke the local Docker CLI".to_owned())
    }

    fn still_owned(&self) -> bool {
        self.docker_output(&[
            "inspect",
            "--format={{index .Config.Labels \"com.dastari.graphql-orm-ai.test-owner\"}}",
            &self.container_id,
        ])
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .is_some_and(|value| value.trim() == self.owner_token)
    }
}

impl Drop for OwnedPostgres {
    fn drop(&mut self) {
        if self.still_owned() {
            let _ = self.docker_output(&["rm", "--force", &self.container_id]);
        }
    }
}

fn local_docker() -> Command {
    let mut command = Command::new("docker");
    command.args(["--host", LOCAL_DOCKER_SOCKET]);
    command
}

fn local_docker_available() -> bool {
    local_docker()
        .args(["version", "--format={{.Server.Version}}"])
        .output()
        .is_ok_and(|output| output.status.success())
}

struct AllowAll;

#[async_trait]
impl AiAccessPolicy for AllowAll {
    async fn can_access_scope(
        &self,
        _principal: &AuthPrincipal,
        _scope: &AiScope,
        _action: AiSessionAction,
    ) -> AiAccessDecision {
        AiAccessDecision::allow("postgres-parity", "postgres-parity-v1")
    }

    async fn can_access_session(
        &self,
        _principal: &AuthPrincipal,
        _session_id: AiSessionId,
        _action: AiSessionAction,
    ) -> AiAccessDecision {
        AiAccessDecision::allow("postgres-parity", "postgres-parity-v1")
    }
}

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

struct AllowSkills;

#[async_trait]
impl AiSkillAccessPolicy for AllowSkills {
    async fn can_access_skill(
        &self,
        _principal: &AuthPrincipal,
        _scope: &AiScope,
        _action: AiSkillAction,
    ) -> bool {
        true
    }
}

struct AllowRules;

#[async_trait]
impl AiRuleAccessPolicy for AllowRules {
    async fn can_access_rule(
        &self,
        _principal: &AuthPrincipal,
        _scope: &AiScope,
        _action: AiRuleAction,
    ) -> bool {
        true
    }
}

struct ExactRuleHierarchy;

#[async_trait]
impl AiRuleHierarchyResolver for ExactRuleHierarchy {
    async fn hierarchy(
        &self,
        _principal: &AuthPrincipal,
        target_scope: &AiScope,
    ) -> Result<Vec<AiScope>, AiError> {
        Ok(vec![target_scope.clone()])
    }
}

#[cfg(feature = "provider-openai")]
struct PostgresWebhookSecrets(SecretRef, String);

#[cfg(feature = "provider-openai")]
#[async_trait]
impl AiSecretStore for PostgresWebhookSecrets {
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

#[cfg(feature = "provider-openai")]
async fn verify_webhook_receipt_parity(database: &Database<PostgresBackend>, now: OffsetDateTime) {
    let reference = SecretRef::parse("openai/postgres-webhook-parity")
        .expect("webhook secret reference should validate");
    let secret = "synthetic-postgres-webhook-secret";
    let verifier = OpenAiWebhookVerifier::new(
        "postgres-parity-profile",
        reference.clone(),
        Arc::new(PostgresWebhookSecrets(reference, secret.to_owned())),
        Arc::new(FixedClock::new(now)),
    )
    .expect("PostgreSQL webhook verifier should build");
    let timestamp = now.unix_timestamp();
    let body = format!(
        r#"{{"id":"evt_postgres_parity","type":"response.completed","created_at":{},"data":{{"id":"resp_postgres_parity"}}}}"#,
        timestamp.saturating_sub(1)
    );
    let webhook_id = "delivery_postgres_parity";
    let mut signed = format!("{webhook_id}.{timestamp}.").into_bytes();
    signed.extend_from_slice(body.as_bytes());
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .expect("synthetic webhook secret should validate");
    mac.update(&signed);
    let signature = STANDARD.encode(mac.finalize().into_bytes());
    let headers =
        OpenAiWebhookHeaders::new(webhook_id, timestamp.to_string(), format!("v1,{signature}"))
            .expect("PostgreSQL webhook headers should validate");
    let event = verifier
        .verify(&headers, body.as_bytes())
        .await
        .expect("PostgreSQL webhook signature should verify");
    let receipts = OrmAiProviderWebhookReceiptService::new(database.clone());
    let first_receipts = receipts.clone();
    let first_event = event.clone();
    let second_receipts = receipts.clone();
    let second_event = event.clone();
    let (first, second) = tokio::join!(
        first_receipts.record(&first_event),
        second_receipts.record(&second_event)
    );
    let outcomes = [
        first.expect("first PostgreSQL webhook intake should succeed"),
        second.expect("second PostgreSQL webhook intake should succeed"),
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
            .filter(|outcome| matches!(outcome, AiProviderWebhookReceiptOutcome::AlreadyRecorded))
            .count(),
        1
    );
}

fn rule_deployment_limits() -> AiRuleDeploymentLimits {
    AiRuleDeploymentLimits::new(
        4,
        AiRuleConstraints {
            enabled: true,
            maximum_classification: DataClassification::Restricted,
            maximum_tool_maturity: ToolMaturity::SupervisedWrite,
            approval_requirement: AiRuleApprovalRequirement::DescriptorPolicy,
            allowed_tool_fingerprints: None,
            allowed_provider_kinds: None,
            allowed_provider_capabilities: None,
            allow_provider_retention: true,
            allow_byok: true,
            budget: AiRuleBudgetCeilings {
                maximum_steps: Some(100),
                maximum_duration_seconds: Some(3_600),
                maximum_output_tokens: Some(32_000),
                maximum_cost_microunits: Some(10_000_000),
                maximum_provider_calls: Some(100),
                maximum_tool_units: Some(100),
                maximum_image_units: Some(10),
            },
        },
    )
    .expect("PostgreSQL rule deployment limits should validate")
}

fn principal(now: OffsetDateTime) -> AuthPrincipal {
    let assurance = SessionAssurance::new(
        now,
        ["otp", "pwd"],
        Some("urn:postgres-parity:loa:2".to_owned()),
        Some("postgres-parity".to_owned()),
        MfaAcceptance::Satisfied,
    )
    .expect("test assurance should validate");
    AuthPrincipal::User(AuthUser {
        user_id: "postgres-parity-owner".to_owned(),
        session_id: Uuid::new_v4(),
        roles: vec![],
        scopes: vec![],
        session: SessionContext::default().with_assurance(assurance),
        token_claims: AccessTokenMetadata {
            auth_time: Some(now.unix_timestamp()),
            amr: Some(vec!["otp".to_owned(), "pwd".to_owned()]),
            acr: Some("urn:postgres-parity:loa:2".to_owned()),
            tenant_id: Some("postgres-parity-tenant".to_owned()),
            ..AccessTokenMetadata::default()
        },
    })
}

#[tokio::test]
async fn owned_postgres_runs_generated_migration_sessions_skills_rules_and_fencing() {
    let Some(container) = OwnedPostgres::start().expect("owned PostgreSQL should start") else {
        return;
    };
    let database = Database::<PostgresBackend>::connect_postgres(container.connection_url())
        .await
        .expect("ORM should connect only to the owned container");
    let module = AiSchemaModule;
    let mut prior_entities = module
        .entities()
        .iter()
        .copied()
        .filter(|entity| entity.table_name != "graphql_orm_ai_provider_background_submissions")
        .collect::<Vec<_>>();
    prior_entities.push(LegacyProviderBackgroundSubmissionRecord::metadata());
    let prior_migration = database
        .schema()
        .plan_migration_to_entities(
            "ai-postgres-parity-v047",
            "graphql-orm-ai prior disposable PostgreSQL parity",
            &prior_entities,
        )
        .await
        .expect("prior generated AI schema should plan for PostgreSQL");
    database
        .schema()
        .apply_migration(&prior_migration, ApplyOptions::default())
        .await
        .expect("prior generated AI schema should apply to the owned PostgreSQL database");
    let migration = database
        .schema()
        .plan_migration_to_entities(
            "ai-postgres-parity-v048",
            "graphql-orm-ai current disposable PostgreSQL parity",
            module.entities(),
        )
        .await
        .expect("current generated AI schema should plan for PostgreSQL");
    database
        .schema()
        .apply_migration(&migration, ApplyOptions::default())
        .await
        .expect("background reconciliation schema should migrate in owned PostgreSQL");

    let sessions = OrmAiSessionService::new(
        database.clone(),
        Arc::new(AllowAll),
        Arc::new(ProtectionPolicy),
        Arc::new(DatabaseManagedContentProtector),
    );
    let now = OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp())
        .expect("current whole-second test time should validate");
    let principal = principal(now);
    #[cfg(feature = "provider-openai")]
    verify_webhook_receipt_parity(&database, now).await;
    let session = sessions
        .create_session(
            &principal,
            CreateAiSessionInput {
                scope: AiScopeInput {
                    kind: "postgres-parity".to_owned(),
                    id: "scope-1".to_owned(),
                    tenant_id: Some("postgres-parity-tenant".to_owned()),
                },
                title: Some("Disposable PostgreSQL parity".to_owned()),
            },
        )
        .await
        .expect("session should be inserted through generated ORM APIs");
    let queued = sessions
        .send_message(
            &principal,
            SendAiMessageInput {
                session_id: session.id,
                text: "Synthetic PostgreSQL parity message".to_owned(),
                attachment_ids: vec![],
                client_message_id: Uuid::new_v4(),
            },
        )
        .await
        .expect("message, block, event, inbox fact, and run should commit atomically");
    let messages = sessions
        .messages(
            &principal,
            AiSessionId(session.id),
            KeysetConnectionInput {
                last: Some(10),
                ..Default::default()
            }
            .validate(10, 10)
            .expect("bounded keyset should validate"),
        )
        .await
        .expect("generated PostgreSQL keyset query should load the message");
    assert_eq!(messages.edges.len(), 1);
    assert_eq!(messages.edges[0].node.id, queued.message_id);

    let skills = OrmAiSkillCatalogService::new(
        database.clone(),
        Arc::new(AllowSkills),
        Arc::new(ProtectionPolicy),
        Arc::new(DatabaseManagedContentProtector),
        RecentMfaPolicy {
            maximum_age: Duration::minutes(5),
            clock_skew: Duration::seconds(30),
            allowed_amr: vec!["otp".to_owned()],
            allowed_acr: vec!["urn:postgres-parity:loa:2".to_owned()],
            match_mode: AssuranceMatchMode::All,
        },
        Arc::new(FixedClock::new(now)),
    );
    let skill = skills
        .upsert_skill(
            &principal,
            UpsertAiSkillInput {
                id: None,
                scope: AiScopeInput {
                    kind: "postgres-parity".to_owned(),
                    id: "scope-1".to_owned(),
                    tenant_id: Some("postgres-parity-tenant".to_owned()),
                },
                name: "PostgreSQL parity skill".to_owned(),
                description: "Exercises generated exact-scope skill persistence.".to_owned(),
                expected_version: None,
            },
        )
        .await
        .expect("skill metadata should persist through generated PostgreSQL ORM APIs");
    let published_skill = skills
        .publish_version(
            &principal,
            PublishAiSkillVersionInput {
                skill_id: skill.id,
                expected_skill_version: skill.row_version,
                version: "1".to_owned(),
                instructions: "Use only freshly authorized tools.".to_owned(),
                allowed_tool_fingerprints: vec!["a".repeat(64)],
                maximum_classification: AiSkillClassificationInput::Internal,
                maximum_tool_maturity: AiSkillMaturityInput::ReadOnly,
                activation: AiSkillActivationInput::Manual,
                input_schema: async_graphql::Json(serde_json::json!({
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "type": "object",
                    "additionalProperties": false
                })),
                output_schema: async_graphql::Json(serde_json::json!({
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "type": "object",
                    "additionalProperties": false
                })),
                required_provider_capabilities: AiSkillProviderCapabilitiesInput::default(),
                budget: AiSkillBudgetInput {
                    maximum_steps: 2,
                    maximum_duration_seconds: 30,
                    maximum_output_tokens: 1_024,
                    maximum_cost_microunits: None,
                },
                allowed_proposal_types: vec![],
                allowed_ui_intents: vec![],
                enable: true,
            },
        )
        .await
        .expect("protected immutable skill version should persist on PostgreSQL");
    assert!(published_skill.enabled);
    assert_eq!(
        skills
            .resolve_enabled_skills(
                &principal,
                AiScope::new("postgres-parity", "scope-1").with_tenant_id("postgres-parity-tenant"),
            )
            .await
            .expect("exact-scope PostgreSQL skill query should resolve")
            .len(),
        1
    );

    let rule_scope =
        AiScope::new("postgres-parity", "scope-1").with_tenant_id("postgres-parity-tenant");
    let rules = OrmAiRulePolicyService::new(
        database.clone(),
        Arc::new(AllowRules),
        Arc::new(ExactRuleHierarchy),
        RecentMfaPolicy {
            maximum_age: Duration::minutes(5),
            clock_skew: Duration::seconds(30),
            allowed_amr: vec!["otp".to_owned()],
            allowed_acr: vec!["urn:postgres-parity:loa:2".to_owned()],
            match_mode: AssuranceMatchMode::All,
        },
        Arc::new(FixedClock::new(now)),
        rule_deployment_limits(),
    );
    rules
        .set_policy(
            &principal,
            SetAiRulePolicyInput {
                scope: AiScopeInput {
                    kind: rule_scope.kind.clone(),
                    id: rule_scope.id.clone(),
                    tenant_id: rule_scope.tenant_id.clone(),
                },
                enabled: true,
                maximum_classification: AiRuleClassificationInput::Internal,
                maximum_tool_maturity: AiRuleToolMaturityInput::ReadOnly,
                approval_requirement: AiRuleApprovalRequirement::OneShotForAllApplicationTools,
                allowed_tool_fingerprints: Some(vec!["b".repeat(64)]),
                allowed_provider_kinds: Some(vec![AiProviderKindInput::Ollama]),
                allowed_provider_capabilities: Some(vec![AiRuleProviderCapability::Streaming]),
                allow_provider_retention: false,
                allow_byok: false,
                budget: AiRuleBudgetInput {
                    maximum_steps: Some(5),
                    maximum_duration_seconds: Some(120),
                    maximum_output_tokens: Some(1_024),
                    maximum_cost_microunits: Some(500_000),
                    maximum_provider_calls: Some(5),
                    maximum_tool_units: Some(2),
                    maximum_image_units: Some(0),
                },
                expected_version: None,
            },
        )
        .await
        .expect("strict rule policy should persist through generated PostgreSQL ORM APIs");
    let resolved_rules = rules
        .resolve_for_run(&principal, rule_scope)
        .await
        .expect("exact PostgreSQL rule hierarchy should resolve");
    assert_eq!(
        resolved_rules.constraints().maximum_classification,
        DataClassification::Internal
    );
    assert_eq!(resolved_rules.constraints().budget.maximum_steps, Some(5));
    assert!(!resolved_rules.constraints().allow_byok);

    let runs = OrmAiRunService::new(
        database,
        Arc::new(SystemClock),
        AiRunServiceLimits::new(Duration::seconds(30), Duration::minutes(5), 16, 2, 8)
            .expect("run limits should validate"),
    );
    let claimed = runs
        .claim_next("postgres-parity-worker")
        .await
        .expect("generated PostgreSQL claim transaction should succeed")
        .expect("queued run should be claimable");
    assert_eq!(claimed.run_id().0, queued.run_id);
    let running = runs
        .start(&claimed)
        .await
        .expect("fenced PostgreSQL state transition should succeed");
    assert!(matches!(runs.start(&claimed).await, Err(AiError::Conflict)));
    assert_eq!(running.run_id(), claimed.run_id());

    drop(sessions);
    drop(skills);
    drop(rules);
    drop(runs);
    drop(container);
}
