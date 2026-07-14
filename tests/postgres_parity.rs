#![cfg(feature = "postgres")]

use std::process::{Command, Output};
use std::sync::Arc;
use std::thread;
use std::time::{Duration as StdDuration, Instant};

use agql_auth::{AccessTokenMetadata, AuthPrincipal, AuthUser, SessionContext, SystemClock};
use async_trait::async_trait;
use graphql_orm::graphql::orm::{ApplyOptions, OrmSchemaModule};
use graphql_orm::graphql::pagination::KeysetConnectionInput;
use graphql_orm::prelude::{Database, PostgresBackend};
use graphql_orm_ai::*;
use time::Duration;
use uuid::Uuid;

const LOCAL_DOCKER_SOCKET: &str = "unix:///var/run/docker.sock";
const POSTGRES_IMAGE: &str = "postgres:17-alpine";

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
                "pg_isready -U $POSTGRES_USER -d $POSTGRES_DB",
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

fn principal() -> AuthPrincipal {
    AuthPrincipal::User(AuthUser {
        user_id: "postgres-parity-owner".to_owned(),
        session_id: Uuid::new_v4(),
        roles: vec![],
        scopes: vec![],
        session: SessionContext::default(),
        token_claims: AccessTokenMetadata {
            tenant_id: Some("postgres-parity-tenant".to_owned()),
            ..AccessTokenMetadata::default()
        },
    })
}

#[tokio::test]
async fn owned_postgres_runs_generated_migration_sessions_and_fencing() {
    let Some(container) = OwnedPostgres::start().expect("owned PostgreSQL should start") else {
        return;
    };
    let database = Database::<PostgresBackend>::connect_postgres(container.connection_url())
        .await
        .expect("ORM should connect only to the owned container");
    let module = AiSchemaModule;
    let migration = database
        .schema()
        .plan_migration_to_entities(
            "ai-postgres-parity-v023",
            "graphql-orm-ai disposable PostgreSQL parity",
            module.entities(),
        )
        .await
        .expect("generated AI schema should plan for PostgreSQL");
    database
        .schema()
        .apply_migration(&migration, ApplyOptions::default())
        .await
        .expect("generated AI schema should apply to the owned PostgreSQL database");

    let sessions = OrmAiSessionService::new(
        database.clone(),
        Arc::new(AllowAll),
        Arc::new(ProtectionPolicy),
        Arc::new(DatabaseManagedContentProtector),
    );
    let principal = principal();
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
    drop(runs);
    drop(container);
}
