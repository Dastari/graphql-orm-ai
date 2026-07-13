#![cfg(feature = "local-harness")]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::TryStreamExt;
use graphql_orm_ai::{
    AiBudgetAmounts, AiBudgetReservation, AiBudgetReservationId, AiDataSourceRef,
    AiDestinationTrust, AiEgressCapability, AiEgressDecision, AiEgressManifest,
    AiJsonLinesLocalHarnessDriver, AiLocalHarnessLimits, AiLocalHarnessProcess,
    AiLocalHarnessProcessError, AiLocalHarnessProcessLauncher, AiLocalHarnessProcessOutput,
    AiLocalHarnessProvider, AiLocalHarnessRegistration, AiLocalHarnessRegistry, AiProvider,
    AiRunId, AiScope, AiSessionId, AiSourceTrust, DataClassification, ModelInputBlock,
    ModelRequest, ProviderCapabilities, ProviderError, ProviderEvent, ProviderKind,
    ProviderRequestContext,
};

#[derive(Default)]
struct FakeState {
    launches: usize,
    input: Vec<u8>,
    stdin_closed: bool,
    terminated: bool,
    dropped_before_exit: bool,
    executable: String,
    arguments: Vec<String>,
    sandbox_profile: String,
}

struct FakeLauncher {
    outputs: Mutex<Option<VecDeque<AiLocalHarnessProcessOutput>>>,
    state: Arc<Mutex<FakeState>>,
}

impl FakeLauncher {
    fn new(outputs: Vec<AiLocalHarnessProcessOutput>) -> (Arc<Self>, Arc<Mutex<FakeState>>) {
        let state = Arc::new(Mutex::new(FakeState::default()));
        (
            Arc::new(Self {
                outputs: Mutex::new(Some(outputs.into())),
                state: state.clone(),
            }),
            state,
        )
    }
}

#[async_trait]
impl AiLocalHarnessProcessLauncher for FakeLauncher {
    async fn launch(
        &self,
        registration: Arc<AiLocalHarnessRegistration>,
    ) -> Result<Box<dyn AiLocalHarnessProcess>, AiLocalHarnessProcessError> {
        let mut state = self.state.lock().expect("fake state should lock");
        state.launches += 1;
        state.executable = registration.executable().to_string_lossy().into_owned();
        state.arguments = registration.arguments().to_vec();
        state.sandbox_profile = registration.sandbox_profile().to_owned();
        drop(state);
        let outputs = self
            .outputs
            .lock()
            .expect("fake outputs should lock")
            .take()
            .ok_or(AiLocalHarnessProcessError::Unavailable)?;
        Ok(Box::new(FakeProcess {
            outputs,
            state: self.state.clone(),
            exited: false,
        }))
    }
}

struct FakeProcess {
    outputs: VecDeque<AiLocalHarnessProcessOutput>,
    state: Arc<Mutex<FakeState>>,
    exited: bool,
}

impl Drop for FakeProcess {
    fn drop(&mut self) {
        if !self.exited {
            self.state
                .lock()
                .expect("fake state should lock")
                .dropped_before_exit = true;
        }
    }
}

#[async_trait]
impl AiLocalHarnessProcess for FakeProcess {
    async fn write_stdin(&mut self, bytes: &[u8]) -> Result<(), AiLocalHarnessProcessError> {
        self.state
            .lock()
            .expect("fake state should lock")
            .input
            .extend_from_slice(bytes);
        Ok(())
    }

    async fn close_stdin(&mut self) -> Result<(), AiLocalHarnessProcessError> {
        self.state
            .lock()
            .expect("fake state should lock")
            .stdin_closed = true;
        Ok(())
    }

    async fn next_output(
        &mut self,
    ) -> Result<AiLocalHarnessProcessOutput, AiLocalHarnessProcessError> {
        let output = self
            .outputs
            .pop_front()
            .ok_or(AiLocalHarnessProcessError::OutputFailed)?;
        if matches!(output, AiLocalHarnessProcessOutput::Exited { .. }) {
            self.exited = true;
        }
        Ok(output)
    }

    async fn terminate(&mut self) -> Result<(), AiLocalHarnessProcessError> {
        self.state
            .lock()
            .expect("fake state should lock")
            .terminated = true;
        self.exited = true;
        Ok(())
    }
}

fn capabilities() -> ProviderCapabilities {
    ProviderCapabilities {
        streaming: true,
        structured_output: true,
        local: true,
        maximum_context_tokens: Some(4_096),
        maximum_output_tokens: Some(128),
        ..ProviderCapabilities::default()
    }
}

fn registration(model: &str) -> AiLocalHarnessRegistration {
    AiLocalHarnessRegistration::new(
        model,
        "/opt/reviewed/bin/synthetic-harness",
        vec!["--json-lines".to_owned(), "--single-turn".to_owned()],
        "/var/empty/graphql-orm-ai-harness",
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "synthetic-1.0.0",
        "isolated-no-network-v1",
        AiLocalHarnessLimits::default(),
        capabilities(),
    )
    .expect("synthetic registration should validate")
}

fn request(model: &str) -> ModelRequest {
    ModelRequest {
        model: model.to_owned(),
        instructions: vec!["Return only a short answer.".to_owned()],
        input: vec![ModelInputBlock::Text {
            text: "synthetic local request".to_owned(),
        }],
        continuation: None,
        tools: vec![],
        builtin_tools: vec![],
        output_schema: None,
        maximum_output_tokens: Some(64),
    }
}

fn context(model_request: &ModelRequest) -> ProviderRequestContext {
    let session_id = AiSessionId::new();
    let run_id = AiRunId::new();
    let attempt_id = uuid::Uuid::new_v4();
    let manifest = AiEgressManifest {
        provider_profile_id: "installed-local-profile".to_owned(),
        provider_kind: ProviderKind::LocalHarness.as_str().to_owned(),
        model: model_request.model.clone(),
        destination: "installed-local-harness".to_owned(),
        destination_trust: AiDestinationTrust::Local,
        capability: AiEgressCapability::ModelInference,
        scope: AiScope::new("project", "synthetic"),
        session_id: Some(session_id),
        run_id: Some(run_id),
        sources: vec![AiDataSourceRef {
            kind: "message".to_owned(),
            reference: "synthetic-message".to_owned(),
            classification: DataClassification::Public,
            trust: AiSourceTrust::UserProvided,
        }],
        estimated_bytes: 1_000_000,
        estimated_tokens: 1_000,
        attachment_count: 0,
        purpose: "test".to_owned(),
        retention: "none".to_owned(),
        residency: Some("local".to_owned()),
        policy_version: "test-v1".to_owned(),
        consent_reference: None,
    };
    let proof = AiEgressDecision::allow(&manifest, "test", "test-principal")
        .authorize(&manifest)
        .expect("manifest should authorize");
    let budget = AiBudgetReservation::new_reserved(
        AiBudgetReservationId::new(),
        run_id,
        attempt_id,
        1,
        ProviderKind::LocalHarness,
        &model_request.model,
        "test-pricing-v1",
        AiBudgetAmounts {
            input_tokens: 1_000,
            output_tokens: 1_000,
            runs: 1,
            ..AiBudgetAmounts::default()
        },
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .expect("budget should validate")
    .authorize_provider_call(
        run_id,
        attempt_id,
        1,
        &ProviderKind::LocalHarness,
        &model_request.model,
        64,
        time::OffsetDateTime::now_utc(),
    )
    .expect("budget should authorize");
    ProviderRequestContext::new(session_id, run_id, "test", budget, manifest, proof)
        .expect("context should validate")
}

fn encoded_events(events: &[ProviderEvent]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for event in events {
        bytes.extend(serde_json::to_vec(event).expect("event should encode"));
        bytes.push(b'\n');
    }
    bytes
}

#[test]
fn deployment_registration_is_fixed_redacted_and_capability_narrow() {
    let registration = registration("local/synthetic");
    let debug = format!("{registration:?}");
    assert!(!debug.contains("/opt/reviewed"));
    assert!(!debug.contains("--json-lines"));
    assert!(!debug.contains(registration.executable_sha256()));
    assert!(!debug.contains("isolated-no-network-v1"));
    assert!(!debug.contains("synthetic-1.0.0"));

    assert!(matches!(
        AiLocalHarnessRegistration::new(
            "local/synthetic",
            "relative/harness",
            vec![],
            "/var/empty",
            registration.executable_sha256(),
            "1.0.0",
            "sandbox",
            AiLocalHarnessLimits::default(),
            capabilities(),
        ),
        Err(ProviderError::InvalidConfiguration(_))
    ));

    let mut unsafe_capabilities = capabilities();
    unsafe_capabilities.code_execution = true;
    assert!(matches!(
        AiLocalHarnessRegistration::new(
            "local/synthetic",
            "/opt/harness",
            vec![],
            "/var/empty",
            registration.executable_sha256(),
            "1.0.0",
            "sandbox",
            AiLocalHarnessLimits::default(),
            unsafe_capabilities,
        ),
        Err(ProviderError::InvalidConfiguration(_))
    ));
}

#[tokio::test]
async fn fake_process_receives_only_bounded_protocol_and_normalizes_output() {
    let expected_events = vec![
        ProviderEvent::ResponseStarted { response_id: None },
        ProviderEvent::TextDelta {
            text: "synthetic answer".to_owned(),
        },
        ProviderEvent::Usage {
            input_tokens: 5,
            output_tokens: 2,
            cached_input_tokens: 0,
        },
        ProviderEvent::ResponseCompleted { response_id: None },
    ];
    let encoded = encoded_events(&expected_events);
    let split = encoded.len() / 2;
    let outputs = vec![
        AiLocalHarnessProcessOutput::Stdout(encoded[..split].to_vec()),
        AiLocalHarnessProcessOutput::Stderr(b"synthetic-secret-diagnostic".to_vec()),
        AiLocalHarnessProcessOutput::Stdout(encoded[split..].to_vec()),
        AiLocalHarnessProcessOutput::Exited { success: true },
    ];
    let (launcher, state) = FakeLauncher::new(outputs);
    let driver = Arc::new(AiJsonLinesLocalHarnessDriver::new(launcher));
    let registry = AiLocalHarnessRegistry::new([registration("local/synthetic")])
        .expect("registry should validate");
    let provider = AiLocalHarnessProvider::new(registry, driver);
    let model_request = request("local/synthetic");
    let events = provider
        .stream(model_request.clone(), context(&model_request))
        .await
        .expect("fake process should start")
        .try_collect::<Vec<_>>()
        .await
        .expect("fake process events should normalize");
    assert_eq!(events, expected_events);

    let state = state.lock().expect("fake state should lock");
    assert_eq!(state.launches, 1);
    assert_eq!(state.executable, "/opt/reviewed/bin/synthetic-harness");
    assert_eq!(state.arguments, ["--json-lines", "--single-turn"]);
    assert_eq!(state.sandbox_profile, "isolated-no-network-v1");
    assert!(state.stdin_closed);
    assert!(!state.terminated);
    assert!(!state.dropped_before_exit);
    let request_value: serde_json::Value = serde_json::from_slice(
        state
            .input
            .strip_suffix(b"\n")
            .expect("input must be one framed line"),
    )
    .expect("input frame should be JSON");
    assert_eq!(
        request_value["protocol"],
        "graphql-orm-ai/local-harness-jsonl/v1"
    );
    assert_eq!(request_value["model"], "local/synthetic");
    let input_text = String::from_utf8_lossy(&state.input);
    assert!(!input_text.contains("/opt/reviewed"));
    assert!(!input_text.contains("--json-lines"));
    assert!(!input_text.contains("isolated-no-network"));
    assert!(!input_text.contains("synthetic-secret-diagnostic"));
}

#[tokio::test]
async fn unknown_model_and_forbidden_process_events_fail_closed() {
    let forbidden = encoded_events(&[
        ProviderEvent::ResponseStarted { response_id: None },
        ProviderEvent::ToolCallStarted {
            call_id: "call-1".to_owned(),
            tool_id: "forbidden".to_owned(),
        },
    ]);
    let outputs = vec![
        AiLocalHarnessProcessOutput::Stdout(forbidden),
        AiLocalHarnessProcessOutput::Exited { success: true },
    ];
    let (launcher, state) = FakeLauncher::new(outputs);
    let driver = Arc::new(AiJsonLinesLocalHarnessDriver::new(launcher));
    let registry = AiLocalHarnessRegistry::new([registration("local/synthetic")])
        .expect("registry should validate");
    let provider = AiLocalHarnessProvider::new(registry, driver);

    let unknown_request = request("local/unknown");
    assert!(matches!(
        provider
            .stream(unknown_request.clone(), context(&unknown_request))
            .await,
        Err(ProviderError::Unsupported)
    ));
    assert_eq!(state.lock().expect("fake state should lock").launches, 0);

    let model_request = request("local/synthetic");
    let swapped_context = context(&request("local/swapped-budget-model"));
    assert!(matches!(
        provider
            .stream(model_request.clone(), swapped_context)
            .await,
        Err(ProviderError::BudgetDenied)
    ));
    assert_eq!(state.lock().expect("fake state should lock").launches, 0);

    let error = provider
        .stream(model_request.clone(), context(&model_request))
        .await
        .expect("fake process should start")
        .try_collect::<Vec<_>>()
        .await
        .expect_err("process-requested tool authority must be rejected");
    assert!(matches!(error, ProviderError::Rejected));
    let state = state.lock().expect("fake state should lock");
    assert!(state.terminated);
    assert!(!state.dropped_before_exit);
}

#[tokio::test]
async fn output_limits_and_truncated_frames_terminate_the_process() {
    let oversized_stderr = vec![b'x'; AiLocalHarnessLimits::default().maximum_stderr_bytes() + 1];
    let outputs = vec![
        AiLocalHarnessProcessOutput::Stderr(oversized_stderr),
        AiLocalHarnessProcessOutput::Exited { success: true },
    ];
    let (launcher, state) = FakeLauncher::new(outputs);
    let provider = AiLocalHarnessProvider::new(
        AiLocalHarnessRegistry::new([registration("local/synthetic")])
            .expect("registry should validate"),
        Arc::new(AiJsonLinesLocalHarnessDriver::new(launcher)),
    );
    let model_request = request("local/synthetic");
    let error = provider
        .stream(model_request.clone(), context(&model_request))
        .await
        .expect("fake process should start")
        .try_collect::<Vec<_>>()
        .await
        .expect_err("oversized stderr must fail");
    assert!(matches!(error, ProviderError::Rejected));
    assert!(state.lock().expect("fake state should lock").terminated);

    let outputs = vec![
        AiLocalHarnessProcessOutput::Stdout(b"{\"type\":\"response_started\"}".to_vec()),
        AiLocalHarnessProcessOutput::Exited { success: true },
    ];
    let (launcher, state) = FakeLauncher::new(outputs);
    let provider = AiLocalHarnessProvider::new(
        AiLocalHarnessRegistry::new([registration("local/synthetic")])
            .expect("registry should validate"),
        Arc::new(AiJsonLinesLocalHarnessDriver::new(launcher)),
    );
    let error = provider
        .stream(model_request.clone(), context(&model_request))
        .await
        .expect("fake process should start")
        .try_collect::<Vec<_>>()
        .await
        .expect_err("a partial final frame must fail");
    assert!(matches!(error, ProviderError::Rejected));
    assert!(state.lock().expect("fake state should lock").terminated);
}

#[tokio::test]
async fn dropping_a_partial_stream_invokes_the_launcher_kill_on_drop_contract() {
    let complete_events = encoded_events(&[
        ProviderEvent::ResponseStarted { response_id: None },
        ProviderEvent::TextDelta {
            text: "more output would follow".to_owned(),
        },
        ProviderEvent::Usage {
            input_tokens: 1,
            output_tokens: 1,
            cached_input_tokens: 0,
        },
        ProviderEvent::ResponseCompleted { response_id: None },
    ]);
    let outputs = vec![
        AiLocalHarnessProcessOutput::Stdout(complete_events),
        AiLocalHarnessProcessOutput::Exited { success: true },
    ];
    let (launcher, state) = FakeLauncher::new(outputs);
    let provider = AiLocalHarnessProvider::new(
        AiLocalHarnessRegistry::new([registration("local/synthetic")])
            .expect("registry should validate"),
        Arc::new(AiJsonLinesLocalHarnessDriver::new(launcher)),
    );
    let model_request = request("local/synthetic");
    let mut stream = provider
        .stream(model_request.clone(), context(&model_request))
        .await
        .expect("fake process should start");
    assert!(matches!(
        stream.try_next().await.expect("first frame should parse"),
        Some(ProviderEvent::ResponseStarted { response_id: None })
    ));
    drop(stream);
    tokio::task::yield_now().await;
    assert!(
        state
            .lock()
            .expect("fake state should lock")
            .dropped_before_exit
    );
}
