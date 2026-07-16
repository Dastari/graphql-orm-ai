use graphql_orm::graphql::orm::LeaseError;
use graphql_orm_ai::*;
use uuid::Uuid;

#[test]
fn reclaimed_worker_cannot_transition_or_append() {
    let mut run = AiRunLeaseMachine::queued("run-1", 0);
    let worker_a = run
        .claim("worker-a", Uuid::from_u128(1), 1_000, 100, 0)
        .expect("worker A should claim");
    run.transition(&worker_a, AiRunState::Running, 1_001, 1)
        .expect("worker A should start");

    run.state = AiRunState::RetryScheduled;
    let worker_b = run
        .claim("worker-b", Uuid::from_u128(2), 1_101, 100, 2)
        .expect("worker B should reclaim expired work");

    assert!(matches!(
        run.commit_child_write(&worker_a, 1_102, 3),
        Err(AiRunTransitionError::Lease(LeaseError::StaleFence))
    ));
    assert_eq!(
        run.transition(&worker_b, AiRunState::Running, 1_102, 3)
            .expect("current worker should transition"),
        4
    );
}

#[test]
fn restore_never_replays_uncertain_external_effect() {
    let fingerprint = "module-fingerprint";
    let reconciler = AiRestoreReconciler::new(fingerprint);
    let uncertain_run_id = AiRunId::new();
    let safe_run_id = AiRunId::new();
    let approval_wait_run_id = AiRunId::new();
    let provider_wait_run_id = AiRunId::new();
    let checkpointed_mutation_run_id = AiRunId::new();
    let uncheckpointed_mutation_run_id = AiRunId::new();
    let plan = reconciler.plan(&AiRestoreSnapshotFacts {
        module_fingerprint: fingerprint.to_owned(),
        missing_key_versions: vec![],
        runs: vec![
            AiRestoredRun {
                run_id: uncertain_run_id,
                state: AiRunState::WaitingTool,
                external_effect: AiExternalEffectState::Uncertain,
                coordinator_checkpoint: AiRestoredCoordinatorCheckpoint::None,
                has_provider_continuation: true,
                has_provider_file: false,
            },
            AiRestoredRun {
                run_id: safe_run_id,
                state: AiRunState::Running,
                external_effect: AiExternalEffectState::ProvenIdempotent,
                coordinator_checkpoint: AiRestoredCoordinatorCheckpoint::None,
                has_provider_continuation: false,
                has_provider_file: true,
            },
            AiRestoredRun {
                run_id: approval_wait_run_id,
                state: AiRunState::WaitingApproval,
                external_effect: AiExternalEffectState::None,
                coordinator_checkpoint: AiRestoredCoordinatorCheckpoint::None,
                has_provider_continuation: true,
                has_provider_file: false,
            },
            AiRestoredRun {
                run_id: provider_wait_run_id,
                state: AiRunState::WaitingProvider,
                external_effect: AiExternalEffectState::ProvenIdempotent,
                coordinator_checkpoint: AiRestoredCoordinatorCheckpoint::None,
                has_provider_continuation: true,
                has_provider_file: false,
            },
            AiRestoredRun {
                run_id: checkpointed_mutation_run_id,
                state: AiRunState::Running,
                external_effect: AiExternalEffectState::Confirmed,
                coordinator_checkpoint: AiRestoredCoordinatorCheckpoint::SupervisedToolBatch,
                has_provider_continuation: true,
                has_provider_file: false,
            },
            AiRestoredRun {
                run_id: uncheckpointed_mutation_run_id,
                state: AiRunState::Running,
                external_effect: AiExternalEffectState::Confirmed,
                coordinator_checkpoint: AiRestoredCoordinatorCheckpoint::None,
                has_provider_continuation: true,
                has_provider_file: false,
            },
        ],
        pending_approval_count: 2,
        pending_egress_consent_count: 3,
        invalid_attachment_count: 0,
        invalid_usage_fact_count: 0,
        invalid_budget_policy_count: 0,
        invalid_pricing_policy_count: 0,
        invalid_skill_catalog_count: 0,
        invalid_rule_policy_count: 0,
        invalid_coordinator_checkpoint_count: 0,
        invalid_context_checkpoint_count: 0,
        invalid_provider_webhook_receipt_count: 0,
        invalid_provider_background_submission_count: 0,
        invalid_ui_intent_event_count: 0,
        invalid_session_retention_count: 0,
        duplicate_stream_sequence_count: 0,
        stream_gap_count: 1,
    });

    let uncertain = plan
        .run_actions
        .iter()
        .find(|action| action.run_id == uncertain_run_id)
        .expect("uncertain action should exist");
    assert_eq!(
        uncertain.disposition,
        AiRestoredRunDisposition::RecoveryRequired
    );
    assert!(uncertain.clear_lease);
    assert!(uncertain.reverify_provider_continuation);

    let checkpointed_mutation = plan
        .run_actions
        .iter()
        .find(|action| action.run_id == checkpointed_mutation_run_id)
        .expect("checkpointed mutation action should exist");
    assert_eq!(
        checkpointed_mutation.disposition,
        AiRestoredRunDisposition::RequeueWithNewAttempt
    );
    assert!(checkpointed_mutation.reverify_provider_continuation);

    let uncheckpointed_mutation = plan
        .run_actions
        .iter()
        .find(|action| action.run_id == uncheckpointed_mutation_run_id)
        .expect("uncheckpointed mutation action should exist");
    assert_eq!(
        uncheckpointed_mutation.disposition,
        AiRestoredRunDisposition::RecoveryRequired
    );

    let safe = plan
        .run_actions
        .iter()
        .find(|action| action.run_id == safe_run_id)
        .expect("safe action should exist");
    assert_eq!(
        safe.disposition,
        AiRestoredRunDisposition::RequeueWithNewAttempt
    );
    assert!(safe.reverify_provider_file);
    let approval_wait = plan
        .run_actions
        .iter()
        .find(|action| action.run_id == approval_wait_run_id)
        .expect("approval wait action should exist");
    assert_eq!(
        approval_wait.disposition,
        AiRestoredRunDisposition::RecoveryRequired
    );
    assert!(approval_wait.reverify_provider_continuation);
    let provider_wait = plan
        .run_actions
        .iter()
        .find(|action| action.run_id == provider_wait_run_id)
        .expect("provider wait action should exist");
    assert_eq!(
        provider_wait.disposition,
        AiRestoredRunDisposition::RecoveryRequired
    );
    assert!(provider_wait.reverify_provider_continuation);
    assert_eq!(plan.approvals_to_revalidate, 2);
    assert_eq!(plan.consents_to_revalidate, 3);
    assert_eq!(plan.fatal_issue_count(), 0);
}

#[test]
fn legacy_restore_fact_has_no_checkpoint_authority() {
    let mut value = serde_json::to_value(AiRestoredRun {
        run_id: AiRunId::new(),
        state: AiRunState::Running,
        external_effect: AiExternalEffectState::Confirmed,
        coordinator_checkpoint: AiRestoredCoordinatorCheckpoint::SupervisedToolBatch,
        has_provider_continuation: true,
        has_provider_file: false,
    })
    .expect("restore fact should serialize");
    value
        .as_object_mut()
        .expect("restore fact should be an object")
        .remove("coordinator_checkpoint");
    let legacy: AiRestoredRun =
        serde_json::from_value(value).expect("legacy restore fact should fail closed");
    assert_eq!(
        legacy.coordinator_checkpoint,
        AiRestoredCoordinatorCheckpoint::None
    );
}

#[test]
fn restore_fatal_checks_keep_start_gate_closed() {
    let reconciler = AiRestoreReconciler::new("expected");
    let plan = reconciler.plan(&AiRestoreSnapshotFacts {
        module_fingerprint: "wrong".to_owned(),
        missing_key_versions: vec!["key-v1".to_owned()],
        runs: vec![],
        pending_approval_count: 0,
        pending_egress_consent_count: 0,
        invalid_attachment_count: 1,
        invalid_usage_fact_count: 1,
        invalid_budget_policy_count: 1,
        invalid_pricing_policy_count: 1,
        invalid_skill_catalog_count: 1,
        invalid_rule_policy_count: 1,
        invalid_coordinator_checkpoint_count: 1,
        invalid_context_checkpoint_count: 1,
        invalid_provider_webhook_receipt_count: 1,
        invalid_provider_background_submission_count: 1,
        invalid_ui_intent_event_count: 1,
        invalid_session_retention_count: 1,
        duplicate_stream_sequence_count: 1,
        stream_gap_count: 0,
    });

    assert_eq!(plan.fatal_issue_count(), 15);
    assert_eq!(
        plan.readiness_report_after_apply(true).fatal_issue_count,
        15
    );
}

#[test]
fn legacy_restore_facts_default_new_validation_counts_to_zero() {
    let facts = AiRestoreSnapshotFacts {
        module_fingerprint: "expected".to_owned(),
        missing_key_versions: Vec::new(),
        runs: Vec::new(),
        pending_approval_count: 0,
        pending_egress_consent_count: 0,
        invalid_attachment_count: 0,
        invalid_usage_fact_count: 0,
        invalid_budget_policy_count: 0,
        invalid_pricing_policy_count: 0,
        invalid_skill_catalog_count: 0,
        invalid_rule_policy_count: 0,
        invalid_coordinator_checkpoint_count: 0,
        invalid_context_checkpoint_count: 0,
        invalid_provider_webhook_receipt_count: 0,
        invalid_provider_background_submission_count: 0,
        invalid_ui_intent_event_count: 0,
        invalid_session_retention_count: 0,
        duplicate_stream_sequence_count: 0,
        stream_gap_count: 0,
    };
    let mut value = serde_json::to_value(facts).expect("restore facts should serialize");
    value
        .as_object_mut()
        .expect("restore facts should be an object")
        .remove("invalid_context_checkpoint_count");
    value
        .as_object_mut()
        .expect("restore facts should be an object")
        .remove("invalid_provider_webhook_receipt_count");
    value
        .as_object_mut()
        .expect("restore facts should be an object")
        .remove("invalid_provider_background_submission_count");
    let decoded: AiRestoreSnapshotFacts =
        serde_json::from_value(value).expect("legacy restore facts should decode");
    assert_eq!(decoded.invalid_context_checkpoint_count, 0);
    assert_eq!(decoded.invalid_provider_webhook_receipt_count, 0);
    assert_eq!(decoded.invalid_provider_background_submission_count, 0);
}
