//! Side-effect-safe restore reconciliation planning.

use serde::{Deserialize, Serialize};

use crate::{AiRunId, AiRunState, AiRuntimeReadinessReport};

/// External side-effect certainty captured for an interrupted run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiExternalEffectState {
    /// No external call/tool could have occurred.
    None,
    /// Interrupted work is proven idempotent under a stable key.
    ProvenIdempotent,
    /// A non-idempotent or unknown external effect may have occurred.
    Uncertain,
    /// External effect is confirmed and must not be repeated automatically.
    Confirmed,
}

/// Restored run facts needed for reconciliation; payloads are intentionally
/// absent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiRestoredRun {
    /// Run ID.
    pub run_id: AiRunId,
    /// State captured in the backup.
    pub state: AiRunState,
    /// External-effect certainty.
    pub external_effect: AiExternalEffectState,
    /// Whether a provider continuation reference exists.
    pub has_provider_continuation: bool,
    /// Whether a provider file reference exists.
    pub has_provider_file: bool,
}

/// Preflight facts for one restored snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiRestoreSnapshotFacts {
    /// Backup module fingerprint.
    pub module_fingerprint: String,
    /// Required encryption key versions missing from the deployment.
    pub missing_key_versions: Vec<String>,
    /// Runs requiring reconciliation.
    pub runs: Vec<AiRestoredRun>,
    /// Number of pending approvals to expire/revalidate.
    pub pending_approval_count: u64,
    /// Number of pending egress consents to expire/revalidate.
    pub pending_egress_consent_count: u64,
    /// Missing/corrupt attachment references.
    pub invalid_attachment_count: u64,
    /// Usage facts that fail reservation, scope, principal, provider, or
    /// non-negative/cached-subset integrity validation.
    pub invalid_usage_fact_count: u64,
    /// Duplicate durable stream sequence count.
    pub duplicate_stream_sequence_count: u64,
    /// Retention/known stream gap count.
    pub stream_gap_count: u64,
}

/// Planned recovery disposition for one run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiRestoredRunDisposition {
    /// Preserve a terminal state.
    PreserveTerminal,
    /// Requeue using a new attempt and fencing generation.
    RequeueWithNewAttempt,
    /// Require manual recovery review and never replay automatically.
    RecoveryRequired,
}

/// Redacted planned run repair.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiRestoredRunAction {
    /// Run ID.
    pub run_id: AiRunId,
    /// Recovery disposition.
    pub disposition: AiRestoredRunDisposition,
    /// Lease owner/attempt/expiry/heartbeat must be cleared.
    pub clear_lease: bool,
    /// Provider continuation must be reverified before use.
    pub reverify_provider_continuation: bool,
    /// Provider file must be reverified before use.
    pub reverify_provider_file: bool,
}

/// Stable restore issue severity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiRestoreIssueSeverity {
    /// Prevents runtime startup.
    Fatal,
    /// Requires reset/review but can be represented safely.
    Warning,
}

/// Redacted restore issue.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiRestoreIssue {
    /// Stable issue code.
    pub code: String,
    /// Severity.
    pub severity: AiRestoreIssueSeverity,
    /// Affected safe reference when useful.
    pub resource_ref: Option<String>,
}

/// Dry-run restore reconciliation plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiRestorePlan {
    /// Expected compiled module fingerprint.
    pub expected_module_fingerprint: String,
    /// Run repairs.
    pub run_actions: Vec<AiRestoredRunAction>,
    /// Pending approvals to expire/revalidate.
    pub approvals_to_revalidate: u64,
    /// Pending egress consents to expire/revalidate.
    pub consents_to_revalidate: u64,
    /// Redacted issues.
    pub issues: Vec<AiRestoreIssue>,
}

impl AiRestorePlan {
    /// Returns fatal issue count.
    pub fn fatal_issue_count(&self) -> u64 {
        self.issues
            .iter()
            .filter(|issue| issue.severity == AiRestoreIssueSeverity::Fatal)
            .count() as u64
    }

    /// Produces start-gate evidence after a trusted persistence adapter has
    /// applied and validated this exact plan.
    pub fn readiness_report_after_apply(&self, executor_bound: bool) -> AiRuntimeReadinessReport {
        AiRuntimeReadinessReport {
            module_fingerprint: self.expected_module_fingerprint.clone(),
            executor_bound,
            restore_reconciled: true,
            fatal_issue_count: self.fatal_issue_count(),
        }
    }
}

/// Pure reconciler. It plans database repairs but performs no I/O and no
/// external calls.
#[derive(Clone, Debug)]
pub struct AiRestoreReconciler {
    expected_module_fingerprint: String,
}

impl AiRestoreReconciler {
    /// Creates a reconciler for the compiled AI schema module.
    pub fn new(expected_module_fingerprint: impl Into<String>) -> Self {
        Self {
            expected_module_fingerprint: expected_module_fingerprint.into(),
        }
    }

    /// Builds a dry-run plan. This method never resumes provider work or
    /// invokes application tools.
    pub fn plan(&self, facts: &AiRestoreSnapshotFacts) -> AiRestorePlan {
        let mut issues = Vec::new();
        if facts.module_fingerprint != self.expected_module_fingerprint {
            issues.push(AiRestoreIssue {
                code: "AI_RESTORE_SCHEMA_FINGERPRINT_MISMATCH".to_owned(),
                severity: AiRestoreIssueSeverity::Fatal,
                resource_ref: None,
            });
        }
        for key_version in &facts.missing_key_versions {
            issues.push(AiRestoreIssue {
                code: "AI_RESTORE_ENCRYPTION_KEY_MISSING".to_owned(),
                severity: AiRestoreIssueSeverity::Fatal,
                resource_ref: Some(key_version.clone()),
            });
        }
        if facts.invalid_attachment_count > 0 {
            issues.push(AiRestoreIssue {
                code: "AI_RESTORE_ATTACHMENT_INVALID".to_owned(),
                severity: AiRestoreIssueSeverity::Fatal,
                resource_ref: None,
            });
        }
        if facts.invalid_usage_fact_count > 0 {
            issues.push(AiRestoreIssue {
                code: "AI_RESTORE_USAGE_FACT_INVALID".to_owned(),
                severity: AiRestoreIssueSeverity::Fatal,
                resource_ref: None,
            });
        }
        if facts.duplicate_stream_sequence_count > 0 {
            issues.push(AiRestoreIssue {
                code: "AI_RESTORE_STREAM_SEQUENCE_DUPLICATE".to_owned(),
                severity: AiRestoreIssueSeverity::Fatal,
                resource_ref: None,
            });
        }
        if facts.stream_gap_count > 0 {
            issues.push(AiRestoreIssue {
                code: "AI_RESTORE_STREAM_GAP_RESET_REQUIRED".to_owned(),
                severity: AiRestoreIssueSeverity::Warning,
                resource_ref: None,
            });
        }

        let run_actions = facts
            .runs
            .iter()
            .map(|run| AiRestoredRunAction {
                run_id: run.run_id,
                disposition: restored_run_disposition(run),
                clear_lease: true,
                reverify_provider_continuation: run.has_provider_continuation,
                reverify_provider_file: run.has_provider_file,
            })
            .collect();

        AiRestorePlan {
            expected_module_fingerprint: self.expected_module_fingerprint.clone(),
            run_actions,
            approvals_to_revalidate: facts.pending_approval_count,
            consents_to_revalidate: facts.pending_egress_consent_count,
            issues,
        }
    }
}

fn restored_run_disposition(run: &AiRestoredRun) -> AiRestoredRunDisposition {
    if run.state.is_terminal() {
        return AiRestoredRunDisposition::PreserveTerminal;
    }
    if run.state == AiRunState::RecoveryRequired {
        return AiRestoredRunDisposition::RecoveryRequired;
    }
    match run.external_effect {
        AiExternalEffectState::None | AiExternalEffectState::ProvenIdempotent => {
            AiRestoredRunDisposition::RequeueWithNewAttempt
        }
        AiExternalEffectState::Uncertain | AiExternalEffectState::Confirmed => {
            AiRestoredRunDisposition::RecoveryRequired
        }
    }
}
