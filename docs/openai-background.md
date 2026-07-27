# Exact OpenAI Background Submission

The `provider-openai` feature plus SQLite or PostgreSQL supplies
`OrmAiOpenAiBackgroundSubmissionService`. It crosses the OpenAI Responses
create boundary once, records only content-free binding facts, and parks the
run without a worker lease. This is submission only: it does not retrieve a
response, match a webhook receipt, settle usage, persist output, or complete a
run.

OpenAI documents `background: true`, status polling, temporary retention, and
the interaction with `store` in its
[background mode guide](https://developers.openai.com/api/docs/guides/background)
and [data controls guide](https://developers.openai.com/api/docs/guides/your-data#default-usage-policies-by-endpoint).
The host must review those provider-side retention terms independently. The
crate requires an exact `provider_response` retention authorization even when
the provider profile keeps `store: false`.

## Accepted plan

Call `submit` only with an active `Running` lease and an
`AiProviderCallPlan` that binds all of the following:

- native `ProviderKind::OpenAi` and a registered adapter that declares
  background capability;
- one initial `ProviderRetained` request with no prior response continuation;
- no application tools, provider built-ins, or attachment inputs;
- one nonzero provider-enforced maximum-output-token ceiling;
- exactly one `ModelInference` egress manifest for the same scope, session,
  run, logical provider profile, model, and request estimate;
- retention exactly `AI_EGRESS_RETENTION_PROVIDER_RESPONSE`; and
- an atomic budget reservation for the same run, attempt, generation,
  provider, model, and output ceiling.

The service uses the plan's existing current-principal, scope/session access,
egress, and budget contracts. Registration and provider capability remain
eligibility only; they do not bypass any proof.

```rust,no_run
# use graphql_orm_ai::prelude::*;
# use std::sync::Arc;
# async fn submit(
#     run_service: OrmAiRunService,
#     runtime: Arc<AiRuntime>,
#     budgets: Arc<dyn AiBudgetService>,
#     egress_audit: Arc<dyn AiEgressDecisionAudit>,
#     clock: Arc<dyn agql_auth::Clock>,
#     lease: AiRunLease,
#     plan: AiProviderCallPlan,
# ) -> Result<AiOpenAiBackgroundSubmission, AiError> {
let service = OrmAiOpenAiBackgroundSubmissionService::new(
    run_service,
    runtime,
    budgets,
    egress_audit,
    clock,
);
let accepted = service.submit(&lease, plan).await?;
# Ok(accepted)
# }
```

## Durable ordering

Before external I/O, the service freshly rehydrates current authority. One
state-machine transaction then validates the live fence, active session scope,
exact reserved budget row, and exact durable egress allow event. It inserts a
deterministic submission record bound to the run, attempt, generation, provider
profile/model, provider-neutral request hash, budget reservation, and egress
manifest. It also records the exact requested output ceiling; the provider
storage choice remains absent until a valid acknowledgement. The transaction
then renews the lease. Request content is never placed in that record. Failures
known to precede preparation or transport release unused reserved capacity.

The service marks the reservation `uncertain` immediately before transport and
periodically heartbeats the exact fence while awaiting the create
acknowledgement. The native adapter sends one non-streaming background request,
preserving the profile's configured `store` choice and adding two opaque
metadata values: the deterministic submission UUID and its full collision-
check key. It accepts at most 1 MiB of JSON and requires an exact response
object, background flag, `resp_` identifier, reviewed status, positive creation
timestamp, exact model, output ceiling, configured `store` value, and echoed
metadata.

A valid acknowledgement atomically binds the provider response and changes the
run to `WaitingProvider`, including the acknowledged provider storage choice.
The attempt and fencing generation remain as historical binding facts, but
owner, expiry, and heartbeat are cleared. The budget stays uncertain for the
future terminal reconciler.

The create request is never retried. A transport error may mean the provider
accepted the request, so the service atomically marks the prepared submission
and run `RecoveryRequired` with a safe redacted code, appends the immutable
attempt outcome, and clears the lease. A malformed acknowledgement or failure
to bind it is treated the same way. The returned
`AiOpenAiBackgroundSubmission` and provider binding types redact submission,
run, profile, response, and budget identities from `Debug`.

## Terminal reconciliation design

Status: design contract only. The current crate still leaves accepted
submissions in `WaitingProvider`; the types, persistent claim fields, retrieval
adapter, and terminal transaction described below are not implemented yet.

OpenAI's background guide says to poll the exact Responses GET endpoint while
the response is `queued` or `in_progress`; leaving those states is terminal.
OpenAI's webhook guide shows a terminal event as a reason to retrieve that exact
response. The reconciler therefore treats a verified webhook as an optional
wake-up hint, never as response content or run authority. Polling must also make
progress when a webhook is delayed, duplicated, absent, or disabled.

### Durable lifecycle

The submission row is the reconciliation unit. A receipt is never independently
claimable. Implementation must extend the existing submission row with the
minimum claim facts: reconciliation owner, monotonically increasing
reconciliation generation, lease expiry, next-attempt time, retry count,
fixed reconciliation deadline, reconciled time, current retrieval-egress
decision, and terminal assistant message/checkpoint reference. The deadline is
captured from the acknowledged storage choice and the deployment's reviewed
provider-retention bound; a later configuration change cannot extend it. The
existing CAS row version remains mandatory. The AI schema module version must
advance when those persistent semantics land.

| Submission state | Meaning | Allowed next state |
| --- | --- | --- |
| `prepared` | Create outcome was not durably accepted. It is not pollable. | `recovery_required` only |
| `waiting_provider` | Exact response binding is parked and eligible at its bounded next-attempt time. | `reconciling` |
| `reconciling` | One unexpired owner/generation may retrieve and commit. | `waiting_provider`, `completed`, `failed`, `cancelled`, or `recovery_required` |
| `completed` | Usage, protected assistant output, immutable attempt outcome, and run completion committed together. | none |
| `failed` | Exact terminal provider failure/incompletion and authoritative usage committed without assistant output. | none |
| `cancelled` | Exact provider cancellation and authoritative usage committed without assistant output. | none |
| `recovery_required` | A provider effect or usage/output fact cannot be proved safely. | privileged recovery only |

`queued` and `in_progress` provider observations release the reconciliation
claim back to `waiting_provider` with bounded exponential backoff. A provider
`completed` response maps to local `Completed` only after the successful
terminal transaction. Exact `failed` or `incomplete` responses map to local
`Failed`, and exact `cancelled` maps to local `Cancelled`, only when complete
authoritative usage is present. Missing or malformed usage after the create
boundary never releases capacity and instead maps to `RecoveryRequired`.
Provider output from failed, incomplete, or cancelled responses is never
persisted as assistant output.

### Claim and exact matching

Each pass scans at most the deployment batch limit, ordered by next-attempt
time, submission creation time, and ID. Candidates are `waiting_provider`, or
`reconciling` rows whose lease expired. A state-machine transaction reloads the
candidate and atomically:

1. validates the deterministic submission ID and full collision key;
2. validates the exact session, run, original attempt and lease generation,
   native OpenAI family, logical profile, model, request hash, output ceiling,
   acknowledged storage choice, response ID and creation time;
3. validates that the run is still lease-free `WaitingProvider` with that same
   attempt/generation and no immutable attempt outcome;
4. validates the active session ownership/scope, the exact uncertain budget
   reservation, and the original durable model-inference allow event;
5. optionally links one signature-verified pending receipt only when profile and
   response ID match exactly; and
6. CAS-increments the reconciliation generation and installs an owner and
   expiry.

A receipt with no response ID, an unknown response ID, or no exact profile match
cannot select a run. It remains content-free receipt work and is closed under
the bounded unmatched-receipt policy described in
[the webhook guide](openai-webhooks.md). Multiple workers may scan the same
candidate, but only one CAS claim may retrieve it. An expired claim can be
reclaimed with a higher generation; every heartbeat, retry release, receipt
update, and terminal write checks the owner, generation, unexpired deadline, and
row version.

Before retrieval, the service freshly rehydrates the run's
`PrincipalReference`, proves current owner/scope/session write access, resolves
the current content-protection policy, and re-authorizes the exact original
profile, fixed destination, model, source classifications, and
`provider_response` retention manifest. The new allow/deny event is audited
before transport and the allow ID is bound to the claim. Registration,
background capability, the original allow, and a verified receipt are only
eligibility facts; none substitutes for this current decision.

### Fixed-destination retrieval

The provider seam must accept an opaque crate-authored retrieval binding, not a
URL or arbitrary response ID. The native OpenAI adapter may issue only:

```text
GET <the registered profile's fixed Responses endpoint>/<the bound resp_ ID>
```

It must not list responses, follow redirects, select another profile, accept an
absolute destination from stored data, or send the original prompt again. It
resolves the profile credential just in time for each attempt, uses a timeout
strictly shorter than the reconciliation lease, and bounds the complete JSON
body by both a compiled hard maximum and deployment output limits. Retrieval is
read-only, so timeouts, rate limits, server errors, and an early not-found may
be retried with bounded backoff while the reviewed provider-retention deadline
remains open. The create request is never repeated.

Every observation must revalidate `object`, exact response ID, positive original
creation time, `background: true`, exact model, exact maximum output tokens,
exact `store`, and both opaque metadata bindings. Only the reviewed statuses
`queued`, `in_progress`, `completed`, `failed`, `incomplete`, and `cancelled`
are accepted. Unknown top-level fields may be ignored only after the whole body
is size-bounded; a wrong type or value in a security-relevant field is fatal.

For `completed`, the terminal normalizer accepts only bounded message output
with reviewed `output_text`, refusal, citation, and visible reasoning-summary
shapes. Because background submission currently forbids application tools,
provider built-ins, and attachments, any function call, hosted-tool result, or
unreviewed output item fails closed. Text, structured values, annotations,
item counts, per-item bytes, total bytes, and output tokens remain bounded by
the existing provider-output limits and the submitted output-token ceiling.
Usage requires nonnegative input/output totals, cached input not exceeding
input, output not exceeding the submitted ceiling, and every billable dimension
needed by the pinned pricing policy. Unknown output item types, unknown terminal
status, inconsistent metadata, malformed usage, or truncated JSON becomes
`RecoveryRequired`, never forward-compatible assistant content.

If a linked receipt exists, its terminal event kind must agree with the
retrieved terminal status. A terminal event followed briefly by a nonterminal
GET is retried within a small bounded consistency window; persistent
disagreement becomes `RecoveryRequired`. The retrieved response is always the
source of output and usage truth.

### Terminal transaction

Pricing settlement and content protection may execute outside the database
transaction, but they produce only in-memory prepared values bound to the
submission, response, budget, principal, scope, and reconciliation fence.
Immediately before mutation, the service rehydrates the current principal again
and repeats current session/scope access and protection-policy checks. If that
proof changed or expired, retrieved content is discarded and never persisted.
A redacted fail-closed recovery operation may close the claim, but it cannot act
on behalf of the stale principal or store provider output.

One generated-ORM `StateMachine` transaction is the only successful terminal
commit boundary. It reloads and revalidates the complete claim, run, session,
submission, optional receipt, original and current egress audits, uncertain
budget reservation, pricing result, and absence of prior terminal facts. It
then atomically:

- commits the exact budget reservation, policy-window aggregates, and
  append-only usage entry once;
- for `completed` only, inserts the protected assistant message and blocks,
  final-output checkpoint, protected session/inbox events, and queued wakeup;
- records the submission terminal status, provider status, current retrieval
  allow, terminal message/checkpoint reference, redacted code, and timestamp;
- links and completes the selected receipt, while leaving duplicate later
  receipts eligible for an idempotent terminal-match close;
- transitions the run to `Completed`, `Failed`, or `Cancelled`, clears all claim
  fields, and appends the one immutable original-attempt outcome; and
- appends one redacted reconciliation audit.

The terminal IDs and hashes are deterministically derived from the submission
and purpose. An exact replay that observes the complete terminal graph returns
`AlreadyReconciled`; a partial or conflicting graph is
`RecoveryRequired`. There is no state in which assistant output is visible while
usage is uncertain, or usage is committed while the run/submission outcome is
absent. `ReleaseUnused` is forbidden after the background create boundary.

### Failure and crash policy

| Condition | Durable result |
| --- | --- |
| Current principal, session access, egress, retention, or protection policy denied before GET | No retrieval; redacted `recovery_required`, budget remains uncertain |
| GET timeout, 429, 5xx, or bounded early not-found | Release to `waiting_provider` with backoff until retry/retention cutoff |
| `queued` or `in_progress` | Release to `waiting_provider`; no output or budget mutation |
| Exact `completed` plus valid usage/output | One atomic `completed` commit |
| Exact `failed`/`incomplete` plus valid usage | One atomic `failed` commit; no assistant output |
| Exact `cancelled` plus valid usage | One atomic `cancelled` commit; no assistant output |
| Missing usage, expired retention, malformed/oversized response, mismatched binding, unsupported output, or exhausted ambiguous retrieval | `recovery_required`; never release uncertain capacity |
| Stale owner/generation, expired claim, or CAS race | Stale worker writes nothing |

A crash before GET only leaves an expiring claim. A crash during or after GET
leaves no provider content in the database; the exact read may be repeated by a
higher-generation claim. A crash during the terminal transaction rolls the
whole transaction back. If the client cannot tell whether commit succeeded, it
reloads the deterministic terminal graph: complete facts are success, the
original `reconciling` claim is safely reclaimable, and any partial/conflicting
facts close readiness. A crash after commit is an idempotent terminal replay.

### Restore and test gate

Restore collection must validate every new claim and terminal field in addition
to the existing deterministic submission facts. A valid unexpired
`reconciling` claim is made reclaimable without provider create replay; valid
`waiting_provider` work remains closed until the live reconciler starts. A valid
terminal row must have the exact run outcome, attempt outcome, budget/usage,
current retrieval-egress audit, and, for success, protected output checkpoint.
Invalid or partial graphs increment
`invalid_provider_background_submission_count` and keep readiness closed.
`RecoveryRequired` remains operator-visible but never automatically retrieves,
releases budget, or changes terminal classification.

Focused tests must cover receipt-present and polling-only success, delayed and
duplicate receipts, concurrent and expired claims, terminal idempotency,
transport retries, every reviewed provider status, every binding mismatch,
revoked/expired authority before GET and before commit, egress/retention/policy
changes, usage and output bounds, terminal-transaction rollback and ambiguous
commit reload, restore validation, and SQLite/disposable-PostgreSQL parity.
