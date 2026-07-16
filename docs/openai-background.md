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

## Deliberately closed next boundary

`WaitingProvider` has no ordinary worker transition. Webhook receipt intake is
separate and grants no matching authority. Until the reconciler exists, an
accepted submission remains parked and a restored one plans manual recovery.

A future reconciler must independently validate the deterministic submission,
matching verified receipt and provider response, original run/attempt/fence,
current principal and session authority, exact profile/model/output ceiling/
storage choice, uncertain budget, egress and retention proofs, bounded terminal
output, and usage before any run mutation. Restore fact collectors must report
invalid submission bindings
through `invalid_provider_background_submission_count`; any nonzero value keeps
runtime readiness closed.
