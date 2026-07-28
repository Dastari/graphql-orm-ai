# Verified OpenAI Webhook Intake

The `provider-openai` feature includes a bounded verifier for the exact raw
request format documented by OpenAI and, on SQLite/PostgreSQL, an ORM service
that durably records a content-free receipt. This is an intake boundary only.
It does not authorize background submission, retrieve a response, resume a
run, settle usage, or grant egress or budget authority. Exact submission is a
separate fenced service described in the
[background submission guide](openai-background.md).

OpenAI documents the current delivery headers, raw-body verification rule,
duplicate delivery behavior, bounded retry window, and background-worker
recommendation in its
[webhook guide](https://developers.openai.com/api/docs/guides/webhooks).
The reviewed event set in this crate is limited to `response.completed`,
`response.failed`, `response.incomplete`, and `response.cancelled`.

## Host route

The host owns the HTTPS route, request authentication/rate limiting, header
extraction, and HTTP acknowledgement. Preserve the exact body bytes and exact
`webhook-id`, `webhook-timestamp`, and `webhook-signature` header values. Do not
let a framework parse and reserialize JSON before verification.

```rust,no_run
# use graphql_orm_ai::prelude::*;
# async fn accept(
#     raw_body: &[u8],
#     webhook_id: String,
#     webhook_timestamp: String,
#     webhook_signature: String,
#     verifier: &OpenAiWebhookVerifier,
#     receipts: &OrmAiProviderWebhookReceiptService,
# ) -> Result<(), AiError> {
let headers = OpenAiWebhookHeaders::new(
    webhook_id,
    webhook_timestamp,
    webhook_signature,
)?;
let event = verifier.verify(&headers, raw_body).await?;
match receipts.record(&event).await? {
    AiProviderWebhookReceiptOutcome::Recorded
    | AiProviderWebhookReceiptOutcome::AlreadyRecorded => {}
}
# Ok(())
# }
```

Construct one `OpenAiWebhookVerifier` for one exact logical provider profile.
Give it a `SecretRef` for that profile's webhook signing secret, an
`AiSecretStore`, and the trusted `agql-auth` clock. The signing secret is
resolved just in time for every delivery and is distinct from provider request
authority. `OpenAiWebhookVerifierLimits` may narrow the default 64 KiB body and
five-minute replay window, but cannot exceed the compiled hard bounds.

Only acknowledge success after `record` commits. Exact re-deliveries are
idempotent even when their delivery timestamp is later. Reuse of the same
profile/event identity with changed signed event kind, provider creation time,
or response ID fails closed. A validly signed event outside the reviewed set is
recorded once as ignored so repeated delivery does not create work.

## Persisted boundary

The receipt contains only the provider family, exact logical profile, event
identity/kind/times, optional response ID, signature-verified fact, processing
state, and redacted error code. The raw body, signature, signing secret,
credential, prompt, output, and provider error are never stored. Receipt and
redacted audit insertion are one generated-ORM transaction; no application SQL
or generic CRUD root is involved.

A verified event proves only possession of the configured signing secret for
the exact delivered bytes within the replay window. It does not prove that the
response belongs to a durable run. Supported events therefore remain
`pending_reconciliation`.

## Role in terminal reconciliation

Status: implemented for exact matching and terminal closure. Receipt intake
remains an authority-free boundary.

The background submission, not the receipt, is the unit of work and the only
claimable row. Polling must complete an exact response even when no webhook
arrives. A receipt is considered only while the reconciler independently
claims an eligible submission with the exact native provider family, logical
profile, and provider response ID; receipt intake does not itself schedule or
claim work.

| Receipt state | Meaning |
| --- | --- |
| `pending_reconciliation` | Verified supported envelope; no run authority or trusted match yet |
| `matched_pending` | Exact submission/run/attempt linkage recorded under its active reconciliation claim; provider GET is still authoritative |
| `processed` | Receipt kind agreed with an atomically committed terminal submission |
| `duplicate_terminal` | Later exact event for an already-terminal submission agreed with the durable terminal graph |
| `unmatched` | No exact submission appeared before the bounded matching deadline |
| `recovery_required` | Missing response ID, conflicting profile/event facts, status disagreement, or malformed durable linkage requires review |
| `ignored` | Validly signed event kind is outside the reviewed set |

Linking is one state-machine transaction with the submission claim and uses the
schema `0.50.0` composite provider/profile/response/state index. It fills
`run_id` and `attempt_id` only after the deterministic submission, run, original
fence, response, profile, budget, egress, and session facts all agree. A receipt
with a missing response ID or an unknown/profile-mismatched response never
selects the nearest run and never causes a provider GET. Pending unknown or
profile-mismatched receipts remain inert; bounded age-based transition to
`unmatched` is reserved for a separate maintenance policy.

The event kind is a hint about the expected terminal state:

- `response.completed` expects retrieved `completed`;
- `response.failed` expects retrieved `failed`;
- `response.incomplete` expects retrieved `incomplete`; and
- `response.cancelled` expects retrieved `cancelled`.

The reconciler always retrieves the exact response through the fixed-destination
binding described in the
[terminal reconciliation design](openai-background.md#terminal-reconciliation-design).
It never treats event body data as output or usage. A brief terminal-event/
nonterminal-GET race is retried within a bounded consistency window. A
persistent mismatch, a different terminal state, or changed immutable receipt
facts becomes `recovery_required`.

OpenAI may retry failed webhook deliveries and may deliver duplicates. The
existing deterministic receipt insert handles the same event idempotently.
Different event IDs for the same terminal response are closed against the one
durable terminal graph: agreement becomes `duplicate_terminal`; disagreement
becomes `recovery_required`. Redelivery never repeats response creation,
settles usage again, or appends another assistant message/outcome.

The successful terminal transaction atomically marks the selected receipt
`processed` with its run/attempt and `processed_at` alongside submission, run,
attempt-outcome, budget/usage, audit, and optional protected-output changes. A
crash before that commit leaves the receipt pending or matched and the
submission claim reclaimable. A crash after commit observes the deterministic
terminal graph and closes idempotently.

Restore adapters must validate the deterministic receipt identity, exact
provider/profile/event/response facts, verified-signature flag, legal state,
optional exact submission/run/attempt link, processed time, and creation/
reconciliation audits. A `processed` or `duplicate_terminal` receipt must agree
with the complete terminal submission graph. Report any failure through
`invalid_provider_webhook_receipt_count`; a nonzero count keeps restored runtime
readiness closed. Valid pending and matched receipts remain inert until the live
submission reconciler owns a valid claim.
