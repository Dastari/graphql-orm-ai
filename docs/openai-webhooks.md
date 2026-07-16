# Verified OpenAI Webhook Intake

The `provider-openai` feature includes a bounded verifier for the exact raw
request format documented by OpenAI and, on SQLite/PostgreSQL, an ORM service
that durably records a content-free receipt. This is an intake boundary only.
It does not authorize background submission, retrieve a response, resume a
run, settle usage, or grant egress or budget authority. Exact submission is a
separate fenced service described in the
[background submission guide](openai-background.md).

OpenAI documents the current delivery headers, raw-body verification rule, and
event handling expectations in its [webhook guide](https://platform.openai.com/docs/guides/webhooks).
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
`pending_reconciliation`. A future worker must independently match an exact
durable background submission and re-prove the original run, attempt, fence,
provider/profile/response, budget, egress, retention, and current-authority
bindings before provider retrieval or any run mutation. Until that worker
exists, intake creates no executable work.

Restore adapters must validate the deterministic receipt identity, exact
provider/profile/event/response facts, verified-signature flag, current intake
state, and creation-audit linkage. Report any failure through
`invalid_provider_webhook_receipt_count`; a nonzero count keeps restored runtime
readiness closed. A valid pending receipt stays inert and does not itself grant
work after restore.
