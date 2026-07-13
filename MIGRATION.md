# Migration Guide

`graphql-orm-ai` is not yet published. This guide is still mandatory so early
Git consumers and disposable test deployments can track schema and API changes
without guessing.

## Unreleased: native Ollama adapter (crate 0.13.0 to 0.14.0)

Enabling `provider-ollama` now compiles the native HTTP adapter and its optional
Base64/Reqwest dependencies. Construct `OllamaProvider` with
`OllamaProviderConfig` and a deployment-owned `AiProviderEndpointPolicy`. The
configured value must be a root `http` or `https` origin without URL
credentials, query, fragment, or path. Redirects are disabled. Endpoint policy
is still responsible for exact host/port allowlisting, DNS rebinding defenses,
and network-zone isolation; the configuration value is not an SSRF proof.

The adapter supports bounded native `/api/chat` NDJSON text streaming,
ephemeral inline PNG/JPEG/WebP inputs, JSON-schema structured output, and
reported prompt/evaluation token usage. Every call still requires a matching
model-inference egress proof and atomic budget proof. Each image additionally
requires its exact image-analysis transfer and freshly reopened attachment
bytes. A local destination does not imply disclosure authorization.

Custom tools, provider built-ins, non-image files, provider-response
continuation, and model thinking output are not supported by this adapter.
They fail closed rather than silently losing conversation state or persisting
hidden reasoning. Native Ollama tool calling remains gated until the runtime
can durably checkpoint and reconstruct a provider-independent stateless
conversation. No API key is required by this adapter; if a deployment places
authentication in front of Ollama, it must use a separately reviewed fixed
transport boundary rather than URL credentials.

This is an additive pre-1.0 public Rust API, feature, dependency, and provider
behavior change. Default features and GraphQL SDL do not change. It adds no
entity, field, index, constraint, persistent semantic, or backup/restore
change. `AI_SCHEMA_MODULE_VERSION` remains `0.16.0`; no AI-owned or
application-domain data migration is needed.

## Unreleased: schema module 0.15.0 to 0.16.0 and principal inbox (crate 0.12.0 to 0.13.0)

Apply AI schema module `0.16.0` while session writes, provider-output commits,
subscriptions, pruning workers, and restore callbacks are closed. Do not start
0.13.0 code against the 0.15.0 module: session creation, message queueing,
archive/restore/delete, and final assistant-output persistence now append a
principal-inbox event in the same state-machine transaction.

The managed schema changes are:

- add private `graphql_orm_ai_inbox_streams`, with deterministic ID, exact
  principal kind/subject, never-rewound `stream_head`,
  `minimum_retained_sequence`, last-event time, row-version fence, and a unique
  principal kind/subject index;
- add a unique principal kind/subject/sequence constraint to
  `graphql_orm_ai_inbox_events`;
- add required captured `scope_key`, `scope_kind`, `scope_id`, and optional
  `tenant_id` to new inbox events; and
- add nullable `scope_key`, `inbox_event_retention_seconds`, and
  `inbox_minimum_events` to retention policies, plus a unique index on
  non-null scope keys.

The inbox-event entity existed as reserved private schema, but 0.12.0 exposed
no writer, query, or subscription for it. A normal deployment should therefore
find it empty. If an early consumer wrote private rows anyway, do not infer
authority or silently assign scopes. A dependency-owned migration must either
prove each row's exact owner/session/scope, backfill captured scope fields,
validate unique contiguous per-principal sequences, and construct the matching
stream head, or remove the unsupported rows while the runtime is closed. No
client cursor existed in the public 0.12.0 GraphQL contract.

Legacy retention rows remain stored but are not effective for inbox pruning
until all three nullable migration fields are populated and valid. Supported
write-backend migration diagnostics may use `ai_scope_key` to reproduce the
stable non-secret scope identity; that value is not authorization. Prefer the
new recent-MFA-protected `setAiRetentionPolicy` mutation to create the current
scope policy. Resolve duplicate logical legacy policies explicitly before
adding/currently relying on the unique keyed policy. Never invent a retention
period or treat absence as permission to delete.

Host `AiConfigurationService` implementations must add `retention_policy` and
`set_retention_policy`. Host `AiConfigurationAccessPolicy` implementations
must handle `ReadRetention` and `ManageRetention`. Compose the corresponding
configuration query/mutation fields if GraphQL management is enabled. The
mutation is CAS-bound, requires current recent MFA in the ORM service, and
audits in the same transaction.

Hosts composing `AiQueryRoot`/`AiSubscriptionRoot` gain
`aiInboxEventPage`/`aiInboxEvents` (or coherent PascalCase names). Install an
explicit `Arc<dyn AiInboxService>`; missing registration fails closed. Schedule
`OrmAiInboxPruningService` only as a trusted host worker after all required
scope policies are current. It deletes only a bounded expired prefix, keeps the
configured recent-event floor, and atomically advances the retained cursor.
Do not expose pruning as an ordinary user mutation or manually renumber rows.

This is a pre-1.0 Rust API, GraphQL SDL, persistence, index/constraint,
authorization, backup/restore, and behavioral contract change. Cargo features
and defaults do not change. Backups must include the new stream entity and
captured inbox scope fields. Restore reconciliation must validate stream
bounds and retained-prefix continuity before reopening. No application-domain
table or data migration is required.

## Unreleased: exact provider attachment reopening (crate 0.11.0 to 0.12.0)

Provider turns containing `ModelInputBlock::Attachment` now require exact
freshly reopened bytes in addition to the attachment egress proof introduced
in 0.10.0. Configure `AiProviderCallExecutor::with_attachment_resolver` with a
trusted `AiProviderAttachmentResolver` and validated
`AiProviderAttachmentResolutionLimits`. SQLite/PostgreSQL hosts can use the
same `OrmAiAttachmentService` that owns intake. A missing resolver fails before
provider transport; do not replace it with a signed URL, raw storage key, or
model-selected object lookup.

The new public `AiProviderAttachmentRequest` and
`AiResolvedProviderAttachment` values bind opaque ID, scanner-detected MIME,
raw byte count, lowercase SHA-256, sanitized filename, and content. The
resolved type validates length/hash but is not authorization proof. Resolver
implementations must use the supplied fresh `ResolvedPrincipal`, recheck the
current session/scope/owner and released/clean/message-linked state, read only
the exact durable object, and fail if either object facts or the row changes.
`ProviderRequestContext::with_resolved_attachments` requires one-to-one exact
coverage; provider adapters retrieve content with `resolved_attachment`.

`ModelRequest::validate` now rejects duplicate attachment IDs. Its estimated
payload includes conservative Base64 expansion, so existing inference and
image/file manifests may need larger `estimated_bytes` values. The exact
capability manifest is still separate and must carry the canonical attachment
source. Deployment limits may only narrow the model/request hard limits.

With `provider-openai`, supported PNG/JPEG/WEBP/GIF inputs are sent as
ephemeral Responses `input_image` data URLs. Host scanning/acceptance must
reject animated GIFs because OpenAI accepts only non-animated GIF input. Other
host-accepted files are sent as inline `input_file` data under the adapter's
less-than-50-MiB
per-file and 50-MiB combined raw-file bounds. This path creates no provider
file ID and therefore no provider-file deletion lifecycle. Hosts remain
responsible for MIME acceptance and must separately authorize `ImageAnalysis`
or `ProviderFile`; provider rejection remains a normal uncertain transport
failure. The feature now enables an optional Base64 dependency; default
features are unchanged.

This pre-1.0 release adds public Rust APIs and changes provider behavior and
payload estimates. It adds no GraphQL SDL, entity, field, index, constraint,
backup/restore, or persistent semantic change. `AI_SCHEMA_MODULE_VERSION`
remains `0.15.0`; no AI-owned or application-domain data migration is needed.

## Unreleased: schema module 0.14.0 to 0.15.0 and attachment cleanup (crate 0.10.0 to 0.11.0)

Apply AI schema module `0.15.0` before starting the new worker. The existing
`graphql_orm_ai_attachments` table gains nullable `processing_expires_at`,
`cleanup_generation`, `cleanup_lease_expires_at`, `cleanup_retry_count`, and
`cleanup_next_attempt_at` columns. Lifecycle state fields become private
maintenance filters. No entity is added or removed; blob references retain
their backup-redaction contract.

Hosts should schedule `AiAttachmentCleanupService::cleanup_once` through a
trusted singleton or distributed worker scheduler. The service itself permits
safe concurrency: every row gets a monotonic generation, expiring CAS claim,
and redacted audit outcome. Do not expose it through GraphQL, delete storage
prefixes, or clear blob references manually. Configure upload-processing time
longer than maximum upload plus full-object scanner latency. Storage ambiguity
enters capped retry backoff rather than being reported as deletion.

`AiAttachmentServiceLimits::new` remains source-compatible and defaults the new
processing lifetime to one hour; `with_upload_processing_ttl` can narrow or
widen it within the documented hard bound. `OrmAiAttachmentService` adds
`with_cleanup_limits`. Public `AiAttachmentCleanupLimits`,
`AiAttachmentCleanupReport`, and `AiAttachmentCleanupService` are new additive
Rust APIs. GraphQL SDL, Cargo features/defaults, and application authorization
contracts do not change.

Existing pending tickets need no rewrite. Legacy interrupted `uploading` rows
with no processing deadline fall back to their ticket expiry; legacy
`deleting` rows with no deadline are reclaimable. After restore, keep the
runtime start gate closed until the module migration and normal restore
reconciliation have completed, then run cleanup. This is an AI-owned metadata
migration only; there is no application-domain data migration.

## Unreleased: exact attachment egress binding (crate 0.9.0 to 0.10.0)

Every `ModelInputBlock::Attachment` constructor must now supply the exact
verified `byte_count` and lowercase `sha256` from the released attachment. Its
separate `ImageAnalysis` or `ProviderFile` egress manifest must include a
source with `kind: "attachment"`, `trust: UserProvided`, and canonical
`reference` returned by `ModelInputBlock::attachment_egress_reference`. The
versioned value binds ID, byte count, detected MIME, and SHA-256. The manifest
byte/count limits must cover the full request including attachment bytes.
Changed content or metadata requires a new manifest decision and audit; never
copy a proof between attachments.

This is a pre-1.0 breaking Rust API and provider behavior change. It adds no
GraphQL SDL, Cargo feature/default, entity, field, index, constraint,
backup/restore, or data semantic change. `AI_SCHEMA_MODULE_VERSION` remains
`0.14.0`; no AI or application-domain data migration is required.

## Unreleased: schema module 0.13.0 to 0.14.0 and attachment intake (crate 0.8.0 to 0.9.0)

This pre-1.0 release adds the owner-isolated attachment service and composable
attachment GraphQL roots. Add the exact pinned `graphql-orm-storage` 0.5.0
dependency universe from this manifest. Construct `OrmAiAttachmentService`
with the ordinary session/scope access policy, content-protection boundaries,
a provider-neutral `BlobStore`, a complete-object `AiAttachmentScanner`, a
separate fail-closed `AiAttachmentAcceptancePolicy`, and a trusted clock.

The GraphQL `createAiAttachmentUpload` mutation returns a one-time token. A
host-owned authenticated streaming endpoint passes that token as
`SecretString` plus `StorageByteStream` to `AiAttachmentUploadService::upload`;
do not put it in a URL, log, database, or GraphQL file body. The current owner
must still authenticate. After clean scanning and policy acceptance, call
`finalizeAiAttachmentUpload`; only released/clean attachment IDs can enter the
ordinary `sendAiMessage` mutation. Existing applications must compose the new
roots explicitly; no fields are silently added.

Apply `AiSchemaModule` `0.14.0` while provider starts, uploads, subscriptions,
and restore callbacks are closed. `graphql_orm_ai_attachments` changes as
follows:

- `blob_reference`, `detected_mime`, `byte_count`, and `sha256` become nullable
  so a durable pending ticket never invents final-object facts;
- nullable `quarantine_blob_reference` keeps cleanup work addressable without
  exposing or overloading the final object reference;
- nullable `expected_byte_count`, `upload_token_hash`, and
  `upload_expires_at` bind new uploads without making legacy finalized rows
  invalid; and
- nullable scanner version, acceptance-policy version, and redacted rejection
  code record lifecycle evidence.

For existing finalized rows, no content rewrite is required. Optionally
backfill `expected_byte_count` from `byte_count`; leave upload token/expiry null.
Do not fabricate a token or move a legacy final object into quarantine. Any
legacy row whose object, checksum, owner, session, clean scan, or release state
cannot be verified must remain unavailable and be reported by restore
reconciliation. The managed migration must change column nullability/add the
new columns and record module `0.14.0`; never relabel an applied `0.13.0`
module.

This changes public Rust APIs and GraphQL SDL, but no Cargo feature/default.
Provider adapter file/image resolution, provider-side file retention/deletion,
derivative artifacts, expired-ticket/orphan pruning, scope quota configuration,
and bulk session purge remain explicit gates. No application-domain data
migration is required.

## Unreleased: schema module 0.12.0 to 0.13.0 and protected live output (crate 0.7.0 to 0.8.0)

This pre-1.0 release adds an optional durable provisional-output boundary to
`AiProviderCallExecutor`. Existing construction is unchanged and emits no
provisional events. To enable it, construct `OrmAiLiveDeltaService` with the
same run service, runtime, trusted clock, and validated protection/freshness
limits, then pass it to `with_live_delta_sink` together with validated
coalescing limits.

The new public `AiLiveDeltaSink` receives only bounded visible text or visible
reasoning-summary batches plus an immutable private-field context. A conforming
sink must rehydrate current authority, protect content, and validate the exact
session/run/attempt/generation/provider/model/budget binding. The built-in sink
does this for every batch, rechecks policy after protection, and commits a
protected `provider_live_delta` session event before the ordinary commit-only
subscription wakeup. Sink failure occurs after provider transport and therefore
leaves usage uncertain; do not automatically replay the provider call.

Clients must treat these events as provisional progress. The authoritative
`assistant_message_completed` event and windowed message blocks remain the
final transcript. A provisional event from an attempt later classified
`RecoveryRequired` remains partial history and must not be presented as a
completed assistant answer. Event payload format version 1 binds the run,
attempt, generation, provider/model/optional response, budget reservation,
batch sequence, visible kind, text, and byte count.

Advance `AiSchemaModule` to `0.13.0` through the managed `graphql-orm` schema
manager while provider starts and subscriptions remain closed. No entity,
field, index, constraint, public GraphQL root, client SDL, Cargo feature, or
default changes. No existing AI row or application-domain data rewrite is
required. The managed migration may be structurally empty, but the module bump
is mandatory because `graphql_orm_ai_session_events` gains the persistent
semantic contract for `provider_live_delta`; never relabel an applied `0.12.0`
module. Restore reconciliation must validate module `0.13.0` before reopening.

This slice does not add delta retention/purge. Existing retention policy fields
remain configuration only until the bounded pruning worker lands; do not delete
event rows manually or break session cursor monotonicity.

## Unreleased: schema module 0.11.0 to 0.12.0 and exact tool-batch adoption (crate 0.6.0 to 0.7.0)

This pre-1.0 release makes the read-only coordinator constructor deliberately
breaking: `AiReadOnlyAgentCoordinator::new` now requires an
`AiAgentCheckpointAdopter` in addition to `AiAgentCheckpointWriter`. Use the
same `OrmAiCoordinatorCheckpointService` value for both boundaries unless a
conforming wrapper preserves its current-principal, protection, durable-record,
and one-shot consumption checks.

Expired `Running` attempts are now requeued only when their linked checkpoint
is an exact `tool_batch_persisted` record with a valid protected-envelope hash,
committed/reconciled provider budget, and complete fenced tool/step rows. The
replacement claim retains that one checkpoint ID. Before planning or transport,
the adopter:

- rehydrates the current principal and rechecks session/scope access;
- reopens the protected checkpoint, tool arguments, and tool results under an
  unchanged ready protection policy;
- validates the original attempt/generation, provider response, budget,
  ordered call IDs, descriptor fingerprints, canonical arguments, disclosure
  outputs, egress manifests and immutable allow audits;
- reconstructs the loop counters and exact opaque continuation under the new
  fence; and
- atomically clears the linked checkpoint before the next provider call.

If a worker dies before consumption, the exact checkpoint can be considered by
bounded recovery again. If it dies after consumption or while the next provider
call may have started, ordinary conservative recovery applies. Provider-turn
checkpoints, partial tool batches, supervised mutations, exhausted adoption
retries, and malformed or missing records are never adopted automatically.
`AiRunRecoveryReport` adds the public `checkpoint_requeued` counter; update
exhaustive struct construction and report handling.

Advance `AiSchemaModule` to `0.12.0` through the managed `graphql-orm` schema
manager while workers and provider starts remain closed. There is no physical
entity, field, index, or constraint change, but the module version must record
the new persistent meaning of `latest_checkpoint_id`, the
`checkpoint_adoption_ready` retry marker, and one-shot checkpoint consumption.
The managed migration may therefore be structurally empty; do not skip its
module-version/readiness record or relabel an applied `0.11.0` module.

This adds no GraphQL root/SDL or Cargo feature/default. Existing completed
history needs no rewrite, and existing active provider-turn/partial/malformed
checkpoints remain closed. No AI row rewrite or application-domain data
migration is required. Restore reconciliation must validate module `0.12.0`
before reopening workers.

## Unreleased: schema module 0.10.0 to 0.11.0

Apply `AiSchemaModule` through the managed `graphql-orm` schema manager with
workers, provider starts, subscriptions, and restore callbacks closed. This
revision adds nullable private `protected_state` to append-only
`graphql_orm_ai_run_checkpoints`. It stores exact protected normalized provider
turns and completed model-visible read-only tool batches. Final assistant-output
checkpoints continue to prove their content through message/block rows and keep
this field null.

The coordinator now requires an `AiAgentCheckpointWriter`. Install
`OrmAiCoordinatorCheckpointService` with the same run service, current-principal
resolver, access policy, content-protection resolver/protector, trusted clock,
and deployment byte/freshness limits. Provider turns are checkpointed only
after authoritative budget reconciliation; tool-batch checkpoints additionally
verify every protected result, egress decision/manifest hash, run step, provider
response, and fence in the same transaction. A checkpoint failure after
external execution becomes `RecoveryRequired`.

This schema revision alone does not permit cross-generation adoption. Adoption
is supplied by the later `0.7.0` runtime contract above and is restricted to
exact complete read-only tool batches. Existing active pre-`0.11.0` runs and
malformed/absent checkpoints must continue through privileged recovery; never
infer provider output or tool results, replay a provider call, or backfill
protected state manually. Historical completed runs and final-output
checkpoints need no rewrite. No application-domain data migration is required.

This adds no public GraphQL root or client-visible SDL and changes no Cargo
feature/default. The AI schema migration is nullable/additive, but the runtime
gate must remain closed until managed validation and restore reconciliation
report module `0.11.0` ready. The new public service/trait and constructor
change advance the pre-1.0 crate from `0.5.0` to `0.6.0`; update the reviewed
Git revision and package expectation together.

## Unreleased: remote authenticated GraphQL execution (0.4.0 to 0.5.0)

This pre-1.0 Rust API boundary adds the project-agnostic private remote
execution adapter and deliberately changes
`GraphqlRequestContextFactory::build` to receive `&ToolGraphqlRequest` instead
of `&GraphqlInvocationContext`. Update every factory implementation to accept
the complete request. Local factories may continue to construct the same
ordinary application context; remote factories should use the additional
operation and variable bindings rather than discard them.

For private routed or direct targets, construct
`AiRemoteAuthenticatedGraphqlAdapter` and use the same cloned adapter value as
both `GraphqlRequestContextFactory` and `AuthenticatedGraphqlExecutor`. Supply:

- an `AiRemoteGraphqlAuthorityIssuer` that mints one audience/resource/
  operation-bound, short-lived credential while preserving the human actor;
- an `AiRemoteGraphqlTransport` that maps only deployment-registered logical
  target IDs to fixed private allowlisted destinations and propagates the
  correlation/causation audit chain; and
- validated authority-lifetime and freshly resolved principal-age limits.

Do not pass or persist the user's bearer token. Do not serialize, log, retain,
or reuse `AiRemoteGraphqlAuthority`. A direct-service transport must never
grant more authority than the equivalent routed request. The issuer and
transport remain trusted deployment boundaries: the crate binds and verifies
the redacted request but cannot inspect proprietary delegated-token claims or
prove private network configuration.

This change adds no GraphQL root or client-visible SDL, changes no Cargo
feature/default, and changes no persistent entity, index, constraint, backup,
restore, or authorization-policy data. `AI_SCHEMA_MODULE_VERSION` remains
`0.10.0`; no AI or application data migration is required. Update the package
expectation and reviewed Git revision together.

## Unreleased: schema module 0.9.0 to 0.10.0

Apply `AiSchemaModule` through the managed `graphql-orm` schema manager while
provider starts, workers, subscriptions, and approval callbacks remain closed.
This revision adds nullable restart/audit bindings to
`graphql_orm_ai_tool_calls`:

- `provider_kind`, `provider_model`, and `provider_response_id`;
- `budget_reservation_id`;
- `correlation_id` and `causation_id`; and
- `delegation_reference`.

New tool calls always populate the applicable fields. Supervised execution
requires provider/model, budget, correlation, and causation bindings and proves
that the referenced budget reservation is committed, reconciled, and matches
the exact session/run/attempt/fencing generation/provider/model before
consuming approval. Historical completed rows need no rewrite. A pending or
approved consequential row created before module `0.10.0` lacks authoritative
restart bindings and must fail closed for privileged reconciliation; do not
invent values or update private tables manually.

No application-domain data migration is required. The AI schema migration is
nullable/additive, but the runtime start gate must remain closed until managed
schema validation and restore reconciliation report module `0.10.0` ready.

### Rust API and behavior changes

- Use `AiProviderCallPlan::new_with_supervised_tools` and
  `new_supervised_continuation_with_tools` only when the deployment/scope
  policy explicitly enables exact supervised descriptors. The read-only plan
  constructors remain restricted to read-only queries.
- Implement `AiCanonicalActionPreviewBuilder` with trusted current application
  state. Returned resource versions and preview content are approval authority;
  model-written prose must never be used.
- Construct `OrmAiConsequentialToolCallService`, call `request_approval`, let a
  human decide through the existing approval GraphQL lifecycle, then call
  `execute_approved` with the exact waiting lease and current result-egress
  route. Replace the lease only when the returned persisted outcome contains a
  renewed one.
- `AiRuntime::execute_tool` now rejects descriptors whose approval rule is not
  `None`. Direct callers that previously passed a one-shot descriptor must use
  the supervised lifecycle; there is no compatibility bypass.
- `AiToolPreauthorization` proves only a fresh host tool-policy decision.
  `execute_approved_tool` recomputes and compares that policy version/state
  before resolver invocation; ordinary resolver authorization remains final.
- A post-consumption resolver or handoff ambiguity returns
  `AiConsequentialToolCallOutcome::RecoveryRequired` and terminally closes the
  run. Never retry that mutation or reuse its consumed approval.

This adds no public GraphQL field or root and changes no client SDL. Approval
query/mutation roots are unchanged. The new public APIs and deliberately
stricter runtime behavior advance the pre-1.0 crate from `0.3.0` to `0.4.0`;
consumers must update the package expectation and reviewed Git revision
together.

## Unreleased: schema module 0.8.0 to 0.9.0

This public/security/persistence slice advances the pre-1.0 crate version from
`0.2.0` to `0.3.0`. Update the Git revision and package expectation together;
the intentional initial-turn constructor restriction and new recovery-report
field are a pre-1.0 breaking API/behavior boundary.

Apply `AiSchemaModule` through the managed `graphql-orm` schema manager while
provider starts, workers, subscriptions, and callbacks remain closed. This
revision adds:

- nullable `graphql_orm_ai_runs.latest_checkpoint_id`; and
- append-only `graphql_orm_ai_run_checkpoints`, bound to the exact run,
  attempt, fencing generation, provider response reference, settled budget
  reservation, final assistant message, and a stable redacted checkpoint hash.

The protected assistant message/blocks, session event, renewed run fence, and
`assistant_output_persisted` checkpoint now commit in one state-machine
transaction. If the worker dies after that transaction but before terminal run
finalization, expired-lease reconciliation verifies the complete checkpoint,
hash, attempt/generation, and finalized assistant message before committing
`Completed`. Missing, swapped, malformed, or any other active checkpoint still
fails closed or becomes `RecoveryRequired`; it is never replay authority.

The module version is `0.9.0`; never apply these semantics under an earlier
version. Existing completed/history rows require no rewrite and may keep a null
checkpoint reference. Reconcile any existing active pre-release run through
the prior owning service before reopening workers. Do not invent checkpoint
rows or update the private tables with manual SQL. No application-domain data
migration is required. Keep the runtime start gate closed until managed schema
validation and restore reconciliation report module `0.9.0` ready.

### Rust API and behavior changes

- `AiReadOnlyAgentCoordinator` now owns the bounded top-level read-only loop.
  Hosts implement `AiReadOnlyAgentTurnPlanner` and supply proof-preserving run,
  provider-turn, tool, and output services. Replace the lease after every
  fenced operation and configure a heartbeat interval comfortably shorter than
  the run-service lease TTL.
- Provider/tool/output ambiguity is durably classified as
  `AiReadOnlyAgentRunOutcome::RecoveryRequired` when the current fence can
  commit it. A lost heartbeat fence returns an error without attempting a
  terminal write.
- `AiProviderCallPlan::new_with_tools` is now for initial turns only. Code that
  previously supplied `ModelContinuation` or `ModelInputBlock::ToolResult`
  directly must retain the exact `AiAgentContinuation` and call
  `new_continuation_with_tools`.
- `AiRunRecoveryReport` has a new public `completed` counter.
- In that release, `AiLiveDeltaCoalescer` and related public types provided
  synchronous bounded batching only. A raw batch remains neither authorization
  nor a durability proof. The later crate `0.8.0` contract at the top of this
  guide supplies the optional protected ORM sink.

This adds no public GraphQL root or client-visible SDL. The new generated ORM
records remain private implementation entities.

## Unreleased: multi-repository ownership workflow

Development now assigns one owning agent and isolated branch/worktree to each
repository. The `graphql-orm-ai` agent may inspect `agql-auth` and
`graphql-orm` read-only but sends requested changes to their owners instead of
mutating sibling worktrees. Upstream crates merge first and report final commit
SHAs; this crate then repins and verifies the reviewed dependency universe.

This is a contributor workflow change only. It changes no consumer Rust API,
GraphQL SDL, feature/default, configuration, authorization behavior, schema
module, backup/restore contract, or persisted data. No consumer or data
migration is required.

## Unreleased: documentation and release enforcement

CI and the documented release gate now deny missing public Rust documentation
in addition to ordinary Rustdoc warnings. The release-policy check also
requires `README.md`, `CHANGELOG.md`, and `MIGRATION.md` to move together with
public Rust/runtime changes. Contributors must add useful Rustdoc for every new
public item, including `# Errors` sections for fallible APIs and explicit proof
boundaries for security-sensitive types. This changes no consumer Rust API,
GraphQL SDL, feature/default, runtime behavior, schema module, or persisted
data; no consumer or data migration is required.

## Unreleased: upstream dependency alignment

The public manifest now resolves one exact Git dependency universe:

- `graphql-orm` 0.7.0 at
  `1e145a124e9e3f1b0ffd70165289170b627ecb73`; and
- `agql-auth` 0.10.0 at the peeled `v0.10.0` target
  `c92dcb441237bbe308499b26525945f60ffa394a`.

Remove host patches or path overrides to older sibling versions. Hosts that
also depend directly on either crate must use these exact revisions so Cargo
resolves one source/type universe. This changes no `graphql-orm-ai` GraphQL
SDL, AI schema-module version, persisted AI data, or application authorization
policy; no AI data migration is required.

`agql-auth` 0.10.0 separately adds a nullable `authorization_policy` field to
OAuth state storage. Hosts using its OIDC authorization-state persistence must
apply the auth crate's 0.10.0 migration: legacy absence remains an ordinary
login, while a flow requiring a bound policy fails closed when that binding is
absent. Hosts that do not use that OIDC storage need no auth data migration.
The `graphql-orm` bridge API remains source-stable for the AI runtime.

## Unreleased: schema module 0.7.0 to 0.8.0

Apply `AiSchemaModule` with provider starts and workers closed. This revision
adds the persistent semantics required by authenticated proposal and approval
lifecycles:

- `graphql_orm_ai_proposals.item_count` stores the schema-validated logical
  review-item count;
- proposal, proposal-item, and approval IDs are assigned by the owning service
  before content protection so envelope associated data binds the real row ID;
- proposal and approval tables expose stable dependency-owned keyset metadata;
  and
- approval `session_id` is an internal filterable field for bounded,
  scope-authorized repository windows (it is not exposed as generic CRUD).

The module version is `0.8.0`; do not apply these semantics under an earlier
module version. Existing pre-release proposal rows have no authoritative item
count, and existing protected proposal/approval envelopes may not have been
bound to a service-assigned row ID. Reconcile and remove unfinished rows through
their owning pre-release service, or recreate a disposable environment. Never
invent item counts, decrypt/rewrite envelopes with manual SQL, or treat a prior
pending/approved row as consumable. Completed chat history and provider usage
need no data rewrite.

Keep the runtime gate closed until managed migration validation and restore
reconciliation report module `0.8.0` ready.

### Rust, GraphQL, and behavior changes

- `OrmAiProposalService::persist_validated` stages a catalog-validated proposal
  through the current `AiRunLease` and returns a renewed lease. The previous
  lease is stale.
- `AiProposalService` provides bounded reads and CAS review. `AcceptEdited`
  requires a replacement payload and item count and revalidates both against
  the current exact registered schema version.
- `AiProposalOutcomeRecorder::record_applied_outcome` now takes an
  `&AuthPrincipal`. Call it only after the ordinary application mutation has
  committed; the service rehydrates/authorizes and links the authoritative
  application audit reference without performing the mutation.
- `OrmAiApprovalService::request_approval` requires a server-generated
  canonical preview and complete binding, then atomically parks the exact run
  and tool call in `WaitingApproval`. Its constructor now also requires the
  current `AiToolCatalog`; request and consumption reject catalog/descriptor/
  GraphQL-contract drift.
- `decide_approval` and `revoke_approval` are CAS-bound authenticated GraphQL
  operations. Recent MFA is enforced when the durable request requires it.
- `consume_approval` requires the current waiting lease plus a freshly rebuilt
  binding/preview, rehydrates the original actor, atomically consumes exactly
  once, and returns a renewed `Running` lease and `ConsumedAiApproval`. That
  proof does not replace the fresh resolver authorization/resource-version
  check that must immediately follow.
- Hosts may compose `AiProposalQueryRoot`, `AiProposalMutationRoot`,
  `AiApprovalQueryRoot`, and `AiApprovalMutationRoot`. This adds public GraphQL
  SDL when those roots are composed; regenerate affected client documents.
  Default names are camelCase and the `graphql-case-pascal` feature changes the
  entire new contract coherently without aliases.
- `AiProposalCatalog::descriptor` is new, and registration now rejects zero or
  excessive payload/source/item limits.

No application-domain data migration is required. No proposal review or
approval decision grants domain write authority by itself.

## Unreleased: schema module 0.6.0 to 0.7.0

Apply `AiSchemaModule` through the managed `graphql-orm` schema manager with
workers and provider starts closed. This revision extends
`graphql_orm_ai_tool_calls` with:

- a unique run/provider-call key plus the opaque provider call ID;
- provider-turn and within-turn ordering;
- current authorization policy and authorization-state bindings;
- static disclosure fingerprint and result classification;
- exact result-egress decision ID and manifest hash; and
- the ordinary application audit reference.

The module version is `0.7.0`; never apply these persistent semantics under a
previous module version. Existing pre-release active tool-call rows cannot
safely infer provider call identity, current authorization, disclosure, or
egress proofs. Stop/reconcile them through their owning pre-release service and
classify ambiguity as recovery-required. Recreate a disposable environment if
that service path is unavailable. Do not backfill invented values or use manual
SQL. Conversational messages and completed provider usage need no data rewrite.

Keep the runtime gate closed until managed schema validation and restore
reconciliation report module `0.7.0` ready. This change adds no public GraphQL
root or SDL field, so GraphQL clients need no document regeneration.

### Rust API and behavior changes

- `ModelRequest` requires a `continuation: Option<ModelContinuation>` field.
  Existing request literals should set it to `None`.
- `ModelInputBlock` adds `ToolResult`. A request containing tool results must
  carry an exact previous-response continuation, and a continuation without a
  tool result is rejected by this initial contract.
- `AiProviderCallLimits::with_maximum_tool_calls` sets a per-turn hard limit.
- `AiProviderCallPlan::new` still rejects custom tools. Use `new_with_tools`
  only with an exact current `AiToolPolicySet`; it accepts registered,
  fingerprint-matching, explicitly enabled, idempotent read-only application
  queries with no approval requirement. Use `new_continuation_with_tools` for
  subsequent turns so result blocks and exact manifests cannot be swapped.
- `AiProviderCallResult::tool_calls` exposes normalized unforgeable call
  requests, and `continuation` returns the exact prior-response identity.
- `OrmAiApplicationToolCallService` and `AiApplicationToolCallLimits` own
  protected/fenced read-only resolver execution and result egress. Replace the
  lease after every returned outcome.
- `AiAgentLoopGuard` enforces provider-turn/tool-call bounds and exact call/
  result/continuation ordering. Reconstructing a guard is not a recovery
  mechanism; uncertain work remains closed for restore/operator review.
- `OrmAiProviderOutputService::persist` now rejects results with pending custom
  tool calls.
- OpenAI stateful continuation requires `store_responses = true` and every
  exact transfer manifest must use
  `AI_EGRESS_RETENTION_PROVIDER_RESPONSE` (`provider_response`). The default
  remains false. No provider retention is silently enabled by migration.

The public API additions and `ModelRequest` field are deliberate pre-1.0
changes within the unreleased `0.2.0` line. This `0.7.0` slice did not change
approval semantics; the later `0.8.0` section defines canonical-preview and
one-shot approval persistence. Consequential execution remains unavailable
until the separately gated executor performs fresh resolver authorization.

## Unreleased: schema module 0.5.0 to 0.6.0

The crate version moves from `0.1.0` to `0.2.0` because this revision changes
public pre-1.0 Rust contracts as well as persistence.

Apply `AiSchemaModule` through the managed `graphql-orm` schema manager. This
revision changes budget persistence and must not reuse a previously applied
`0.5.0` module version.

Budget policy rows gain optional principal-kind matching and tool/image unit
ceilings. Budget counter rows gain:

- a stable `period_key` and unique `(budget_policy_id, period_key)` boundary;
- reserved and committed tool/image units; and
- an upsert identity for safe concurrent counter creation.

Budget reservation rows gain principal-kind binding, unique
`(principal_kind, principal_subject, idempotency_key)` enforcement, and an
`actual_runs` field so every reserved dimension reconciles completely.

Run-attempt completion/retry/recovery is now stored in the new append-only
`graphql_orm_ai_run_attempt_outcomes` table, uniquely keyed by `attempt_id`.
The existing append-only claim row remains immutable. Egress event IDs are now
caller-supplied exact `AiEgressDecisionId` values rather than unrelated
generated UUIDs.

The legacy optional completion columns on the pre-release attempt-claim table
remain physically present for non-destructive migration compatibility, but new
workers leave them null. The separate outcome table is the source of truth;
do not disable append-only enforcement to populate the legacy columns.

The `0.5.0` pre-release counter and reservation shapes cannot safely infer
principal kind or a stable interval key. Before applying this migration in an
early test deployment, stop workers and provider starts, classify every
in-flight call, preserve required audit/usage facts, and remove the old active
counter/reservation rows through the owning pre-release service. Do not invent
bindings or run manual SQL. Recreate disposable environments when that safe
service path is unavailable. Keep the runtime start gate closed until schema
validation and reconciliation report module `0.6.0` ready.

### Rust API and behavior changes

- `AiBudgetService::reconcile` returns `AiBudgetReconciliationResult` with
  committed, released, and still-held amounts.
- `AiError::BudgetDenied` reports missing/exhausted applicable capacity.
- `AiBudgetReservation` and `AiBudgetReconciliationResult` are no longer Serde
  deserializable. Obtain reservations from `AiBudgetService`; do not reconstruct
  proof-bearing values from persisted or client-controlled JSON.
- `OrmAiBudgetService` requires validated deployment-owned
  `AiBudgetServiceLimits` and a trusted `agql-auth::Clock`.
- A request now requires exactly one run unit, a fresh `ResolvedPrincipal`, a
  current running lease/attempt/fencing generation, an active persisted
  session with matching owner/tenant/exact scope, an expiry no later than the
  lease, and at least one applicable policy.
- Ordinary reconciliation may move an uncertain reservation to committed when
  authoritative actual usage arrives, but cannot optimistically release it.
- Orchestration must persist `MarkUncertain` immediately before handing the
  authorized reservation proof to provider transport. `ReleaseUnused` is only
  available while the durable reservation is still `Reserved`.
- `OrmAiRunService` and `AiRunServiceLimits` now own queue claims, heartbeats,
  retries, completion, and expired-lease recovery. Callers must replace the
  lease value after every successful fenced write; an older row-version proof
  is deliberately rejected.
- `AiProviderCallExecutor` requires a durable `AiEgressDecisionAudit`, budget
  service, `AiProviderUsageAccounting`, trusted clock, and bounded
  `AiProviderCallLimits`. Accounting implementations must resolve the exact
  `pricing_policy_version`, return provider-observed input/output tokens and
  one run exactly, and authoritatively compute cost/tool/image units. It
  supports one security-ordered provider turn. Tool-free construction remains
  the default; the later `0.7.0` section documents the separately gated
  read-only application-tool path.
- A successful provider result must be passed to
  `OrmAiProviderOutputService::persist` before terminal completion. Use the
  renewed lease returned by that service for `OrmAiRunService::finish`.
- `AiProviderCallResult` is bound to session/run/attempt/generation/provider/
  model and is not a transferable authorization proof.

This release adds no GraphQL budget configuration fields yet. Existing host
GraphQL SDL is unchanged, so no client document regeneration is needed for
this worker/provider slice. Hosts that previously implemented the budget trait
must update the reconciliation return type and preserve the same atomic,
fenced, idempotent semantics. Existing pre-release run claims have no outcome
fact to backfill unless an authoritative final state is known; never infer a
provider outcome. Keep ambiguous active work closed for recovery review.

## Unreleased: schema module 0.4.0 to 0.5.0

Apply the dependency-owned `AiSchemaModule` through the normal
`graphql-orm` schema manager using a new managed migration version. Do not copy
SQL or create AI tables manually.

The additive schema change creates:

- `graphql_orm_ai_budget_counters`
- `graphql_orm_ai_budget_reservations`

It also adds exact target/schema/document/projection/disclosure, principal/
delegation, resource precondition, policy/auth-state, canonical preview, and
one-shot consumption columns to `graphql_orm_ai_approvals`.

No existing conversational content needs rewriting. Existing pre-release
approval rows cannot safely manufacture the new bindings: expire/revoke them
during restore/startup reconciliation and require a fresh approval. Existing
unaccounted provider work must complete or be classified as uncertain before
enabling hard budgets.

Back up a disposable environment before rehearsing the migration. Runtime
workers, subscriptions, webhooks, and schedules remain closed until managed
schema validation and restore reconciliation report module `0.5.0` ready.

### Rust API changes

- `ProviderRequestContext::new` requires an `AuthorizedBudgetReservation`.
- `AiRuntimeBuilder` requires `graphql_targets(...)`.
- `GraphqlRequestContextFactory::build` receives the validated
  `GraphqlExecutionTarget`.
- `ToolGraphqlRequest` carries an exact `GraphqlOperationContract`.
- `GraphqlInvocationContext` carries explicit causation and optional safe
  delegation references plus the exact application scope.
- Application GraphQL tools use
  `AiToolCatalog::register_with_disclosure`; `register` is reserved for
  internal proposal-staging tools.
- `AiRuntimeBuilder` requires `tool_authorization_policy(...)` so current
  principal/scope/descriptor/arguments are authorized on every call.
- `AiRuntime::execute_tool` requires the registered `AiToolId` and returns an
  `AiToolExecutionResult` after argument, output-limit, and disclosure checks.
- Tool argument schemas must explicitly declare JSON Schema 2020-12.

These are deliberate pre-1.0 breaking changes. Update host construction and
mock fixtures together; do not create permissive placeholder targets,
disclosure schemas, or budget grants.

### Provider error classification

The OpenAI adapter now maps HTTP 401 to `ProviderError::CredentialUnavailable`
instead of `ProviderError::Rejected`. Hosts matching public error categories
should handle the credential category as a redacted configuration/rotation
failure. No data migration is required.

### GraphQL naming

The default SDL remains async-graphql camelCase. Hosts requiring PascalCase
enable:

```toml
graphql-orm-ai = {
  version = "0.1.0",
  features = ["sqlite", "graphql-case-pascal"]
}
```

This changes resolver, argument, input, output, subscription, and generated ORM
field names as one compile-time schema contract. There are no lowercase aliases.
Regenerate client documents and compare SDL before rollout. No database
migration is caused solely by the naming feature.

## Initial adoption

New deployments compose `AiSchemaModule`, apply its managed schema, configure
content protection and immutable deployment boundaries, and keep the runtime
start gate closed until readiness succeeds. PostgreSQL/MSSQL rehearsal must use
a disposable Docker-owned database; never point migration commands at a live
machine database.
