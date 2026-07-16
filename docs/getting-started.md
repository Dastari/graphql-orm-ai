# Getting Started

`graphql-orm-ai` is currently a Git-only pre-release crate. Use the same
reviewed dependency universe for `graphql-orm-ai`, `graphql-orm`, and
`agql-auth`. The public manifest pins reviewed full Git revisions; local path
overrides are unsupported release artifacts.

The current public source snapshot consumes the final reviewed `graphql-orm`
0.9.0 merge commit and `agql-auth` 0.10.0 annotated-tag target. Standalone Git
builds therefore resolve the reviewed dependency universe without depending
on moving sibling default branches.

## Features

Exactly one persistence backend is currently required:

- `sqlite` (default)
- `postgres`
- `mssql` (schema/compile support until ORM write parity lands)

Provider adapters are opt-in. `provider-openai` enables the native OpenAI
Responses adapter. `provider-anthropic` enables the native Anthropic Messages
adapter with a fixed official endpoint and secret-store credential reference.
`provider-xai` enables the native xAI Responses adapter with a fixed official
endpoint and zero-data-retention verification enabled by default.
`provider-ollama` enables the native Ollama `/api/chat` adapter; it needs an
explicit deployment endpoint policy even for loopback.
`provider-openai-compatible` enables the deliberately narrow Responses/SSE
adapter for endpoints with an exact GraphQL-managed capability and retention
profile plus deployment endpoint authorization.
`local-harness` enables the installed JSON-lines v2 text/structured/stateless-
tool protocol and provider wrapper; it still requires a deployment-owned
sandbox launcher.
`graphql-case-pascal` changes the complete GraphQL naming contract from default
camelCase to PascalCase.

## Host integration outline

1. Compose `AiSchemaModule` and apply its managed schema through
   `graphql-orm`.
2. Install `AuthPrincipal` in normal GraphQL request context and provide a
   `CurrentPrincipalResolver` for durable work.
3. Provide session/configuration access, fresh principal-aware
   `AiToolAuthorizationPolicy`, egress, secret-store, and content-protection
   implementations.
4. Register immutable logical GraphQL targets. Remote target URLs and
   credential issuance remain deployment-owned and never model-visible.
5. Register reviewed application tools with server-authored documents, exact
   operation contracts, and static disclosure schemas. Registration does not
   enable a tool.
6. Register proposal types, exact UI-intent descriptors, and provider adapters.
7. Install `OrmAiSkillCatalogService` as `Arc<dyn AiSkillCatalogService>` when
   composing the separate skill roots. Supply exact scope access, recent-MFA,
   trusted clock, and content-protection implementations. Skill resolution is
   eligibility data only and must be intersected with current tool, egress,
   provider, proposal, approval, and budget policy. See the
   [skill and UI-intent guide](skills-and-ui-intents.md).
8. Construct `OrmAiUiIntentDeliveryService` with the immutable UI-intent
   catalog, fenced run service, current-principal resolver, exact access and
   protection policies, and trusted clock. After ordinary assistant-output
   persistence, pass its renewed lease into intent delivery, then carry the
   returned renewed lease forward. A delivered intent remains a suggestion;
   the frontend must reauthorize resources and own all route mapping.
9. Construct `OrmAiRulePolicyService` with immutable deployment ceilings, an
   exact-scope access policy, a current-principal-derived hierarchy resolver,
   recent-MFA policy, and trusted clock. Compose the separate rule roots and
   resolve the complete hierarchy before planning a run. Wrap that service in
   `OrmAiCurrentRuleResolver` with the durable principal resolver, trusted
   clock, and principal-freshness limit. Pass the same rule resolver to the
   read-only coordinator and checkpoint service; every turn plan supplies the
   exact resolved set and trusted BYOK classification. Treat the result only as
   additional narrowing evidence. See the
   [hierarchical-rule guide](hierarchical-rules.md).
10. Install `OrmAiProposalService`/`OrmAiApprovalService` when composing their
   authenticated GraphQL roots. Supply host policies, fresh principal
   rehydration, content protection, recent-MFA policy, and the same fenced run
   service; do not expose the private generated ORM entities.
11. Install `OrmAiInboxService` as `Arc<dyn AiInboxService>` when composing the
   ordinary query/subscription roots. Configure `OrmAiConfigurationService`
   retention policy access, then schedule `OrmAiInboxPruningService` only as a
   trusted bounded host worker.
12. Schedule `OrmAiSessionRetentionService` as a separate trusted host worker.
   Start a scan cycle with no cursor and continue its bounded keyset pages until
   `next_session_cursor` is absent. This prunes eligible provisional deltas and
   age-expired terminal tool/approval protected payloads under the current
   `raw_payload_retention_seconds` policy while preserving newer/live authority.
   It also purges expired orphaned protected coordinator checkpoints only after
   terminal run/attempt/budget and final-output or tombstoned-tool re-proof;
   after a deleting-session cutoff it also removes bounded protected session
   events and CAS-tombstones bounded session-bound principal-inbox payloads
   without removing their stream sequences, then protected context-summary
   checkpoints, tombstones terminal
   proposal/item protected content under whole-session bounds, then tombstones
   terminal tool/approval protected payloads only after proving the complete
   bounded run/call/step/approval graph. It next coordinates attachment
   artifacts and then their parent objects through the separately scheduled
   `OrmAiAttachmentService::cleanup_once`. Install a reviewed
   `AiProviderFileDeletionService` if artifacts may carry provider references;
   success must mean authoritative exact-reference absence. The worker deletes
   only confirmed artifact/attachment tombstones, scrubs eligible terminal
   message content, clears validated
   terminal-run checkpoint pointers, and purges bounded immutable coordinator
   checkpoints. After a final complete proof it redacts the title and moves the
   hidden shell to `deleted`. It does not erase unresolved accepted proposals,
   active or uncertain tool authority, ambiguous artifact/provider files,
   required redacted metadata, other append-only facts, or runs; see the
   [retention guide](session-retention.md).
13. Construct `OrmAiContextCompactionService` from the same fenced run,
   current-principal, owner/scope access, content-protection, and clock
   dependencies. Call `prepare` only for a running lease and a boundary that
   leaves the configured recent tail. Build the ordinary provider-call plan
   from its exact request and `Restricted` `egress_sources` with purpose
   `context_compaction`; execute through `AiProviderCallExecutor`, then pass
   that exact result to `persist` and carry the renewed lease. Treat a loaded
   summary as untrusted model content. See
   [protected context compaction](context-compaction.md).
14. Install `OrmAiUsageService` as `Arc<dyn AiUsageService>` with a host
   `AiUsageAccessPolicy`. Grant current-principal-only reporting by default;
   exact-scope reporting needs separate administrative authorization.
15. Opt into budget-policy mutations with
   `OrmAiConfigurationService::with_budget_policy_management`, using deployment
   ceilings no broader than operational spend policy. Authorize reads and
   writes independently; writes require recent MFA.
16. Construct `OrmAiPricingService` with an independent configuration access
   policy, recent-MFA policy, trusted clock, and
   `AiPricingCatalogManagementLimits`. Install the same instance as
   `Arc<dyn AiPricingCatalogService>`, `Arc<dyn AiPricingQuoteService>`, and
   `Arc<dyn AiProviderUsageAccounting>` when using its token and completed
   web/file-search accounting. Built-in rate administration stays disabled
   unless the deployment sets an independent per-call management ceiling.
17. Apply/validate migrations and restore reconciliation, then open the runtime
   start gate.

For Ollama, configure one fixed root origin, apply host/DNS/network isolation,
and pass the same exact egress and atomic budget proofs as any remote provider.
The adapter supports streaming text, exact PNG/JPEG/WebP input, structured
output, and exact registered application tools through
`ModelContinuationMode::StatelessReplay`. Every replayed tool result needs its
own fresh manifest. Provider-retained continuation, built-ins, and hidden
thinking remain rejected. An exact completed checkpoint may cross a lease
generation only after the adopter revalidates every historical durable row;
ambiguous work remains closed. See the [Ollama guide](ollama.md).

For Anthropic, construct `AnthropicProviderConfig` with a `SecretRef` and pass
an `AiSecretStore` to `AnthropicProvider::new`. The native adapter supports
streaming text/JSON, structured output, and strict stateless application-tool
continuation. It requires an explicit output-token ceiling and the ordinary
exact egress and budget proofs. Attachments, provider built-ins, extended
thinking, provider-retained continuation, and prompt-cache creation remain
closed. See the [Anthropic guide](anthropic.md).

For xAI, construct `XAiProviderConfig` with a `SecretRef` and pass an
`AiSecretStore` to `XAiProvider::new`. Text/JSON, structured output, and strict
parallel application tools are supported. The adapter requires an xAI
zero-data-retention response attestation by default. Disabling that check is
an explicit deployment choice and still requires egress policy to disclose and
permit xAI's ordinary retention. Response-ID tool continuation also requires
`store_responses`, an exact per-call retention proof, and ZDR verification to
be disabled; stateless encrypted-reasoning replay remains closed. See the
[xAI guide](xai.md).

For an OpenAI-compatible Responses endpoint, create or update an
`OpenAiCompatible` provider profile through the authenticated configuration
mutation. Declare only reviewed capabilities and use a specific retention
label understood by egress policy. Obtain its secret reference through the
trusted configuration boundary, call
`OpenAiCompatibleProviderConfig::from_profile`, and construct the provider
with the same deployment endpoint policy and secret store. The endpoint is
immutable after construction; each request must reproduce its exact profile,
destination, and retention in the egress proof. See the
[OpenAI-compatible guide](openai-compatible.md).

For installed programs, build an immutable `AiLocalHarnessRegistry`, implement
`AiLocalHarnessProcessLauncher` at a reviewed OS/container sandbox boundary,
wrap it in `AiJsonLinesLocalHarnessDriver`, then register the resulting
`AiLocalHarnessProvider` as `ProviderKind::LocalHarness`. The GraphQL provider
profile has no process configuration or base URL. Do not implement the launcher
as a plain inherited `Command`; it must meet the digest, clean-environment,
network-denial, resource, process-tree, and kill-on-drop obligations in the
[local harness guide](local-harness.md).
Tool-capable harnesses implement JSON-lines v2 and advertise `custom_tools`
and `stateless_continuation` together. A text-only harness may leave both false;
neither form receives filesystem, network, shell, or credential authority.

The principal inbox lets a virtualized chat drawer refresh bounded session
shells across multiple conversations without loading transcript history. Its
subscription reauthorizes the current principal and rechecks each referenced
session/scope; its pruning worker uses only current GraphQL-managed retention
policies. See the [principal inbox guide](principal-inbox.md).

For attachments, construct `OrmAiAttachmentService` with the same access and
protection boundaries plus an exact `graphql-orm-storage::BlobStore`, trusted
full-object scanner, separate fail-closed acceptance policy, and clock. Compose
the attachment roots for ticket/finalization metadata, and route large bytes
through a host-owned authenticated streaming handler using
`AiAttachmentUploadService`; never accept file bytes in ordinary GraphQL JSON.
Install the same service on `AiProviderCallExecutor` with
`with_attachment_resolver` only when provider image/file input is enabled, and
set deployment-owned reopening limits. This does not authorize disclosure:
each exact input still needs atomic budget proof plus separate audited
inference and attachment capability manifests. See the
[attachment guide](attachments.md).

For private routed/direct targets, use one cloned
`AiRemoteAuthenticatedGraphqlAdapter` as both request-context factory and
executor. Implement its authority issuer at the short-lived credential boundary
and its transport at the fixed private logical-route boundary. Do not retain or
forward the user's bearer token. See the
[remote execution guide](remote-graphql-execution.md).

For SQLite/PostgreSQL hosts, construct `OrmAiBudgetService` with a trusted
`agql-auth::Clock` and validated deployment-owned `AiBudgetServiceLimits`.
Provider orchestration must call `reserve` before egress and `reconcile` after
the result classification. It must durably mark the reservation uncertain
immediately before handing the authorized proof to provider transport; after
that boundary the ordinary unused-release path is deliberately unavailable.
Budget and pricing policy configuration is exposed only through authenticated
GraphQL lifecycle services. Do not seed either with application SQL or expose
the private generated ORM entities. Budget updates use exact CAS; pricing
versions are append-only and selected only by their exact returned reference.

Construct `OrmAiRunService` from the same ORM database and trusted clock. The
lower-level concrete provider path is deliberately explicit:

1. `claim_next` and `start` a run, replacing the returned lease after every
   successful fenced call.
2. Execute a server-authored `AiProviderCallPlan` through
   `AiProviderCallExecutor`, configured with the budget service and a durable
   `OrmAiEgressDecisionAudit`. Supply `AiProviderUsageAccounting` backed by an
   immutable deployment pricing catalog; `OrmAiPricingService` supplies the
   concrete immutable implementation. It settles the exact pricing version
   rather than substituting current rates or reserved estimates. Web/file
   search is charged only from exact normalized completed-call pairs;
   advertised-but-unused tools cost zero, while code interpreter and image
   generation remain closed in the concrete accountant.
   To emit provisional visible output, explicitly install
   `OrmAiLiveDeltaService` with `with_live_delta_sink`. Use the same runtime,
   run service, current-principal/access/protection boundaries, and validated
   coalescing/persistence limits. The default executor emits no live events.
   Attachment turns also require explicitly installing a trusted
   `AiProviderAttachmentResolver`; without it they fail before transport.
3. If the provider result is terminal and has no application-tool calls,
   persist it with `OrmAiProviderOutputService::persist`. This reauthorizes
   again, protects content, writes windowable blocks and a session event, and
   returns a renewed lease.
4. Finish the run with that renewed lease.

For the bounded registered read-only tool path, prefer
`AiReadOnlyAgentCoordinator` over manually sequencing these calls. Supply a
trusted `AiReadOnlyAgentTurnPlanner` that constructs initial turns with
`new_with_tools` and consumes exact later `AiAgentContinuation` values with
`new_continuation_with_tools`. Configure its heartbeat interval comfortably
shorter than the run-service lease TTL. Also supply an
`OrmAiCoordinatorCheckpointService` as both the required
`AiAgentCheckpointWriter` and `AiAgentCheckpointAdopter`, using the same
principal/access/protection boundaries as transcript persistence. Resolve the
complete hierarchy through one shared `OrmAiCurrentRuleResolver`, pass it to
both the coordinator and checkpoint service, and include its exact
`AiResolvedRuleSet` plus the trusted server-derived BYOK decision in every
turn plan. A successful coordinator outcome means the terminal/recovery state
was durably committed; a lost fence returns an error and must not be followed
by another write from that worker. Only an exact
completed provider-retained or bounded stateless read-only tool-batch
checkpoint has cross-generation adoption authority, and only after fresh
protected validation of every current and historical durable proof; see the
[checkpoint guide](coordinator-checkpoints.md).

For sequential provider-retained supervised mutations, construct
`AiSupervisedAgentCoordinator` with the same run, provider, output, checkpoint,
rule, and clock boundaries plus `OrmAiConsequentialToolCallService`,
`OrmAiSupervisedResumeService`, and a host
`AiSupervisedAgentTurnPlanner`. The planner must use
`new_with_supervised_tools` for the first turn and
`new_supervised_continuation_with_tools` for the opaque continuation, and its
`AiSupervisedAgentTurnPlan` must expose only exact
`SupervisedWrite`/`OneShot` definitions. Route normal claims to
`execute_claimed`; after a human approves, route the exact
`claim_next_approved` handoff to `execute_approved_claim`. Stop the staging
worker on `WaitingApproval`; never heartbeat or poll the human wait. Each later
mutation receives a new independent preview and approval.

In the live worker cycle, run
`OrmAiApprovalWaitReconciliationService::reconcile_waits` before
`OrmAiRunService::recover_expired_leases`. Supply a deployment policy that
reevaluates the current principal and exact scope. The pass leaves valid
pending/approved waits parked, cancels denied/revoked/expired/cutoff or
policy-cancelled waits, and sends malformed linkage to `RecoveryRequired`.
It never claims or executes approved work. Keep it closed during snapshot
restore; restored human waits remain recovery-only.

If transport or streaming becomes ambiguous, do not finish or release the
reservation. It remains uncertain and expired-run reconciliation moves the run
to `RecoveryRequired`.

`AiProviderCallPlan::new` intentionally remains tool-free. The separately
gated `new_with_tools` path exposes only exact registered, policy-enabled,
idempotent read-only application queries. Execute returned calls with
`OrmAiApplicationToolCallService`, carry its renewed lease forward, and use
`AiAgentLoopGuard` plus `new_continuation_with_tools` to prevent missing,
duplicated, or swapped results. Supervised plans use the dedicated constructors
and `OrmAiConsequentialToolCallService` to request a server-previewed one-shot
approval and execute it through fresh ordinary resolver authorization;
after a human decision, a different process may use the one-owner
`claim_next_approved` handoff before the same fresh execution path. Protected
provider-turn adoption plus continuation checkpointing is implemented for one
provider-retained mutation through `OrmAiSupervisedResumeService`; its exact
completed result can be re-adopted under a new generation before one-shot
checkpoint consumption. `AiSupervisedAgentCoordinator` now performs that
consumption and the remaining bounded sequential provider loop. Multi-call,
mixed, parallel, and stateless supervised continuation remain unfinished.
Arbitrary GraphQL and
ambiguous replay remain closed. See the
[read-only tool-loop guide](read-only-tool-loop.md) and
[supervised tool-loop guide](supervised-tool-loop.md).

See the [worker and provider-turn guide](worker-provider-turn.md) and
[implementation status](implementation-status.md). Provider-persistent file
upload/search, attachment quotas/derivative production, authoritative
code-interpreter/image-generation unit pricing, and external-content
retention,
provider-turn and partial-batch restart adoption and stateless/parallel
supervised continuation remain
under implementation. Protected provisional live output is opt-in and documented in
the [live-streaming guide](live-streaming.md). The proposal/approval GraphQL
lifecycles and consequential executor are implemented; approval consumption is
always followed by fresh ordinary resolver authorization in that path. See the
[proposal and approval lifecycle guide](review-lifecycles.md) and
[supervised tool guide](supervised-tool-loop.md).
