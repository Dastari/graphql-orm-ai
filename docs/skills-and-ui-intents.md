# Protected skills and typed UI intents

`graphql-orm-ai` treats skills as protected, versioned data. A skill is not a
Rust plugin, executable, GraphQL document, shell command, route, or authority
grant. It can request a previously registered capability, but every request is
intersected with current independent host policy before use.

## Skill catalog

`OrmAiSkillCatalogService` persists through generated `graphql-orm` entities,
queries, compare-and-swap updates, and transactions. It issues no raw SQL and
never accesses application data directly.

A logical skill has an opaque UUID identity, exact `AiScope`, safe name and
description, enabled flag, current immutable version, and row version. Names
are presentation metadata and need not be unique; callers use the UUID for
updates and publication.

One published version contains:

- protected trusted instructions;
- exact application-tool descriptor fingerprints;
- maximum data classification and tool maturity requests;
- JSON Schema 2020-12 input and output contracts;
- requested provider capabilities;
- hard step, duration, output-token, and optional cost ceilings;
- registered proposal type IDs;
- exact logical UI-intent type and descriptor-fingerprint bindings;
- an activation rule, author, publication time, and canonical checksum.

Publishing, safe-metadata updates, and enablement require recent MFA, exact
host `AiSkillAccessPolicy` authorization, and current compare-and-swap state.
Publication protects instructions before ORM persistence, appends a redacted
audit fact in the same state-machine transaction, and never modifies an older
version. A skill cannot be enabled before a version exists.

Reads return at most 100 redacted skills for one exact scope. Instruction text,
schemas, and provider-capability details are not exposed by the GraphQL view.
Runtime resolution requires a current principal, current scope access, and a
ready exact-scope content-protection policy. It opens the current version and
verifies its protected envelope, strict stored format, provenance, and
checksum. Corruption, unknown fields, stale formats, scope swaps, and enabled
skills without a current published version fail closed.

`AiResolvedSkill` still grants no tool, resolver, mutation, provider, egress,
budget, proposal, approval, or UI authority. In particular:

- registration and discovery are not enablement;
- a tool fingerprint must still be registered, policy-enabled, mature enough,
  freshly authorized, and executed through the ordinary GraphQL resolver;
- classification is only a ceiling request; static disclosure and current
  egress policy may narrow or reject it;
- skill budgets only narrow deployment, scope, principal, session, and route
  budgets; they cannot add spend;
- `Manual` skills require trusted planner selection;
- `AlwaysForScope` means eligible for consideration, not automatically trusted
  or authorized.

## GraphQL composition

Compose `AiSkillQueryRoot` and `AiSkillMutationRoot` into the host schema and
install the service as `Arc<dyn AiSkillCatalogService>` in async-graphql data.
The roots expose:

- `aiSkills` / `AiSkills`;
- `upsertAiSkill` / `UpsertAiSkill`;
- `publishAiSkillVersion` / `PublishAiSkillVersion`;
- `setAiSkillEnabled` / `SetAiSkillEnabled`.

The selected names depend on `graphql-case-pascal`; lowercase aliases are not
added. Hosts remain responsible for authenticating requests, constructing the
current `AuthPrincipal`, implementing scope policy, choosing recent-MFA
requirements, and configuring content protection through their ordinary
GraphQL management workflow.

Do not expose the private generated skill entities as generic CRUD roots. The
catalog service is the security boundary because it enforces protection,
immutability, bounded reads, CAS, MFA, exact scope checks, and audit.

## Typed UI intents

UI intents are backend-validated logical suggestions. They are not route
commands. The host registers an `AiUiIntentTypeDescriptor` with:

- a bounded lower-case namespaced type ID;
- an immutable schema version;
- a bounded JSON Schema 2020-12 payload contract;
- a maximum serialized payload size;
- optional bounded display metadata.

The descriptor fingerprint binds its type, schema version, schema, and payload
limit. A published skill stores the exact type/fingerprint pair so changing a
schema under the same logical ID cannot silently widen an existing skill.
`AiUiIntentCatalog::validate_bound` rejects unknown, swapped, stale, oversized,
or schema-invalid drafts and returns a `ValidatedAiUiIntent` with a
server-assigned ID.

Even a validated intent does not prove that the user may access a referenced
resource. Before presenting or acting on a suggestion, the host and frontend
must recheck current application state and authorization. The frontend maps a
logical type through application-owned code; the crate never stores or
constructs TanStack Router paths, URLs, component names, callbacks, or domain
entity types, and it never forces navigation.

For example, a consumer might privately register
`generic.open_resource` with an object schema containing opaque
`resourceKind` and `resourceId` strings. Another consumer can map the same
logical idea differently without changing this crate. Product-specific type
names and route mappings belong only in the consumer.

`OrmAiUiIntentDeliveryService` persists a provider-produced suggestion only
when all of the following remain true:

- the result belongs to the exact running session/run/attempt/generation and
  contains no application-tool call;
- its normalized events form one ordered response-start, visible-text, usage,
  and response-completion sequence with exact response and usage identity;
- the complete visible text is one bounded, deny-unknown-field camelCase
  envelope containing only `formatVersion`, `intentType`, and `payload`;
- the immutable catalog still contains the exact type and descriptor
  fingerprint, and the payload passes that JSON Schema;
- the session is active and bound to the exact owner, scope, and tenant;
- current session/scope write authority and the exact protection policy pass
  both before and after content protection;
- the provider usage has an exact committed budget reservation for the same
  provider, model, run, attempt, generation, principal, scope, and usage; and
- the worker lease remains current at the committing transaction.

Success atomically advances the session stream and run fence, appends a
protected `ui_intent_suggested` session event, appends a protected event to the
owner's principal inbox, and records a redacted audit fact. The source-bound
event IDs make an exact retry idempotent. Reasoning, tools, built-ins,
citations, unknown events, extra envelope fields, missing budget evidence, and
stale/swapped bindings fail closed.

The normal orchestration order is: persist protected assistant output, pass
its renewed lease to UI-intent delivery, and pass the delivery result's
renewed lease to completion or the next fenced write. The returned
`AiPersistedUiIntent` proves schema validation and durable persistence only.
It does not authorize a referenced resource or frontend action.

## Persistent format and restore

AI schema module `0.24.0` gives the existing skill/version fields strict v1
meaning. Module `0.25.0` additionally gives existing session/inbox event rows
the strict protected UI-intent delivery meaning described above. The skill
format uses deny-unknown-field JSON wrappers and a canonical
SHA-256 checksum over plaintext instructions plus all security-relevant
metadata and provenance. The protected envelope itself is also bound to the
exact version row, field, and scope by the content-protection contract.

Restore validation must keep the runtime closed when a skill row or its
current version is malformed, missing, mismatched, or uses an unsupported
format, or when a UI-intent event pair cannot prove its exact protected
payload, source binding, committed budget, owner/scope, and audit linkage.
Skill selection and event delivery must not resume until reconciliation
succeeds. See the [migration guide](../MIGRATION.md) for handling private rows
created before these strict services existed.
