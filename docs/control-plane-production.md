# Control-plane and production integration gates

This document records the Slice 6 audit. It distinguishes implemented
project-agnostic host contracts from crate work that remains closed. Tool
discovery never grants authorization, and an administrative configuration
mutation never grants resolver access by itself.

## Current control-plane state

| Area | Current state | Production gate |
| --- | --- | --- |
| Tool catalog | Implemented. Registration requires a server-authored document, exact GraphQL contract, static disclosure schema, bounded projection, and current descriptor fingerprint. | Hosts review and register every descriptor; absence stays disabled. |
| In-memory enablement | Implemented. `AiToolPolicySet` requires an explicit enabled binding to the exact current descriptor fingerprint and a deployment maturity ceiling. | The host supplies the current set and the principal-aware `AiToolAuthorizationPolicy`; ordinary resolver authorization still runs. |
| Durable tool-policy record | Schema exists, but authenticated read/manage and live-policy resolution are not implemented. Existing `maximum_calls`, `maximum_output_bytes`, `constraints`, risk, and approval fields are not a runtime proof. | Keep the GraphQL lifecycle closed until the applied-restore gate passes and every stored bound is enforced at execution, not merely persisted. |
| Operation/disclosure metadata | Explicit reviewed application contracts and disclosure schemas are implemented. Generated ORM resolver metadata is not available upstream. | A copy-ready `graphql-orm` owning-agent prompt is staged in `.handoffs/`. Keep generation closed until a reviewed final upstream SHA is pinned. |
| Recursion prevention | Explicit operation domains plus a fail-closed identifier scanner reject known AI control-plane and introspection names. This is defense in depth, not a complete schema-aware proof. | Continue requiring reviewed application-only descriptors. Add schema-aware validation only after generated resolver metadata can identify the exact target. |
| Provider-persistent files | Raw provider store IDs are rejected before transport. Inline attachment input and exact deletion of already-known provider artifacts remain separate capabilities. | Upload/index/search stays closed under the lifecycle in [provider-persistent files](provider-files.md). |

## Durable tool-enablement lifecycle

The existing private `graphql_orm_ai_tool_policies` entity is intentionally not
exposed by generated public roots. A complete management service must:

1. rehydrate an authenticated administrator and require current recent MFA;
2. authorize a distinct read or manage action for one exact `AiScope`;
3. resolve the stable tool ID in the current immutable catalog;
4. require the exact current descriptor fingerprint and reject
   AI-control-plane, introspection, internal, secret, autonomous, or
   unsupported operation/risk/approval combinations;
5. validate deployment ceilings for policy count, maturity, call count, and
   output bytes;
6. create or compare-and-swap one scope/tool binding and append a redacted
   audit fact in the same state-machine transaction;
7. treat missing or stale fingerprints as disabled;
8. build the live `AiToolPolicySet` only from exact-scope rows whose complete
   constraints are valid and currently enforceable;
9. enforce per-tool call/output constraints in the coordinator and disclosure
   path, rather than trusting stored values; and
10. include the policy graph in backup, empty-target restore, reconciliation,
    and runtime-readiness checks before enabling its mutation root.

Implementing only the GraphQL mutation or only loading the `enabled` boolean
would create false authority, so neither partial path is opened. This work is
downstream-owned after the reviewed `graphql-orm-backup` integration returns
and applied restore is available.

## Resolver-operation metadata boundary

`graphql-orm` 0.15.0 computes generated resolver names and exposure inside its
derive implementation, while public `EntityMetadata` contains storage/schema
facts only. `graphql-orm-ai` must not reimplement those naming and exposure
rules: a duplicate table could silently authorize the wrong resolver after a
case, rename, projection, mutation-policy, or backend-capability change.

The staged upstream prompt requests project-agnostic static descriptors for
generated operations. It deliberately leaves data classification, result
projection, AI control-plane ownership, registration, and authorization in
this crate or the host. Until a reviewed final upstream SHA is repinned,
explicit host-reviewed `GraphqlOperationContract` plus
`AiDisclosureSchema` remains the only supported path.

## Production host seams

The crate supplies secure interfaces and fail-closed composition; it does not
embed deployment authority.

### Secret stores and keyrings

`AiSecretStore` uses bounded opaque `SecretRef` values and resolves plaintext
only immediately before use. Mutable production adapters must prove:

- fresh unguessable allocation when `put(None, ...)` is requested;
- current-value resolution after rotation;
- deletion/revocation and bounded orphan expiry after a failed compensation;
- encryption and access control in the deployment-owned backend;
- no plaintext/reference leakage through logs, errors, GraphQL, telemetry, or
  backup; and
- fail-closed behavior on backend outage.

`EnvironmentSecretStore` is a read-only bootstrap adapter, not a production
mutable keyring. It maps only host-registered references to variable names and
cannot satisfy configuration rotation.

### Delegated GraphQL authority

`AiRemoteGraphqlAuthorityIssuer` receives a redacted exact request bound to the
fresh principal, actor, logical target, audience/resource, operation and schema
fingerprints, canonical variables, scope, run/tool call, audit chain,
idempotency-key hash, and expiry. A production issuer must demonstrate that
the credential's actual claims are no broader than that request and that no
incoming bearer token is persisted or forwarded.

### Private GraphQL transport

`AiRemoteGraphqlTransport` receives only a fixed logical target, one ephemeral
authority value, and the server-authored operation. A production adapter must
prove logical-target-to-destination allowlisting, redirect denial, private
network isolation, TLS identity, response byte/record bounds, routed/direct
authorization parity, one-request credential use, safe errors, and
application audit correlation.

The adapter's deterministic unit suite already covers request/target swaps,
canonical-variable binding, expiry, principal freshness, context swapping, and
credential redaction. Destination routing, token claim contents, network
isolation, KMS/Vault behavior, and application authorization parity can only
be proven by the owning deployment and consumer acceptance suites.

## Slice 6 result

No safe runtime capability is opened by this audit. Provider-file authority
remains closed, the durable tool-policy lifecycle waits on applied restore,
and schema-aware metadata generation waits on the reviewed upstream resolver
metadata contract. Existing explicit catalog, disclosure, secret, delegation,
and private-transport seams remain supported and fail closed when a required
host implementation is absent.
