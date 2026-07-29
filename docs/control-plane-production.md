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
| Operation/disclosure metadata | Explicit reviewed application contracts and disclosure schemas are implemented. ORM 0.16.0 generated catalogs now bind an exact exposed generated resolver, catalog/operation fingerprints, operation kind, and one server-authored root selection. | Hosts must explicitly classify the descriptor as an application operation and still supply the finished-schema, projection, disclosure, enablement, and authorization contracts. Custom roots remain explicit. |
| Recursion prevention | Generated operations require exact catalog resolution plus an explicit host application-domain policy. All descriptors still carry explicit operation domains, and the fail-closed identifier scanner rejects known AI control-plane and introspection names. | Metadata is discovery/drift evidence, not authorization. Keep the scanner for custom roots and defense in depth; ordinary resolver authorization remains authoritative. |
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

`graphql-orm` 0.16.0 exposes project-agnostic static descriptors and an
exposure-resolved operation catalog. `GraphqlOperationContract` can bind one
current catalog fingerprint, one exposed operation fingerprint/kind/category,
and one server-authored document containing exactly one named operation and
one unaliased generated root. `register_generated_with_disclosure` re-resolves
that binding against the current catalog and then requires an explicit
`AiGeneratedGraphqlOperationPolicy`. The provided policy denies all by
default.

The upstream metadata deliberately does not define application versus AI
control-plane ownership, complete host SDL, server-authored documents, result
projections, disclosure classification, runtime limits, or authorization.
Those remain host/downstream responsibilities. A generated descriptor is not
enabled merely because it is discoverable, and every invocation still passes
fresh host policy plus ordinary resolver authorization.

Custom roots are outside the generated catalog. They continue to use explicit
host-reviewed `GraphqlOperationContract` plus `AiDisclosureSchema` and the
fail-closed identifier scanner. The crate does not infer custom-root exposure
from partial SDL or reimplement ORM naming rules.

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

Generated-operation registration is now implemented as a safer drift-binding
path, but it does not independently open or authorize a tool. Provider-file
authority remains closed and the durable tool-policy lifecycle still waits on
applied restore. Existing explicit catalog, disclosure, secret, delegation,
and private-transport seams remain supported and fail closed when a required
host implementation is absent.
