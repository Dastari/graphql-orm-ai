---
name: agql-auth
description: >
  Use when working on authentication, authorization, principal references,
  current-principal rehydration, delegation, recent-MFA, async-graphql context
  wiring, or long-lived subscription authorization in graphql-orm-ai.
---

# agql-auth Skill

## Use This Skill When

- accepting `AuthPrincipal` from an authenticated GraphQL request
- persisting a non-secret principal reference for background AI work
- rehydrating current roles, scopes, tenant membership, and assurance
- checking token/session revocation during a run
- wiring authenticated websocket subscriptions
- enforcing recent MFA for high-impact approvals or secret configuration
- using audience/resource-bound API or service tokens
- designing bounded delegation for disconnected/background tasks
- deciding what auth behavior belongs in `agql-auth` versus `graphql-orm-ai`

## Crate

- Dependency: `agql-auth`
- Local repo: `../agql-auth`
- Upstream repo: `https://github.com/Dastari/agql-auth`
- The name refers to async-graphql integration, not to the ORM layer.

## Preferred Usage

Use `agql_auth::prelude::*` unless narrower imports make a public module clearer.

Important existing types:

- `AuthPrincipal`
- `AuthUser`
- `ApiTokenPrincipal`
- `AccessTokenValidator`
- `TokenStatusChecker`
- `TokenStatusRequest`
- `ReauthorizationPolicy`
- `SessionAssurance`
- `RecentMfaPolicy`
- `AuthorizationDecision`

Planned reusable additions:

- `PrincipalReference`
- `CurrentPrincipalResolver`
- bounded `DelegationGrant`
- reusable long-lived connection authorization state

## Boundary

`agql-auth` is the reusable authentication and principal-lifecycle runtime.

It should own:

- access/session/API-token validation
- revocation and expiry status contracts
- safe, serializable principal references
- current-principal rehydration contracts
- scope subset and audience/resource binding for delegations
- recent-MFA and assurance aging
- GraphQL request-context and websocket reauthorization helpers
- generic guards, status checks, and redacted authorization decisions

`graphql-orm-ai` is the reusable agent runtime.

It should own:

- AI sessions, runs, messages, tools, approvals, and budgets
- tool-risk classification and argument-bound approval records
- AI-specific delegation constraints such as tool allowlists and cost ceilings
- provider egress and data-classification policy
- decisions to pause a run as `WAITING_REAUTH`

Host applications should own:

- concrete user/session/token persistence
- implementations that rehydrate current principals
- tenant/project membership and application resource policy
- HTTP/cookie/bearer extraction
- application GraphQL schema composition
- record- and field-level authorization

Do not make `agql-auth` depend directly on `graphql-orm` unless there is a
deliberate shared-library design decision. Integrate through traits and safe
principal types.

## Integration Rules

1. Never persist bearer tokens.
Store only a safe `PrincipalReference` containing subject, session/token IDs,
tenant/resource binding, actor, correlation, and expiry metadata.

2. Never trust stale role or scope snapshots.
Rehydrate the principal before provider egress, every application tool call,
after approval, and at long-run checkpoints.

3. Reauthorize long-lived subscriptions.
Authenticate `connection_init`, schedule fail-closed status checks, age recent
MFA, and close or pause on revocation, expiry, or permission loss.

4. Delegation cannot add authority.
Delegated scopes must be a subset of the current principal, have bounded
expiry, preserve actor/correlation identity, and remain revocable.

5. Keep application authorization authoritative.
`agql-auth` provides authentication, coarse scopes, token lifecycle, and
assurance. The host's GraphQL resolver plus entity/row/field policies decide
whether a particular operation and record are allowed.

6. Bind high-impact approvals to current assurance.
Publish, delete, permission, credential, and other sensitive operations should
require recent MFA when configured and must reauthorize after approval.

7. Use resource-bound service principals for scheduled work.
Do not keep a user bearer token alive or silently convert user work into
unbounded system access.

8. Keep audits redacted.
Record principal references, requirements, resource, result, reason code, and
correlation. Never include tokens, provider keys, prompts, or tool arguments in
auth audit structures.

9. Keep MCP tokens audience-bound.
An optional MCP facade must authenticate its caller and must never pass an
application access token through to a downstream MCP server.

10. Keep database tests isolated.
Any PostgreSQL or MSSQL auth integration test must use a disposable Docker
container and must never connect to a live local database.

## When Not To Use

- provider streaming or model event normalization
- ORM schema generation or database migration work
- tool discovery with no authentication impact
- frontend login UI with no backend contract change

## Request Execution Pattern

1. The transport authenticates a bearer token, cookie, or connection-init value.
2. It inserts `AuthPrincipal` and related safe auth context into the
   async-graphql request.
3. `graphql-orm-ai` stores only `PrincipalReference` with durable work.
4. `CurrentPrincipalResolver` reconstructs current authority before execution.
5. The AI runtime evaluates its tool and approval policy.
6. The host executes the server-owned GraphQL document as that principal.
7. Normal resolver/entity/row/field policies make the final authorization
   decision.

## Project Guidance

- expand `agql-auth` when the primitive is reusable across projects
- keep tool approval, model policy, and AI budgets in `graphql-orm-ai`
- keep application record policy in the host schema
- fail closed on status-check or rehydration errors
- preserve recent-MFA semantics across long-lived connections
- never trade a disconnected browser for broader background authority
