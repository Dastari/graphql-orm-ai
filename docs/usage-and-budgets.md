# Usage Ledger, Budgets, and Reporting

Provider execution has two related but distinct persistent contracts:

- budget reservations prevent concurrent calls from independently spending the
  same remaining capacity; and
- usage facts record only authoritative completed consumption for reporting.

Neither contract grants provider egress, resolver access, or permission to
read a transcript.

## Transactional lifecycle

Before transport, `OrmAiBudgetService` checks every applicable policy and the
current principal/session/run fence, then reserves all relevant counters in one
state-machine transaction. At the transport boundary the reservation becomes
uncertain. Capacity remains held until there is authoritative evidence.

`Commit` requires complete actual amounts and provider-reported cached input.
Cached input must be no greater than total input. The reconciliation moves the
actual amounts to committed counters, releases only proven unused capacity,
updates the reservation, and appends one `graphql_orm_ai_usage_entries` fact in
the same transaction. The usage fact has a unique reservation ID, so an exact
idempotent replay returns the prior result without duplicating usage.

`ReleaseUnused` is permitted only when transport provably did not occur and
appends no usage. `MarkUncertain` retains capacity and appends no usage;
privileged recovery remains a separately gated future surface.

`input_tokens` is the provider-reported total. `cached_input_tokens` is a
validated subset, not an additional amount. Deployment pricing can calculate
cached and uncached rates from those two facts while preserving the provider's
authoritative total.

## Reporting authorization

Compose `AiQueryRoot` (or `AiUsageQueryRoot` separately) and install
`Arc<dyn AiUsageService>`. The ORM implementation also requires an
`AiUsageAccessPolicy` that returns one of:

- `OwnPrincipal`: exact principal kind and subject within the requested exact
  scope;
- `WholeScope`: every fact in the requested exact scope, only after the host
  independently authorizes that administrative view; or
- `Denied`.

This decision is evaluated from the current GraphQL `AuthPrincipal` for every
query. Stored principal/scope values are row partitions and audit dimensions,
not cached authorization. API-token principal kinds remain distinct from human
users.

The `aiUsage`/`AiUsage` field accepts an exact scope, optional provider/model
and creation-time filters, and Relay-style `first/after` or `last/before`
keysets. The default is 50 rows, the hard maximum is 200, and one time filter
requires both bounds and cannot exceed 366 days. Clients should retain only the
visible window.

The public view contains IDs, redacted scope/principal dimensions,
provider/model, token/unit amounts, settled cost, and commit time. It excludes
prompts, responses, transcript blocks, attachments, tool arguments/results,
pricing catalogs, budget ceilings/counters, credentials, and raw provider
payloads.

## GraphQL composition

```rust,no_run
use std::sync::Arc;

use async_graphql::{EmptySubscription, Schema};
use graphql_orm_ai::{
    AiMutationRoot, AiQueryRoot, AiUsageAccessPolicy, AiUsageService,
    OrmAiUsageService,
};

# fn example(
#     database: graphql_orm::db::Database<graphql_orm::graphql::orm::DefaultWriteBackend>,
#     policy: Arc<dyn AiUsageAccessPolicy>,
# ) {
let usage = Arc::new(OrmAiUsageService::new(database, policy));
let schema = Schema::build(AiQueryRoot, AiMutationRoot, EmptySubscription)
    .data(usage as Arc<dyn AiUsageService>)
    .finish();
# let _ = schema;
# }
```

The service's presence is not authorization. A missing service, missing
principal, denied policy, malformed scope, invalid filter, stale cursor, or ORM
failure closes the query.

## Migration, backup, and restore

Apply schema module `0.17.0` with workers and readers closed. Unsupported legacy
private usage rows must be proven from complete committed reservation evidence
or removed; never fabricate a binding. Existing committed reservations are not
automatically treated as historical usage.

Backups preserve usage facts as append-only records. Before reopening after a
restore, validate unique reservation linkage, committed reservation state,
matching scope/principal/provider/model, non-negative amounts, and cached input
not exceeding total input. Set
`AiRestoreSnapshotFacts::invalid_usage_fact_count` to the number of failures;
any nonzero value is fatal. Reporting must remain closed until restore
reconciliation succeeds.
