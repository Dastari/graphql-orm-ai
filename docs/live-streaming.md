# Protected Live Streaming

Provider output can be exposed as bounded provisional session events without
turning raw transport frames into an unaudited client channel. This path is
optional. Without an `AiLiveDeltaSink`, provider execution retains its normal
bounded result and only the final protected assistant message becomes durable.

## Data path

The built-in path is deliberately ordered:

1. the normalized provider adapter yields a visible text or visible reasoning-
   summary event;
2. `AiLiveDeltaCoalescer` creates UTF-8-safe batches within the deployment
   limits, which may be no weaker than 50 milliseconds or 4 KiB;
3. `OrmAiLiveDeltaService` rehydrates the current principal and checks current
   scope and session write access;
4. it resolves the current ready content-protection policy, protects the batch,
   then rehydrates and resolves policy again to reject asynchronous drift;
5. one state-machine transaction validates the exact active run, worker,
   attempt, generation, unexpired lease, provider/model, uncertain budget
   reservation, active session owner, and scope;
6. that transaction advances the session cursor, inserts a protected
   `provider_live_delta` event, and queues the commit-only wakeup; and
7. an authorized subscription wakes after commit and re-reads the event through
   the ordinary protected cursor window.

The sink awaits each batch sequentially. This is intentional backpressure:
client-visible durability cannot fall behind an unbounded provider stream.
The sink transaction does not renew or rotate the worker lease. Coordinator
heartbeats remain authoritative, while state-machine transaction isolation
serializes each fence check with recovery and other run transitions.

## Enabling the sink

Construct the ORM sink from the same runtime and run service used by the worker,
then install it explicitly on the provider executor:

```rust,no_run
# use std::sync::Arc;
# use agql_auth::{Clock, SystemClock};
# use graphql_orm_ai::{AiLiveDeltaCoalescerLimits, AiLiveDeltaPersistenceLimits,
#     AiProviderCallExecutor, OrmAiLiveDeltaService};
# use time::Duration;
# fn configure(
#     executor: AiProviderCallExecutor,
#     run_service: graphql_orm_ai::OrmAiRunService,
#     runtime: Arc<graphql_orm_ai::AiRuntime>,
# ) -> Result<AiProviderCallExecutor, graphql_orm_ai::AiError> {
let persistence_limits =
    AiLiveDeltaPersistenceLimits::new(4_096, Duration::seconds(30))?;
let sink = Arc::new(OrmAiLiveDeltaService::new(
    run_service,
    runtime,
    Arc::new(SystemClock) as Arc<dyn Clock>,
    persistence_limits,
));

let executor = executor.with_live_delta_sink(
    sink,
    AiLiveDeltaCoalescerLimits::default(),
);
# Ok(executor)
# }
```

The coalescer and persistence byte limits should agree. A stricter persistence
limit is allowed but will fail closed if a generated batch exceeds it.

## Durable event contract

The protected payload currently has `formatVersion: 1` and includes:

- `provisional: true`;
- exact run, attempt, lease generation, and batch sequence;
- provider kind, model, and optional response reference;
- the exact uncertain budget reservation ID;
- visible kind (`text` or `reasoning_summary`), text, and UTF-8 byte count.

The containing session event supplies the authoritative monotonic cursor,
correlation ID, run ID, and causation link. Consumers fetch it only through an
authorized session-event page or subscription. Protected persistence does not
make a batch public and does not authorize any provider or third-party egress.

The path structurally excludes tool arguments, structured tool events, raw
provider frames, hidden chain-of-thought, credentials, and arbitrary model
metadata. A custom `AiLiveDeltaSink` has the same security obligations and must
not weaken these exclusions.

## Client reconciliation

`provider_live_delta` is progress, not a final message. A client may render its
text in a virtualized transient view keyed by run, attempt, and batch sequence.
When `assistant_message_completed` arrives, replace the provisional rendering
with the authoritative windowed message and blocks. Do not append both copies.

If the run becomes `RecoveryRequired`, retained provisional events describe an
incomplete historical attempt. They must not be relabeled as a complete answer.
Cursor pagination remains stable, so a client can discard old DOM nodes and
re-fetch a bounded window without holding the full session in memory.

## Failure and recovery

Once transport begins, the exact budget reservation is uncertain. Protection,
authorization, fencing, or persistence failure returns an error and leaves it
uncertain for privileged reconciliation. The worker must not replay that
provider call. A stale worker cannot append after lease expiry or recovery
because the durable transaction re-reads the exact current fence.

The host-only `OrmAiSessionRetentionService` now deletes expired
`provider_live_delta` rows in bounded per-session transactions under the exact
current GraphQL-managed scope policy. It never rewinds or reuses sequence
values. A later replay that crosses a removed sequence returns
`reset_required`, so the client discards provisional rendering and reloads its
bounded authoritative windows. Stable lifecycle/completion events are not
deleted by this worker. See the [session-retention guide](session-retention.md).
