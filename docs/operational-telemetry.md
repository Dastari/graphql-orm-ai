# Operational telemetry

`graphql-orm-ai` supplies a typed operational observation vocabulary and a
deployment-owned sink. It does not select an SDK, collector, transport, sampling
rule, storage destination, or network exporter.

The contract covers:

- provider call start/finish, provider family, outcome, duration, and
  authoritative successful token counts;
- committed durable run transitions;
- application/internal tool start/finish, reviewed operation class, maturity,
  outcome, and duration;
- bounded expired-run recovery aggregates;
- bounded retention aggregates; and
- restore-plan and start-readiness aggregates.

## Content boundary

The Rust event types have no field capable of carrying prompts, system
instructions, model output, tool definitions, tool arguments/results, GraphQL
documents, arbitrary error text, principal references, durable session/run/tool
IDs, model/profile names, provider response IDs, endpoints, credential/secret
references, restore issue/resource text, fingerprints, or retention cursors.

OpenTelemetry identifies input/output messages, system instructions, tool
arguments, and tool results as potentially sensitive. This crate therefore
does not expose an opt-in content mode. A host exporter must not enrich the
typed events with content from adjacent runtime values. See the current
[OpenTelemetry GenAI attribute registry](https://opentelemetry.io/docs/specs/semconv/registry/attributes/gen-ai/).

## Installing a sink

```rust
use std::sync::Arc;

use graphql_orm_ai::{
    AiOperationalTelemetry, AiOperationalTelemetryEvent,
    AiOperationalTelemetrySink,
};

struct QueueSink;

impl AiOperationalTelemetrySink for QueueSink {
    fn record(&self, event: AiOperationalTelemetryEvent) {
        // Enqueue into a deployment-owned bounded channel. Do not perform
        // network I/O or wait for an exporter here.
        let _ = event;
    }
}

let telemetry = AiOperationalTelemetry::new(Arc::new(QueueSink));
```

The sink method is synchronous and infallible by design. Exporter availability
must not alter authoritative provider, tool, recovery, retention, or restore
state. Implementations should enqueue and return promptly, and may drop events
under bounded backpressure. Use `NoopAiOperationalTelemetrySink` when an
explicit disabled sink is useful. Telemetry event, phase, and outcome enums are
non-exhaustive; exporter matches must retain a safe unknown branch as the
versioned vocabulary grows.

## OpenTelemetry mapping

Provider observations expose `otel_operation_name()` as `chat` and
`otel_provider_name()` only for provider families with a well-known current
value. Compatible profiles, Ollama, and installed harnesses return no provider
mapping; a deployment may add a reviewed immutable mapping without exporting
an endpoint or arbitrary profile name.

Create a fresh `AiTelemetryOperationId` for each measured operation and reuse it
only to correlate that operation's start/finish events or spans. It is random
and not derived from durable runtime identity. It is still high-cardinality:
never attach it to metrics. Durations belong on completed observations and map
naturally to spans/histograms; state transitions and bounded pass summaries map
to span events or structured logs. The crate's stable event names are available
through `AiOperationalTelemetryEvent::event_name()`.

Provider model names are deliberately absent. If a deployment chooses to add
`gen_ai.request.model` or `gen_ai.response.model`, it owns a bounded reviewed
model registry, cardinality policy, and classification review. This extension
must not weaken the content boundary above.

## Proof boundary

Telemetry is never authorization, audit, budget evidence, an erasure
certificate, recovery authority, or restore/start-gate evidence. Emit an event
only after the corresponding authoritative operation reaches the represented
boundary. Dropped, duplicated, delayed, or reordered observations must be safe.
