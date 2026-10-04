# StreamRune Integration API

Shared integration implementations used by all framework integrations: the Micrometer-backed `StreamRuneMetrics`, the `SseAuthorizer` SPI, `AuthenticatedUserResolver`/`RequestIdentityPolicy`/`RequestEdgeAuthority` for request-edge identity (`RequestIdentityPolicy` is the one rule — authenticated principal, trusted gateway or anonymous — by which an integration derives a request's user), the `RelaxedBoolean` parser for `streamrune.*` boolean knobs, and the startup validators the auto-configurations share (`CommandInboxWiringValidator`, `SagaRetentionValidator`).

## Overview

This module has no framework-specific dependencies (it depends on `streamrune-core` and `micrometer-core`). Each framework integration (Spring, Quarkus, Micronaut) defines its own native configuration properties and adapts health reporting to its own health API; this module only holds the pieces that are genuinely shared.

## Installation

```groovy
// build.gradle
implementation("org.streamrune:streamrune-integration-api:1.0.0-alpha-SNAPSHOT")
```

```kotlin
// build.gradle.kts
implementation("org.streamrune:streamrune-integration-api:1.0.0-alpha-SNAPSHOT")
```

> `1.0.0-alpha-SNAPSHOT` is an unreleased preview, published only to the Maven Central snapshot
> repository: add that repository as shown in [Preview builds](../../README.md#preview-builds). See
> the [CHANGELOG](../../CHANGELOG.md) for what the preview contains.

## Key Components

### MicrometerStreamRuneMetrics

Micrometer-backed metrics implementation, built with
`MicrometerStreamRuneMetrics.builder().registry(meterRegistry).prefix("streamrune").build()` (the
prefix defaults to `streamrune`). It registers every meter eagerly at construction, so each is
discoverable before its first sample. Every meter name is derived from a constant in
`org.streamrune.core.metrics.MetricNames` (rebased onto the configured prefix), so the documented
contract and the emitted series cannot drift. The core meters:

| Meter | Type | Description |
|---|---|---|
| `streamrune.commands.dispatched` | Counter | Total commands dispatched |
| `streamrune.commands.succeeded` | Counter | Commands that completed successfully (including no-op results and idempotent replays) |
| `streamrune.commands.failed` | Counter | Commands that failed |
| `streamrune.commands.duration` | Timer | Command execution time |
| `streamrune.events.appended` | Counter | Events appended to store |
| `streamrune.events.replayed` | Counter | Events folded into aggregate state on load (the events after the snapshot, or the whole stream without one) |
| `streamrune.events.duration` | Timer | Event-store append latency |
| `streamrune.projections.processed` | Counter | Events processed by projections |
| `streamrune.projections.failed` | Counter | Projection failures |
| `streamrune.projections.duration` | Timer | Projection processing time |
| `streamrune.subscriptions.events.received` | Counter | Events received by subscriptions |
| `streamrune.subscriptions.delivery.latency` | Timer | Event delivery latency |

`MetricNames` is the complete list — it also defines the command retry/short-circuit, DLQ, lock,
snapshot, crypto, GDPR, query, saga, inbox and outbox meters. Each component records its meters once
a `StreamRuneMetrics` is passed to its builder's `metrics(...)`:

- `VirtualThreadCommandBus` records the command meters and `events.appended`, `events.replayed` and
  `events.duration`.
- `ContinuousProjectionRunner`, `PollingProjectionRunner` and `ScheduledProjectionRunner` record
  `projections.*`.
- `PollingEventSubscription` and `StreamEventSubscription` record `subscriptions.events.received`
  and `subscriptions.delivery.latency`.

The Spring, Quarkus and Micronaut integrations pass their `StreamRuneMetrics` bean to the command bus
and the projection runners they build.

## Requirements

- Java 25
- Micrometer (`micrometer-core`, a runtime dependency of this module; pass the application's `MeterRegistry` to `MicrometerStreamRuneMetrics`)

## Best Practices

- **Use `StreamRuneMetrics.NOOP` when you do not collect metrics** — a component built without `metrics(...)` records nothing.

## Known Limitations

- **One tag-key set per meter name** — an entry point that has no dimension value (for example `recordCommandDispatched()` without a command type) reports the tag as `unknown` rather than emitting an untagged series, because a Prometheus registry keeps only the first label set it sees for a metric family.
