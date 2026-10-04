# Snapshot Versioning

## When to use

Use snapshots when your aggregate accumulates many events (hundreds or more) and replay time becomes a bottleneck.

Use `snapshotVersion` (the second `SnapshotPolicy` parameter) when your aggregate state class structure changes — incrementing it causes old snapshots to be ignored and the aggregate is rebuilt from events automatically.

Use `SnapshotMigration` instead of ignoring old snapshots when you want to migrate them without full replay. Register your migrations at startup and a version-mismatched snapshot is upgraded on load rather than discarded.

> **`SnapshotMigration` is wired into both shipped stores.** `org.streamrune.core.SnapshotMigration` is a core interface, applied on load by `PostgresEventStore` and `InMemoryEventStore`. The core `EventStore` interface itself defines no registration method (the registration point is store-specific); register migrations through the store's factory/builder — `PostgresEventStoreFactory.snapshotMigrations(...)` (or `PostgresEventStore.Builder.migrations(...)`), and `InMemoryEventStore.withMigrations(...)` for tests. When a stored snapshot's schema version differs from the version the command bus expects, a **complete** migration chain upgrades it in place (only events after the snapshot then replay); when no complete chain exists the snapshot is discarded and all events are replayed (see [How it works](#how-it-works)) — the same safe fallback as a plain version bump.

Do **not** use snapshots for aggregates with fewer than ~50 events.

Do **not** use snapshots as a substitute for projections.

## How it works

`SnapshotPolicy` is a sealed interface with two implementations: `EveryNEvents(int n, int snapshotVersion)` and `Never`.

`VirtualThreadCommandBus` checks the policy after every successful `EventStore.append()`. If `eventsSinceSnapshot >= n`, it calls `EventStore.saveSnapshot(streamId, version, state, snapshotVersion)`.

A snapshot belongs to one typed stream: `snapshot_store` is keyed by the two columns `aggregate_type` and `aggregate_id`, so two aggregate types that share an id value keep separate snapshots.

On load, `EventStore.load(streamId, expectedSnapshotVersion)` returns an `AggregateHistory` including the snapshot state (if any) and only events after it.

If the stored snapshot's `snapshotVersion` does not match `expectedSnapshotVersion`, the store first tries to **migrate** the snapshot up to the expected version using the registered `SnapshotMigration` chain. If a complete chain from the stored version to the expected version exists, the snapshot is transformed and used (only events after the snapshot replay). If no complete chain exists — no migrations were registered, or the chain has a gap — the snapshot is discarded and all events are replayed. Discard-and-replay is the safe fallback; migration is the faster path when a chain is available.

`SnapshotMigration` provides that cheaper upgrade path: supply `fromVersion()`, `toVersion()`, and `migrate(AggregateState)` steps to transform old snapshots rather than discard them. Steps are applied in ascending `fromVersion()` order. The store deserializes the stored snapshot against the **old** (stored) state class the type registry resolves — which differs from the current class — then chains the migrations up to the current class. Keep old state classes registered in the `EventTypeRegistry` for as long as snapshots at their version may still exist: if the stored type is no longer registered, its snapshot is undeserializable and is discarded (full replay) rather than causing an error.

## Quick example

```java
// Snapshot every 50 events, schema version 1
SnapshotPolicy policy = SnapshotPolicy.everyNEvents(50);

// After a state class change, bump to version 2
SnapshotPolicy policy2 = SnapshotPolicy.everyNEvents(50, 2);

// Never snapshot
SnapshotPolicy never = SnapshotPolicy.never();

// Register policy with the command bus
VirtualThreadCommandBus bus = VirtualThreadCommandBus.builder()
    .eventStore(eventStore)
    .locker(locker)
    .snapshotPolicy(SnapshotPolicy.everyNEvents(100, 1))
    .build();
```

## Full example

Scenario: `UserState` gains an `age` field in schema version 2.

```java
// Original state (schema version 1)
public record UserState(String name) implements AggregateState {}

// New state (schema version 2)
public record UserStateV2(String name, int age) implements AggregateState {}

// Migration implementation
public class AddAgeFieldMigration implements SnapshotMigration {
    @Override public int fromVersion() { return 1; }
    @Override public int toVersion()   { return 2; }

    @Override
    public AggregateState migrate(AggregateState state) {
        UserState v1 = (UserState) state;
        return new UserStateV2(v1.name(), 0); // default age = 0
    }
}
```

Register the migration at startup, on the store's factory (the registration point), and bump `snapshotVersion` to 2 on the policy. Keep **both** state classes registered in the `EventTypeRegistry` — the old `UserState` so an existing v1 snapshot can be deserialized against it, and the new `UserStateV2` for the migrated result:

```java
var typeRegistry = SimpleEventTypeRegistry.builder()
    // ... event registrations ...
    .registerState("UserState",   UserState.class)   // old — still needed to read v1 snapshots
    .registerState("UserStateV2", UserStateV2.class) // new
    .build();

EventStore store = new PostgresEventStoreFactory(dataSource, typeRegistry)
    .snapshotMigrations(List.of(new AddAgeFieldMigration()))
    .create();

// Bump the snapshot version so the command bus expects v2.
SnapshotPolicy policy = SnapshotPolicy.everyNEvents(100, 2);
```

On load, a stored v1 `UserState` snapshot is deserialized against `UserState`, run through `AddAgeFieldMigration.migrate(...)`, and returned as a `UserStateV2` — only the events after the snapshot are replayed. If the `AddAgeFieldMigration` were not registered (or the chain had a gap before reaching v2), the v1 snapshot would instead be discarded and `UserStateV2` rebuilt from events.

For multi-step upgrades (v1 → v2 → v3), register one migration per step (`1→2` and `2→3`); the store applies them in ascending order. Every step in the chain from the stored version to the expected version must be present — a gap (e.g. `2→3` missing when going from v1 to v3) makes the whole snapshot unmigratable and it is discarded, never partially migrated.

The in-memory test double takes the same migrations via `InMemoryEventStore.withMigrations(List.of(new AddAgeFieldMigration()))`, so migration behavior can be unit-tested without a database.

## Configuration

| Framework | Property | Default |
|---|---|---|
| Spring | `streamrune.snapshot-every-n-events` | 100 |
| Quarkus | `streamrune.snapshot-every-n-events` | 100 |
| Micronaut | `streamrune.snapshot-every-n-events` | 100 |

`snapshotVersion` is set programmatically — not a configuration property, because it must match the application's current state class schema and therefore belongs next to that code, versioned and released with it.

**On the framework integrations, that means supplying a `SnapshotPolicy` bean.** The property above only feeds the *default* policy (`everyNEvents(n)` at snapshot schema version 1). Define a `SnapshotPolicy` bean and it replaces the default wholesale — this is the only way to bump the schema version or to select `never()`, and it is what makes a registered `SnapshotMigration` reachable at all (the store's migrate-or-discard branch is entered only when the expected version differs from the stored one):

| Framework | How |
|---|---|
| Spring | `@Bean SnapshotPolicy snapshotPolicy() { return SnapshotPolicy.everyNEvents(100, 2); }` |
| Quarkus | `@Produces @Singleton SnapshotPolicy snapshotPolicy() { ... }` |
| Micronaut | `@Singleton SnapshotPolicy snapshotPolicy() { ... }` (on a `@Factory`) |

Bump the bean's `snapshotVersion` in the same commit that changes the state class, exactly as you would the migration that carries it forward.

Flyway: `snapshot_store`, with its `snapshot_version` column (default 1), is part of the baseline schema shipped in `streamrune-postgres` (`V001__streamrune_baseline.sql`).

## Caveats

- When you increment `snapshotVersion` **without** a registered migration chain that reaches the new version, old snapshots are silently discarded and all events are replayed. Safe but may be slow for large aggregates. Register a `SnapshotMigration` chain to avoid the replay.
- Register migrations at startup — before the application begins serving commands — through the store's registration point: `PostgresEventStoreFactory.snapshotMigrations(...)` / `PostgresEventStore.Builder.migrations(...)`, or `InMemoryEventStore.withMigrations(...)` in tests. The core `EventStore` interface deliberately defines no registration method; it is store-specific. Registration validates the chain eagerly — a duplicate `fromVersion`, or a step whose `toVersion` is not greater than its `fromVersion`, throws at construction.
- A gapped chain is never applied partially. If the steps from the stored version to the expected version are incomplete (a missing step, or a step that overshoots the expected version), the snapshot is discarded and replayed from events — exactly as if no migration existed. This keeps a half-migrated state from ever being loaded.
- Keep old state classes registered in the `EventTypeRegistry` while snapshots at their version may still exist. The stored snapshot is deserialized against its **stored** (old) type before migration; if that type is no longer registered, the snapshot is undeserializable and is discarded (full replay) rather than raising an error.
- `SnapshotPolicy` is applied globally across all aggregates managed by a `VirtualThreadCommandBus`. Different aggregate types needing different intervals require separate command bus instances.
- `snapshotVersion` must be >= 1. Using 0 causes an `IllegalArgumentException`.
- Switching an actively-snapshotted aggregate to `SnapshotPolicy.never()` does not delete its
  existing rows in `snapshot_store` — they are simply never read again (`never()` is "off" on
  the read side too — the bus loads every stream with `EventStore.IGNORE_SNAPSHOT` — not merely
  "stop writing new ones") and never overwritten.
  The rows are harmless but permanent; if you want them gone, purge them yourself (`DELETE
  FROM snapshot_store WHERE aggregate_type = ... AND aggregate_id = ...`). `PostgresEventStore` logs one WARN per process
  the first time it notices a leftover row under a `never()` policy, as a nudge, not an error.
- `SnapshotPolicy.never()` is the `VirtualThreadCommandBus` builder's default and the safe starting point — adding snapshots later is always possible. (The framework integrations default to `everyNEvents(streamrune.snapshot-every-n-events)` instead; see [Configuration](#configuration).)
