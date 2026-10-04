# Audit Logging

## When to use

Use audit logging for compliance requirements (SOC 2, GDPR, HIPAA) where you must record who did what and when.

Use it for operational observability: audit logs show which commands succeeded and failed, how many events they produced, and error messages for failures.

Do **not** use the audit log as a substitute for your event store — the audit log records commands, not domain events.

Do **not** use `AuditStore` for application logging — use SLF4J / Logback for application logs.

## How it works

`AuditCommandInterceptor` is a `CommandInterceptor` that writes an `AuditEntry` after every command — both successful and failed.

In `before(ctx)`: captures the `UserId` and the `CorrelationId` from `StreamRuneContext.CURRENT` onto a per-thread stack (a `ThreadLocal`), so a nested command executed on the same thread — e.g. from an inline projection's `after()` — cannot overwrite the outer command's capture.

In `after(ctx)`: writes `AuditEntry` with `AuditOutcome.SUCCESS` and `eventCount = ctx.result().events().size()` — **unless the command was vetoed** (a later interceptor's `before()` returned `false`), in which case it writes `AuditOutcome.VETOED` with `errorMessage = "vetoed by <interceptor>"` and `eventCount = 0`. A veto is a rejection — the command never executed and produced no events — so auditing it as `SUCCESS` would be a false record. An idempotent replay is still audited `SUCCESS` (its events were persisted by the original keyed execution).

In `onError(ctx, error)`: writes `AuditEntry` with `AuditOutcome.FAILURE`, `errorMessage = LogSanitizer.sanitizeFreeText(error.getMessage())`, and `eventCount = 0`. The veto text is sanitized the same way (see the notes below).

`AuditEntry` fields: `commandId` (`CommandId`), `commandType` (simple class name), `aggregateType` (`AggregateType`, nullable — `null` for query-origin and subject entries), `aggregateId` (`AggregateId`), `userId` (`UserId`, nullable), `occurredAt` (`Instant`), `outcome` (`AuditOutcome`), `errorMessage` (nullable), `eventCount` (int), `correlationId` (`CorrelationId`, nullable — ties the row to the originating request flow).

`AuditStore` is append-only (`void save(AuditEntry entry)`).

**Auto-registration:** in all three frameworks, if a `DataSource` bean is present, a `PostgresAuditStore` is auto-registered. If an `AuditStore` bean is present, `AuditCommandInterceptor` is auto-registered.

## Quick example

```java
// Custom in-memory AuditStore for tests:
public class InMemoryAuditStore implements AuditStore {
    private final List<AuditEntry> entries = new CopyOnWriteArrayList<>();

    @Override
    public void save(AuditEntry entry) {
        entries.add(entry);
    }

    public List<AuditEntry> all() { return List.copyOf(entries); }
}
```

Query the `audit_log` table directly for reads (AuditStore is write-only):

```sql
SELECT * FROM audit_log WHERE aggregate_type = ? AND aggregate_id = ? ORDER BY occurred_at DESC
```

The query is served by `idx_audit_aggregate`. `AuditEntry.aggregateType()` is `null` for query-origin and subject entries, which have no registered aggregate type.

## Full example

```java
AuditStore auditStore = new PostgresAuditStore(dataSource);
AuditCommandInterceptor auditInterceptor = new AuditCommandInterceptor(auditStore);

VirtualThreadCommandBus commandBus = VirtualThreadCommandBus.builder()
    .eventStore(eventStore)
    .locker(locker)
    .interceptors(auditInterceptor)
    .build();
```

After a command runs, the audit log contains:

```
AuditEntry(
  commandType="PlaceOrder",
  aggregateType="order",
  aggregateId="order-123",
  userId="user-456",
  occurredAt=Instant,
  outcome=AuditOutcome.SUCCESS,
  eventCount=2
)
```

For failures:

```
AuditEntry(
  outcome=AuditOutcome.FAILURE,
  errorMessage="Insufficient stock",
  eventCount=0
)
```

For a vetoed (rejected) command:

```
AuditEntry(
  outcome=AuditOutcome.VETOED,
  errorMessage="vetoed by MaintenanceModeInterceptor",
  eventCount=0
)
```

Query patterns:

```sql
-- All commands by user
SELECT * FROM audit_log WHERE user_id = 'user-456';

-- Failed commands in last hour
SELECT * FROM audit_log
WHERE outcome = 'FAILURE' AND occurred_at > NOW() - INTERVAL '1 hour';

-- Event production rate by command type
SELECT command_type, SUM(event_count) FROM audit_log GROUP BY command_type;
```

## Configuration

No configuration properties — audit behavior is always-on when an `AuditStore` bean is present.

| Framework | DataSource present? | AuditStore created | AuditCommandInterceptor registered |
|---|---|---|---|
| Spring | Yes | `PostgresAuditStore` (auto) | Yes |
| Spring | No | None | No |
| Quarkus | Yes | `PostgresAuditStore` (auto) | Yes |
| Micronaut | Yes | `PostgresAuditStore` (auto) | Yes |

To disable while keeping `DataSource`:

```java
@Bean
public AuditStore noOpAuditStore() {
    return entry -> {}; // discard
}
```

Flyway: `audit_log` is part of the baseline schema shipped in `streamrune-postgres` (`V001__streamrune_baseline.sql`).

## Caveats

- `AuditCommandInterceptor` uses a `ThreadLocal` stack for the `userId`/`correlationId` capture. Requires that `before()`, `after()`/`onError()` are called on the same thread — `VirtualThreadCommandBus` guarantees this.
- `AuditStore` is append-only by interface contract — do not add `delete()`. For GDPR right to erasure, implement deletion outside `AuditStore` directly on the underlying table.
- `AuditEntry.eventCount` is always 0 for `FAILURE` and `VETOED` outcomes (the record's constructor rejects anything else).
- `userId` in `AuditEntry` is a `UserId`, nullable. `null` means anonymous command.
- `aggregateType` and `aggregateId` are stored in two columns, `aggregate_type` and `aggregate_id`; no column holds a composed id. `aggregateId` is stored verbatim: it is the command's target, not free text. A client-supplied id that carries a control character or is longer than 255 characters is refused before the audit interceptor runs, when your id extractor builds it with `AggregateId.of` (see [Getting started](../getting-started.md)). An id an application builds with the `AggregateId` constructor skips that check and is stored as given.
- An audit write failure never changes the command's outcome: `VirtualThreadCommandBus` logs an exception thrown from an interceptor's `after()`/`onError()` at ERROR and swallows it — so a failed `AuditStore.save()` loses that audit row, with only the ERROR log as evidence. If a complete audit trail is a compliance requirement, make `AuditStore.save()` robust and alert on that log.
- `errorMessage` is the failure's `getMessage()`, stored after `LogSanitizer.sanitizeFreeText` has processed it. Each run of control characters (C0, DEL, C1) and Unicode line or paragraph separators becomes one space. Invisible format characters and unpaired surrogates are removed. Text longer than 2048 characters is cut there and ends with `...`. An exception message can carry caller-supplied text, such as an idempotency key that a key-reuse rejection names. Stored as thrown, a CR/LF in it forges a line for every reader of `audit_log`, and a NUL makes the PostgreSQL INSERT fail, so the FAILURE row is lost. `AuditingQueryBus` and the GDPR services store their messages the same way. So does every other error-text column the framework writes: `dead_letter_queue.error_message`, `saga_dead_letters.error_message`, `projection_dead_letters.error_message` and `outbox_events.last_error`. Only the stored copy changes; the exception the caller receives does not. Do not put personal data or secrets in exception messages of commands you audit.
