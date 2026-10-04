# Command idempotency

## When to use

Use command idempotency when a command may be delivered more than once and you need it to *apply* at most once. Command delivery is at-least-once in practice:

- **HTTP clients retry.** A browser, mobile app, or upstream service that times out on a slow `POST` will re-send the same request — often the same logical "place order" twice.
- **Message-driven flows redeliver.** A command triggered off a broker or an event subscription (a saga dispatching commands, an integration consuming a queue) inherits the broker's at-least-once semantics.
- **The dead-letter retry runner re-dispatches.** `DeadLetterRetryRunner` re-runs a failed command; without a key it would re-append events on a partial success.

Idempotency makes those redeliveries safe: the first execution runs the decider and appends events; every later execution under the **same** `IdempotencyKey` returns the recorded result of the first, without re-running the handler or re-persisting events. This is *effectively-once* execution on top of at-least-once delivery.

Do **not** reach for idempotency keys to make a *non-deterministic* command safe — the key only deduplicates identical logical operations. Two genuinely different commands must use two different keys.

Do **not** use it as a substitute for optimistic concurrency (`expectedVersion` on the event store) — idempotency dedupes *redeliveries of one command*; the version check protects against *concurrent conflicting commands*.

## How it works

The keyed overload lives on the framework-agnostic command bus:

```java
default <C extends Command> CommandResult execute(C command, IdempotencyKey key);
```

`CommandBus.execute(command, key)` records the key atomically with the appended events, inside the same event-store transaction (`EventStore.appendWithKey`). The read and prune sides of that record live behind the `CommandInbox` SPI; the write happens in the append transaction, so a key is only ever committed together with the events it produced — there is no window in which a key is recorded but its events are not, or vice versa.

On a **cache miss** (key never seen), the bus loads state, runs `Decider.decide`, and commits the inbox row plus the events together. On a **cache hit** (key already recorded), the bus skips the handler entirely and reconstructs the prior `CommandResult` from the inbox row. A replayed result reports `idempotentReplay() == true`, so callers can distinguish a fresh execution from a deduplicated redelivery:

```java
CommandBus.CommandResult r = bus.execute(new CreateOrder("o-1", "c-1"),
                                         IdempotencyKey.of("order-create-o-1"));
if (r.idempotentReplay()) {
    // redelivery: no new events were appended; r carries the original outcome
}
StreamId stream = r.streamId();            // order:o-1
Version   v     = r.finalVersion();
```

> **An idempotent replay is a success, not a rejection.** `shortCircuited()` is `true` for *two unrelated* outcomes — an interceptor **veto** (`vetoed()`, a rejection: nothing ran) and an **idempotent replay** (`idempotentReplay()`, a success: the events were persisted by the prior execution). Branch on `CommandResult.reason()` (or the `vetoed()` / `idempotentReplay()` predicates), **not** on `shortCircuited()` alone: mapping every short-circuit onto an error would misreport an at-least-once redelivery as a `403`/`409`. Only `vetoed()` is a rejection.

**A key is bound to its command *and* its typed stream.** A recorded key may only be re-presented by the same command type targeting the same aggregate — the same aggregate type *and* the same aggregate id. A key derived from an id value shared by two aggregate types is one key: a second command presenting it for the other type is refused as a reuse. Any other combination throws `IllegalArgumentException` instead of returning the stored result, so a reused key can never hand back another command's — or another aggregate's — `streamId`, `finalVersion` and offsets. Both discriminators are checked identically on **every** path that can observe a recorded key — the three bus lookups described next and `EventStore.appendWithKey`'s claim-loser branch. The claim-loser branch is not redundant: two *concurrent* executions of one key can miss every lookup (neither inbox row is committed yet), so it is the last place their collision can be caught, and it must reach the same verdict a sequential collision does. Reuse a key only for a genuine redelivery of the same logical command.

**A duplicate that overlaps its original is answered from the inbox too.** The pre-execution lookup is only the fast path for a redelivery whose original has already committed; it runs outside the aggregate lock, so two dispatches of one key that overlap both miss it. The bus therefore looks the key up again **under the aggregate lock**, before loading state, on every attempt of its retry loop: a duplicate serialized behind the original's lock — a second replica behind `PgAdvisoryLocker`, a second thread in one JVM — returns the recorded outcome the moment it acquires the lock, without running the decider. If the lock serializes nothing across processes (`LocalStripedLocker` on two replicas) and a keyed `Decider.guard`/`decide` throws anyway, the bus consults the inbox before the failure propagates: a recorded key means the decider rejected the duplicate *because the original already applied it*, and the caller receives the original's `IDEMPOTENT_REPLAY`; an unrecorded key lets the rejection propagate as the business error it is; a lookup that itself fails propagates *that* failure, with the rejection attached as a suppressed exception, because the outcome is unknown — and it is never dead-lettered, since a replay could only repeat the recorded outcome or re-run the rejected decision. A keyed duplicate is therefore never answered by re-running the decider against state its original already changed — which is what allows a saga to classify a forward command's business rejection as permanent (see [saga.md — Concurrency](saga.md#concurrency)).

> **The key namespace is caller-owned and trusted — scope it yourself.** For one command type on one stream, an idempotency key behaves as a **bearer token for the recorded result**: whoever presents it receives that execution's outcome, and their own command is deduplicated away rather than executed. That is the whole point on a redelivery — and a hazard if your keys come from untrusted input. The framework deliberately does **not** bind the inbox row to an ambient caller identity: it never sees your tenant model, and the same logical command legitimately reaches the bus under different (or absent) request contexts — the saga timeout runner and the correlated-event path share one compensation key by design, across threads that do not share a principal, so identity-scoping would either re-execute a compensation twice or reject it forever. **If your keys are derived from client input, put the authenticated principal or tenant in the key** with `IdempotencyKey.scopedTo(principal, clientKey)`. Note that authorization is unaffected either way: interceptors run on every call including a replay (see above), so a replay is authorized exactly like a first execution.

### The keyed overload is opt-in — probe before you call

On a bus with no inbox configured, the keyed `execute(command, key)` throws instead of executing: the shipped `VirtualThreadCommandBus` — the bus every integration produces — throws `IllegalStateException`, and the interface's default implementation (inherited by a custom `CommandBus` that does not override it) throws `UnsupportedOperationException`. Before choosing the keyed path, check the capability probe:

```java
boolean supportsIdempotentExecution();
```

It returns `true` only once a `CommandInbox` has been wired into the bus (see [Auto-wiring](#auto-wiring)). Callers that need to choose between keyed and unkeyed execution (as `DeadLetterRetryRunner` does when replaying a dead-lettered command) **must** check this up front rather than attempting the keyed call and catching an exception — the "no inbox" failure mode is implementation-specific, and catching by exception type risks mistaking an unrelated failure (e.g. a decider that itself throws `UnsupportedOperationException`) for "keyed execution is unsupported."

```java
if (bus.supportsIdempotentExecution()) {
    bus.execute(cmd, key);
} else {
    bus.execute(cmd);        // no inbox: fall back to at-least-once
}
```

## The IdempotencyKey

`IdempotencyKey` is a value type wrapping a non-blank String of at most **512 characters**; construct it with the static factory:

```java
IdempotencyKey key = IdempotencyKey.of("order-create-o-1");
```

`IdempotencyKey.of(...)` throws `IllegalArgumentException` for a blank string, one longer than 512 characters, or one containing a control character — C0 (`U+0000`–`U+001F`, which includes CR, LF and TAB), DEL (`U+007F`) or C1 (`U+0080`–`U+009F`). The message does not echo the rejected value. `of` and `scopedTo` are the *ingress* factories: call them with a value you just read from a request. The record's canonical constructor keeps only the blank and 512-character rules, because it is the *decode* path. Jackson rebuilds a key through it, the dead-letter queue rebuilds a stored key through it, and the framework derives saga and replay keys through it from ids it reads back from storage, so a charset rule there would make stored data unreadable. Don't call the constructor with client input, and don't declare `IdempotencyKey` itself as a request-body or header-bound field: Jackson binds it through the same lenient constructor, so the control-character rule is skipped. Bind the client token as a `String` and build the key with `of` or `scopedTo`. The key is entirely caller-derived — the framework never generates one for you on the keyed path. Derive it deterministically from whatever *identifies the logical operation* so that a redelivery produces the *same* key. Good sources:

- A client-supplied request id (an `Idempotency-Key` HTTP header, a mobile-generated UUID per user action).
- A business identifier that is unique per intended effect (`"order-create-" + orderId`).

### Scoping a key to its caller

When the key comes from client input, derive it inside a namespace you control so two callers cannot collide:

```java
IdempotencyKey key = IdempotencyKey.scopedTo(tenantId, "order-create-" + requestId);
```

`scopedTo` prefixes the scope unambiguously: the scope must not itself contain the `|` separator (`IllegalArgumentException` if it does), so exactly one parse of `scope|key` exists and no client key can be crafted to impersonate another scope. Both parts are refused when they contain a control character, as `of` refuses one. The check covers the scope as well as the key, because an authenticated principal name is not guaranteed to be charset-safe. It is deterministic — same scope, same key, same result — so redeliveries still dedupe. Use the authenticated principal, the tenant, or whatever isolation boundary your application enforces; use plain `IdempotencyKey.of(...)` when the key is already server-derived and unguessable. Keys the framework derives from ids it reads back from storage (saga and DLQ-replay keys) are built with the constructor instead, because they come through the decode door.

Sagas derive their keys from the triggering event's global offset plus the command's position in the dispatch list, so saga command dispatch is effectively-once for free — see [Saga › Effectively-once dispatch](saga.md#effectively-once-dispatch).

## HTTP controller example

Deriving the key from a client request id makes a `POST` safe to retry. Inject the framework-agnostic `org.streamrune.core.CommandBus` (no integration produces an injectable facade — you inject the bus itself). The header is untrusted, so scope it to the authenticated caller:

```java
@RestController
@RequestMapping("/orders")
class OrderController {

    private final org.streamrune.core.CommandBus bus;

    OrderController(org.streamrune.core.CommandBus bus) {
        this.bus = bus;
    }

    @PostMapping
    ResponseEntity<String> place(@RequestHeader("Idempotency-Key") String requestId,
                                 Principal caller,
                                 @RequestBody CreateOrder cmd) {
        // scopedTo, not of(...): the header is client-supplied, so two callers must not
        // share one key namespace — see "Scoping a key to its caller" above.
        var key = IdempotencyKey.scopedTo(caller.getName(), "order-create-" + requestId);
        var result = bus.execute(cmd, key);
        var status = result.idempotentReplay() ? HttpStatus.OK       // replayed redelivery (success)
                                               : HttpStatus.CREATED;  // first execution
        return ResponseEntity.status(status).body(result.streamId().toString());   // "order:<id>"
    }
}
```

The client sends the same `Idempotency-Key` on every retry of one logical action. The first request creates the order and appends its events; every retry returns the recorded result with `idempotentReplay() == true` and appends nothing. The controller maps that distinction onto `201 Created` vs `200 OK`, but the important guarantee is that no duplicate order is ever created.

The same pattern applies on Quarkus and Micronaut — inject `org.streamrune.core.CommandBus` and read the client's request id from the framework's header abstraction.

## Idempotency at the ingress: a recipe for public command endpoints

The framework owns no HTTP command endpoint, so making idempotency an invariant of your public API is the application's job. The recipe below uses only the API described on this page.

1. **Derive a server-owned composite key.** Build it from the authenticated tenant, the authenticated principal, the command route (or command type) and the client's retry token — never from the client token alone. `IdempotencyKey.scopedTo(scope, key)` refuses a scope containing `|`, so nesting it yields a composite that parses exactly one way (`tenant|principal|route|token`):

   ```java
   static IdempotencyKey ingressKey(String tenantId, String principalId,
                                    String route, String clientToken) {
       // Every scope must be non-blank and free of '|'; every scope and the token must be free of
       // control characters. scopedTo throws IllegalArgumentException otherwise, and when the
       // combined value exceeds 512 characters.
       String byRoute = IdempotencyKey.scopedTo(route, clientToken).value();
       String byPrincipal = IdempotencyKey.scopedTo(principalId, byRoute).value();
       return IdempotencyKey.scopedTo(tenantId, byPrincipal);
   }
   ```

   Take the tenant and the principal from the authenticated identity, not from request headers, and the route from your own routing (`"POST /orders"`) or the command class name. Scoping by route keeps one client token reused on two endpoints from colliding.

2. **Validate the client retry token.** Accept only a bounded, well-formed value (for example a UUID) and answer anything else with a client error before building the key. The factories check only that the value is non-blank, at most 512 characters and free of control characters. That check is a minimum, not a format: map its `IllegalArgumentException` to a `400` like any other malformed token.

3. **Reject a missing token on mutating endpoints**, unless the endpoint is explicitly fire-and-forget. Without a key, a client retry executes the command a second time.

4. **Retain completed keys for the whole redelivery window.** `streamrune.inbox.retention-max-age` (see [Retention](#retention)) must be longer than the maximum client retry horizon plus queue or broker redelivery plus any operator replay — `DeadLetterRetryRunner` re-executes a dead-lettered command under its original key, which only deduplicates while that key is still in the inbox.

5. **Record only a hash of the key in your own audit data and logs.** A key is a bearer token for its recorded result (see above). Know where the framework keeps the raw value: it is the primary key of `command_inbox`, and it is stored with a dead-lettered command in `dead_letter_queue.idempotency_key`. A key-reuse rejection's `IllegalArgumentException` message also names it, and the audit interceptor records that message in `audit_log.error_message` for a failed command. The message renders the key through `LogSanitizer.sanitizeForLog`, so control characters are stripped and only the first 64 characters appear. The audit interceptor stores the message through `LogSanitizer.sanitizeFreeText`, as the framework does for every error text it persists. That still leaves up to 64 characters of the key in the row.

## The CommandInbox SPI

The read and prune sides of the inbox are the `org.streamrune.core.CommandInbox` interface:

```java
public interface CommandInbox {
    Optional<InboxResult> find(IdempotencyKey key);
    int deleteProcessedBefore(Instant cutoff);

    record InboxResult(
        IdempotencyKey key,
        String commandType,
        StreamId streamId,
        Version finalVersion,
        List<GlobalOffset> globalOffsets,
        Instant processedAt) { /* ... */ }
}
```

- `find` is the pre-execution lookup — a present `InboxResult` means the key was already processed and the recorded outcome should be returned instead of running the handler.
- `deleteProcessedBefore` is the prune side, called by the retention sweeper (below).
- `InboxResult` carries exactly enough to reconstruct the prior `CommandResult`: the command type (used to enforce the key-to-command binding), the resulting `StreamId`, the `finalVersion`, and the `GlobalOffset`s of the appended events.

The write side is deliberately *not* on this interface: a new inbox row is committed inside `EventStore.appendWithKey`, atomically with the events, so the `CommandInbox` never exposes an un-transactional write.

The production implementation is **`PostgresCommandInbox`**, backed by the `command_inbox` table (part of the Flyway baseline schema shipped in `streamrune-postgres`, `V001__streamrune_baseline.sql`). An in-memory implementation, `InMemoryCommandInbox`, ships in `streamrune-test` for unit tests.

> **Least-privilege note:** the retention sweeper issues `DELETE FROM command_inbox`, so a least-privilege database role for StreamRune must include `DELETE` on that table (see the [production guide](../production.md) grant list).

## Auto-wiring

Spring, Quarkus, and Micronaut all auto-configure a `PostgresCommandInbox` and wire it into the command bus **when a `DataSource` bean is available** — no explicit setup is required. Once the inbox is wired, `supportsIdempotentExecution()` on the produced bus returns `true`, and the keyed `execute(command, key)` overload runs the effectively-once path.

If no `DataSource` is present, the bus has no inbox: `supportsIdempotentExecution()` stays `false` and the keyed overload throws `IllegalStateException`. Always gate keyed calls on the probe when your deployment might run without a database.

### Supplying your own `CommandInbox`

The inbox is wired into **two** places from **two** injection points: the command bus takes the `CommandInbox` *interface*, while the event-store factory takes the concrete `PostgresCommandInbox` — it has to, because the authoritative claim happens inside `EventStore.appendWithKey`, in the same transaction as the append. The bus's inbox is more than a fast path: the bus consults it before taking the aggregate lock, again under the lock, and once more after the decider rejects a keyed command, and those lookups carry the [overlapping-duplicate guarantee](#how-it-works) above. It must therefore read the very rows `appendWithKey` claims.

A `CommandInbox` bean that is **not** a `PostgresCommandInbox` therefore only half-overrides the framework: the bus would use it, the framework's PostgreSQL event store could not. All three integrations **fail startup** on that composition (`IllegalStateException`, naming this remedy) rather than booting green and breaking every keyed execution — including the saga executor's command dispatch — at runtime.

Two supported shapes:

- **Use the default class.** Supply your own `PostgresCommandInbox` bean and both sides share one implementation.
- **Own the whole path.** Supply your own `CommandInbox` *and* your own `EventStoreFactory`/`EventStore` whose `appendWithKey` claims the idempotency key in the same transaction as the append. The framework's factory then steps aside entirely and the check does not run — the atomicity contract above becomes yours to keep.

## Retention

Inbox rows are pruned by a background `InboxRetentionSweeper` (a virtual-thread loop) so the table does not grow without bound. Retention is auto-configured across all three frameworks:

| Framework | Property | Default |
|---|---|---|
| Spring | `streamrune.inbox.retention-max-age` | `7d` |
| Quarkus | `streamrune.inbox.retention-max-age` (via `StreamRuneQuarkusProperties.inbox().retentionMaxAge()`) | `PT168H` |
| Micronaut | `streamrune.inbox.retention-max-age` | `7d` |

The default window is **7 days**, chosen to comfortably exceed realistic broker-redelivery and subscription-replay horizons. **Do not shrink this below your actual redelivery window** — a pruned key can no longer prevent a genuine duplicate, so a redelivery arriving after its key was swept would re-execute the command. A zero or negative value disables pruning entirely (`InboxRetentionSweeper.start()` becomes a no-op).

Unlike a GDPR storage-limitation retention window, the inbox window is a **correctness** setting: it must outlast the longest interval over which the same command could be redelivered.

## Caveats

- **Keys are caller-supplied and must be deterministic.** A random key per attempt defeats deduplication — the redelivery would produce a different key and re-execute. Derive the key from the identifier of the logical operation.
- **A key is bound to its command type and its stream.** Re-presenting a key with a different command, or against a different aggregate, throws `IllegalArgumentException`; it does not return the stored result. This is a leakage guard, not a convenience, and it holds for concurrent collisions as well as sequential ones.
- **The key namespace is yours to isolate.** A key is a bearer token for the result recorded under it. Keys derived from client input must be scoped to the authenticated principal or tenant with `IdempotencyKey.scopedTo(...)`; the framework does not bind inbox rows to a caller identity, deliberately (see above).
- **The keyed overload is opt-in.** Without a `CommandInbox` (no `DataSource`), `execute(command, key)` throws — `IllegalStateException` on the shipped `VirtualThreadCommandBus`. Probe `supportsIdempotentExecution()` before calling.
- **Idempotency is not concurrency control.** The inbox dedupes redeliveries of *one* command; the event store's `expectedVersion` check handles *concurrent conflicting* commands. Use both.
- **Retention is a correctness knob, not just cleanup.** Keep `streamrune.inbox.retention-max-age` longer than your maximum redelivery horizon.

## Related guides

| Guide | Description |
|---|---|
| [saga.md](saga.md) | Sagas dispatch commands with deterministic idempotency keys for effectively-once delivery |
| [retry-and-resilience.md](retry-and-resilience.md) | Retry policy, circuit breaker, and the command dead-letter queue that re-dispatches keyed |
| [outbox.md](outbox.md) | Transactional outbox — the same atomic-with-append pattern on the publish side |
