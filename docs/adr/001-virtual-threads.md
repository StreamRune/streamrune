# ADR-001: Virtual Threads for Command Execution

**Status:** Accepted
**Date:** 2026-04-15

## Context

Java concurrency has four main camps:

1. **Traditional platform threads** — one thread per request, straightforward but expensive at scale because each thread consumes ~1 MB of stack.
2. **Reactive programming** (Project Reactor / Webflux, Mutiny) — high throughput but at the cost of callback chains, a new mental model, and difficult debugging across thread boundaries.
3. **`CompletableFuture` composition** — less boilerplate than raw reactive but still non-blocking composition that leaks through every API layer.
4. **Virtual threads** (Project Loom, GA in Java 21) — lightweight green threads scheduled onto a pool of carrier threads; blocking I/O calls are transparently parked without holding the carrier.

The `VirtualThreadCommandBus` is the core execution engine. Every `executeAsync()` call spawns a virtual thread named `streamrune-cmd-<CommandSimpleName>`. The implementation uses blocking JDBC calls in `EventStore.load()` and `EventStore.append()` — exactly the kind of I/O that virtual threads handle efficiently.

The team evaluated reactive (Mutiny / Reactor) as an alternative. The primary objection was that reactive programming forces the entire call stack to become reactive: every interface, every test, every caller must be written in reactive style. Debugging stack traces lose coherence and exception handling becomes ceremony.

## Decision

Adopt Java 21 virtual threads as the concurrency model for command execution.

- `VirtualThreadCommandBus.executeAsync()` wraps the synchronous `execute()` path in `Thread.ofVirtual().name("streamrune-cmd-" + ...).start(...)`.
- The synchronous `execute()` path acquires an aggregate lock, loads event history from PostgreSQL (blocking JDBC), runs `Decider.decide()`, appends events, and optionally saves a snapshot — all in plain blocking style. Virtual thread scheduling ensures carrier threads are not blocked during JDBC waits.
- `ScopedValue` (`StreamRuneContext.CURRENT`) is captured before spawning the virtual thread and re-bound inside it, because ScopedValues do not inherit across `Thread.start()`. This preserves correlation ID, trace ID, and user ID across the asynchronous boundary.
- Reactive backpressure is explicitly out of scope for Horizon 1. The command bus is fire-and-forget-async; callers compose `CompletableFuture<CommandResult>`.

## Consequences

**Positive:**

- Blocking code style throughout — no reactive operators, no `flatMap` chains. Stack traces are linear and debuggable. Junior developers can read and understand the concurrency model without reactive training.
- Command throughput scales to tens of thousands of concurrent virtual threads with minimal memory overhead compared to platform threads.
- Integration with Spring Boot (via `spring.threads.virtual.enabled=true`), Quarkus, and Micronaut virtual thread support is natural — no framework-specific reactive adapters needed.
- Testing is simple: call `execute()` synchronously in tests without mock executors or reactive test utilities.

**Negative:**

- Java 21 is the minimum required version. Teams on Java 17 LTS cannot use StreamRune without upgrading.
- No built-in backpressure. If command producers are faster than the aggregate lock allows, commands queue up in unbounded `CompletableFuture` chains. Callers are responsible for rate limiting upstream.
- Pinning: virtual threads can be pinned to carrier threads inside `synchronized` blocks. StreamRune uses `java.util.concurrent` locks (not `synchronized`) in `LocalStripedLocker` to avoid this, but third-party JDBC drivers that use `synchronized` internally can cause pinning under high concurrency.
