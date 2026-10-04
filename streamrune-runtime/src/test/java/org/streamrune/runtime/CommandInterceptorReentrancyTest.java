package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.streamrune.core.AggregateLocker;
import org.streamrune.core.AggregateState;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.CommandInterceptor.CommandContext;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.LockException;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.test.InMemoryEventStore;

/**
 * Three interceptors stash per-execution state in a flat {@link ThreadLocal} on the documented
 * assumption that {@code before()}, {@code after()} and {@code onError()} all run on one thread.
 * They do — but the bus invokes {@code after()} in REVERSE registration order, so {@link
 * InlineProjectionInterceptor} (innermost) runs user projection code on that same thread while the
 * outer interceptors are still mid-callback. A process-manager projection that dispatches a
 * follow-up command through the same bus therefore runs a NESTED execution inside the outer
 * command's {@code after()} chain, and the nested run's {@code before()} overwrites — and its
 * {@code after()} removes — the outer run's ThreadLocal.
 *
 * <p>The damage per interceptor: the audit row of a privileged, authenticated operation is written
 * with {@code userId == null}, which the class's own javadoc defines as "submitted without an
 * authenticated user (anonymous request)"; the outer OpenTelemetry span is never ended and its
 * Scope never closed, so a pooled platform thread returns carrying a stale current Context; and a
 * designated circuit-breaker probe never reports back, leaving the circuit HALF_OPEN rejecting all
 * traffic until the probe timeout.
 *
 * <p>A per-thread STACK is the fix: nesting pushes and pops instead of clobbering.
 */
class CommandInterceptorReentrancyTest {

  private static final AggregateType AGG = AggregateType.of("agg");
  private static final AggregateType OUTER = AggregateType.of("outer");
  private static final AggregateType INNER = AggregateType.of("inner");

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  record OuterCommand(String id) implements Command {}

  record InnerCommand(String id) implements Command {}

  sealed interface ThingEvent extends DomainEvent {
    record ThingHappened(String id) implements ThingEvent {}
  }

  record ThingState(int count) implements AggregateState {}

  static final class OneEventDecider<C extends Command>
      implements Decider<C, ThingState, ThingEvent> {
    @Override
    public ThingState initialState() {
      return new ThingState(0);
    }

    @Override
    public List<ThingEvent> decide(C command, ThingState state) {
      return List.of(new ThingEvent.ThingHappened("e"));
    }

    @Override
    public ThingState evolve(ThingState state, ThingEvent event) {
      return new ThingState(state.count() + 1);
    }
  }

  /** Records every audit entry in call order. */
  static final class RecordingAuditStore implements AuditStore {
    final List<AuditEntry> entries = new ArrayList<>();

    @Override
    public void save(AuditEntry entry) {
      entries.add(entry);
    }
  }

  private static CommandContext ctx(String commandId) {
    return new CommandContext(
        new OuterCommand(commandId),
        "OuterCommand",
        CommandId.of(commandId),
        AGG,
        AggregateId.of("agg-" + commandId),
        null,
        Instant.now());
  }

  private static CommandContext ctxWithResult(String commandId) {
    return new CommandContext(
        new OuterCommand(commandId),
        "OuterCommand",
        CommandId.of(commandId),
        AGG,
        AggregateId.of("agg-" + commandId),
        new CommandBus.CommandResult(List.of(), null, null, null),
        Instant.now());
  }

  private static int eventCount(InMemoryEventStore eventStore, AggregateType type, String id) {
    return eventStore.load(StreamId.of(type, AggregateId.of(id))).events().size();
  }

  // ---------------------------------------------------------------------------------------------
  // Audit
  // ---------------------------------------------------------------------------------------------

  @Test
  void auditInterceptor_nestedExecutionOnTheSameThread_doesNotStealTheOuterActor() {
    var store = new RecordingAuditStore();
    var interceptor = new AuditCommandInterceptor(store);

    ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, UserId.of("operator-7"), CorrelationId.of("corr-outer"), Instant.now(), null))
        .run(
            () -> {
              interceptor.before(ctx("outer")); // outer command starts

              // A projection running in the outer command's after() chain dispatches a follow-up
              // command; it carries its own (here: anonymous, unbound) context.
              ScopedValue.where(
                      StreamRuneContext.CURRENT,
                      new StreamRuneContext.RequestContext(
                          null, null, CorrelationId.of("corr-inner"), Instant.now(), null))
                  .run(
                      () -> {
                        interceptor.before(ctx("inner"));
                        interceptor.after(ctxWithResult("inner"));
                      });

              interceptor.after(ctxWithResult("outer"));
            });

    assertEquals(2, store.entries.size());
    AuditEntry inner = store.entries.get(0);
    AuditEntry outer = store.entries.get(1);
    assertEquals(CommandId.of("inner"), inner.commandId());
    assertEquals(CommandId.of("outer"), outer.commandId());
    assertEquals(
        UserId.of("operator-7"),
        outer.userId(),
        "the outer command's compliance row must keep its own authenticated actor — a null userId"
            + " durably records a privileged operation as an anonymous request");
    assertEquals(CorrelationId.of("corr-outer"), outer.correlationId());
    assertEquals(CorrelationId.of("corr-inner"), inner.correlationId());
  }

  @Test
  void auditInterceptor_nestedCommandThroughTheBus_doesNotStealTheOuterActor() {
    // The reachability half: no hand-driven callbacks, a real bus whose inline projection
    // dispatches a follow-up command exactly as a process manager does.
    var store = new RecordingAuditStore();
    var eventStore = new InMemoryEventStore();
    var busHolder = new CommandBus[1];
    var dispatched = new AtomicBoolean(false);

    Projection processManager =
        batch -> {
          if (dispatched.compareAndSet(false, true)) {
            busHolder[0].execute(new InnerCommand("inner-1"));
          }
        };

    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            // Registration order mirrors CommandInterceptorOrdering: ORDER_AUDIT (2000) is outer,
            // ORDER_INLINE_PROJECTION (6000) innermost — and after() runs in REVERSE, so the
            // projection (and the nested command it dispatches) runs while audit is still pending.
            .interceptors(
                new AuditCommandInterceptor(store),
                InlineProjectionInterceptor.builder()
                    .register("process-manager", processManager)
                    .build())
            .register(
                AGG, OuterCommand.class, cmd -> AggregateId.of(cmd.id()), new OneEventDecider<>())
            .register(
                AGG, InnerCommand.class, cmd -> AggregateId.of(cmd.id()), new OneEventDecider<>())
            .build();
    busHolder[0] = bus;

    ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, UserId.of("operator-9"), CorrelationId.of("flow-42"), Instant.now(), null))
        .run(() -> bus.execute(new OuterCommand("outer-1")));

    assertTrue(dispatched.get(), "the nested command must actually have run");
    assertEquals(2, store.entries.size(), "one audit row per executed command");
    AuditEntry outer =
        store.entries.stream()
            .filter(e -> "OuterCommand".equals(e.commandType()))
            .findFirst()
            .orElseThrow();
    assertEquals(
        UserId.of("operator-9"),
        outer.userId(),
        "the outer command ran as operator-9 and its audit row must say so");
    assertEquals(CorrelationId.of("flow-42"), outer.correlationId());
  }

  // ---------------------------------------------------------------------------------------------
  // OpenTelemetry
  // ---------------------------------------------------------------------------------------------

  @Test
  void openTelemetryInterceptor_nestedExecution_endsBothSpansAndClosesBothScopes() {
    var interceptor = new OpenTelemetryCommandInterceptor(otelTesting.getOpenTelemetry());

    interceptor.before(ctx("outer"));
    interceptor.before(ctx("inner"));
    interceptor.after(ctxWithResult("inner"));
    interceptor.after(ctxWithResult("outer"));

    List<SpanData> spans = otelTesting.getSpans();
    assertEquals(
        2,
        spans.size(),
        "both spans must be ended — a nested execution must not orphan the outer span forever");
    assertTrue(spans.stream().allMatch(SpanData::hasEnded));
    assertEquals(
        io.opentelemetry.api.trace.Span.getInvalid().getSpanContext(),
        io.opentelemetry.api.trace.Span.current().getSpanContext(),
        "both Scopes must be closed, or a pooled platform thread returns carrying a stale Context"
            + " and every later operation parents to a dead span");
  }

  // ---------------------------------------------------------------------------------------------
  // Circuit breaker
  // ---------------------------------------------------------------------------------------------

  @Test
  void circuitBreaker_nestedExecutionDuringAProbe_theProbeStillReportsBack() {
    var cb = new CircuitBreakerCommandInterceptor(1, Duration.ZERO);
    cb.onError(ctx("seed"), new RuntimeException("database down")); // OPEN
    assertTrue(cb.before(ctx("outer")), "zero cooldown admits a probe");
    assertEquals("HALF_OPEN", cb.circuitState());

    // A nested command on the SAME thread: the breaker rejects it (a probe is already in flight),
    // and that rejection must leave the outer probe's designation untouched.
    assertThrows(CircuitBreakerOpenException.class, () -> cb.before(ctx("inner")));

    cb.after(ctxWithResult("outer")); // the outer probe reports success
    assertEquals(
        "CLOSED",
        cb.circuitState(),
        "the designated probe must still be able to report back after a nested execution — losing"
            + " the designation strands the circuit HALF_OPEN, rejecting all traffic until the"
            + " probe timeout");
  }

  @Test
  void circuitBreaker_aNestedExecutionThatCompletes_doesNotResurrectTheOuterProbeFlag() {
    // The reverse direction: an inner execution that runs to completion while the circuit is CLOSED
    // must not leave a probe designation behind for the outer frame to consume.
    var cb = new CircuitBreakerCommandInterceptor(5, Duration.ofMinutes(5));
    assertTrue(cb.before(ctx("outer")));
    assertTrue(cb.before(ctx("inner")));
    cb.after(ctxWithResult("inner"));
    cb.after(ctxWithResult("outer"));
    assertEquals("CLOSED", cb.circuitState());
    assertNotNull(cb.circuitState());
  }

  // ---------------------------------------------------------------------------------------------
  // Lock reentrancy: the after() phase moved INSIDE the aggregate
  // lock's try-with-resources. VirtualThreadCommandBus's own javadoc claims a nested same-thread
  // dispatch "re-enters the held lock instead of deadlocking" — true only for LocalStripedLocker's
  // ReentrantLock. PgAdvisoryLocker checks out a NEW pool connection per acquireLock and takes a
  // SESSION-scoped advisory lock, so the identical pattern self-blocks (or, against a fake
  // non-blocking locker like the one below, fails outright) on any OTHER locker. The fix must be
  // locker-agnostic, at the bus level.
  // ---------------------------------------------------------------------------------------------

  /**
   * Simulates any NON-reentrant locker (the documented shape of {@code PgAdvisoryLocker}, which
   * takes a session-scoped advisory lock on a freshly checked-out connection every call): a second
   * acquire for a stream that is already held — by anyone, tracked purely by "is there an
   * outstanding un-closed acquisition for this stream" — fails immediately instead of re-entering.
   * {@code LocalStripedLocker}'s {@code ReentrantLock} would instead succeed here, which is exactly
   * the discrepancy this fix closes. Every real acquisition is recorded in {@link #acquired}.
   */
  static final class NonReentrantLocker implements AggregateLocker {
    private final Set<StreamId> held = ConcurrentHashMap.newKeySet();
    final List<StreamId> acquired = new CopyOnWriteArrayList<>();

    @Override
    public AutoCloseable acquireLock(StreamId streamId, Duration timeout) {
      if (!held.add(streamId)) {
        throw new LockException(
            "advisory lock for stream: " + streamId + " is held by another session");
      }
      acquired.add(streamId);
      return () -> held.remove(streamId);
    }
  }

  @Test
  void inlineProjection_nestedCommandAgainstTheSameAggregate_reentersOnANonReentrantLocker() {
    var nonReentrant = new NonReentrantLocker();
    var eventStore = new InMemoryEventStore();
    var busHolder = new CommandBus[1];
    var dispatched = new AtomicBoolean(false);

    Projection processManager =
        batch -> {
          if (dispatched.compareAndSet(false, true)) {
            // Same aggregate ID as the outer command — the case the pre-fix javadoc called safe
            // "on every locker" but is only actually safe on a ReentrantLock-backed one.
            busHolder[0].execute(new InnerCommand("agg-same"));
          }
        };

    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .locker(nonReentrant)
            .interceptors(
                InlineProjectionInterceptor.builder()
                    .register("process-manager", processManager)
                    .build())
            .register(
                AGG, OuterCommand.class, cmd -> AggregateId.of(cmd.id()), new OneEventDecider<>())
            .register(
                AGG, InnerCommand.class, cmd -> AggregateId.of(cmd.id()), new OneEventDecider<>())
            .build();
    busHolder[0] = bus;

    var result = bus.execute(new OuterCommand("agg-same"));

    assertTrue(dispatched.get(), "the nested command must actually have run");
    assertNotNull(result);
    assertEquals(
        2,
        eventCount(eventStore, AGG, "agg-same"),
        "both the outer and the nested inner command must have committed — a self-block or a"
            + " LockException on the non-reentrant locker would leave only the outer event");
    assertEquals(
        List.of(StreamId.of(AGG, AggregateId.of("agg-same"))),
        nonReentrant.acquired,
        "a nested dispatch into the same stream re-enters: one acquisition");
  }

  @Test
  void nestedDispatchIntoAnotherTypeWithTheSameIdValue_takesItsOwnLock() {
    var locker = new NonReentrantLocker();
    var eventStore = new InMemoryEventStore();
    var busHolder = new CommandBus[1];
    var dispatched = new AtomicBoolean(false);
    Projection processManager =
        batch -> {
          if (dispatched.compareAndSet(false, true)) {
            busHolder[0].execute(new InnerCommand("agg-shared"));
          }
        };
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .locker(locker)
            .interceptors(
                InlineProjectionInterceptor.builder()
                    .register("process-manager", processManager)
                    .build())
            .register(
                OUTER, OuterCommand.class, cmd -> AggregateId.of(cmd.id()), new OneEventDecider<>())
            .register(
                INNER, InnerCommand.class, cmd -> AggregateId.of(cmd.id()), new OneEventDecider<>())
            .build();
    busHolder[0] = bus;

    bus.execute(new OuterCommand("agg-shared"));

    assertTrue(dispatched.get());
    assertEquals(
        List.of(
            StreamId.of(OUTER, AggregateId.of("agg-shared")),
            StreamId.of(INNER, AggregateId.of("agg-shared"))),
        locker.acquired,
        "same id value, two types: two streams, two locks — not a re-entry");
    assertEquals(1, eventCount(eventStore, OUTER, "agg-shared"));
    assertEquals(1, eventCount(eventStore, INNER, "agg-shared"));
  }

  @Test
  void inlineProjection_nestedCommandAgainstADifferentAggregate_stillAcquiresItsOwnLock() {
    // The fix must NOT make every lock acquisition a no-op — only a re-entry into an aggregate
    // ALREADY held by this same thread. A different aggregate must still go through the real
    // locker and can therefore still genuinely fail if THAT lock is unavailable.
    var nonReentrant = new NonReentrantLocker();
    var eventStore = new InMemoryEventStore();
    var busHolder = new CommandBus[1];
    var dispatched = new AtomicBoolean(false);

    Projection processManager =
        batch -> {
          if (dispatched.compareAndSet(false, true)) {
            busHolder[0].execute(new InnerCommand("agg-different"));
          }
        };

    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .locker(nonReentrant)
            .interceptors(
                InlineProjectionInterceptor.builder()
                    .register("process-manager", processManager)
                    .build())
            .register(
                AGG, OuterCommand.class, cmd -> AggregateId.of(cmd.id()), new OneEventDecider<>())
            .register(
                AGG, InnerCommand.class, cmd -> AggregateId.of(cmd.id()), new OneEventDecider<>())
            .build();
    busHolder[0] = bus;

    var result = bus.execute(new OuterCommand("agg-outer"));

    assertTrue(dispatched.get(), "the nested command must actually have run");
    assertNotNull(result);
    assertEquals(1, eventCount(eventStore, AGG, "agg-outer"));
    assertEquals(
        1,
        eventCount(eventStore, AGG, "agg-different"),
        "a different aggregate must still acquire its own (successful) lock — not be treated as"
            + " already held");
  }

  @Test
  void sameAggregateReentrancy_doesNotLeakTheHeldMarkerAcrossUnrelatedCommands() {
    // Regression guard for the tracking mechanism itself: once the outer command's lock is
    // released, the SAME aggregate must go through the real locker again for a later, unrelated
    // command on the same thread — the "already held" marker must not outlive the lock it tracks.
    var nonReentrant = new NonReentrantLocker();
    var eventStore = new InMemoryEventStore();

    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .locker(nonReentrant)
            .register(
                AGG, OuterCommand.class, cmd -> AggregateId.of(cmd.id()), new OneEventDecider<>())
            .build();

    bus.execute(new OuterCommand("agg-reused"));
    bus.execute(new OuterCommand("agg-reused"));

    assertEquals(
        2,
        eventCount(eventStore, AGG, "agg-reused"),
        "a second, unrelated command against the same aggregate after the first lock was released"
            + " must still succeed — the reentrancy marker must be cleared, not left held forever");
  }
}
