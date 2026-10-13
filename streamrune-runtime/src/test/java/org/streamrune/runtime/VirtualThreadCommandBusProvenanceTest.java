package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandBus;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.ReservedCorrelationIdException;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.test.CommandBusProvenanceContract;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.InMemoryEventStore;

/** Runs the provenance contract against {@link VirtualThreadCommandBus}. */
class VirtualThreadCommandBusProvenanceTest extends CommandBusProvenanceContract {

  @Override
  protected CommandBus newBus(EventStore eventStore) {
    return bus(eventStore);
  }

  private static VirtualThreadCommandBus bus(EventStore eventStore) {
    return VirtualThreadCommandBus.builder()
        .eventStore(eventStore)
        .locker(new LocalStripedLocker(16))
        .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
        .objectMapper(new ObjectMapper())
        .register(AGGREGATE_TYPE, Mark.class, CommandBusProvenanceContract::aggregateIdOf, DECIDER)
        .build();
  }

  @Test
  void executeAsync_carriesTheCauseOntoItsThread() {
    var cause = new StreamRuneContext.DispatchCause(CausationId.of("evt-1"), ORIGIN);
    var context =
        new StreamRuneContext.RequestContext(
            null, null, CorrelationId.of("order-saga-42"), Instant.now(), Map.of());

    // Bound alone, and bound beside a context and the saga flag as saga dispatch binds it.
    assertStoredUnder("async-1", ScopedValue.where(StreamRuneContext.CAUSE, cause), cause);
    assertStoredUnder(
        "async-2",
        ScopedValue.where(StreamRuneContext.CURRENT, context)
            .where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE)
            .where(StreamRuneContext.CAUSE, cause),
        cause);
  }

  private static void assertStoredUnder(
      String aggregateId, ScopedValue.Carrier bindings, StreamRuneContext.DispatchCause cause) {
    InMemoryEventStore eventStore = new InMemoryEventStore();
    try (VirtualThreadCommandBus bus = bus(eventStore)) {
      // The bindings end when call() returns; join() then waits for a command that runs on the
      // bus's own virtual thread, where only what executeAsync carried over is bound.
      CompletableFuture<CommandBus.CommandResult> future =
          bindings.call(() -> bus.executeAsync(new Mark(aggregateId)));
      future.join();
    }

    List<EventEnvelope> stored =
        eventStore.load(StreamId.of(AGGREGATE_TYPE, AggregateId.of(aggregateId))).events();
    assertEquals(2, stored.size());
    for (EventEnvelope envelope : stored) {
      assertEquals(cause.sagaOrigin(), envelope.metadata().sagaOrigin());
      assertEquals(cause.causationId(), envelope.metadata().causationId());
    }
  }

  // ==== A reserved correlation id with no saga origin bound ====

  private static final String REFUSAL =
      "the bound correlation id uses the reserved 'streamrune:' prefix and is not the bound saga"
          + " origin's; only a saga dispatch or StreamRuneContext.continuing binds it";

  /** A context built with the plain constructor around a correlation id in the reserved form. */
  private static StreamRuneContext.RequestContext reservedContext() {
    return new StreamRuneContext.RequestContext(
        null, null, CorrelationId.of("streamrune:saga:victim"), Instant.now(), Map.of());
  }

  /** Counts the callbacks of an interceptor; lets every command through. */
  private static final class CountingInterceptor implements CommandInterceptor {
    final AtomicInteger calls = new AtomicInteger();

    @Override
    public boolean before(CommandContext context) {
      calls.incrementAndGet();
      return true;
    }

    @Override
    public void after(CommandContext context) {
      calls.incrementAndGet();
    }

    @Override
    public void onError(CommandContext context, Throwable error) {
      calls.incrementAndGet();
    }
  }

  /** Counts dispatched and failed commands. */
  private static final class CountingMetrics implements StreamRuneMetrics {
    final AtomicInteger dispatched = new AtomicInteger();
    final AtomicInteger failed = new AtomicInteger();
    final AtomicInteger deadLettered = new AtomicInteger();

    @Override
    public void recordCommandDispatched() {
      dispatched.incrementAndGet();
    }

    @Override
    public void recordCommandFailed() {
      failed.incrementAndGet();
    }

    @Override
    public void recordDeadLetterPublished(String commandType) {
      deadLettered.incrementAndGet();
    }
  }

  /** The bus with everything a refusal must not reach: an interceptor, an inbox, a queue. */
  private record Guarded(
      VirtualThreadCommandBus bus,
      InMemoryEventStore eventStore,
      CountingInterceptor interceptor,
      InMemoryDeadLetterQueue deadLetters,
      CountingMetrics metrics)
      implements AutoCloseable {

    static Guarded create() {
      InMemoryCommandInbox inbox = new InMemoryCommandInbox();
      InMemoryEventStore eventStore = new InMemoryEventStore().withCommandInbox(inbox);
      CountingInterceptor interceptor = new CountingInterceptor();
      InMemoryDeadLetterQueue deadLetters = new InMemoryDeadLetterQueue();
      CountingMetrics metrics = new CountingMetrics();
      VirtualThreadCommandBus bus =
          VirtualThreadCommandBus.builder()
              .eventStore(eventStore)
              .locker(new LocalStripedLocker(16))
              // Three attempts, and a rule that dead-letters every failure: the refusal is neither
              // retried nor published.
              .retryPolicy(new RetryPolicy(3, Duration.ofMillis(1), 1.0))
              .objectMapper(new ObjectMapper())
              .commandInbox(inbox)
              .deadLetterQueue(deadLetters)
              .dlqEligible(failure -> true)
              .metrics(metrics)
              .interceptors(interceptor)
              .register(
                  AGGREGATE_TYPE, Mark.class, CommandBusProvenanceContract::aggregateIdOf, DECIDER)
              .build();
      return new Guarded(bus, eventStore, interceptor, deadLetters, metrics);
    }

    void assertNothingHappened() {
      assertEquals(0, interceptor.calls.get(), "no interceptor callback runs");
      assertTrue(deadLetters.all().isEmpty(), "nothing is dead-lettered");
      assertEquals(0, metrics.deadLettered.get());
      assertEquals(List.of(), eventStore.readGlobalStream(GlobalOffset.initial(), 10));
      assertEquals(1, metrics.dispatched.get());
      assertEquals(1, metrics.failed.get(), "the refusal is counted as a failed command");
    }

    @Override
    public void close() {
      bus.close();
    }
  }

  @Test
  void aRefusedReservedCorrelationId_reachesNoInterceptorAndNoDeadLetterQueue() {
    try (Guarded guarded = Guarded.create()) {
      ReservedCorrelationIdException refused =
          assertThrows(
              ReservedCorrelationIdException.class,
              () ->
                  ScopedValue.where(StreamRuneContext.CURRENT, reservedContext())
                      .run(() -> guarded.bus().execute(new Mark("refused-1"))));

      assertEquals(REFUSAL, refused.getMessage());
      guarded.assertNothingHappened();
    }
  }

  @Test
  void aRefusedReservedCorrelationId_isRefusedUnderAnIdempotencyKeyToo() {
    try (Guarded guarded = Guarded.create()) {
      ReservedCorrelationIdException refused =
          assertThrows(
              ReservedCorrelationIdException.class,
              () ->
                  ScopedValue.where(StreamRuneContext.CURRENT, reservedContext())
                      .run(
                          () ->
                              guarded
                                  .bus()
                                  .execute(
                                      new Mark("refused-2"),
                                      org.streamrune.core.types.IdempotencyKey.of("key-1"))));

      assertEquals(REFUSAL, refused.getMessage());
      guarded.assertNothingHappened();
    }
  }

  @Test
  void executeAsync_completesExceptionallyForARefusedCorrelationId() {
    try (Guarded guarded = Guarded.create()) {
      CompletableFuture<CommandBus.CommandResult> future =
          ScopedValue.where(StreamRuneContext.CURRENT, reservedContext())
              .call(() -> guarded.bus().executeAsync(new Mark("refused-3")));

      CompletionException failed = assertThrows(CompletionException.class, future::join);

      ReservedCorrelationIdException refused =
          assertInstanceOf(ReservedCorrelationIdException.class, failed.getCause());
      assertEquals(REFUSAL, refused.getMessage());
      guarded.assertNothingHappened();
    }
  }

  @Test
  void executeAsync_insideASagaDispatch_keepsTheOriginAndIsNotRefused() {
    var cause = new StreamRuneContext.DispatchCause(CausationId.of("evt-1"), ORIGIN);
    var sagaContext =
        new StreamRuneContext.RequestContext(
            null, null, ORIGIN.correlationId(), Instant.now(), Map.of());

    try (Guarded guarded = Guarded.create()) {
      ScopedValue.where(StreamRuneContext.CURRENT, sagaContext)
          .where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE)
          .where(StreamRuneContext.CAUSE, cause)
          .call(() -> guarded.bus().executeAsync(new Mark("async-saga-1")))
          .join();

      List<EventEnvelope> stored =
          guarded.eventStore().readGlobalStream(GlobalOffset.initial(), 10);
      assertEquals(2, stored.size());
      for (EventEnvelope envelope : stored) {
        assertEquals(ORIGIN, envelope.metadata().sagaOrigin());
        assertEquals(ORIGIN.correlationId(), envelope.metadata().correlationId());
      }
    }
  }

  // ==== The refusal is loud ====

  /** The WARN and ERROR lines the bus logs while {@code body} runs. */
  private static List<String> warningsOfTheBus(Runnable body) {
    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(VirtualThreadCommandBus.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      body.run();
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }
    return appender.list.stream()
        .filter(event -> event.getLevel().isGreaterOrEqual(ch.qos.logback.classic.Level.WARN))
        .map(event -> event.getLevel() + " " + event.getFormattedMessage())
        .toList();
  }

  @Test
  void aRefusedReservedCorrelationId_isLoggedOnceAtWarn_withoutTheBoundValue() {
    try (Guarded guarded = Guarded.create()) {
      List<String> warnings =
          warningsOfTheBus(
              () ->
                  assertThrows(
                      ReservedCorrelationIdException.class,
                      () ->
                          ScopedValue.where(StreamRuneContext.CURRENT, reservedContext())
                              .run(() -> guarded.bus().execute(new Mark("refused-4")))));

      assertEquals(List.of("WARN Refusing command Mark: " + REFUSAL), warnings);
      assertTrue(warnings.getFirst().indexOf("victim") < 0, "the bound value is not logged");
    }
  }

  // ==== A context bound to null ====

  @Test
  void aContextBoundToNull_runsItsInterceptorsAndFailsWhereTheBusReadsTheContext() {
    // ScopedValue.where(CURRENT, StreamRuneContext.capture()) with nothing bound binds null. The
    // check of the correlation id passes it; the command reaches its interceptors, fails with the
    // NullPointerException of the first read of the context, and is audited as a failure.
    List<AuditEntry> audit = new java.util.concurrent.CopyOnWriteArrayList<>();
    InMemoryEventStore eventStore = new InMemoryEventStore();
    try (VirtualThreadCommandBus bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .locker(new LocalStripedLocker(16))
            .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
            .objectMapper(new ObjectMapper())
            .interceptors(new AuditCommandInterceptor(audit::add))
            .register(
                AGGREGATE_TYPE, Mark.class, CommandBusProvenanceContract::aggregateIdOf, DECIDER)
            .build()) {

      assertThrows(
          NullPointerException.class,
          () ->
              ScopedValue.where(StreamRuneContext.CURRENT, (StreamRuneContext.RequestContext) null)
                  .run(() -> bus.execute(new Mark("null-context-1"))));
    }

    assertEquals(List.of(), eventStore.readGlobalStream(GlobalOffset.initial(), 10));
    assertEquals(1, audit.size());
    assertEquals(AuditOutcome.FAILURE, audit.getFirst().outcome());
    assertEquals(null, audit.getFirst().correlationId());
  }

  // ==== Commands executed on the thread of a saga's dispatch, and off it ====

  /**
   * Executes a nested {@link Mark} from {@code after()} of the command named {@code outer}, under
   * the bindings {@code nested} adds to those of the outer command, and keeps what it threw.
   */
  private static final class NestingInterceptor implements CommandInterceptor {
    private final java.util.function.Supplier<VirtualThreadCommandBus> bus;
    private final String outer;
    private final String nestedAggregate;
    private final java.util.function.UnaryOperator<Runnable> nested;
    volatile Throwable thrown;

    NestingInterceptor(
        java.util.function.Supplier<VirtualThreadCommandBus> bus,
        String outer,
        String nestedAggregate,
        java.util.function.UnaryOperator<Runnable> nested) {
      this.bus = bus;
      this.outer = outer;
      this.nestedAggregate = nestedAggregate;
      this.nested = nested;
    }

    @Override
    public void after(CommandContext context) {
      if (!context.aggregateId().value().equals(outer)) {
        return;
      }
      try {
        nested.apply(() -> bus.get().execute(new Mark(nestedAggregate))).run();
      } catch (RuntimeException e) {
        thrown = e;
      }
    }
  }

  /** A saga's dispatch of {@code Mark(outer)} on a bus that nests a command in {@code after()}. */
  private record NestedRun(
      List<EventEnvelope> outerEvents,
      List<EventEnvelope> nestedEvents,
      List<AuditEntry> audit,
      Throwable nestedFailure) {

    static NestedRun of(java.util.function.UnaryOperator<Runnable> nestedBindings) {
      List<AuditEntry> audit = new java.util.concurrent.CopyOnWriteArrayList<>();
      InMemoryCommandInbox inbox = new InMemoryCommandInbox();
      InMemoryEventStore eventStore = new InMemoryEventStore().withCommandInbox(inbox);
      VirtualThreadCommandBus[] holder = new VirtualThreadCommandBus[1];
      NestingInterceptor nesting =
          new NestingInterceptor(() -> holder[0], "outer-1", "nested-1", nestedBindings);
      holder[0] =
          VirtualThreadCommandBus.builder()
              .eventStore(eventStore)
              .locker(new LocalStripedLocker(16))
              .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
              .objectMapper(new ObjectMapper())
              .commandInbox(inbox)
              .interceptors(new AuditCommandInterceptor(audit::add), nesting)
              .register(
                  AGGREGATE_TYPE, Mark.class, CommandBusProvenanceContract::aggregateIdOf, DECIDER)
              .build();
      try (VirtualThreadCommandBus bus = holder[0]) {
        SagaCommandDispatch.dispatch(
            bus,
            ORIGIN,
            EventId.of("evt-trigger"),
            new Mark("outer-1"),
            new IdempotencyKey("saga:order-saga-42:7:0"));
      }
      return new NestedRun(
          eventStore.load(StreamId.of(AGGREGATE_TYPE, AggregateId.of("outer-1"))).events(),
          eventStore.load(StreamId.of(AGGREGATE_TYPE, AggregateId.of("nested-1"))).events(),
          List.copyOf(audit),
          nesting.thrown);
    }

    AuditEntry auditOf(String aggregateId) {
      return audit.stream()
          .filter(entry -> entry.aggregateId().value().equals(aggregateId))
          .findFirst()
          .orElseThrow();
    }
  }

  private static void assertTheSagasWork(List<EventEnvelope> events) {
    assertEquals(2, events.size());
    for (EventEnvelope envelope : events) {
      assertEquals(ORIGIN, envelope.metadata().sagaOrigin());
      assertEquals(ORIGIN.correlationId(), envelope.metadata().correlationId());
    }
  }

  @Test
  void aNestedCommandOnTheThreadOfASagasDispatch_isTheSagasWork() {
    NestedRun run = NestedRun.of(nested -> nested);

    assertEquals(null, run.nestedFailure());
    assertTheSagasWork(run.outerEvents());
    assertTheSagasWork(run.nestedEvents());
    assertEquals(ORIGIN.correlationId(), run.auditOf("outer-1").correlationId());
    assertEquals(ORIGIN.correlationId(), run.auditOf("nested-1").correlationId());
  }

  @Test
  void aNestedCommandUnderTheApplicationsOwnContext_keepsTheOrigin_andItsAuditRowAgrees() {
    // The application binds a context of its own around the nested command; the saga's cause
    // stays bound. The events carry the origin and its correlation id, and so does the audit row.
    var own =
        new StreamRuneContext.RequestContext(
            null, null, CorrelationId.of("app-own-correlation"), Instant.now(), Map.of());
    NestedRun run =
        NestedRun.of(nested -> () -> ScopedValue.where(StreamRuneContext.CURRENT, own).run(nested));

    assertEquals(null, run.nestedFailure());
    assertTheSagasWork(run.nestedEvents());
    assertEquals(ORIGIN.correlationId(), run.auditOf("nested-1").correlationId());
    assertEquals(AuditOutcome.SUCCESS, run.auditOf("nested-1").outcome());
  }

  @Test
  void aNestedCommandUnderAnotherSagasReservedId_isRefused() {
    var anotherSagas =
        new StreamRuneContext.RequestContext(
            null, null, CorrelationId.of("streamrune:saga:another-saga"), Instant.now(), Map.of());
    NestedRun run =
        NestedRun.of(
            nested -> () -> ScopedValue.where(StreamRuneContext.CURRENT, anotherSagas).run(nested));

    assertInstanceOf(ReservedCorrelationIdException.class, run.nestedFailure());
    assertEquals(List.of(), run.nestedEvents());
    assertTheSagasWork(run.outerEvents());
    assertEquals(1, run.audit().size(), "the refused command is not audited");
  }

  @Test
  void aTaskWrappedInsideARealSagaDispatch_isNotRefusedOnAnotherThread() throws Exception {
    // The bindings of SagaCommandDispatch itself, captured by wrap on the dispatching thread.
    InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    InMemoryEventStore eventStore = new InMemoryEventStore().withCommandInbox(inbox);
    Runnable[] wrapped = new Runnable[1];
    Runnable[] captured = new Runnable[1];
    try (VirtualThreadCommandBus bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .locker(new LocalStripedLocker(16))
            .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
            .objectMapper(new ObjectMapper())
            .commandInbox(inbox)
            .interceptors(
                new CommandInterceptor() {
                  @Override
                  public void after(CommandContext context) {
                    Runnable wrappedCommand = () -> executeOnABusOfItsOwn(eventStore, "wrapped-2");
                    wrapped[0] = StreamRuneContext.wrap(wrappedCommand);
                    // The context alone, without the cause: what capture() and a rebind carry.
                    StreamRuneContext.RequestContext alone = StreamRuneContext.capture();
                    captured[0] =
                        () ->
                            ScopedValue.where(StreamRuneContext.CURRENT, alone)
                                .run(() -> executeOnABusOfItsOwn(eventStore, "captured-2"));
                  }
                })
            .register(
                AGGREGATE_TYPE, Mark.class, CommandBusProvenanceContract::aggregateIdOf, DECIDER)
            .build()) {
      SagaCommandDispatch.dispatch(
          bus,
          ORIGIN,
          EventId.of("evt-trigger"),
          new Mark("outer-2"),
          new IdempotencyKey("saga:order-saga-42:8:0"));
    }

    Throwable[] failures = new Throwable[2];
    Thread first = new Thread(wrapped[0]);
    first.setUncaughtExceptionHandler((thread, thrown) -> failures[0] = thrown);
    Thread second = new Thread(captured[0]);
    second.setUncaughtExceptionHandler((thread, thrown) -> failures[1] = thrown);
    first.start();
    second.start();
    first.join();
    second.join();

    assertEquals(null, failures[0]);
    assertTheSagasWork(
        eventStore.load(StreamId.of(AGGREGATE_TYPE, AggregateId.of("wrapped-2"))).events());
    assertInstanceOf(ReservedCorrelationIdException.class, failures[1]);
    assertEquals(
        List.of(),
        eventStore.load(StreamId.of(AGGREGATE_TYPE, AggregateId.of("captured-2"))).events());
  }

  /** Executes one {@link Mark} on a bus of its own over {@code eventStore}. */
  private static void executeOnABusOfItsOwn(InMemoryEventStore eventStore, String aggregateId) {
    try (VirtualThreadCommandBus bus = bus((EventStore) eventStore)) {
      bus.execute(new Mark(aggregateId));
    }
  }
}
