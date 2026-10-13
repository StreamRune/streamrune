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
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.GlobalOffset;
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
      "the bound correlation id uses the reserved 'streamrune:' prefix; only a saga dispatch or"
          + " StreamRuneContext.continuing binds it";

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
      IllegalArgumentException refused =
          assertThrows(
              IllegalArgumentException.class,
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
      IllegalArgumentException refused =
          assertThrows(
              IllegalArgumentException.class,
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

      IllegalArgumentException refused =
          assertInstanceOf(IllegalArgumentException.class, failed.getCause());
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
}
