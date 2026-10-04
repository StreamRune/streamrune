package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRunner;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.test.InMemoryEventStore;

class StreamRuneFacadeTest {

  private static final AggregateType TYPE = AggregateType.of("counter");

  sealed interface Cmd extends Command {
    record Inc(String id) implements Cmd {}
  }

  sealed interface Evt extends DomainEvent {
    record Incd() implements Evt {}
  }

  record State(int n) implements AggregateState {
    State() {
      this(0);
    }
  }

  static class Dec implements Decider<Cmd, State, Evt> {
    @Override
    public State initialState() {
      return new State();
    }

    @Override
    public List<Evt> decide(Cmd command, State state) {
      return List.of(new Evt.Incd());
    }

    @Override
    public State evolve(State state, Evt event) {
      return new State(state.n() + 1);
    }
  }

  private StreamRune buildStreamRune(InMemoryEventStore store) {
    return StreamRune.builder()
        .eventStore(store)
        .locker(new LocalStripedLocker(16))
        .register(
            TYPE,
            Cmd.class,
            c ->
                switch (c) {
                  case Cmd.Inc i -> AggregateId.of(i.id());
                },
            new Dec())
        .build();
  }

  private void withCtx(Runnable r) {
    var ctx =
        new StreamRuneContext.RequestContext(
            null, null, CorrelationId.of("corr-1"), Instant.now(), Map.of());
    ScopedValue.where(StreamRuneContext.CURRENT, ctx).run(r);
  }

  @Test
  void executeDelegatesToCommandBus() {
    var store = new InMemoryEventStore();
    try (var streamRune = buildStreamRune(store)) {
      withCtx(() -> streamRune.execute(new Cmd.Inc("counter-1")));
      assertEquals(1, store.load(StreamId.of(TYPE, AggregateId.of("counter-1"))).events().size());
    }
  }

  @Test
  void facadeDelegatesIdempotentExecutionCapabilityAndKeyedExecute() {
    var store = new InMemoryEventStore();
    try (var streamRune = buildStreamRune(store)) {
      // The facade must report the SAME keyed-execution capability as the bus it wraps (here no
      // CommandInbox → false), not the CommandBus interface default.
      assertEquals(
          streamRune.commandBus().supportsIdempotentExecution(),
          streamRune.supportsIdempotentExecution());
      // execute(command, key) must DELEGATE to the underlying bus: an inbox-less bus throws
      // IllegalStateException, not the interface-default UnsupportedOperationException that a
      // non-delegating facade would throw.
      withCtx(
          () ->
              assertThrows(
                  IllegalStateException.class,
                  () -> streamRune.execute(new Cmd.Inc("counter-1"), IdempotencyKey.of("k1"))));
    }
  }

  @Test
  void withEventStoreIsAlias() {
    var store = new InMemoryEventStore();
    var streamRune =
        StreamRune.builder()
            .withEventStore(store)
            .locker(new LocalStripedLocker(16))
            .register(
                TYPE,
                Cmd.class,
                c ->
                    switch (c) {
                      case Cmd.Inc i -> AggregateId.of(i.id());
                    },
                new Dec())
            .build();
    try {
      withCtx(() -> streamRune.execute(new Cmd.Inc("counter-1")));
      assertEquals(1, store.load(StreamId.of(TYPE, AggregateId.of("counter-1"))).events().size());
    } finally {
      streamRune.close();
    }
  }

  @Test
  void doubleCloseIsIdempotent() {
    // StreamRune.close() delegates to VirtualThreadCommandBus.close(), which is idempotent
    // per the AutoCloseable convention: a repeated close() waits for any remaining in-flight
    // commands and returns instead of throwing.
    var store = new InMemoryEventStore();
    var streamRune = buildStreamRune(store);
    streamRune.close();
    assertDoesNotThrow(streamRune::close);
  }

  @Test
  void startAndStopProjectionsWithoutRegistered() {
    var store = new InMemoryEventStore();
    try (var streamRune = buildStreamRune(store)) {
      assertDoesNotThrow(streamRune::startProjections);
      assertDoesNotThrow(streamRune::stopProjections);
    }
  }

  @Test
  void closeClosesCommandBusEvenWhenStoppingProjectionsThrows() {
    class ThrowingRunner implements ProjectionRunner, AutoCloseable {
      @Override
      public void run(
          ProjectionName projectionName, Projection projection, ProjectionDeliveryMode mode) {}

      @Override
      public void reset(ProjectionName projectionName) {}

      @Override
      public void close() {
        throw new IllegalStateException("runner close failed");
      }
    }

    var store = new InMemoryEventStore();
    var streamRune =
        StreamRune.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .register(
                TYPE,
                Cmd.class,
                c ->
                    switch (c) {
                      case Cmd.Inc i -> AggregateId.of(i.id());
                    },
                new Dec())
            .projectionRunner(new ThrowingRunner())
            .build();

    var thrown = assertThrows(RuntimeException.class, streamRune::close);
    assertEquals("Failed to stop projections", thrown.getMessage());
    assertTrue(
        ((VirtualThreadCommandBus) streamRune.commandBus()).isClosed(),
        "command bus must be closed even when stopping projections throws");
  }

  @Test
  void registerDeciderIsAlias() {
    var store = new InMemoryEventStore();
    try (var streamRune =
        StreamRune.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .registerDecider(
                TYPE,
                Cmd.class,
                c ->
                    switch (c) {
                      case Cmd.Inc i -> AggregateId.of(i.id());
                    },
                new Dec())
            .build()) {
      withCtx(() -> streamRune.execute(new Cmd.Inc("counter-2")));
      assertEquals(1, store.load(StreamId.of(TYPE, AggregateId.of("counter-2"))).events().size());
    }
  }
}
