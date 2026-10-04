package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateLocker;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus.CommandResult;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.test.InMemoryEventStore;

/**
 * An unkeyed command whose decider returns no events has nothing to persist, yet it has reached its
 * result and completed the interceptors' after() phase. A failure after that point — a lock release
 * that throws — must not turn it into a reported failure: the interceptors would receive onError()
 * on top of after(), the no-op would be dead-lettered, and the caller told the command failed.
 */
class VirtualThreadCommandBusNoOpCommitTest {

  private static final AggregateType TYPE = AggregateType.of("touch");

  record Touch(String id) implements Command {}

  record Touched() implements DomainEvent {}

  record TouchState() implements AggregateState {}

  static final class NoOpDecider implements Decider<Touch, TouchState, Touched> {
    @Override
    public TouchState initialState() {
      return new TouchState();
    }

    @Override
    public List<Touched> decide(Touch command, TouchState state) {
      return List.of();
    }

    @Override
    public TouchState evolve(TouchState state, Touched event) {
      return state;
    }
  }

  static final class CountingInterceptor implements CommandInterceptor {
    final AtomicInteger afters = new AtomicInteger();
    final AtomicInteger errors = new AtomicInteger();

    @Override
    public boolean before(CommandContext ctx) {
      return true;
    }

    @Override
    public void after(CommandContext ctx) {
      afters.incrementAndGet();
    }

    @Override
    public void onError(CommandContext ctx, Throwable error) {
      errors.incrementAndGet();
    }
  }

  /** Acquires freely; every release throws, as a lock backend losing its connection would. */
  static final AggregateLocker RELEASE_FAILS =
      (StreamId streamId, Duration timeout) ->
          () -> {
            throw new IllegalStateException("lock release failed");
          };

  @Test
  void unkeyedNoOp_whoseLockReleaseFails_isStillReportedAsSuccess() {
    var interceptor = new CountingInterceptor();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(new InMemoryEventStore())
            .locker(RELEASE_FAILS)
            .interceptors(interceptor)
            .register(TYPE, Touch.class, cmd -> AggregateId.of(cmd.id()), new NoOpDecider())
            .build();

    CommandResult result = bus.execute(new Touch("t-1"));

    assertTrue(result.events().isEmpty(), "a no-op produces no events");
    assertEquals(1, interceptor.afters.get(), "after() ran once");
    assertEquals(0, interceptor.errors.get(), "onError() never follows a delivered after()");
  }
}
