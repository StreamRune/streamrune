package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;

/**
 * {@link InlineProjectionInterceptor}'s {@code after()} used to run AFTER {@link
 * VirtualThreadCommandBus} released the per-aggregate lock — the lock is scoped to a
 * try-with-resources inside {@code executeWithRetry}, which returns (releasing the lock) before
 * {@code executeCounted} invoked {@code notifyAfter}. Two commands committed against the SAME
 * aggregate could therefore apply their inline projections in the REVERSE of commit order: command
 * A commits v1 and is slow to project; command B commits v2 and projects first; A then overwrites
 * the read model with the stale v1 value, and the update B just applied is lost. Fixed by running
 * the post-commit {@code after()} phase while STILL holding the aggregate lock, so a second command
 * on the same aggregate cannot even start (load/decide/append) until the first command's entire
 * after() phase — including its inline projection — has completed and released the lock.
 *
 * <p>BOTH commands run on their own virtual thread. After the fix, the second command's {@code
 * execute()} genuinely BLOCKS on lock acquisition until the first command's after() phase finishes
 * — driving it from the same thread that must also release the first command's latch would deadlock
 * the test itself.
 */
class InlineProjectionOrderingTest {

  private static final AggregateType TYPE = AggregateType.of("cell");

  record SetValue(AggregateId id, int value) implements Command {}

  record ValueSet(int value) implements DomainEvent {}

  record ValueState(int value) implements AggregateState {}

  static final class SetValueDecider implements Decider<SetValue, ValueState, ValueSet> {
    @Override
    public ValueState initialState() {
      return new ValueState(0);
    }

    @Override
    public List<ValueSet> decide(SetValue command, ValueState state) {
      return List.of(new ValueSet(command.value()));
    }

    @Override
    public ValueState evolve(ValueState state, ValueSet event) {
      return new ValueState(event.value());
    }
  }

  @Test
  @Timeout(30)
  void concurrentCommandsOnSameAggregate_applyInlineProjectionsInCommitOrder() throws Exception {
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var readModel = new AtomicInteger();
    var order = new CopyOnWriteArrayList<Integer>();

    Projection projection =
        batch -> {
          for (var envelope : batch) {
            int value = ((ValueSet) envelope.event()).value();
            if (value == 1) {
              // Blocks the FIRST command's after() phase until the test explicitly releases it,
              // giving the second command every opportunity to run ahead of it.
              entered.countDown();
              try {
                assertTrue(release.await(10, TimeUnit.SECONDS), "release latch timed out");
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            }
            readModel.set(value);
            order.add(value);
          }
        };

    var inline = InlineProjectionInterceptor.builder().register("probe", projection).build();
    var store = new InMemoryEventStore();
    var aggregateId = AggregateId.of("same");

    try (var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .lockTimeout(Duration.ofSeconds(20))
            .interceptors(inline)
            .register(TYPE, SetValue.class, SetValue::id, new SetValueDecider())
            .build()) {

      var failureA = new AtomicReference<Throwable>();
      var failureB = new AtomicReference<Throwable>();

      Thread threadA =
          Thread.ofVirtual()
              .start(
                  () -> {
                    try {
                      bus.execute(new SetValue(aggregateId, 1));
                    } catch (Throwable e) {
                      failureA.set(e);
                    }
                  });

      assertTrue(
          entered.await(10, TimeUnit.SECONDS), "command A never entered its inline projection");

      Thread threadB =
          Thread.ofVirtual()
              .start(
                  () -> {
                    try {
                      bus.execute(new SetValue(aggregateId, 2));
                    } catch (Throwable e) {
                      failureB.set(e);
                    }
                  });

      // Give command B a chance to actually reach (and block on) lock acquisition before A is
      // released — not required for correctness (the lock ordering is deterministic either way),
      // but it keeps the test honest about exercising the blocked-on-the-lock path.
      Thread.sleep(200);
      release.countDown();

      threadA.join(TimeUnit.SECONDS.toMillis(15));
      threadB.join(TimeUnit.SECONDS.toMillis(15));

      assertFalse(threadA.isAlive(), "command A did not finish in time");
      assertFalse(threadB.isAlive(), "command B did not finish in time");
      assertNull(failureA.get(), "command A failed: " + failureA.get());
      assertNull(failureB.get(), "command B failed: " + failureB.get());
    }

    assertEquals(
        List.of(1, 2),
        store.load(StreamId.of(TYPE, AggregateId.of("same"))).events().stream()
            .map(env -> ((ValueSet) env.event()).value())
            .toList(),
        "committed event order is unaffected by the fix");
    assertEquals(
        List.of(1, 2),
        order,
        "Inline projection must apply in COMMIT order — B must not be able to even START"
            + " (load/decide/append) until A's entire after() phase, including its inline"
            + " projection, has released the aggregate lock");
    assertEquals(2, readModel.get(), "the read model must reflect the LATEST committed value");
  }

  @Test
  void idempotentReplay_doesNotReapplyInlineProjection() {
    var calls = new AtomicInteger();
    Projection projection = batch -> calls.incrementAndGet();
    var inline = InlineProjectionInterceptor.builder().register("probe", projection).build();
    var inbox = new InMemoryCommandInbox();
    var store = new InMemoryEventStore().withCommandInbox(inbox);
    var aggregateId = AggregateId.of("replay-agg");

    try (var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .commandInbox(inbox)
            .interceptors(inline)
            .register(TYPE, SetValue.class, SetValue::id, new SetValueDecider())
            .build()) {

      var key = IdempotencyKey.of("replay-key-1");
      var first = bus.execute(new SetValue(aggregateId, 7), key);
      assertFalse(first.shortCircuited(), "the first execution actually ran");
      assertEquals(1, calls.get(), "the inline projection ran once for the original execution");

      var second = bus.execute(new SetValue(aggregateId, 7), key);
      assertTrue(second.idempotentReplay(), "the second call must be served from the inbox");
      assertEquals(
          1,
          calls.get(),
          "A replayed (idempotent) execution appended no new events — its CommandResult"
              + " carries no envelopes and is shortCircuited(), so InlineProjectionInterceptor"
              + " must not re-run the projection");
    }
  }
}
