package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateLocker;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.CommandBus.CommandResult;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.LockException;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.InMemoryEventStore;

/**
 * Pins the contract for a duplicate <em>keyed</em> dispatch that reaches the bus while — or just
 * after — the original execution of the same key runs: the duplicate must receive the recorded
 * outcome ({@code IDEMPOTENT_REPLAY}), never a verdict produced by re-running the decider against
 * state the original already changed.
 *
 * <p>The failure this guards against: two saga runners (two replicas, or the event path and a
 * sweeper in one JVM) dispatch the same keyed command. Both miss the pre-execution inbox lookup
 * (neither row is committed yet). The winner commits under the aggregate lock; the loser,
 * serialized behind that lock, then loads the winner's state and the decider rejects the command as
 * a business error ("order already confirmed"). Treated as a permanent failure, that rejection made
 * the saga compensate a step that had in fact succeeded.
 */
class VirtualThreadCommandBusKeyedDuplicateTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  // === Minimal test domain ===

  record ConfirmOrder(String orderId) implements Command {}

  record OrderConfirmed(String orderId) implements DomainEvent {}

  record OrderState(boolean confirmed) implements AggregateState {}

  /** Rejects a second confirmation of the same order — the shape of every "already done" guard. */
  static final class ConfirmDecider implements Decider<ConfirmOrder, OrderState, OrderConfirmed> {
    final AtomicInteger decisions = new AtomicInteger();
    final AtomicBoolean rejected = new AtomicBoolean();

    @Override
    public OrderState initialState() {
      return new OrderState(false);
    }

    @Override
    public List<OrderConfirmed> decide(ConfirmOrder command, OrderState state) {
      decisions.incrementAndGet();
      if (state.confirmed()) {
        rejected.set(true);
        throw new DomainException("order already confirmed: " + command.orderId());
      }
      return List.of(new OrderConfirmed(command.orderId()));
    }

    @Override
    public OrderState evolve(OrderState state, OrderConfirmed event) {
      return new OrderState(true);
    }
  }

  /** Counts the interceptor callbacks so a replay is shown to complete the after() phase. */
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

  /**
   * A real mutual-exclusion lock that first makes BOTH callers arrive at {@code acquireLock} before
   * either may proceed — i.e. both have passed the pre-execution inbox lookup and missed. Whoever
   * then wins the lock executes; the other is serialized behind it exactly like a second replica
   * behind {@code PgAdvisoryLocker}.
   */
  static final class RendezvousLocker implements AggregateLocker {
    private final CountDownLatch bothPastPreCheck = new CountDownLatch(2);
    private final ReentrantLock lock = new ReentrantLock();
    final AtomicInteger acquisitions = new AtomicInteger();

    @Override
    public AutoCloseable acquireLock(StreamId streamId, Duration timeout) {
      acquisitions.incrementAndGet();
      bothPastPreCheck.countDown();
      await(bothPastPreCheck, "second caller never reached the lock");
      lock.lock();
      return lock::unlock;
    }
  }

  /** A locker that serializes nothing — two replicas each holding their own in-process lock. */
  static final AggregateLocker NON_SERIALIZING = (streamId, timeout) -> () -> {};

  /** Forwards to an {@link InMemoryEventStore}; subclasses intercept single calls. */
  abstract static class ForwardingEventStore implements EventStore {
    final InMemoryEventStore delegate;

    ForwardingEventStore(InMemoryEventStore delegate) {
      this.delegate = delegate;
    }

    @Override
    public AggregateHistory load(StreamId streamId) {
      return delegate.load(streamId);
    }

    @Override
    public AggregateHistory load(StreamId streamId, int expectedSnapshotVersion) {
      return delegate.load(streamId, expectedSnapshotVersion);
    }

    @Override
    public AppendResult append(StreamId streamId, List<EventEnvelope> events, Version expected) {
      return delegate.append(streamId, events, expected);
    }

    @Override
    public IdempotentAppendResult appendWithKey(
        StreamId streamId,
        List<EventEnvelope> events,
        Version expectedVersion,
        IdempotencyKey key,
        String commandType) {
      return delegate.appendWithKey(streamId, events, expectedVersion, key, commandType);
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
      delegate.saveSnapshot(streamId, version, state);
    }

    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      return delegate.readGlobalStream(afterOffset, maxCount);
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      return delegate.readStream(streamId, afterVersion, maxCount);
    }
  }

  private static void await(CountDownLatch latch, String what) {
    try {
      if (!latch.await(10, TimeUnit.SECONDS)) {
        throw new LockException("test rendezvous timed out: " + what);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new LockException("test rendezvous interrupted", e);
    }
  }

  private static VirtualThreadCommandBus bus(
      EventStore store,
      CommandInbox inbox,
      AggregateLocker locker,
      ConfirmDecider decider,
      CommandInterceptor... interceptors) {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .commandInbox(inbox)
        .locker(locker)
        .interceptors(interceptors)
        .register(TYPE, ConfirmOrder.class, cmd -> AggregateId.of(cmd.orderId()), decider)
        .build();
  }

  /** An inbox that answers normally until the decider has rejected, then fails every lookup. */
  private static CommandInbox inboxFailingAfterRejection(
      InMemoryCommandInbox delegate, ConfirmDecider decider) {
    return new CommandInbox() {
      @Override
      public Optional<InboxResult> find(IdempotencyKey key) {
        if (decider.rejected.get()) {
          throw new EventStoreException(
              "Failed to read command inbox", new java.sql.SQLException("connection reset"));
        }
        return delegate.find(key);
      }

      @Override
      public int deleteProcessedBefore(Instant cutoff) {
        return delegate.deleteProcessedBefore(cutoff);
      }
    };
  }

  private static VirtualThreadCommandBus busWithDeadLetterQueue(
      EventStore store,
      CommandInbox inbox,
      ConfirmDecider decider,
      DeadLetterQueue dlq,
      CommandInterceptor... interceptors) {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .commandInbox(inbox)
        .locker(new LocalStripedLocker(16))
        .deadLetterQueue(dlq)
        .objectMapper(new ObjectMapper())
        .interceptors(interceptors)
        .register(TYPE, ConfirmOrder.class, cmd -> AggregateId.of(cmd.orderId()), decider)
        .build();
  }

  private static <T> T join(Future<T> future) throws Exception {
    try {
      return future.get(10, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof Exception cause) {
        throw cause;
      }
      throw e;
    }
  }

  // === Tests ===

  /**
   * Two threads, same key, a lock that serializes them after both missed the pre-check. The loser
   * must get the winner's recorded outcome — not a business rejection from a decider run against
   * the winner's committed state.
   */
  @Test
  void lockSerializedDuplicate_receivesRecordedOutcome_deciderRunsOnce() throws Exception {
    var inbox = new InMemoryCommandInbox();
    var store = new InMemoryEventStore().withCommandInbox(inbox);
    var decider = new ConfirmDecider();
    var locker = new RendezvousLocker();
    var interceptor = new CountingInterceptor();
    var bus = bus(store, inbox, locker, decider, interceptor);

    var cmd = new ConfirmOrder("o-1");
    var key = IdempotencyKey.of("saga:s-1:7:0");

    CommandResult first;
    CommandResult second;
    try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
      Future<CommandResult> a = pool.submit(() -> bus.execute(cmd, key));
      Future<CommandResult> b = pool.submit(() -> bus.execute(cmd, key));
      first = join(a);
      second = join(b);
    }

    CommandResult executed = first.idempotentReplay() ? second : first;
    CommandResult replayed = first.idempotentReplay() ? first : second;
    assertFalse(executed.shortCircuited(), "exactly one caller executes the command");
    assertEquals(1, executed.globalOffsets().size());
    assertTrue(
        replayed.idempotentReplay(), "the lock-serialized duplicate is an idempotent replay");
    assertEquals(CommandBus.ShortCircuitReason.IDEMPOTENT_REPLAY, replayed.reason());
    assertEquals(executed.globalOffsets(), replayed.globalOffsets());
    assertEquals(executed.finalVersion(), replayed.finalVersion());
    assertEquals(executed.streamId(), replayed.streamId());

    assertEquals(
        2, locker.acquisitions.get(), "both callers passed the pre-check and took the lock");
    assertEquals(1, decider.decisions.get(), "the decider never runs for a recorded key");
    assertEquals(
        1,
        store.readStream(StreamId.of(TYPE, AggregateId.of("o-1")), Version.initial(), 100).size());
    assertEquals(2, interceptor.afters.get(), "the replay completes the after() phase too");
    assertEquals(0, interceptor.errors.get());
  }

  /**
   * The lock does not serialize across processes ({@code LocalStripedLocker} on two replicas). The
   * duplicate passes the under-lock lookup before the original commits, then loads the committed
   * state and the decider rejects. The rejection must be answered from the inbox, not propagated.
   */
  @Test
  void deciderRejectionAfterConcurrentCommit_isAnsweredFromTheInbox() throws Exception {
    var inbox = new InMemoryCommandInbox();
    var inMemory = new InMemoryEventStore().withCommandInbox(inbox);
    var secondLoaderInsideLoad = new CountDownLatch(1);
    var firstWriterCommitted = new CountDownLatch(1);
    var loads = new AtomicInteger();
    var appends = new AtomicInteger();
    var store =
        new ForwardingEventStore(inMemory) {
          @Override
          public AggregateHistory load(StreamId streamId, int expectedSnapshotVersion) {
            if (loads.incrementAndGet() == 2) {
              // The duplicate: every inbox lookup it could do before loading is behind it now.
              secondLoaderInsideLoad.countDown();
              await(firstWriterCommitted, "original never committed");
            }
            return super.load(streamId, expectedSnapshotVersion);
          }

          @Override
          public IdempotentAppendResult appendWithKey(
              StreamId streamId,
              List<EventEnvelope> events,
              Version expectedVersion,
              IdempotencyKey key,
              String commandType) {
            if (appends.incrementAndGet() == 1) {
              await(secondLoaderInsideLoad, "duplicate never reached load");
              var result = super.appendWithKey(streamId, events, expectedVersion, key, commandType);
              firstWriterCommitted.countDown();
              return result;
            }
            return super.appendWithKey(streamId, events, expectedVersion, key, commandType);
          }
        };
    var decider = new ConfirmDecider();
    var bus = bus(store, inbox, NON_SERIALIZING, decider);

    var cmd = new ConfirmOrder("o-2");
    var key = IdempotencyKey.of("saga:s-2:9:0");

    CommandResult first;
    CommandResult second;
    try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
      Future<CommandResult> a = pool.submit(() -> bus.execute(cmd, key));
      Future<CommandResult> b = pool.submit(() -> bus.execute(cmd, key));
      first = join(a);
      second = join(b);
    }

    CommandResult executed = first.idempotentReplay() ? second : first;
    CommandResult replayed = first.idempotentReplay() ? first : second;
    assertFalse(executed.shortCircuited());
    assertTrue(replayed.idempotentReplay(), "the rejected duplicate is answered as a replay");
    assertEquals(executed.globalOffsets(), replayed.globalOffsets());
    assertEquals(
        2, decider.decisions.get(), "the decider did run twice — the lock serialized nothing");
    assertTrue(decider.rejected.get(), "and the second run rejected against the committed state");
    assertEquals(1, appends.get(), "the rejected duplicate appended nothing");
    assertEquals(
        1,
        inMemory
            .readStream(StreamId.of(TYPE, AggregateId.of("o-2")), Version.initial(), 100)
            .size());
  }

  /**
   * The under-lock lookup runs on every attempt of the retry loop: an attempt that lost an
   * optimistic-lock race while a duplicate committed the same key must come back as a replay on the
   * next attempt, without a second decider run.
   */
  @Test
  void retryAttempt_findsKeyRecordedBetweenAttempts_returnsReplayWithoutRedeciding() {
    var inbox = new InMemoryCommandInbox();
    var inMemory = new InMemoryEventStore().withCommandInbox(inbox);
    var appends = new AtomicInteger();
    var store =
        new ForwardingEventStore(inMemory) {
          @Override
          public IdempotentAppendResult appendWithKey(
              StreamId streamId,
              List<EventEnvelope> events,
              Version expectedVersion,
              IdempotencyKey key,
              String commandType) {
            if (appends.incrementAndGet() == 1) {
              // A duplicate on another replica commits the same key; this attempt's own write
              // loses.
              super.appendWithKey(streamId, events, expectedVersion, key, commandType);
              throw new OptimisticLockException(streamId.value(), expectedVersion.value(), 1);
            }
            return super.appendWithKey(streamId, events, expectedVersion, key, commandType);
          }
        };
    var decider = new ConfirmDecider();
    var bus = bus(store, inbox, new LocalStripedLocker(16), decider);

    CommandResult result = bus.execute(new ConfirmOrder("o-3"), IdempotencyKey.of("saga:s-3:1:0"));

    assertTrue(result.idempotentReplay(), "the retried attempt returns the recorded outcome");
    assertEquals(1, decider.decisions.get(), "no second decider run on the retry");
    assertEquals(1, appends.get(), "the retry never reached the store again");
    var events =
        inMemory.readStream(StreamId.of(TYPE, AggregateId.of("o-3")), Version.initial(), 100);
    assertEquals(1, events.size());
    assertEquals(List.of(events.getFirst().globalOffset()), result.globalOffsets());
  }

  /** A genuine rejection — no recorded key — still propagates as the business error it is. */
  @Test
  void deciderRejection_withNoRecordedKey_propagates() {
    var inbox = new InMemoryCommandInbox();
    var store = new InMemoryEventStore().withCommandInbox(inbox);
    var decider = new ConfirmDecider();
    var bus = bus(store, inbox, new LocalStripedLocker(16), decider);

    bus.execute(new ConfirmOrder("o-4")); // unkeyed: confirmed, nothing recorded in the inbox
    var key = IdempotencyKey.of("saga:s-4:3:0");

    assertThrows(DomainException.class, () -> bus.execute(new ConfirmOrder("o-4"), key));
    assertTrue(inbox.find(key).isEmpty(), "a rejection records nothing");
    assertEquals(2, decider.decisions.get());
  }

  /**
   * When the decider rejects and the inbox cannot be read, the caller is told the outcome is
   * unknown (the infrastructure failure, with the rejection attached) rather than handed a
   * rejection that may contradict a recorded success. A saga classifies this as retry-later and
   * redelivers; the redelivery finds the key, or re-earns the rejection.
   */
  @Test
  void deciderRejection_whenInboxLookupFails_propagatesTheLookupFailureNotTheRejection() {
    var inMemoryInbox = new InMemoryCommandInbox();
    var decider = new ConfirmDecider();
    CommandInbox failingAfterRejection = inboxFailingAfterRejection(inMemoryInbox, decider);
    var store = new InMemoryEventStore().withCommandInbox(inMemoryInbox);
    var bus = bus(store, failingAfterRejection, new LocalStripedLocker(16), decider);

    bus.execute(new ConfirmOrder("o-5")); // confirmed, unkeyed
    var key = IdempotencyKey.of("saga:s-5:4:0");

    EventStoreException failure =
        assertThrows(EventStoreException.class, () -> bus.execute(new ConfirmOrder("o-5"), key));
    assertEquals(1, failure.getSuppressed().length, "the rejection travels with the failure");
    assertInstanceOf(DomainException.class, failure.getSuppressed()[0]);
    assertTrue(
        SagaCommandDispatch.isRetryLater(failure), "a saga redelivers instead of compensating");
    if (inMemoryInbox.find(key).isPresent()) {
      fail("nothing may be recorded for a command that did not execute");
    }
  }

  /**
   * The failure that says "outcome unknown" after a keyed rejection is never dead-lettered. Either
   * the key is recorded, and a replay would be an inbox no-op, or the domain rejected the command,
   * and a replay would re-run that decision later — possibly executing, once the state has moved
   * on, a command whose caller was told it was rejected.
   */
  @Test
  void deciderRejection_whenInboxLookupFails_isNeverDeadLettered() {
    var inMemoryInbox = new InMemoryCommandInbox();
    var decider = new ConfirmDecider();
    var store = new InMemoryEventStore().withCommandInbox(inMemoryInbox);
    var dlq = new InMemoryDeadLetterQueue();
    var interceptor = new CountingInterceptor();
    var bus =
        busWithDeadLetterQueue(
            store, inboxFailingAfterRejection(inMemoryInbox, decider), decider, dlq, interceptor);

    bus.execute(new ConfirmOrder("o-6")); // confirmed, unkeyed
    var key = IdempotencyKey.of("order-6-confirm");

    EventStoreException failure =
        assertThrows(EventStoreException.class, () -> bus.execute(new ConfirmOrder("o-6"), key));
    assertInstanceOf(DomainException.class, failure.getSuppressed()[0]);
    assertEquals(List.of(), dlq.all(), "a rejected command must never reach the dead letter queue");
    assertEquals(1, interceptor.errors.get(), "the caller's interceptors still see the failure");
  }

  /**
   * A dead-letter predicate that admits every failure does not change that: the lookup failure is
   * not a fault a replay could repair, whatever the predicate says about infrastructure failures.
   */
  @Test
  void deciderRejection_whenInboxLookupFails_isNotDeadLetteredEvenByAPermissivePredicate() {
    var inMemoryInbox = new InMemoryCommandInbox();
    var decider = new ConfirmDecider();
    var store = new InMemoryEventStore().withCommandInbox(inMemoryInbox);
    var dlq = new InMemoryDeadLetterQueue();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .commandInbox(inboxFailingAfterRejection(inMemoryInbox, decider))
            .locker(new LocalStripedLocker(16))
            .deadLetterQueue(dlq)
            .dlqEligible(failure -> true)
            .objectMapper(new ObjectMapper())
            .register(TYPE, ConfirmOrder.class, cmd -> AggregateId.of(cmd.orderId()), decider)
            .build();

    bus.execute(new ConfirmOrder("o-7"));
    var key = IdempotencyKey.of("order-7-confirm");

    assertThrows(EventStoreException.class, () -> bus.execute(new ConfirmOrder("o-7"), key));
    assertEquals(List.of(), dlq.all());
  }

  /**
   * A key recorded for another command type between the pre-execution lookup and the lock is
   * refused under the lock exactly like the pre-execution mismatch: nothing was committed, so the
   * caller gets the binding failure through the ordinary failure path — no "failure after events
   * were committed" report for a command that committed nothing.
   */
  @Test
  void underLockHit_boundToAnotherCommandType_failsAsNotCommitted() {
    var key = IdempotencyKey.of("order-8-confirm");
    var otherCommandsRow =
        new CommandInbox.InboxResult(
            key,
            "com.example.SomeOtherCommand",
            StreamId.of(TYPE, AggregateId.of("o-8")),
            new Version(1),
            List.of(GlobalOffset.of(1)),
            Instant.now());
    var lookups = new AtomicInteger();
    CommandInbox recordedBetweenPreCheckAndLock =
        new CommandInbox() {
          @Override
          public Optional<InboxResult> find(IdempotencyKey k) {
            // The pre-execution lookup misses; the lookup under the lock finds the other row.
            return lookups.incrementAndGet() == 1
                ? Optional.empty()
                : Optional.of(otherCommandsRow);
          }

          @Override
          public int deleteProcessedBefore(Instant cutoff) {
            return 0;
          }
        };
    var decider = new ConfirmDecider();
    var interceptor = new CountingInterceptor();
    var bus =
        busWithDeadLetterQueue(
            new InMemoryEventStore(),
            recordedBetweenPreCheckAndLock,
            decider,
            new InMemoryDeadLetterQueue(),
            interceptor);

    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(VirtualThreadCommandBus.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      assertThrows(IllegalArgumentException.class, () -> bus.execute(new ConfirmOrder("o-8"), key));
    } finally {
      logger.detachAppender(appender);
    }

    assertEquals(2, lookups.get());
    assertEquals(0, decider.decisions.get(), "the mismatch is refused before any decision");
    assertEquals(0, interceptor.afters.get());
    assertEquals(1, interceptor.errors.get());
    assertFalse(
        appender.list.stream()
            .anyMatch(e -> e.getFormattedMessage().contains("after events were committed")),
        "nothing was committed, so nothing may be reported as a post-commit failure");
  }
}
