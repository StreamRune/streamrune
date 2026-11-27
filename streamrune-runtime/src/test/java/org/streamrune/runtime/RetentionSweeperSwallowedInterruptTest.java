package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.InMemoryOutboxStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * A retention sweeper's {@code close()} ends its thread even when the store swallows the interrupt
 * that ends the sweep in flight: a connection pool, a driver or a wrapper around them that catches
 * the interruption and returns normally, leaving the thread's interrupt status clear. The sweeper
 * then has to see the stop without the status, or it sleeps out its interval, which is an hour by
 * default, after {@code close()} gave up waiting for it.
 */
class RetentionSweeperSwallowedInterruptTest {

  private static final Duration INTERVAL = Duration.ofHours(1);
  private static final Duration MAX_AGE = Duration.ofDays(1);
  private static final long JOIN_TIMEOUT_MS = 5_000;

  /** A sweeper under test, with what it was handed. */
  private interface Sweeper {
    void start();

    void close();
  }

  static Stream<Arguments> sweepers() {
    return Stream.of(
        sweeper(
            "inbox",
            CommandInbox.class,
            new InMemoryCommandInbox(),
            "deleteProcessedBefore",
            inbox -> {
              var s =
                  new InboxRetentionSweeper(
                      inbox, MAX_AGE, INTERVAL, Clock.systemUTC(), StreamRuneMetrics.NOOP);
              s.closeJoinTimeoutMs(JOIN_TIMEOUT_MS);
              return wrap(s::start, s::close);
            }),
        sweeper(
            "outbox",
            OutboxStore.class,
            new InMemoryOutboxStore(),
            "deleteDelivered",
            store -> {
              var s =
                  new OutboxRetentionSweeper(
                      store, MAX_AGE, MAX_AGE, INTERVAL, Clock.systemUTC(), StreamRuneMetrics.NOOP);
              s.closeJoinTimeoutMs(JOIN_TIMEOUT_MS);
              return wrap(s::start, s::close);
            }),
        sweeper(
            "command dead-letter",
            DeadLetterQueue.class,
            new InMemoryDeadLetterQueue(),
            "deleteOlderThan",
            queue -> {
              var s =
                  new DeadLetterRetentionSweeper(
                      queue, MAX_AGE, INTERVAL, Clock.systemUTC(), StreamRuneMetrics.NOOP);
              s.closeJoinTimeoutMs(JOIN_TIMEOUT_MS);
              return wrap(s::start, s::close);
            }),
        sweeper(
            "saga dead-letter",
            SagaDeadLetterStore.class,
            new InMemorySagaDeadLetterStore(new InMemorySagaStore()),
            "deleteOlderThan",
            store -> {
              var s =
                  new SagaDeadLetterRetentionSweeper(
                      store, MAX_AGE, INTERVAL, Clock.systemUTC(), StreamRuneMetrics.NOOP);
              s.closeJoinTimeoutMs(JOIN_TIMEOUT_MS);
              return wrap(s::start, s::close);
            }));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("sweepers")
  void closeEndsTheThreadWhenTheStoreSwallowsTheInterrupt(SwallowingStore<?> store)
      throws Exception {
    Sweeper sweeper = store.sweeper();
    sweeper.start();
    assertThat(store.entered.await(10, TimeUnit.SECONDS))
        .as("the sweep reached the store")
        .isTrue();

    sweeper.close();

    assertThat(store.sweepThread.isAlive()).as("the sweep thread after close()").isFalse();
  }

  private static Sweeper wrap(Runnable start, Runnable close) {
    return new Sweeper() {
      @Override
      public void start() {
        start.run();
      }

      @Override
      public void close() {
        close.run();
      }
    };
  }

  private static <T> Arguments sweeper(
      String name,
      Class<T> type,
      T delegate,
      String deleteMethod,
      Function<T, Sweeper> sweeperOverTheStore) {
    return Arguments.of(
        Named.of(name, new SwallowingStore<>(type, delegate, deleteMethod, sweeperOverTheStore)));
  }

  /**
   * A store that forwards to {@code delegate}, except its delete method, which waits until the
   * sweep thread is interrupted, swallows the interruption and returns {@code 0}.
   */
  static final class SwallowingStore<T> {
    final CountDownLatch entered = new CountDownLatch(1);
    volatile Thread sweepThread;
    private final T store;
    private final Function<T, Sweeper> sweeperOverTheStore;

    SwallowingStore(
        Class<T> type, T delegate, String deleteMethod, Function<T, Sweeper> sweeperOverTheStore) {
      this.sweeperOverTheStore = sweeperOverTheStore;
      this.store =
          type.cast(
              Proxy.newProxyInstance(
                  type.getClassLoader(),
                  new Class<?>[] {type},
                  (proxy, method, args) -> {
                    if (method.getName().equals(deleteMethod)) {
                      sweepThread = Thread.currentThread();
                      entered.countDown();
                      try {
                        new CountDownLatch(1).await();
                      } catch (InterruptedException swallowed) {
                        // The interrupt status stays clear, as a store that swallows it leaves it.
                      }
                      return 0;
                    }
                    try {
                      return method.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                      throw e.getCause();
                    }
                  }));
    }

    Sweeper sweeper() {
      return sweeperOverTheStore.apply(store);
    }
  }
}
