package org.streamrune.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;

/**
 * The reads of the request context answer with the bindings of the thread that reads them, on
 * virtual threads under load.
 *
 * <p>Saga-like threads bind what saga dispatch binds — the context with the saga's reserved
 * correlation id, {@code SAGA_OWNED}, the cause with the saga's origin and the forward marker — one
 * saga after another, and read the bindings back the way the command bus does: {@link
 * StreamRuneContext#requireNoForeignReservedCorrelationId()} first, then {@link
 * StreamRuneContext#capture()}, {@link StreamRuneContext#cause()}, {@link
 * StreamRuneContext#isSagaDispatch()} and {@link StreamRuneContext#sagaForwardOf}. Between their
 * reads they block the way a command does (a contended lock, a monitor, a park), and other virtual
 * threads run with bindings of their own. Every read must name the thread's own saga; outside a
 * binding a thread reads that nothing is bound.
 *
 * <p>On JDK 25 with the Graal JIT, {@code ScopedValue} reads on virtual threads are not reliable;
 * every test JVM of the build runs with a workaround (the root build), and the GraalVM workflow
 * runs this test on the Graal JIT with each of the two. {@code
 * streamrune.test.virtualThreadReadSeconds} lengthens the run.
 */
class StreamRuneContextVirtualThreadReadTest {

  private static final SagaType SAGA = SagaType.of("org.example.FulfilmentState");
  private static final Object[] MONITORS = {new Object(), new Object(), new Object()};
  private static final ReentrantLock[] LOCKS = {
    new ReentrantLock(), new ReentrantLock(), new ReentrantLock()
  };

  @Test
  void everyReadNamesTheBindingsOfTheReadingThread() throws InterruptedException {
    Duration run = Duration.ofSeconds(Long.getLong("streamrune.test.virtualThreadReadSeconds", 4L));
    AtomicBoolean stop = new AtomicBoolean();
    AtomicLong reads = new AtomicLong();
    ConcurrentLinkedQueue<String> disagreements = new ConcurrentLinkedQueue<>();
    List<Thread> threads = new ArrayList<>();
    AtomicLong sagas = new AtomicLong();
    for (int t = 0; t < 6; t++) {
      threads.add(
          Thread.ofVirtual()
              .name("saga-like-" + t)
              .start(
                  () -> {
                    while (!stop.get()) {
                      dispatchAsASagaDoes(
                          SagaId.of("saga-" + sagas.incrementAndGet()), reads, disagreements);
                    }
                  }));
    }
    for (int t = 0; t < 100; t++) {
      threads.add(
          Thread.ofVirtual()
              .name("noise-" + t)
              .start(
                  () -> {
                    var random = ThreadLocalRandom.current();
                    while (!stop.get()) {
                      ScopedValue.where(
                              StreamRuneContext.SAGA_OWNED, Boolean.valueOf(random.nextBoolean()))
                          .run(StreamRuneContextVirtualThreadReadTest::blockAsACommandDoes);
                    }
                  }));
    }
    Thread.sleep(run.toMillis());
    stop.set(true);
    for (Thread thread : threads) {
      thread.join(Duration.ofSeconds(10));
    }

    assertThat(reads.get()).as("the run read the bindings").isPositive();
    assertThat(disagreements).as("reads that did not name the thread's own bindings").isEmpty();
  }

  /** What saga dispatch binds around a forward command, and what the command bus reads under it. */
  private static void dispatchAsASagaDoes(
      SagaId sagaId, AtomicLong reads, ConcurrentLinkedQueue<String> disagreements) {
    expectNothingBound(sagaId, "before the dispatch", disagreements);
    SagaOrigin origin = SagaOrigin.dispatchedBy(SAGA, sagaId);
    IdempotencyKey key = new IdempotencyKey("saga:" + sagaId.value() + ":1:0");
    var context =
        new StreamRuneContext.RequestContext(
            null, null, origin.correlationId(), Instant.now(), Map.of());
    ScopedValue.where(StreamRuneContext.CURRENT, context)
        .where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE)
        .where(
            StreamRuneContext.CAUSE,
            new StreamRuneContext.DispatchCause(CausationId.of("evt-" + sagaId.value()), origin))
        .where(StreamRuneContext.SAGA_FORWARD, new StreamRuneContext.SagaForward(origin, key))
        .run(
            () -> {
              expectTheSagasBindings(origin, key, "at the dispatch", disagreements);
              blockAsACommandDoes();
              expectTheSagasBindings(origin, key, "after blocking", disagreements);
              reads.addAndGet(2);
            });
  }

  private static void expectTheSagasBindings(
      SagaOrigin origin,
      IdempotencyKey key,
      String where,
      ConcurrentLinkedQueue<String> disagreements) {
    try {
      StreamRuneContext.requireNoForeignReservedCorrelationId();
    } catch (ReservedCorrelationIdException refused) {
      disagreements.add(where + ": the saga's own correlation id was refused");
      return;
    }
    StreamRuneContext.RequestContext context = StreamRuneContext.capture();
    StreamRuneContext.DispatchCause cause = StreamRuneContext.cause();
    if (context == null
        || !origin.correlationId().equals(context.correlationId())
        || cause == null
        || !origin.equals(cause.sagaOrigin())
        || !StreamRuneContext.isSagaDispatch()
        || !origin.equals(StreamRuneContext.sagaForwardOf(key))) {
      disagreements.add(
          where
              + ": context "
              + (context == null ? null : context.correlationId())
              + ", cause "
              + (cause == null ? null : cause.sagaOrigin())
              + ", saga dispatch "
              + StreamRuneContext.isSagaDispatch()
              + ", expected "
              + origin);
    }
  }

  private static void expectNothingBound(
      SagaId sagaId, String where, ConcurrentLinkedQueue<String> disagreements) {
    StreamRuneContext.RequestContext context = StreamRuneContext.capture();
    StreamRuneContext.DispatchCause cause = StreamRuneContext.cause();
    boolean sagaDispatch = StreamRuneContext.isSagaDispatch();
    if (context != null || cause != null || sagaDispatch) {
      disagreements.add(
          where
              + " "
              + sagaId.value()
              + ": read outside every binding: context "
              + (context == null ? null : context.correlationId())
              + ", cause "
              + (cause == null ? null : cause.sagaOrigin())
              + ", saga dispatch "
              + sagaDispatch);
    }
  }

  private static void blockAsACommandDoes() {
    var random = ThreadLocalRandom.current();
    switch (random.nextInt(4)) {
      case 0 -> {
        ReentrantLock lock = LOCKS[random.nextInt(LOCKS.length)];
        lock.lock();
        try {
          LockSupport.parkNanos(random.nextInt(20_000));
        } finally {
          lock.unlock();
        }
      }
      case 1 -> {
        synchronized (MONITORS[random.nextInt(MONITORS.length)]) {
          LockSupport.parkNanos(random.nextInt(20_000));
        }
      }
      case 2 -> Thread.yield();
      default -> LockSupport.parkNanos(random.nextInt(10_000));
    }
  }
}
