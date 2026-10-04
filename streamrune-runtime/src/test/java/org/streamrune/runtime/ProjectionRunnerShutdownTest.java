package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;

/**
 * Stopping a projection runner or a subscription while a batch is in flight. {@code close()} /
 * {@code stop()} interrupt the worker thread to cut a sleep short; on a virtual thread a JDBC call
 * made while the interrupt flag is set fails at the socket, so the offset store used here refuses
 * every call made on an interrupted thread, the way the PostgreSQL stores do. The batch in flight
 * must still record its checkpoint, and leadership must be resigned on a thread whose flag is
 * clear.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class ProjectionRunnerShutdownTest {

  private static final AggregateType TYPE = AggregateType.of("thing");

  record Happened(String what) implements DomainEvent {}

  private static final ProjectionName NAME = ProjectionName.of("shutdown_test");
  private static final SubscriptionConfig FAST =
      new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(5));

  /** An offset store that fails, and records, every call made on an interrupted thread. */
  private static final class InterruptSensitiveOffsetStore implements OffsetStore {
    final InMemoryOffsetStore inner = new InMemoryOffsetStore();
    final List<String> callsOnInterruptedThread = new CopyOnWriteArrayList<>();

    private void refuseIfInterrupted(String call) {
      if (Thread.currentThread().isInterrupted()) {
        callsOnInterruptedThread.add(call);
        throw new IllegalStateException("offset store call " + call + " on an interrupted thread");
      }
    }

    @Override
    public GlobalOffset getLastOffset(ProjectionName projectionName) {
      refuseIfInterrupted("getLastOffset");
      return inner.getLastOffset(projectionName);
    }

    @Override
    public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
      refuseIfInterrupted("saveOffset");
      inner.saveOffset(projectionName, offset);
    }
  }

  /** Leadership that is always held at epoch 0 and records the interrupt flag at each resign. */
  private static final class RecordingLeadership implements SubscriptionLeadership {
    final List<Boolean> interruptedAtResign = new CopyOnWriteArrayList<>();

    @Override
    public Optional<Lease> tryAcquire(String consumerName) {
      return Optional.of(new Lease(0L));
    }

    @Override
    public Optional<Lease> current(String consumerName) {
      return Optional.of(new Lease(0L));
    }

    @Override
    public void resign(String consumerName) {
      interruptedAtResign.add(Thread.currentThread().isInterrupted());
    }

    @Override
    public void close() {}
  }

  /**
   * The non-transactional processor (the read model write, then {@code saveOffset} on the runner's
   * offset store), declared fencing-capable so a leadership can be configured; every lease here is
   * epoch 0, which no processor fences.
   */
  private static AtomicBatchProcessor fencingCapable() {
    AtomicBatchProcessor inner = AtomicBatchProcessor.nonAtomicAtLeastOnce();
    return new AtomicBatchProcessor() {
      @Override
      public void executeAtomically(
          ProjectionName projectionName,
          List<EventEnvelope> batch,
          GlobalOffset newOffset,
          long fencingEpoch,
          ProjectionUpdater projectionUpdater,
          OffsetStore offsetStore) {
        inner.executeAtomically(
            projectionName, batch, newOffset, fencingEpoch, projectionUpdater, offsetStore);
      }

      @Override
      public boolean supportsFencing() {
        return true;
      }
    };
  }

  /** A projection whose first batch keeps running until released, ignoring interrupts like JDBC. */
  private static final class HeldProjection implements Projection {
    final CountDownLatch entered = new CountDownLatch(1);
    final AtomicBoolean release = new AtomicBoolean();

    @Override
    public void process(List<EventEnvelope> batch) {
      entered.countDown();
      while (!release.get()) {
        Thread.onSpinWait();
      }
    }
  }

  private static InMemoryEventStore storeWithOneEvent() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("thing-1"));
    store.append(
        s,
        List.of(
            new EventEnvelope(
                GlobalOffset.of(1),
                s,
                new Version(1),
                new EventType("Happened"),
                new Happened("x"),
                new EventMetadata(
                    IdGenerator.generateEventId(),
                    IdGenerator.generateCommandId(),
                    null,
                    null,
                    CorrelationId.of("corr"),
                    null,
                    null,
                    Instant.now()))),
        Version.initial());
    return store;
  }

  /**
   * Starts {@code stop} on its own thread once the batch is in flight, gives it time to reach the
   * worker, then releases the held batch and waits for the stop to return.
   */
  private static void stopWhileHeld(Runnable stop, HeldProjection projection)
      throws InterruptedException {
    assertTrue(projection.entered.await(10, TimeUnit.SECONDS), "the batch must be in flight");
    Thread stopper = Thread.ofVirtual().start(stop);
    Thread.sleep(300); // long enough for an immediate interrupt to land on the worker
    projection.release.set(true);
    stopper.join(TimeUnit.SECONDS.toMillis(20));
    assertFalse(stopper.isAlive(), "the stop must return");
  }

  @Test
  void continuousRunner_closeDuringACatchUpBatch_recordsItsCheckpoint_andResignsUninterrupted()
      throws Exception {
    var offsets = new InterruptSensitiveOffsetStore();
    var leadership = new RecordingLeadership();
    var projection = new HeldProjection();
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(fencingCapable())
            .eventStore(storeWithOneEvent())
            .offsetStore(offsets)
            .fetchSize(100)
            .batchSize(50)
            .subscriptionConfig(FAST)
            .leadership(leadership)
            .build();
    var flagAfterRun = new AtomicReference<Boolean>();
    Thread worker =
        Thread.ofVirtual()
            .start(
                () -> {
                  runner.run(NAME, projection, AT_LEAST_ONCE_IDEMPOTENT);
                  flagAfterRun.set(Thread.currentThread().isInterrupted());
                });

    stopWhileHeld(runner::close, projection);
    worker.join(TimeUnit.SECONDS.toMillis(20));

    assertFalse(worker.isAlive());
    assertEquals(
        List.of(), offsets.callsOnInterruptedThread, "no store call on the interrupted run");
    assertEquals(
        1L,
        offsets.inner.getLastOffset(NAME).value(),
        "the batch in flight records its checkpoint");
    assertEquals(List.of(false), leadership.interruptedAtResign, "resign runs uninterrupted");
    assertEquals(Boolean.TRUE, flagAfterRun.get(), "the stop's interrupt is restored afterwards");
  }

  @Test
  void continuousRunner_closeInLiveMode_resignsOnAThreadWhoseFlagIsClear() throws Exception {
    var offsets = new InterruptSensitiveOffsetStore();
    var leadership = new RecordingLeadership();
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(fencingCapable())
            .eventStore(new InMemoryEventStore())
            .offsetStore(offsets)
            .fetchSize(100)
            .batchSize(50)
            .subscriptionConfig(FAST)
            .leadership(leadership)
            .build();
    var flagAfterRun = new AtomicReference<Boolean>();
    Thread worker =
        Thread.ofVirtual()
            .start(
                () -> {
                  runner.run(NAME, batch -> {}, AT_LEAST_ONCE_IDEMPOTENT);
                  flagAfterRun.set(Thread.currentThread().isInterrupted());
                });
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (runner.state() != ProjectionState.LIVE && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(ProjectionState.LIVE, runner.state());

    runner.close();
    worker.join(TimeUnit.SECONDS.toMillis(20));

    assertFalse(worker.isAlive());
    assertEquals(List.of(false), leadership.interruptedAtResign, "resign runs uninterrupted");
    assertEquals(Boolean.TRUE, flagAfterRun.get(), "the stop's interrupt is restored afterwards");
    assertEquals(List.of(), offsets.callsOnInterruptedThread);
  }

  @Test
  void scheduledRunner_stopDuringABatch_recordsItsCheckpoint_andResignsUninterrupted()
      throws Exception {
    var offsets = new InterruptSensitiveOffsetStore();
    var leadership = new RecordingLeadership();
    var projection = new HeldProjection();
    var runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(fencingCapable())
            .eventStore(storeWithOneEvent())
            .offsetStore(offsets)
            .leadership(leadership)
            .register(NAME.value(), projection, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    stopWhileHeld(runner::stop, projection);

    assertEquals(
        List.of(), offsets.callsOnInterruptedThread, "no store call on the interrupted run");
    assertEquals(
        1L,
        offsets.inner.getLastOffset(NAME).value(),
        "the batch in flight records its checkpoint");
    assertEquals(List.of(false), leadership.interruptedAtResign, "resign runs uninterrupted");
  }

  @Test
  void multiProjectionRunner_stopDuringACatchUpBatch_recordsItsCheckpoint() throws Exception {
    var offsets = new InterruptSensitiveOffsetStore();
    var leadership = new RecordingLeadership();
    var projection = new HeldProjection();
    var runner =
        MultiProjectionRunner.builder()
            .atomicProcessor(fencingCapable())
            .eventStore(storeWithOneEvent())
            .offsetStore(offsets)
            .subscriptionConfig(FAST)
            .leadership(leadership)
            .register(NAME.value(), projection, AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    stopWhileHeld(runner::stop, projection);

    assertEquals(
        List.of(), offsets.callsOnInterruptedThread, "no store call on the interrupted run");
    assertEquals(
        1L,
        offsets.inner.getLastOffset(NAME).value(),
        "the batch in flight records its checkpoint");
    assertEquals(List.of(false), leadership.interruptedAtResign, "resign runs uninterrupted");
  }

  @Test
  void pollingSubscription_closeDuringADelivery_savesItsOffset() throws Exception {
    var offsets = new InterruptSensitiveOffsetStore();
    var projection = new HeldProjection();
    var subscription =
        new PollingEventSubscription(
            SubscriptionName.of(NAME.value()),
            storeWithOneEvent(),
            offsets,
            projection::process,
            FAST,
            10);
    subscription.start();

    stopWhileHeld(subscription::close, projection);

    assertEquals(
        List.of(), offsets.callsOnInterruptedThread, "no store call on the interrupted poll");
    assertEquals(
        1L, offsets.inner.getLastOffset(NAME).value(), "the delivery in flight saves its offset");
  }

  @Test
  void streamSubscription_closeDuringADelivery_savesItsOffset() throws Exception {
    var offsets = new InterruptSensitiveOffsetStore();
    var projection = new HeldProjection();
    var subscription =
        StreamEventSubscription.builder()
            .subscriptionName(NAME.value())
            .streamId(StreamId.of(TYPE, AggregateId.of("thing-1")))
            .eventStore(storeWithOneEvent())
            .offsetStore(offsets)
            .listener(projection::process)
            .config(FAST)
            .fetchSize(10)
            .build();
    subscription.start();

    stopWhileHeld(subscription::close, projection);

    assertEquals(
        List.of(), offsets.callsOnInterruptedThread, "no store call on the interrupted poll");
    assertEquals(
        1L, offsets.inner.getLastOffset(NAME).value(), "the delivery in flight saves its offset");
  }
}
