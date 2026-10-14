package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStore;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.saga.SagaStore.AppliedEvent;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/** The forward fence of {@link InMemoryEventStore} against an {@link InMemorySagaStore}. */
class InMemorySagaForwardFenceTest extends SagaForwardFenceContract {

  private final InMemorySagaStore rows = new InMemorySagaStore();
  private final InMemoryEventStore events =
      new InMemoryEventStore().withCommandInbox(new InMemoryCommandInbox()).withSagaStore(rows);

  @Override
  protected EventStore eventStore() {
    return events;
  }

  @Override
  protected SagaStore sagaStore() {
    return rows;
  }

  /**
   * A claim that arrives while a fenced append is between its check of the saga entry and the end
   * of its writes waits for the append: the forward command is stored before the episode opens.
   */
  @Test
  void aClaimWaitsForAFencedAppendInFlight() throws Exception {
    forwardRow(ID);
    var insideFence = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    events.afterFenceRead(
        () -> {
          insideFence.countDown();
          await(release);
        });

    var appended =
        CompletableFuture.supplyAsync(
            () -> append(TARGET, IdempotencyKey.of("fwd-in-flight"), FORWARD_OF));
    assertTrue(insideFence.await(10, TimeUnit.SECONDS), "the append reached the fence");
    var claimed =
        CompletableFuture.runAsync(
            () -> rows.claimCompensating(ID, TYPE, new FenceState(SagaStatus.COMPENSATING), 1L));

    assertThrows(
        TimeoutException.class,
        () -> claimed.get(300, TimeUnit.MILLISECONDS),
        "the claim waits while the fenced append holds the saga");
    assertFalse(appended.isDone());

    release.countDown();
    assertFalse(appended.get(10, TimeUnit.SECONDS).alreadyApplied());
    claimed.get(10, TimeUnit.SECONDS);

    var row = rows.load(ID, TYPE, FenceState.class).orElseThrow();
    assertEquals(SagaStatus.COMPENSATING, row.status());
    assertEquals(2L, row.episodeVersion(), "the claim stamped the episode after the append");
    assertEquals(1, stored(TARGET).size(), "the forward command was stored before the stamp");
  }

  /** The hook runs once: a second fenced append does not run it again. */
  @Test
  void theFenceHookRunsOnce() {
    forwardRow(ID);
    int[] runs = new int[1];
    events.afterFenceRead(() -> runs[0]++);

    append(TARGET, IdempotencyKey.of("fwd-a"), FORWARD_OF);
    append(TARGET, IdempotencyKey.of("fwd-b"), FORWARD_OF);

    assertEquals(1, runs[0]);
  }

  @Test
  void aFencedAppendWithoutAWiredSagaStoreIsUnsupported() {
    var unwired = new InMemoryEventStore().withCommandInbox(new InMemoryCommandInbox());

    var ex =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                unwired.appendWithKey(
                    TARGET,
                    List.of(event(TARGET, 1)),
                    Version.initial(),
                    IdempotencyKey.of("fwd-unwired"),
                    COMMAND_TYPE,
                    FORWARD_OF));
    assertEquals("No InMemorySagaStore wired; call withSagaStore(...)", ex.getMessage());
    assertNull(unwired.sagaFenceStorageIdentity(), "an unwired store cannot fence");
    assertTrue(unwired.readStream(TARGET, Version.initial(), 10).isEmpty());
  }

  @Test
  void withSagaStoreRequiresAStore() {
    var ex =
        assertThrows(
            NullPointerException.class, () -> new InMemoryEventStore().withSagaStore(null));
    assertEquals("sagaStore is required", ex.getMessage());
  }

  /** The writes that end a saga or touch its row wait for a fenced append, like the claim. */
  @Test
  void everyRowWriteWaitsForAFencedAppendInFlight() throws Exception {
    for (String write :
        List.of(
            "markFaulted",
            "setDeadLetterPending",
            "delete",
            "update",
            "applyEvent",
            "claimCompensating")) {
      var sagaId = SagaId.of("fence-write-" + write);
      forwardRow(sagaId);
      var origin = forwardOf(sagaId);
      var target = StreamId.of(AggregateType.of("fence_target"), AggregateId.of(sagaId.value()));
      var insideFence = new CountDownLatch(1);
      var release = new CountDownLatch(1);
      events.afterFenceRead(
          () -> {
            insideFence.countDown();
            await(release);
          });
      var appended =
          CompletableFuture.supplyAsync(
              () -> append(target, IdempotencyKey.of("k-" + write), origin));
      assertTrue(insideFence.await(10, TimeUnit.SECONDS));
      Runnable rowWrite =
          switch (write) {
            case "markFaulted" -> () -> rows.markFaulted(sagaId, TYPE, 1L);
            case "setDeadLetterPending" -> () -> rows.setDeadLetterPending(sagaId, TYPE, true);
            case "delete" -> () -> rows.delete(sagaId, TYPE);
            case "applyEvent" ->
                () ->
                    rows.applyEvent(
                        sagaId,
                        TYPE,
                        new FenceState(SagaStatus.COMPLETED),
                        SagaStatus.COMPLETED,
                        1L,
                        AppliedEvent.live(GlobalOffset.of(42L)));
            case "claimCompensating" ->
                () ->
                    rows.claimCompensating(
                        sagaId, TYPE, new FenceState(SagaStatus.COMPENSATING), 1L);
            default ->
                () ->
                    rows.update(
                        sagaId,
                        TYPE,
                        new FenceState(SagaStatus.COMPLETED),
                        SagaStatus.COMPLETED,
                        1L);
          };
      var written = CompletableFuture.runAsync(rowWrite);

      assertThrows(
          TimeoutException.class, () -> written.get(300, TimeUnit.MILLISECONDS), write + " waits");
      release.countDown();
      appended.get(10, TimeUnit.SECONDS);
      written.get(10, TimeUnit.SECONDS);
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(30, TimeUnit.SECONDS)) {
        throw new AssertionError("the test never released the fence");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }
}
