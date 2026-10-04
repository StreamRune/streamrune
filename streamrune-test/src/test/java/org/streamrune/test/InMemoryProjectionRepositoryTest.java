package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.streamrune.core.LockMode;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.PageRequest;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

class InMemoryProjectionRepositoryTest {

  record View(String id, int n) {}

  private final InMemoryProjectionRepository repo = new InMemoryProjectionRepository();
  private final ProjectionName orders = ProjectionName.of("orders");

  @Test
  void saveIsAnUpsert_andVersionsAdvanceFromZeroLikeTheJdbcRepository() {
    repo.save(orders, "a", new View("a", 1));
    assertEquals(0L, versionOf("a"), "an inserted row starts at version 0");
    repo.save(orders, "a", new View("a", 2));
    assertEquals(new View("a", 2), repo.findById(orders, "a", View.class).orElseThrow());
    assertEquals(1L, versionOf("a"));
    repo.save(orders, "a", new View("a", 3), 1L);
    assertEquals(2L, versionOf("a"));
    assertThrows(OptimisticLockException.class, () -> repo.save(orders, "a", new View("a", 4), 1L));
    assertEquals(new View("a", 3), repo.findById(orders, "a", View.class).orElseThrow());
  }

  @Test
  void aVersionedSaveNeverInsertsAMissingRow_atAnyExpectedVersion_onBothFaces() {
    var never = ProjectionName.of("never_written");
    repo.save(orders, "a", new View("a", 1));
    for (long expected : new long[] {0L, 1L}) {
      assertThrows(
          OptimisticLockException.class, () -> repo.save(orders, "b", new View("b", 1), expected));
      assertThrows(
          OptimisticLockException.class, () -> repo.save(never, "b", new View("b", 1), expected));
    }
    var conflict =
        assertThrows(
            OptimisticLockException.class, () -> repo.save(orders, "b", new View("b", 1), 0L));
    assertTrue(conflict.getMessage().contains("'orders/b'"), conflict.getMessage());
    assertTrue(repo.findById(orders, "b", View.class).isEmpty());
    assertFalse(repo.hasReadModels(never));

    repo.executeAtomically(
        orders,
        AtomicBatchProcessorContract.batch(1, 1),
        GlobalOffset.of(1),
        0L,
        tx -> {
          assertThrows(
              OptimisticLockException.class, () -> tx.save(orders, "b", new View("b", 1), 0L));
          tx.delete(orders, "a");
          assertThrows(
              OptimisticLockException.class, () -> tx.save(orders, "a", new View("a", 9), 0L));
        },
        repo);
    assertTrue(repo.findById(orders, "a", View.class).isEmpty(), "the delete committed");
    assertTrue(repo.findById(orders, "b", View.class).isEmpty(), "nothing was inserted");
  }

  @Test
  void inputsAreRefusedAtTheCallLikeTheJdbcRepository_onTheAutocommitFace() {
    var view = new View("a", 1);
    assertRefused("projectionName cannot be null", () -> repo.save(null, "a", view));
    assertRefused("id cannot be null or blank", () -> repo.save(orders, null, view));
    assertRefused("id cannot be null or blank", () -> repo.save(orders, " ", view));
    assertRefused("readModel cannot be null", () -> repo.save(orders, "a", null));
    assertRefused("readModel cannot be null", () -> repo.save(orders, "a", null, 0L));
    assertRefused("expectedVersion must be >= 0", () -> repo.save(orders, "a", view, -1L));
    assertRefused("id cannot be null or blank", () -> repo.save(orders, "", view, 0L));
    assertRefused("id cannot be null or blank", () -> repo.delete(orders, null));
    assertRefused("id cannot be null or blank", () -> repo.findById(orders, null, View.class));
    assertRefused(
        "id cannot be null or blank",
        () -> repo.findById(orders, "", View.class, LockMode.OPTIMISTIC));
    assertRefused("projectionName cannot be null", () -> repo.findAll(null, View.class));
    assertFalse(repo.hasReadModels(orders));
  }

  @Test
  void anInvalidWriteInsideTheTransactionIsRefusedAtTheWrite_andNothingOfTheBatchIsApplied() {
    var view = new View("a", 1);
    var customers = ProjectionName.of("customers");
    repo.save(orders, "a", view);
    var refused =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                repo.executeAtomically(
                    orders,
                    AtomicBatchProcessorContract.batch(1, 1),
                    GlobalOffset.of(1),
                    0L,
                    tx -> {
                      assertRefused("readModel cannot be null", () -> tx.save(orders, "x", null));
                      assertRefused(
                          "expectedVersion must be >= 0", () -> tx.save(orders, "a", view, -1L));
                      assertRefused(
                          "readModel cannot be null", () -> tx.save(orders, "a", null, 0L));
                      assertRefused(
                          "id cannot be null or blank", () -> tx.findById(orders, " ", View.class));
                      assertRefused(
                          "id cannot be null or blank",
                          () -> tx.findById(orders, null, View.class, LockMode.PESSIMISTIC));
                      assertRefused(
                          "projectionName cannot be null", () -> tx.findAll(null, View.class));
                      assertRefused("id cannot be null or blank", () -> tx.delete(orders, ""));
                      tx.delete(orders, "a");
                      tx.save(customers, "c", view);
                      tx.save(orders, null, view); // refused here, never at commit time
                    },
                    repo));
    assertEquals("id cannot be null or blank", refused.getMessage());
    assertEquals(view, repo.findById(orders, "a", View.class).orElseThrow(), "delete not applied");
    assertFalse(repo.hasReadModels(customers), "other names' writes not applied");
    assertEquals(GlobalOffset.initial(), repo.committedOffset(orders));
  }

  @Test
  void findAllAndPagesAreSortedById_andDeleteIsSilentForUnknownIds() {
    repo.save(orders, "c", new View("c", 3));
    repo.save(orders, "a", new View("a", 1));
    repo.save(orders, "b", new View("b", 2));
    assertEquals(
        List.of("a", "b", "c"), repo.findAll(orders, View.class).stream().map(View::id).toList());
    var page = repo.findAll(orders, View.class, PageRequest.of(1, 2));
    assertEquals(3L, page.totalElements());
    assertEquals(List.of("c"), page.content().stream().map(View::id).toList());
    repo.delete(orders, "zzz");
    repo.delete(orders, "a");
    assertEquals(2, repo.rowCount(orders));
    assertTrue(repo.hasReadModels(orders));
    assertFalse(repo.hasReadModels(ProjectionName.of("never_written")));
  }

  @Test
  void namesAreIsolated() {
    repo.save(orders, "a", new View("a", 1));
    assertTrue(repo.findById(ProjectionName.of("customers"), "a", View.class).isEmpty());
  }

  @Test
  void unknownNamesReadAsEmpty_andADeleteCreatesNoTable() {
    var never = ProjectionName.of("never_written");
    assertTrue(repo.findById(never, "a", View.class, LockMode.PESSIMISTIC).isEmpty());
    assertEquals(List.of(), repo.findAll(never, View.class));
    assertEquals(0L, repo.findAll(never, View.class, PageRequest.of(0, 10)).totalElements());
    repo.delete(never, "a");
    assertFalse(repo.hasReadModels(never));
    assertEquals(0, repo.rowCount(never));
  }

  @Test
  void offsetStoreFace_isTheProcessorsOwnCheckpoint_withTheMonotonicGuardAndAnUnguardedReset() {
    assertEquals(GlobalOffset.initial(), repo.getLastOffset(orders));
    repo.saveOffset(orders, GlobalOffset.of(5));
    repo.saveOffset(orders, GlobalOffset.of(3)); // sideways/backward: silent no-op
    assertEquals(GlobalOffset.of(5), repo.getLastOffset(orders));
    assertEquals(GlobalOffset.of(5), repo.committedOffset(orders));
    repo.reset(orders);
    assertEquals(GlobalOffset.initial(), repo.getLastOffset(orders));
  }

  @Test
  void stampFencingEpochIsMonotonic_andNeverMovesTheOffset() {
    assertTrue(repo.supportsFencing());
    repo.saveOffset(orders, GlobalOffset.of(5));
    repo.stampFencingEpoch(orders, 3L);
    repo.stampFencingEpoch(orders, 2L);
    assertEquals(3L, repo.stampedEpoch(orders));
    assertEquals(GlobalOffset.of(5), repo.committedOffset(orders));
    repo.stampFencingEpoch(orders, 0L);
    assertEquals(3L, repo.stampedEpoch(orders));
  }

  @Test
  void aCommitStampsTheGreaterEpoch_andANonAdvancingOffsetIsRejected() {
    repo.executeAtomically(orders, List.of(), GlobalOffset.of(4), 2L, r -> {}, repo);
    assertEquals(2L, repo.stampedEpoch(orders));
    assertThrows(
        ProjectionCommitFencedException.class,
        () -> repo.executeAtomically(orders, List.of(), GlobalOffset.of(4), 2L, r -> {}, repo));
  }

  @Test
  void readYourOwnWritesInsideTheTransaction() {
    repo.executeAtomically(
        orders,
        AtomicBatchProcessorContract.batch(1, 1),
        GlobalOffset.of(1),
        0L,
        tx -> {
          tx.save(orders, "a", new View("a", 1));
          assertEquals(new View("a", 1), tx.findById(orders, "a", View.class).orElseThrow());
          assertTrue(repo.findById(orders, "a", View.class).isEmpty(), "not yet committed");
        },
        repo);
    assertEquals(new View("a", 1), repo.findById(orders, "a", View.class).orElseThrow());
  }

  @Test
  void aDeleteInsideTheTransactionReadsAsAbsent_andIsGoneOnlyAfterTheCommit() {
    repo.save(orders, "a", new View("a", 1));
    repo.save(orders, "b", new View("b", 2));
    var never = ProjectionName.of("never_written");
    repo.executeAtomically(
        orders,
        AtomicBatchProcessorContract.batch(1, 1),
        GlobalOffset.of(1),
        0L,
        tx -> {
          tx.delete(orders, "a");
          tx.delete(never, "x");
          assertEquals(List.of(), tx.findAll(never, View.class));
          assertTrue(tx.findById(orders, "a", View.class).isEmpty(), "deleted inside");
          assertTrue(tx.findById(orders, "a", View.class, LockMode.OPTIMISTIC).isEmpty());
          assertEquals(
              List.of("b"), tx.findAll(orders, View.class).stream().map(View::id).toList());
          assertTrue(repo.findById(orders, "a", View.class).isPresent(), "not yet committed");
        },
        repo);
    assertTrue(repo.findById(orders, "a", View.class).isEmpty(), "gone after the commit");
    assertEquals(1, repo.rowCount(orders));
    assertFalse(repo.hasReadModels(never), "a delete of a never-written name creates no table");
  }

  @Test
  void aRowDeletedThenRecreatedInOneTransaction_isCommittedWithItsNewValue() {
    repo.save(orders, "a", new View("a", 1));
    repo.save(orders, "a", new View("a", 2));
    repo.executeAtomically(
        orders,
        AtomicBatchProcessorContract.batch(1, 1),
        GlobalOffset.of(1),
        0L,
        tx -> {
          assertEquals(
              1L,
              tx.findById(orders, "a", View.class, LockMode.OPTIMISTIC).orElseThrow().version());
          tx.delete(orders, "a");
          tx.save(orders, "a", new View("a", 9));
          assertEquals(new View("a", 9), tx.findById(orders, "a", View.class).orElseThrow());
          assertEquals(
              0L,
              tx.findById(orders, "a", View.class, LockMode.PESSIMISTIC).orElseThrow().version(),
              "a recreated row is a new row");
          tx.save(orders, "a", new View("a", 10), 0L);
          assertEquals(
              1L,
              tx.findById(orders, "a", View.class, LockMode.OPTIMISTIC).orElseThrow().version());
          assertThrows(
              OptimisticLockException.class, () -> tx.save(orders, "a", new View("a", 11), 5L));
          tx.save(orders, "c", new View("c", 3));
          tx.save(orders, "c", new View("c", 4));
          tx.delete(orders, "c");
          tx.delete(orders, "c");
          tx.save(orders, "d", new View("d", 5));
          var page = tx.findAll(orders, View.class, PageRequest.of(0, 1));
          assertEquals(2L, page.totalElements());
          assertEquals(List.of("a"), page.content().stream().map(View::id).toList());
          assertThrows(IllegalArgumentException.class, () -> tx.afterCommit(null));
          assertSame(repo, tx.writeTargetIdentity());
        },
        repo);
    assertEquals(new View("a", 10), repo.findById(orders, "a", View.class).orElseThrow());
    assertEquals(1L, versionOf("a"));
    assertTrue(repo.findById(orders, "c", View.class).isEmpty());
    assertEquals(
        List.of("a", "d"), repo.findAll(orders, View.class).stream().map(View::id).toList());
  }

  @Test
  void aCommitWithTheUnfencedEpochKeepsTheStampedOne() {
    repo.stampFencingEpoch(orders, 4L);
    repo.executeAtomically(orders, List.of(), GlobalOffset.of(1), 0L, r -> {}, repo);
    assertEquals(4L, repo.stampedEpoch(orders));
    assertEquals(GlobalOffset.of(1), repo.committedOffset(orders));
    repo.executeAtomically(orders, List.of(), GlobalOffset.of(2), 5L, r -> {}, repo);
    assertEquals(5L, repo.stampedEpoch(orders));
  }

  @Test
  void theMonotonicGuardReadsTheCheckpointAfterTheUpdater() {
    assertThrows(
        ProjectionCommitFencedException.class,
        () ->
            repo.executeAtomically(
                orders,
                AtomicBatchProcessorContract.batch(1, 1),
                GlobalOffset.of(1),
                0L,
                tx -> {
                  tx.save(orders, "a", new View("a", 1));
                  repo.saveOffset(orders, GlobalOffset.of(5));
                },
                repo));
    assertTrue(repo.findById(orders, "a", View.class).isEmpty(), "the staged write is dropped");
    assertEquals(GlobalOffset.of(5), repo.committedOffset(orders));
  }

  @Test
  void aTransactionViewIsOverOnceItsBatchIs_afterACommitAndAfterARollback() {
    var committedView = new AtomicReference<ProjectionRepository>();
    var lateWrite = new AtomicReference<IllegalStateException>();
    var registeredFromAnAction = new AtomicBoolean();
    repo.executeAtomically(
        orders,
        AtomicBatchProcessorContract.batch(1, 1),
        GlobalOffset.of(1),
        0L,
        tx -> {
          committedView.set(tx);
          tx.save(orders, "a", new View("a", 1));
          tx.afterCommit(
              () -> {
                lateWrite.set(
                    assertThrows(
                        IllegalStateException.class,
                        () -> tx.save(orders, "late", new View("late", 1))));
                tx.afterCommit(() -> registeredFromAnAction.set(true));
              });
        },
        repo);
    assertNotNull(lateWrite.get(), "a write from an after-commit action is refused");
    assertTrue(repo.findById(orders, "late", View.class).isEmpty());
    assertTrue(registeredFromAnAction.get(), "an action registered after the commit runs at once");
    assertEquals(new View("a", 1), repo.findById(orders, "a", View.class).orElseThrow());
    assertRefusesEveryReadAndWrite(committedView.get());
    var registeredLater = new AtomicBoolean();
    committedView.get().afterCommit(() -> registeredLater.set(true));
    assertTrue(registeredLater.get(), "registered on a committed view: runs at once");

    var rolledBackView = new AtomicReference<ProjectionRepository>();
    var registeredBeforeTheRollback = new AtomicBoolean();
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            repo.executeAtomically(
                orders,
                AtomicBatchProcessorContract.batch(2, 2),
                GlobalOffset.of(2),
                0L,
                tx -> {
                  rolledBackView.set(tx);
                  tx.afterCommit(() -> registeredBeforeTheRollback.set(true));
                  throw new UnsupportedOperationException("updater failed");
                },
                repo));
    assertRefusesEveryReadAndWrite(rolledBackView.get());
    var registeredAfterTheRollback = new AtomicBoolean();
    rolledBackView.get().afterCommit(() -> registeredAfterTheRollback.set(true));
    assertFalse(registeredBeforeTheRollback.get(), "dropped on rollback");
    assertFalse(registeredAfterTheRollback.get(), "registered on a rolled-back view: discarded");
    assertEquals(GlobalOffset.of(1), repo.committedOffset(orders));
  }

  /**
   * An after-commit action that throws does not stop the remaining actions and never undoes the
   * applied batch or its checkpoint. Where the JDBC repository logs the failure, the test double
   * rethrows the first one, the later ones suppressed, after every action has run.
   */
  @Test
  void aFailingAfterCommitAction_theRestStillRun_theBatchStaysApplied_theFirstFailureIsRethrown() {
    var first = new IllegalStateException("first action failed");
    var second = new IllegalArgumentException("second action failed");
    var laterActionRan = new AtomicBoolean();
    var thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                repo.executeAtomically(
                    orders,
                    AtomicBatchProcessorContract.batch(1, 1),
                    GlobalOffset.of(1),
                    0L,
                    tx -> {
                      tx.save(orders, "a", new View("a", 1));
                      tx.afterCommit(
                          () -> {
                            throw first;
                          });
                      tx.afterCommit(
                          () -> {
                            throw second;
                          });
                      tx.afterCommit(() -> laterActionRan.set(true));
                    },
                    repo));
    assertSame(first, thrown);
    assertArrayEquals(new Throwable[] {second}, thrown.getSuppressed());
    assertTrue(laterActionRan.get(), "the remaining actions still run");
    assertEquals(new View("a", 1), repo.findById(orders, "a", View.class).orElseThrow());
    assertEquals(GlobalOffset.of(1), repo.committedOffset(orders));
  }

  @Test
  void aReaderOnAnotherThreadSeesABatchsRowsAndItsCheckpointTogether() throws Exception {
    int batches = 3_000;
    int rowsPerBatch = 200;
    var done = new AtomicBoolean();
    var torn = new AtomicInteger();
    var started = new CountDownLatch(1);
    Thread reader =
        Thread.ofPlatform()
            .start(
                () -> {
                  started.countDown();
                  do {
                    int newestRow =
                        repo.findAll(orders, View.class).stream().mapToInt(View::n).max().orElse(0);
                    long checkpoint = repo.committedOffset(orders).value();
                    if (checkpoint < newestRow) {
                      torn.incrementAndGet();
                    }
                  } while (!done.get());
                });
    try {
      assertTrue(started.await(10, TimeUnit.SECONDS), "the reader started");
      for (int i = 1; i <= batches; i++) {
        int n = i;
        repo.executeAtomically(
            orders,
            List.of(),
            GlobalOffset.of(n),
            0L,
            tx ->
                IntStream.range(0, rowsPerBatch)
                    .forEach(k -> tx.save(orders, "row-" + k, new View("row-" + k, n))),
            repo);
      }
    } finally {
      done.set(true);
      reader.join(10_000);
    }
    assertEquals(0, torn.get(), "reads that saw a batch's rows before its checkpoint");
  }

  private long versionOf(String id) {
    return repo.findById(orders, id, View.class, LockMode.OPTIMISTIC).orElseThrow().version();
  }

  private void assertRefusesEveryReadAndWrite(ProjectionRepository view) {
    var v = new View("x", 1);
    List<Executable> calls =
        List.of(
            () -> view.save(orders, "x", v),
            () -> view.save(orders, "a", v, 0L),
            () -> view.findById(orders, "a", View.class),
            () -> view.findById(orders, "a", View.class, LockMode.OPTIMISTIC),
            () -> view.findAll(orders, View.class),
            () -> view.findAll(orders, View.class, PageRequest.of(0, 1)),
            () -> view.delete(orders, "a"));
    for (Executable call : calls) {
      var refused = assertThrows(IllegalStateException.class, call);
      assertTrue(refused.getMessage().contains("transaction is over"), refused.getMessage());
    }
  }

  private static void assertRefused(String message, Executable call) {
    assertEquals(message, assertThrows(IllegalArgumentException.class, call).getMessage());
  }
}
